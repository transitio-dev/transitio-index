"""Stage 6, curation half: apply ``overrides/edges.yaml`` to the classified
edges and publish the ``curate`` generation (``edges_final.jsonl``).

The generated index is disposable, the override file is the asset: every
build re-applies the curator's decisions to freshly classified edges. Four
operations, two scopes. ``set_tiers`` is pair-scoped — it redefines the tier
set of a (feed, place) pair: tiers dropped lose their edges, tiers added get
a new edge with ``selector_state = "unavailable"`` (adding a tier to a bundle
is no evidence that every route qualifies for it), tiers kept keep their
selector, service, evidence and confidence and gain the curation stamp like
the rest of the pair. ``set_selector``, ``add_edge`` and ``remove_edge`` are
tier-edge scoped. ``place`` and ``tier`` accept ``"*"``; a more specific
entry always wins the targets it covers, whatever the file order, and two
entries of the same specificity aimed at one target are a build error,
never a last-one-wins race. Operations compose in a fixed order — tiers
set, edges added, selectors set, edges removed — so a selector for a tier
another entry creates works wherever it sits in the file, and the pair
invariants are checked over the final state, not mid-batch.

Every operation stamps ``method = "human"`` and a ``curation`` struct. A tier
decision (``set_tiers``, ``add_edge``) sets ``tier_confidence = 1.0`` unless
the entry says otherwise — an ``unknown`` tier always 0.0; ``set_selector``
is a mechanical correction and changes no confidence; ``needs_review`` is
recomputed against the usual cutoff. Membership evidence and route evidence
stay independent: a new edge copies its pair's ``service``, keeps the
pair's coverage evidence and the machine's per-tier classification
evidence under ``classified``, and takes the feed's fingerprint when the
feed was crawled; a curator's selector predicate (``agency_id`` and/or
``route_type``, ANDed) is expanded to route ids against the crawl artifacts
— a feed with no route evidence can only get ``unavailable``, ``whole_feed``
included.

GTFS-RT feeds are not tiered — tier needs route-level evidence — so a
linked one inherits the place membership of its static feed, as curated,
as one ``unknown`` edge per place carrying the pair's service level: the
propagation runs after the static-side overrides and before the RT-side
ones. An RT feed with no static link keeps its declared coverage.

Stale does not mean ignored. An entry's ``evidence_hash`` records the
machine evidence of everything the entry targets, hashed once as a set;
when the current evidence differs the override is still applied, every
edge it touches carries ``curation.stale`` (sticky across later
operations), the entry is listed in ``cache/override_staleness_report.jsonl``
with the current hash to record, and the manifest counts it — ``strict``
turns that count into a failure. The manifest also records the digest of
the very bytes the overrides were parsed from, so publish refuses a
generation built from an edited file.
"""

import collections
import contextlib
import copy
import datetime
import hashlib
import json

from transitio.index import fingerprint
from index_build import classify, coverage, crawl, overrides, store

CURATE_POINTER = classify.CURATE_POINTER
EDGES_ARTIFACT = classify.CURATED_EDGES_ARTIFACT
FEEDS_ARTIFACT = classify.CURATED_FEEDS_ARTIFACT
REPORT_FILE = "override_staleness_report.jsonl"
REVIEW_CUTOFF = classify.REVIEW_CUTOFF
WILDCARD = "*"
# The order operations compose in, whatever the file order.
PHASES = ("set_tiers", "add_edge", "set_selector", "remove_edge")
# The coverage stage's evidence keys: facts about the pair, not a tier.
PAIR_EVIDENCE = ("stops_in_place", "stop_share", "declared_level", "declared_place_id")


class CurateError(RuntimeError):
    """An override cannot be applied to the edges as they are."""


def evidence_hash(edges, targets=(), feed_id=None, route_evidence=None):
    """The hash a curator records: the machine evidence of the edges an
    override consumes — their evidence struct and their classification
    fingerprint, which carries the route data a selector resolves against
    — canonicalised by (feed, place, tier), together with the targets the
    entry resolved to, and, for a selector resolved against a feed whose
    consumed rows carry no fingerprint, the feed's current route-evidence
    fingerprint: a wildcard that reaches a new place, an addition whose
    pair evidence moved, or route data that changed under unchanged
    metrics all read as stale. Curators copy it from the staleness report,
    which prints the current value next to the stale one.
    """
    rows = sorted(
        (
            edge["feed_id"],
            edge["place_id"],
            edge["tier"],
            _canonical(edge.get("evidence")),
            edge.get("classification_fingerprint"),
        )
        for edge in edges
    )
    payload = {
        "feed": feed_id,
        "targets": sorted(list(t) for t in targets),
        "evidence": rows,
        "route_evidence": list(route_evidence) if route_evidence else None,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _needs_review(edge):
    return edge["tier"] == "unknown" or edge["tier_confidence"] < REVIEW_CUTOFF


def _curation(entry, stale):
    return {
        "reason": entry.get("reason"),
        "author": entry.get("author"),
        "date": str(entry["date"]) if entry.get("date") is not None else None,
        "evidence_hash": entry.get("evidence_hash"),
        "stale": stale,
    }


def _specificity(entry):
    """Exact keys outrank wildcards: two exact keys beat one, one beats none."""
    return (entry["place"] != WILDCARD) + (entry.get("tier", WILDCARD) != WILDCARD)


def _declared_confidence(entry):
    """The confidence an entry declares, top-level or inside ``add_edge``
    (the loader refuses both at once), or None."""
    spec = entry.get("add_edge")
    if isinstance(spec, dict) and "tier_confidence" in spec:
        return float(spec["tier_confidence"])
    if "tier_confidence" in entry:
        return float(entry["tier_confidence"])
    return None


def _confidence(entry, tier):
    """The tier confidence a decision carries: the entry's, else 1.0 — and
    0.0 for ``unknown``, which no explicit value may contradict."""
    explicit = _declared_confidence(entry)
    if tier == "unknown":
        if explicit:
            raise CurateError(
                f"{_where(entry)}: an unknown tier has no confidence; "
                "tier_confidence must be 0"
            )
        return 0.0
    return 1.0 if explicit is None else explicit


def _where(entry):
    return f"edges.yaml {entry['feed']}/{entry['place']}/{entry.get('tier', '-')}"


class _Feed:
    """A feed's route evidence, read once from its crawl artifact: the routes
    (with agency and type), the stop coordinates, and — with complete
    stop_times — each route's served stops. ``sole_agency`` is the one
    agency of a single-agency feed, which GTFS lets routes leave blank."""

    def __init__(self, routes, coords, served, kind, sole_agency=None):
        self.routes = routes
        self.coords = coords
        self.served = served
        self.kind = kind
        self.sole_agency = sole_agency

    def expand(self, spec):
        """The route ids a curator predicate selects, ANDed across clauses.
        The effective agency is what the predicate matches; the fingerprint
        keeps the raw field, so build and fetch canonicalise alike."""
        chosen = []
        for route_id, info in sorted(self.routes.items()):
            agency = info["agency_id"] or self.sole_agency
            if "agency_id" in spec and agency not in spec["agency_id"]:
                continue
            if "route_type" in spec and info["route_type"] not in spec["route_type"]:
                continue
            chosen.append(route_id)
        return chosen

    def fingerprint(self):
        return fingerprint.compute(self.kind, self.routes, self.coords, self.served)


def _sole_agency(feed_dir, state):
    """The agency id of a single-agency feed, or None: with several agencies
    a blank route agency is a data error, never a guess."""
    try:
        with crawl.verified_member(feed_dir, state, "agency.txt") as opened:
            if opened is None:
                return None
            ids = {row.get("agency_id") or "" for row in classify._reader(opened)}
    except crawl.MEMBER_ERRORS:
        return None
    return next(iter(ids)) if len(ids) == 1 else None


def _read_feed(feed_dir, state):
    mode = (state.get("stop_times") or {}).get("state")
    names = ("trips.txt",) if mode == "complete" else ()
    parsed = classify._members(feed_dir, state, names)
    if parsed is None or not parsed["routes"]:
        return None
    sole = _sole_agency(feed_dir, state)
    if mode == "complete":
        return _Feed(
            parsed["routes"], parsed["coords"], parsed["stops"], "route_stops", sole
        )
    if mode == "skipped":
        return _Feed(parsed["routes"], parsed["coords"], None, "feed_stops", sole)
    return None


class _Curator:
    def __init__(self, edges, feeds, crawled, places, candidates):
        self.pairs = collections.defaultdict(dict)
        for edge in edges:
            self.pairs[(edge["feed_id"], edge["place_id"])][edge["tier"]] = edge
        # The machine's edges as classified: what the curator looked at is
        # the evidence BEFORE any override touched it. Whether a pair was
        # measured is the coverage stage's fact — a pair coverage admitted
        # but no route served has evidence and a service level even though
        # classification kept no edge for it.
        # Deep copies: the edges themselves are what the phases edit, and a
        # later phase's staleness must be judged against what the machine
        # produced, never against an earlier override's work.
        self.original = {
            (e["feed_id"], e["place_id"], e["tier"]): copy.deepcopy(e) for e in edges
        }
        self.classified = collections.defaultdict(dict)
        for (feed_id, place_id, tier), edge in self.original.items():
            self.classified[(feed_id, place_id)][tier] = edge
        self.measured = {
            (c["feed_id"], c["place_id"]): copy.deepcopy(c) for c in candidates
        }
        self.canonical = coverage._canonical_ids(feeds)
        self.crawled = crawled
        self.places = places
        self.evidence = {}
        self.stale = []
        self.counts = collections.Counter()

    # -- targets ---------------------------------------------------------

    def feed_id(self, ref):
        feed_id = self.canonical.get(ref)
        if feed_id is None:
            raise CurateError(f"override names a feed the index lacks: {ref!r}")
        return feed_id

    def targets(self, entry):
        """``(feed_id, [(place_id, tier)])`` the entry addresses in the
        current state; ``set_tiers`` targets carry the wildcard as tier.

        An exact place must exist in the gazetteer — an edge to a place
        nobody published would break the index's referential integrity —
        and, for anything but ``add_edge``, must hold edges of this feed: a
        correction aimed at nothing is the curator's error, never a silent
        no-op. A wildcard place means the pairs the operation fits: pairs
        holding the tier to change or remove, pairs lacking it to fill.
        """
        feed_id = self.feed_id(entry["feed"])
        operation = entry["operation"]
        if entry["place"] == WILDCARD:
            served = {p for f, p in self.pairs if f == feed_id and self.pairs[(f, p)]}
            if operation == "add_edge":
                # Everywhere the feed serves includes the pairs coverage
                # measured but classification kept no edge for.
                served |= {p for f, p in self.measured if f == feed_id}
            place_ids = sorted(served)
        elif entry["place"] not in self.places:
            raise CurateError(f"{_where(entry)}: a place the gazetteer lacks")
        elif operation != "add_edge" and not self.pairs[(feed_id, entry["place"])]:
            raise CurateError(f"{_where(entry)}: no edges for this pair; use add_edge")
        else:
            place_ids = [entry["place"]]
        targets = []
        for place_id in place_ids:
            tiers = self.pairs[(feed_id, place_id)]
            if operation == "set_tiers":
                targets.append((place_id, WILDCARD))
            elif entry["tier"] == WILDCARD:
                targets.extend((place_id, t) for t in sorted(tiers))
            elif entry["place"] == WILDCARD:
                if (entry["tier"] in tiers) != (operation == "add_edge"):
                    targets.append((place_id, entry["tier"]))
            else:
                targets.append((place_id, entry["tier"]))
        return feed_id, targets

    def route_evidence(self, feed_id):
        if feed_id not in self.evidence:
            located = self.crawled.get(feed_id)
            self.evidence[feed_id] = (
                _read_feed(*located) if located is not None else None
            )
        return self.evidence[feed_id]

    # -- staleness -------------------------------------------------------

    def judge(self, entry, feed_id, targets):
        """Whether the entry is stale: one hash over the machine evidence of
        everything it targets, judged before anything changes; one report
        row per entry. An entry left with nothing to target is judged
        against the empty set — vanished evidence is stale evidence."""
        recorded = entry.get("evidence_hash")
        if recorded is None:
            return False
        originals = []
        for place_id, tier in targets:
            # A pair-scoped decision, and an addition — which consumes the
            # pair's evidence rather than an edge's — hash the whole pair.
            if tier == WILDCARD or entry["operation"] == "add_edge":
                pair = [
                    edge
                    for (f, p, _), edge in self.original.items()
                    if f == feed_id and p == place_id
                ]
                if not pair and (feed_id, place_id) in self.measured:
                    # Classification kept no edge: what a new edge consumes
                    # is the coverage candidate, so that is what can move.
                    pair = [self.measured[(feed_id, place_id)]]
                originals.extend(pair)
            elif (feed_id, place_id, tier) in self.original:
                originals.append(self.original[(feed_id, place_id, tier)])
        route_evidence = None
        if entry["operation"] in ("set_selector", "add_edge") and (
            not originals
            or not all(e.get("classification_fingerprint") for e in originals)
        ):
            # The selector resolves against the crawl's route evidence; when
            # the consumed rows carry no fingerprint of it, hash it directly.
            evidence = self.route_evidence(feed_id)
            if evidence is not None:
                route_evidence = (evidence.kind, evidence.fingerprint())
        current = evidence_hash(originals, targets, feed_id, route_evidence)
        if current == recorded:
            return False
        self.stale.append(
            {
                "feed": entry["feed"],
                "place": entry["place"],
                "tier": entry.get("tier"),
                "operation": entry["operation"],
                "recorded_evidence_hash": recorded,
                "current_evidence_hash": current,
            }
        )
        return True

    # -- selectors and edges ---------------------------------------------

    def selector(self, feed_id, spec, entry):
        """``(selector_state, selector)`` for a curator's selector spec. The
        whole-feed invariant is a property of the final pair, checked once
        every phase has run."""
        evidence = self.route_evidence(feed_id)
        if evidence is None:
            # No route evidence: neither a predicate nobody can resolve nor
            # a whole-feed claim nobody can validate ever ships.
            return "unavailable", None
        if spec == "whole_feed":
            return "whole_feed", None
        if "route_id" in spec:
            unknown = sorted(set(spec["route_id"]) - set(evidence.routes))
            if unknown:
                raise CurateError(
                    f"{_where(entry)}: route ids the feed lacks: {unknown}"
                )
            route_ids = sorted(set(spec["route_id"]))
            declared_as = None
        else:
            route_ids = evidence.expand(spec)
            declared_as = {k: spec[k] for k in ("agency_id", "route_type") if k in spec}
        if not route_ids:
            raise CurateError(f"{_where(entry)}: the selector matches no route")
        selector = {"route_id": route_ids}
        if declared_as:
            selector["declared_as"] = declared_as
        return "complete", selector

    def new_edge(self, feed_id, place_id, tier, entry):
        classified = self.classified.get((feed_id, place_id)) or {}
        # The pair's service and coverage facts: the classify stage's struct
        # (identical on every tier edge) when classification kept the pair,
        # the coverage candidate's when no route served it.
        template = next(iter(classified.values()), None) or self.measured.get(
            (feed_id, place_id)
        )
        reason = entry.get("reason")
        if template is None and not (isinstance(reason, str) and reason.strip()):
            raise CurateError(
                f"{_where(entry)}: no coverage evidence for this pair; an edge "
                "asserting service nobody measured needs a reason"
            )
        # The machine's full context travels with the human decision: the
        # pair's coverage facts, and each classified tier's own evidence.
        evidence = {"curator_reason": reason}
        if template is not None:
            evidence.update(
                {
                    k: template["evidence"][k]
                    for k in PAIR_EVIDENCE
                    if k in (template.get("evidence") or {})
                }
            )
            evidence["classified"] = {
                t: dict(e.get("evidence") or {}) for t, e in sorted(classified.items())
            }
        route_evidence = self.route_evidence(feed_id)
        return {
            "place_id": place_id,
            "feed_id": feed_id,
            "tier": tier,
            "service": template.get("service") if template else None,
            "tier_confidence": _confidence(entry, tier),
            "method": "human",
            "rehomed_from": [],
            "evidence": evidence,
            "curation": None,
            "merged_evidence": [],
            "curation_history": [],
            "classification_fingerprint": (
                route_evidence.fingerprint() if route_evidence else None
            ),
            "fingerprint_kind": route_evidence.kind if route_evidence else "none",
            "selector_state": "unavailable",
            "selector": None,
            "needs_review": True,
        }

    def stamp(self, edge, entry, stale):
        # The latest operation's stamp, but staleness is sticky: an edge a
        # stale decision shaped stays flagged whatever touches it later.
        stale = stale or bool((edge.get("curation") or {}).get("stale"))
        edge["method"] = "human"
        edge["curation"] = _curation(entry, stale)
        edge["needs_review"] = _needs_review(edge)

    # -- operations ------------------------------------------------------

    def set_tiers(self, feed_id, place_id, _tier, entry, stale):
        pair = self.pairs[(feed_id, place_id)]
        if not pair:
            raise CurateError(f"{_where(entry)}: no edges for this pair; use add_edge")
        wanted = set(entry["set_tiers"])
        if "unknown" in wanted:
            # Retained or new, an unknown tier has no confidence to take.
            _confidence(entry, "unknown")
        for tier in sorted(set(pair) - wanted):
            del pair[tier]
            self.counts["edges_removed"] += 1
        for tier in sorted(wanted - set(pair)):
            pair[tier] = self.new_edge(feed_id, place_id, tier, entry)
            self.counts["edges_added"] += 1
        # The whole pair is the curator's decision now: retained edges keep
        # their selector, service, evidence and confidence and take the stamp.
        for edge in pair.values():
            self.stamp(edge, entry, stale)
        self.counts["pairs_retiered"] += 1

    def add_edge(self, feed_id, place_id, tier, entry, stale):
        pair = self.pairs[(feed_id, place_id)]
        if tier in pair:
            raise CurateError(
                f"{_where(entry)}: the edge exists; use set_selector or set_tiers"
            )
        spec = entry["add_edge"] if isinstance(entry["add_edge"], dict) else {}
        if "selector" not in spec and self.route_evidence(feed_id) is not None:
            # A crawled feed's routes are known: an edge added to it says
            # which of them it means, or it is not an edge the reader can
            # trust. Only a feed without route evidence falls to unavailable.
            raise CurateError(
                f"{_where(entry)}: this feed has route evidence; add_edge needs "
                "a selector"
            )
        edge = self.new_edge(feed_id, place_id, tier, entry)
        if "selector" in spec:
            edge["selector_state"], edge["selector"] = self.selector(
                feed_id, spec["selector"], entry
            )
        self.stamp(edge, entry, stale)
        pair[tier] = edge
        self.counts["edges_added"] += 1

    def set_selector(self, feed_id, place_id, tier, entry, stale):
        edge = self.pairs[(feed_id, place_id)].get(tier)
        if edge is None:
            raise CurateError(f"{_where(entry)}: no such edge to set a selector on")
        edge["selector_state"], edge["selector"] = self.selector(
            feed_id, entry["set_selector"], entry
        )
        if edge["selector_state"] != "unavailable":
            # A trusted selector is validated at fetch time against the
            # fingerprint of the very evidence it was built from — an edge
            # classified without route evidence has none until now.
            evidence = self.route_evidence(feed_id)
            edge["classification_fingerprint"] = evidence.fingerprint()
            edge["fingerprint_kind"] = evidence.kind
        self.stamp(edge, entry, stale)
        self.counts["selectors_set"] += 1

    def remove_edge(self, feed_id, place_id, tier, entry, _stale):
        pair = self.pairs[(feed_id, place_id)]
        if tier not in pair:
            raise CurateError(f"{_where(entry)}: no such edge to remove")
        del pair[tier]
        self.counts["edges_removed"] += 1

    def apply(self, entries):
        """One phase: resolve every entry's targets and claims against the
        current state, then apply each entry, in file order, to the targets
        it holds. Returns how many entries applied to anything."""
        claims = {}
        resolved = []
        for entry in entries:
            feed_id, targets = self.targets(entry)
            resolved.append((entry, feed_id, targets))
            rank = _specificity(entry)
            for place_id, tier in targets:
                key = (feed_id, place_id, tier)
                held = claims.get(key)
                if held is not None and held[0] == rank:
                    raise CurateError(
                        f"{_where(entry)} and {_where(held[1])} both aim at "
                        f"{place_id}/{tier} at the same specificity"
                    )
                if held is None or held[0] < rank:
                    claims[key] = (rank, entry)
        applied = 0
        for entry, feed_id, targets in resolved:
            mine = [t for t in targets if claims[(feed_id, *t)][1] is entry]
            stale = self.judge(entry, feed_id, mine)
            if not mine:
                continue
            operation = getattr(self, entry["operation"])
            for place_id, tier in mine:
                operation(feed_id, place_id, tier, entry, stale)
            applied += 1
        return applied

    def check_pairs(self):
        """The whole-feed invariant over the final state, whatever order the
        operations ran in: a claim that every route qualifies for a tier is
        the pair's only tier — no route can then be another tier's, filtered
        or not."""
        for (feed_id, place_id), pair in self.pairs.items():
            whole = sorted(
                t for t, e in pair.items() if e["selector_state"] == "whole_feed"
            )
            if whole and len(pair) > 1:
                raise CurateError(
                    f"{feed_id}/{place_id}: whole_feed on {whole} claims every "
                    f"route qualifies, but the pair also carries "
                    f"{sorted(set(pair) - set(whole))}"
                )

    # -- GTFS-RT propagation ---------------------------------------------

    def propagate(self, feeds):
        """GTFS-RT feeds inherit the place membership of their static feed —
        not its tiers — from the static edges AS CURATED: one ``unknown``
        edge per place the static feed serves, carrying the pair's service
        level and naming the link it came along. Run after the static-side
        overrides and before the RT-side ones, so a curator's change to the
        static feed reaches its companion and a curator can still correct an
        inherited edge without this pass overwriting it. Returns how many
        feeds and edges were propagated."""
        propagated = 0
        inherited = 0
        for feed in feeds:
            static_id = feed.get("static_feed_id")
            if feed.get("spec") != "gtfs-rt" or not static_id:
                continue
            rt_id = feed["feed_id"]
            for key in [k for k in self.pairs if k[0] == rt_id]:
                del self.pairs[key]
            for (f, place_id), pair in list(self.pairs.items()):
                if f != static_id or not pair:
                    continue
                template = next(iter(pair.values()))
                edge = {
                    "place_id": place_id,
                    "feed_id": rt_id,
                    "tier": "unknown",
                    "service": template.get("service"),
                    "tier_confidence": 0.0,
                    "method": "inferred",
                    "rehomed_from": [],
                    "evidence": {
                        "inherited_from": static_id,
                        "static_link_method": feed.get("static_link_method"),
                        "inherited_tiers": sorted(pair),
                    },
                    "curation": None,
                    "merged_evidence": [],
                    "curation_history": [],
                    "classification_fingerprint": None,
                    "fingerprint_kind": "none",
                    "selector_state": "unavailable",
                    "selector": None,
                    "needs_review": True,
                }
                self.pairs[(rt_id, place_id)]["unknown"] = edge
                # The inherited edge is the machine's state for the RT feed:
                # what an RT-side override is judged against.
                self.original[(rt_id, place_id, "unknown")] = copy.deepcopy(edge)
                self.classified[(rt_id, place_id)]["unknown"] = self.original[
                    (rt_id, place_id, "unknown")
                ]
                inherited += 1
            propagated += 1
        return propagated, inherited

    def edges(self):
        return sorted(
            (edge for pair in self.pairs.values() for edge in pair.values()),
            key=lambda e: (e["place_id"], e["feed_id"], e["tier"]),
        )


def curate(cache_dir, *, overrides_dir=None, strict=False):
    """Apply the edge overrides; publish the ``curate`` generation. Returns
    the manifest. With ``strict``, a stale override fails the stage after
    the staleness report is written."""
    entries, digest = overrides.load_edge_overrides(overrides_dir)
    # Every upstream stage's writer lock is held from the lineage checks
    # through the commit, in the order publish takes them, then this
    # stage's own, then the crawl's: no parent pointer can move in between,
    # so a generation published here is never stale the moment it lands.
    with contextlib.ExitStack() as stack:
        for subdir in ("resolve", "gazetteer", "coverage", "classify"):
            # Created when absent, so a stage that has not run yet cannot
            # slip its first publication in between: the lock exists first.
            held = store.open_subdir(cache_dir, subdir)
            stack.callback(held.close)
            stack.enter_context(store.exclusive_writer(held))
        directory = store.open_subdir(cache_dir, "curate")
        stack.callback(directory.close)
        stack.enter_context(store.exclusive_writer(directory))
        stack.enter_context(crawl.reading(cache_dir))
        feeds, edges, classified = classify.read_edges(
            cache_dir, locked=True, final=False
        )
        if classified is None or classified.get("source") != "classify":
            raise CurateError("no classify generation to curate; run classify")
        place_rows, expanded = store.read_jsonl(
            cache_dir / "gazetteer", "expanded.json", "places_expanded.jsonl"
        )
        if classified.get("expanded_generation") != expanded.get("generation"):
            raise CurateError(
                "the classified edges were not derived from the current "
                "expanded places; re-run the pipeline in stage order"
            )
        places = {place["place_id"]: place for place in place_rows}
        # The coverage candidates the classified edges descend from: the
        # pairs coverage measured, whether or not a route served them.
        covered, covered_manifest = store.resolve(
            cache_dir / "coverage", coverage.COVERAGE_POINTER
        )
        with covered:
            if covered_manifest.get("generation") != classified.get(
                "coverage_generation"
            ):
                raise CurateError(
                    "the classified edges were not derived from the current "
                    "coverage generation; re-run the pipeline in stage order"
                )
            candidates = store.parse_jsonl(covered.read_bytes(coverage.EDGES_ARTIFACT))
        canonical = coverage._canonical_ids(feeds)
        crawled = {}
        for feed_dir, state in crawl.crawled_feeds(cache_dir):
            state_id = state.get("feed_id")
            feed_id = canonical.get(state_id) if isinstance(state_id, str) else None
            if feed_id is not None:
                crawled[feed_id] = (feed_dir, state)
        curator = _Curator(edges, feeds, crawled, places, candidates)
        # Static-side overrides first, then the GTFS-RT propagation along
        # the inferred static link, then the overrides aimed at RT feeds.
        rt_ids = {f["feed_id"] for f in feeds if f.get("spec") == "gtfs-rt"}
        static_entries = [
            e for e in entries if curator.feed_id(e["feed"]) not in rt_ids
        ]
        rt_entries = [e for e in entries if curator.feed_id(e["feed"]) in rt_ids]
        applied = 0
        for phase in PHASES:
            applied += curator.apply(
                [e for e in static_entries if e["operation"] == phase]
            )
        rt_feeds, rt_edges = curator.propagate(feeds)
        for phase in PHASES:
            applied += curator.apply([e for e in rt_entries if e["operation"] == phase])
        curator.check_pairs()
        final = curator.edges()
        _write_report(cache_dir, curator.stale)
        if strict and curator.stale:
            raise CurateError(
                f"{len(curator.stale)} stale override(s); see {REPORT_FILE}"
            )
        by_tier = collections.Counter(e["tier"] for e in final)
        manifest = {
            "source": "curate",
            "mode": classified.get("mode"),
            "sources": classified.get("sources"),
            "overture_release": classified.get("overture_release"),
            "classify_generation": classified.get("generation"),
            "coverage_generation": classified.get("coverage_generation"),
            "expanded_generation": classified.get("expanded_generation"),
            # The digest of the very bytes the entries were parsed from:
            # publish refuses these edges once the file has been edited.
            "overrides_sha256": digest,
            "feeds": len(feeds),
            "overrides": len(entries),
            "overrides_applied": applied,
            "stale_overrides": len(curator.stale),
            "strict": strict,
            "rt_feeds_propagated": rt_feeds,
            "rt_edges_inherited": rt_edges,
            **dict(curator.counts),
            "edges": len(final),
            "edges_by_tier": dict(by_tier),
            "unknown_share": (by_tier["unknown"] / len(final)) if final else 0.0,
            "needs_review": sum(1 for e in final if e["needs_review"]),
            "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        return store.publish(
            cache_dir / "curate",
            CURATE_POINTER,
            {
                FEEDS_ARTIFACT: store.jsonl_chunks(feeds),
                EDGES_ARTIFACT: store.jsonl_chunks(final),
            },
            manifest,
            held=directory,
        )


def _write_report(cache_dir, rows):
    """The staleness report for the curator, rewritten every run — an empty
    report is the good news, and it is written too."""
    root = store.open_directory(cache_dir)
    try:
        store.write_file(
            root, REPORT_FILE, lambda: (json.dumps(row) + "\n" for row in rows)
        )
    finally:
        root.close()
