"""Coverage stage: membership edges from crawled stops, else declared locations.

Which places does a feed serve? For a crawled feed the answer is measured: its
digest-verified stops resolve through the boundary lookup, and every place
holding at least ``MIN_STOPS`` stops and ``MIN_STOP_SHARE`` of the feed's stops
(or half the feed's stops regardless of count) gets an edge, at the plan's
formula confidence with the counts and thresholds recorded as evidence.
Ancestor edges come free — a stop inside a city is inside its region and
country polygons too. Metro edges are propagated from member-city edges,
because minted metros carry no geometry yet (the merged metros stage's own
convention); when metro polygons exist, PIP takes over with no schema change.
Crawled evidence supersedes the declared placements feed by feed.

For every other feed the answer comes from
what the catalogues declare: the seed stage resolved each feed's declared
municipality, subdivision or country to a gazetteer place, and this stage turns
those placements into candidate edges — one ``(place, feed)`` row each, at the
plan's flat declared confidences (always below the review cutoff), with
``tier = "unknown"`` and no selector, since nothing measured which routes or
stops are involved.

Declared edges propagate explicitly. A crawled feed would reach its city's
ancestors and metros for free, since its stops fall inside every enclosing
polygon; a declared feed has no geometry to test, so an edge to a city also
yields edges to the city's administrative ancestors and to every metro in its
``metro_ids``, at the same confidence. Without that a declared-only feed would be
invisible to the bare-name query, which promotes to the default metro.

A GTFS-RT feed linked to a static feed gets no edges here: it inherits the static
feed's membership in the edge-override stage, after curation, so a curated change
to the static feed reaches its companion. An unlinked GTFS-RT feed falls back to
declared coverage like any uncrawlable feed. Crawled evidence supersedes all of
this feed by feed once the crawl exists.
"""

import collections
import datetime

from index_build import store

COVERAGE_POINTER = "coverage.json"
FEEDS_ARTIFACT = "feeds_covered.jsonl"
EDGES_ARTIFACT = "edges_candidate.jsonl"


class CoverageError(RuntimeError):
    """The coverage inputs do not describe one consistent build."""


def _check_lineage(resolve_manifest, seed_manifest, expanded_manifest):
    """Refuse a mixed snapshot: the three inputs move independently.

    The resolved feeds and the seed placements must descend from the same
    crosswalk (same catalogue ``sources``), and the placements and the expanded
    places from the same Overture release — otherwise a partial rerun or a
    concurrent republish would silently mix builds.
    """
    if resolve_manifest.get("sources") != seed_manifest.get("sources"):
        raise CoverageError(
            "resolved feeds and seed placements come from different "
            "catalogue versions; re-run the pipeline in stage order"
        )
    if seed_manifest.get("overture_release") != expanded_manifest.get(
        "overture_release"
    ):
        raise CoverageError(
            "seed placements and expanded places come from different "
            "Overture releases; re-run the pipeline in stage order"
        )
    if expanded_manifest.get("sources") != seed_manifest.get("sources"):
        raise CoverageError(
            "expanded places do not descend from the seed placements' "
            "catalogue snapshot; re-run the pipeline in stage order"
        )
    # Exact ancestry, not labels: the expanded places must carry the very
    # seed generation these placements came from.
    if expanded_manifest.get("seed_generation") != seed_manifest.get("generation"):
        raise CoverageError(
            "expanded places do not descend from the current seed "
            "placements; re-run the pipeline in stage order"
        )


# Flat declared membership confidences, all below the 0.70 review cutoff: an
# exact municipality match, a coarser subdivision/country or bounding-box
# match, or a >=4-character geohash.
DECLARED_CONFIDENCE = {
    "municipality": 0.50,
    "subdivision": 0.35,
    "country": 0.35,
    "bbox": 0.35,
    "geohash": 0.25,
}
REVIEW_CUTOFF = 0.70

# The crawled admission thresholds: a place needs this many stops AND this
# share of the feed's stops — or half the feed's stops regardless of count.
MIN_STOPS = 5
MIN_STOP_SHARE = 0.02
FULL_SHARE = 0.5


def _edge(place_id, feed_id, confidence, evidence, method):
    return {
        "place_id": place_id,
        "feed_id": feed_id,
        "tier": "unknown",
        "confidence": confidence,
        "tier_confidence": 0.0,
        "method": method,
        "rehomed_from": [],
        "evidence": evidence,
        "curation": None,
        "merged_evidence": [],
        "curation_history": [],
        "classification_fingerprint": None,
        "fingerprint_kind": "none",
        "selector_state": "unavailable",
        "selector": None,
        "needs_review": confidence < REVIEW_CUTOFF,
    }


def _reach(place_id, places):
    """The place, its administrative ancestors, and its metros — by id."""
    reached = []
    seen = set()
    current = place_id
    while current and current not in seen and current in places:
        seen.add(current)
        reached.append(current)
        current = places[current].get("parent_id")
    for metro_id in places[place_id].get("metro_ids") or []:
        if metro_id in places and metro_id not in seen:
            seen.add(metro_id)
            reached.append(metro_id)
    return reached


def _canonical_ids(feeds):
    """Every resolved feed id and alias, mapped to the feed's canonical id.

    Placements were recorded against the crosswalk ids; an identity override may
    have renamed a feed since, keeping the old id in its aliases. The resolve
    stage guarantees this namespace is unique.
    """
    lookup = {}
    for feed in feeds:
        for key in [feed["feed_id"], *(feed.get("aliases") or [])]:
            lookup[key] = feed["feed_id"]
    return lookup


def link_static_feeds(feeds, canonical):
    """Canonicalise each GTFS-RT feed's ``static_feed_id``; return the linked ids.

    The link was recorded against crosswalk ids, so it is rewritten to the
    static feed's canonical id (a rename keeps the old one as an alias). A link
    naming no resolved feed — or a target that is not a static GTFS feed (a
    self, GTFS-RT or GBFS reference) — is dangling: the RT feed is left
    unlinked, so it falls back to declared coverage rather than waiting on an
    inheritance that could never happen, and the bad id is recorded on the feed.
    """
    by_id = {feed["feed_id"]: feed for feed in feeds}
    linked = set()
    for feed in feeds:
        if feed.get("spec") != "gtfs-rt" or not feed.get("static_feed_id"):
            continue
        static_id = canonical.get(feed["static_feed_id"])
        target = by_id.get(static_id) if static_id else None
        if (
            target is None
            or target.get("spec") != "gtfs"
            or static_id == feed["feed_id"]
        ):
            feed["dangling_static_feed_id"] = feed["static_feed_id"]
            feed["static_feed_id"] = None
            # The row is now explicitly unlinked; the method must say so too.
            feed["static_link_method"] = "none"
            continue
        feed["static_feed_id"] = static_id
        linked.add(feed["feed_id"])
    return linked


def declared_edges(feeds, places, placements, *, superseded=frozenset()):
    """Candidate edges for the placements, and what could not be placed.

    Returns ``(edges, unknown_place_ids, unmatched_feed_ids)``: one edge per
    reached ``(place, feed)`` pair, keeping the higher confidence when a feed
    reaches a place twice; the placement place ids absent from the expanded
    places; and the placement feed ids that match no resolved feed. A linked
    GTFS-RT feed's placements are skipped — it inherits later.
    """
    canonical = _canonical_ids(feeds)
    linked = link_static_feeds(feeds, canonical)
    by_key = {}
    unknown_places = set()
    unmatched_feeds = set()
    unknown_levels = set()
    for placement in placements:
        feed_id = canonical.get(placement["feed_id"])
        if feed_id is None:
            unmatched_feeds.add(placement["feed_id"])
            continue
        if feed_id in linked or feed_id in superseded:
            continue
        if placement["place_id"] not in places:
            unknown_places.add(placement["place_id"])
            continue
        confidence = DECLARED_CONFIDENCE.get(placement["level"])
        if confidence is None:
            # A level this stage does not know is reported, never guessed at.
            unknown_levels.add(str(placement["level"]))
            continue
        evidence = {
            "declared_level": placement["level"],
            "declared_place_id": placement["place_id"],
        }
        for place_id in _reach(placement["place_id"], places):
            key = (place_id, feed_id)
            if key not in by_key or by_key[key]["confidence"] < confidence:
                by_key[key] = _edge(place_id, feed_id, confidence, evidence, "inferred")
    edges = [by_key[key] for key in sorted(by_key)]
    return (
        edges,
        sorted(unknown_places),
        sorted(unmatched_feeds),
        sorted(unknown_levels),
    )


def shifted_frame(points):
    """The points in a +360-shifted longitude frame when that is the
    narrower reading, else None.

    The frame is chosen by the largest circular longitude gap: rotating
    that gap out of the frame makes a Pacific feed's stops contiguous,
    while a feed genuinely spanning most longitudes keeps the plain one.
    """
    xs = sorted({x for x, _ in points})
    width = xs[-1] - xs[0]
    if width <= 180.0:
        return None
    gaps = [(xs[i + 1] - xs[i], xs[i + 1]) for i in range(len(xs) - 1)]
    gaps.append((xs[0] + 360.0 - xs[-1], xs[0]))
    gap, cut = max(gaps)
    if 360.0 - gap >= width:
        return None
    return [(x + 360.0 if x < cut else x, y) for x, y in points]


def _stop_hull(points):
    """The convex hull of the stops, honest across the antimeridian: built
    in the shifted frame when one is narrower and split back at the
    dateline into a two-part geometry."""
    import shapely

    shifted = shifted_frame(points)
    if shifted is None:
        return shapely.convex_hull(shapely.MultiPoint(points))
    hull = shapely.convex_hull(shapely.MultiPoint(shifted))
    east = shapely.intersection(hull, shapely.box(-180.0, -90.0, 180.0, 90.0))
    west = shapely.transform(
        shapely.intersection(hull, shapely.box(180.0, -90.0, 540.0, 90.0)),
        lambda coords: coords - [360.0, 0.0],
    )
    return shapely.union(east, west)


def place_index(places):
    """``{overture_id: place_id}``: how division hits map onto places.

    Overture id first — a P402-resolved place has no wikidata on the division
    record — with the QID as the fallback in :func:`stop_places`.
    """
    return {
        place.get("overture_id"): place_id
        for place_id, place in places.items()
        if place.get("overture_id")
    }


def stop_places(lookup, x, y, places, by_overture):
    """``(place_ids, countries, stale)`` for one stop coordinate.

    ``stale`` holds QID-bearing divisions the gazetteer does not know, which
    can only mean ``places_expanded`` predates the crawl.
    """
    from index_build import overture

    hit = set()
    countries = set()
    stale = set()
    for record in lookup.divisions_at(x, y):
        if record.get("country"):
            countries.add(record["country"])
        place_id = by_overture.get(record.get("overture_id"))
        if place_id is None and record.get("wikidata") in places:
            place_id = record["wikidata"]
        if place_id is not None:
            hit.add(place_id)
            continue
        qid = record.get("wikidata")
        if record.get("kind") and qid and overture.QID_PATTERN.match(qid):
            stale.add(qid)
    return hit, countries, stale


def _crawled_confidence(stops_in_place, stop_share):
    """The plan's membership formula: 0.6 at the admission threshold, 1.0 at a
    tenth of the stops or fifty of them."""
    return 0.6 + 0.4 * min(1.0, max(stop_share / 0.10, stops_in_place / 50))


def crawled_edges(
    cache_dir,
    feeds,
    places,
    lookup,
    *,
    min_stops=MIN_STOPS,
    min_stop_share=MIN_STOP_SHARE,
):
    """Measured edges for the crawled feeds; ``(edges_by_key, report)``.

    Reads each crawled feed's digest-verified stops (the crawl's state.json is
    the commit point, exactly as expansion reads them), resolves every stop
    through the boundary lookup, and admits ``(place, feed)`` pairs by the
    thresholds. A feed whose stops were read supersedes its declared
    placements even when nothing passes — the crawl saw where it stops. Metro
    edges are propagated from passing member-city edges at the same
    confidence.
    """
    # Heavy deps (shapely/pyarrow via the gazetteer modules) load only when
    # crawl artifacts exist; the declared path stays importable without them.
    import shapely

    from index_build import crawl, expand

    canonical = _canonical_ids(feeds)
    by_overture = place_index(places)
    # Minted metros have no geometry: a stop is inside a metro when it is
    # inside ANY member city — counted once however many members it hits.
    metro_members = {
        place_id: set(place.get("member_ids") or [])
        for place_id, place in places.items()
        if place.get("kind") == "metro" and place.get("member_ids")
    }
    by_key = {}
    superseded = set()
    crawl_fields = {}
    stale = set()
    mismatches = 0
    unmatched = set()
    for feed_dir, state in crawl.crawled_feeds(cache_dir):
        state_id = state.get("feed_id")
        feed_id = canonical.get(state_id) if isinstance(state_id, str) else None
        if feed_id is None:
            unmatched.add(str(state_id))
            continue
        read = expand._stop_points(feed_dir, state)
        if read is None:
            mismatches += 1
            crawl_fields[feed_id] = {"crawl_status": "state_mismatch"}
            continue
        points, dropped = read
        superseded.add(feed_id)
        # The schema's crawl bookkeeping, from the state that verified the
        # evidence: measured hull, stop count, validators, retrieval time.
        # A digest-valid file whose rows are all unparsable is still the
        # crawl's answer: it supersedes, counts its rows, and places nothing.
        crawl_fields[feed_id] = {
            "stop_count": len(points) + dropped,
            "coverage": (shapely.to_wkb(_stop_hull(points)).hex() if points else None),
            "etag": state.get("etag"),
            "last_modified": state.get("last_modified"),
            "last_crawled": state.get("retrieved_at"),
            "crawl_status": "ok",
        }
        if not points:
            continue
        lookup.ensure(crawl.cluster_boxes(points))
        counts = collections.Counter()
        for x, y in points:
            hit, _, stale_here = stop_places(lookup, x, y, places, by_overture)
            stale.update(stale_here)
            counts.update(hit)
            for metro_id, members in metro_members.items():
                if hit & members:
                    counts[metro_id] += 1
        # Dropped rows stay in the denominator: a stop whose coordinates do
        # not parse is still one of the feed's stops, and excluding it would
        # let a mostly-corrupt file inflate a share to false confidence.
        total = len(points) + dropped
        passing = {}
        for place_id, stops_in_place in counts.items():
            share = stops_in_place / total
            if share >= FULL_SHARE or (
                stops_in_place >= min_stops and share >= min_stop_share
            ):
                passing[place_id] = (stops_in_place, share)
        for place_id, (stops_in_place, share) in passing.items():
            evidence = {
                "stops_in_place": stops_in_place,
                "stop_share": share,
                "min_stops": min_stops,
                "min_stop_share": min_stop_share,
                "review_cutoff": REVIEW_CUTOFF,
            }
            confidence = _crawled_confidence(stops_in_place, share)
            targets = [place_id]
            if places[place_id].get("kind") == "city":
                # Minted metros have no geometry to test yet; membership
                # propagates from the member city, like the declared path.
                targets.extend(
                    metro_id
                    for metro_id in places[place_id].get("metro_ids") or []
                    if metro_id in places
                )
            for target in targets:
                key = (target, feed_id)
                current = by_key.get(key)
                # A place's own evidence outranks propagation at equal
                # confidence, so a metro reports its aggregated stops.
                own = target == place_id
                if (
                    current is None
                    or current["confidence"] < confidence
                    or (own and current["confidence"] <= confidence)
                ):
                    by_key[key] = _edge(target, feed_id, confidence, evidence, "crawl")
    if stale:
        raise CoverageError(
            "crawled stops hit QID-bearing divisions the gazetteer does not "
            f"know ({', '.join(sorted(stale)[:5])}); places_expanded predates "
            "the crawl — re-run the expand stage"
        )
    report = {
        "superseded": superseded,
        "state_mismatches": mismatches,
        "unmatched_crawl_ids": sorted(unmatched),
        "crawl_fields": crawl_fields,
    }
    return by_key, report


def cover(
    cache_dir, *, lookup=None, min_stops=MIN_STOPS, min_stop_share=MIN_STOP_SHARE
):
    """Derive declared membership edges; publish the ``coverage`` generation.

    Reads the resolved feeds, the expanded places and the seed placements, and
    writes ``feeds_covered.jsonl`` (every feed stamped with its
    ``coverage_source``) and ``edges_candidate.jsonl``. Returns the manifest.
    """
    directory = store.open_subdir(cache_dir, "coverage")
    try:
        with store.exclusive_writer(directory):
            feeds, resolve_manifest = store.read_jsonl(
                cache_dir / "resolve", "feeds_resolved.json", "feeds_resolved.jsonl"
            )
            place_rows, expanded_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "expanded.json", "places_expanded.jsonl"
            )
            placements, seed_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "seed.json", "feed_places.jsonl"
            )
            _check_lineage(resolve_manifest, seed_manifest, expanded_manifest)
            from index_build import crawl

            places = {place["place_id"]: place for place in place_rows}

            crawled_by_key = {}
            crawl_report = {
                "superseded": set(),
                "state_mismatches": 0,
                "unmatched_crawl_ids": [],
                "crawl_fields": {},
            }
            opened_lookup = None
            crawl_digest = None
            crawl_lock = crawl.reading(cache_dir)
            crawl_lock.__enter__()
            try:
                # Taken under the lock: the crawl these edges are measured
                # against, which the expanded places must also descend from.
                crawl_digest = crawl.states_digest(cache_dir)
                if expanded_manifest.get("crawl_digest") != crawl_digest:
                    raise CoverageError(
                        "the crawl changed since the expand stage read it; "
                        "re-run the expand stage"
                    )
                if (cache_dir / "crawl" / "crawl_log.jsonl").is_file():
                    if lookup is None:
                        from index_build import boundaries

                        opened_lookup = boundaries.BoundaryLookup(
                            cache_dir,
                            release=expanded_manifest.get("overture_release"),
                        )
                        lookup = opened_lookup
                    try:
                        crawled_by_key, crawl_report = crawled_edges(
                            cache_dir,
                            feeds,
                            places,
                            lookup,
                            min_stops=min_stops,
                            min_stop_share=min_stop_share,
                        )
                    except store.StoreError as error:
                        raise CoverageError(
                            "the boundary lookup cannot answer the crawled "
                            "stops; run the expand stage first"
                        ) from error
            finally:
                crawl_lock.__exit__(None, None, None)
                if opened_lookup is not None:
                    opened_lookup.close()

            edges, unknown_places, unmatched_feeds, unknown_levels = declared_edges(
                feeds, places, placements, superseded=crawl_report["superseded"]
            )
            merged = dict(crawled_by_key)
            for edge in edges:
                merged[(edge["place_id"], edge["feed_id"])] = edge
            edges = [merged[key] for key in sorted(merged)]

            declared_covered = {
                edge["feed_id"]
                for edge in edges
                if edge["feed_id"] not in crawl_report["superseded"]
            }
            for feed in feeds:
                if feed["feed_id"] in crawl_report["superseded"]:
                    # The schema's crawl fields: measured hull and stop count
                    # replace whatever the catalogues declared.
                    feed["coverage_source"] = "crawl"
                    feed.update(crawl_report["crawl_fields"][feed["feed_id"]])
                elif feed["feed_id"] in declared_covered:
                    feed["coverage_source"] = "declared"
                    feed.update(crawl_report["crawl_fields"].get(feed["feed_id"]) or {})
                else:
                    feed["coverage_source"] = None
            covered = {
                feed["feed_id"] for feed in feeds if feed["coverage_source"] is not None
            }

            manifest = {
                "source": "coverage",
                "mode": "crawled" if crawl_report["superseded"] else "declared",
                # The exact input generations, so later stages can prove
                # their inputs are the ones these edges were derived from.
                "resolve_generation": resolve_manifest.get("generation"),
                "expanded_generation": expanded_manifest.get("generation"),
                "crawl_digest": crawl_digest,
                "sources": resolve_manifest.get("sources"),
                "overture_release": expanded_manifest.get("overture_release"),
                "feeds": len(feeds),
                "feeds_covered": len(covered),
                "linked_rt_feeds": sum(
                    1
                    for feed in feeds
                    if feed.get("spec") == "gtfs-rt" and feed.get("static_feed_id")
                ),
                "dangling_static_links": sorted(
                    feed["feed_id"]
                    for feed in feeds
                    if feed.get("dangling_static_feed_id")
                ),
                "feeds_crawl_covered": len(crawl_report["superseded"]),
                "crawl_state_mismatches": crawl_report["state_mismatches"],
                "unmatched_crawl_ids": crawl_report["unmatched_crawl_ids"],
                "edges": len(edges),
                "edges_by_place_kind": dict(
                    collections.Counter(places[e["place_id"]]["kind"] for e in edges)
                ),
                "unknown_place_ids": unknown_places,
                "unmatched_feed_ids": unmatched_feeds,
                "unknown_placement_levels": unknown_levels,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "coverage",
                COVERAGE_POINTER,
                {
                    FEEDS_ARTIFACT: store.jsonl_chunks(feeds),
                    EDGES_ARTIFACT: store.jsonl_chunks(edges),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
