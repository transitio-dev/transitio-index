"""Load the maintainer override files (``overrides/*.yaml``).

Overrides are the durable curation the plan protects: the generated index is
disposable, the override file is the asset, and every build reads them fresh.
This loads the feed-keyed and edge-keyed files and returns their entries for
the stages that own them to apply; staleness detection belongs to those
stages. Place overrides are added with the stage that applies them.
"""

import hashlib
import json
import os
import pathlib

from index_build import store

import re

FEEDS_FILE = "feeds.yaml"
EDGES_FILE = "edges.yaml"
PLACES_FILE = "places.yaml"
PLACE_KINDS = ("country", "region", "city", "metro")
COVERAGE_LEVELS = ("municipality", "subdivision", "country", "bbox", "geohash")
TIERS = ("local", "regional", "national", "international", "unknown")

# The identity fields ``set_identity`` may correct — ``feed_id`` included, since
# the corrected id is the crawl cache key. Renaming it preserves the old id in
# ``aliases`` (the resolve stage) so a later override filed against it still lands.
IDENTITY_FIELDS = frozenset(
    {
        "feed_id",
        "onestop_id",
        "mdb_id",
        "name",
        "aliases",
        "static_feed_id",
        "static_link_method",
    }
)

# A feed id flows into JSONL, the Parquet index and a crawl-cache path component,
# so reject values that would corrupt those: empty, over-long, control bytes, a
# path separator, or a traversal. Full path-component portability (Windows
# reserved names and the like) is the crawl stage's own concern.
_MAX_FEED_ID = 512
_UNSAFE_ID = re.compile(r"[\x00-\x1f\x7f/\\]")
_NULLABLE_STR = ("onestop_id", "mdb_id", "name", "static_feed_id", "static_link_method")

# static_link_method is a closed set the crosswalk assigns; an override may only
# set it to one of these or null, never an arbitrary string.
_STATIC_LINK_METHODS = frozenset({"declared", "same_file", "same_host", "none"})

# The operations a feed entry may carry. ``set_coverage`` is applied by the
# coverage stage, not the resolve stage, but is a valid key here.
_OPERATIONS = frozenset({"set_identity", "mark_uncrawlable", "set_coverage"})
# The operations the resolve stage applies; set_coverage enters at coverage.
RESOLVE_OPERATIONS = frozenset({"set_identity", "mark_uncrawlable"})
_METADATA = frozenset({"feed", "reason", "author", "date", "evidence_hash"})


class OverrideError(RuntimeError):
    """An override file is malformed."""


_LOADER = None


def _strict_loader():
    """A YAML loader that rejects duplicate mapping keys (PyYAML keeps the last)."""
    global _LOADER
    if _LOADER is None:
        import yaml

        class _StrictLoader(yaml.SafeLoader):
            pass

        def _no_duplicate_keys(loader, node, deep=False):
            mapping = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node, deep=deep)
                if key in mapping:
                    raise OverrideError(f"duplicate key {key!r} in an override entry")
                mapping[key] = loader.construct_object(value_node, deep=deep)
            return mapping

        _StrictLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
        )
        _LOADER = _StrictLoader
    return _LOADER


def _valid_feed_id(value):
    return (
        isinstance(value, str)
        and 0 < len(value) <= _MAX_FEED_ID
        and value not in (".", "..")
        and _UNSAFE_ID.search(value) is None
    )


def _validate_identity(path, ref, identity):
    """Reject a ``set_identity`` whose fields or values would corrupt the feed."""
    if not isinstance(identity, dict) or not identity:
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity must be a non-empty mapping"
        )
    unknown = set(identity) - IDENTITY_FIELDS
    if unknown:
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity has unknown fields {sorted(unknown)}"
        )
    if "feed_id" in identity and not _valid_feed_id(identity["feed_id"]):
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity feed_id {identity['feed_id']!r} is "
            f"not a valid feed id"
        )
    for field in _NULLABLE_STR:
        value = identity.get(field)
        if field in identity and value is not None and not isinstance(value, str):
            raise OverrideError(
                f"{path}: feed {ref!r} set_identity {field} must be a string or null"
            )
    if "aliases" in identity:
        aliases = identity["aliases"]
        if not isinstance(aliases, list) or not all(
            isinstance(alias, str) for alias in aliases
        ):
            raise OverrideError(
                f"{path}: feed {ref!r} set_identity aliases must be a list of strings"
            )
    method = identity.get("static_link_method")
    if method is not None and method not in _STATIC_LINK_METHODS:
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity static_link_method {method!r} must "
            f"be one of {sorted(_STATIC_LINK_METHODS)} or null"
        )


def _validate_operations(path, ref, entry):
    if not set(entry) & _OPERATIONS:
        raise OverrideError(f"{path}: feed {ref!r} carries no operation")
    if "set_identity" in entry:
        _validate_identity(path, ref, entry["set_identity"])
    if "mark_uncrawlable" in entry:
        spec = entry["mark_uncrawlable"]
        if spec is not True and not isinstance(spec, dict):
            raise OverrideError(
                f"{path}: feed {ref!r} mark_uncrawlable must be true or a mapping"
            )
    if "set_coverage" in entry:
        spec = entry["set_coverage"]
        if not isinstance(spec, dict) or set(spec) != {"level", "place_id"}:
            raise OverrideError(
                f"{path}: feed {ref!r} set_coverage must be a mapping of level and "
                "place_id"
            )
        if spec["level"] not in COVERAGE_LEVELS:
            raise OverrideError(
                f"{path}: feed {ref!r} set_coverage level must be one of "
                f"{list(COVERAGE_LEVELS)}"
            )
        if not isinstance(spec["place_id"], str) or not spec["place_id"]:
            raise OverrideError(
                f"{path}: feed {ref!r} set_coverage place_id must be a place id"
            )


def load_feed_overrides(overrides_dir):
    """The ``feeds.yaml`` entries keyed by feed reference and the digest of
    the bytes they came from: ``({}, None)`` when absent.

    Returns ``(feed_ref -> entry, digest)``. A duplicate reference, an entry
    with no ``feed`` key or no operation, an unknown operation, or a malformed
    operation value is a build error rather than a silent skip.
    """
    if overrides_dir is None:
        return {}, None
    path = pathlib.Path(overrides_dir) / FEEDS_FILE
    data, digest = read_override(overrides_dir, FEEDS_FILE)
    if data is None:
        return {}, None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return {}, digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of override entries")
    entries = raw
    by_feed = {}
    for entry in entries:
        if not isinstance(entry, dict) or "feed" not in entry:
            raise OverrideError(f"{path}: every entry needs a 'feed' key")
        ref = entry["feed"]
        if not isinstance(ref, str) or not ref:
            raise OverrideError(f"{path}: a 'feed' key must be a non-empty string")
        if ref in by_feed:
            raise OverrideError(f"{path}: duplicate override for feed {ref!r}")
        unknown = set(entry) - _OPERATIONS - _METADATA
        if unknown:
            raise OverrideError(
                f"{path}: feed {ref!r} has unknown keys {sorted(unknown)}"
            )
        _validate_operations(path, ref, entry)
        by_feed[ref] = entry
    return by_feed, digest


def read_override(overrides_dir, name):
    """``(bytes, sha256)`` of the override file ``name`` under
    ``overrides_dir``, or ``(None, None)`` when it does not exist.

    The directory is opened refusing a symlink at its own component, and
    the fixed basename relative to that directory descriptor — never
    following a symlink, non-blocking so a FIFO cannot wedge the open, and
    checked on the descriptor to be a regular file — through the store's
    own helpers, so an override file is repository data and never a pointer
    to something outside it. Read once: what is parsed is what is hashed.
    """
    try:
        directory = store.open_directory(pathlib.Path(overrides_dir))
    except store.StoreError as error:
        raise OverrideError(str(error)) from error
    try:
        try:
            handle = store.open_regular(directory, name)
        except store.MissingEntry:
            return None, None
        except store.StoreError as error:
            raise OverrideError(str(error)) from error
        with os.fdopen(handle, "rb") as opened:
            data = opened.read()
    finally:
        directory.close()
    return data, hashlib.sha256(data).hexdigest()


def applied_digest(manifest, overrides_dir):
    """The digest of the current ``edges.yaml`` an edge generation must have
    applied, or :class:`OverrideError`: a curate generation whose recorded
    digest differs from the file was built from another version of it, and
    an ``edges.yaml`` with no curate generation at all is a stage that has
    not run. Returns the current digest (None without a file)."""
    current = edges_digest(overrides_dir)
    if manifest is not None and manifest.get("source") == "curate":
        if manifest.get("overrides_sha256") != current:
            raise OverrideError(
                "edges.yaml changed since the curate stage applied it; "
                "re-run the curate stage"
            )
    elif current is not None:
        raise OverrideError(
            "edge overrides exist but no curate generation applied them; "
            "run the curate stage"
        )
    return current


def edges_digest(overrides_dir):
    """The SHA-256 of the exact ``edges.yaml`` bytes, or None when there is
    no file: what a curate generation records, and what publish checks the
    current file against, so an edited override can never ship through a
    generation built before the edit."""
    if overrides_dir is None:
        return None
    return read_override(overrides_dir, EDGES_FILE)[1]


# ---- edges.yaml ----

# One operation per entry. ``set_tiers`` is pair-scoped (no ``tier``); the
# other three name the tier edge they touch.
_EDGE_OPERATIONS = frozenset({"set_tiers", "set_selector", "add_edge", "remove_edge"})
_EDGE_METADATA = frozenset(
    {"feed", "place", "tier", "reason", "author", "date", "evidence_hash"}
)
# Only a tier decision carries a confidence.
_CONFIDENCE_OPERATIONS = frozenset({"set_tiers", "add_edge"})
_SELECTOR_CLAUSES = frozenset({"route_id", "agency_id", "route_type"})


def _validate_selector(path, where, spec):
    """A selector is ``whole_feed``, an explicit ``route_id`` list, or a
    predicate of ``agency_id`` / ``route_type`` lists (ANDed); never both an
    id list and a predicate, which would leave the ids' meaning ambiguous."""
    if spec == "whole_feed":
        return
    if not isinstance(spec, dict) or not spec:
        raise OverrideError(
            f"{path}: {where} selector must be 'whole_feed' or a non-empty mapping"
        )
    unknown = set(spec) - _SELECTOR_CLAUSES
    if unknown:
        raise OverrideError(
            f"{path}: {where} selector has unknown keys {sorted(unknown)}"
        )
    if "route_id" in spec and len(spec) > 1:
        raise OverrideError(
            f"{path}: {where} selector lists route ids and a predicate; pick one"
        )
    for key, kind in (("route_id", str), ("agency_id", str), ("route_type", int)):
        values = spec.get(key)
        if key in spec and (
            not isinstance(values, list)
            or not values
            or any(not isinstance(v, kind) or isinstance(v, bool) for v in values)
        ):
            raise OverrideError(
                f"{path}: {where} selector {key} must be a non-empty list of "
                f"{kind.__name__}"
            )


def _validate_confidence(path, where, value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OverrideError(f"{path}: {where} tier_confidence must be a number")
    if not 0.0 <= float(value) <= 1.0:
        raise OverrideError(f"{path}: {where} tier_confidence must lie in [0, 1]")


def load_edge_overrides(overrides_dir):
    """``(entries, sha256)``: the ``edges.yaml`` entries in file order and
    the digest of the bytes they were parsed from — ``([], None)`` when
    there is no file.

    Each entry gains an ``operation`` key naming its single operation. Every
    entry needs ``feed`` and ``place`` (``"*"`` for every place the feed
    serves); tier-edge operations need ``tier`` (``"*"`` allowed), pair-scoped
    ``set_tiers`` must not carry one. Two entries with the same keys and
    operation are a duplicate, a build error rather than a silent skip.
    """
    if overrides_dir is None:
        return [], None
    path = pathlib.Path(overrides_dir) / EDGES_FILE
    data, digest = read_override(overrides_dir, EDGES_FILE)
    if data is None:
        return [], None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return [], digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of override entries")
    entries = []
    seen = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise OverrideError(f"{path}: every entry must be a mapping")
        for key in ("feed", "place"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                raise OverrideError(f"{path}: every entry needs a non-empty '{key}'")
        where = f"{entry['feed']}/{entry['place']}"
        unknown = set(entry) - _EDGE_OPERATIONS - _EDGE_METADATA - {"tier_confidence"}
        if unknown:
            raise OverrideError(f"{path}: {where} has unknown keys {sorted(unknown)}")
        operations = sorted(set(entry) & _EDGE_OPERATIONS)
        if len(operations) != 1:
            raise OverrideError(f"{path}: {where} needs exactly one operation")
        (operation,) = operations
        tier = entry.get("tier")
        if operation == "set_tiers":
            if tier is not None:
                raise OverrideError(
                    f"{path}: {where} set_tiers is pair-scoped: no tier"
                )
            tiers = entry["set_tiers"]
            if (
                not isinstance(tiers, list)
                or not tiers
                or any(t not in TIERS for t in tiers)
                or len(set(tiers)) != len(tiers)
            ):
                raise OverrideError(
                    f"{path}: {where} set_tiers must be a non-empty list of tiers"
                )
        else:
            if tier != "*" and tier not in TIERS:
                raise OverrideError(
                    f"{path}: {where} {operation} needs a tier (or '*')"
                )
            where = f"{where}/{tier}"
        if operation == "set_selector":
            _validate_selector(path, where, entry["set_selector"])
        if operation == "add_edge":
            spec = entry["add_edge"]
            if spec is not True and not isinstance(spec, dict):
                raise OverrideError(
                    f"{path}: {where} add_edge must be true or a mapping"
                )
            if isinstance(spec, dict):
                unknown = set(spec) - {"selector", "tier_confidence"}
                if unknown:
                    raise OverrideError(
                        f"{path}: {where} add_edge has unknown keys {sorted(unknown)}"
                    )
                if "selector" in spec:
                    _validate_selector(path, where, spec["selector"])
                if "tier_confidence" in spec:
                    if "tier_confidence" in entry:
                        raise OverrideError(
                            f"{path}: {where} tier_confidence declared twice"
                        )
                    _validate_confidence(path, where, spec["tier_confidence"])
            if tier == "*":
                raise OverrideError(f"{path}: {where} add_edge names one tier")
        if operation == "remove_edge" and entry["remove_edge"] is not True:
            raise OverrideError(f"{path}: {where} remove_edge must be true")
        if "tier_confidence" in entry:
            if operation not in _CONFIDENCE_OPERATIONS:
                raise OverrideError(
                    f"{path}: {where} {operation} takes no tier_confidence"
                )
            _validate_confidence(path, where, entry["tier_confidence"])
        if "evidence_hash" in entry and not isinstance(entry["evidence_hash"], str):
            raise OverrideError(f"{path}: {where} evidence_hash must be a string")
        key = (entry["feed"], entry["place"], tier, operation)
        if key in seen:
            raise OverrideError(f"{path}: duplicate {operation} for {where}")
        seen.add(key)
        entries.append({**entry, "operation": operation})
    return entries, digest


# ---- places.yaml ----

_PLACE_OPERATIONS = frozenset(
    {"add_place", "set_place_members", "set_boundary", "set_aliases", "resolve_place"}
)
_PLACE_METADATA = frozenset(
    {"place", "source_ref", "reason", "author", "date", "evidence_hash"}
)
_ADD_PLACE_FIELDS = frozenset(
    {"kind", "name", "parent_id", "boundary", "member_ids", "country_code"}
)


def _qid(value):
    return isinstance(value, str) and bool(re.match(r"\AQ[1-9][0-9]*\Z", value))


def _qid_list(value):
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(v, str) and v for v in value)
    )


def _validate_place_entry(path, entry):
    where = f"place {entry.get('place') or entry.get('source_ref')!r}"
    unknown = set(entry) - _PLACE_OPERATIONS - _PLACE_METADATA
    if unknown:
        raise OverrideError(f"{path}: {where} has unknown keys {sorted(unknown)}")
    operations = sorted(set(entry) & _PLACE_OPERATIONS)
    if len(operations) != 1:
        raise OverrideError(f"{path}: {where} needs exactly one operation")
    (operation,) = operations
    if not isinstance(entry.get("place"), str) or not entry["place"]:
        raise OverrideError(f"{path}: {where} needs a 'place' id")
    if operation in ("add_place", "resolve_place") and not _qid(entry["place"]):
        raise OverrideError(f"{path}: {where} {operation} needs a real QID as 'place'")
    if operation == "resolve_place":
        if not isinstance(entry.get("source_ref"), str) or not entry["source_ref"]:
            raise OverrideError(f"{path}: {where} resolve_place needs a source_ref")
        if entry["resolve_place"] is not True:
            raise OverrideError(f"{path}: {where} resolve_place must be true")
    elif "source_ref" in entry:
        raise OverrideError(f"{path}: {where} only resolve_place takes a source_ref")
    spec = entry[operation]
    if operation == "add_place":
        if not isinstance(spec, dict):
            raise OverrideError(f"{path}: {where} add_place must be a mapping")
        unknown = set(spec) - _ADD_PLACE_FIELDS
        if unknown:
            raise OverrideError(
                f"{path}: {where} add_place has unknown keys {sorted(unknown)}"
            )
        if spec.get("kind") not in PLACE_KINDS or not (
            isinstance(spec.get("name"), str) and spec["name"].strip()
        ):
            raise OverrideError(f"{path}: {where} add_place needs a kind and a name")
        code = spec.get("country_code")
        if "country_code" in spec and not (
            isinstance(code, str) and re.fullmatch(r"[A-Z]{2}", code)
        ):
            raise OverrideError(
                f"{path}: {where} add_place country_code must be a two-letter "
                "upper-case ISO code"
            )
        if spec["kind"] in ("city", "region") and not (
            isinstance(spec.get("parent_id"), str) and spec["parent_id"]
        ):
            raise OverrideError(
                f"{path}: {where} add_place: a {spec['kind']} needs a parent_id"
            )
        if "boundary" in spec and "member_ids" in spec:
            raise OverrideError(
                f"{path}: {where} add_place takes a boundary or a member list, not both"
            )
        if "boundary" in spec and not isinstance(spec["boundary"], str):
            raise OverrideError(f"{path}: {where} add_place boundary must be WKT")
        if "member_ids" in spec and (
            spec["kind"] != "metro" or not _qid_list(spec["member_ids"])
        ):
            raise OverrideError(
                f"{path}: {where} add_place member_ids belong to a metro, as a QID list"
            )
    elif operation == "set_place_members":
        if not _qid_list(spec):
            raise OverrideError(
                f"{path}: {where} set_place_members must be a non-empty QID list"
            )
    elif operation == "set_boundary":
        if not isinstance(spec, str) or not spec:
            raise OverrideError(f"{path}: {where} set_boundary must be WKT")
    elif operation == "set_aliases":
        if (
            not isinstance(spec, list)
            or not spec
            or any(not isinstance(a, str) or not a for a in spec)
        ):
            raise OverrideError(
                f"{path}: {where} set_aliases must be a non-empty list of strings"
            )
    if "evidence_hash" in entry and not isinstance(entry["evidence_hash"], str):
        raise OverrideError(f"{path}: {where} evidence_hash must be a string")
    return operation


def load_place_overrides(overrides_dir):
    """``(entries, sha256)``: the ``places.yaml`` entries in file order, each
    with an ``operation`` key, and the digest of the bytes they were parsed
    from — ``([], None)`` when there is no file. Every entry names the
    ``place`` QID it concerns; ``resolve_place`` also names the ``source_ref``
    (the unresolved candidate's Overture id) it assigns that QID to."""
    if overrides_dir is None:
        return [], None
    path = pathlib.Path(overrides_dir) / PLACES_FILE
    data, digest = read_override(overrides_dir, PLACES_FILE)
    if data is None:
        return [], None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return [], digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of override entries")
    entries = []
    seen = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise OverrideError(f"{path}: every entry must be a mapping")
        operation = _validate_place_entry(path, entry)
        # resolve_place is keyed by the candidate it resolves: two entries
        # naming one candidate would race for its QID.
        key = (
            (entry["source_ref"], operation)
            if operation == "resolve_place"
            else (entry["place"], operation)
        )
        if key in seen:
            raise OverrideError(f"{path}: duplicate {operation} for {key[0]!r}")
        seen.add(key)
        entries.append({**entry, "operation": operation})
    return entries, digest


def by_operation(entries, operation):
    return [entry for entry in entries if entry["operation"] == operation]


def canonical_digest(payload):
    """The SHA-256 of a payload's canonical JSON — the one way every stage
    hashes the evidence a curator recorded against."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def judge(entry, evidence, report, scope):
    """Whether an entry is stale against ``evidence`` (the derived data the
    curator looked at, hashed canonically); a mismatch is applied anyway and
    reported with the current hash to record. An entry without an
    ``evidence_hash`` is never stale."""
    recorded = entry.get("evidence_hash")
    if recorded is None:
        return False
    current = canonical_digest(evidence)
    if current == recorded:
        return False
    report.append(
        {
            "scope": scope,
            "place": entry.get("place"),
            "feed": entry.get("feed"),
            "source_ref": entry.get("source_ref"),
            "operation": entry["operation"],
            "recorded_evidence_hash": recorded,
            "current_evidence_hash": current,
        }
    )
    return True


def phase_digest(by_feed, operations):
    """The digest of the feed entries' given operations alone, None when no
    entry carries one: an edit to another phase's operations does not send
    this phase's stage back."""
    subset = {}
    for ref, entry in by_feed.items():
        ops = {op: entry[op] for op in sorted(operations) if op in entry}
        if ops:
            subset[ref] = ops
    return canonical_digest(subset) if subset else None


def feeds_digest(overrides_dir):
    """The SHA-256 of the current ``feeds.yaml`` bytes, or None without one."""
    return (
        None if overrides_dir is None else read_override(overrides_dir, FEEDS_FILE)[1]
    )


def places_digest(overrides_dir):
    """The SHA-256 of the current ``places.yaml`` bytes, or None without one."""
    return (
        None if overrides_dir is None else read_override(overrides_dir, PLACES_FILE)[1]
    )


def expect_digest(recorded, current, what, rerun):
    """An override file read by a later stage must be the one an earlier
    stage applied: a mixed snapshot never ships. ``recorded`` is what the
    earlier manifest carries (None when it predates the file)."""
    if recorded != current:
        raise OverrideError(
            f"{what} changed since the {rerun} stage applied it; re-run the "
            f"{rerun} stage"
        )


def strict_check(strict, report, stage):
    """``--strict-overrides``: a stale override fails the stage once its
    report is preserved in the generation just published."""
    if strict and report:
        raise OverrideError(
            f"{len(report)} stale override(s) in the {stage} stage; see its "
            "override_report.jsonl"
        )
