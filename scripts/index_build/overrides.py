"""Load the maintainer override files (``overrides/*.yaml``).

Overrides are the durable curation the plan protects: the generated index is
disposable, the override file is the asset, and every build reads them fresh.
This loads the feed-keyed and edge-keyed files and returns their entries for
the stages that own them to apply; staleness detection belongs to those
stages. Place overrides are added with the stage that applies them.
"""

import hashlib
import os
import pathlib

from index_build import store

import re

FEEDS_FILE = "feeds.yaml"
EDGES_FILE = "edges.yaml"
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
    if "set_coverage" in entry and not isinstance(entry["set_coverage"], dict):
        raise OverrideError(f"{path}: feed {ref!r} set_coverage must be a mapping")


def load_feed_overrides(overrides_dir):
    """The ``feeds.yaml`` entries keyed by feed reference, or ``{}`` when absent.

    Returns a mapping ``feed_ref -> entry``. A duplicate reference, an entry with
    no ``feed`` key or no operation, an unknown operation, or a malformed
    operation value is a build error rather than a silent skip.
    """
    if overrides_dir is None:
        return {}
    path = pathlib.Path(overrides_dir) / FEEDS_FILE
    data, _ = read_override(overrides_dir, FEEDS_FILE)
    if data is None:
        return {}
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return {}
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
    return by_feed


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
