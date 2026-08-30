"""Load the maintainer override files (``overrides/*.yaml``).

Overrides are the durable curation the plan protects: the generated index is
disposable, the override file is the asset, and every build reads them fresh.
This loads the feed-keyed file and returns its entries for a stage to apply.
Place and edge overrides, and staleness detection, belong to the stages that own
them and are added there.
"""

import pathlib
import re

FEEDS_FILE = "feeds.yaml"

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
    if not path.is_file():
        return {}
    import yaml

    raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_strict_loader())
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
