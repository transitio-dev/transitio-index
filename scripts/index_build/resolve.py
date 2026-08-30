"""Stage 3, resolve half: settle feed identity and crawlability from overrides.

Applies the ``set_identity`` and ``mark_uncrawlable`` operations from
``overrides/feeds.yaml`` to the crosswalk feeds and writes
``feeds_resolved.jsonl``. These two are settled before any crawl because identity
is the crawl cache key and an uncrawlable feed must never be fetched at all; the
crawl half itself is a later stage, and ``set_coverage`` is left for the coverage
stage. It fetches nothing. An override references a feed by its ``feed_id`` or any
of its aliases — the crosswalk keeps superseded ids in ``aliases`` for exactly
this — so a correction filed against a pre-crosswalk id still lands.
"""

import collections
import datetime

from index_build import overrides, store

RESOLVE_POINTER = "feeds_resolved.json"
RESOLVE_ARTIFACT = "feeds_resolved.jsonl"


def _matching_refs(feed_overrides, feed):
    """Every override reference this feed matches, by feed_id or any alias."""
    keys = [feed["feed_id"], *(feed.get("aliases") or [])]
    return {key for key in keys if key in feed_overrides}


def _check_namespace(feeds):
    """Every lookup key — a feed_id or an alias — must resolve to one feed.

    Overrides can rename ids and add aliases, so after applying them the whole
    lookup namespace is checked, not just the primary keys: a key shared across
    two feeds (a duplicate id, or an alias equal to another feed's id or alias)
    would make the next build's override matching ambiguous.
    """
    namespace = collections.defaultdict(set)
    for index, feed in enumerate(feeds):
        for key in [feed["feed_id"], *(feed.get("aliases") or [])]:
            namespace[key].add(index)
    shared = sorted(key for key, at in namespace.items() if len(at) > 1)
    if shared:
        raise overrides.OverrideError(
            f"resolved feeds share lookup keys (id or alias): {shared}"
        )


def _apply(feed, entry):
    identity = entry.get("set_identity") or {}
    old_id = feed["feed_id"]
    new_id = identity.get("feed_id")
    if "feed_id" in identity or "onestop_id" in identity:
        # A curator-supplied id is authoritative, not machine-minted.
        feed["id_minted"] = False
    for field, value in identity.items():
        feed[field] = value
    if new_id and new_id != old_id:
        # Preserve the old id in aliases — after any aliases the override itself
        # set — so the override chain and a crawl artifact filed under it still
        # resolve to this feed.
        aliases = feed.setdefault("aliases", [])
        if old_id not in aliases:
            aliases.append(old_id)
    if "mark_uncrawlable" in entry:
        spec = entry["mark_uncrawlable"]
        feed["crawlable"] = False
        feed["uncrawlable_reason"] = (
            spec.get("reason") if isinstance(spec, dict) else None
        )


def resolve(cache_dir, *, overrides_dir=None):
    """Resolve feed identity and crawlability; publish the ``feeds_resolved`` gen.

    Reads the crosswalk feeds, applies matching feed overrides, stamps every feed
    with a ``crawlable`` flag (and any ``uncrawlable_reason``), and republishes
    them. One writer lock spans the read and the publish. Returns the manifest.
    """
    feed_overrides = overrides.load_feed_overrides(overrides_dir)
    directory = store.open_subdir(cache_dir, "resolve")
    try:
        with store.exclusive_writer(directory):
            feeds, _ = store.read_jsonl(
                cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
            )
            # Build the whole feed<->override match graph first, so neither an
            # override matching several feeds nor several overrides matching one
            # feed can slip through a first-match shortcut.
            ref_to_feeds = collections.defaultdict(list)
            for feed in feeds:
                feed.setdefault("crawlable", True)
                feed.setdefault("uncrawlable_reason", None)
                refs = _matching_refs(feed_overrides, feed)
                if len(refs) > 1:
                    raise overrides.OverrideError(
                        f"feed {feed['feed_id']!r} matched by several overrides: "
                        f"{sorted(refs)}"
                    )
                for ref in refs:
                    ref_to_feeds[ref].append(feed)
            ambiguous = sorted(ref for ref, hit in ref_to_feeds.items() if len(hit) > 1)
            if ambiguous:
                raise overrides.OverrideError(
                    f"override matches several feeds: {ambiguous}"
                )
            for ref, hit in ref_to_feeds.items():
                _apply(hit[0], feed_overrides[ref])
            matched = set(ref_to_feeds)
            _check_namespace(feeds)
            manifest = {
                "source": "resolve",
                "feeds": len(feeds),
                "overridden_feeds": len(matched),
                "uncrawlable": sum(1 for feed in feeds if not feed["crawlable"]),
                "unmatched_overrides": sorted(set(feed_overrides) - matched),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "resolve",
                RESOLVE_POINTER,
                {RESOLVE_ARTIFACT: store.jsonl_chunks(feeds)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()
