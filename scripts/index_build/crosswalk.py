"""Crosswalk stage: resolve the same feed across the ingest catalogues.

Reads the raw Atlas and Mobility Database generations and writes one
``feeds.jsonl`` of unified feed records, each with a stable ``feed_id`` and
the crosswalk method that produced it. This is the url-exact step — the
unambiguous case, where a GTFS feed's download URL is byte-identical in both
catalogues. The gated same-host and geohash steps, the GBFS ``systems.csv``
link and the GTFS-RT static link are later steps of the same stage and refine
the records this step leaves at ``crosswalk_method = "none"``.

Identity follows decision L: ``feed_id`` is the Onestop ID where one exists,
else a minted ``f-mdb-<mdb_id>``. A record keeps the contributing source rows
verbatim under ``atlas`` / ``mdb`` so nothing downstream must re-read raw.
"""

import json

from index_build import store

FEEDS_POINTER = "feeds.json"
FEEDS_ARTIFACT = "feeds.jsonl"

# url-exact identity is resolved for GTFS static feeds only. An Atlas GTFS-RT
# feed bundles three endpoint URLs that MDB lists as three separate feeds, so
# a URL match there is one-to-many and cannot assert a single identity; RT
# linkage is the static-link step, not this one.
ATLAS_STATIC_URL = "static_current"
MDB_DOWNLOAD_URL = "direct_download"


class CrosswalkError(RuntimeError):
    """A crosswalk input this stage cannot represent."""


def _clean_url(value):
    """A URL usable as an identity key, or None.

    Only a plain string with non-whitespace content counts. The match is on the
    exact bytes — surrounding whitespace is *not* trimmed, since a value that
    differs only by whitespace is a different string and must not be asserted to
    be the same feed (a wrong merge adopts identity and would corrupt a licence
    block and dataset history). Such a pair can still resolve through the later
    same-host step.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def _read_feeds(cache_dir, pointer, artifact):
    generation, _ = store.resolve(cache_dir / "raw", pointer)
    with generation:
        text = generation.read_bytes(artifact).decode("utf-8")
    # Split only on the LF the writer inserts: str.splitlines() would also
    # break on U+2028/U+2029/U+0085, which ensure_ascii=False writes raw inside
    # a feed name, corrupting the record.
    return [json.loads(line) for line in text.split("\n") if line]


def _unique_gtfs_url_index(feeds, url_of):
    """GTFS feeds keyed by download URL, keeping only URLs unique in the source.

    A URL shared by several feeds cannot resolve a single identity, so it is
    dropped from the index and left for the later same-host step rather than
    resolved to an arbitrary one of them.
    """
    by_url = {}
    dropped = set()
    for feed in feeds:
        if feed["spec"] != "gtfs":
            continue
        url = _clean_url(url_of(feed))
        if url is None:
            continue
        if url in by_url:
            dropped.add(url)
        else:
            by_url[url] = feed
    for url in dropped:
        del by_url[url]
    return by_url


def _url_exact_pairs(atlas_feeds, mdb_feeds):
    """``(atlas_feed, mdb_feed)`` pairs whose GTFS URL is identical and unique.

    Uniqueness on both sides makes each pair a clean one-to-one identity;
    anything ambiguous stays unmatched.
    """
    atlas_index = _unique_gtfs_url_index(
        atlas_feeds, lambda feed: (feed.get("urls") or {}).get(ATLAS_STATIC_URL)
    )
    mdb_index = _unique_gtfs_url_index(
        mdb_feeds, lambda feed: (feed.get("urls") or {}).get(MDB_DOWNLOAD_URL)
    )
    pairs = []
    for url, atlas_feed in atlas_index.items():
        mdb_feed = mdb_index.get(url)
        if mdb_feed is not None:
            pairs.append((atlas_feed, mdb_feed))
    return pairs


def _mint_mdb(mdb_id):
    """A stable ``f-mdb-*`` id for a feed MDB carries but Atlas does not.

    MDB's numeric ids already read ``mdb-1234``; the redundant prefix is
    dropped so the mint is ``f-mdb-1234`` rather than ``f-mdb-mdb-1234``. Slug
    ids (``jbda-...``) carry no such prefix and are kept whole. Two ids can in
    principle mint the same string (``mdb-1`` and a bare ``1`` both give
    ``f-mdb-1``); ``build_records`` refuses any such collision. It does not
    occur in the real catalogue.
    """
    if mdb_id.startswith("mdb-") and len(mdb_id) > len("mdb-"):
        mdb_id = mdb_id[len("mdb-") :]
    return f"f-mdb-{mdb_id}"


def _both_record(atlas_feed, mdb_feed):
    """One feed carried by both catalogues, keyed on its Onestop ID.

    The minted ``f-mdb-*`` id is kept in ``aliases`` so overrides filed against
    it before the crosswalk resolved still find the feed — unless the Onestop
    ID already equals it, when the alias would be a redundant self-reference.
    """
    onestop_id = atlas_feed["onestop_id"]
    minted = _mint_mdb(mdb_feed["mdb_id"])
    return {
        "feed_id": onestop_id,
        "onestop_id": onestop_id,
        "mdb_id": mdb_feed["mdb_id"],
        "aliases": [minted] if minted != onestop_id else [],
        "id_minted": False,
        "source": "both",
        "spec": atlas_feed["spec"],
        "name": atlas_feed.get("name")
        or mdb_feed.get("name")
        or mdb_feed.get("provider"),
        "crosswalk_method": "url_exact",
        "crosswalk_confidence": 1.0,
        "atlas": atlas_feed,
        "mdb": mdb_feed,
    }


def _atlas_record(feed):
    return {
        "feed_id": feed["onestop_id"],
        "onestop_id": feed["onestop_id"],
        "mdb_id": None,
        "aliases": [],
        "id_minted": False,
        "source": "atlas",
        "spec": feed["spec"],
        "name": feed.get("name"),
        "crosswalk_method": "none",
        "crosswalk_confidence": 0.0,
        "atlas": feed,
    }


def _mdb_record(feed):
    return {
        "feed_id": _mint_mdb(feed["mdb_id"]),
        "onestop_id": None,
        "mdb_id": feed["mdb_id"],
        "aliases": [],
        "id_minted": True,
        "source": "mdb",
        "spec": feed["spec"],
        "name": feed.get("name") or feed.get("provider"),
        "crosswalk_method": "none",
        "crosswalk_confidence": 0.0,
        "mdb": feed,
    }


def _require_unique_ids(feeds, key, kind):
    """Refuse a duplicate source id.

    The ingests already key on these, but this stage matches and mints from
    them, so a duplicate must stop the build rather than silently drop the
    row that the match-tracking set would omit.
    """
    seen = set()
    for feed in feeds:
        value = feed[key]
        if value in seen:
            raise CrosswalkError(f"{kind} id {value!r} appears more than once")
        seen.add(value)


def _require_unique_namespace(records):
    """Every ``feed_id`` and alias must name exactly one feed.

    ``feed_id`` and ``aliases`` are one lookup namespace, so any id repeated
    across records makes a lookup ambiguous and is refused here rather than
    published — whether two records mint the same id (``mdb-1`` and ``1`` both
    mint ``f-mdb-1``) or a minted id collides with a canonical one (an Atlas
    feed whose Onestop ID is literally ``f-mdb-1``). Within one record the
    identities are always distinct, so a repeat is always a cross-record clash.
    """
    seen = set()
    for record in records:
        for identity in (record["feed_id"], *record["aliases"]):
            if identity in seen:
                raise CrosswalkError(
                    f"feed id {identity!r} is claimed by more than one feed"
                )
            seen.add(identity)


def build_records(atlas_feeds, mdb_feeds):
    """Unified feed records for the whole catalogue, url-exact resolved.

    Returns ``(records, summary)``. Every Atlas and MDB feed appears exactly
    once: a matched pair as one ``both`` record, everything else on its own.
    """
    _require_unique_ids(atlas_feeds, "onestop_id", "atlas feed")
    _require_unique_ids(mdb_feeds, "mdb_id", "mdb feed")
    pairs = _url_exact_pairs(atlas_feeds, mdb_feeds)
    matched_onestop = {atlas_feed["onestop_id"] for atlas_feed, _ in pairs}
    matched_mdb = {mdb_feed["mdb_id"] for _, mdb_feed in pairs}

    records = [_both_record(atlas_feed, mdb_feed) for atlas_feed, mdb_feed in pairs]
    records.extend(
        _atlas_record(feed)
        for feed in atlas_feeds
        if feed["onestop_id"] not in matched_onestop
    )
    records.extend(
        _mdb_record(feed) for feed in mdb_feeds if feed["mdb_id"] not in matched_mdb
    )
    _require_unique_namespace(records)

    by_source = {"atlas": 0, "mdb": 0, "both": 0}
    by_method = {"url_exact": 0, "none": 0}
    for record in records:
        by_source[record["source"]] += 1
        by_method[record["crosswalk_method"]] += 1
    summary = {
        "feeds": len(records),
        "feeds_by_source": by_source,
        "crosswalk_by_method": by_method,
        "url_exact_pairs": len(pairs),
    }
    return records, summary


def crosswalk(cache_dir):
    """Run the crosswalk stage, publishing a ``feeds.json`` generation."""
    atlas_feeds = _read_feeds(cache_dir, "atlas.json", "atlas_feeds.jsonl")
    mdb_feeds = _read_feeds(cache_dir, "mdb.json", "mdb_feeds.jsonl")
    records, summary = build_records(atlas_feeds, mdb_feeds)
    if not records:
        raise CrosswalkError("crosswalk produced no feeds")

    out = cache_dir / "crosswalk"
    # Reach the store through `cache_dir` so a symlink at the cache root cannot
    # redirect the publish; reads go through store.resolve, which guards it too.
    directory = store.open_subdir(cache_dir, "crosswalk")
    try:
        with store.exclusive_writer(directory):
            manifest = {"source": "crosswalk", **summary}
            return store.publish(
                out,
                FEEDS_POINTER,
                {FEEDS_ARTIFACT: store.jsonl_chunks(records)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()
