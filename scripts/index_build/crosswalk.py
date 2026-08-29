"""Crosswalk stage: resolve the same feed across the ingest catalogues.

Reads the raw Atlas and Mobility Database generations and writes one
``feeds.jsonl`` of unified feed records, each with a stable ``feed_id`` and
the crosswalk method that produced it. Identity is resolved in a cascade of
narrowing confidence: url-exact (a GTFS download URL byte-identical in both
catalogues), then a gated same-host match (feeds sharing a download host whose
names agree). Ambiguous same-host candidates are not merged — they go to a
``provisional_links`` report for a human to adjudicate. The geohash-confirm
step, the GBFS ``systems.csv`` link and the GTFS-RT static link are later steps
of the same stage and refine the records left at ``crosswalk_method = "none"``.

Identity follows decision L: ``feed_id`` is the Onestop ID where one exists,
else a minted ``f-mdb-<mdb_id>``. A record keeps the contributing source rows
verbatim under ``atlas`` / ``mdb`` so nothing downstream must re-read raw.
"""

import collections
import json
import re
import unicodedata
import urllib.parse

from index_build import store

FEEDS_POINTER = "feeds.json"
FEEDS_ARTIFACT = "feeds.jsonl"
PROVISIONAL_ARTIFACT = "provisional_links.jsonl"

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

    Only a real URL — one that parses with both a scheme and a host — counts, so
    a sentinel like ``N/A`` cannot be asserted as an identity. The match is on
    the exact bytes: surrounding whitespace is *not* trimmed away and then
    matched, since a value differing only by whitespace is a different string
    and must not be asserted the same feed (a wrong merge adopts identity and
    would corrupt a licence block and dataset history). Such a pair can still
    resolve through the later same-host step.
    """
    if not isinstance(value, str):
        return None
    try:
        parts = urllib.parse.urlsplit(value)
    except ValueError:
        return None
    if not parts.scheme or not parts.hostname:
        return None
    return value


def _parse_jsonl(raw):
    # Split only on the LF the writer inserts: str.splitlines() would also
    # break on U+2028/U+2029/U+0085, which ensure_ascii=False writes raw inside
    # a feed name, corrupting the record.
    return [json.loads(line) for line in raw.decode("utf-8").split("\n") if line]


def _read_feeds(cache_dir, pointer, artifact):
    generation, _ = store.resolve(cache_dir / "raw", pointer)
    with generation:
        return _parse_jsonl(generation.read_bytes(artifact))


def _read_atlas(cache_dir):
    """Atlas feeds and operators from one generation.

    Both are read while a single resolved generation is held, so an ingest that
    republishes Atlas mid-crosswalk cannot pair feeds from one generation with
    operator associations from another.
    """
    generation, _ = store.resolve(cache_dir / "raw", "atlas.json")
    with generation:
        return (
            _parse_jsonl(generation.read_bytes("atlas_feeds.jsonl")),
            _parse_jsonl(generation.read_bytes("atlas_operators.jsonl")),
        )


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


# The same-host step gates a shared download host on name agreement. Vendor
# hosts serve hundreds of unrelated agencies, so a host match alone is not an
# identity; the feeds' names must agree too. The plan's bbox-overlap signal is
# unavailable here — Atlas feed records carry no declared bounding box — so name
# agreement is the operative gate.
SAME_HOST_MIN_OVERLAP = 0.8
SAME_HOST_CONFIDENCE = 0.8

# Legal and transit suffixes dropped before comparing names, so e.g. "MTA" and
# "MTA Authority" agree.
_NAME_SUFFIXES = frozenset(
    {
        "inc",
        "ltd",
        "llc",
        "gmbh",
        "co",
        "corp",
        "transit",
        "transportation",
        "authority",
    }
)


def _name_tokens(name):
    """A name as comparable tokens: accent- and case-folded words, minus the
    legal/transit suffixes. A non-string or empty name gives an empty set.

    Tokens are runs of Unicode letters and digits (``[^\\W_]``), not ASCII only,
    so a non-Latin name keeps its script rather than collapsing to a stray
    romanized fragment — which would both miss real matches and let two
    unrelated names agree on a shared digit or ASCII word.
    """
    if not isinstance(name, str):
        return frozenset()
    decomposed = unicodedata.normalize("NFKD", name)
    folded = "".join(c for c in decomposed if not unicodedata.combining(c)).casefold()
    return frozenset(
        token for token in re.findall(r"[^\W_]+", folded) if token not in _NAME_SUFFIXES
    )


def _token_overlap(left, right):
    """Jaccard overlap of two token sets, in [0, 1]; 0 if either is empty."""
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _host(url):
    """The host of a download URL, or None.

    ``hostname`` rather than ``netloc``: the host alone, already lower-cased and
    without the userinfo or port that would otherwise split one vendor host into
    several. A trailing dot is stripped so ``host.`` and ``host`` agree.
    """
    url = _clean_url(url)
    if url is None:
        return None
    try:
        host = urllib.parse.urlsplit(url).hostname
    except ValueError:
        return None
    if not host:
        return None
    return host.rstrip(".") or None


def _operator_names_by_feed(operators):
    """Feed Onestop ID -> the names of the operators that list it.

    Most Atlas feeds carry no ``name`` of their own; the agency name lives on
    the operator records that associate to the feed, so those carry the
    crosswalk's main name evidence.
    """
    by_feed = collections.defaultdict(list)
    for operator in operators:
        name = operator.get("name")
        if not name:
            continue
        for feed_id in operator.get("associated_feed_ids") or []:
            by_feed[feed_id].append(name)
    return by_feed


def _name_token_sets(names):
    """One token set per non-empty name, dropping names that tokenize to empty.

    Names are kept apart, not merged: unioning a feed's several names would let
    tokens recombine across them and manufacture agreement — ``Alpha Bus`` +
    ``Beta Rail`` must not match ``Alpha Rail`` + ``Beta Bus``.
    """
    token_sets = []
    for name in names:
        tokens = _name_tokens(name)
        if tokens:
            token_sets.append(tokens)
    return token_sets


def _atlas_name_token_sets(feed, operator_names):
    names = [feed.get("name")]
    names.extend(operator.get("name") for operator in feed.get("operators") or [])
    names.extend(operator_names.get(feed["onestop_id"], ()))
    return _name_token_sets(names)


def _mdb_name_token_sets(feed):
    return _name_token_sets([feed.get("provider"), feed.get("name")])


def _best_name_overlap(atlas_token_sets, mdb_token_sets):
    """The strongest agreement between any one Atlas name and any one MDB name."""
    best = 0.0
    for atlas_tokens in atlas_token_sets:
        for mdb_tokens in mdb_token_sets:
            best = max(best, _token_overlap(atlas_tokens, mdb_tokens))
    return best


def _gtfs_by_host(feeds, url_key):
    by_host = collections.defaultdict(list)
    for feed in feeds:
        if feed["spec"] != "gtfs":
            continue
        host = _host((feed.get("urls") or {}).get(url_key))
        if host is not None:
            by_host[host].append(feed)
    return by_host


def _match_within_host(atlas_feeds, mdb_feeds, operator_names, host):
    """Match the feeds on one host, returning ``(pairs, provisional)``.

    A clean one-to-one name agreement is a match; a name agreeing with more
    than one feed on the host is ambiguous and every such candidate is recorded
    for review rather than merged to an arbitrary one.
    """
    atlas_tokens = {
        feed["onestop_id"]: _atlas_name_token_sets(feed, operator_names)
        for feed in atlas_feeds
    }
    mdb_tokens = {feed["mdb_id"]: _mdb_name_token_sets(feed) for feed in mdb_feeds}
    agreements = {}
    mdb_hit_count = collections.Counter()
    for atlas_feed in atlas_feeds:
        agreeing = []
        for mdb_feed in mdb_feeds:
            overlap = _best_name_overlap(
                atlas_tokens[atlas_feed["onestop_id"]], mdb_tokens[mdb_feed["mdb_id"]]
            )
            if overlap >= SAME_HOST_MIN_OVERLAP:
                agreeing.append((mdb_feed, overlap))
        agreements[atlas_feed["onestop_id"]] = agreeing
        for mdb_feed, _ in agreeing:
            mdb_hit_count[mdb_feed["mdb_id"]] += 1

    pairs = []
    provisional = []
    for atlas_feed in atlas_feeds:
        agreeing = agreements[atlas_feed["onestop_id"]]
        if not agreeing:
            continue
        if len(agreeing) == 1 and mdb_hit_count[agreeing[0][0]["mdb_id"]] == 1:
            pairs.append((atlas_feed, agreeing[0][0]))
        else:
            provisional.extend(
                {
                    "onestop_id": atlas_feed["onestop_id"],
                    "mdb_id": mdb_feed["mdb_id"],
                    "host": host,
                    "name_overlap": round(overlap, 3),
                }
                for mdb_feed, overlap in agreeing
            )
    return pairs, provisional


def _same_host_matches(atlas_feeds, mdb_feeds, operators):
    """Resolve GTFS feeds sharing a download host by name agreement.

    Returns ``(pairs, provisional, candidates)``: clean one-to-one name matches,
    the ambiguous candidates left for a human to adjudicate, and the count of
    feeds that shared a host at all — the raw population the name gate reduces.
    Feeds whose names do not agree are unrelated and appear in neither list.
    """
    operator_names = _operator_names_by_feed(operators)
    atlas_by_host = _gtfs_by_host(atlas_feeds, ATLAS_STATIC_URL)
    mdb_by_host = _gtfs_by_host(mdb_feeds, MDB_DOWNLOAD_URL)
    pairs = []
    provisional = []
    candidates = 0
    for host in sorted(atlas_by_host.keys() & mdb_by_host.keys()):
        atlas_on_host = atlas_by_host[host]
        mdb_on_host = mdb_by_host[host]
        candidates += len(atlas_on_host) + len(mdb_on_host)
        host_pairs, host_provisional = _match_within_host(
            atlas_on_host, mdb_on_host, operator_names, host
        )
        pairs.extend(host_pairs)
        provisional.extend(host_provisional)
    return pairs, provisional, candidates


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


def _both_record(atlas_feed, mdb_feed, *, method, confidence):
    """One feed carried by both catalogues, keyed on its Onestop ID.

    ``method`` and ``confidence`` record how the match was made — ``url_exact``
    at 1.0, ``same_host`` lower. The minted ``f-mdb-*`` id is kept in ``aliases``
    so overrides filed against it before the crosswalk resolved still find the
    feed — unless the Onestop ID already equals it, when the alias would be a
    redundant self-reference.
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
        "crosswalk_method": method,
        "crosswalk_confidence": confidence,
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


def build_records(atlas_feeds, mdb_feeds, operators=()):
    """Unified feed records for the whole catalogue.

    Resolved in a cascade of narrowing confidence: url-exact identity first,
    then a gated same-host match over the residual. Returns ``(records,
    summary)``; every Atlas and MDB feed appears exactly once, a matched pair as
    one ``both`` record. ``summary`` also carries ``provisional_links`` — the
    ambiguous same-host candidates a human must adjudicate.
    """
    _require_unique_ids(atlas_feeds, "onestop_id", "atlas feed")
    _require_unique_ids(mdb_feeds, "mdb_id", "mdb feed")

    url_pairs = _url_exact_pairs(atlas_feeds, mdb_feeds)
    matched_onestop = {atlas_feed["onestop_id"] for atlas_feed, _ in url_pairs}
    matched_mdb = {mdb_feed["mdb_id"] for _, mdb_feed in url_pairs}

    atlas_residual = [
        feed for feed in atlas_feeds if feed["onestop_id"] not in matched_onestop
    ]
    mdb_residual = [feed for feed in mdb_feeds if feed["mdb_id"] not in matched_mdb]
    host_pairs, provisional, same_host_candidates = _same_host_matches(
        atlas_residual, mdb_residual, operators
    )
    matched_onestop.update(atlas_feed["onestop_id"] for atlas_feed, _ in host_pairs)
    matched_mdb.update(mdb_feed["mdb_id"] for _, mdb_feed in host_pairs)

    records = [
        _both_record(atlas_feed, mdb_feed, method="url_exact", confidence=1.0)
        for atlas_feed, mdb_feed in url_pairs
    ]
    records.extend(
        _both_record(
            atlas_feed, mdb_feed, method="same_host", confidence=SAME_HOST_CONFIDENCE
        )
        for atlas_feed, mdb_feed in host_pairs
    )
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
    by_method = {"url_exact": 0, "same_host": 0, "none": 0}
    for record in records:
        by_source[record["source"]] += 1
        by_method[record["crosswalk_method"]] += 1
    summary = {
        "feeds": len(records),
        "feeds_by_source": by_source,
        "crosswalk_by_method": by_method,
        "url_exact_pairs": len(url_pairs),
        "same_host_candidates": same_host_candidates,
        "same_host_pairs": len(host_pairs),
        "provisional_links": provisional,
    }
    return records, summary


def crosswalk(cache_dir):
    """Run the crosswalk stage, publishing a ``feeds.json`` generation."""
    atlas_feeds, atlas_operators = _read_atlas(cache_dir)
    mdb_feeds = _read_feeds(cache_dir, "mdb.json", "mdb_feeds.jsonl")
    records, summary = build_records(atlas_feeds, mdb_feeds, atlas_operators)
    if not records:
        raise CrosswalkError("crosswalk produced no feeds")

    # The provisional links are their own artifact, with only their count in the
    # manifest, so the pointer stays small however many accumulate.
    provisional = summary.pop("provisional_links")
    manifest = {"source": "crosswalk", **summary, "provisional_links": len(provisional)}

    out = cache_dir / "crosswalk"
    # Reach the store through `cache_dir` so a symlink at the cache root cannot
    # redirect the publish; reads go through store.resolve, which guards it too.
    directory = store.open_subdir(cache_dir, "crosswalk")
    try:
        with store.exclusive_writer(directory):
            return store.publish(
                out,
                FEEDS_POINTER,
                {
                    FEEDS_ARTIFACT: store.jsonl_chunks(records),
                    PROVISIONAL_ARTIFACT: store.jsonl_chunks(provisional),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
