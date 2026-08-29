"""Crosswalk stage: resolve the same feed across the ingest catalogues.

Reads the raw Atlas, Mobility Database and GBFS generations and writes one
``feeds.jsonl`` of unified feed records, each with a stable ``feed_id`` and
the crosswalk method that produced it. Identity is resolved in a cascade of
narrowing confidence: url-exact (a GTFS download URL byte-identical in both
catalogues), then a gated same-host match (feeds sharing a download host whose
names agree), then geohash-confirm (a same-host candidate whose Onestop-ID
geohash meets the MDB centroid geohash). GBFS ``systems.csv`` systems are linked
to their Atlas feed by auto-discovery URL, or minted ``f-gbfs-*`` where no Atlas
feed carries them. Ambiguous same-host candidates are not merged — they go to a
``provisional_links`` report for a human to adjudicate. GTFS-RT feeds are not
merged (their URL match is one-to-many) but are given a ``static_feed_id`` — the
static feed they belong to, declared on the operator or inferred — which a later
stage uses to propagate that feed's places.

Identity follows decision L: ``feed_id`` is the Onestop ID where one exists,
else a minted ``f-mdb-<mdb_id>``. A record keeps the contributing source rows
verbatim under ``atlas`` / ``mdb`` so nothing downstream must re-read raw.
"""

import collections
import itertools
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
ATLAS_REALTIME_URLS = (
    "realtime_trip_updates",
    "realtime_vehicle_positions",
    "realtime_alerts",
)


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


def _unique_url_index(records, spec, url_of):
    """Records of ``spec`` keyed by URL, keeping only URLs unique in the source.

    A URL shared by several records cannot resolve a single identity, so it is
    dropped from the index and left unmatched rather than resolved to an
    arbitrary one of them.
    """
    by_url = {}
    dropped = set()
    for record in records:
        if record["spec"] != spec:
            continue
        url = _clean_url(url_of(record))
        if url is None:
            continue
        if url in by_url:
            dropped.add(url)
        else:
            by_url[url] = record
    for url in dropped:
        del by_url[url]
    return by_url


def _unique_url_pairs(left, right, spec, left_url, right_url):
    """``(left, right)`` pairs of ``spec`` whose URL is identical and unique.

    Uniqueness on both sides makes each pair a clean one-to-one identity;
    anything ambiguous stays unmatched.
    """
    left_index = _unique_url_index(left, spec, left_url)
    right_index = _unique_url_index(right, spec, right_url)
    pairs = []
    for url, left_record in left_index.items():
        right_record = right_index.get(url)
        if right_record is not None:
            pairs.append((left_record, right_record))
    return pairs


def _url_exact_pairs(atlas_feeds, mdb_feeds):
    return _unique_url_pairs(
        atlas_feeds,
        mdb_feeds,
        "gtfs",
        lambda feed: (feed.get("urls") or {}).get(ATLAS_STATIC_URL),
        lambda feed: (feed.get("urls") or {}).get(MDB_DOWNLOAD_URL),
    )


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


# Geohash-confirm corroborates a same-host candidate geographically: an Atlas
# Onestop ID embeds a geohash (``f-<geohash>-<name>``), compared with the
# geohash of an MDB feed's bounding-box centroid. It resolves identity only at
# >= 4 characters (~20 km), only within a shared host, and at a lower confidence
# than a name match.
GEOHASH_PRECISION = 4
GEOHASH_CONFIDENCE = 0.6

_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_ONESTOP_GEOHASH = re.compile(r"\Af-([0-9b-hjkmnp-z]+)-")


def _geohash_encode(lat, lon, precision):
    """The geohash of ``(lat, lon)`` to ``precision`` characters."""
    lat_low, lat_high = -90.0, 90.0
    lon_low, lon_high = -180.0, 180.0
    geohash = []
    value = 0
    bits = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lon_low + lon_high) / 2
            if lon >= mid:
                value = (value << 1) | 1
                lon_low = mid
            else:
                value <<= 1
                lon_high = mid
        else:
            mid = (lat_low + lat_high) / 2
            if lat >= mid:
                value = (value << 1) | 1
                lat_low = mid
            else:
                value <<= 1
                lat_high = mid
        even = not even
        bits += 1
        if bits == 5:
            geohash.append(_GEOHASH_BASE32[value])
            value = 0
            bits = 0
    return "".join(geohash)


def _onestop_geohash(onestop_id):
    """The Onestop ID's geohash prefix at ``GEOHASH_PRECISION``, or None."""
    match = _ONESTOP_GEOHASH.match(onestop_id)
    if match is None:
        return None
    geohash = match.group(1)
    return geohash[:GEOHASH_PRECISION] if len(geohash) >= GEOHASH_PRECISION else None


def _mdb_centroid_geohash(feed):
    """The geohash of an MDB feed's bounding-box centroid, or None.

    A box crossing the antimeridian (``min_lon > max_lon``) has no meaningful
    centroid from a plain average, so it yields no geohash rather than a point
    in the wrong hemisphere.
    """
    box = feed.get("bounding_box")
    if not box or box["min_lon"] > box["max_lon"]:
        return None
    lat = (box["min_lat"] + box["max_lat"]) / 2
    lon = (box["min_lon"] + box["max_lon"]) / 2
    return _geohash_encode(lat, lon, GEOHASH_PRECISION)


def _gtfs_by_host(feeds, url_key):
    by_host = collections.defaultdict(list)
    for feed in feeds:
        if feed["spec"] != "gtfs":
            continue
        host = _host((feed.get("urls") or {}).get(url_key))
        if host is not None:
            by_host[host].append(feed)
    return by_host


def _clean_matches(atlas_feeds, mdb_feeds, agree):
    """Clean one-to-one matches under ``agree(atlas_feed, mdb_feed) -> bool``.

    Returns ``(pairs, contested)``: mutual one-to-one matches, and the Atlas
    feeds whose agreement was ambiguous — each paired with the MDB feeds it
    agreed with — for the caller to record or discard.
    """
    agreements = {}
    mdb_hit_count = collections.Counter()
    for atlas_feed in atlas_feeds:
        agreeing = [feed for feed in mdb_feeds if agree(atlas_feed, feed)]
        agreements[atlas_feed["onestop_id"]] = agreeing
        for mdb_feed in agreeing:
            mdb_hit_count[mdb_feed["mdb_id"]] += 1
    pairs = []
    contested = []
    for atlas_feed in atlas_feeds:
        agreeing = agreements[atlas_feed["onestop_id"]]
        if not agreeing:
            continue
        if len(agreeing) == 1 and mdb_hit_count[agreeing[0]["mdb_id"]] == 1:
            pairs.append((atlas_feed, agreeing[0]))
        else:
            contested.append((atlas_feed, agreeing))
    return pairs, contested


def _match_within_host_by_name(atlas_feeds, mdb_feeds, operator_names, host):
    """Name-agreement matches on one host, returning ``(pairs, provisional)``.

    A clean one-to-one name agreement is a match; a name agreeing with more than
    one feed on the host is ambiguous and recorded for review, not merged.
    """
    atlas_tokens = {
        feed["onestop_id"]: _atlas_name_token_sets(feed, operator_names)
        for feed in atlas_feeds
    }
    mdb_tokens = {feed["mdb_id"]: _mdb_name_token_sets(feed) for feed in mdb_feeds}

    def agree(atlas_feed, mdb_feed):
        return (
            _best_name_overlap(
                atlas_tokens[atlas_feed["onestop_id"]], mdb_tokens[mdb_feed["mdb_id"]]
            )
            >= SAME_HOST_MIN_OVERLAP
        )

    pairs, contested = _clean_matches(atlas_feeds, mdb_feeds, agree)
    provisional = [
        {
            "onestop_id": atlas_feed["onestop_id"],
            "mdb_id": mdb_feed["mdb_id"],
            "host": host,
            "name_overlap": round(
                _best_name_overlap(
                    atlas_tokens[atlas_feed["onestop_id"]],
                    mdb_tokens[mdb_feed["mdb_id"]],
                ),
                3,
            ),
        }
        for atlas_feed, agreeing in contested
        for mdb_feed in agreeing
    ]
    return pairs, provisional


def _match_within_host_by_geohash(atlas_feeds, mdb_feeds, host):
    """Geohash-confirm matches on one host, returning ``(pairs, provisional)``.

    An Atlas Onestop geohash equal to an MDB centroid geohash at
    ``GEOHASH_PRECISION`` is a match, one-to-one; a geohash agreeing with more
    than one feed is ambiguous and recorded for review, not merged. Weaker
    evidence than a name, so it runs only on feeds a name did not resolve.
    """
    atlas_geohash = {
        feed["onestop_id"]: _onestop_geohash(feed["onestop_id"]) for feed in atlas_feeds
    }
    mdb_geohash = {feed["mdb_id"]: _mdb_centroid_geohash(feed) for feed in mdb_feeds}

    def agree(atlas_feed, mdb_feed):
        atlas_gh = atlas_geohash[atlas_feed["onestop_id"]]
        return atlas_gh is not None and atlas_gh == mdb_geohash[mdb_feed["mdb_id"]]

    pairs, contested = _clean_matches(atlas_feeds, mdb_feeds, agree)
    provisional = [
        {
            "onestop_id": atlas_feed["onestop_id"],
            "mdb_id": mdb_feed["mdb_id"],
            "host": host,
            "geohash": atlas_geohash[atlas_feed["onestop_id"]],
        }
        for atlas_feed, agreeing in contested
        for mdb_feed in agreeing
    ]
    return pairs, provisional


def _same_host_matches(atlas_feeds, mdb_feeds, operators):
    """Resolve GTFS feeds sharing a download host.

    Returns ``(name_pairs, geohash_pairs, provisional, candidates)``: within
    each host, name agreement first, then geohash-confirm on the feeds a name
    did not resolve. ``candidates`` is the raw shared-host population both gates
    reduce; ``provisional`` holds the ambiguous name and geohash candidates for
    a human to adjudicate.
    """
    operator_names = _operator_names_by_feed(operators)
    atlas_by_host = _gtfs_by_host(atlas_feeds, ATLAS_STATIC_URL)
    mdb_by_host = _gtfs_by_host(mdb_feeds, MDB_DOWNLOAD_URL)
    name_pairs = []
    geohash_pairs = []
    provisional = []
    candidates = 0
    for host in sorted(atlas_by_host.keys() & mdb_by_host.keys()):
        atlas_on_host = atlas_by_host[host]
        mdb_on_host = mdb_by_host[host]
        candidates += len(atlas_on_host) + len(mdb_on_host)
        host_name_pairs, host_provisional = _match_within_host_by_name(
            atlas_on_host, mdb_on_host, operator_names, host
        )
        name_pairs.extend(host_name_pairs)
        provisional.extend(host_provisional)
        named_atlas = {atlas_feed["onestop_id"] for atlas_feed, _ in host_name_pairs}
        named_mdb = {mdb_feed["mdb_id"] for _, mdb_feed in host_name_pairs}
        host_geohash_pairs, host_geohash_provisional = _match_within_host_by_geohash(
            [f for f in atlas_on_host if f["onestop_id"] not in named_atlas],
            [f for f in mdb_on_host if f["mdb_id"] not in named_mdb],
            host,
        )
        geohash_pairs.extend(host_geohash_pairs)
        provisional.extend(host_geohash_provisional)
    # A geohash match can resolve a feed whose name was ambiguous; drop the now
    # stale provisional rows — on either endpoint, since a resolved MDB feed can
    # no longer pair with the other Atlas feeds that named it.
    resolved_onestop = {atlas_feed["onestop_id"] for atlas_feed, _ in geohash_pairs}
    resolved_mdb = {mdb_feed["mdb_id"] for _, mdb_feed in geohash_pairs}
    provisional = [
        link
        for link in provisional
        if link["onestop_id"] not in resolved_onestop
        and link["mdb_id"] not in resolved_mdb
    ]
    return name_pairs, geohash_pairs, provisional, candidates


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


# GBFS: a systems.csv system is the same feed as the Atlas GBFS feed advertising
# the same auto-discovery URL; a system no Atlas feed carries is minted f-gbfs-*.
GBFS_DISCOVERY_URL = "gbfs_auto_discovery"
SYSTEMS_CSV_CONFIDENCE = 1.0


def _gbfs_links(atlas_feeds, systems):
    """``(atlas_gbfs_feed, system)`` pairs sharing an auto-discovery URL.

    A GBFS system in ``systems.csv`` is the same feed as the Atlas GBFS feed
    that advertises the same auto-discovery URL; the match is exact and unique
    on both sides, like url-exact.
    """
    return _unique_url_pairs(
        atlas_feeds,
        systems,
        "gbfs",
        lambda feed: (feed.get("urls") or {}).get(GBFS_DISCOVERY_URL),
        lambda system: system.get("auto_discovery_url"),
    )


def _mint_gbfs(system, duplicate_ids):
    """A stable ``f-gbfs-*`` id for a system, or None if it cannot be unambiguous.

    System ids are not unique upstream, so a duplicated one is disambiguated by
    its country code. A duplicated id with no country code cannot be minted
    unambiguously and returns None; the caller refuses it (an orphan) or omits
    it (a linked feed's alias) rather than publish a colliding id.
    """
    system_id = system["system_id"]
    if system_id not in duplicate_ids:
        return f"f-gbfs-{system_id}"
    country_code = system.get("country_code")
    if not country_code:
        return None
    return f"f-gbfs-{system_id}-{country_code.lower()}"


def _gbfs_linked_record(atlas_feed, system, minted_alias):
    """An Atlas GBFS feed linked to its ``systems.csv`` system.

    Keyed on the Onestop ID like any Atlas feed; the system row is kept verbatim
    for the placement its ``Location`` + ``Country Code`` later drive. The minted
    ``f-gbfs-*`` id is kept in ``aliases`` so overrides filed against it still
    resolve — unless it is None (the system id was ambiguous) or equals the
    Onestop ID.
    """
    onestop_id = atlas_feed["onestop_id"]
    aliases = [minted_alias] if minted_alias and minted_alias != onestop_id else []
    return {
        "feed_id": onestop_id,
        "onestop_id": onestop_id,
        "mdb_id": None,
        "aliases": aliases,
        "id_minted": False,
        "source": "atlas",
        "spec": atlas_feed["spec"],
        "name": atlas_feed.get("name") or system.get("name"),
        "crosswalk_method": "systems_csv",
        "crosswalk_confidence": SYSTEMS_CSV_CONFIDENCE,
        "atlas": atlas_feed,
        "gbfs": system,
    }


def _gbfs_system_record(system, feed_id):
    """A ``systems.csv`` system no Atlas feed carries, on its minted id."""
    return {
        "feed_id": feed_id,
        "onestop_id": None,
        "mdb_id": None,
        "aliases": [],
        "id_minted": True,
        "source": "systems_csv",
        "spec": "gbfs",
        "name": system.get("name"),
        "crosswalk_method": "none",
        "crosswalk_confidence": 0.0,
        "gbfs": system,
    }


def _rt_hosts(feed):
    urls = feed.get("urls") or {}
    return {_host(urls.get(key)) for key in ATLAS_REALTIME_URLS} - {None}


def _static_link_graph(atlas_feeds, operators):
    """An undirected feed-association graph from the operator declarations.

    An operator inline on a feed associates that feed with each feed it lists;
    a top-level operator associates the feeds it lists with each other. Both
    kinds are how a static feed and its realtime companion are declared together
    upstream (the link is on the operator, never on the feed).
    """
    present = {feed["onestop_id"] for feed in atlas_feeds}
    graph = collections.defaultdict(set)

    def link(left, right):
        if left != right and left in present and right in present:
            graph[left].add(right)
            graph[right].add(left)

    for feed in atlas_feeds:
        for operator in feed.get("operators") or []:
            for feed_id in operator.get("associated_feed_ids") or []:
                link(feed["onestop_id"], feed_id)
    for operator in operators:
        listed = [
            feed_id
            for feed_id in operator.get("associated_feed_ids") or []
            if feed_id in present
        ]
        for left, right in itertools.combinations(listed, 2):
            link(left, right)
    return graph


def _static_link(rt_feed, graph, atlas_by_id, gtfs_by_file, gtfs_by_host):
    """A GTFS-RT feed's ``(static_feed_id, static_link_method)``.

    Declared first — a single associated static feed, or the one of several that
    shares the RT feed's DMFR — then inferred: a lone static feed in the same
    DMFR (``same_file``), or a single static feed sharing a realtime URL host
    (``same_host``); ``none`` when nothing resolves it to exactly one feed.
    """
    onestop_id = rt_feed["onestop_id"]
    source_file = rt_feed.get("source_file")
    candidates = sorted(
        feed_id
        for feed_id in graph.get(onestop_id, ())
        if atlas_by_id[feed_id]["spec"] == "gtfs"
    )
    if len(candidates) == 1:
        return candidates[0], "declared"
    if len(candidates) > 1:
        in_file = [
            c for c in candidates if atlas_by_id[c].get("source_file") == source_file
        ]
        if len(in_file) == 1:
            return in_file[0], "declared"
    else:
        file_statics = gtfs_by_file.get(source_file, [])
        if len(file_statics) == 1:
            return file_statics[0]["onestop_id"], "same_file"
        host_statics = {
            feed["onestop_id"]
            for host in _rt_hosts(rt_feed)
            for feed in gtfs_by_host.get(host, [])
        }
        if len(host_statics) == 1:
            return next(iter(host_statics)), "same_host"
    return None, "none"


def _apply_static_links(records, atlas_feeds, operators):
    """Stamp ``static_feed_id`` / ``static_link_method`` on every record.

    GTFS-RT feeds inherit their static feed's identity here; the propagation of
    that feed's places is a later stage. The link is computed from the Atlas
    operator declarations, so an MDB-only RT feed — which no operator claims —
    stays ``none``, and a non-RT feed carries the fields as null.
    """
    graph = _static_link_graph(atlas_feeds, operators)
    atlas_by_id = {feed["onestop_id"]: feed for feed in atlas_feeds}
    gtfs_feeds = [feed for feed in atlas_feeds if feed["spec"] == "gtfs"]
    gtfs_by_file = collections.defaultdict(list)
    for feed in gtfs_feeds:
        gtfs_by_file[feed.get("source_file")].append(feed)
    gtfs_by_host = collections.defaultdict(list)
    for feed in gtfs_feeds:
        host = _host((feed.get("urls") or {}).get(ATLAS_STATIC_URL))
        if host is not None:
            gtfs_by_host[host].append(feed)

    counts = collections.Counter()
    for record in records:
        if record["spec"] != "gtfs-rt":
            record["static_feed_id"] = None
            record["static_link_method"] = None
            continue
        atlas_rt = record.get("atlas")
        if atlas_rt is None:
            static_feed_id, method = None, "none"
        else:
            static_feed_id, method = _static_link(
                atlas_rt, graph, atlas_by_id, gtfs_by_file, gtfs_by_host
            )
        record["static_feed_id"] = static_feed_id
        record["static_link_method"] = method
        counts[method] += 1
    return counts


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


def build_records(atlas_feeds, mdb_feeds, operators=(), systems=()):
    """Unified feed records for the whole catalogue.

    Resolved in a cascade of narrowing confidence: url-exact, then a gated
    same-host match (name agreement, then geohash-confirm) over the residual;
    GBFS systems are linked to their Atlas feed by auto-discovery URL and minted
    ``f-gbfs-*`` where no Atlas feed carries them. Returns ``(records,
    summary)``; every feed appears exactly once, a matched pair as one ``both``
    record. ``summary`` also carries ``provisional_links`` — the ambiguous
    same-host candidates a human must adjudicate.
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
    name_pairs, geohash_pairs, provisional, same_host_candidates = _same_host_matches(
        atlas_residual, mdb_residual, operators
    )
    for atlas_feed, mdb_feed in (*name_pairs, *geohash_pairs):
        matched_onestop.add(atlas_feed["onestop_id"])
        matched_mdb.add(mdb_feed["mdb_id"])

    gbfs_pairs = _gbfs_links(atlas_feeds, systems)
    system_by_onestop = {feed["onestop_id"]: system for feed, system in gbfs_pairs}
    linked_systems = {id(system) for _, system in gbfs_pairs}
    orphan_systems = [system for system in systems if id(system) not in linked_systems]
    # Duplicate system ids across ALL systems drive the country-code suffix; a
    # minted id shared by more than one system is ambiguous and cannot serve as
    # an identity or alias for any of them.
    duplicate_ids = {
        system_id
        for system_id, count in collections.Counter(
            system["system_id"] for system in systems
        ).items()
        if count > 1
    }
    minted_counts = collections.Counter(
        minted
        for minted in (_mint_gbfs(system, duplicate_ids) for system in systems)
        if minted is not None
    )
    # An id is usable only if it is mintable, unique among systems, and does not
    # collide with an Atlas Onestop ID that some feed already carries.
    onestop_ids = {feed["onestop_id"] for feed in atlas_feeds}

    def _usable_mint(system):
        minted = _mint_gbfs(system, duplicate_ids)
        if minted is None or minted_counts[minted] != 1 or minted in onestop_ids:
            return None
        return minted

    def orphan_feed_id(system):
        minted = _usable_mint(system)
        if minted is None:
            raise CrosswalkError(
                f"gbfs system {system['system_id']!r}: no unambiguous id to mint"
            )
        return minted

    records = [
        _both_record(atlas_feed, mdb_feed, method="url_exact", confidence=1.0)
        for atlas_feed, mdb_feed in url_pairs
    ]
    records.extend(
        _both_record(a, m, method="same_host", confidence=SAME_HOST_CONFIDENCE)
        for a, m in name_pairs
    )
    records.extend(
        _both_record(a, m, method="geohash", confidence=GEOHASH_CONFIDENCE)
        for a, m in geohash_pairs
    )
    for feed in atlas_feeds:
        if feed["onestop_id"] in matched_onestop:
            continue
        system = system_by_onestop.get(feed["onestop_id"])
        records.append(
            _gbfs_linked_record(feed, system, _usable_mint(system))
            if system is not None
            else _atlas_record(feed)
        )
    records.extend(
        _mdb_record(feed) for feed in mdb_feeds if feed["mdb_id"] not in matched_mdb
    )
    records.extend(
        _gbfs_system_record(system, orphan_feed_id(system)) for system in orphan_systems
    )
    _require_unique_namespace(records)
    rt_static_links = _apply_static_links(records, atlas_feeds, operators)

    by_source = {"atlas": 0, "mdb": 0, "both": 0, "systems_csv": 0}
    by_method = {
        "url_exact": 0,
        "same_host": 0,
        "geohash": 0,
        "systems_csv": 0,
        "none": 0,
    }
    for record in records:
        by_source[record["source"]] += 1
        by_method[record["crosswalk_method"]] += 1
    summary = {
        "feeds": len(records),
        "feeds_by_source": by_source,
        "crosswalk_by_method": by_method,
        "url_exact_pairs": len(url_pairs),
        "same_host_candidates": same_host_candidates,
        "same_host_pairs": len(name_pairs),
        "geohash_pairs": len(geohash_pairs),
        "gbfs_linked": len(gbfs_pairs),
        "gbfs_minted": len(orphan_systems),
        "gbfs_system_id_collisions": sorted(duplicate_ids),
        "rt_static_links": dict(rt_static_links),
        "provisional_links": provisional,
    }
    return records, summary


def crosswalk(cache_dir):
    """Run the crosswalk stage, publishing a ``feeds.json`` generation."""
    atlas_feeds, atlas_operators = _read_atlas(cache_dir)
    mdb_feeds = _read_feeds(cache_dir, "mdb.json", "mdb_feeds.jsonl")
    systems = _read_feeds(cache_dir, "gbfs.json", "gbfs_systems.jsonl")
    records, summary = build_records(atlas_feeds, mdb_feeds, atlas_operators, systems)
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
