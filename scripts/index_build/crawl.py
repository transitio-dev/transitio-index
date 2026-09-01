"""Crawl stage: fetch each crawlable feed's GTFS members into the build cache.

For every resolved feed that is crawlable, the stage fetches the members later
stages read — ``agency.txt``, ``routes.txt``, ``stops.txt``, ``trips.txt`` and
the complete ``stop_times.txt`` — into ``cache/crawl/<feed dir>/`` alongside a
``state.json`` provenance record (URL, validators, digests, members, time).

Feeds large enough to pay for it (past the size threshold, on a server that
honours ranges and offers a strong validator) are read member-by-member through
:mod:`index_build.ziprange`; everything else — and any range oddity — downloads
whole and extracts. Members are written to disk one at a time, never
accumulated, so several large members cannot compound in memory. Re-runs are
cheap: a feed whose validators match the stored state — and whose cached
members verify against their recorded digests — is skipped, except when
``cache/recrawl_requests.jsonl`` names it, which bypasses the skip so a
requested complete read cannot be starved by an unchanged ETag. One feed's
failure of any kind is logged, never fatal to the run.

``crawl_log.jsonl`` records, per feed, the method taken, the bytes fetched, the
bytes a range read saved and the reason any fallback happened — the plan's
first-run instrumentation for judging whether the range machinery pays.

The stop_times predicate: the complete ``stop_times.txt`` is read for every
feed except one whose routes are all settled by a geography-free tier rule AND
whose stops sit in one country and exactly one city — answered against the
boundary memo. Any doubt (no memo coverage, unparsable rows, a stop matching
no city) reads the member: the safe direction. A recrawl request always reads
it.
"""

import csv
import datetime
import hashlib
import io
import json
import math
import os
import tempfile
import zipfile
import zlib

from index_build import fetch, store, ziprange

MEMBERS = (
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
)

STOP_TIMES = "stop_times.txt"
CHEAP_MEMBERS = tuple(name for name in MEMBERS if name != STOP_TIMES)

# Stop clusters become grid cells of this size, padded into lookup boxes.
GRID_DEG = 0.2
PAD_DEG = 0.05

LOG_FILE = "crawl_log.jsonl"
STATE_FILE = "state.json"
ARCHIVE_FILE = "feed.zip"

# Ceiling on one extracted member from a whole-download archive; mirrors the
# ziprange member ceiling so both paths agree.
MAX_MEMBER_BYTES = ziprange.MAX_MEMBER_BYTES


def _feed_url(feed):
    """The URL to crawl: the Atlas static feed, else the MDB direct download."""
    atlas = feed.get("atlas") or {}
    mdb = feed.get("mdb") or {}
    return ((atlas.get("urls") or {}).get("static_current")) or (
        (mdb.get("urls") or {}).get("direct_download")
    )


def _dir_name(feed_id):
    """The digest-keyed cache directory for the feed.

    Paths never key on the id itself: Onestop ids are Unicode, can exceed a
    filesystem's byte limit while passing a character count, and can collide
    as filenames under normalisation. ``state.json`` carries the real id. The
    digest is kept whole — ids are upstream-controlled, and a truncated hash
    would make chosen collisions feasible.
    """
    return "id-" + hashlib.sha256(feed_id.encode("utf-8")).hexdigest()


def stop_coordinates(source):
    """``(points, dropped)`` parsed from ``stops.txt`` bytes or a binary file.

    ``points`` are the parseable ``(lon, lat)`` pairs; ``dropped`` counts rows
    refused for a malformed or out-of-range coordinate — range-checked, not
    just finite, because a huge value would overflow the grid-cell arithmetic.
    Evidence consumers use the points; the skip predicate refuses any drop.
    """
    points = []
    dropped = 0
    stream = io.BytesIO(source) if isinstance(source, (bytes, bytearray)) else source
    text = io.TextIOWrapper(stream, encoding="utf-8-sig", errors="replace")
    for row in csv.DictReader(text):
        try:
            x = float(row.get("stop_lon") or "")
            y = float(row.get("stop_lat") or "")
        except ValueError:
            dropped += 1
            continue
        if -180.0 <= x <= 180.0 and -90.0 <= y <= 90.0:
            points.append((x, y))
        else:
            dropped += 1
    return points, dropped


def cluster_boxes(points):
    """Padded grid-cell boxes covering the points, one box per occupied cell."""
    cells = {(math.floor(x / GRID_DEG), math.floor(y / GRID_DEG)) for x, y in points}
    return [
        (
            cx * GRID_DEG - PAD_DEG,
            cy * GRID_DEG - PAD_DEG,
            (cx + 1) * GRID_DEG + PAD_DEG,
            (cy + 1) * GRID_DEG + PAD_DEG,
        )
        for cx, cy in sorted(cells)
    ]


def _fixed_tier(route_type):
    """Whether the route type settles a tier with no route geography.

    The geography-free rules: tram/subway (rule 3) and the fixed-tier half of
    rule 2 — coach 200-209, urban rail 400-405, bus 700-716, trolleybus 800s,
    tram 900s.
    """
    return (
        route_type in (0, 1)
        or 200 <= route_type <= 209
        or 400 <= route_type <= 405
        or 700 <= route_type <= 716
        or 800 <= route_type <= 999
    )


def _skip_stop_times(feed_dir, digests, lookup, force):
    """Whether the complete stop_times read may be skipped; ``(skip, reason)``.

    The plan's predicate, all three required: (a) every route settled by a
    geography-free tier rule, (b) every stop in a single country, (c) every
    stop in exactly one distinct city — most-specific city-kind division,
    against the boundary memo's full pinned geometry. ``reason`` says why the
    member is read when it is; any doubt or error reads it. Only members the
    CURRENT fetch wrote (``digests``) count as evidence — a member the new
    archive dropped must not leave a stale file deciding the skip.
    """
    if force:
        return False, "recrawl requested"
    if lookup is None:
        return False, "no boundary lookup"
    if "routes.txt" not in digests or "stops.txt" not in digests:
        return False, "routes or stops member missing"
    try:
        routes_path = feed_dir.path / "routes.txt"
        stops_path = feed_dir.path / "stops.txt"
        rows = list(
            csv.DictReader(
                io.TextIOWrapper(
                    io.BytesIO(routes_path.read_bytes()),
                    encoding="utf-8-sig",
                    errors="replace",
                )
            )
        )
        if not rows:
            return False, "no routes"
        for row in rows:
            value = (row.get("route_type") or "").strip()
            if not value.isdigit() or not _fixed_tier(int(value)):
                return False, "route types need geography"
        with open(stops_path, "rb") as opened:
            points, dropped = stop_coordinates(opened)
        if dropped:
            # A whole-feed claim cannot rest on the parseable subset.
            return False, "unparsable stop rows"
        if not points:
            return False, "no parseable stops"
        lookup.ensure(cluster_boxes(points))
        countries = set()
        cities = set()
        for x, y in points:
            found = lookup.divisions_at(x, y)
            stop_countries = {r.get("country") for r in found if r.get("country")}
            if not stop_countries:
                return False, "a stop matches no division"
            countries.update(stop_countries)
            if len(countries) > 1:
                return False, "stops span countries"
            stop_cities = {
                r.get("overture_id") for r in found if r.get("kind") == "city"
            }
            if not stop_cities:
                return False, "a stop matches no city"
            if len(stop_cities) > 1:
                # Ambiguity is itself disqualifying, not a tie to break.
                return False, "a stop matches several cities"
            cities.update(stop_cities)
            if len(cities) > 1:
                return False, "stops span cities"
        return True, None
    except Exception as error:
        # The predicate is an optimisation gate; any failure inside it means
        # the full read, never a failed feed.
        return False, f"predicate error: {error}"


def _cache_reusable(feed_dir, state, url, force, lookup):
    """Whether the cached members may stand in for a fetch.

    The digests must verify against the state (the commit point), the URL
    must be the one the state was recorded against, and a cached whole-feed
    stop_times skip is re-judged against the CURRENT boundary memo and rules
    — a skip that no longer holds must refetch, not be reused via 304.
    """
    if state.get("url") != url or force or not _members_intact(feed_dir, state):
        return False
    if (state.get("stop_times") or {}).get("state") == "skipped":
        still_skipped, _ = _skip_stop_times(
            feed_dir, state.get("member_sha256") or {}, lookup, force
        )
        return still_skipped
    return True


def _read_state(directory):
    try:
        state = json.loads(store.read_text(directory, STATE_FILE))
    except (store.StoreError, ValueError):
        return {}
    # A corrupt file must read as "no usable state", never abort the crawl.
    return state if isinstance(state, dict) else {}


def _write_state(directory, state):
    store.write_file(
        directory, STATE_FILE, lambda: [json.dumps(state, indent=2, sort_keys=True)]
    )


def _members_intact(feed_dir, state):
    """Whether every member state records is on disk, regular, and unchanged.

    The digests are what make a skip safe: names alone would bless corrupted,
    truncated or symlinked files, and the 304 path reuses exactly this cache.
    """
    names = state.get("members") or []
    digests = state.get("member_sha256") or {}
    # Persisted state never chooses paths: names outside the fixed member set
    # (or a malformed list) mean a corrupt state, not a valid cache.
    if not isinstance(names, list) or not all(name in MEMBERS for name in names):
        return False
    if not names or not isinstance(digests, dict) or set(digests) != set(names):
        return False
    for name in names:
        path = feed_dir.path / name
        if not path.is_file():
            return False
        try:
            handle = store.open_nofollow(path)
        except OSError:
            return False
        digest = hashlib.sha256()
        with os.fdopen(handle, "rb") as opened:
            for chunk in iter(lambda: opened.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != digests[name]:
            return False
    return True


def _recrawl_ids(cache_dir):
    """The feed ids later stages asked to re-read completely, if any."""
    path = cache_dir / "recrawl_requests.jsonl"
    try:
        handle = store.open_nofollow(path)
    except OSError:
        return frozenset()
    with os.fdopen(handle, "rb") as opened:
        records = store.parse_jsonl(opened.read())
    return frozenset(
        r["feed_id"]
        for r in records
        if isinstance(r, dict) and isinstance(r.get("feed_id"), str) and r["feed_id"]
    )


def _range_validator(probe):
    """A validator strong enough to pin range reads, or None.

    Only a strong ETag qualifies: a weak ETag (``W/...``) permits
    byte-different representations, and a Last-Modified timestamp can stay
    unchanged across several representations within its one-second
    granularity — either could let multi-request reads assemble members from
    different snapshots. Last-Modified still serves the conditional-skip
    paths; it just never pins ranges.
    """
    etag = probe.get("etag")
    if etag and not etag.startswith("W/"):
        return etag
    return None


def _prune_members(feed_dir, kept):
    """Remove member files a newer archive no longer carries.

    ``state.json`` is written after the member writes and is the commit point:
    a run that dies mid-write leaves state recording the previous members, and
    the next run's intact-check refetches rather than trusts a mixed directory.
    """
    for name in MEMBERS:
        if name not in kept and (feed_dir.path / name).is_file():
            store.unlink(feed_dir, name)


def _write_ranged(fetcher, url, probe, feed_dir, decide):
    """Write members via range reads, one at a time; return their digests.

    The cheap members land first; ``decide`` then rules on the complete
    ``stop_times.txt`` read from what is on disk.
    """
    read = fetcher.range_reader(url, validator=_range_validator(probe))
    directory = ziprange.central_directory(read, probe["size"])
    digests = {}

    def write(name):
        entry = directory.get(name)
        if entry is None:
            return
        data = ziprange.read_member(read, entry)
        store.write_bytes(feed_dir, name, data)
        digests[name] = hashlib.sha256(data).hexdigest()

    for name in CHEAP_MEMBERS:
        write(name)
    skipped, reason = decide(feed_dir, digests)
    if not skipped:
        write(STOP_TIMES)
    return digests, skipped, reason


def _extract_members(feed_dir, decide):
    """Extract the members from the downloaded archive, streamed and bounded.

    ``zipfile`` handles what ziprange deliberately refuses (ZIP64, data
    descriptors); each member is capped and streamed straight to its file, so
    members never accumulate in memory. The cheap members land first; ``decide``
    then rules on extracting ``stop_times.txt``. Returns the digests.
    """
    digests = {}
    with zipfile.ZipFile(feed_dir.path / ARCHIVE_FILE) as archive:

        def extract(name):
            try:
                info = archive.getinfo(name)
            except KeyError:
                return
            if info.file_size > MAX_MEMBER_BYTES:
                raise fetch.FetchError(
                    f"{name}: {info.file_size} bytes is over the member ceiling"
                )
            handle, partial = store.create_temporary(feed_dir)
            try:
                digest = hashlib.sha256()
                got = 0
                with os.fdopen(handle, "wb") as opened_file:
                    handle = None
                    with archive.open(info) as opened:
                        while True:
                            chunk = opened.read(1024 * 1024)
                            if not chunk:
                                break
                            got += len(chunk)
                            if got > info.file_size:
                                raise fetch.FetchError(f"{name}: longer than declared")
                            digest.update(chunk)
                            opened_file.write(chunk)
                feed_dir.replace(partial, name)
                digests[name] = digest.hexdigest()
            finally:
                if handle is not None:
                    os.close(handle)
                store.unlink(feed_dir, partial)

        for name in CHEAP_MEMBERS:
            extract(name)
        skipped, reason = decide(feed_dir, digests)
        if not skipped:
            extract(STOP_TIMES)
    return digests, skipped, reason


def _crawl_one(fetcher, cache_dir, feed, *, force, range_threshold, lookup):
    """Crawl one feed; returns its log record (never raises)."""
    feed_id = feed["feed_id"]
    url = _feed_url(feed)
    record = {
        "feed_id": feed_id,
        "url": url,
        "directory": _dir_name(feed_id),
        "method": None,
        "bytes_fetched": 0,
        "bytes_saved": 0,
        "fallback_reason": None,
        "members": [],
        "stop_times": None,
        "stop_times_reason": None,
    }
    if not url:
        record["method"] = "skipped"
        record["fallback_reason"] = "no download URL"
        return record

    feed_dir = None
    fetched_before = fetcher.bytes_fetched

    def decide(directory, digests):
        return _skip_stop_times(directory, digests, lookup, force)

    try:
        feed_dir = store.open_subdir(cache_dir / "crawl", record["directory"])
        state = _read_state(feed_dir)
        probe = None
        fallback_reason = None
        try:
            probe = fetcher.head(url)
        except fetch.FetchError as error:
            fallback_reason = f"HEAD failed: {error}"

        if probe is not None and not force:
            # Strict: the URL must be the one the state was recorded against,
            # and the ETag decides when both sides have one — a changed ETag
            # with an unchanged coarse Last-Modified is a change.
            if probe.get("etag") and state.get("etag"):
                unchanged = probe["etag"] == state["etag"]
            elif probe.get("last_modified") and state.get("last_modified"):
                unchanged = probe["last_modified"] == state["last_modified"]
            else:
                unchanged = False
            unchanged = unchanged and state.get("url") == url
            if unchanged and _cache_reusable(feed_dir, state, url, force, lookup):
                record["method"] = "not_modified"
                record["members"] = sorted(state.get("members") or [])
                record["stop_times"] = (state.get("stop_times") or {}).get("state")
                return record

        digests = None
        # Range reads span several requests, so they are only taken when a
        # strong validator can pin them all to one representation.
        ranged = (
            probe is not None
            and probe.get("size")
            and probe["size"] >= range_threshold
            and probe.get("accept_ranges")
            and _range_validator(probe)
        )
        if ranged:
            try:
                digests, skipped, skip_reason = _write_ranged(
                    fetcher, url, probe, feed_dir, decide
                )
                record["method"] = "range"
                record["bytes_saved"] = probe["size"] - (
                    fetcher.bytes_fetched - fetched_before
                )
                validators = {
                    "etag": probe.get("etag"),
                    "last_modified": probe.get("last_modified"),
                }
            except (fetch.FetchError, ziprange.RangeReadError) as error:
                fallback_reason = str(error)
                digests = None
        elif probe is not None:
            if not probe.get("size") or probe["size"] < range_threshold:
                fallback_reason = "below the range threshold"
            elif not probe.get("accept_ranges"):
                fallback_reason = "no range support"
            else:
                fallback_reason = "no validator to pin range reads"

        if digests is None:
            # Validators only apply to the URL they were recorded against, and
            # a 304 reuses the cache verbatim, so it may only be requested when
            # the cache verifies against its recorded digests AND any cached
            # skip decision still holds.
            usable = _cache_reusable(feed_dir, state, url, force, lookup)
            conditional = state if usable else {}
            outcome = fetcher.download(
                url,
                feed_dir,
                ARCHIVE_FILE,
                etag=conditional.get("etag"),
                last_modified=conditional.get("last_modified"),
            )
            if outcome["status"] == "not_modified":
                record["method"] = "not_modified"
                record["members"] = sorted(state.get("members") or [])
                record["stop_times"] = (state.get("stop_times") or {}).get("state")
                return record
            try:
                digests, skipped, skip_reason = _extract_members(feed_dir, decide)
            finally:
                # Never leave the archive behind, extraction failures included.
                store.unlink(feed_dir, ARCHIVE_FILE)
            record["method"] = "download"
            record["archive_sha256"] = outcome["sha256"]
            validators = {
                "etag": outcome.get("etag"),
                "last_modified": outcome.get("last_modified"),
            }

        _prune_members(feed_dir, digests)
        record["members"] = sorted(digests)
        record["fallback_reason"] = fallback_reason
        # "complete" must mean the member exists AND was read; an archive
        # that simply has none is "absent", never a false completeness claim.
        record["stop_times"] = (
            "skipped" if skipped else "complete" if STOP_TIMES in digests else "absent"
        )
        record["stop_times_reason"] = skip_reason
        _write_state(
            feed_dir,
            {
                "feed_id": feed_id,
                "url": url,
                "etag": validators.get("etag"),
                "last_modified": validators.get("last_modified"),
                # Whole downloads digest the archive bytes that arrived; a
                # range read has no archive bytes, so its provenance is the
                # per-member digests, recorded for both paths.
                "archive_sha256": record.get("archive_sha256"),
                "member_sha256": digests,
                "members": sorted(digests),
                # Whether the complete stop_times read was withheld by the
                # predicate, so later stages can tell "skipped" from "absent"
                # and a recrawl request knows what to restore.
                "stop_times": {
                    "state": record["stop_times"],
                    "reason": skip_reason,
                },
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            },
        )
        return record
    except (
        fetch.FetchError,
        ziprange.RangeReadError,
        zipfile.BadZipFile,
        OSError,
        # RuntimeError covers zipfile's encrypted-member errors, its
        # NotImplementedError for unsupported compression, and the store's
        # errors; ValueError/LookupError/TypeError/zlib.error cover malformed
        # archives and state — one bad feed must never abort the crawl.
        RuntimeError,
        ValueError,
        LookupError,
        TypeError,
        zlib.error,
        # A member below the byte ceiling can still exhaust memory when
        # buffered; that is this feed's failure, never the run's.
        MemoryError,
    ) as error:
        record["method"] = "failed"
        record["fallback_reason"] = str(error)
        return record
    finally:
        record["bytes_fetched"] = fetcher.bytes_fetched - fetched_before
        if feed_dir is not None:
            feed_dir.close()


def crawl(
    cache_dir, *, fetcher=None, range_threshold=fetch.RANGE_THRESHOLD, lookup=None
):
    """Crawl every crawlable resolved feed. Returns the run summary.

    Reads ``feeds_resolved``, crawls each crawlable feed into
    ``cache/crawl/<feed dir>/``, writes ``crawl_log.jsonl`` and returns the
    summary counts. ``fetcher`` and ``range_threshold`` are injectable for
    tests. ``lookup`` is the boundary lookup the stop_times predicate answers
    against; by default the memoized one under the cache is opened read-only —
    with no memo (or without its dependencies) every feed reads the complete
    ``stop_times.txt``, the safe direction.
    """
    if fetcher is None:
        fetcher = fetch.Fetcher()
    feeds, resolve_manifest = store.read_jsonl(
        cache_dir / "resolve", "feeds_resolved.json", "feeds_resolved.jsonl"
    )
    opened_lookup = None
    log = []
    try:
        if lookup is None:
            try:
                from index_build import boundaries

                opened_lookup = boundaries.BoundaryLookup(cache_dir)
                lookup = opened_lookup
            except Exception:
                lookup = None
        directory = store.open_subdir(cache_dir, "crawl")
        try:
            with store.exclusive_writer(directory):
                # Read under the same lock appenders and the clearing rewrite
                # hold, so a compliant concurrent writer can neither expose a
                # partial file nor have a fresh request missed by this run.
                recrawl = _recrawl_ids(cache_dir)
                # Requests written under a feed's previous id (an identity
                # override keeps it as an alias) must still force and clear.
                canonical = {}
                for feed in feeds:
                    for key in [feed["feed_id"], *(feed.get("aliases") or [])]:
                        canonical[key] = feed["feed_id"]
                forced = {canonical.get(rid, rid) for rid in recrawl}
                for feed in feeds:
                    if not feed.get("crawlable"):
                        continue
                    log.append(
                        _crawl_one(
                            fetcher,
                            cache_dir,
                            feed,
                            force=feed["feed_id"] in forced,
                            range_threshold=range_threshold,
                            lookup=lookup,
                        )
                    )
                store.write_file(
                    directory,
                    LOG_FILE,
                    lambda: (json.dumps(record) + "\n" for record in log),
                )
                cleared = 0
                if recrawl:
                    # A request is cleared only by the complete read that satisfied
                    # it; everything else survives the run for the next build.
                    # "absent" fulfils too: a forced read that found no
                    # member has read everything there is to read.
                    fulfilled = {
                        record["feed_id"]
                        for record in log
                        if record["method"] in ("range", "download")
                        and record.get("stop_times") in ("complete", "absent")
                    }
                    cleared = sum(
                        1 for rid in recrawl if canonical.get(rid, rid) in fulfilled
                    )
                    if cleared:
                        path = cache_dir / "recrawl_requests.jsonl"
                        rows = None
                        try:
                            handle = store.open_nofollow(path)
                        except OSError:
                            # Unreadable or symlinked now: leave the file
                            # alone rather than rewrite from a snapshot we
                            # could not take.
                            handle = None
                        if handle is not None:
                            with os.fdopen(handle, "rb") as opened:
                                rows = store.parse_jsonl(opened.read())
                        if rows is not None:
                            # Non-object rows survive untouched: only rows
                            # whose request was satisfied are removed.
                            remaining = [
                                row
                                for row in rows
                                if not isinstance(row, dict)
                                or canonical.get(row.get("feed_id"), row.get("feed_id"))
                                not in fulfilled
                            ]
                            # A random O_EXCL temporary: a predictable name
                            # could be a pre-planted symlink. Appenders must
                            # hold this crawl lock.
                            handle, partial = tempfile.mkstemp(
                                dir=cache_dir, suffix=".recrawl"
                            )
                            try:
                                with os.fdopen(handle, "w") as opened:
                                    opened.write(
                                        "".join(
                                            json.dumps(row) + "\n" for row in remaining
                                        )
                                    )
                                os.replace(partial, path)
                            except BaseException:
                                os.unlink(partial)
                                raise
        finally:
            directory.close()
    finally:
        if opened_lookup is not None:
            opened_lookup.close()

    methods = {}
    for record in log:
        methods[record["method"]] = methods.get(record["method"], 0) + 1
    return {
        "source": "crawl",
        "sources": resolve_manifest.get("sources"),
        "feeds_crawlable": len(log),
        "by_method": methods,
        "stop_times_skipped": sum(1 for r in log if r.get("stop_times") == "skipped"),
        "bytes_fetched": sum(r["bytes_fetched"] for r in log),
        "bytes_saved": sum(r["bytes_saved"] for r in log),
        "recrawl_requested": len(recrawl),
        "recrawl_cleared": cleared,
        "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
