"""Crawl stage: fetch each crawlable feed's GTFS members into the build cache.

For every resolved feed that is crawlable, the stage fetches the members later
stages read — ``agency.txt``, ``routes.txt``, ``stops.txt``, ``trips.txt``,
``calendar.txt``, ``calendar_dates.txt`` and the complete ``stop_times.txt`` —
into ``cache/crawl/<feed dir>/`` alongside a ``state.json`` provenance record
(URL, validators, digests, members, the member set asked for, the archive's full
root-file manifest, time).

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

import concurrent.futures
import contextlib
import csv
import datetime
import hashlib
import io
import json
import math
import os
import re
import tempfile
import zipfile
import zlib

from index_build import fetch, store, ziprange

MEMBERS = (
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "stop_times.txt",
)

STOP_TIMES = "stop_times.txt"
CHEAP_MEMBERS = tuple(name for name in MEMBERS if name != STOP_TIMES)

# Stop clusters become grid cells of this size, padded into lookup boxes.
GRID_DEG = 0.2
PAD_DEG = 0.05

# The only directory shape the crawl produces; anything else in the log is
# refused rather than joined into a path.
_CRAWL_DIR = re.compile(r"\Aid-[0-9a-f]{64}\Z")

# What a corrupt crawled member can raise while being read as evidence; a
# programming defect is deliberately NOT in this tuple, so it surfaces.
MEMBER_ERRORS = (OSError, ValueError, csv.Error, UnicodeError, MemoryError)

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


def stop_rows(source):
    """``(rows, dropped)`` parsed from ``stops.txt`` bytes or a binary file.

    ``rows`` are ``(stop_id, lon, lat)`` for the parseable rows; ``dropped``
    counts rows refused for a malformed or out-of-range coordinate —
    range-checked, not just finite, because a huge value would overflow the
    grid-cell arithmetic. Evidence consumers use the rows; the skip predicate
    refuses any drop.
    """
    rows = []
    dropped = 0
    stream = io.BytesIO(source) if isinstance(source, (bytes, bytearray)) else source
    # Strict: replacing malformed bytes would let distinct invalid ids
    # collapse into one; a bad byte is a data error the caller handles.
    text = io.TextIOWrapper(stream, encoding="utf-8-sig", errors="strict")
    for row in csv.DictReader(text):
        try:
            x = float(row.get("stop_lon") or "")
            y = float(row.get("stop_lat") or "")
        except ValueError:
            dropped += 1
            continue
        if -180.0 <= x <= 180.0 and -90.0 <= y <= 90.0:
            # Ids verbatim: two legal ids differing only by padding must
            # never collapse into one.
            rows.append((row.get("stop_id") or "", x, y))
        else:
            dropped += 1
    return rows, dropped


def stop_coordinates(source):
    """``(points, dropped)``: the ``(lon, lat)`` pairs of :func:`stop_rows`."""
    rows, dropped = stop_rows(source)
    return [(x, y) for _, x, y in rows], dropped


@contextlib.contextmanager
def verified_member(feed_dir, state, name):
    """A crawled member opened for binary reading after digest verification.

    Yields the file positioned at its start, or None when the state records
    no digest for the member, the file is missing or symlinked, or its bytes
    do not match — ``state.json`` is the crawl's per-feed commit point, and a
    member a crash left newer or older than it must never be read as
    evidence. The digest streams the file, so no whole member is buffered.
    """
    digests = state.get("member_sha256")
    expected = digests.get(name) if isinstance(digests, dict) else None
    if not expected:
        yield None
        return
    try:
        handle = store.open_nofollow(feed_dir / name)
    except OSError:
        yield None
        return
    with os.fdopen(handle, "rb") as opened:
        digest = hashlib.sha256()
        for chunk in iter(lambda: opened.read(1024 * 1024), b""):
            digest.update(chunk)
        if digest.hexdigest() != expected:
            yield None
            return
        opened.seek(0)
        yield opened


def states_digest(cache_dir):
    """A digest over every crawled feed's WHOLE committed state — what a
    stage read, so a later stage can prove it read the same crawl; None
    when no crawl has ever committed a log. Every field counts: a state
    rewritten with the same retrieval time but other members, digests,
    validators or stop_times mode is a different crawl."""
    root = cache_dir / "crawl"
    if root.is_symlink() or not (root / LOG_FILE).exists():
        return None
    lines = sorted(
        json.dumps(state, sort_keys=True, ensure_ascii=False)
        for _, state in crawled_feeds(cache_dir)
    )
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


@contextlib.contextmanager
def reading(cache_dir):
    """Hold the crawl root's writer lock while crawl artifacts are read as
    evidence, so a concurrent crawl cannot rewrite states between a stage's
    reads (or between its reads and the digest it records). The directory
    is created when absent so the lock exists BEFORE any first crawl could:
    a crawl that starts while a stage holds it waits for the stage."""
    if (cache_dir / "crawl").is_symlink():
        raise store.StoreError("the crawl directory is a symlink")
    directory = store.open_subdir(cache_dir, "crawl")
    try:
        with store.exclusive_writer(directory):
            yield
    finally:
        directory.close()


def crawled_feeds(cache_dir):
    """``(feed_dir, state)`` for crawled feeds whose state records stops.

    The per-feed ``state.json`` is the crawl's commit point, so it — not the
    run-level log alone — decides what may be read as evidence. Every path
    component is refused when symlinked, and one corrupt log line or state
    skips one feed, never the caller's run.
    """
    crawl_root = cache_dir / "crawl"
    if crawl_root.is_symlink() or not crawl_root.is_dir():
        return []
    log_path = crawl_root / LOG_FILE
    try:
        handle = store.open_nofollow(log_path)
    except OSError:
        return []
    with os.fdopen(handle, "rb") as opened:
        raw = opened.read()
    found = []
    for line in raw.split(b"\n"):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            # One corrupt log line skips one feed, never every consumer.
            continue
        if not isinstance(record, dict):
            continue
        name = record.get("directory")
        if not isinstance(name, str) or not _CRAWL_DIR.fullmatch(name):
            continue
        feed_dir = crawl_root / name
        if feed_dir.is_symlink() or not feed_dir.is_dir():
            continue
        try:
            handle = store.open_nofollow(feed_dir / STATE_FILE)
            with os.fdopen(handle, "rb") as opened:
                state = json.loads(opened.read())
        except (OSError, ValueError):
            continue
        if not _valid_state(state) or not isinstance(state.get("members"), list):
            continue
        if "stops.txt" in state["members"]:
            found.append((feed_dir, state))
    return found


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
    """The tier a route type settles with no route geography, or None.

    The geography-free rules, ranges exactly as the classifier's: tram/subway
    (rule 3) and the fixed-tier half of rule 2 — coach 200-209 national;
    urban rail 400-405, bus 700-716, trolleybus 800s and tram 900s local.
    Anything else (717-799 included) is unknown to the classifier too.
    """
    if route_type in (0, 1) or 400 <= route_type <= 405:
        return "local"
    if 700 <= route_type <= 716 or 800 <= route_type <= 999:
        return "local"
    if 200 <= route_type <= 209:
        return "national"
    return None


def _skip_stop_times(feed_dir, digests, lookup, force):
    """Whether the complete stop_times read may be skipped; ``(skip, reason)``.

    Reads the members the CURRENT fetch wrote (``digests``) — a member the
    new archive dropped must not leave a stale file deciding the skip — and
    judges them by :func:`skip_predicate`. ``reason`` says why the member is
    read when it is; any doubt or error reads it.
    """
    if force:
        return False, "recrawl requested"
    if "routes.txt" not in digests or "stops.txt" not in digests:
        return False, "routes or stops member missing"
    try:
        routes_path = feed_dir.path / "routes.txt"
        stops_path = feed_dir.path / "stops.txt"
        with os.fdopen(store.open_nofollow(routes_path), "rb") as opened:
            rows = list(
                csv.DictReader(
                    io.TextIOWrapper(opened, encoding="utf-8-sig", errors="strict")
                )
            )
        route_types = []
        for row in rows:
            value = (row.get("route_type") or "").strip()
            route_types.append(int(value) if value.isdigit() else None)
        with os.fdopen(store.open_nofollow(stops_path), "rb") as opened:
            points, dropped = stop_coordinates(opened)
    except Exception as error:
        # The predicate is an optimisation gate; any failure inside it means
        # the full read, never a failed feed.
        return False, f"predicate error: {error}"
    return skip_predicate(route_types, points, dropped, lookup)


def skip_predicate(route_types, points, dropped, lookup):
    """The plan's skip predicate over parsed evidence; ``(skip, reason)``.

    All three required: (a) every route settled by a geography-free tier
    rule, and all to the SAME tier — a tram plus a coach would need a
    per-tier selector, which only the complete read can build — (b) every
    stop in a single country, (c) every stop in exactly one distinct city —
    most-specific city-kind division, against the boundary memo's full
    pinned geometry. ``route_types`` holds one entry per route (None when
    unparsable), ``points`` the parseable stop coordinates and ``dropped``
    how many stop rows were not. The crawler judges the members it just
    wrote; the classify stage judges the same digest-verified data again
    later, so a whole-feed claim is never re-read from disk.
    """
    if lookup is None:
        return False, "no boundary lookup"
    if not route_types:
        return False, "no routes"
    tiers = set()
    for route_type in route_types:
        tier = _fixed_tier(route_type) if route_type is not None else None
        if tier is None:
            return False, "route types need geography"
        tiers.add(tier)
    if len(tiers) > 1:
        return False, "route types span tiers"
    if dropped:
        # A whole-feed claim cannot rest on the parseable subset.
        return False, "unparsable stop rows"
    if not points:
        return False, "no parseable stops"
    try:
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
    except Exception as error:
        return False, f"predicate error: {error}"
    return True, None


def _cache_reusable(feed_dir, state, url, force, lookup):
    """Whether the cached members may stand in for a fetch.

    The digests must verify against the state (the commit point), the URL
    must be the one the state was recorded against, and a cached whole-feed
    stop_times skip is re-judged against the CURRENT boundary memo and rules
    — a skip that no longer holds must refetch, not be reused via 304.
    """
    if state.get("url") != url or force or not _members_intact(feed_dir, state):
        return False
    if state.get("members_requested") != sorted(MEMBERS):
        # A crawl that asked for fewer members (before the calendar files
        # joined the set) cannot stand in for one that asks for them all.
        return False
    if (state.get("stop_times") or {}).get("state") == "skipped":
        still_skipped, _ = _skip_stop_times(
            feed_dir, state.get("member_sha256") or {}, lookup, force
        )
        return still_skipped
    return True


def _valid_state(state):
    """Whether a parsed state has the shape the crawl writes; a corrupt
    file must read as "no usable state", never abort a run."""
    if not isinstance(state, dict):
        return False
    if "stop_times" in state and not isinstance(state["stop_times"], dict):
        return False
    return True


def _read_state(directory):
    try:
        state = json.loads(store.read_text(directory, STATE_FILE))
    except (store.StoreError, ValueError):
        return {}
    return state if _valid_state(state) else {}


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


def _root_files(names):
    """The archive's distinct root-level file names, sorted: a member with a
    ``/`` is in a subfolder (which the crawl never reads) or a directory marker,
    so only root entries count, deduplicated. Restricted to printable ASCII —
    every GTFS spec file qualifies, while a control-character or non-ASCII entry
    (which the range and download paths could decode differently, or which could
    spoof a capability name) is dropped: the manifest is a best-effort
    capability hint, not a security boundary. Each path names files with the
    reader that also extracts them — ``ziprange`` on the range path (raw
    central-directory bytes), ``zipfile`` on the download path (which normalises
    a name, e.g. truncating at an embedded NUL) — so a feed's manifest always
    matches its own evidence; a hand-crafted NUL name is a residual of the hint,
    since real GTFS feeds carry none of these."""
    return sorted(
        {
            name
            for name in names
            if name and "/" not in name and name.isascii() and name.isprintable()
        }
    )


def _manifest_list(value):
    """The sanitized manifest when ``value`` is a list of strings, else None: a
    missing (legacy) or corrupt (non-list, or mixed-type) ``files`` re-fetches
    rather than publishing garbage or raising on ``sorted`` every crawl."""
    if isinstance(value, list) and all(isinstance(name, str) for name in value):
        return _root_files(value)
    return None


def _write_ranged(fetcher, url, probe, feed_dir, decide):
    """Write members via range reads, one at a time; return their digests and
    the archive's file manifest.

    The cheap members land first; ``decide`` then rules on the complete
    ``stop_times.txt`` read from what is on disk. The manifest is the central
    directory the range reads already parse, so it costs no extra fetch.
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
    return digests, _root_files(directory), skipped, reason


def _extract_members(feed_dir, decide):
    """Extract the members from the downloaded archive, streamed and bounded.

    ``zipfile`` handles what ziprange deliberately refuses (ZIP64, data
    descriptors); each member is capped and streamed straight to its file, so
    members never accumulate in memory. The cheap members land first; ``decide``
    then rules on extracting ``stop_times.txt``. Returns the digests and the
    archive file manifest, which comes from the same ``zipfile`` reader that
    extracts the members, so the two never disagree about what the archive
    holds.
    """
    digests = {}
    with zipfile.ZipFile(feed_dir.path / ARCHIVE_FILE) as archive:
        files = _root_files(archive.namelist())

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
    return digests, files, skipped, reason


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
        "files": [],
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
            # A state written before the manifest existed (or a corrupt one)
            # has no valid ``files`` list; re-fetch it once (rather than carry an
            # empty or garbage manifest forward) so the directory is read and
            # recorded. A genuine empty manifest is the list ``[]`` and reuses
            # the cache.
            manifest = _manifest_list(state.get("files"))
            if (
                unchanged
                and manifest is not None
                and _cache_reusable(feed_dir, state, url, force, lookup)
            ):
                record["method"] = "not_modified"
                record["members"] = sorted(state.get("members") or [])
                record["files"] = manifest
                record["stop_times"] = (state.get("stop_times") or {}).get("state")
                return record

        digests = None
        files = []
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
                digests, files, skipped, skip_reason = _write_ranged(
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
            # As above, a state without a valid recorded manifest is re-fetched
            # once rather than reused via a 304, so its directory is read.
            manifest = _manifest_list(state.get("files"))
            usable = (
                _cache_reusable(feed_dir, state, url, force, lookup)
                and manifest is not None
            )
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
                record["files"] = manifest
                record["stop_times"] = (state.get("stop_times") or {}).get("state")
                return record
            try:
                digests, files, skipped, skip_reason = _extract_members(
                    feed_dir, decide
                )
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
        record["files"] = files
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
                # Every root file the archive carries (a superset of the
                # extracted members), so callers can filter feeds by capability.
                "files": files,
                # The member set this crawler asked for: a cache written
                # for a smaller set is not reusable, optional members or not.
                "members_requested": sorted(MEMBERS),
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


def _safe_crawl_one(fetcher, cache_dir, feed, *, force, range_threshold, lookup):
    """:func:`_crawl_one`, guaranteed not to raise. An unexpected error (not one
    it already turns into a ``failed`` record) becomes a ``skipped`` record so a
    single broken feed never aborts the run — identically on the sequential and
    the pooled paths, which both call this."""
    try:
        return _crawl_one(
            fetcher,
            cache_dir,
            feed,
            force=force,
            range_threshold=range_threshold,
            lookup=lookup,
        )
    except Exception as error:  # noqa: B902 — containment at any worker count
        return {
            "feed_id": feed["feed_id"],
            "url": _feed_url(feed),
            "directory": _dir_name(feed["feed_id"]),
            "method": "skipped",
            "bytes_fetched": 0,
            "bytes_saved": 0,
            "fallback_reason": f"unexpected error: {error}",
            "members": [],
            "files": [],
            "stop_times": None,
            "stop_times_reason": None,
        }


def crawl(
    cache_dir,
    *,
    fetcher=None,
    range_threshold=fetch.RANGE_THRESHOLD,
    lookup=None,
    workers=1,
):
    """Crawl every crawlable resolved feed. Returns the run summary.

    Reads ``feeds_resolved``, crawls each crawlable feed into
    ``cache/crawl/<feed dir>/``, writes ``crawl_log.jsonl`` and returns the
    summary counts. ``fetcher`` and ``range_threshold`` are injectable for
    tests. ``lookup`` is the boundary lookup the stop_times predicate answers
    against; by default the memoized one under the cache is opened read-only —
    with no memo (or without its dependencies) every feed reads the complete
    ``stop_times.txt``, the safe direction. ``workers`` runs that many feeds
    concurrently over one shared network-bound ``Fetcher`` (per-feed dirs never
    collide, one host stays rate-limited); the built artifacts are identical to
    a sequential run at any worker count.
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
                # Eligible feeds in traversal order; each keeps its ordinal so
                # the log is byte-identical to a sequential build whatever order
                # the workers finish in. Never sort by feed_id: that would
                # change today's workers=1 output too.
                eligible = [
                    (feed, feed["feed_id"] in forced)
                    for feed in feeds
                    if feed.get("crawlable")
                ]
                if workers == 1:
                    log = [
                        _safe_crawl_one(
                            fetcher,
                            cache_dir,
                            feed,
                            force=force,
                            range_threshold=range_threshold,
                            lookup=lookup,
                        )
                        for feed, force in eligible
                    ]
                else:
                    log = [None] * len(eligible)
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=workers
                    ) as pool:
                        futures = {
                            pool.submit(
                                _safe_crawl_one,
                                fetcher,
                                cache_dir,
                                feed,
                                force=force,
                                range_threshold=range_threshold,
                                lookup=lookup,
                            ): ordinal
                            for ordinal, (feed, force) in enumerate(eligible)
                        }
                        for future in concurrent.futures.as_completed(futures):
                            log[futures[future]] = future.result()
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
