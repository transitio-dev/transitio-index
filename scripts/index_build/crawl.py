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

The stop_times predicate: the plan's skip conditions need per-stop country and
place resolution, which the boundary cache will provide; until it exists every
crawled feed reads the complete ``stop_times.txt`` — the safe direction.
"""

import datetime
import hashlib
import json
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
            # O_NOFOLLOW refuses a symlink atomically at open, where a
            # check-then-open pair could be raced by a replacement.
            handle = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
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
    if not path.is_file():
        return frozenset()
    records = store.parse_jsonl(path.read_bytes())
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


def _write_ranged(fetcher, url, probe, feed_dir):
    """Write the members via range reads, one at a time; return their digests."""
    read = fetcher.range_reader(url, validator=_range_validator(probe))
    directory = ziprange.central_directory(read, probe["size"])
    digests = {}
    for name in MEMBERS:
        entry = directory.get(name)
        if entry is None:
            continue
        data = ziprange.read_member(read, entry)
        store.write_bytes(feed_dir, name, data)
        digests[name] = hashlib.sha256(data).hexdigest()
        del data
    return digests


def _extract_members(feed_dir):
    """Extract the members from the downloaded archive, streamed and bounded.

    ``zipfile`` handles what ziprange deliberately refuses (ZIP64, data
    descriptors); each member is capped and streamed straight to its file, so
    members never accumulate in memory. Returns their digests.
    """
    digests = {}
    with zipfile.ZipFile(feed_dir.path / ARCHIVE_FILE) as archive:
        for name in MEMBERS:
            try:
                info = archive.getinfo(name)
            except KeyError:
                continue
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
    return digests


def _crawl_one(fetcher, cache_dir, feed, *, force, range_threshold):
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
    }
    if not url:
        record["method"] = "skipped"
        record["fallback_reason"] = "no download URL"
        return record

    feed_dir = None
    fetched_before = fetcher.bytes_fetched
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
            if unchanged and _members_intact(feed_dir, state):
                record["method"] = "not_modified"
                record["members"] = sorted(state.get("members") or [])
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
                digests = _write_ranged(fetcher, url, probe, feed_dir)
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
            # the cache verifies against its recorded digests.
            same_url = state.get("url") == url
            usable = same_url and not force and _members_intact(feed_dir, state)
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
                return record
            try:
                digests = _extract_members(feed_dir)
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
    ) as error:
        record["method"] = "failed"
        record["fallback_reason"] = str(error)
        return record
    finally:
        record["bytes_fetched"] = fetcher.bytes_fetched - fetched_before
        if feed_dir is not None:
            feed_dir.close()


def crawl(cache_dir, *, fetcher=None, range_threshold=fetch.RANGE_THRESHOLD):
    """Crawl every crawlable resolved feed. Returns the run summary.

    Reads ``feeds_resolved``, crawls each crawlable feed into
    ``cache/crawl/<feed dir>/``, writes ``crawl_log.jsonl`` and returns the
    summary counts. ``fetcher`` and ``range_threshold`` are injectable for
    tests.
    """
    if fetcher is None:
        fetcher = fetch.Fetcher()
    feeds, resolve_manifest = store.read_jsonl(
        cache_dir / "resolve", "feeds_resolved.json", "feeds_resolved.jsonl"
    )
    directory = store.open_subdir(cache_dir, "crawl")
    log = []
    try:
        with store.exclusive_writer(directory):
            # Read under the same lock appenders and the clearing rewrite
            # hold, so a compliant concurrent writer can neither expose a
            # partial file nor have a fresh request missed by this run.
            recrawl = _recrawl_ids(cache_dir)
            for feed in feeds:
                if not feed.get("crawlable"):
                    continue
                log.append(
                    _crawl_one(
                        fetcher,
                        cache_dir,
                        feed,
                        force=feed["feed_id"] in recrawl,
                        range_threshold=range_threshold,
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
                fulfilled = {
                    record["feed_id"]
                    for record in log
                    if record["method"] in ("range", "download")
                    and "stop_times.txt" in record["members"]
                }
                cleared = sum(1 for feed_id in recrawl if feed_id in fulfilled)
                if cleared:
                    path = cache_dir / "recrawl_requests.jsonl"
                    # Non-object rows survive untouched: only rows whose
                    # request was satisfied are removed.
                    remaining = [
                        row
                        for row in store.parse_jsonl(path.read_bytes())
                        if not isinstance(row, dict)
                        or row.get("feed_id") not in fulfilled
                    ]
                    # A random O_EXCL temporary: a predictable name could be a
                    # pre-planted symlink. Appenders must hold this crawl lock.
                    handle, partial = tempfile.mkstemp(dir=cache_dir, suffix=".recrawl")
                    try:
                        with os.fdopen(handle, "w") as opened:
                            opened.write(
                                "".join(json.dumps(row) + "\n" for row in remaining)
                            )
                        os.replace(partial, path)
                    except BaseException:
                        os.unlink(partial)
                        raise
    finally:
        directory.close()

    methods = {}
    for record in log:
        methods[record["method"]] = methods.get(record["method"], 0) + 1
    return {
        "source": "crawl",
        "sources": resolve_manifest.get("sources"),
        "feeds_crawlable": len(log),
        "by_method": methods,
        "bytes_fetched": sum(r["bytes_fetched"] for r in log),
        "bytes_saved": sum(r["bytes_saved"] for r in log),
        "recrawl_requested": len(recrawl),
        "recrawl_cleared": cleared,
        "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
