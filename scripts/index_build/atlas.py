"""Transitland Atlas ingest: pinned DMFR fetch and normalization.

The Atlas is a repository of ``.dmfr.json`` files, one per operator domain,
each listing that operator's feeds. It is pinned by commit rather than by
checksum: GitHub's generated tarballs are not byte-stable, but the commit
identifies the content exactly.
"""

import contextlib
import datetime
import gzip
import hashlib
import json
import os
import re
import stat
import tarfile
import urllib.request

from index_build import store


class IngestError(RuntimeError):
    """The Atlas archive could not be ingested as specified."""


ATLAS_REPO = "transitland/transitland-atlas"
ATLAS_COMMIT = "a4d02044f59f954bf3d2fe13b52f7cd1b7e92846"

DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_TIMEOUT = 60

# The Atlas tarball is a few MB. These ceilings bound a hostile or
# corrupt archive: bytes off the wire, members read, and bytes expanded.
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_MEMBERS = 100_000
MAX_TOTAL_BYTES = 512 * 1024 * 1024

# A DMFR file is a few hundred KB at most; the cap keeps a malformed or
# hostile archive from being read into memory whole.
MAX_MEMBER_BYTES = 32 * 1024 * 1024

SPECS = ("gtfs", "gtfs-rt", "gbfs")

# Onestop IDs are not ASCII: 870 of the 6,638 ids in the pinned Atlas carry
# Japanese, Cyrillic, Greek, Hebrew or accented Latin characters, so a
# "canonical syntax" pattern would reject an eighth of the catalogue. What
# is rejected instead is what is actually unsafe. This keeps malformed
# identifiers out of the artifacts; it is not what makes an id safe as a
# filename — the crawl cache keys on a digest of the id, never the id.
COMMIT_PATTERN = re.compile(r"\A[0-9a-f]{40}\Z")

UNSAFE_IN_ID = re.compile(r"[\x00-\x1f\x7f/\\]")
MAX_ID_BYTES = 200


def is_commit_sha(value):
    """A full 40-character hex SHA — the only form that cannot move."""
    return bool(isinstance(value, str) and COMMIT_PATTERN.match(value))


def require_commit(commit):
    """Refuse a ref that could resolve differently tomorrow.

    Checked here rather than only in the CLI: a branch or tag name reaching
    ``ingest`` programmatically would be fetched and then recorded as a
    verified pin, which is exactly the guarantee the pin exists to make.
    """
    if not is_commit_sha(commit):
        raise IngestError(f"{commit!r} is not a full 40-character commit SHA")
    return commit


def archive_url(commit=ATLAS_COMMIT):
    """URL of the repository tarball at ``commit``."""
    return f"https://codeload.github.com/{ATLAS_REPO}/tar.gz/{commit}"


def _digest_of(directory, name):
    """SHA-256 of a cache file, or None when it is unusable or absent.

    A cache entry is only hashed once it is known to be a regular file of
    plausible size: a FIFO would block here forever, and a device or an
    oversized file would be read past every ceiling this module sets.
    """
    size = store.regular_file_size(directory, name)
    if size is None or size > MAX_ARCHIVE_BYTES:
        return None
    try:
        handle = store.open_regular(directory, name)
    except (OSError, store.StoreError):
        return None
    digest = hashlib.sha256()
    read = 0
    with os.fdopen(handle, "rb") as opened_file:
        info = os.fstat(opened_file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_ARCHIVE_BYTES:
            return None
        for chunk in iter(lambda: opened_file.read(1024 * 1024), b""):
            read += len(chunk)
            if read > MAX_ARCHIVE_BYTES:
                return None
            digest.update(chunk)
    return digest.hexdigest()


def download_archive(directory, name, commit=ATLAS_COMMIT):
    """Download the Atlas tarball at ``commit`` to ``name`` in ``directory``.

    Each attempt owns its own exclusively created temporary file, and the
    bytes only take the real name once they open as a tarball: an
    interrupted run must not leave something a later run reads as cached.

    Returns the SHA-256 of the bytes that arrived. Re-reading the cache
    path for that digest would bless whatever is sitting there by then.
    """
    require_commit(commit)
    url = archive_url(commit)
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        handle, partial = store.create_temporary(directory)
        try:
            opened_url = urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT)
            with opened_url as response, os.fdopen(handle, "wb") as opened_file:
                handle = None
                digest = _copy_bounded(response, opened_file, MAX_ARCHIVE_BYTES)
                opened_file.flush()
                os.fsync(opened_file.fileno())
            check = store.open_regular(directory, partial)
            with os.fdopen(check, "rb") as opened_file:
                if not is_tarball(opened_file):
                    raise IngestError(f"{url}: not a tarball")
            directory.replace(partial, name)
            return digest
        except (OSError, EOFError, tarfile.TarError, IngestError):
            # Only failures a retry can plausibly fix; a bug in this module
            # should surface on the first attempt with its own traceback.
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
        finally:
            if handle is not None:
                os.close(handle)
            store.unlink(directory, partial)


def _copy_bounded(response, opened_file, limit):
    """Copy at most ``limit`` bytes, returning their digest.

    ``Content-Length`` is the server's claim, so the ceiling is enforced on
    what actually arrives; the digest is taken from the same bytes, so the
    trusted value never depends on re-reading the file afterwards.
    """
    digest = hashlib.sha256()
    written = 0
    for chunk in iter(lambda: response.read(1024 * 1024), b""):
        written += len(chunk)
        if written > limit:
            raise IngestError(f"download exceeds the {limit}-byte ceiling")
        digest.update(chunk)
        opened_file.write(chunk)
    return digest.hexdigest()


def _is_dmfr(member):
    # Members are read, never extracted, so path traversal is not a concern;
    # the checks keep non-regular entries and stray files out of the parse.
    # The layout is exact — <root>/feeds/<name>.dmfr.json — because the
    # basename becomes a feed's `source_domain`, and two files of the same
    # name at different depths would look like the same publisher.
    parts = member.name.split("/")
    return (
        member.isfile()
        and len(parts) == 3
        and parts[1] == "feeds"
        and parts[2].endswith(".dmfr.json")
    )


def _reject_constant(name):
    """Python's JSON reader accepts NaN and Infinity; JSON itself does not.

    Letting one through a preserved block would produce artifacts that are
    not valid JSON for anything else to read.
    """
    raise IngestError(f"non-finite JSON constant {name!r} in a DMFR file")


class _BoundedReader:
    """A read-only stream that refuses to yield more than ``limit`` bytes.

    The member ceilings in :func:`iter_dmfr` are checked once ``tarfile``
    yields a member, but ``tarfile`` reads PAX and GNU long-name extension
    bodies before that happens — so a small archive can expand without
    limit before any check runs. Bounding the *decompressed* stream is what
    actually closes that.
    """

    def __init__(self, stream, limit):
        self._stream = stream
        self._limit = limit
        self._read = 0

    def read(self, size=-1):
        chunk = self._stream.read(size)
        self._read += len(chunk)
        if self._read > self._limit:
            raise IngestError(f"archive expands past the {self._limit}-byte ceiling")
        return chunk

    def close(self):
        self._stream.close()


def _open_regular_path(path):
    """A binary reader for ``path``, refusing a symlink or non-regular file.

    A path input skips the cache's descriptor guard, so it goes through the
    same portable regular-file opener the store uses, with the store's
    failures reported as :class:`IngestError`.
    """
    try:
        return os.fdopen(store.open_regular_path(path), "rb")
    except store.StoreError as error:
        raise IngestError(str(error)) from None


@contextlib.contextmanager
def _open_tarball(archive):
    """Open a gzipped tarball with its expansion bounded.

    Streaming mode (``r|``) is what a non-seekable bounded reader supports,
    and it matches how this module reads: members in order, each one's
    content taken as it goes past. A caller passing a file object owns it;
    a path is opened, guarded, and closed here.
    """
    to_close = []
    try:
        if hasattr(archive, "read"):
            raw = archive
        else:
            raw = _open_regular_path(archive)
            to_close.append(raw)
        decompressed = gzip.GzipFile(fileobj=raw, mode="rb")
        to_close.append(decompressed)
        tar = tarfile.open(
            fileobj=_BoundedReader(decompressed, MAX_TOTAL_BYTES), mode="r|"
        )
        to_close.append(tar)
        yield tar
    finally:
        # Best-effort cleanup; a close error here would only mask whatever
        # is already propagating.
        for opened in reversed(to_close):
            try:
                opened.close()
            except (OSError, tarfile.TarError):
                pass


def is_tarball(opened_file):
    """Whether the stream opens as a gzipped tarball with a first member."""
    try:
        with _open_tarball(opened_file) as tar:
            return tar.next() is not None
    except (tarfile.TarError, OSError, EOFError, IngestError):
        return False


def iter_dmfr(archive):
    """Yield ``(source_file, payload)`` for every DMFR file in the tarball."""
    members = 0
    total = 0
    seen = set()
    with _open_tarball(archive) as tar:
        for member in tar:
            members += 1
            total += max(member.size, 0)
            if members > MAX_MEMBERS or total > MAX_TOTAL_BYTES:
                raise IngestError(
                    f"archive expands past the "
                    f"{MAX_MEMBERS}-member / {MAX_TOTAL_BYTES}-byte ceiling"
                )
            if not _is_dmfr(member):
                continue
            if member.size > MAX_MEMBER_BYTES:
                # Skipping would produce a short ingest that still looks
                # successful; a DMFR file this large is a problem to see.
                raise IngestError(
                    f"{member.name}: {member.size} bytes exceeds the "
                    f"{MAX_MEMBER_BYTES}-byte member limit"
                )
            extracted = tar.extractfile(member)
            if extracted is None:
                continue
            source_file = member.name.split("/")[2]
            if source_file in seen:
                raise IngestError(f"{source_file}: appears twice in the archive")
            seen.add(source_file)
            with extracted:
                payload = json.loads(
                    extracted.read().decode("utf-8"),
                    parse_constant=_reject_constant,
                )
            yield source_file, payload


def source_domain(source_file):
    """``mta.info.dmfr.json`` -> ``mta.info``, the secondary join key."""
    return source_file[: -len(".dmfr.json")]


def _require(value, kind, source_file, position, field, expected):
    if not isinstance(value, expected):
        raise IngestError(
            f"{source_file}: {kind} {position}: {field} is "
            f"{type(value).__name__}, expected {expected.__name__}"
        )
    return value


def _require_id(value, kind, source_file, position):
    if not isinstance(value, str) or not value.strip():
        raise IngestError(f"{source_file}: {kind} {position} has no usable id")
    if value != value.strip():
        raise IngestError(
            f"{source_file}: {kind} {position}: id {value!r} has surrounding "
            "whitespace"
        )
    if UNSAFE_IN_ID.search(value) or value in (".", ".."):
        raise IngestError(
            f"{source_file}: {kind} {position}: id {value!r} contains a path "
            "separator or control character"
        )
    encoded = len(value.encode("utf-8"))
    if encoded > MAX_ID_BYTES:
        # Bytes, not characters: a multibyte id well inside a character
        # count can still exceed a filesystem's limit.
        raise IngestError(
            f"{source_file}: {kind} {position}: id is {encoded} bytes, "
            f"over the {MAX_ID_BYTES}-byte limit"
        )
    return value


def _require_str_items(value, kind, source_file, position, field):
    for item in value or []:
        _require(item, kind, source_file, position, f"{field} item", str)


def _require_id_items(value, kind, source_file, position):
    for item in value or []:
        _require_id(item, kind, source_file, position)


FEED_SHAPES = {
    "urls": dict,
    "license": dict,
    "authorization": dict,
    "tags": dict,
    "operators": list,
    "supersedes_ids": list,
    "languages": list,
}


def _check_shapes(record, kind, source_file, position, shapes):
    """Reject a record whose optional fields are not the shape we assume.

    ``record.get(field) or {}`` would turn a string ``authorization`` into
    an empty mapping and quietly misstate ``requires_auth``; an upstream
    shape change should be visible instead.
    """
    for field, expected in shapes.items():
        value = record.get(field)
        if value is not None:
            _require(value, kind, source_file, position, field, expected)


OPERATOR_SHAPES = {
    "tags": dict,
    "associated_feeds": list,
    "supersedes_ids": list,
}


def _associations(record, kind, source_file, position):
    """What an operator declares it publishes.

    An entry links the operator either to another **feed**
    (``feed_onestop_id`` — the declared static-to-realtime link, on 312
    top-level operators and 1,481 inline entries, covering 670 feeds) or to
    an **agency inside the containing feed** (``gtfs_agency_id``, on 1,217
    entries). Both are kept: the first is the linking evidence, the second
    is what ties an operator to a specific agency in a bundled feed. An
    entry carrying neither is malformed and stops the ingest.
    """
    associations = []
    for entry in record.get("associated_feeds") or []:
        _require(entry, kind, source_file, position, "associated_feeds entry", dict)
        feed_id = entry.get("feed_onestop_id")
        agency_id = entry.get("gtfs_agency_id")
        if feed_id is None and agency_id is None:
            raise IngestError(
                f"{source_file}: {kind} {position}: an associated_feeds entry "
                "names neither a feed nor an agency"
            )
        if feed_id is not None:
            _require_id(feed_id, f"{kind} associated feed", source_file, position)
        if agency_id is not None:
            _require(agency_id, kind, source_file, position, "gtfs_agency_id", str)
        associations.append({"feed_onestop_id": feed_id, "gtfs_agency_id": agency_id})
    return associations


def _operator_fields(record, kind, source_file, position):
    _require(record, kind, source_file, position, "record", dict)
    _check_shapes(record, kind, source_file, position, OPERATOR_SHAPES)
    for field in ("name", "short_name", "website"):
        value = record.get(field)
        if value is not None:
            _require(value, kind, source_file, position, field, str)
    _require_id_items(
        record.get("supersedes_ids"), f"{kind} superseded", source_file, position
    )
    onestop_id = record.get("onestop_id")
    if onestop_id is not None:
        _require_id(onestop_id, kind, source_file, position)
    associations = _associations(record, kind, source_file, position)
    return {
        "onestop_id": onestop_id,
        "associations": associations,
        "associated_feed_ids": [
            entry["feed_onestop_id"]
            for entry in associations
            if entry["feed_onestop_id"]
        ],
        "name": record.get("name"),
        "short_name": record.get("short_name"),
        "website": record.get("website"),
        "tags": record.get("tags") or {},
        "supersedes_ids": record.get("supersedes_ids") or [],
    }


def _feed_operators(record, source_file, position):
    """Inline operator entries, whole.

    An entry may carry a name without an id, and most carry the
    ``associated_feeds`` link as well; collapsing them to bare ids would
    throw away both the crosswalk's name evidence and the realtime link.
    """
    operators = []
    for entry in record.get("operators") or []:
        _require(entry, "feed", source_file, position, "operators entry", dict)
        fields = _operator_fields(entry, "feed", source_file, position)
        # An entry with no id and no name can still carry the declared
        # realtime association, which is the one link no later stage can
        # rederive; dropping it would push that feed onto weaker evidence.
        if any(fields[field] for field in fields):
            operators.append(fields)
    return operators


def normalize_feed(record, source_file, position=0):
    """One Atlas feed as an ingest record.

    Blocks that downstream stages must not reinterpret — ``license``,
    ``authorization``, ``tags`` — are carried verbatim; the derived
    ``requires_auth`` is a convenience, not a replacement.

    Raises ``IngestError`` on a record this stage cannot represent. An
    upstream schema change should stop the build rather than quietly
    produce a short or non-conforming artifact.
    """
    _require(record, "feed", source_file, position, "record", dict)
    onestop_id = _require_id(record.get("id"), "feed", source_file, position)
    spec = record.get("spec")
    if not isinstance(spec, str) or spec.lower() not in SPECS:
        raise IngestError(
            f"{source_file}: feed {onestop_id}: unsupported spec {spec!r} "
            f"(expected one of {', '.join(SPECS)})"
        )
    _check_shapes(record, "feed", source_file, position, FEED_SHAPES)
    name = record.get("name")
    if name is not None:
        _require(name, "feed", source_file, position, "name", str)
    _require_str_items(
        record.get("languages"), "feed", source_file, position, "languages"
    )
    _require_id_items(
        record.get("supersedes_ids"), "feed superseded", source_file, position
    )
    authorization = record.get("authorization") or {}
    return {
        "source": "atlas",
        "onestop_id": onestop_id,
        "spec": spec.lower(),
        "urls": record.get("urls") or {},
        "name": record.get("name"),
        "operators": _feed_operators(record, source_file, position),
        "license": record.get("license") or {},
        "authorization": authorization,
        "requires_auth": bool(authorization),
        "tags": record.get("tags") or {},
        "supersedes_ids": record.get("supersedes_ids") or [],
        "languages": record.get("languages") or [],
        "source_file": source_file,
        "source_domain": source_domain(source_file),
    }


def normalize_operator(record, source_file, position=0):
    """One Atlas operator; ``name`` feeds the crosswalk's name gate."""
    fields = _operator_fields(record, "operator", source_file, position)
    _require_id(fields["onestop_id"], "operator", source_file, position)
    return dict(
        fields,
        source="atlas",
        source_file=source_file,
        source_domain=source_domain(source_file),
    )


def parse(archive):
    """Normalize every feed and operator in the tarball.

    Returns a dict of ``feeds``, ``operators``, ``dmfr_files`` and
    ``operator_id_collisions``. Every record must carry an id and a
    supported spec; anything else raises ``IngestError`` naming the source
    file, since a record this stage cannot represent is a change upstream
    rather than something to skip past.

    Feed ids must be unique across the whole archive — everything
    downstream keys on them. Operator ids are *not* unique upstream: the
    pinned Atlas lists two operators once per feed group, so those rows are
    merged, unioning their ``associated_feed_ids``, and the count of
    merges is reported rather than passed over.
    """
    feeds = []
    feed_sources = {}
    operators = {}
    collisions = 0
    file_count = 0
    for source_file, payload in iter_dmfr(archive):
        file_count += 1
        _require(payload, "file", source_file, 0, "payload", dict)
        for field in ("feeds", "operators"):
            if field in payload:
                # `or []` would read `"feeds": {}` — or an explicit null —
                # as "no feeds" and ingest the file as if it were empty. A
                # missing key is absence; a present one must be a list.
                _require(payload[field], "file", source_file, 0, field, list)
        for position, record in enumerate(payload.get("feeds") or []):
            feed = normalize_feed(record, source_file, position)
            first = feed_sources.get(feed["onestop_id"])
            if first is not None:
                raise IngestError(
                    f"{feed['onestop_id']}: declared in both {first} and "
                    f"{source_file}"
                )
            feed_sources[feed["onestop_id"]] = source_file
            feeds.append(feed)
        for position, record in enumerate(payload.get("operators") or []):
            operator = normalize_operator(record, source_file, position)
            existing = operators.get(operator["onestop_id"])
            if existing is None:
                operators[operator["onestop_id"]] = operator
                continue
            collisions += 1
            # The union is what later stages read; the extra listings are
            # kept whole beside it, because the groups an operator was
            # listed in — and any name or tag differences between them —
            # are evidence the crosswalk and the realtime linking draw on.
            # Only the *extra* listings, since the first is this record.
            existing.setdefault("other_listings", []).append(operator)
            existing["associated_feed_ids"] = list(
                dict.fromkeys(
                    existing["associated_feed_ids"] + operator["associated_feed_ids"]
                )
            )
    operators = list(operators.values())
    if not file_count or not feeds:
        # An upstream layout change would otherwise look like a valid
        # ingest and publish an empty catalogue over a good one.
        raise IngestError(
            f"{archive}: no DMFR feeds found "
            f"({file_count} DMFR files, {len(feeds)} feeds)"
        )
    return {
        "feeds": feeds,
        "operators": operators,
        "dmfr_files": file_count,
        "operator_id_collisions": collisions,
    }


def _spec_counts(feeds):
    counts = {spec: 0 for spec in SPECS}
    for feed in feeds:
        counts[feed["spec"]] = counts.get(feed["spec"], 0) + 1
    return counts


def _open_source(directory, cached, archive):
    """The archive to read, opened once and never resolved by path again.

    A cached archive is opened through the held directory handle: reopening
    it by pathname would leave a window in which the entry, or an ancestor,
    could be swapped for a symlink.
    """
    if cached is not None:
        return os.fdopen(store.open_regular(directory, cached), "rb")
    # A caller-supplied archive is outside the cache, so it gets the same
    # portable guard: no followed symlink, no FIFO to wedge the read.
    return _open_regular_path(archive)


def _snapshot(directory, opened_source, label):
    """Copy the archive once, hashing as we go.

    Returns ``(name, digest, opened)`` with ``opened`` rewound and still
    open: the caller parses *that* handle rather than reopening the name,
    so no path is resolved twice and the digest and the parsed records
    cannot describe different bytes. The caller closes it.
    """
    digest = hashlib.sha256()
    copied = 0
    handle, name = store.create_temporary(directory, readable=True)
    target = None
    try:
        target = os.fdopen(handle, "w+b")
        handle = None
        for chunk in iter(lambda: opened_source.read(1024 * 1024), b""):
            copied += len(chunk)
            if copied > MAX_ARCHIVE_BYTES:
                raise IngestError(
                    f"{label}: exceeds the " f"{MAX_ARCHIVE_BYTES}-byte archive ceiling"
                )
            digest.update(chunk)
            target.write(chunk)
        target.flush()
        target.seek(0)
    except BaseException:
        if handle is not None:
            os.close(handle)
        if target is not None:
            target.close()
        store.unlink(directory, name)
        raise
    return name, digest.hexdigest(), target


def ingest(cache_dir, archive=None, commit=ATLAS_COMMIT):
    """Run the Atlas ingest, publishing a generation into ``cache_dir``.

    With ``archive`` the tarball on disk is used as-is and nothing is
    downloaded, which is how the tests run offline. Its ``commit`` is then
    an assertion by the caller rather than something this stage fetched,
    so the manifest records ``commit_verified: false``; only a downloaded
    archive can vouch for the commit it is named after.

    The feeds, the operators and the manifest describing them are published
    together as one generation, and ``atlas.json`` is swapped to point at
    it. A reader resolves through that pointer, so it either sees the whole
    previous generation or the whole new one.

    One writer lock covers the whole stage rather than just the publish:
    the cached archive is inspected, replaced and digested first, and a
    concurrent run changing it in between would publish input that does not
    match the manifest describing it.
    """
    raw = cache_dir / "raw"
    # `cache_dir` is opened first and `raw` created relative to it: opening
    # only the leaf would follow a symlink or junction planted at the parent.
    parent = store.open_directory(cache_dir)
    try:
        directory = parent.child("raw")
    finally:
        parent.close()
    try:
        with store.exclusive_writer(directory):
            expected = None
            cached = None
            digest_name = None
            if archive is None:
                require_commit(commit)
                cached = f"transitland-atlas-{commit}.tar.gz"
                digest_name = f"{cached}.sha256"
                try:
                    expected = store.read_text(directory, digest_name).strip()
                except store.MissingEntry:
                    expected = None
                # A cached archive only vouches for its commit if its bytes
                # still match what the download for that commit recorded.
                if expected is None or _digest_of(directory, cached) != expected:
                    expected = download_archive(directory, cached, commit)
                archive = raw / cached

            with _open_source(directory, cached, archive) as opened_source:
                snapshot, digest, opened_snapshot = _snapshot(
                    directory, opened_source, archive
                )
            try:
                if expected is not None and digest != expected:
                    # The archive changed between the check and the copy, so
                    # the snapshot is not the commit's bytes and nothing may
                    # claim that it is.
                    raise IngestError(f"{archive}: does not match the recorded digest")
                parsed = parse(opened_snapshot)
            except (tarfile.TarError, gzip.BadGzipFile, EOFError, OSError):
                if cached is not None:
                    # A tar header is not proof the body is whole. Keeping an
                    # archive that will not parse — with a sidecar vouching
                    # for it — wedges every later run, because the cache then
                    # looks valid and is never fetched again.
                    # Only when the bytes themselves are unreadable. An
                    # archive we refuse on content is exactly what upstream
                    # published, and deleting it would re-download it on
                    # every run to reach the same verdict.
                    store.unlink(directory, cached)
                    store.unlink(directory, digest_name)
                raise
            finally:
                opened_snapshot.close()
                store.unlink(directory, snapshot)

            if cached is not None:
                # Only now, with the archive proven parseable end to end.
                store.write_file(directory, digest_name, lambda: [f"{expected}\n"])

            manifest = {
                "source": "atlas",
                "repo": ATLAS_REPO,
                "commit": commit,
                "commit_verified": expected is not None,
                "archive_url": archive_url(commit) if expected is not None else None,
                "archive_sha256": digest,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
                "dmfr_files": parsed["dmfr_files"],
                "feeds": len(parsed["feeds"]),
                "feeds_by_spec": _spec_counts(parsed["feeds"]),
                "operators": len(parsed["operators"]),
                "operator_id_collisions": parsed["operator_id_collisions"],
            }
            return store.publish(
                raw,
                "atlas.json",
                {
                    "atlas_feeds.jsonl": store.jsonl_chunks(parsed["feeds"]),
                    "atlas_operators.jsonl": store.jsonl_chunks(parsed["operators"]),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
