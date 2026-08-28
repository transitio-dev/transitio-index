"""Shared scaffolding for the single-CSV catalogue sources (MDB, GBFS).

Both fetch one CSV export, normalize its rows, and publish the result as a
generation through :mod:`index_build.store` — the same store the Atlas
ingest uses. Only the row parser differs between them, so the download, the
digest, and the publish live here once.
"""

import csv
import datetime
import hashlib
import io
import json
import os
import urllib.request

from index_build import store


class IngestError(RuntimeError):
    """A CSV source could not be ingested as specified."""


DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_TIMEOUT = 120

# The real catalogues are a few MB (MDB ~2.6 MB, GBFS ~0.2 MB). The ceilings
# bound memory: the whole CSV, every row dict, and every record coexist, so
# both the byte size and the row count are capped well above the real data
# rather than at the store's generic artifact limit.
MAX_CSV_BYTES = 32 * 1024 * 1024
MAX_ROWS = 500_000


def _copy_bounded(response, opened_file, limit):
    """Copy at most ``limit`` bytes, returning their digest.

    ``Content-Length`` is the server's claim, so the ceiling is enforced on
    what actually arrives; the digest is taken from the same bytes so the
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
    return digest.hexdigest(), written


def download_csv(directory, name, url):
    """Download ``url`` to ``name`` in ``directory``; return its SHA-256.

    Each attempt owns an exclusively created temporary file, replaced into
    place only once the body is complete — an interrupted run must not leave
    a truncated file a later run reads as cached.
    """
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        handle, partial = store.create_temporary(directory)
        try:
            opened = urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT)
            with opened as response, os.fdopen(handle, "wb") as opened_file:
                handle = None
                declared = response.headers.get("Content-Length")
                digest, written = _copy_bounded(response, opened_file, MAX_CSV_BYTES)
                opened_file.flush()
                os.fsync(opened_file.fileno())
            if declared is not None and written != int(declared):
                # A body that ended short of its Content-Length is truncated,
                # not complete; raise so the attempt is retried rather than
                # publishing a partial catalogue.
                raise IngestError(f"{name}: got {written} bytes, expected {declared}")
            directory.replace(partial, name)
            return digest
        except (OSError, IngestError):
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
        finally:
            if handle is not None:
                os.close(handle)
            store.unlink(directory, partial)
    return None


def read_rows(text, required_headers):
    """Parse CSV ``text`` into rows, requiring ``required_headers``.

    ``row.get`` turns a missing or renamed column into ``None`` silently,
    so an upstream header change would otherwise publish a generation with
    a whole field erased. The header set is checked once, up front; value
    optionality stays at the value level.
    """
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    if len(set(fieldnames)) != len(fieldnames):
        raise IngestError("duplicate CSV column names")
    missing = set(required_headers) - set(fieldnames)
    if missing:
        raise IngestError(f"missing CSV columns: {', '.join(sorted(missing))}")
    rows = []
    for position, row in enumerate(reader):
        if len(rows) >= MAX_ROWS:
            # Abort as soon as the cap is exceeded, before materializing the
            # rest of an over-large body.
            raise IngestError(f"more than {MAX_ROWS} rows")
        # DictReader files extra cells under a None key and fills missing
        # cells with None; both mean a ragged record rather than data.
        if None in row:
            raise IngestError(f"row {position}: more cells than columns")
        if any(value is None for value in row.values()):
            raise IngestError(f"row {position}: fewer cells than columns")
        rows.append(row)
    return rows


def blank_to_none(value):
    """CSV empties are blank strings; normalise them to ``None``."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def require_id(value, kind, source_file, position, *, max_bytes=200):
    """A usable, filesystem-safe id, or an :class:`IngestError`.

    This keeps malformed identifiers out of the published artifacts; the
    crawl cache keys on a *digest* of the id, never the id itself, so the
    check is for what is actually unsafe — control characters and path
    separators — not a canonical shape: these catalogues carry non-ASCII
    ids too.
    """
    value = blank_to_none(value)
    if value is None:
        raise IngestError(f"{source_file}: {kind} {position} has no usable id")
    if store.INVALID_IN_NAME.intersection(value) or "/" in value or "\\" in value:
        raise IngestError(
            f"{source_file}: {kind} {position}: id {value!r} contains a path "
            "separator or control character"
        )
    if value in (".", ".."):
        raise IngestError(f"{source_file}: {kind} {position}: id {value!r}")
    if len(value.encode("utf-8")) > max_bytes:
        raise IngestError(
            f"{source_file}: {kind} {position}: id is over the {max_bytes}-byte limit"
        )
    return value


def optional_id(value, kind, source_file, position):
    """``require_id`` for a reference field that may legitimately be blank."""
    if blank_to_none(value) is None:
        return None
    return require_id(value, kind, source_file, position)


def id_list(value, kind, source_file, position, separator="|"):
    """A ``separator``-joined list of ids, each validated.

    MDB's ``redirect.id`` and ``static_reference`` are pipe-joined lists of
    ids where a feed points at (or is replaced by) several others; the
    separator cannot occur inside an id, so the split is unambiguous.
    """
    value = blank_to_none(value)
    if value is None:
        return []
    return [
        require_id(part, kind, source_file, position) for part in value.split(separator)
    ]


def _jsonl_chunks(records):
    def chunks():
        for record in records:
            yield json.dumps(
                record, ensure_ascii=False, sort_keys=True, allow_nan=False
            )
            yield "\n"

    return chunks


def _read_local(path):
    """Read a caller-supplied CSV outside the cache, guarded like any read.

    A symlink or FIFO there is refused the same way the download path
    guards its reads; the store's failure is reported as an ingest error.
    """
    try:
        handle = store.open_regular_path(path)
    except store.StoreError as error:
        raise IngestError(str(error)) from None
    try:
        return store.read_all(handle, MAX_CSV_BYTES).decode("utf-8")
    finally:
        os.close(handle)


def ingest_csv(
    cache_dir,
    *,
    source,
    label,
    url,
    pointer,
    artifact,
    parse_rows,
    required_headers,
    csv_path=None,
    expected_sha256=None,
):
    """Fetch (or reuse) the source CSV, normalize it, and publish a generation.

    ``parse_rows`` takes ``(rows, source_file)`` and returns
    ``(records, summary_extra)``. With ``csv_path`` the local CSV is used and
    nothing is downloaded, which is how the tests run offline.

    The export at ``url`` is always-latest — it carries no immutable version
    — so the content SHA-256 is the real identity: ``csv_sha256`` records the
    exact bytes that produced the records, and ``pinned`` says whether they
    matched an ``expected_sha256`` the caller asserted. ``label`` is a
    human tag only, never a content claim.
    """
    raw = cache_dir / "raw"
    directory = store.open_directory(raw)
    try:
        with store.exclusive_writer(directory):
            if csv_path is not None:
                text = _read_local(csv_path)
                digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                fetched_url = None
            else:
                cached = f"{source}-{label}.csv"
                download_digest = download_csv(directory, cached, url)
                text = store.read_text(directory, cached)
                # Parse exactly the bytes the digest was taken over: a
                # replacement between the download and this read would show
                # up as a digest mismatch rather than silently diverge.
                digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                if digest != download_digest:
                    raise IngestError(f"{cached}: changed after it was downloaded")
                fetched_url = url

            if expected_sha256 is not None and digest != expected_sha256:
                raise IngestError(
                    f"{source}: content sha256 {digest} does not match the "
                    f"pinned {expected_sha256}"
                )

            rows = read_rows(text, required_headers)
            records, extra = parse_rows(rows, source)
            if not records:
                raise IngestError(f"{source}: no rows found")

            manifest = {
                "source": source,
                "csv_url": fetched_url,
                "csv_label": label,
                "csv_sha256": digest,
                "pinned": expected_sha256 is not None,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
                "rows": len(rows),
                "records": len(records),
                **extra,
            }
            return store.publish(
                raw,
                pointer,
                {artifact: _jsonl_chunks(records)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()
