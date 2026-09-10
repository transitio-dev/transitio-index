"""Resumable HTTP download for the pinned catalogue sources.

Both the Atlas archive and the MDB/GBFS CSVs are one large file fetched over a
proxy that transiently truncates transfers. This streams a URL into a store
temporary file, resuming a short body with a byte-range request from where it
stopped — restarting cleanly if the server ignores the range (answers 200, not
206) — so a file completes even when no single transfer arrives whole. A resume
is pinned to the representation it started with (``If-Range`` plus a check that
the ``206`` resumes at the requested offset), so a resource that changes between
attempts is refetched whole rather than stitched into a hybrid. The temporary
file is put in place only once the body is complete and validated, so an
interrupted run never leaves a truncated file a later run reads as cached.
"""

import hashlib
import http.client
import os
import time
import urllib.request

from transitio_index import store

DEFAULT_ATTEMPTS = 6
DEFAULT_TIMEOUT = 120


class DownloadError(RuntimeError):
    """A download could not be completed."""


def _full_length(response):
    """The full resource size a response declares — the total after the slash of
    a 206 ``Content-Range``, else ``Content-Length`` — or None when it does not
    say (a stub response in a test, an unsized stream)."""
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    content_range = headers.get("Content-Range")
    if content_range and "/" in content_range:
        total = content_range.rsplit("/", 1)[-1].strip()
        if total.isdigit():
            return int(total)
    length = headers.get("Content-Length")
    return int(length) if length and str(length).isdigit() else None


def _content_range_start(response):
    """The start offset a 206 ``Content-Range`` reports (the number before the
    dash in ``bytes <start>-<end>/<total>``), or None when the header is absent
    or unparseable."""
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    content_range = headers.get("Content-Range")
    if not content_range:
        return None
    spec = content_range.split(" ", 1)[-1].split("/", 1)[0]
    start = spec.split("-", 1)[0].strip()
    return int(start) if start.isdigit() else None


def _validator_tag(response):
    """An ``ETag`` or ``Last-Modified`` value that pins a resume to one
    representation through ``If-Range``, or None when the response carries
    neither (then a resume cannot be pinned and a mid-transfer change is caught
    only by the offset check and the validator)."""
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    return headers.get("ETag") or headers.get("Last-Modified")


def to_file(
    directory,
    name,
    url,
    *,
    limit,
    attempts=DEFAULT_ATTEMPTS,
    timeout=DEFAULT_TIMEOUT,
    error=DownloadError,
    validate=None,
):
    """Download ``url`` to ``name`` in ``directory``; return its SHA-256.

    ``limit`` caps the bytes accepted. ``error`` is the exception class raised
    for a failed or over-ceiling download. ``validate`` — given the completed
    temporary file open for reading — may raise ``error`` to reject it (e.g. a
    tarball check); a rejection is retried within the attempt budget, since a
    body that arrives complete-by-length can still be corrupt.
    """
    handle, partial = store.create_temporary(directory)
    try:
        with os.fdopen(handle, "wb") as opened_file:
            handle = None
            digest = hashlib.sha256()
            written = 0
            declared = None
            if_range = None
            last_error = None

            def restart():
                nonlocal digest, written, if_range
                opened_file.seek(0)
                opened_file.truncate()
                digest, written, if_range = hashlib.sha256(), 0, None

            for attempt in range(1, attempts + 1):
                if attempt > 1:
                    time.sleep(min(2 ** (attempt - 2), 8))
                request = urllib.request.Request(url)
                if written:
                    request.add_header("Range", f"bytes={written}-")
                    if if_range:
                        request.add_header("If-Range", if_range)
                try:
                    with urllib.request.urlopen(request, timeout=timeout) as response:
                        status = getattr(response, "status", 200)
                        if written and status != 206:
                            # The range was ignored, or ``If-Range`` saw the
                            # resource change and returned the whole body anew:
                            # start the file over from this response.
                            restart()
                        elif written:
                            start = _content_range_start(response)
                            if start is not None and start != written:
                                # A 206 that does not resume at our offset would
                                # stitch a hybrid file: discard and refetch whole.
                                last_error = error(
                                    f"{url}: 206 resumed at {start}, not {written}"
                                )
                                restart()
                                continue
                        if declared is None:
                            declared = _full_length(response)
                        if if_range is None:
                            if_range = _validator_tag(response)
                        for chunk in iter(lambda: response.read(1024 * 1024), b""):
                            written += len(chunk)
                            if written > limit:
                                raise error(f"{url}: exceeds the {limit}-byte ceiling")
                            digest.update(chunk)
                            opened_file.write(chunk)
                    if declared is not None and written < declared:
                        last_error = error(
                            f"{url}: got {written} bytes, expected {declared}"
                        )
                        continue
                    if validate is not None:
                        opened_file.flush()
                        os.fsync(opened_file.fileno())
                        check = store.open_regular(directory, partial)
                        try:
                            with os.fdopen(check, "rb") as verify_file:
                                validate(verify_file)
                        except error as bad:
                            # Complete by length but still corrupt (e.g. a gzip
                            # truncated after its first member): refetch whole.
                            last_error = bad
                            restart()
                            continue
                    break
                except (OSError, http.client.IncompleteRead) as exc:
                    # A dropped or truncated read: resume from ``written`` on the
                    # next attempt rather than refetching the whole body.
                    last_error = exc
            else:
                raise last_error or error(f"{url}: download did not complete")
            opened_file.flush()
            os.fsync(opened_file.fileno())
        directory.replace(partial, name)
        return digest.hexdigest()
    finally:
        if handle is not None:
            os.close(handle)
        store.unlink(directory, partial)
