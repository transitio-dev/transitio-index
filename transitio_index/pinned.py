"""Pinned external inputs: fetched once (or read from local files), verified
against their pinned checksums and published together as one ``raw``
generation whose verified bytes every reader parses — so a later build works
from identical bytes or refuses.
"""

import datetime
import hashlib
import os

from transitio_index import csv_source, store

MAX_INPUT_BYTES = 64 * 1024 * 1024


class PinnedInputError(RuntimeError):
    """A pinned input is missing, altered or inconsistent."""


def _read_input(directory, name, files, urls, error):
    """The bytes of one input: a caller-supplied local file, else its download."""
    if files is not None and name in files:
        try:
            handle = store.open_regular_path(files[name])
        except store.StoreError as exc:
            raise error(str(exc)) from None
        try:
            return store.read_all(handle, MAX_INPUT_BYTES)
        finally:
            os.close(handle)
    if name not in urls:
        raise error(f"{name}: no pinned URL and no local file")
    csv_source.download_file(directory, name, urls[name], limit=MAX_INPUT_BYTES)
    try:
        return store.read_bytes(directory, name)
    finally:
        store.unlink(directory, name)


def prepare(
    cache_dir,
    *,
    pointer,
    urls,
    expected,
    files=None,
    manifest=None,
    error=PinnedInputError,
):
    """Ensure ``raw/<pointer>`` holds the pinned inputs; return its manifest.

    ``urls`` maps each input name to its download URL and ``expected`` to its
    SHA-256; ``files`` maps names to local paths used instead of the URLs (how
    tests and offline builds run); ``manifest`` carries the source's own
    manifest fields. A generation that resolves — every artifact hashed —
    under exactly the expected digests is reused; otherwise every input is
    read, verified against its pin and published together. ``error`` is the
    exception class raised for a missing or mismatching input.
    """
    expected = dict(expected)
    directory = store.open_subdir(cache_dir, "raw")
    try:
        with store.exclusive_writer(directory):
            try:
                generation, current = store.resolve(cache_dir / "raw", pointer)
            except store.StoreError:
                current = None
            else:
                generation.close()
            if current is not None and current.get("digests") == expected:
                return current
            payloads = {}
            for name, digest in sorted(expected.items()):
                data = _read_input(directory, name, files, urls, error)
                actual = hashlib.sha256(data).hexdigest()
                if actual != digest:
                    raise error(
                        f"{name}: sha256 {actual} does not match the pinned {digest}"
                    )
                payloads[name] = data
            published = {
                **(manifest or {}),
                "urls": {
                    name: None if files and name in files else urls.get(name)
                    for name in sorted(expected)
                },
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "raw",
                pointer,
                {name: (lambda data=data: [data]) for name, data in payloads.items()},
                published,
                held=directory,
            )
    finally:
        directory.close()


def resolve(cache_dir, *, pointer, expected, error=PinnedInputError):
    """``(generation, manifest)`` of the published inputs, which must carry
    exactly ``expected``: a generation prepared under other pins is refused
    rather than parsed. The caller closes the generation after reading."""
    generation, manifest = store.resolve(cache_dir / "raw", pointer)
    if manifest.get("digests") != dict(expected):
        generation.close()
        raise error(f"raw/{pointer} holds other inputs than this build's pins")
    return generation, manifest


def derive(cache_dir, *, pointer, sources, build):
    """Ensure ``raw/<pointer>`` holds an artifact derived once from verified
    inputs; return its manifest. ``sources`` maps each input name to the
    digest it was read at: a generation that resolves — every artifact hashed
    — with the same ``sources`` in its manifest is reused, otherwise
    ``build()`` returns ``(artifacts, fields)``, the artifacts to publish and
    the manifest fields beside ``sources`` and ``retrieved_at``."""
    sources = dict(sources)
    directory = store.open_subdir(cache_dir, "raw")
    try:
        with store.exclusive_writer(directory):
            try:
                generation, current = store.resolve(cache_dir / "raw", pointer)
            except store.StoreError:
                current = None
            else:
                generation.close()
            if current is not None and current.get("sources") == sources:
                return current
            artifacts, fields = build()
            return store.publish(
                cache_dir / "raw",
                pointer,
                artifacts,
                {
                    **fields,
                    "sources": sources,
                    "retrieved_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                },
                held=directory,
            )
    finally:
        directory.close()
