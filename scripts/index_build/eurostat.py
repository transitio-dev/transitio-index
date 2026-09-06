"""Eurostat metropolitan regions: the pinned inputs.

The composition table (Eurostat's ``NUTS2021.xlsx``, whose ``Metropolitan``
sheet lists each NUTS-3 region with the metro code it belongs to, if any) and
the GISCO NUTS-3 polygons of the same NUTS revision are fetched once, checked
against pinned checksums and published together as the ``raw/eurostat.json``
generation. Every later reader takes the generation's verified bytes, so what
was hashed under this build's pins is what gets parsed.
"""

import datetime
import hashlib
import os

from index_build import csv_source, store

NUTS_VERSION = "2021"
NUTS_SCALE = "01M"
POINTER = "eurostat.json"

COMPOSITION_FILE = "NUTS2021.xlsx"
COMPOSITION_URL = "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx"
BOUNDARIES_FILE = f"NUTS_RG_{NUTS_SCALE}_{NUTS_VERSION}_4326_LEVL_3.parquet"
BOUNDARIES_URL = (
    "https://gisco-services.ec.europa.eu/distribution/v2/nuts/parquet/"
    + BOUNDARIES_FILE
)

# The bytes verified live on 2026-09-06; a fetch that differs is refused.
PINS = {
    COMPOSITION_FILE: (
        "b17dcc379bb3586550ec3b16f3f474dd2a8a55b4cfdcdc1e1575097f1c2d4761"
    ),
    BOUNDARIES_FILE: (
        "0356e77f0903b101a03b29ddb76a6fcdb8314405048333beb8f2dcba0b6fb701"
    ),
}
URLS = {COMPOSITION_FILE: COMPOSITION_URL, BOUNDARIES_FILE: BOUNDARIES_URL}
MAX_INPUT_BYTES = 64 * 1024 * 1024


class EurostatError(RuntimeError):
    """A pinned Eurostat input is missing, altered or inconsistent."""


def _read_input(directory, name, files):
    """The bytes of one input: a caller-supplied local file, else its download."""
    if files is not None and name in files:
        try:
            handle = store.open_regular_path(files[name])
        except store.StoreError as error:
            raise EurostatError(str(error)) from None
        try:
            return store.read_all(handle, MAX_INPUT_BYTES)
        finally:
            os.close(handle)
    if name not in URLS:
        raise EurostatError(f"{name}: no pinned URL and no local file")
    csv_source.download_file(directory, name, URLS[name], limit=MAX_INPUT_BYTES)
    try:
        return store.read_bytes(directory, name)
    finally:
        store.unlink(directory, name)


def prepare_inputs(cache_dir, *, files=None, expected=PINS):
    """Ensure ``raw/eurostat.json`` holds the pinned inputs; return its manifest.

    ``files`` maps an input name to a local path used instead of its URL
    (how tests and offline builds run); ``expected`` is each input's SHA-256.
    A generation that resolves — every artifact hashed — under exactly those
    digests is reused; otherwise every input is read, verified against its
    pin and published together, so a later build parses identical bytes or
    refuses.
    """
    expected = dict(expected)
    directory = store.open_subdir(cache_dir, "raw")
    try:
        with store.exclusive_writer(directory):
            try:
                generation, current = store.resolve(cache_dir / "raw", POINTER)
            except store.StoreError:
                current = None
            else:
                generation.close()
            if current is not None and current.get("digests") == expected:
                return current
            payloads = {}
            for name, digest in sorted(expected.items()):
                data = _read_input(directory, name, files)
                actual = hashlib.sha256(data).hexdigest()
                if actual != digest:
                    raise EurostatError(
                        f"{name}: sha256 {actual} does not match the pinned {digest}"
                    )
                payloads[name] = data
            manifest = {
                "source": "eurostat",
                "nuts_version": NUTS_VERSION,
                "urls": {
                    name: None if files and name in files else URLS.get(name)
                    for name in sorted(expected)
                },
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "raw",
                POINTER,
                {name: (lambda data=data: [data]) for name, data in payloads.items()},
                manifest,
                held=directory,
            )
    finally:
        directory.close()


def resolve_inputs(cache_dir, *, expected=PINS):
    """``(generation, manifest)`` of the published inputs, which must carry
    exactly ``expected``: a generation prepared under other pins is refused
    rather than parsed. The caller closes the generation after reading."""
    generation, manifest = store.resolve(cache_dir / "raw", POINTER)
    if manifest.get("digests") != dict(expected):
        generation.close()
        raise EurostatError(f"raw/{POINTER} holds other inputs than this build's pins")
    return generation, manifest
