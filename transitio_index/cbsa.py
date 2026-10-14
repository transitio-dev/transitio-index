"""The US metropolitan statistical areas' official counties: OMB's delineation.

The Census Bureau's ``list1_2023.xlsx`` (OMB's delineation of July 2023)
lists every county of every core-based statistical area by its FIPS code.
It is fetched once, checked against its pinned checksum and published as the
``raw/cbsa.json`` generation; the reader parses that generation's verified
bytes. A US metro of the index is a metropolitan statistical area keyed by
its CBSA code, so its official area is the counties the delineation lists
under that code.
"""

import io

from transitio_index import pinned

POINTER = "cbsa.json"
DELINEATION_FILE = "list1_2023.xlsx"
DELINEATION_URL = (
    "https://www2.census.gov/programs-surveys/metro-micro/geographies/"
    "reference-files/2023/delineation-files/" + DELINEATION_FILE
)
# The first two lines are titles; the header is the third.
HEADER_ROW = 3
COLUMNS = {
    "CBSA Code": "cbsa",
    "Metropolitan/Micropolitan Statistical Area": "kind",
    "FIPS State Code": "state",
    "FIPS County Code": "county",
}
METROPOLITAN = "Metropolitan Statistical Area"

# The bytes verified live on 2026-10-14; a fetch that differs is refused.
PINS = {
    DELINEATION_FILE: (
        "952c4b1e78acbb54e6ec9412434b7602fedacbf021736351a63c181bdb753629"
    ),
}
URLS = {DELINEATION_FILE: DELINEATION_URL}
# The derived input a US metro's official area consumes, as an allowlist key.
DERIVED = ("OMB CBSA delineation July 2023", "US-PD")


class CbsaError(pinned.PinnedInputError):
    """The pinned delineation is missing, altered or not the table it was."""


def prepare_inputs(cache_dir, *, files=None, expected=PINS):
    """Ensure ``raw/cbsa.json`` holds the pinned delineation; return its
    manifest. See :func:`pinned.prepare`."""
    return pinned.prepare(
        cache_dir,
        pointer=POINTER,
        urls=URLS,
        expected=expected,
        files=files,
        manifest={"source": "cbsa"},
        error=CbsaError,
    )


def load_inputs(cache_dir, *, expected=PINS):
    """``(counties, manifest)``: :func:`read_delineation` over the verified
    bytes of the generation published under ``expected``."""
    generation, manifest = pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=CbsaError
    )
    with generation:
        counties = read_delineation(generation.read_bytes(DELINEATION_FILE))
    return counties, manifest


def _cell(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def read_delineation(data):
    """``{cbsa code: sorted county FIPS}`` — five digits, the state's two
    then the county's three — for the metropolitan statistical areas;
    micropolitan rows are left out. A workbook without the header the table
    has always had, or a metropolitan row without its codes, is refused."""
    import openpyxl

    try:
        workbook = openpyxl.load_workbook(
            io.BytesIO(data), read_only=True, data_only=True
        )
    except Exception as error:  # noqa: B902 - openpyxl raises its own hierarchy
        raise CbsaError(f"{DELINEATION_FILE}: not a workbook: {error}") from None
    try:
        rows = workbook.active.iter_rows(min_row=HEADER_ROW, values_only=True)
        header = [_cell(value) for value in next(rows, ())]
        if not set(COLUMNS) <= set(header):
            raise CbsaError(f"{DELINEATION_FILE}: header {header!r}")
        index = {COLUMNS[name]: header.index(name) for name in COLUMNS}
        counties = {}
        for position, row in enumerate(rows, HEADER_ROW + 1):
            cells = {
                key: _cell(row[i]) if i < len(row) else None for key, i in index.items()
            }
            # The notes below the table carry no code.
            if cells["cbsa"] is None or not cells["cbsa"].isdigit():
                continue
            if cells["kind"] != METROPOLITAN:
                continue
            state, county = cells["state"], cells["county"]
            if not (state and county and state.isdigit() and county.isdigit()):
                raise CbsaError(
                    f"{DELINEATION_FILE}: row {position}: FIPS {state!r} {county!r}"
                )
            fips = state.zfill(2) + county.zfill(3)
            counties.setdefault(cells["cbsa"], set()).add(fips)
    finally:
        workbook.close()
    if not counties:
        raise CbsaError(f"{DELINEATION_FILE}: no metropolitan statistical areas")
    return {code: sorted(fips) for code, fips in sorted(counties.items())}
