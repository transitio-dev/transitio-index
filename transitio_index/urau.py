"""Eurostat functional urban areas: the pinned Urban Audit polygons, read into
the shapes the Eurostat assignment consumes.

A functional urban area is a city and the municipalities where at least 15%
of the employed residents commute into it (the EC–OECD definition); the
Urban Audit publishes one polygon per area. The GISCO shapefile is fetched
once, checked against a pinned checksum and published as the ``raw/urau.json``
generation; the reader parses the generation's verified bytes. Each area is
the one region of a metro of its own, so ``eurostat.assign`` places a city in
the area covering its footprint exactly as it places one in a NUTS-3 region.
"""

import re

import shapely

from transitio_index import eurostat, geometry, pinned

EDITION = "2024"
POINTER = "urau.json"
AREAS_FILE = f"URAU_RG_100K_{EDITION}_4326_FUA.shp.zip"
AREAS_URL = "https://gisco-services.ec.europa.eu/distribution/v2/urau/shp/" + AREAS_FILE
# The bytes verified live on 2026-09-16; a fetch that differs is refused.
PINS = {
    AREAS_FILE: ("70714e2b724e55a402c964c44e45582ac0ca835374d3db4009d81688190558db"),
}
URLS = {AREAS_FILE: AREAS_URL}
SUBTYPE = "functional urban area"
NAMESPACE = "eurostat_fua"
DERIVED = geometry.URAU_DERIVED
NUTS3_COLUMN = f"NUTS3_{EDITION}"
COLUMNS = ("URAU_CODE", "URAU_CATG", "CNTR_CODE", "URAU_NAME", NUTS3_COLUMN)
CATEGORY = "F"  # the file's other categories are cities and greater cities
# Country code, three digits, then F.
AREA_CODE = re.compile(r"\A([A-Z]{2})[0-9]{3}F\Z")
CROSS_BORDER = "CB"  # one country's part of an area astride a border
NUTS3_CODE = re.compile(r"\A([A-Z]{2})[0-9A-Z]{3}\Z")


class UrauError(pinned.PinnedInputError):
    """A pinned Urban Audit input is missing, altered or inconsistent."""


def prepare_inputs(cache_dir, *, files=None, expected=PINS):
    """Ensure ``raw/urau.json`` holds the pinned input; return its manifest.
    See :func:`pinned.prepare`."""
    return pinned.prepare(
        cache_dir,
        pointer=POINTER,
        urls=URLS,
        expected=expected,
        files=files,
        manifest={"source": "urau", "edition": EDITION},
        error=UrauError,
    )


def resolve_inputs(cache_dir, *, expected=PINS):
    """``(generation, manifest)`` of the published input under this build's
    pins; see :func:`pinned.resolve`."""
    return pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=UrauError
    )


def read_areas(data):
    """``(composition, boundaries)`` from the zipped shapefile, in the shapes
    ``eurostat.assign`` reads: ``{code: {"name", "country", "nuts3": [code]}}``
    — each area the one region of its own metro — and ``{code: polygon}``.

    The country is the gazetteer's ISO code (Eurostat's prefixes mapped); a
    part of an area astride a border, filed under ``CB``, takes it from the
    NUTS-3 region the file names for it. A file whose codes repeat or are not
    area codes, whose rows are not functional urban areas, whose country is
    missing or not the one its code names, whose cross-border part names no
    NUTS-3 region, whose names are missing or whose geometry is not a valid
    polygon refuses the build rather than changing what an area means.
    """
    from transitio_index import fao

    frame = fao.read_zipped(data, AREAS_FILE, error=UrauError)
    missing = [column for column in COLUMNS if column not in frame.columns]
    if missing:
        raise UrauError(f"{AREAS_FILE}: columns missing {missing}")
    if frame.crs.to_epsg() != 4326:
        raise UrauError(f"{AREAS_FILE}: CRS {frame.crs.to_string()}")
    composition, boundaries = {}, {}
    for row in frame.itertuples(index=False):
        code = eurostat._cell(row.URAU_CODE)
        match = AREA_CODE.match(code or "")
        if match is None:
            raise UrauError(f"{AREAS_FILE}: area code {code!r}")
        if code in composition:
            raise UrauError(f"{AREAS_FILE}: {code} twice")
        if eurostat._cell(row.URAU_CATG) != CATEGORY:
            raise UrauError(f"{AREAS_FILE}: {code} is not a functional urban area")
        name = eurostat._cell(row.URAU_NAME)
        if name is None:
            raise UrauError(f"{AREAS_FILE}: {code} has no name")
        repaired = None
        if row.geometry is not None:
            repaired = geometry._repaired_polygon(shapely.force_2d(row.geometry))
        if repaired is None:
            raise UrauError(f"{AREAS_FILE}: {code} is not a valid polygon")
        prefix = eurostat._cell(row.CNTR_CODE)
        if prefix is None:
            raise UrauError(f"{AREAS_FILE}: {code} has no country")
        if prefix != match.group(1):
            raise UrauError(f"{AREAS_FILE}: {code} is filed under country {prefix!r}")
        if prefix == CROSS_BORDER:
            # The file draws the part of a cross-border area outside its
            # core's country as a row of its own, in a NUTS-3 region it names.
            nuts3 = eurostat._cell(getattr(row, NUTS3_COLUMN))
            if nuts3 is None:
                raise UrauError(f"{AREAS_FILE}: {code} has no NUTS-3 region")
            region = NUTS3_CODE.match(nuts3)
            if region is None:
                raise UrauError(f"{AREAS_FILE}: {code} is in NUTS-3 region {nuts3!r}")
            prefix = region.group(1)
        country = eurostat.EUROSTAT_COUNTRY.get(prefix, prefix)
        composition[code] = {"name": name, "country": country, "nuts3": [code]}
        boundaries[code] = repaired
    if not composition:
        raise UrauError(f"{AREAS_FILE}: no areas")
    return composition, boundaries


def load_inputs(cache_dir, *, expected=PINS):
    """``(composition, boundaries, manifest)`` parsed from the verified bytes
    of the generation :func:`resolve_inputs` accepts."""
    generation, manifest = resolve_inputs(cache_dir, expected=expected)
    with generation:
        composition, boundaries = read_areas(generation.read_bytes(AREAS_FILE))
    return composition, boundaries, manifest
