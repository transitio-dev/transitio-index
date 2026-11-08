"""US Census place outlines: the boundary a US city ships when its Overture
division has none.

The Census Bureau's cartographic boundary file of places (2024, 1:500,000) —
incorporated places and census-designated places, a work of the US federal
government in the public domain — is fetched once, checked against its
pinned checksum and published as the ``raw/tiger.json`` generation; the
reader parses that generation's verified bytes. A city takes the outline of
the one place that holds its point and carries its name: a point inside a
larger city it is a neighbourhood of, or two places of its name, leave it
without.
"""

import re
import unicodedata

import shapely

from transitio_index import fao, geometry, pinned, ucdb

POINTER = "tiger.json"
PLACES_FILE = "cb_2024_us_place_500k.zip"
PLACES_URL = "https://www2.census.gov/geo/tiger/GENZ2024/shp/" + PLACES_FILE
COLUMNS = ("GEOID", "NAME", "STUSPS")

# The bytes verified live on 2026-11-08; a fetch that differs is refused.
PINS = {
    PLACES_FILE: "2be68094759b710979f41afd1996cf0132eec78350dfef45566a65478a6944b3",
}
URLS = {PLACES_FILE: PLACES_URL}
DERIVED = geometry.TIGER_DERIVED


class TigerError(pinned.PinnedInputError):
    """The pinned places file is missing, altered or not the file it was."""


def prepare_inputs(cache_dir, *, files=None, expected=PINS):
    """Ensure ``raw/tiger.json`` holds the pinned places file; return its
    manifest. See :func:`pinned.prepare`."""
    return pinned.prepare(
        cache_dir,
        pointer=POINTER,
        urls=URLS,
        expected=expected,
        files=files,
        manifest={"source": "tiger", "license": DERIVED[1]},
        error=TigerError,
    )


def load_places(cache_dir, *, expected=PINS):
    """``(places, manifest)``: :func:`read_places` over the verified bytes of
    the generation published under ``expected``."""
    data, manifest = pinned.read_input(
        cache_dir,
        pointer=POINTER,
        expected=expected,
        name=PLACES_FILE,
        error=TigerError,
    )
    return read_places(data), manifest


def read_places(data):
    """``[{"geoid", "name", "state", "geometry"}]`` from the zipped
    shapefile bytes, the polygons in EPSG:4326. A file without its columns,
    with a repeated GEOID or with a geometry that is not a polygon with area
    is refused."""
    frame = fao.read_zipped(data, PLACES_FILE, error=TigerError)
    missing = [column for column in COLUMNS if column not in frame.columns]
    if missing:
        raise TigerError(f"{PLACES_FILE}: missing columns {missing}")
    if frame["GEOID"].duplicated().any():
        raise TigerError(f"{PLACES_FILE}: GEOIDs are not unique")
    polygons = ucdb._polygons(frame, PLACES_FILE, crs="EPSG:4326", error=TigerError)
    return [
        {"geoid": str(geoid), "name": str(name), "state": str(state), "geometry": geom}
        for geoid, name, state, geom in zip(
            frame["GEOID"], frame["NAME"], frame["STUSPS"], polygons
        )
    ]


def name_key(name):
    """The name as compared: casefolded, without accents, punctuation or
    spaces, "St." and "Saint" alike."""
    text = unicodedata.normalize("NFKD", name or "")
    text = "".join(c for c in text if not unicodedata.combining(c)).casefold()
    text = re.sub(r"\bst\b\.?", "saint", text)
    return re.sub(r"[^0-9a-z]", "", text)


def match_outlines(places, cities):
    """``{city id: place}`` for ``cities``, ``(id, name, x, y)`` tuples: the
    one place (:func:`read_places`) whose polygon holds the point and whose
    name has the city's :func:`name_key`; a city with no such place, or two,
    is left out."""
    tree = shapely.STRtree([place["geometry"] for place in places])
    found = {}
    for city_id, name, x, y in cities:
        key = name_key(name)
        if not key:
            continue
        hits = [
            places[i]
            for i in tree.query(shapely.Point(x, y), predicate="covered_by")
            if name_key(places[i]["name"]) == key
        ]
        if len(hits) == 1:
            found[city_id] = hits[0]
    return found
