#!/usr/bin/env python3

"""Flag the cities of a published index whose area disagrees with Wikidata.

A maintainer check, not part of the build: it reads a published index
(default ``cache/merged/index``) through ``builds.load_tables``, measures each
city's geodesic area and compares it with the area (P2046) of the city's
Wikidata entity. A city is flagged ``larger`` or ``smaller`` when its area is
more than ``--ratio`` times the nearest of its entity's areas, or less than
the inverse, and ``no_qid`` when it has no QID; a QID without an area, or a
city without a polygon, is not judged. The flagged cities go to the ``--out``
CSV — the area mismatches first, furthest off first, then the cities without
a QID, largest first — and the count per flag and country to stdout.

It reads the published index, not a stage's output, because only the index
holds the polygons the licence stage derives from feeds.
"""

import argparse
import collections
import csv
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyproj
import shapely

from transitio_index import builds, overture

DEFAULT_INDEX = Path("cache/merged/index")
PLACE_COLUMNS = (
    "place_id",
    "name",
    "country_code",
    "wikidata_id",
    "geometry_source",
    "geometry",
)
COLUMNS = (
    "place_id",
    "name",
    "country_code",
    "wikidata_id",
    "geometry_source",
    "area_km2",
    "wikidata_km2",
    "ratio",
    "flag",
)
FLAGS = ("larger", "smaller", "no_qid")
FORMULA_OPENERS = ("=", "+", "-", "@", "\t", "\r")
_GEOD = pyproj.Geod(ellps="WGS84")


def _ring_km2(ring):
    """The geodesic area a ring encloses in km², whatever its winding."""
    return abs(_GEOD.polygon_area_perimeter(*ring.xy)[0]) / 1e6


def area_km2(wkb):
    """The geodesic area of a WKB geometry's polygons in km²: each exterior
    less its holes, every ring measured unsigned, since a ring's winding in
    longitude and latitude need not be its geodesic one (across the
    antimeridian it is reversed)."""
    return sum(
        _ring_km2(part.exterior) - sum(_ring_km2(hole) for hole in part.interiors)
        for part in shapely.get_parts(shapely.from_wkb(wkb))
        if part.geom_type == "Polygon" and not part.is_empty
    )


def _spread(area, other):
    """How many times the larger of two areas is the smaller."""
    low = min(area, other)
    return max(area, other) / low if low > 0 else math.inf


def judge(place, areas, ratio):
    """The report row for a city, or None when it is not flagged.

    ``areas`` maps a QID to its Wikidata areas in km². The city's area is
    compared with the nearest of its QID's: ``larger`` or ``smaller`` when the
    two are more than ``ratio`` times apart. A city without a QID is
    ``no_qid``; one whose QID has no area, or that has no polygon to measure,
    is not judged.
    """
    qid = place.get("wikidata_id")
    if qid and not areas.get(qid):
        return None
    area = area_km2(place.get("geometry"))
    if not area > 0:
        return None
    row = {
        "place_id": place["place_id"],
        "name": place.get("name"),
        "country_code": place.get("country_code"),
        "wikidata_id": qid,
        "geometry_source": place.get("geometry_source"),
        "area_km2": area,
        "wikidata_km2": None,
        "ratio": None,
        "flag": "no_qid",
    }
    if qid:
        nearest = min(areas[qid], key=lambda km2: _spread(area, km2))
        if _spread(area, nearest) <= ratio:
            return None
        row["wikidata_km2"] = nearest
        row["ratio"] = area / nearest
        row["flag"] = "larger" if area > nearest else "smaller"
    return row


def _order(row):
    """Area mismatches by how far off they are, then the cities without a QID
    by area, each largest first; ties by place id."""
    if row["flag"] == "no_qid":
        return (1, -row["area_km2"], row["place_id"])
    return (0, -_spread(row["ratio"], 1.0), row["place_id"])


def _cities(places, countries=None):
    """The city rows of a places table, of ``countries`` when given, with only
    the columns the check reads."""
    keep = pc.equal(places["kind"], "city")
    if countries:
        codes = pa.array(sorted({code.strip().upper() for code in countries}))
        keep = pc.and_(keep, pc.is_in(places["country_code"], value_set=codes))
    places = places.filter(keep)
    columns = [name for name in PLACE_COLUMNS if name in places.column_names]
    return places.select(columns).to_pylist()


def _cell(key, value):
    """A row's value as written: areas to three decimals, the ratio to four
    significant digits, and text opening with a formula or control character
    quoted with a leading apostrophe, so a spreadsheet does not run it."""
    if isinstance(value, float):
        return float(f"{value:.4g}") if key == "ratio" else round(value, 3)
    if isinstance(value, str) and value.startswith(FORMULA_OPENERS):
        return "'" + value
    return value


def _summary(rows):
    """One line per flag: its count and the count per country, most first."""
    counts = collections.Counter((row["flag"], row["country_code"]) for row in rows)
    lines = []
    for flag in FLAGS:
        per = sorted(
            ((code or "?", n) for (kind, code), n in counts.items() if kind == flag),
            key=lambda item: (-item[1], item[0]),
        )
        line = f"{flag}: {sum(n for _, n in per)}"
        if per:
            line += " (" + ", ".join(f"{code} {n}" for code, n in per) + ")"
        lines.append(line)
    return lines


def main(argv=None, wikidata=None):
    parser = argparse.ArgumentParser(
        prog="python scripts/check_place_geometry.py",
        description="Flag the cities whose area disagrees with their Wikidata entity",
    )
    parser.add_argument(
        "--index",
        type=Path,
        default=DEFAULT_INDEX,
        help="a published index directory (default: cache/merged/index)",
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        metavar="CC",
        help="check only these ISO country codes (default: every country)",
    )
    parser.add_argument(
        "--ratio",
        type=float,
        default=10.0,
        help="flag an area this many times off Wikidata's (default: 10)",
    )
    parser.add_argument("--out", type=Path, required=True, help="the CSV to write")
    args = parser.parse_args(argv)
    if not args.ratio > 1:
        parser.error("--ratio must be above 1")

    loaded = builds.load_tables(args.index)
    if loaded is None:
        raise SystemExit(f"{args.index}: no verified index (missing or mid-publish)")
    cities = _cities(loaded[2]["places.parquet"], args.countries)
    wikidata = wikidata or overture.WikidataClient()
    areas = wikidata.areas(place.get("wikidata_id") for place in cities)
    rows = [row for place in cities if (row := judge(place, areas, args.ratio))]
    rows.sort(key=_order)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(
            {key: _cell(key, value) for key, value in row.items()} for row in rows
        )
    with_area = sum(1 for place in cities if areas.get(place.get("wikidata_id")))
    print(f"cities: {len(cities)} ({with_area} with a Wikidata area)")
    for line in _summary(rows):
        print(line)
    print(f"wrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
