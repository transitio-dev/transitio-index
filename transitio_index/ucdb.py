"""GHS-UCDB names for the FAO urban centres: the pinned inputs and the
build-time match that names a FAO city-region after the GHS Urban Centre
Database centre its urban centre overlaps.

The FAO record numbers its centres and names none, while the GHS Urban
Centre Database (JRC, CC BY 4.0) names every urban centre above 50 000
people. The two are matched by geometry, never by id: a FAO centre takes the
name of the UCDB centre covering the largest share of its area, provided that
share reaches ``MIN_SHARE``; a runner-up with at least ``AMBIGUITY`` of the
best share makes the match ambiguous. Towns below the UCDB threshold stay
unnamed, for the curator to name from the member cities.
"""

import collections

import shapely

from transitio_index import fao, geometry, pinned, store

CENTRES_FILE = "Tier_1_Urban_Centres.shp.zip"  # every FAO centre, tiers 1 to 4
UCDB_FILE = "GHS_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A_V1_2.zip"
UCDB_URL = (
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_UCDB_GLOBE_R2024A/"
    "GHS_UCDB_THEME_GLOBE_R2024A/GHS_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A/"
    f"V1-2/{UCDB_FILE}"
)
UCDB_MEMBER = "GHS_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A.gpkg"
UCDB_LAYER = "GHSL_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A"
UCDB_ID = "ID_UC_G0"
UCDB_NAME = "GC_UCN_MAI_2025"
UCDB_NAMES = "GC_UCN_LIS_2025"
UCDB_COUNTRY = "GC_CNT_GAD_2025"
UCDB_POPULATION = "GC_POP_TOT_2025"
UCDB_CAPITAL = "GC_UCM_CAP"
POINTER = "ucdb.json"
NAMES_POINTER = "fao-names.json"
NAMES_FILE = "centre_names.jsonl"
RELEASE = "R2024A V1-2"
DOI = "10.2905/1a338be6-7eaf-480c-9664-3a8ade88cbcd"
LICENCE = "CC-BY-4.0"
DERIVED = geometry.UCDB_DERIVED
EQUAL_AREA = "EPSG:6933"
MIN_SHARE = 0.1
AMBIGUITY = 0.5
# The centres seeded as places: national capitals and cities this large.
MIN_POPULATION = 200_000

# The bytes verified live on 2026-09-07; a fetch that differs is refused.
PINS = {
    CENTRES_FILE: "a0ecab66d7dfe597f8ae857077d160e2f7639621822e7f077dcd38f0674b6ef1",
    UCDB_FILE: "bc879d82320504f89df2041b7936221c8239cd808abe93980493aed062b4f3d6",
}
URLS = {CENTRES_FILE: fao.RECORD_FILES.format(name=CENTRES_FILE), UCDB_FILE: UCDB_URL}


class UcdbError(pinned.PinnedInputError):
    """A pinned UCDB or FAO urban-centre input is missing, altered or inconsistent."""


def prepare_inputs(cache_dir, *, files=None, expected=PINS):
    """Ensure ``raw/ucdb.json`` holds the pinned inputs and ``raw/fao-names.json``
    the centre names matched from them; returns the inputs' manifest."""
    manifest = pinned.prepare(
        cache_dir,
        pointer=POINTER,
        urls=URLS,
        expected=expected,
        files=files,
        manifest={
            "source": "ghs-ucdb",
            "release": RELEASE,
            "doi": DOI,
            "license": LICENCE,
        },
        error=UcdbError,
    )
    match_centres(cache_dir, expected=expected)
    return manifest


def _polygons(frame, name, crs=EQUAL_AREA):
    """The frame's geometries in ``crs`` (the equal-area projection by
    default) as valid polygons: repaired where the source's are not, refused
    where missing, empty, of another type or without area."""
    geoms = frame.geometry
    if geoms.isna().any() or geoms.is_empty.any():
        raise UcdbError(f"{name}: a geometry is missing or empty")
    if not set(geoms.geom_type) <= {"Polygon", "MultiPolygon"}:
        raise UcdbError(f"{name}: a geometry is not a polygon")
    valid = shapely.make_valid(geoms.to_crs(crs).to_numpy())
    if (shapely.area(valid) <= 0).any():
        raise UcdbError(f"{name}: a geometry has no area")
    return valid


def _centre_frame(data):
    """``(frame, ids, types)`` of the FAO urban centres in the zipped
    shapefile bytes: unique integer ids, a type in 1–4."""
    frame = fao.read_zipped(data, CENTRES_FILE, error=UcdbError)
    missing = [c for c in ("id", "type") if c not in frame.columns]
    if missing:
        raise UcdbError(f"{CENTRES_FILE}: missing columns {missing}")
    ids = fao.integer_ids(frame, "id", CENTRES_FILE, error=UcdbError)
    if len(set(ids)) != len(ids):
        raise UcdbError(f"{CENTRES_FILE}: centre ids are not unique")
    types = fao.integer_ids(frame, "type", CENTRES_FILE, error=UcdbError)
    if not set(types) <= set(fao.TIERS):
        raise UcdbError(f"{CENTRES_FILE}: a centre type is not a tier")
    return frame, ids, types


def read_centres(data):
    """``[{"id", "type", "geom"}]`` for every FAO urban centre in the zipped
    shapefile bytes: unique integer ids, a type in 1–4, polygons."""
    frame, ids, types = _centre_frame(data)
    geoms = _polygons(frame, CENTRES_FILE)
    return [
        {"id": int(i), "type": int(t), "geom": g} for i, t, g in zip(ids, types, geoms)
    ]


def centre_points(cache_dir, *, expected=PINS):
    """``{centre_id: Point}``: a point on each FAO urban centre's polygon, in
    WGS84, read from the pinned inputs. A FAO region's id is its centre's."""
    generation, _ = pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=UcdbError
    )
    with generation:
        data = generation.read_bytes(CENTRES_FILE)
    frame, ids, _ = _centre_frame(data)
    points = shapely.point_on_surface(_polygons(frame, CENTRES_FILE, "EPSG:4326"))
    return {str(i): point for i, point in zip(ids, points)}


def _text(value):
    """A stripped non-empty string, else None (NaN and blanks alike)."""
    if value is None or value != value:
        return None
    value = str(value).strip()
    return value or None


def _count(value):
    """A number rounded to an int, else None (NaN and missing alike)."""
    if value is None or value != value:
        return None
    return int(round(float(value)))


def read_ucdb(data, crs=EQUAL_AREA):
    """``(centres, nameless)``: ``[{"id", "name", "names", "country",
    "population", "capital", "geom"}]`` for every named UCDB centre in the
    zipped GeoPackage bytes — unique integer ids, the 2025 population rounded,
    polygons in ``crs`` — and the count of nameless ones dropped."""
    frame = fao.read_zipped(
        data, UCDB_FILE, member=UCDB_MEMBER, layer=UCDB_LAYER, error=UcdbError
    )
    columns = (
        UCDB_ID,
        UCDB_NAME,
        UCDB_NAMES,
        UCDB_COUNTRY,
        UCDB_POPULATION,
        UCDB_CAPITAL,
    )
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise UcdbError(f"{UCDB_FILE}: missing columns {missing}")
    ids = fao.integer_ids(frame, UCDB_ID, UCDB_FILE, error=UcdbError)
    if len(set(ids)) != len(ids):
        raise UcdbError(f"{UCDB_FILE}: centre ids are not unique")
    geoms = _polygons(frame, UCDB_FILE, crs)
    centres = []
    nameless = 0
    for i, name, names, country, population, capital, geom in zip(
        ids, *(frame[column] for column in columns[1:]), geoms
    ):
        name = _text(name)
        if name is None:
            nameless += 1
            continue
        listed = [_text(n) for n in (_text(names) or "").split(";")]
        centres.append(
            {
                "id": int(i),
                "name": name,
                "names": [n for n in listed if n],
                "country": _text(country),
                "population": _count(population),
                "capital": bool(capital == 1),
                "geom": geom,
            }
        )
    return centres, nameless


def qualifies(centre):
    """Whether a centre is seeded as a place: a capital, or at least
    ``MIN_POPULATION`` people."""
    return centre["capital"] or (centre["population"] or 0) >= MIN_POPULATION


def match(centres, ucdb):
    """One row per FAO centre a UCDB centre names — the one covering the
    largest share of its area, at least ``MIN_SHARE``; ``ambiguous`` when a
    runner-up reaches ``AMBIGUITY`` of that share; every candidate at or above
    ``MIN_SHARE`` listed by falling share. Centres without one are absent."""
    tree = shapely.STRtree([c["geom"] for c in ucdb])
    rows = []
    for centre in centres:
        area = shapely.area(centre["geom"])
        candidates = []
        for index in tree.query(centre["geom"], predicate="intersects"):
            overlap = shapely.intersection(centre["geom"], ucdb[index]["geom"])
            share = shapely.area(overlap) / area
            if share >= MIN_SHARE:
                candidates.append((float(share), ucdb[index]))
        if not candidates:
            continue
        candidates.sort(key=lambda item: (-item[0], item[1]["id"]))
        (share, best), runners = candidates[0], candidates[1:]
        rows.append(
            {
                "centre_id": str(centre["id"]),
                "type": centre["type"],
                "ucdb_id": best["id"],
                "name": best["name"],
                "names": best["names"],
                "country": best["country"],
                "share": round(share, 4),
                "ambiguous": bool(runners) and runners[0][0] >= AMBIGUITY * share,
                "candidates": [
                    {"ucdb_id": c["id"], "name": c["name"], "share": round(s, 4)}
                    for s, c in candidates
                ],
            }
        )
    return rows


def match_centres(cache_dir, *, expected=PINS):
    """Publish ``raw/fao-names.json`` holding the matched centre names, computed
    once from the verified inputs and reused while its manifest names the
    same source digests. Returns the manifest."""
    generation, manifest = pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=UcdbError
    )
    with generation:
        centres_data = generation.read_bytes(CENTRES_FILE)
        ucdb_data = generation.read_bytes(UCDB_FILE)

    def build():
        centres = read_centres(centres_data)
        ucdb, nameless = read_ucdb(ucdb_data)
        rows = match(centres, ucdb)
        by_type = collections.Counter(row["type"] for row in rows)
        return {NAMES_FILE: store.jsonl_chunks(rows)}, {
            "source": "ghs-ucdb-names",
            "release": RELEASE,
            "doi": DOI,
            "license": LICENCE,
            "min_share": MIN_SHARE,
            "ambiguity": AMBIGUITY,
            "centres": len(centres),
            "named": len(rows),
            "ambiguous": sum(row["ambiguous"] for row in rows),
            "named_by_type": {str(t): by_type.get(t, 0) for t in fao.TIERS},
            "nameless_ucdb": nameless,
        }

    return pinned.derive(
        cache_dir, pointer=NAMES_POINTER, sources=manifest["digests"], build=build
    )


def load_names(cache_dir, *, expected=PINS):
    """``({centre_id: row}, manifest)`` of the matched names, refused unless
    they derive from this build's pinned inputs."""
    generation, manifest = pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=UcdbError
    )
    generation.close()
    rows, names_manifest = store.read_jsonl(
        cache_dir / "raw", NAMES_POINTER, NAMES_FILE
    )
    if names_manifest.get("sources") != manifest["digests"]:
        raise UcdbError(
            f"raw/{NAMES_POINTER} derives from other inputs than this build's pins"
        )
    return {row["centre_id"]: row for row in rows}, names_manifest
