"""FAO multi-tier city-regions at the 1-hour cutoff: the pinned inputs and
the suggested-curation report for metros the official sources do not cover.

The city-region patches (a zipped shapefile, converted once to GeoParquet
from the verified bytes) and the regions table are pinned like the Eurostat
inputs. Regions nest — each is one urban centre's patch set at its tier — so
a patch's region is that of its highest-tier centre. Gazetteer cities with no
metro and no known official assignment are joined to their patch's region and
grouped, one report entry per region, for a curator to publish through the
``set_statistical_area`` crosswalk. Nothing is minted here.
"""

import collections
import datetime
import json
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from index_build import csv_source, eurostat, geometry, overrides, pinned, store

CUTOFF_HOURS = 1
DOI = "10.5281/zenodo.11187634"
RECORD_FILES = "https://zenodo.org/api/records/11187634/files/{name}/content"
PATCHES_FILE = f"City_Region_Patches_{CUTOFF_HOURS}h.shp.zip"
REGIONS_FILE = f"City_Regions_{CUTOFF_HOURS}h.csv"
POINTER = "fao.json"
CONVERTED_POINTER = "fao-patches.json"
PATCHES_PARQUET = "patches.parquet"
LICENCE = "CC-BY-4.0"
CREDIT = (
    "Girgin, Cattaneo, de By, McMenomy, Nelson and Vaz (2024), Worldwide "
    "Delineation of Multi-Tier City-Regions (Zenodo), CC BY 4.0"
)

# The bytes verified live on 2026-09-06; a fetch that differs is refused.
PINS = {
    PATCHES_FILE: ("c25044aa9d4821a57cb97b3fa9f2ba7a6b2adb6893c27b300d8fd4f72a6ab077"),
    REGIONS_FILE: ("e8eab0eebeb6d2e11b0657eeec17a147b35900a911d9c75d0f6e83866b91b22f"),
}
URLS = {name: RECORD_FILES.format(name=name) for name in PINS}
REGION_COLUMNS = {"id", "country", "tier", "patches", "category"}
CENTRE_COLUMNS = ("T1_id", "T2_id", "T3_id", "T4_id")
TIERS = (1, 2, 3, 4)


class FaoError(pinned.PinnedInputError):
    """A pinned FAO input is missing, altered or inconsistent."""


def prepare_inputs(cache_dir, *, files=None, expected=PINS):
    """Ensure ``raw/fao.json`` holds the pinned inputs and ``raw/fao-patches.json``
    their converted patches; returns the inputs' manifest."""
    manifest = pinned.prepare(
        cache_dir,
        pointer=POINTER,
        urls=URLS,
        expected=expected,
        files=files,
        manifest={
            "source": "fao",
            "doi": DOI,
            "cutoff_hours": CUTOFF_HOURS,
            "license": LICENCE,
        },
        error=FaoError,
    )
    convert_patches(cache_dir, expected=expected)
    return manifest


def read_zipped(data, name, *, member=None, error=FaoError, **options):
    """A GeoDataFrame, with its CRS, read from the bytes of a zip archive:
    the zipped shapefile itself, or ``member`` inside it. ``name`` labels
    the diagnostics only; the bytes land under a fixed scratch filename."""
    import geopandas

    with tempfile.TemporaryDirectory() as scratch:
        path = os.path.join(scratch, "input.zip")
        with open(path, "wb") as opened:
            opened.write(data)
        source = f"zip://{path}" if member is None else f"zip://{path}!{member}"
        try:
            frame = geopandas.read_file(source, **options)
        except Exception as exc:  # noqa: B902 - geopandas raises its own hierarchy
            raise error(f"{name}: not readable: {exc}") from None
    if frame.crs is None:
        raise error(f"{name}: no CRS")
    return frame


def integer_ids(frame, column, name, *, error=FaoError):
    """The column as exact int64 ids, never coerced: an integer column as it
    is, a float column only while every value is integral and within both
    the range its width represents exactly (2**53 for float64, 2**24 for
    float32) and int64, anything else refused — a rounded or overflowing
    value would otherwise pass the relational checks under the wrong
    identity."""
    import numpy
    import pandas

    values = frame[column]
    problem = error(f"{name}: {column} is not a column of integer ids")
    if values.isna().any():
        raise problem
    array = values.to_numpy()
    if pandas.api.types.is_integer_dtype(values):
        if (array > numpy.iinfo("int64").max).any():
            raise problem
    elif pandas.api.types.is_float_dtype(values):
        exact = min(2 ** (numpy.finfo(array.dtype).nmant + 1), 2**63)
        if (
            not numpy.isfinite(array).all()
            or (array != numpy.floor(array)).any()
            or (numpy.abs(array) >= exact).any()
        ):
            raise problem
    else:
        raise problem
    return array.astype("int64")


def _patches_table(data):
    """The patch polygons and their tier centres as an Arrow table with WKB
    geometry in EPSG:4326, read from the zipped shapefile bytes."""
    frame = read_zipped(data, PATCHES_FILE)
    if frame.crs.to_epsg() != 4326:
        frame = frame.to_crs(4326)
    missing = [c for c in ("id", *CENTRE_COLUMNS) if c not in frame.columns]
    if missing:
        raise FaoError(f"{PATCHES_FILE}: missing columns {missing}")
    columns = {
        column: pa.array(integer_ids(frame, column, PATCHES_FILE))
        for column in ("id", *CENTRE_COLUMNS)
    }
    ids = columns["id"].to_pylist()
    if len(set(ids)) != len(ids):
        raise FaoError(f"{PATCHES_FILE}: patch ids are not unique")
    geoms = frame.geometry
    if geoms.isna().any() or geoms.is_empty.any() or not geoms.is_valid.all():
        raise FaoError(f"{PATCHES_FILE}: a patch geometry is missing, empty or invalid")
    if not set(geoms.geom_type) <= {"Polygon", "MultiPolygon"}:
        raise FaoError(f"{PATCHES_FILE}: a patch is not a polygon")
    columns["geometry"] = pa.array(
        [shapely.to_wkb(shapely.force_2d(geom)) for geom in geoms], pa.binary()
    )
    geo = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(set(geoms.geom_type)),
                "crs": frame.crs.to_json_dict(),
            }
        },
    }
    return pa.table(columns).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode("utf-8")}
    )


def convert_patches(cache_dir, *, expected=PINS):
    """Publish ``raw/fao-patches.json`` holding the patches as GeoParquet,
    converted once from the verified shapefile bytes and reused while its
    manifest names the same source digest. Returns the manifest."""
    generation, manifest = pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=FaoError
    )
    with generation:
        data = generation.read_bytes(PATCHES_FILE)

    def build():
        table = _patches_table(data)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        payload = sink.getvalue().to_pybytes()
        return {PATCHES_PARQUET: lambda: [payload]}, {
            "source": "fao-patches",
            "patches": table.num_rows,
        }

    return pinned.derive(
        cache_dir,
        pointer=CONVERTED_POINTER,
        sources={PATCHES_FILE: manifest["digests"][PATCHES_FILE]},
        build=build,
    )


def read_regions(data):
    """``{region_id: {"tier", "country", "category", "patches"}}`` from the
    regions table, under the verified contract: unique digit ids, a tier in
    1–4, patch lists of digit ids, a category of P, S or T."""
    try:
        rows = csv_source.read_rows(data.decode("utf-8"), REGION_COLUMNS)
    except (UnicodeDecodeError, csv_source.IngestError) as error:
        raise FaoError(f"{REGIONS_FILE}: {error}") from None
    regions = {}
    for position, row in enumerate(rows, 2):
        region_id = row["id"].strip()
        if not region_id.isdigit():
            raise FaoError(f"{REGIONS_FILE}: row {position}: id {region_id!r}")
        if region_id in regions:
            raise FaoError(f"{REGIONS_FILE}: row {position}: id {region_id} twice")
        tier = row["tier"].strip()
        if tier not in {str(t) for t in TIERS}:
            raise FaoError(f"{REGIONS_FILE}: row {position}: tier {tier!r}")
        patches = [p.strip() for p in row["patches"].split(";") if p.strip()]
        if any(not p.isdigit() for p in patches):
            raise FaoError(f"{REGIONS_FILE}: row {position}: patch ids")
        category = row["category"].strip()
        if category not in {"P", "S", "T"}:
            raise FaoError(f"{REGIONS_FILE}: row {position}: category {category!r}")
        regions[region_id] = {
            "tier": int(tier),
            "country": row["country"].strip() or None,
            "category": category,
            "patches": patches,
        }
    if not regions:
        raise FaoError(f"{REGIONS_FILE}: no regions")
    return regions


def read_patches(data):
    """``{patch_id: {"centres": (T1_id, …, T4_id), "geom"}}`` from the
    converted GeoParquet bytes."""
    try:
        table = pq.read_table(pa.BufferReader(data))
    except Exception as error:  # noqa: B902 - pyarrow raises its own hierarchy
        raise FaoError(f"{PATCHES_PARQUET}: not readable: {error}") from None
    patches = {}
    for row in table.to_pylist():
        patches[str(row["id"])] = {
            "centres": tuple(int(row[column]) for column in CENTRE_COLUMNS),
            "geom": shapely.from_wkb(row["geometry"]),
        }
    return patches


def region_of(patch):
    """The id of the patch's highest-tier centre — its city-region."""
    for centre in reversed(patch["centres"]):
        if centre:
            return str(centre)
    return None


def load_inputs(cache_dir, *, expected=PINS):
    """``(regions, patches, manifest)`` parsed from the verified bytes under
    this build's pins. The verified contract is asserted on every run: every
    non-zero centre id of a patch is a region of at least that column's tier
    (a centre of tier t serves tiers 1 to t); every patch a region lists exists
    and names that region as its centre at the region's tier; the patches a
    primary (category P) region lists resolve to it as their highest-tier
    region — only the listed ones: on the pinned data 15 unlisted patches name
    a primary region at its tier yet belong to a higher-tier one; and the
    converted patches derive from the pinned shapefile."""
    generation, manifest = pinned.resolve(
        cache_dir, pointer=POINTER, expected=expected, error=FaoError
    )
    with generation:
        regions = read_regions(generation.read_bytes(REGIONS_FILE))
    converted, converted_manifest = store.resolve(cache_dir / "raw", CONVERTED_POINTER)
    with converted:
        if converted_manifest.get("sources") != {
            PATCHES_FILE: manifest["digests"][PATCHES_FILE]
        }:
            raise FaoError(f"raw/{CONVERTED_POINTER} was converted from other bytes")
        patches = read_patches(converted.read_bytes(PATCHES_PARQUET))
    for patch_id, patch in patches.items():
        for tier, centre in zip(TIERS, patch["centres"]):
            if not centre:
                continue
            region = regions.get(str(centre))
            if region is None or region["tier"] < tier:
                raise FaoError(
                    f"patch {patch_id}: tier-{tier} centre {centre} is not a "
                    f"region of tier {tier} or above"
                )
    for region_id, region in regions.items():
        column = region["tier"] - 1
        for patch_id in region["patches"]:
            patch = patches.get(patch_id)
            if patch is None:
                raise FaoError(
                    f"region {region_id}: patch {patch_id} is not in the patches file"
                )
            if str(patch["centres"][column]) != region_id:
                raise FaoError(
                    f"region {region_id}: patch {patch_id} does not list it as its "
                    f"tier-{region['tier']} centre"
                )
            if region["category"] == "P" and region_of(patch) != region_id:
                raise FaoError(
                    f"primary region {region_id}: patch {patch_id} belongs to the "
                    f"higher-tier region {region_of(patch)}"
                )
    return regions, patches, manifest


def _patch_of(footprint, containment, geoms, region_by_patch):
    """``(patch_id, ambiguous)`` for the patches covering the footprint's
    representative point, settled as NUTS-3 regions are: one candidate, or
    several of one region, or the one holding the larger share of the
    footprint; equal shares between regions are ambiguous."""
    hits = containment.regions_at(footprint.representative_point())
    return eurostat._pick(hits, footprint, geoms, region_by_patch)


def suggest(
    places, areas, regions, patches, assignments, metro_report, provenance, names=None
):
    """``(entries, unplaced)``: one report entry per city-region holding an
    eligible city — a city with no metro and no known official assignment —
    with the cities already in a metro there as context; and every city, eligible
    or not, that could not be placed (no usable land area, or a footprint on a
    boundary between regions) with the reason. ``provenance`` (DOI, cutoff,
    licence, credit, input digests) is copied into every entry so each stands
    on its own. ``names`` maps a centre id to its matched UCDB name row; a
    region is named after its centre (a region's id is its centre's), the
    match's ambiguity and candidates carried along, and the pasteable
    ``add_place`` prefilled with the name — a region without one keeps the
    placeholder."""
    known = {
        row["city_id"]
        for row in assignments
        if row.get("status") in ("assigned", "ambiguous")
    }
    known |= {
        row["city_id"]
        for row in metro_report
        if row.get("branch") == "us" and row.get("city_id")
    }
    geoms = {pid: p["geom"] for pid, p in patches.items()}
    region_by_patch = {pid: region_of(p) for pid, p in patches.items()}
    containment = eurostat.Containment(geoms)
    grouped = {}
    context = {}
    countries = {}
    unplaced = []
    qid_of = {}
    for place in places:
        if place.get("kind") != "city":
            continue
        city = place["place_id"]
        qid_of[city] = place.get("wikidata_id")
        eligible = not place.get("metro_ids") and city not in known
        overture_id = place.get("overture_id")
        footprint = eurostat._footprint(areas.get(overture_id)) if overture_id else None
        if footprint is None:
            unplaced.append(
                {"city_id": city, "eligible": eligible, "reason": "no usable land area"}
            )
            continue
        patch_id, ambiguous = _patch_of(footprint, containment, geoms, region_by_patch)
        if ambiguous:
            unplaced.append(
                {
                    "city_id": city,
                    "eligible": eligible,
                    "reason": "on a boundary between regions",
                }
            )
            continue
        if patch_id is None:
            continue
        region_id = region_by_patch[patch_id]
        if region_id is None:
            continue
        (grouped if eligible else context).setdefault(region_id, []).append(city)
        countries.setdefault(region_id, set()).add(place.get("country_code"))
    entries = []
    for region_id, cities in sorted(grouped.items()):
        region = regions[region_id]
        cities = sorted(cities)
        # FAO's country is ISO-3; the pasteable code is the gazetteer code every
        # city in the region shares — omitted for a cross-border region, and
        # when any city's country is unknown.
        codes = countries[region_id]
        named = (names or {}).get(region_id)
        add_place = {"kind": "metro", "name": named["name"] if named else "<name>"}
        if len(codes) == 1 and None not in codes:
            add_place["country_code"] = next(iter(codes))
        entries.append(
            {
                **provenance,
                "region_id": region_id,
                "tier": region["tier"],
                "category": region["category"],
                "country": region["country"],
                "name": named["name"] if named else None,
                "name_ambiguous": bool(named and named["ambiguous"]),
                "name_candidates": named["candidates"] if named else [],
                "cities": cities,
                # The QID beside each city's own id, where it has one.
                "cities_wikidata": [qid_of.get(city) for city in cities],
                "context": sorted(context.get(region_id, [])),
                "evidence_hash": overrides.canonical_digest(cities),
                # The pair a curator pastes into places.yaml, keyed by the
                # region's own concordance — the metro is minted from it and
                # needs no QID — with the name filled in; refused until the
                # scheme is registered (PR D).
                "override": [
                    {"place": f"fao_city_region:{region_id}", "add_place": add_place},
                    {
                        "place": f"fao_city_region:{region_id}",
                        "set_statistical_area": {
                            "scheme": "fao_city_region",
                            "code": region_id,
                        },
                        "evidence_hash": overrides.canonical_digest(cities),
                    },
                ],
            }
        )
    return entries, sorted(unplaced, key=lambda row: row["city_id"])


def suggest_metros(cache_dir, *, dataset=None, pins=None, ucdb_pins=None, run=None):
    """Publish ``gazetteer/fao.json``: the suggested-curation report of FAO
    city-regions holding cities no official metro covers, each named after
    its centre's GHS-UCDB match. ``dataset`` is the Overture ``division_area``
    dataset (the pinned release by default), ``pins`` the FAO inputs' digests
    and ``ucdb_pins`` the UCDB inputs'. Returns the generation manifest."""
    from index_build import ucdb  # builds on this module, so imported here

    pins = dict(pins or PINS)
    ucdb_pins = dict(ucdb_pins or ucdb.PINS)
    # All under the raw store's own lock, before the gazetteer lock below.
    prepare_inputs(cache_dir, expected=pins)
    convert_patches(cache_dir, expected=pins)
    ucdb.prepare_inputs(cache_dir, expected=ucdb_pins)

    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, metros_manifest = store.read_jsonl(
                cache_dir / "gazetteer",
                "metros.json",
                "places_seed.jsonl",
                generations=run,
            )
            assignments, _ = store.read_jsonl(
                cache_dir / "gazetteer",
                "metros.json",
                "metro_assignments.jsonl",
                generations=run,
            )
            metro_report, _ = store.read_jsonl(
                cache_dir / "gazetteer",
                "metros.json",
                "metro_report.jsonl",
                generations=run,
            )
            regions, patches, inputs_manifest = load_inputs(cache_dir, expected=pins)
            wanted = {
                p["overture_id"]
                for p in places
                if p.get("kind") == "city" and p.get("overture_id")
            }
            if wanted and dataset is None:
                dataset = geometry.division_area_dataset()
            areas = geometry.read_areas(dataset, wanted) if wanted else {}
            # Names are a derived use of the UCDB: attached only while its
            # allowlist entry stands; the report goes out unnamed otherwise.
            names, names_manifest = ucdb.load_names(cache_dir, expected=ucdb_pins)
            allowed = ucdb.DERIVED in geometry.DERIVED_SOURCE_ALLOWLIST
            provenance = {
                "doi": DOI,
                "cutoff_hours": CUTOFF_HOURS,
                "license": LICENCE,
                "credit": CREDIT,
                "digests": inputs_manifest.get("digests"),
                "names": {
                    "source": "ghs-ucdb",
                    "release": ucdb.RELEASE,
                    "doi": ucdb.DOI,
                    "license": ucdb.LICENCE,
                    "credit": geometry.DERIVED_SOURCES[ucdb.DERIVED]["credit"],
                    "digests": names_manifest.get("sources"),
                    "allowed": allowed,
                },
            }
            entries, unplaced = suggest(
                places,
                areas,
                regions,
                patches,
                assignments,
                metro_report,
                provenance,
                names if allowed else {},
            )
            manifest = {
                "source": "fao",
                "doi": DOI,
                "cutoff_hours": CUTOFF_HOURS,
                "license": LICENCE,
                "credit": CREDIT,
                "digests": inputs_manifest.get("digests"),
                "metros_generation": metros_manifest.get("generation"),
                "names": provenance["names"],
                "names_generation": names_manifest.get("generation"),
                "entries": len(entries),
                "named_entries": sum(1 for entry in entries if entry["name"]),
                "eligible_cities": sum(len(entry["cities"]) for entry in entries),
                "tiers": dict(
                    sorted(collections.Counter(e["tier"] for e in entries).items())
                ),
                "unplaced": len(unplaced),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            published = store.publish(
                cache_dir / "gazetteer",
                "fao.json",
                {
                    "suggested_metros_report.jsonl": store.jsonl_chunks(entries),
                    "unplaced.jsonl": store.jsonl_chunks(unplaced),
                },
                manifest,
                held=directory,
                staged=run is not None,
            )
            if run is not None:
                run["fao.json"] = published["generation"]
            return published
    finally:
        directory.close()
