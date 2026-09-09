#!/usr/bin/env python3

"""Export the Overture division hierarchy, with geometry, for inspection.

A maintainer tool, not part of the package: it reads the pinned Overture
release the build resolves places against and writes the administrative
hierarchy — countries, regions, counties, localadmins and localities (down to
city level) — for the requested countries as GeoParquet and GeoJSON, so the
geography can be investigated in QGIS or geopandas before running any index
build. Each division row carries its name, all-language names, admin level,
Wikidata QID, the source provenance (the ``dataset``/``license`` pairs Overture
records, whichever it populates), the parent id/name and the full ancestor
chain, and (where the release has one) its boundary polygon from the
``division_area`` theme.

Everything is read straight from public Overture S3 (anonymous); nothing here
runs the index build. Outputs are written into a fresh per-run subdirectory of
``--out-dir`` (default ``cache/divisions``, gitignored). The heavy
dependencies (pyarrow, shapely, geopandas) are imported lazily so the
row-shaping stays unit-testable without them.
"""

import argparse
import json
import tempfile
from pathlib import Path

from transitio_index.progress import configure, progress

DEFAULT_COUNTRIES = ("FI", "EE")

# The administrative skeleton plus localities, i.e. the hierarchy down to city
# level (the build seeds cities from feeds; here every locality is included).
SUBTYPES = ("country", "dependency", "region", "county", "localadmin", "locality")


def _read_divisions(countries):
    """Normalised Overture divisions for ``countries``, from the pinned release."""
    import pyarrow.dataset as ds

    from transitio_index import overture

    dataset = overture.overture_dataset()
    predicate = ds.field("subtype").isin(list(SUBTYPES)) & ds.field("country").isin(
        sorted(countries)
    )
    rows = dataset.to_table(columns=overture.PROJECT, filter=predicate).to_pylist()
    return [overture.normalize_division(row) for row in progress(rows, "divisions")]


def _read_geometry(division_ids):
    """One unioned land-area polygon per division id (``None`` where there is none)."""
    import shapely

    from transitio_index import geometry

    areas = geometry.read_areas(geometry.division_area_dataset(), division_ids)
    geoms = {}
    for division_id, rows in areas.items():
        parts = [row["geom"] for row in rows if row["geom"] is not None]
        merged = shapely.unary_union(parts) if parts else None
        geoms[division_id] = None if merged is None or merged.is_empty else merged
    return geoms


def _rows(divisions, geoms):
    """Attribute rows (hierarchy + geometry) for the divisions.

    Pure and free of the heavy geospatial dependencies, so it is unit-tested
    directly; ``geoms`` maps an overture id to its geometry (or is missing it).
    """
    rows = []
    for division in divisions:
        ancestors = division.get("ancestors") or []
        parent = ancestors[-1] if ancestors else None
        rows.append(
            {
                "overture_id": division["overture_id"],
                "subtype": division["subtype"],
                "kind": division["kind"],
                "admin_level": division["admin_level"],
                "country": division["country"],
                "name": division["name"],
                "wikidata": division["wikidata"],
                "parent_id": parent["overture_id"] if parent else None,
                "parent_name": parent["name"] if parent else None,
                "ancestor_ids": json.dumps([a["overture_id"] for a in ancestors]),
                "ancestor_names": json.dumps(
                    [a["name"] for a in ancestors], ensure_ascii=False
                ),
                "names": json.dumps(
                    division["names"], ensure_ascii=False, sort_keys=True
                ),
                "sources": json.dumps(
                    division["sources"], ensure_ascii=False, sort_keys=True
                ),
                "geometry": geoms.get(division["overture_id"]),
            }
        )
    return rows


def _write(rows, out_dir):
    """Write ``rows`` as GeoParquet and GeoJSON; return the two paths."""
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    parquet = out_dir / "divisions.parquet"
    geojson = out_dir / "divisions.geojson"
    gdf.to_parquet(parquet)
    gdf.to_file(geojson, driver="GeoJSON")
    return parquet, geojson


def _by_subtype(rows):
    counts = {}
    for row in rows:
        counts[row["subtype"]] = counts.get(row["subtype"], 0) + 1
    return dict(sorted(counts.items()))


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python scripts/export_divisions.py",
        description="Export the Overture division hierarchy for inspection",
    )
    parser.add_argument(
        "--country",
        dest="countries",
        action="append",
        metavar="CC",
        help="ISO country code to export (repeatable; default: "
        + " ".join(DEFAULT_COUNTRIES)
        + ")",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("cache/divisions"),
        help="parent for the per-run output directory (default: cache/divisions)",
    )
    args = parser.parse_args(argv)
    countries = {c.strip().upper() for c in (args.countries or DEFAULT_COUNTRIES)}
    configure()  # show the read progress on the screen

    divisions = _read_divisions(countries)
    if not divisions:
        raise SystemExit(f"no Overture divisions for {sorted(countries)}")
    geoms = _read_geometry([division["overture_id"] for division in divisions])
    rows = _rows(divisions, geoms)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=args.out_dir))
    parquet, geojson = _write(rows, run_dir)

    with_geometry = sum(1 for row in rows if row["geometry"] is not None)
    print(f"countries: {', '.join(sorted(countries))}")
    print(f"divisions: {len(rows)} {_by_subtype(rows)}; with geometry: {with_geometry}")
    print(f"wrote: {parquet}")
    print(f"wrote: {geojson}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
