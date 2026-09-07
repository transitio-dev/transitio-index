"""Builders for the FAO city-region and GHS-UCDB inputs the tests pin: zipped
shapefiles, a zipped GeoPackage and a regions table, as bytes."""

import io
import os
import tempfile
import zipfile

import pytest

pytest.importorskip("pyarrow")
geopandas = pytest.importorskip("geopandas")

REGION_HEADER = (
    "id,country,tier,patches,type,cities,ghspop,gpw,landscan,worldpop,category"
)
UCDB_COLUMNS = ("ID_UC_G0", "GC_UCN_MAI_2025", "GC_UCN_LIS_2025", "GC_CNT_GAD_2025")


def _zipped(frame, name, **options):
    """The frame written as ``name`` (a shapefile or a GeoPackage) and zipped
    with its sidecars."""
    with tempfile.TemporaryDirectory() as scratch:
        frame.to_file(os.path.join(scratch, name), **options)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for entry in sorted(os.listdir(scratch)):
                archive.write(os.path.join(scratch, entry), entry)
    return buffer.getvalue()


def patches_zip(rows, crs="EPSG:4326"):
    """A zipped shapefile of ``(id, (T1_id, T2_id, T3_id, T4_id), geometry)`` rows."""
    frame = geopandas.GeoDataFrame(
        {
            "id": [row[0] for row in rows],
            **{f"T{k}_id": [row[1][k - 1] for row in rows] for k in (1, 2, 3, 4)},
        },
        geometry=[row[2] for row in rows],
        crs=crs,
    )
    return _zipped(frame, "patches.shp")


def centres_zip(rows, crs="EPSG:4326"):
    """A zipped shapefile of FAO urban centres: ``(id, type, geometry)`` rows."""
    frame = geopandas.GeoDataFrame(
        {"id": [row[0] for row in rows], "type": [row[1] for row in rows]},
        geometry=[row[2] for row in rows],
        crs=crs,
    )
    return _zipped(frame, "centres.shp")


def ucdb_zip(rows, member, layer, crs="EPSG:4326", columns=UCDB_COLUMNS):
    """A zipped GeoPackage (``member``, one ``layer``) of UCDB centres:
    ``(id, name, names, country, geometry)`` rows under ``columns``."""
    frame = geopandas.GeoDataFrame(
        {column: [row[i] for row in rows] for i, column in enumerate(columns)},
        geometry=[row[4] for row in rows],
        crs=crs,
    )
    return _zipped(frame, member, driver="GPKG", layer=layer)


def regions_csv(rows):
    """A regions table of ``(id, country, tier, patches, category)`` rows."""
    lines = [REGION_HEADER]
    for region_id, country, tier, patches, category in rows:
        lines.append(
            f"{region_id},{country},{tier},{';'.join(patches)},3,{region_id},0,0,0,0,{category}"
        )
    return ("\n".join(lines) + "\n").encode("utf-8")
