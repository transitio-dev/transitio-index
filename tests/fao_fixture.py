"""Builders for the FAO city-region, GHS-UCDB and Urban Audit inputs the
tests pin: zipped shapefiles, a zipped GeoPackage and a regions table, as
bytes, and the files-and-digests pair a pinned input is prepared from."""

import hashlib
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


def fua_zip(rows, crs="EPSG:4326"):
    """A zipped shapefile of Urban Audit areas: ``(code, category, country,
    name, geometry)`` rows, with a sixth NUTS-3 region where one is named."""
    frame = geopandas.GeoDataFrame(
        {
            "URAU_CODE": [row[0] for row in rows],
            "URAU_CATG": [row[1] for row in rows],
            "CNTR_CODE": [row[2] for row in rows],
            "URAU_NAME": [row[3] for row in rows],
            "NUTS3_2024": [row[5] if len(row) > 5 else None for row in rows],
        },
        geometry=[row[4] for row in rows],
        crs=crs,
    )
    return _zipped(frame, "areas.shp")


def pinned_files(directory, payloads):
    """``(files, expected)`` for pinned inputs written from ``{name: bytes}``
    under ``directory``: the paths to prepare from and their digests."""
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, data in payloads.items():
        files[name] = directory / name
        files[name].write_bytes(data)
    expected = {
        name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()
    }
    return files, expected


def regions_csv(rows):
    """A regions table of ``(id, country, tier, patches, category)`` rows."""
    lines = [REGION_HEADER]
    for region_id, country, tier, patches, category in rows:
        lines.append(
            f"{region_id},{country},{tier},{';'.join(patches)},3,{region_id},0,0,0,0,{category}"
        )
    return ("\n".join(lines) + "\n").encode("utf-8")
