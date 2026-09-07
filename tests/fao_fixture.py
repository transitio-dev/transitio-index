"""Builders for the FAO city-region inputs the tests pin: a zipped patches
shapefile and a regions table, as bytes."""

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
    with tempfile.TemporaryDirectory() as scratch:
        frame.to_file(os.path.join(scratch, "patches.shp"))
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for name in sorted(os.listdir(scratch)):
                archive.write(os.path.join(scratch, name), name)
    return buffer.getvalue()


def regions_csv(rows):
    """A regions table of ``(id, country, tier, patches, category)`` rows."""
    lines = [REGION_HEADER]
    for region_id, country, tier, patches, category in rows:
        lines.append(
            f"{region_id},{country},{tier},{';'.join(patches)},3,{region_id},0,0,0,0,{category}"
        )
    return ("\n".join(lines) + "\n").encode("utf-8")
