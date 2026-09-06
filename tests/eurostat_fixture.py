"""Builders for the Eurostat inputs the tests pin: a metropolitan-regions
workbook and a GISCO-shaped NUTS-3 GeoParquet file, as bytes."""

import io

import pytest

pytest.importorskip("pyarrow")
openpyxl = pytest.importorskip("openpyxl")
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import shapely  # noqa: E402

from index_build import eurostat  # noqa: E402


def workbook(
    rows, header=eurostat.COMPOSITION_HEADER, sheet=eurostat.COMPOSITION_SHEET
):
    """A workbook whose ``sheet`` holds ``header`` and ``rows``."""
    book = openpyxl.Workbook()
    book.active.title = "Version Date"
    book.active.append(["2.2"])
    table = book.create_sheet(sheet)
    table.append(list(header))
    for row in rows:
        table.append(list(row))
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def parquet(regions, level=3):
    """A GISCO-shaped file: ``(nuts_id, geometry-or-wkb)`` rows."""
    table = pa.table(
        {
            "NUTS_ID": [nuts_id for nuts_id, _ in regions],
            "LEVL_CODE": [level] * len(regions),
            "Shape": [
                geom if isinstance(geom, bytes) else shapely.to_wkb(geom)
                for _, geom in regions
            ],
        }
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()
