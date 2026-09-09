"""Unit tests for the division-export tool's row shaping.

The tool imports pyarrow/shapely/geopandas lazily, so importing it and
exercising the pure ``_rows`` / ``_by_subtype`` helpers needs none of them.
"""

import importlib.util
import json
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "export_divisions.py"
_spec = importlib.util.spec_from_file_location("export_divisions", _SCRIPT)
ed = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ed)


def _division(**kw):
    base = {
        "overture_id": "d1",
        "subtype": "localadmin",
        "kind": "city",
        "admin_level": 8,
        "country": "FI",
        "name": "Helsinki",
        "wikidata": "Q1757",
        "names": {"fi": "Helsinki", "sv": "Helsingfors"},
        "sources": [],
        "ancestors": [
            {"overture_id": "c-fi", "subtype": "country", "name": "Finland"},
            {"overture_id": "r-uusimaa", "subtype": "region", "name": "Uusimaa"},
        ],
    }
    base.update(kw)
    return base


def test_rows_extract_parent_and_ancestor_chain():
    row = ed._rows([_division()], {"d1": "GEOM"})[0]
    assert row["overture_id"] == "d1"
    assert row["parent_id"] == "r-uusimaa"  # the immediate parent is the last ancestor
    assert row["parent_name"] == "Uusimaa"
    assert json.loads(row["ancestor_ids"]) == ["c-fi", "r-uusimaa"]
    assert json.loads(row["names"])["sv"] == "Helsingfors"
    assert row["geometry"] == "GEOM"


def test_rows_handle_a_root_without_ancestors_or_geometry():
    row = ed._rows(
        [_division(overture_id="c-fi", subtype="country", ancestors=[])], {}
    )[0]
    assert row["parent_id"] is None
    assert row["parent_name"] is None
    assert json.loads(row["ancestor_ids"]) == []
    assert row["geometry"] is None


def test_by_subtype_counts():
    rows = ed._rows([_division(), _division(overture_id="d2", subtype="region")], {})
    assert ed._by_subtype(rows) == {"localadmin": 1, "region": 1}


def test_rows_export_source_provenance():
    sources = [{"dataset": "OpenStreetMap", "license": "ODbL-1.0"}]
    row = ed._rows([_division(sources=sources)], {})[0]
    assert json.loads(row["sources"]) == sources
