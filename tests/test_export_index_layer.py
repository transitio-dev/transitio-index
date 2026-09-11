"""Unit tests for the index-layer export tool's join and row shaping.

The tool imports pyarrow/shapely/geopandas lazily, so importing it and
exercising the pure helpers needs none of them.
"""

import importlib.util
import json
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "export_index_layer.py"
_spec = importlib.util.spec_from_file_location("export_index_layer", _SCRIPT)
eil = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(eil)


def test_feed_names_fall_back_to_the_id():
    names = eil._feed_names([{"feed_id": "f1", "name": "HSL"}, {"feed_id": "f2"}])
    assert names == {"f1": "HSL", "f2": "f2"}


def test_feeds_by_place_gathers_tiers_per_pair():
    edges = [
        {"place_id": "p1", "feed_id": "f1", "tier": "b", "relevance_category": "y"},
        {"place_id": "p1", "feed_id": "f1", "tier": "a", "relevance_category": "x"},
        {"place_id": "p1", "feed_id": "f2", "tier": "a"},  # unranked
    ]
    by_place = eil._feeds_by_place(edges, {"f1": "HSL", "f2": "Föli"})
    assert by_place["p1"] == [
        {"feed_id": "f1", "name": "HSL", "tiers": ["a", "b"], "categories": ["x", "y"]},
        {"feed_id": "f2", "name": "Föli", "tiers": ["a"], "categories": []},
    ]


def _place(**kw):
    base = {
        "place_id": "p1",
        "name": "Helsinki",
        "names": {"fi": "Helsinki", "sv": "Helsingfors"},
        "kind": "city",
        "country_code": "FI",
        "wikidata_id": "Q1757",
        "overture_id": "o1",
        "parent_id": "r-uusimaa",
        "metro_ids": [],
        "member_ids": [],
        "service": '{"feeds": 2}',
        "geometry_source": "overture",
        "geometry": b"\x00wkb",
    }
    base.update(kw)
    return base


def test_enrich_marks_served_places_and_lists_their_feeds():
    feeds_by_place = {"p1": [{"feed_id": "f1", "name": "HSL", "tiers": ["a"]}]}
    row = eil._enrich([_place()], feeds_by_place)[0]
    assert row["served"] is True
    assert row["feed_count"] == 1
    assert json.loads(row["feeds"])[0]["name"] == "HSL"
    assert json.loads(row["names"])["sv"] == "Helsingfors"
    assert row["service"] == '{"feeds": 2}'
    assert "geometry" not in row  # geometry is re-attached at write time


def test_enrich_marks_an_unserved_ancestor():
    row = eil._enrich([_place(place_id="r-uusimaa", kind="region")], {})[0]
    assert row["served"] is False
    assert row["feed_count"] == 0
    assert json.loads(row["feeds"]) == []
