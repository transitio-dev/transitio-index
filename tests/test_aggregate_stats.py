"""The stats aggregation recipe over several archived stats generations."""

import importlib.util
import json
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from transitio_index import stats  # noqa: E402

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "aggregate_stats.py"
_spec = importlib.util.spec_from_file_location("aggregate_stats", _SCRIPT)
agg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(agg)

SCHEMAS = {
    "catalogue": stats.CATALOGUE_SCHEMA,
    "feeds": stats.FEED_SCHEMA,
    "places": stats.PLACE_SCHEMA,
}


def _full(schema, **values):
    row = {name: None for name in schema.names}
    row.update(values)
    return row


def _catalogue(source_id, snapshot, feed_id=None, **kw):
    return _full(
        stats.CATALOGUE_SCHEMA,
        source="mdb",
        source_id=source_id,
        spec="gtfs",
        status="active",
        declared_country=kw.get("country", "FI"),
        has_name=True,
        has_license_url=False,
        has_bbox=False,
        has_latest_url=False,
        requires_auth=False,
        redirect_targets=[],
        id_namespace="mdb",
        feed_id=feed_id,
        drop_reason=None if feed_id else "not_in_index",
        snapshot_id=snapshot,
    )


def _feed(feed_id, snapshot, **kw):
    return _full(
        stats.FEED_SCHEMA,
        feed_id=feed_id,
        source="mdb",
        spec="gtfs",
        crosswalk_method="none",
        crawl_outcome=kw.get("outcome", "ok"),
        has_calendar=True,
        scope="domestic",
        country_shares="{}",
        declared_countries=["FI"],
        country_agreement=kw.get("agreement", "agree"),
        places_served=kw.get("places", 1),
        cities_served=1,
        countries_served=1,
        edges_by_tier="{}",
        edges_by_category="{}",
        licence_state="none",
        snapshot_id=snapshot,
    )


def _place(place_id, snapshot):
    return _full(
        stats.PLACE_SCHEMA,
        place_id=place_id,
        kind="city",
        country_code="FI",
        feeds=1,
        feeds_by_category="{}",
        has_primary=True,
        snapshot_id=snapshot,
    )


def _archive(tmp_path, label, tables, **build):
    directory = tmp_path / label / "stats"
    directory.mkdir(parents=True)
    for table, rows in tables.items():
        pq.write_table(
            pa.Table.from_pylist(rows, schema=SCHEMAS[table]),
            directory / f"{table}.parquet",
        )
    # The summary names the snapshot the rows carry.
    snapshot = next(r["snapshot_id"] for rows in tables.values() for r in rows)
    summary = {
        "build": {
            "snapshot_id": snapshot,
            "stats_schema_version": stats.STATS_SCHEMA_VERSION,
            "overture_release": "2026-08-19.0",
            "catalogue_dates": {"mdb": "2026-08-28"},
            **build,
        },
        "distributions": {"tiers_by_kind": {"city": {"local": len(tables["feeds"])}}},
        "duplicate_coverage": {"pairs": 0},
    }
    (directory / "summary.json").write_text(json.dumps(summary))
    return tmp_path / label


def test_archives_are_merged_on_their_keys_and_the_summary_recomputed(tmp_path):
    shared = _catalogue("mdb-9", "a")  # a dropped row both samples saw
    first = _archive(
        tmp_path,
        "fi-a",
        {
            "catalogue": [_catalogue("mdb-1", "a", "f-1"), shared],
            "feeds": [_feed("f-1", "a")],
            "places": [_place("hel", "a")],
        },
    )
    second = _archive(
        tmp_path,
        "ee-b",
        {
            "catalogue": [
                _catalogue("mdb-2", "b", "f-2", country="EE"),
                {**shared, "snapshot_id": "b"},
            ],
            "feeds": [_feed("f-2", "b", agreement="disagree", outcome="failed")],
            "places": [_place("tll", "b")],
        },
    )
    out = tmp_path / "out"
    # The second archive is given as its stats directory: labelled by the parent.
    assert agg.main([str(first), str(second / "stats"), "--out-dir", str(out)]) == 0
    summary = json.loads((out / "summary.json").read_text())
    assert summary["build"]["archives"] == {"fi-a": "a", "ee-b": "b"}
    assert summary["build"]["catalogue_rows"] == {"mdb": 3, "atlas": 0, "gbfs": 0}
    assert summary["identity"]["rows_into_feeds"] == 2
    assert summary["identity"]["rows_dropped_by_reason"] == {"not_in_index": 1}
    assert summary["availability"]["by_outcome"] == {"ok": 1, "failed": 1}
    assert summary["country_agreement"]["by_agreement"] == {"agree": 1, "disagree": 1}
    # Per-build measures are repeated per archive, never merged.
    assert summary["distributions"] == {
        "fi-a": {"tiers_by_kind": {"city": {"local": 1}}},
        "ee-b": {"tiers_by_kind": {"city": {"local": 1}}},
    }
    report = (out / "report.md").read_text()
    assert report.startswith("# Build statistics") and "## Availability" in report


@pytest.mark.parametrize(
    "change, message",
    [
        ("feed", "overlapping: feeds"),
        ("place", "overlapping: places"),
        ("catalogue", "differs between"),
        ("release", "overture_release"),
    ],
)
def test_overlaps_and_incompatible_archives_are_refused(tmp_path, change, message):
    base = {
        "catalogue": [_catalogue("mdb-1", "a", "f-1")],
        "feeds": [_feed("f-1", "a")],
        "places": [_place("hel", "a")],
    }
    other = {
        "catalogue": [_catalogue("mdb-2", "b", "f-2")],
        "feeds": [_feed("f-2", "b")],
        "places": [_place("tll", "b")],
    }
    build = {}
    if change == "feed":
        # Every overlap is listed, not just the first.
        other["feeds"].append(_feed("f-1", "b"))
        other["places"].append(_place("hel", "b"))
        message = r"feeds 'f-1' \(one, two\), places 'hel' \(one, two\)"
    elif change == "place":
        other["places"].append(_place("hel", "b"))
    elif change == "catalogue":
        other["catalogue"].append(_catalogue("mdb-1", "b", "f-1", country="EE"))
    else:
        build["overture_release"] = "2026-09-01.0"
    first = _archive(tmp_path, "one", base)
    second = _archive(tmp_path, "two", other, **build)
    with pytest.raises(SystemExit, match=message):
        agg.main([str(first), str(second), "--out-dir", str(tmp_path / "out")])


def test_mixed_snapshots_and_duplicate_labels_are_refused(tmp_path):
    tables = {
        "catalogue": [_catalogue("mdb-1", "a", "f-1")],
        "feeds": [_feed("f-1", "other")],
        "places": [],
    }
    mixed = _archive(tmp_path, "mixed", tables)
    with pytest.raises(SystemExit, match="rows of snapshot"):
        agg.main([str(mixed), "--out-dir", str(tmp_path / "out")])
    same = _archive(tmp_path / "twin", "mixed", {**tables, "feeds": []})
    with pytest.raises(SystemExit, match="not unique"):
        agg.main([str(same), str(same / "stats"), "--out-dir", str(tmp_path / "o")])
