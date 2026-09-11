"""The stats stage: catalogue-level rows and summary sections."""

import json

import pytest

pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from transitio_index import stats, store  # noqa: E402


def _mdb(mdb_id, status="active", **kw):
    return {
        "source": "mdb",
        "mdb_id": mdb_id,
        "spec": kw.get("spec", "gtfs"),
        "name": kw.get("name"),
        "status": status,
        "location": {
            "country_code": kw.get("country", "FI"),
            "subdivision_name": kw.get("subdivision", "Uusimaa"),
            "municipality": kw.get("municipality", "Helsinki"),
        },
        "bounding_box": kw.get("box"),
        "bounding_box_extracted_on": kw.get("extracted", "2026-01-02"),
        "urls": {
            "direct_download": kw.get("url", f"https://{mdb_id}.example/gtfs.zip"),
            "latest": kw.get("latest"),
            "license": kw.get("license"),
        },
        "requires_auth": kw.get("auth", False),
        "redirect_ids": kw.get("redirects", []),
    }


BOX = {"min_lat": 60.0, "max_lat": 61.0, "min_lon": 24.0, "max_lon": 26.0}
RAW = {
    "mdb": [
        _mdb("mdb-1", name="HSL", box=BOX, license="https://l", latest="https://x"),
        # A wide box, a municipality repeating the subdivision, no name.
        _mdb(
            "tdg-2",
            subdivision="Bayern",
            municipality="bayern",
            box={"min_lat": 0.0, "max_lat": 50.0, "min_lon": 0.0, "max_lon": 20.0},
        ),
        # Deprecated, redirecting to an active row with the same URL.
        _mdb(
            "mdb-3",
            status="deprecated",
            redirects=["mdb-1"],
            url="https://mdb-1.example/gtfs.zip",
            municipality="Espoo, Kauniainen",
            country=None,
        ),
        # Deprecated, redirecting first to a row the catalogue lacks, then to
        # one it holds; no municipality.
        _mdb(
            "mdb-4",
            status="deprecated",
            redirects=["mdb-9", "tdg-2"],
            municipality=None,
            extracted=None,
        ),
    ],
    "atlas": [
        {
            "source": "atlas",
            "onestop_id": "f-hsl",
            "spec": "gtfs",
            "name": None,
            "license": {"url": "https://l"},
            "requires_auth": False,
            "urls": {"static_current": "https://mdb-1.example/gtfs.zip"},
        }
    ],
    "gbfs": [
        {
            "source": "gbfs",
            "system_id": "bikes",
            "spec": "gbfs",
            "country_code": "FI",
            "location": "Helsinki",
            "name": "Bikes",
            "requires_auth": False,
            "auto_discovery_url": "https://b.example/gbfs.json",
        },
        {
            "source": "gbfs",
            "system_id": "bikes",
            "spec": "gbfs",
            "country_code": None,
            "location": "Elsewhere",
            "name": "Bikes",
            "requires_auth": True,
            "auto_discovery_url": "https://c.example/gbfs.json",
        },
    ],
}
FEEDS = [
    {
        "feed_id": "f-hsl",
        "source": "both",
        "spec": "gtfs",
        "mdb_id": "mdb-1",
        "onestop_id": "f-hsl",
        "crosswalk_method": "url_exact",
    },
    {
        "feed_id": "f-tdg-2",
        "source": "mdb",
        "spec": "gtfs",
        "mdb_id": "tdg-2",
        "onestop_id": None,
        "crosswalk_method": "none",
    },
    {
        "feed_id": "f-mdb-3",
        "source": "mdb",
        "spec": "gtfs",
        "mdb_id": "mdb-3",
        "onestop_id": None,
        "crosswalk_method": "none",
    },
    {
        "feed_id": "f-mdb-4",
        "source": "mdb",
        "spec": "gtfs",
        "mdb_id": "mdb-4",
        "onestop_id": None,
        "crosswalk_method": "none",
    },
    {
        "feed_id": "f-gbfs-bikes-fi",
        "source": "systems_csv",
        "spec": "gbfs",
        "mdb_id": None,
        "onestop_id": None,
        "crosswalk_method": "none",
        "gbfs": {"system_id": "bikes", "country_code": "FI"},
    },
]


def test_catalogue_rows_name_the_feed_or_the_reason_they_were_dropped():
    rows = stats.catalogue_rows(RAW, FEEDS, "snap")
    by_id = {
        (row["source"], row["source_id"], row["declared_country"]): row for row in rows
    }
    hsl = by_id[("mdb", "mdb-1", "FI")]
    assert hsl["feed_id"] == "f-hsl" and hsl["drop_reason"] is None
    assert hsl["download_host"] == "mdb-1.example" and hsl["id_namespace"] == "mdb"
    assert hsl["bbox_lon_span"] == 2.0 and hsl["bbox_extracted_year"] == 2026
    assert hsl["has_license_url"] and hsl["has_latest_url"] and hsl["has_bbox"]
    assert by_id[("mdb", "mdb-3", None)]["redirect_target_status"] == "active"
    assert by_id[("mdb", "mdb-3", None)]["redirect_targets"] == ["mdb-1"]
    assert by_id[("mdb", "mdb-4", "FI")]["redirect_target_status"] == "active"
    assert by_id[("mdb", "mdb-4", "FI")]["redirect_targets"] == ["mdb-9", "tdg-2"]
    assert by_id[("mdb", "mdb-4", "FI")]["redirect_target"] == "tdg-2"
    assert by_id[("mdb", "mdb-1", "FI")]["redirect_targets"] == []
    assert by_id[("atlas", "f-hsl", None)]["feed_id"] == "f-hsl"
    assert by_id[("gbfs", "bikes/FI", "FI")]["feed_id"] == "f-gbfs-bikes-fi"
    dropped = by_id[("gbfs", "bikes/?", None)]
    assert dropped["feed_id"] is None and dropped["drop_reason"] == "ambiguous_id"
    assert all(row["snapshot_id"] == "snap" for row in rows)
    assert set(rows[0]) == set(stats.CATALOGUE_SCHEMA.names)
    # A box across the antimeridian is narrow, not global.
    wrapped = {"min_lat": 0.0, "max_lat": 1.0, "min_lon": 179.0, "max_lon": -179.0}
    assert stats._lon_span(wrapped) == pytest.approx(2.0)
    with pytest.raises(stats.StatsError, match="share a key"):
        stats.catalogue_rows({"mdb": [_mdb("mdb-1"), _mdb("mdb-1")]}, [])


def test_summary_sections_count_the_catalogue_defects():
    rows = stats.catalogue_rows(RAW, FEEDS)
    declared = stats.declared_places(rows)
    assert declared["mdb_rows"] == 4
    assert declared["missing_country"] == 1 and declared["missing_municipality"] == 1
    assert declared["subdivision_without_municipality"] == 1
    assert declared["municipality_repeats_subdivision"] == 1
    assert declared["municipality_lists_several"] == 1
    assert declared["missing_bbox"] == 2
    assert (
        declared["bbox_over_15_degrees"] == 1 and declared["bbox_over_40_degrees"] == 1
    )
    assert declared["bbox_by_extracted_year"] == {"2026": 3}
    assert declared["atlas_rows_without_location"] == 1
    assert declared["gbfs_rows_with_free_text_location"] == 2
    identity = stats.identity(rows, FEEDS)
    assert identity["rows_by_source"] == {"mdb": 4, "atlas": 1, "gbfs": 2}
    assert identity["id_namespaces"] == {"mdb": 3, "tdg": 1}
    assert identity["deprecated_rows"] == 2 == identity["deprecated_with_redirect"]
    assert identity["redirect_target_present"] == 2
    assert identity["redirect_target_status"] == {"active": 2}
    assert identity["redirect_shares_target_url"] == 1
    assert identity["mdb_rows_without_name"] == 3
    assert identity["gbfs_duplicate_system_ids"] == 1
    assert identity["rows_into_feeds"] == 6
    assert identity["rows_dropped_by_reason"] == {"ambiguous_id": 1}
    assert identity["feeds_by_crosswalk_method"] == {"url_exact": 1, "none": 4}


def _publish(cache, subdir, pointer, artifacts, manifest):
    directory = store.open_subdir(cache, subdir)
    try:
        with store.exclusive_writer(directory):
            return store.publish(
                cache / subdir,
                pointer,
                {name: store.jsonl_chunks(rows) for name, rows in artifacts.items()},
                manifest,
                held=directory,
            )
    finally:
        directory.close()


def test_the_stage_publishes_the_catalogue_table_and_summary(tmp_path):
    cache = tmp_path / "cache"
    with pytest.raises(stats.StatsError, match="no crosswalk"):
        stats.stats(cache)
    generations = {}
    for source, (pointer, artifact) in stats.RAW_SOURCES.items():
        published = _publish(
            cache,
            "raw",
            pointer,
            {artifact: RAW[source]},
            {"source": source, "rows": len(RAW[source]), "csv_label": "2026-09-01"},
        )
        generations[f"raw/{pointer}"] = published["generation"]
    crosswalk = _publish(
        cache,
        "crosswalk",
        "feeds.json",
        {"feeds.jsonl": FEEDS},
        {"source": "crosswalk", "sources": {"mdb": {"csv_label": "x"}}},
    )
    generations["crosswalk/feeds.json"] = crosswalk["generation"]
    (cache / "index").mkdir(exist_ok=True)
    snapshot = cache / "index" / "snapshot.json"
    with pytest.raises(stats.StatsError, match="no published index"):
        stats.stats(cache)
    # A snapshot built from other generations, or from an ingest that has
    # since vanished, is refused, never relabelled.
    snapshot.write_text(json.dumps({"snapshot_id": "old", "generations": {}}))
    with pytest.raises(stats.StatsError, match="not built from the current"):
        stats.stats(cache)
    vanished = {**generations, "raw/eurostat.json": "gen-x"}
    snapshot.write_text(json.dumps({"snapshot_id": "old", "generations": vanished}))
    with pytest.raises(stats.StatsError, match="raw/eurostat.json"):
        stats.stats(cache)
    snapshot.write_text(
        json.dumps(
            {"snapshot_id": "abc", "schema_version": 7, "generations": generations}
        )
    )
    manifest = stats.stats(cache)
    assert manifest["catalogue_rows"] == 7 and manifest["snapshot_id"] == "abc"
    assert manifest["sections"] == ["build", "declared_places", "identity"]
    generation, _ = store.resolve(cache / "stats", "stats.json")
    with generation:
        table = pq.read_table(pa_source(generation.read_bytes("catalogue.parquet")))
        summary = json.loads(generation.read_bytes("summary.json"))
    assert table.num_rows == 7 and table.schema.names == stats.CATALOGUE_SCHEMA.names
    assert summary["build"]["snapshot_id"] == "abc"
    assert summary["build"]["schema_version"] == 7
    assert summary["build"]["catalogue_rows"] == {"mdb": 4, "atlas": 1, "gbfs": 2}
    assert summary["build"]["catalogue_dates"]["mdb"] == "2026-09-01"
    # Every ingest read a local file: a cut sample, not the full catalogues.
    assert summary["build"]["sample"] == "sample"
    assert summary["build"]["sample_sources"] == ["mdb", "atlas", "gbfs"]
    assert summary["identity"]["feeds"] == 5


def test_the_stage_reads_the_ingest_fixtures_through_the_crosswalk(tmp_path):
    from test_index_publish import _build_index

    cache, published = _build_index(tmp_path)
    manifest = stats.stats(cache)
    assert manifest["snapshot_id"] == published["snapshot_id"]
    generation, _ = store.resolve(cache / "stats", "stats.json")
    with generation:
        table = pq.read_table(pa_source(generation.read_bytes("catalogue.parquet")))
        summary = json.loads(generation.read_bytes("summary.json"))
    rows = table.to_pylist()
    # The url-matched MDB row and its Atlas feed both name the same feed.
    assert {r["feed_id"] for r in rows if r["source_id"] in ("mdb-1", "f-a")} == {"f-a"}
    assert summary["identity"]["rows_into_feeds"] == len(rows)
    assert summary["identity"]["feeds"] == published["counts"]["feeds"]


def pa_source(data):
    import io

    return io.BytesIO(data)
