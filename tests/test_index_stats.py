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
        feeds = pq.read_table(pa_source(generation.read_bytes("feeds.parquet")))
    rows = table.to_pylist()
    # The url-matched MDB row and its Atlas feed both name the same feed.
    assert {r["feed_id"] for r in rows if r["source_id"] in ("mdb-1", "f-a")} == {"f-a"}
    assert summary["identity"]["rows_into_feeds"] == len(rows)
    assert summary["identity"]["feeds"] == published["counts"]["feeds"]
    # The published feeds (never crawled here) become the feed table too.
    assert feeds.num_rows == manifest["feeds"] == published["counts"]["feeds"]
    assert set(feeds.column("crawl_outcome").to_pylist()) == {"not_crawled"}
    assert summary["availability"]["crawled"] == 0


def pa_source(data):
    import io

    return io.BytesIO(data)


@pytest.mark.parametrize(
    "reason, expected",
    [
        ("GET https://x/gtfs.zip: HTTP 404", "404"),
        ("GET https://x/gtfs.zip: HTTP 401", "401"),
        ("GET https://x/gtfs.zip: HTTP 503", "5xx"),
        ("GET https://x/gtfs.zip: HTTP 302", "other_http"),
        ("File is not a zip file", "not-an-archive"),
        ("cache/crawl/id-1/stop_times.txt: over the 3 GB member ceiling", "oversize"),
        (
            "GET https://x: host x unreachable this run (3 consecutive transport failures)",
            "transport",
        ),
        ("GET https://x: certificate verify failed", "tls"),
        ("GET https://x: read timed out", "timeout"),
        ("GET ftp://x/gtfs.zip: scheme 'ftp' is not fetched", "blocked_url"),
        ("something else entirely", "other"),
        (None, None),
    ],
)
def test_failure_classes_follow_the_recorded_reason(reason, expected):
    assert stats.failure_class(reason) == expected


PLACES = {
    "fi": {
        "place_id": "fi",
        "kind": "country",
        "country_code": "FI",
        "parent_id": None,
    },
    "uus": {
        "place_id": "uus",
        "kind": "region",
        "country_code": "FI",
        "parent_id": "fi",
    },
    "hel": {
        "place_id": "hel",
        "kind": "city",
        "country_code": "FI",
        "parent_id": "uus",
    },
    "esp": {
        "place_id": "esp",
        "kind": "city",
        "country_code": "FI",
        "parent_id": "uus",
    },
    "tll": {"place_id": "tll", "kind": "city", "country_code": "EE", "parent_id": None},
}


def _feed(feed_id, **kw):
    return {
        "feed_id": feed_id,
        "aliases": kw.get("aliases", []),
        "source": kw.get("source", "mdb"),
        "spec": "gtfs",
        "crosswalk_method": "none",
        "crosswalk_confidence": None,
        "mdb_id": kw.get("mdb_id"),
        "stop_count": kw.get("stops"),
        "files": kw.get("files", []),
        "home_country": kw.get("home"),
        "scope": kw.get("scope", "declared"),
        "country_shares": kw.get("shares", {}),
        "declared_countries": kw.get("declared", []),
        "redistribution_allowed": kw.get("allowed"),
        "atlas": kw.get("atlas"),
        "mdb": kw.get("mdb"),
        "gbfs": kw.get("gbfs"),
    }


def _edge(feed_id, place_id, tier, relevance=None, method="crawl"):
    return {
        "feed_id": feed_id,
        "place_id": place_id,
        "tier": tier,
        "method": method,
        "relevance": relevance,
        "relevance_category": None if relevance is None else "primary",
    }


def test_feed_rows_join_the_crawl_log_placements_and_edges():
    feeds = [
        _feed(
            "hsl",
            mdb_id="mdb-1",
            stops=100,
            files=["calendar.txt"],
            home="FI",
            scope="domestic",
            shares={"FI": 1.0},
            declared=["FI"],
            allowed=True,
            atlas={"license": {"spdx_identifier": "CC-BY-4.0"}},
            mdb={"urls": {"direct_download": "https://hsl.fi/g.zip"}},
        ),
        _feed(
            "rail",
            aliases=["f-old-rail"],
            mdb_id="mdb-2",
            stops=500,
            home="FI",
            scope="domestic",
            declared=["EE"],
            mdb={"urls": {"license": "https://l"}},
        ),
        _feed("gone", mdb_id="mdb-3", declared=["FI"]),
        _feed("far", mdb_id="mdb-4", home="FI", scope="domestic"),
        _feed("lost", mdb_id="mdb-5", home="FI", scope="domestic"),
        _feed(
            "bikes",
            source="systems_csv",
            gbfs={"auto_discovery_url": "https://b/gbfs.json"},
        ),
    ]
    edges = [
        _edge("hsl", "hel", "local", 0.9),
        _edge("hsl", "hel", "regional", 0.9),
        _edge("hsl", "uus", "regional", 0.4),
        _edge("hsl", "tll", "international", 0.1),
        _edge("rail", "esp", "national", 0.5),  # declared Helsinki: in its region
        _edge("gone", "hel", "unknown", method="inferred"),
        _edge("far", "tll", "national", 0.2),  # declared Helsinki: elsewhere
        _edge("lost", "hel", "local", 0.3),  # declared a place the index lost
    ]
    crawl_log = [
        {"feed_id": "hsl", "method": "download", "route_count": 7},
        {"feed_id": "f-old-rail", "method": "download", "route_count": 2},
        {
            "feed_id": "gone",
            "method": "failed",
            "fallback_reason": "GET https://x: HTTP 404",
        },
        {"feed_id": "far", "method": "not_modified"},
        {"feed_id": "lost", "method": "range"},
        {"feed_id": "bikes", "method": "skipped", "fallback_reason": "boom"},
    ]
    placements = [
        {"feed_id": "hsl", "level": "municipality", "place_id": "hel"},
        {"feed_id": "f-old-rail", "level": "municipality", "place_id": "hel"},
        {"feed_id": "far", "level": "municipality", "place_id": "hel"},
        {"feed_id": "lost", "level": "district", "place_id": "vanished"},
    ]
    statuses = {"mdb-1": "active", "mdb-3": "deprecated"}
    rows = stats.feed_rows(
        feeds, edges, PLACES, crawl_log, placements, statuses, "snap"
    )
    by_id = {row["feed_id"]: row for row in rows}
    hsl = by_id["hsl"]
    assert hsl["crawl_outcome"] == "ok" and hsl["failure_class"] is None
    assert hsl["route_count"] == 7 and hsl["has_calendar"] is True
    assert hsl["country_agreement"] == "agree"
    assert hsl["municipality_outcome"] == "in_place"
    assert hsl["places_served"] == 3 and hsl["cities_served"] == 2
    assert hsl["countries_served"] == 2
    assert json.loads(hsl["edges_by_tier"]) == {
        "local": 1,
        "regional": 2,
        "international": 1,
    }
    assert json.loads(hsl["edges_by_category"]) == {"primary": 4}
    assert hsl["relevance_max"] == 0.9 and hsl["relevance_median"] == pytest.approx(
        0.65
    )
    assert hsl["licence_state"] == "declared" and hsl["redistribution_allowed"] is True
    assert hsl["download_url"] == "https://hsl.fi/g.zip"
    assert hsl["catalogue_status"] == "active" and hsl["snapshot_id"] == "snap"
    # The crawl log and the placement name an alias: both still join.
    rail = by_id["rail"]
    assert rail["route_count"] == 2 and rail["municipality_outcome"] == "in_region"
    assert rail["country_agreement"] == "disagree"
    assert rail["licence_state"] == "declared"
    gone = by_id["gone"]
    assert gone["crawl_outcome"] == "failed" and gone["failure_class"] == "404"
    assert gone["country_agreement"] == "unobserved"
    assert gone["municipality_outcome"] is None and gone["places_served"] == 1
    assert gone["relevance_max"] is None and gone["licence_state"] == "none"
    assert by_id["far"]["municipality_outcome"] == "elsewhere"
    assert by_id["far"]["crawl_outcome"] == "not_modified"
    assert by_id["lost"]["municipality_outcome"] == "unplaceable"
    assert by_id["lost"]["country_agreement"] == "undeclared"
    bikes = by_id["bikes"]
    assert bikes["crawl_outcome"] == "skipped" and bikes["failure_class"] is None
    assert bikes["download_url"] == "https://b/gbfs.json"
    sections = stats.feed_sections(rows)
    assert sections["availability"]["by_outcome"] == {
        "ok": 2,
        "failed": 1,
        "not_modified": 1,
        "range": 1,
        "skipped": 1,
    }
    assert sections["availability"]["failures_by_class"] == {"404": 1}
    assert sections["availability"]["outcome_by_catalogue_status"] == {
        "active": {"ok": 1},
        "deprecated": {"failed": 1},
        "unknown": {"ok": 1, "not_modified": 1, "range": 1, "skipped": 1},
    }
    assert sections["licensing"]["licence_state"] == {"declared": 2, "none": 4}
    assert sections["licensing"]["redistribution_allowed"] == {"yes": 1, "unknown": 5}
    assert sections["scale"]["stop_count"] == {
        "count": 2,
        "median": 300.0,
        "p95": 500,
        "max": 500,
    }
    assert sections["scale"]["countries_served"] == {"2": 1, "1": 4}
    assert sections["country_agreement"]["by_agreement"] == {
        "agree": 1,
        "disagree": 1,
        "unobserved": 2,
        "undeclared": 2,
    }
    assert sections["declared_municipality"]["outcome"] == {
        "in_place": 1,
        "in_region": 1,
        "elsewhere": 1,
        "unplaceable": 1,
    }
    assert sections["declared_municipality"]["by_level"] == {
        "municipality": 3,
        "district": 1,
    }
