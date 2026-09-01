import hashlib
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
from index_build import boundaries, crawl, expand, store  # noqa: E402

# A source the geometry allowlist accepts, and one it does not.
GOOD = [{"dataset": "OpenStreetMap", "license": "ODbL-1.0", "property": ""}]
BAD = [{"dataset": "Mystery Maps", "license": "proprietary", "property": ""}]


def _wkb(minx, miny, maxx, maxy):
    return shapely.to_wkb(shapely.box(minx, miny, maxx, maxy))


SEED_PLACES = [
    {
        "place_id": "Q33",
        "kind": "country",
        "name": "Finland",
        "country_code": "FI",
        "overture_id": "fi",
        "metro_ids": [],
        "member_ids": [],
    },
]

DIVISIONS = [
    fx.division(
        "fi",
        "FI",
        "country",
        wikidata="Q33",
        name="Finland",
        hierarchies=fx.chain(("fi", "country", "Finland")),
    ),
    fx.division(
        "fi-tre",
        "FI",
        "locality",
        wikidata="Q40840",
        name="Tampere",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-pirk", "region", "Pirkanmaa"),
            ("fi-tre", "locality", "Tampere"),
        ),
    ),
    fx.division(
        "fi-pirk",
        "FI",
        "region",
        wikidata="Q5697",
        name="Pirkanmaa",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-pirk", "region", "Pirkanmaa"),
        ),
    ),
    fx.division(
        "fi-noqid",
        "FI",
        "locality",
        name="Nowhere",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-noqid", "locality", "Nowhere")
        ),
    ),
    fx.division(
        "us-spring",
        "US",
        "locality",
        wikidata="Q28515",
        name="Springfield",
        hierarchies=fx.chain(
            ("us", "country", "USA"), ("us-spring", "locality", "Springfield")
        ),
    ),
    fx.division(
        "fi-badgeo",
        "FI",
        "locality",
        wikidata="Q999",
        name="Badgeo",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-badgeo", "locality", "Badgeo")
        ),
    ),
]

AREAS = [
    fx.area("fi", _wkb(19.0, 59.0, 32.0, 71.0), GOOD, country="FI"),
    fx.area("fi-tre", _wkb(23.6, 61.4, 24.0, 61.6), GOOD, country="FI"),
    # A second, disjoint component of the same city, far from any stop.
    fx.area("fi-tre", _wkb(21.9, 60.9, 22.1, 61.1), GOOD, country="FI"),
    fx.area("fi-pirk", _wkb(22.5, 61.0, 24.5, 62.0), GOOD, country="FI"),
    fx.area("fi-noqid", _wkb(26.0, 62.0, 26.4, 62.2), GOOD, country="FI"),
    fx.area("us-spring", _wkb(-89.8, 39.7, -89.5, 39.9), GOOD, country="US"),
    fx.area("fi-badgeo", _wkb(28.0, 62.0, 28.4, 62.2), BAD, country="FI"),
]


def _publish_names(cache, places):
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "gazetteer",
                "names.json",
                {"places_seed.jsonl": store.jsonl_chunks(places)},
                {"source": "names", "overture_release": "2026-08-19.0"},
                held=directory,
            )
    finally:
        directory.close()


def _write_crawl(cache, feed_id, stops_rows):
    feed_dir = cache / "crawl" / crawl._dir_name(feed_id)
    feed_dir.mkdir(parents=True, exist_ok=True)
    stops_text = "stop_id,stop_lat,stop_lon\n" + "".join(stops_rows)
    (feed_dir / "stops.txt").write_text(stops_text)
    (feed_dir / "state.json").write_text(
        json.dumps(
            {
                "feed_id": feed_id,
                "members": ["stops.txt"],
                "member_sha256": {
                    "stops.txt": hashlib.sha256(stops_text.encode()).hexdigest()
                },
            }
        )
    )
    log = {
        "feed_id": feed_id,
        "directory": crawl._dir_name(feed_id),
        "members": ["stops.txt"],
        "method": "download",
    }
    log_path = cache / "crawl" / "crawl_log.jsonl"
    existing = log_path.read_text() if log_path.is_file() else ""
    log_path.write_text(existing + json.dumps(log) + "\n")


def _expand(tmp_path, cache):
    divisions = fx.write_dataset(tmp_path / "divisions.parquet", DIVISIONS)
    areas = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    lookup = boundaries.BoundaryLookup(
        cache, release="2026-08-19.0", area_dataset=areas, division_dataset=divisions
    )
    wikidata = fx.StubWikidata(
        {},
        {"Q28515": [{"qid": "Q912579", "name": "Springfield MSA", "cbsa": "44100"}]},
        {
            "Q40840": {
                "labels": {"en": "Tampere", "fi": "Tampere"},
                "aliases": ["Manse"],
            },
            "Q912579": {
                "labels": {"fi": "Springfieldin metropolialue"},
                "aliases": ["Greater Springfield"],
            },
        },
    )
    try:
        manifest = expand.expand(
            cache, lookup=lookup, wikidata=wikidata, area_dataset=areas
        )
    finally:
        lookup.close()
    places, _ = store.read_jsonl(
        cache / "gazetteer", "expanded.json", "places_expanded.jsonl"
    )
    report, _ = store.read_jsonl(
        cache / "gazetteer", "expanded.json", "expansion_report.jsonl"
    )
    return manifest, {p["place_id"]: p for p in places}, report


def test_no_crawl_artifacts_pass_the_seed_through(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    manifest, places, report = _expand(tmp_path, cache)
    assert manifest["mode"] == "declared"
    assert manifest["places_added"] == 0
    assert set(places) == {"Q33"}
    assert report == []


def test_a_crawled_stop_discovers_an_unseeded_city(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["mode"] == "expanded"
    tampere = places["Q40840"]
    assert tampere["kind"] == "city"
    # The unseeded intermediate region was added too, chained to the seed.
    region = places["Q5697"]
    assert region["kind"] == "region"
    assert region["parent_id"] == "Q33"
    assert tampere["parent_id"] == "Q5697"
    assert tampere["resolution_method"] == "overture_wikidata"
    # The boundary passed the licence audit and was simplified in — and it is
    # the COMPLETE area read, so the disjoint far component shipped too.
    assert tampere["geometry"]
    assert tampere["geometry_source"] == "overture"
    boundary = shapely.from_wkb(bytes.fromhex(tampere["geometry"]))
    assert boundary.covers(shapely.Point(22.0, 61.0))
    # Wikidata names merged.
    assert tampere["names"]["fi"] == "Tampere"
    assert tampere["aliases"] == ["Manse"]
    # The country was already seeded and is not duplicated or replaced.
    assert places["Q33"] is not tampere
    assert manifest["places_added"] == 2  # the city and its region


def test_an_already_seeded_place_is_not_re_added(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-country", ["s1,68.0,27.0\n"])  # only Finland covers it
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["places_added"] == 0
    assert set(places) == {"Q33"}


def test_a_qidless_division_is_reported_not_minted(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-noqid", ["s1,62.1,26.2\n"])
    manifest, places, report = _expand(tmp_path, cache)
    assert "Q" not in "".join(p for p in places if p != "Q33")
    assert any(r["overture_id"] == "fi-noqid" for r in report)
    assert manifest["reported"] >= 1


def test_a_discovered_us_city_gains_its_metro(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-spring", ["s1,39.8,-89.65\n"])
    manifest, places, _ = _expand(tmp_path, cache)
    city = places["Q28515"]
    metro = places["Q912579"]
    assert metro["kind"] == "metro"
    assert metro["statistical_area_id"] == "44100"
    assert city["metro_ids"] == ["Q912579"]
    assert metro["member_ids"] == ["Q28515"]
    assert manifest["metros_added"] == 1
    # The minted metro is enriched like every other new place: Wikidata labels
    # join for languages the row lacks (its own English label keeps precedence)
    # and aliases merge in.
    assert metro["names"]["fi"] == "Springfieldin metropolialue"
    assert "Greater Springfield" in metro["aliases"]


def test_an_unauditable_boundary_ships_without_geometry(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-badgeo", ["s1,62.1,28.2\n"])
    _, places, _ = _expand(tmp_path, cache)
    badgeo = places["Q999"]
    assert badgeo["geometry"] is None
    assert badgeo["geometry_source"] is None


def test_a_stops_file_that_fails_its_state_digest_is_skipped(tmp_path):
    # A crash can leave a member newer or older than state.json; mismatched
    # bytes must not become gazetteer evidence.
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    stops = cache / "crawl" / crawl._dir_name("f-tre") / "stops.txt"
    stops.write_text(stops.read_text() + "s2,61.5,23.81\n")
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["state_mismatches"] == 1
    assert manifest["places_added"] == 0
    assert set(places) == {"Q33"}


def test_a_corrupt_state_file_skips_only_that_feed(tmp_path):
    # A syntactically valid but non-object state is one feed's corruption;
    # it must be skipped, never abort the expansion.
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    _write_crawl(cache, "f-corrupt", ["s1,61.5,23.8\n"])
    state = cache / "crawl" / crawl._dir_name("f-corrupt") / "state.json"
    state.write_text("[1, 2]")
    log_path = cache / "crawl" / "crawl_log.jsonl"
    log_path.write_text(log_path.read_text() + '[]\n{"directory": 3}\n')
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["feeds_scanned"] == 1
    assert "Q40840" in places


def test_unparsable_stop_rows_are_skipped(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(
        cache,
        "f-tre",
        ["s1,not-a-lat,23.8\n", "s2,61.5,23.8\n", "s3,1e308,23.8\n", "s4,61.5,200\n"],
    )
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["stops_read"] == 1
    assert "Q40840" in places
