"""Gazetteer pass B2: pruning the expanded places against the final edges."""

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import classify, curate, prune, publish, store  # noqa: E402
from test_index_classify import (  # noqa: E402
    LOOKUP,
    PLACES,
    ROUTES_A,
    STOP_TIMES_A,
    STOPS_A,
    TRIPS_A,
    _candidate,
    _coverage,
    _place,
    _write_crawl,
)


def _edge(place_id, feed_id="f"):
    return {"place_id": place_id, "feed_id": feed_id}


def test_the_keep_set_is_edges_curation_ancestors_and_metros_of_kept_cities():
    places = {
        p["place_id"]: p
        for p in [
            _place("Q-c", "country"),
            _place("Q-reg", "region", parent_id="Q-c"),
            _place("Q-city", "city", parent_id="Q-reg", metro_ids=["Q-metro"]),
            _place("Q-empty", "city", parent_id="Q-reg", metro_ids=["Q-lone"]),
            _place("Q-metro", "metro", member_ids=["Q-city"]),
            _place("Q-lone", "metro", member_ids=["Q-empty"]),
            {**_place("Q-hand", "city", parent_id="Q-reg"), "curated": True},
        ]
    }
    kept = prune.keep_set(places, [_edge("Q-city")])
    # The city, its ancestors, its metro, and the curated city; not the
    # empty city nor the metro only it belonged to.
    assert kept == {"Q-city", "Q-reg", "Q-c", "Q-metro", "Q-hand"}


def test_removals_cascade_without_dangling_ids():
    places = [
        _place("Q-c", "country"),
        _place("Q-reg", "region", parent_id="Q-c"),
        # The kept city's parent was never in the gazetteer, its default
        # metro belongs only to the empty city, and one of its metro ids
        # names nothing: re-pointed, cleared and trimmed respectively.
        {
            **_place(
                "Q-city", "city", parent_id="Q-gone", metro_ids=["Q-metro", "Q-dead"]
            ),
            "default_metro_id": "Q-lone",
        },
        _place("Q-empty", "city", parent_id="Q-reg", metro_ids=["Q-lone"]),
        # A city in exactly one metro with no explicit default: the implicit
        # default is made explicit here, before anything can be cleared.
        _place("Q-solo", "city", parent_id="Q-reg", metro_ids=["Q-metro"]),
        _place("Q-metro", "metro", member_ids=["Q-city", "Q-empty", "Q-solo"]),
        _place("Q-lone", "metro", member_ids=["Q-empty"]),
    ]
    survivors, report = prune.prune_places(places, [_edge("Q-city"), _edge("Q-solo")])
    by_id = {p["place_id"] for p in survivors}
    assert by_id == {"Q-city", "Q-metro", "Q-solo", "Q-reg", "Q-c"}
    city = next(p for p in survivors if p["place_id"] == "Q-city")
    metro = next(p for p in survivors if p["place_id"] == "Q-metro")
    solo = next(p for p in survivors if p["place_id"] == "Q-solo")
    assert solo["default_metro_id"] == "Q-metro"
    assert city["parent_id"] is None and city["default_metro_id"] is None
    # Cleared stays cleared through publication: the lone surviving metro
    # is not re-inferred as a default.
    assert city["default_metro_cleared"] is True
    assert publish._place_row(city, "snap")["default_metro_id"] is None
    assert publish._place_row(solo, "snap")["default_metro_id"] == "Q-metro"
    assert city["metro_ids"] == ["Q-metro"]
    assert metro["member_ids"] == ["Q-city", "Q-solo"]
    assert report["dropped_city"] == 1 and report["dropped_metro"] == 1
    assert report["dropped_region"] == 0 and report["dropped_country"] == 0
    assert report["default_metro_cleared"] == 1 and report["reparented"] == 1
    assert report["metro_ids_trimmed"] == 1 and report["member_ids_trimmed"] == 1
    # The input rows are untouched.
    assert places[2]["default_metro_id"] == "Q-lone"


def _curated_cache(tmp_path, extra_places=()):
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-a", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-a",
        {
            "stops.txt": STOPS_A,
            "routes.txt": ROUTES_A,
            "trips.txt": TRIPS_A,
            "stop_times.txt": STOP_TIMES_A,
        },
        "complete",
    )
    _coverage(
        cache,
        feeds,
        [_candidate(place, "f-a") for place in ("Q-city", "Q-reg", "Q-c", "Q-metro")],
        places=PLACES + list(extra_places),
    )
    classify.classify(cache, lookup=LOOKUP)
    curate.curate(cache, overrides_dir=None)
    return cache


def test_the_stage_prunes_against_the_curated_edges_and_publish_ships_the_result(
    tmp_path,
):
    cache = _curated_cache(tmp_path, [_place("Q-empty", "city", parent_id="Q-reg")])
    manifest = prune.prune(cache)
    places, _ = store.read_jsonl(
        cache / "prune", "places_pruned.json", "places_pruned.jsonl"
    )
    # Q-other had no candidate here, so it goes with the empty city.
    assert {p["place_id"] for p in places} == {"Q-city", "Q-reg", "Q-c", "Q-metro"}
    assert manifest["dropped_city"] == 2 and manifest["kept"] == 4
    _, _, edge_manifest, _ = publish._read_coverage(cache)
    shipped, release, generation, _ = publish._read_places(cache, edge_manifest)
    assert {p["place_id"] for p in shipped} == {"Q-city", "Q-reg", "Q-c", "Q-metro"}
    assert generation == edge_manifest["expanded_generation"] and release
    # A re-curation leaves the pruned places behind.
    curate.curate(cache, overrides_dir=None)
    _, _, edge_manifest, _ = publish._read_coverage(cache)
    with pytest.raises(publish.PublishError, match="re-run the prune"):
        publish._read_places(cache, edge_manifest)


def test_a_pruning_that_drops_nothing_reports_zeros():
    survivors, report = prune.prune_places(
        PLACES, [_edge(p["place_id"]) for p in PLACES]
    )
    assert len(survivors) == len(PLACES)
    assert all(report[metric] == 0 for metric in prune.METRICS if metric != "kept")
    assert report["kept"] == len(PLACES) and report["dropped_city"] == 0


def test_a_curated_build_without_pruned_places_is_refused(tmp_path):
    cache = _curated_cache(tmp_path)
    _, _, edge_manifest, _ = publish._read_coverage(cache)
    with pytest.raises(publish.PublishError, match="run the prune stage"):
        publish._read_places(cache, edge_manifest)


def test_pruning_needs_final_edges(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    with pytest.raises(prune.PruneError, match="run curate"):
        prune.prune(cache)
