import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import coverage, store  # noqa: E402


def _publish(cache, subdir, pointer, artifact, records, manifest=None):
    directory = store.open_subdir(cache, subdir)
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / subdir,
                pointer,
                {artifact: store.jsonl_chunks(records)},
                manifest or {"source": subdir},
                held=directory,
            )
    finally:
        directory.close()


def _place(place_id, kind, *, parent_id=None, metro_ids=()):
    return {
        "place_id": place_id,
        "kind": kind,
        "name": place_id,
        "parent_id": parent_id,
        "metro_ids": list(metro_ids),
        "member_ids": [],
    }


def _feed(feed_id, *, spec="gtfs", static_feed_id=None, aliases=()):
    return {
        "feed_id": feed_id,
        "spec": spec,
        "static_feed_id": static_feed_id,
        "static_link_method": "declared" if static_feed_id else None,
        "aliases": list(aliases),
    }


PLACES = [
    _place("Q-c", "country"),
    _place("Q-reg", "region", parent_id="Q-c"),
    _place("Q-city", "city", parent_id="Q-reg", metro_ids=["Q-metro", "Q-csa"]),
    _place("Q-metro", "metro"),
    _place("Q-csa", "metro"),
    _place("Q-other", "city", parent_id="Q-reg"),
]

FEEDS = [
    _feed("f-city"),
    _feed("f-reg"),
    _feed("f-rt-linked", spec="gtfs-rt", static_feed_id="f-city"),
    _feed("f-rt-alone", spec="gtfs-rt"),
    _feed("f-rt-oldlink", spec="gtfs-rt", static_feed_id="f-old"),
    _feed("f-rt-dangling", spec="gtfs-rt", static_feed_id="f-vanished"),
    _feed("f-rt-badlink", spec="gtfs-rt", static_feed_id="f-rt-linked"),
    _feed("f-none"),
    _feed("f-lost"),
    _feed("f-new", aliases=["f-old"]),  # renamed by an identity override
]

PLACEMENTS = [
    {"feed_id": "f-city", "place_id": "Q-city", "level": "municipality"},
    {"feed_id": "f-reg", "place_id": "Q-reg", "level": "subdivision"},
    {"feed_id": "f-rt-linked", "place_id": "Q-other", "level": "municipality"},
    {"feed_id": "f-rt-alone", "place_id": "Q-other", "level": "municipality"},
    {"feed_id": "f-rt-oldlink", "place_id": "Q-other", "level": "municipality"},
    {"feed_id": "f-rt-dangling", "place_id": "Q-other", "level": "municipality"},
    {"feed_id": "f-rt-badlink", "place_id": "Q-other", "level": "municipality"},
    {"feed_id": "f-lost", "place_id": "Q-ghost", "level": "municipality"},
    {"feed_id": "f-old", "place_id": "Q-other", "level": "country"},
    {"feed_id": "f-gone", "place_id": "Q-city", "level": "municipality"},
]


SOURCES = {"atlas": {"commit": "abc"}}
RELEASE = "2026-08-19.0"


def _cover(
    tmp_path,
    feeds=FEEDS,
    places=PLACES,
    placements=PLACEMENTS,
    seed_sources=SOURCES,
    seed_release=RELEASE,
):
    cache = tmp_path / "cache"
    _publish(
        cache,
        "resolve",
        "feeds_resolved.json",
        "feeds_resolved.jsonl",
        feeds,
        {"source": "resolve", "sources": SOURCES},
    )
    _publish(
        cache,
        "gazetteer",
        "expanded.json",
        "places_expanded.jsonl",
        places,
        {"source": "expand", "overture_release": RELEASE},
    )
    _publish(
        cache,
        "gazetteer",
        "seed.json",
        "feed_places.jsonl",
        placements,
        {"source": "seed", "sources": seed_sources, "overture_release": seed_release},
    )
    manifest = coverage.cover(cache)
    covered, _ = store.read_jsonl(
        cache / "coverage", "coverage.json", "feeds_covered.jsonl"
    )
    edges, _ = store.read_jsonl(
        cache / "coverage", "coverage.json", "edges_candidate.jsonl"
    )
    grouped = {}
    for edge in edges:
        grouped.setdefault(edge["feed_id"], {})[edge["place_id"]] = edge
    return manifest, {f["feed_id"]: f for f in covered}, grouped


def test_a_city_placement_reaches_its_ancestors_and_every_metro(tmp_path):
    _, _, edges = _cover(tmp_path)
    city = edges["f-city"]
    assert set(city) == {"Q-city", "Q-reg", "Q-c", "Q-metro", "Q-csa"}
    # Same flat confidence everywhere, and every declared edge is unclassified.
    for edge in city.values():
        assert edge["confidence"] == 0.50
        assert edge["tier"] == "unknown"
        assert edge["tier_confidence"] == 0.0
        assert edge["selector_state"] == "unavailable"
        assert edge["fingerprint_kind"] == "none"
        assert edge["needs_review"] is True
        assert edge["evidence"]["declared_place_id"] == "Q-city"
    assert "Q-other" not in city


def test_a_subdivision_placement_is_coarser_and_reaches_no_metro(tmp_path):
    _, _, edges = _cover(tmp_path)
    region = edges["f-reg"]
    assert set(region) == {"Q-reg", "Q-c"}
    assert region["Q-reg"]["confidence"] == 0.35


def test_a_linked_gtfs_rt_feed_gets_no_direct_edges(tmp_path):
    # It inherits its static feed's membership in the edge-override stage.
    manifest, covered, edges = _cover(tmp_path)
    assert "f-rt-linked" not in edges
    assert covered["f-rt-linked"]["coverage_source"] is None
    assert manifest["linked_rt_feeds"] == 2  # f-rt-linked and f-rt-oldlink


def test_an_unlinked_gtfs_rt_feed_falls_back_to_declared_coverage(tmp_path):
    _, covered, edges = _cover(tmp_path)
    assert "Q-other" in edges["f-rt-alone"]
    assert covered["f-rt-alone"]["coverage_source"] == "declared"


def test_a_static_link_to_a_renamed_feed_is_canonicalised_and_kept(tmp_path):
    manifest, covered, edges = _cover(tmp_path)
    assert covered["f-rt-oldlink"]["static_feed_id"] == "f-new"
    assert "f-rt-oldlink" not in edges  # linked, so it inherits later
    assert manifest["linked_rt_feeds"] == 2


def test_a_dangling_static_link_falls_back_to_declared_and_is_reported(tmp_path):
    manifest, covered, edges = _cover(tmp_path)
    assert covered["f-rt-dangling"]["static_feed_id"] is None
    assert covered["f-rt-dangling"]["static_link_method"] == "none"
    assert covered["f-rt-dangling"]["dangling_static_feed_id"] == "f-vanished"
    assert "Q-other" in edges["f-rt-dangling"]
    assert manifest["dangling_static_links"] == ["f-rt-badlink", "f-rt-dangling"]


def test_a_link_to_a_non_static_target_is_dangling(tmp_path):
    # f-rt-badlink points at another GTFS-RT feed; inheritance can only follow a
    # static GTFS feed, so the link is dropped and declared coverage applies.
    _, covered, edges = _cover(tmp_path)
    assert covered["f-rt-badlink"]["static_feed_id"] is None
    assert "Q-other" in edges["f-rt-badlink"]


def test_a_placement_under_a_renamed_feeds_old_id_lands_on_the_new_id(tmp_path):
    _, covered, edges = _cover(tmp_path)
    assert "f-old" not in edges
    assert set(edges["f-new"]) == {"Q-other", "Q-reg", "Q-c"}
    assert covered["f-new"]["coverage_source"] == "declared"


def test_an_unplaced_feed_has_no_edges_and_no_coverage_source(tmp_path):
    _, covered, edges = _cover(tmp_path)
    assert "f-none" not in edges
    assert covered["f-none"]["coverage_source"] is None


def test_unknown_places_and_unmatched_feeds_are_reported(tmp_path):
    manifest, _, edges = _cover(tmp_path)
    assert "f-lost" not in edges
    assert manifest["unknown_place_ids"] == ["Q-ghost"]
    assert manifest["unmatched_feed_ids"] == ["f-gone"]


def test_the_manifest_carries_sources_release_and_counts(tmp_path):
    manifest, _, edges = _cover(tmp_path)
    assert manifest["sources"] == SOURCES
    assert manifest["overture_release"] == RELEASE
    assert manifest["mode"] == "declared"
    assert manifest["feeds_covered"] == 6
    assert manifest["edges"] == sum(len(e) for e in edges.values())
    assert manifest["edges_by_place_kind"]["metro"] == 2


def test_mixed_catalogue_lineage_is_refused(tmp_path):
    with pytest.raises(coverage.CoverageError, match="catalogue versions"):
        _cover(tmp_path, seed_sources={"atlas": {"commit": "other"}})


def test_mixed_overture_releases_are_refused(tmp_path):
    with pytest.raises(coverage.CoverageError, match="Overture releases"):
        _cover(tmp_path, seed_release="2026-07-01.0")


def test_a_parent_cycle_cannot_loop_the_reach():
    places = {
        "A": _place("A", "city", parent_id="B"),
        "B": _place("B", "region", parent_id="A"),
    }
    assert coverage._reach("A", places) == ["A", "B"]
