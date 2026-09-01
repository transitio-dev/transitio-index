import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import hashlib  # noqa: E402
import json  # noqa: E402

from index_build import coverage, crawl, store  # noqa: E402


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
    crawls=None,
    lookup=None,
    tamper=None,
    **cover_args,
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
        {"source": "expand", "overture_release": RELEASE, "sources": SOURCES},
    )
    _publish(
        cache,
        "gazetteer",
        "seed.json",
        "feed_places.jsonl",
        placements,
        {"source": "seed", "sources": seed_sources, "overture_release": seed_release},
    )
    for feed_id, stops_rows in (crawls or {}).items():
        _write_crawl(cache, feed_id, stops_rows)
    if tamper:
        stops = cache / "crawl" / crawl._dir_name(tamper) / "stops.txt"
        stops.write_bytes(stops.read_bytes() + b"sx,1.0,10.0\n")
    manifest = coverage.cover(cache, lookup=lookup, **cover_args)
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


def _write_crawl(cache, feed_id, stops_rows):
    feed_dir = cache / "crawl" / crawl._dir_name(feed_id)
    feed_dir.mkdir(parents=True, exist_ok=True)
    stops = ("stop_id,stop_lat,stop_lon\n" + "".join(stops_rows)).encode()
    (feed_dir / "stops.txt").write_bytes(stops)
    (feed_dir / "state.json").write_text(
        json.dumps(
            {
                "feed_id": feed_id,
                "members": ["stops.txt"],
                "member_sha256": {"stops.txt": hashlib.sha256(stops).hexdigest()},
            }
        )
    )
    log_path = cache / "crawl" / "crawl_log.jsonl"
    existing = log_path.read_text() if log_path.is_file() else ""
    log_path.write_text(
        existing
        + json.dumps({"feed_id": feed_id, "directory": crawl._dir_name(feed_id)})
        + "\n"
    )


class StubLookup:
    """Answers divisions_at from a fixed table, keyed by stop longitude."""

    def __init__(self, by_x):
        self.by_x = by_x

    def ensure(self, boxes):
        return 0

    def divisions_at(self, x, y):
        return self.by_x.get(x, [])


# Longitude 10.0 sits in Q-city (and its region and country); 20.0 in Q-other.
LOOKUP = StubLookup(
    {
        10.0: [
            {"kind": "city", "wikidata": "Q-city"},
            {"kind": "region", "wikidata": "Q-reg"},
            {"kind": "country", "wikidata": "Q-c"},
        ],
        20.0: [
            {"kind": "city", "wikidata": "Q-other"},
            {"kind": "region", "wikidata": "Q-reg"},
            {"kind": "country", "wikidata": "Q-c"},
        ],
    }
)


def _rows(count, lon):
    return [f"s{lon}-{i},1.0,{lon}\n" for i in range(count)]


def test_crawled_stops_supersede_declared_and_pin_the_cutoff(tmp_path):
    # 245 of 250 stops in Q-city (confidence 1.0), 5 in Q-other — exactly the
    # admission threshold, so its confidence 0.68 sits below the 0.70 cutoff.
    manifest, covered, edges = _cover(
        tmp_path,
        crawls={"f-city": _rows(245, 10.0) + _rows(5, 20.0)},
        lookup=LOOKUP,
    )
    assert manifest["mode"] == "crawled"
    assert manifest["feeds_crawl_covered"] == 1
    assert covered["f-city"]["coverage_source"] == "crawl"
    assert covered["f-city"]["stop_count"] == 250
    assert covered["f-city"]["coverage"]  # measured hull replaces declared
    city = edges["f-city"]
    assert city["Q-city"]["method"] == "crawl"
    assert set(city) == {"Q-city", "Q-other", "Q-reg", "Q-c", "Q-metro", "Q-csa"}
    assert city["Q-city"]["confidence"] == 1.0
    assert city["Q-city"]["needs_review"] is False
    assert city["Q-other"]["confidence"] == pytest.approx(0.68)
    assert city["Q-other"]["needs_review"] is True
    assert city["Q-other"]["evidence"] == {
        "stops_in_place": 5,
        "stop_share": pytest.approx(0.02),
        "min_stops": 5,
        "min_stop_share": 0.02,
        "review_cutoff": 0.70,
    }
    # The metros propagate from the member city at its confidence, and no
    # declared evidence survives on a superseded feed.
    assert city["Q-metro"]["confidence"] == 1.0
    assert "declared_level" not in city["Q-city"]["evidence"]


def test_a_tiny_feed_passes_on_share_and_a_sparse_place_fails(tmp_path):
    # Two of three stops in Q-other: below min_stops but at two-thirds share,
    # the override admits it; the one Q-city stop fails both rules.
    _, _, edges = _cover(
        tmp_path,
        crawls={"f-none": _rows(2, 20.0) + _rows(1, 10.0)},
        lookup=LOOKUP,
    )
    tiny = edges["f-none"]
    assert "Q-city" not in tiny
    assert tiny["Q-other"]["confidence"] == 1.0
    assert set(tiny) == {"Q-other", "Q-reg", "Q-c"}


def test_the_admission_thresholds_are_configurable(tmp_path):
    _, _, edges = _cover(
        tmp_path,
        crawls={"f-city": _rows(245, 10.0) + _rows(5, 20.0)},
        lookup=LOOKUP,
        min_stops=6,
    )
    # With min_stops raised past the five Q-other stops, only the share
    # override could admit it, and 0.02 is far below half.
    assert "Q-other" not in edges["f-city"]
    assert edges["f-city"]["Q-city"]["evidence"]["min_stops"] == 6


def test_dropped_rows_stay_in_the_share_denominator(tmp_path):
    # Five good stops among 245 unparsable rows must not read as 100% share:
    # the share is 5/250, exactly the threshold, and confidence 0.68.
    rows = _rows(5, 20.0) + ["s-bad%d,not-a-lat,20.0\n" % i for i in range(245)]
    _, covered, edges = _cover(tmp_path, crawls={"f-none": rows}, lookup=LOOKUP)
    edge = edges["f-none"]["Q-other"]
    assert edge["confidence"] == pytest.approx(0.68)
    assert edge["evidence"]["stop_share"] == pytest.approx(0.02)
    assert covered["f-none"]["stop_count"] == 250


def test_the_coverage_hull_respects_the_antimeridian(tmp_path):
    shapely = pytest.importorskip("shapely")
    lookup = StubLookup(
        {
            179.9: [{"kind": "city", "wikidata": "Q-other"}],
            -179.9: [{"kind": "city", "wikidata": "Q-other"}],
        }
    )
    _, covered, _ = _cover(
        tmp_path,
        crawls={"f-none": _rows(3, 179.9) + _rows(3, -179.9)},
        lookup=lookup,
    )
    hull = shapely.from_wkb(bytes.fromhex(covered["f-none"]["coverage"]))
    assert hull.covers(shapely.Point(179.9, 1.0))
    assert hull.covers(shapely.Point(-179.9, 1.0))
    assert not hull.covers(shapely.Point(0.0, 1.0))


def test_the_hull_frame_survives_a_greenwich_and_dateline_mix(tmp_path):
    # -1, 0 and 180: the naive "shift every negative" frame would span 359
    # degrees; the largest-gap frame keeps the hull to the short way round.
    shapely = pytest.importorskip("shapely")
    records = [{"kind": "city", "wikidata": "Q-other"}]
    lookup = StubLookup({-1.0: records, 0.0: records, 180.0: records})
    _, covered, _ = _cover(
        tmp_path,
        crawls={"f-none": _rows(2, -1.0) + _rows(2, 0.0) + _rows(2, 180.0)},
        lookup=lookup,
    )
    hull = shapely.from_wkb(bytes.fromhex(covered["f-none"]["coverage"]))
    assert hull.covers(shapely.Point(-0.5, 1.0))
    assert hull.covers(shapely.Point(180.0, 1.0))
    assert not hull.covers(shapely.Point(90.0, 1.0))


def test_all_unparsable_stops_still_supersede_declared(tmp_path):
    # A digest-valid stops.txt whose rows are all unparsable is the crawl's
    # answer: declared edges go, the row count is kept, nothing is placed.
    manifest, covered, edges = _cover(
        tmp_path,
        crawls={"f-city": ["s%d,not-a-lat,10.0\n" % i for i in range(4)]},
        lookup=LOOKUP,
    )
    assert covered["f-city"]["coverage_source"] == "crawl"
    assert covered["f-city"]["stop_count"] == 4
    assert covered["f-city"]["coverage"] is None
    assert "f-city" not in edges


def test_a_state_mismatched_crawl_falls_back_to_declared(tmp_path):
    manifest, covered, edges = _cover(
        tmp_path,
        crawls={"f-city": _rows(10, 10.0)},
        lookup=LOOKUP,
        tamper="f-city",
    )
    assert manifest["crawl_state_mismatches"] == 1
    assert covered["f-city"]["coverage_source"] == "declared"
    assert edges["f-city"]["Q-city"]["confidence"] == 0.50


def test_an_unknown_qid_division_means_a_stale_gazetteer(tmp_path):
    # A QID-bearing division the gazetteer does not know can only mean
    # places_expanded predates this crawl: refuse, never drop edges silently.
    lookup = StubLookup(
        {30.0: [{"kind": "city", "wikidata": "Q999999", "overture_id": "xx"}]}
    )
    with pytest.raises(coverage.CoverageError, match="expand stage"):
        _cover(tmp_path, crawls={"f-city": _rows(6, 30.0)}, lookup=lookup)


def test_an_uncovered_lookup_is_a_stage_order_error(tmp_path):
    class Refusing:
        def ensure(self, boxes):
            raise store.StoreError("needs its datasets")

        def divisions_at(self, x, y):
            return []

    with pytest.raises(coverage.CoverageError, match="expand stage"):
        _cover(tmp_path, crawls={"f-city": _rows(6, 10.0)}, lookup=Refusing())


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
