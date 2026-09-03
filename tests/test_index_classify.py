import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import classify, coverage, crawl, publish, store  # noqa: E402


@pytest.mark.parametrize(
    ("route_type", "countries", "span", "median", "tier", "confidence", "rule"),
    [
        (3, {"AA", "BB"}, 5.0, 1.0, "international", 0.95, 1),  # rule 1 wins
        (109, {"AA"}, 100.0, None, "regional", 0.95, 2),
        (109, {"AA"}, 400.0, None, "national", 0.95, 2),
        (109, {"AA"}, None, None, "unknown", 0.0, 9),  # span needed
        (204, None, None, None, "national", 0.95, 2),
        (715, None, None, None, "local", 0.95, 2),
        (1000, {"AA"}, 20.0, None, "local", 0.95, 2),
        (1100, {"AA"}, 20.0, None, "unknown", 0.0, 9),  # unlisted extended
        (1, None, None, None, "local", 0.90, 3),
        (2, {"AA"}, 100.0, None, "regional", 0.75, 4),
        (4, {"AA"}, 80.0, None, "regional", 0.75, 5),
        (3, {"AA"}, 30.0, 1.0, "local", 0.85, 6),
        (3, {"AA"}, 150.0, 5.0, "regional", 0.65, 7),
        (3, {"AA"}, 1000.0, 50.0, "national", 0.60, 8),
        (3, {"AA"}, None, 1.0, "unknown", 0.0, 9),  # bus without span
        (None, {"AA"}, 30.0, 1.0, "unknown", 0.0, 9),
    ],
)
def test_the_decision_table(
    route_type, countries, span, median, tier, confidence, rule
):
    decision = classify.classify_route(route_type, countries, span, median)
    assert decision["tier"] == tier
    assert decision["tier_confidence"] == pytest.approx(confidence)
    assert decision["rule"] == rule
    assert decision["margin"] is False


def test_the_margin_penalty_applies_within_a_fifth_of_a_threshold():
    # 36 km is within 20 % of the 40 km local span threshold.
    decision = classify.classify_route(3, {"AA"}, 36.0, 1.0)
    assert decision["tier"] == "local"
    assert decision["margin"] is True
    assert decision["tier_confidence"] == pytest.approx(0.85 * 0.7)
    # A rail route right past 150 km is penalised on the other side too.
    decision = classify.classify_route(2, {"AA"}, 160.0, None)
    assert decision["tier"] == "national"
    assert decision["margin"] is True


def test_the_span_is_exact_or_missing(monkeypatch):
    import math

    # Exact across the antimeridian and for a lopsided cloud; past the stop
    # cap it is a missing signal, never an approximation.
    dateline = [(179.9, 0.0), (-179.9, 0.0), (179.95, 0.1), (-179.95, 0.1)]
    assert classify._span_km(dateline) == pytest.approx(
        classify.haversine_km((179.9, 0.0), (-179.9, 0.0))
    )
    cloud = [(10.0, 1.0), (11.0, 1.0)] + [(10.5 + i / 400, 1.05) for i in range(40)]
    assert classify._span_km(cloud) == pytest.approx(
        classify.haversine_km((10.0, 1.0), (11.0, 1.0))
    )
    monkeypatch.setattr(classify, "SPAN_MAX_STOPS", 16)
    ring = [
        (10.0 + math.cos(a) * 0.5, 1.0 + math.sin(a) * 0.5)
        for a in [i * 2 * math.pi / 40 for i in range(40)]
    ]
    assert classify._span_km(ring) is None
    assert classify._route_geography(
        {f"s{i}" for i in range(40)}, [], {f"s{i}": ring[i] for i in range(40)}
    ) == (None, None, 40)


def test_a_historical_skip_is_rejudged_against_the_current_lookup(tmp_path):
    # The crawl skipped stop_times when its memo showed one city; the
    # current lookup shows two, so the whole-feed claim no longer holds.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-k", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-k",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\nk1,1.0,25.0\n",
            "routes.txt": b"route_id,route_type\nm1,1\n",
        },
        "skipped",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-k")])
    # An unrelated request from an earlier build must survive the append.
    requests = cache / "recrawl_requests.jsonl"
    requests.write_text(json.dumps({"feed_id": "f-else"}) + "\n")
    manifest = classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    assert [(e["tier"], e["evidence"]["unknown_reason"]) for e in edges] == [
        ("unknown", "skip_stale")
    ]
    assert edges[0]["selector_state"] == "unavailable"
    assert manifest["feeds_by_status"] == {"skip_stale": 1}
    # The back-edge: the feed is asked for a complete read next crawl, once.
    assert manifest["recrawl_requested"] == 1
    assert [
        json.loads(line)["feed_id"] for line in requests.read_text().splitlines()
    ] == [
        "f-else",
        "f-k",
    ]
    assert classify.classify(cache, lookup=LOOKUP)["recrawl_requested"] == 0
    assert len(requests.read_text().splitlines()) == 2
    # The request file is appended through a no-follow descriptor: a
    # symlink planted in its place is refused, never written through.
    aside = tmp_path / "aside.jsonl"
    aside.write_text("")
    requests.unlink()
    try:
        requests.symlink_to(aside)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(classify.ClassifyError, match="recrawl_requests.jsonl"):
        classify.classify(cache, lookup=LOOKUP)
    assert aside.read_text() == ""


def test_expanded_places_must_descend_from_the_current_seed(tmp_path):
    cache = tmp_path / "cache"
    seed = _publish(
        cache,
        "gazetteer",
        "seed.json",
        {"feed_places.jsonl": []},
        {"source": "seed", "sources": SOURCES, "overture_release": RELEASE},
    )
    _publish(
        cache,
        "gazetteer",
        "expanded.json",
        {"places_expanded.jsonl": PLACES},
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
            "seed_generation": seed["generation"],
        },
    )
    assert publish._read_places(cache)[2] is not None
    _publish(
        cache,
        "gazetteer",
        "seed.json",
        {"feed_places.jsonl": []},
        {"source": "seed", "sources": SOURCES, "overture_release": RELEASE},
    )
    with pytest.raises(publish.PublishError, match="seed.json"):
        publish._read_places(cache)


def test_a_vanished_names_generation_is_refused_by_publish(tmp_path):
    cache = tmp_path / "cache"
    names = _publish(
        cache,
        "gazetteer",
        "names.json",
        {"places_seed.jsonl": PLACES},
        {
            "source": "names",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
        },
    )
    _publish(
        cache,
        "gazetteer",
        "expanded.json",
        {"places_expanded.jsonl": PLACES},
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
            "names_generation": names["generation"],
        },
    )
    assert publish._read_places(cache)[2] is not None
    (cache / "gazetteer" / "names.json").unlink()
    with pytest.raises(publish.PublishError, match="no longer exists"):
        publish._read_places(cache)


def test_expanded_places_must_descend_from_the_current_names(tmp_path):
    cache, _, _ = _build(tmp_path)
    _publish(
        cache,
        "gazetteer",
        "names.json",
        {"places_seed.jsonl": PLACES},
        {
            "source": "names",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
        },
    )
    with pytest.raises(publish.PublishError, match="re-run the expand"):
        publish._read_places(cache)


def test_shorter_patterns_survive_many_longer_duplicates():
    import io

    # Sixty-four identical long trips and one short branch: the branch is
    # a distinct length, so it keeps its slot.
    rows = b"trip_id,stop_id,stop_sequence\n"
    for i in range(64):
        rows += f"t{i:02d},a,1\nt{i:02d},b,2\nt{i:02d},c,3\n".encode()
    rows += b"branch,a,1\nbranch,d,2\n"
    trips = {f"t{i:02d}": "r" for i in range(64)}
    trips["branch"] = "r"
    _, sequences, _, _ = classify._read_stop_times(io.BytesIO(rows), trips, {})
    assert sequences["r"] == [("a", "b", "c"), ("a", "d")]


def test_a_blank_stop_id_row_leaves_no_legs():
    import io

    rows = b"trip_id,stop_id,stop_sequence\nt,a,1\nt,,2\nt,c,3\n"
    stops, sequences, _, _ = classify._read_stop_times(io.BytesIO(rows), {"t": "r"}, {})
    assert stops["r"] == {"a", "c"}
    assert sequences == {}


def test_pattern_sampling_keeps_distinct_patterns():
    import io

    # Nine trips of one three-stop pattern and one two-stop branch: the
    # branch must survive even though duplicates outnumber the sample.
    rows = b"trip_id,stop_id,stop_sequence\n"
    for i in range(9):
        rows += f"t{i},a,1\nt{i},b,2\nt{i},c,3\n".encode()
    rows += b"branch,a,1\nbranch,d,2\n"
    trips = {f"t{i}": "r" for i in range(9)}
    trips["branch"] = "r"
    stops, sequences, _, _ = classify._read_stop_times(io.BytesIO(rows), trips, {})
    assert stops["r"] == {"a", "b", "c", "d"}
    assert sequences["r"] == [("a", "b", "c"), ("a", "d")]


def test_a_classify_generation_without_coverage_is_corruption(tmp_path):
    cache, _, _ = _build(tmp_path)
    (cache / "coverage" / "coverage.json").unlink()
    with pytest.raises(classify.ClassifyError, match="without its coverage"):
        classify.read_edges(cache)


def test_the_cli_refuses_to_lose_the_golden_gate(tmp_path):
    import build_index

    arguments = build_index.parse_args(
        ["--stage", "publish", "--golden", str(tmp_path / "missing.jsonl")]
    )
    with pytest.raises(SystemExit, match="no-golden"):
        build_index.run_publish(arguments)
    assert (
        build_index.parse_args(["--stage", "publish"]).golden
        == build_index.DEFAULT_GOLDEN
    )


def test_ids_are_joined_verbatim():
    import io

    routes, route_types = classify._read_routes(
        io.BytesIO(b"route_id,agency_id,route_type\na ,x,3\na,,0\n,,7\n")
    )
    assert routes == {
        "a ": {"route_type": 3, "agency_id": "x"},
        "a": {"route_type": 0, "agency_id": ""},
    }
    # The id-less row still counts for the skip predicate's tier check.
    assert route_types == [3, 0, 7]


@pytest.mark.parametrize(
    ("inside", "total", "serves"),
    [(2, 50, True), (1, 50, True), (0, 3, False), (1, 0, False)],
)
def test_when_a_route_serves_a_place(inside, total, serves):
    assert classify.route_serves(inside, total) is serves


# ---- the stage over a fixture cache ----

SOURCES = {"atlas": {"commit": "abc"}}
RELEASE = "2026-08-19.0"


def _place(place_id, kind, *, parent_id=None, metro_ids=(), member_ids=()):
    return {
        "place_id": place_id,
        "kind": kind,
        "name": place_id,
        "parent_id": parent_id,
        "metro_ids": list(metro_ids),
        "member_ids": list(member_ids),
    }


PLACES = [
    _place("Q-c", "country"),
    _place("Q-reg", "region", parent_id="Q-c"),
    _place("Q-city", "city", parent_id="Q-reg", metro_ids=["Q-metro"]),
    _place("Q-other", "city", parent_id="Q-reg"),
    _place("Q-metro", "metro", member_ids=["Q-city"]),
]


def _records(city):
    # Real boundary records always carry the division id; the crawl's skip
    # predicate tells cities apart by it.
    return [
        {
            "kind": "city",
            "wikidata": city,
            "country": "AA",
            "overture_id": f"ov-{city}",
        },
        {"kind": "region", "wikidata": "Q-reg", "country": "AA"},
        {"kind": "country", "wikidata": "Q-c", "country": "AA"},
    ]


class StubLookup:
    def __init__(self, by_x):
        self.by_x = by_x

    def ensure(self, boxes):
        return 0

    def divisions_at(self, x, y):
        return self.by_x.get(x, [])


LOOKUP = StubLookup(
    {
        10.0: _records("Q-city"),
        10.01: _records("Q-city"),
        10.02: _records("Q-city"),
        10.3: _records("Q-city"),
        20.0: _records("Q-other"),
        # A point only the metro's own polygon places.
        40.0: [{"kind": "metro", "wikidata": "Q-metro", "country": "AA"}],
        # A point inside two overlapping member cities.
        25.0: [
            {
                "kind": "city",
                "wikidata": "Q-other",
                "country": "AA",
                "overture_id": "ov-Q-other",
            }
        ]
        + _records("Q-city"),
    }
)


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


def _write_crawl(cache, feed_id, members, stop_times_state):
    feed_dir = cache / "crawl" / crawl._dir_name(feed_id)
    feed_dir.mkdir(parents=True, exist_ok=True)
    digests = {}
    for name, data in members.items():
        (feed_dir / name).write_bytes(data)
        digests[name] = hashlib.sha256(data).hexdigest()
    (feed_dir / "state.json").write_text(
        json.dumps(
            {
                "feed_id": feed_id,
                "members": sorted(members),
                "member_sha256": digests,
                "members_requested": sorted(crawl.MEMBERS),
                "stop_times": {"state": stop_times_state, "reason": None},
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


def _candidate(place_id, feed_id, stops=3):
    return coverage._edge(
        place_id,
        feed_id,
        {"stops_in_place": stops, "stop_share": 1.0},
        "crawl",
        {"stops": stops, "routes": None, "departures_per_day": None},
    )


STOPS_A = (
    b"stop_id,stop_lat,stop_lon\n"
    b"s1,1.0,10.0\ns2,1.0,10.01\ns3,1.0,10.02\nb2,1.0,10.3\nb3,1.0,20.0\n"
)
ROUTES_A = b"route_id,route_type\ntram,0\nbus,3\n"
TRIPS_A = b"trip_id,route_id\nt1,tram\nt2,bus\nt0,bus\n"
STOP_TIMES_A = (
    b"trip_id,stop_id,stop_sequence\n"
    b"t0,s1,1\nt0,b2,2\n"  # a shorter bus pattern listed first
    b"t1,s1,1\nt1,s2,2\nt1,s3,3\n"
    b"t2,b3,3\nt2,s1,1\nt2,b2,2\n"  # out of order on purpose
)


def _coverage(cache, feeds, candidates, *, places=PLACES):
    expanded = _publish(
        cache,
        "gazetteer",
        "expanded.json",
        {"places_expanded.jsonl": places},
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
        },
    )
    return _publish(
        cache,
        "coverage",
        "coverage.json",
        {"feeds_covered.jsonl": feeds, "edges_candidate.jsonl": candidates},
        {
            "source": "coverage",
            "sources": SOURCES,
            "overture_release": RELEASE,
            "expanded_generation": expanded["generation"],
            "crawl_digest": crawl.states_digest(cache),
        },
    )


def _build(tmp_path):
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-a", "spec": "gtfs", "coverage_source": "crawl", "aliases": []},
        {
            "feed_id": "f-skip",
            "spec": "gtfs",
            "coverage_source": "crawl",
            "aliases": [],
        },
        {
            "feed_id": "f-none",
            "spec": "gtfs",
            "coverage_source": "crawl",
            "aliases": [],
        },
        {
            "feed_id": "f-declared",
            "spec": "gtfs",
            "coverage_source": "declared",
            "aliases": [],
        },
    ]
    candidates = [
        _candidate("Q-city", "f-a"),
        _candidate("Q-reg", "f-a"),
        _candidate("Q-c", "f-a"),
        _candidate("Q-metro", "f-a"),
        _candidate("Q-other", "f-a"),
        _candidate("Q-other", "f-skip"),
        _candidate("Q-reg", "f-skip"),
        _candidate("Q-city", "f-none"),
        _candidate("Q-other", "f-declared"),
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
    _write_crawl(
        cache,
        "f-skip",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\nk1,1.0,20.0\nk2,1.0,20.0\n",
            "routes.txt": b"route_id,route_type\nm1,1\n",
        },
        "skipped",
    )
    _write_crawl(
        cache,
        "f-none",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\nn1,1.0,10.0\n",
            "routes.txt": b"route_id,route_type\nx,3\n",
        },
        "absent",
    )
    _coverage(cache, feeds, candidates)
    manifest = classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    grouped = {}
    for edge in edges:
        grouped.setdefault(edge["feed_id"], {})[(edge["place_id"], edge["tier"])] = edge
    return cache, manifest, grouped


def test_routes_are_measured_and_edges_split_by_tier(tmp_path):
    _, manifest, edges = _build(tmp_path)
    a = edges["f-a"]
    # The tram (rule 3) and the long bus (rule 8) each serve the city.
    local = a[("Q-city", "local")]
    assert local["tier_confidence"] == pytest.approx(0.90)
    assert local["needs_review"] is False
    assert local["evidence"]["matched_route_types"] == [0]
    assert local["evidence"]["serving_routes"] == 1
    assert local["evidence"]["route_min_stops"] == 1
    national = a[("Q-city", "national")]
    assert national["tier_confidence"] == pytest.approx(0.60)
    assert national["needs_review"] is True
    assert national["evidence"]["spread_km"] > 1000
    # Legs come from every sampled pattern, each sorted by sequence: the
    # short t0 (one 33 km leg) and the out-of-order t2 (33 km and 1078 km).
    assert national["evidence"]["median_interstop_km"] == pytest.approx(33.4, rel=0.01)
    # The bus's single stop in Q-other is service; the tram never reaches it.
    assert set(k[1] for k in a if k[0] == "Q-other") == {"national"}
    # The geometry-less metro follows its member city.
    assert set(k[1] for k in a if k[0] == "Q-metro") == {"local", "national"}
    assert a[("Q-metro", "local")]["tier_confidence"] == pytest.approx(0.90)
    # The service level is per (place, feed): one route, one stop, and no
    # departures without a calendar; every tier edge of the pair shares it.
    assert a[("Q-other", "national")]["service"] == {
        "stops": 1,
        "routes": 1,
        "departures_per_day": None,
    }
    assert a[("Q-city", "local")]["service"] == a[("Q-city", "national")]["service"]
    # Each tier's selector is exactly its serving routes; the fingerprint
    # describes the feed, so every edge of it carries the same digest.
    assert local["selector_state"] == "complete"
    assert local["selector"] == {"route_id": ["tram"]}
    assert national["selector"] == {"route_id": ["bus"]}
    assert a[("Q-other", "national")]["selector"] == {"route_id": ["bus"]}
    assert {e["fingerprint_kind"] for e in a.values()} == {"route_stops"}
    assert len({e["classification_fingerprint"] for e in a.values()}) == 1
    # f-a's nine tier edges (five places, two tiers, Q-other national only)
    # are complete; f-skip's two are whole-feed; f-none and f-declared stay
    # unavailable.
    assert manifest["edges_by_selector_state"] == {
        "complete": 9,
        "whole_feed": 2,
        "unavailable": 2,
    }
    assert manifest["recrawl_requested"] == 0
    assert manifest["feeds_by_status"] == {
        "route_stops": 1,
        "whole_feed": 1,
        "no_route_evidence": 1,
        "declared": 1,
    }
    assert manifest["routes_classified"] == 3


def test_a_skipped_feed_is_whole_feed_at_its_fixed_tier(tmp_path):
    _, _, edges = _build(tmp_path)
    skip = edges["f-skip"]
    assert set(skip) == {("Q-other", "local"), ("Q-reg", "local")}
    for edge in skip.values():
        assert edge["tier_confidence"] == pytest.approx(0.90)
        assert edge["needs_review"] is False
        assert edge["evidence"]["spread_km"] is None
        # Whole feed: every stop and route, no timetable to count.
        assert edge["service"] == {"stops": 2, "routes": 1, "departures_per_day": None}
        assert edge["selector_state"] == "whole_feed" and edge["selector"] is None
        assert edge["fingerprint_kind"] == "feed_stops"
        assert len(edge["classification_fingerprint"]) == 64


def test_unknown_is_explicit_and_keeps_the_coverage_service(tmp_path):
    _, manifest, edges = _build(tmp_path)
    none = edges["f-none"][("Q-city", "unknown")]
    assert none["tier_confidence"] == 0.0
    assert none["needs_review"] is True
    assert none["service"] == {"stops": 3, "routes": None, "departures_per_day": None}
    # No route evidence: never a selector, never a fingerprint.
    assert none["selector_state"] == "unavailable"
    assert none["fingerprint_kind"] == "none"
    assert none["classification_fingerprint"] is None
    assert none["evidence"]["unknown_reason"] == "no_route_evidence"
    declared = edges["f-declared"][("Q-other", "unknown")]
    assert declared["evidence"]["unknown_reason"] == "declared"
    assert manifest["edges_by_tier"]["unknown"] == 2
    assert 0 < manifest["unknown_share"] < 1


def test_publish_reads_the_classified_edges(tmp_path):
    cache, _, _ = _build(tmp_path)
    feeds, edges, manifest, _ = publish._read_coverage(cache)
    assert manifest["source"] == "classify"
    assert {e["tier"] for e in edges} >= {"local", "national", "unknown"}
    assert len(feeds) == 4


def test_stale_classified_edges_are_refused_downstream(tmp_path):
    # Coverage (and its expanded places) republished after classify: the
    # classified edges no longer descend from them, so neither publish nor
    # the golden gate may use them.
    cache, _, _ = _build(tmp_path)
    _coverage(cache, [], [])
    with pytest.raises(publish.PublishError, match="re-run the classify"):
        publish._read_coverage(cache)
    with pytest.raises(classify.ClassifyError):
        classify.read_edges(cache)


def test_a_skipped_feed_without_parseable_routes_is_unknown(tmp_path):
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-s", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-s",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\nk1,1.0,20.0\n",
            "routes.txt": b"route_id,route_type\n,1\n",  # blank route id
        },
        "skipped",
    )
    _coverage(cache, feeds, [_candidate("Q-other", "f-s")])
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    assert [(e["place_id"], e["tier"]) for e in edges] == [("Q-other", "unknown")]
    assert edges[0]["evidence"]["unknown_reason"] == "no_routes"


def test_mixed_lineage_is_refused(tmp_path):
    # Expanded places republished after coverage read them.
    cache = tmp_path / "cache"
    _coverage(cache, [], [])
    _publish(
        cache,
        "gazetteer",
        "expanded.json",
        {"places_expanded.jsonl": PLACES},
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
        },
    )
    with pytest.raises(classify.ClassifyError, match="stage order"):
        classify.classify(cache, lookup=LOOKUP)


def test_a_crawl_that_changed_after_coverage_is_refused(tmp_path):
    # Coverage measured membership against no crawl at all; a crawl that
    # appears afterwards would give tiers from evidence membership never saw.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-m", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _coverage(cache, feeds, [_candidate("Q-other", "f-m")])
    _write_crawl(
        cache,
        "f-m",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\nk1,1.0,20.0\n",
            "routes.txt": b"route_id,route_type\nm1,1\n",
        },
        "skipped",
    )
    with pytest.raises(classify.ClassifyError, match="re-run the coverage"):
        classify.classify(cache, lookup=LOOKUP)


def test_tier_confidence_is_weighted_by_stop_share(tmp_path):
    # A tram entirely inside the city (share 1.0, 0.90) and a long extended
    # bus with two of a hundred stops inside (share 0.02, 0.95): the share
    # weighting keeps the mean near the tram, raw counts would not.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-w", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    far = b"".join(f"o{i},1.0,30.0\n".encode() for i in range(98))
    far_times = b"".join(f"b,o{i},{i + 3}\n".encode() for i in range(98))
    _write_crawl(
        cache,
        "f-w",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n"
            + far,
            "routes.txt": b"route_id,route_type\ntram,0\nbus,715\n",
            "trips.txt": b"trip_id,route_id\nt,tram\nb,bus\n",
            "stop_times.txt": (
                b"trip_id,stop_id,stop_sequence\nt,s1,1\nt,s2,2\nb,s1,1\nb,s2,2\n"
                + far_times
            ),
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-w")])
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    (edge,) = edges
    assert edge["tier"] == "local"
    assert edge["tier_confidence"] == pytest.approx((0.90 * 1.0 + 0.95 * 0.02) / 1.02)


def test_join_gaps_are_counted_not_silent(tmp_path):
    _, manifest, edges = _build(tmp_path)
    assert manifest["join_gaps"] == {"orphan_trips": 0, "dangling_stop_times": 0}
    cache = tmp_path / "gaps"
    feeds = [
        {"feed_id": "f-g", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-g",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n",
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id\nt,tram\nghost,nope\n",
            "stop_times.txt": (
                b"trip_id,stop_id,stop_sequence\nt,s1,1\nt,s2,2\nlost,s1,1\n"
            ),
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-g")])
    manifest = classify.classify(cache, lookup=LOOKUP)
    assert manifest["join_gaps"] == {"orphan_trips": 1, "dangling_stop_times": 1}
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    assert edges[0]["evidence"]["join_gaps"] == {
        "orphan_trips": 1,
        "dangling_stop_times": 1,
    }


def test_a_state_rewritten_after_coverage_makes_the_edges_stale(tmp_path):
    # Same retrieval time, different committed state: the digest covers the
    # whole state, and every downstream consumer refuses the edges.
    cache, _, _ = _build(tmp_path)
    state_path = cache / "crawl" / crawl._dir_name("f-a") / "state.json"
    state = json.loads(state_path.read_text())
    state["members"] = sorted(state["members"] + ["agency.txt"])
    state_path.write_text(json.dumps(state))
    with pytest.raises(classify.ClassifyError, match="re-run the coverage"):
        classify.read_edges(cache)


def test_a_trip_without_stop_sequences_gives_no_legs(tmp_path):
    # Row order is arbitrary in GTFS, so a trip with a blank stop_sequence
    # contributes its stops but no legs: the bus keeps its span but has no
    # median, and rule 6-8 cannot fire.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-q", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-q",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n",
            "routes.txt": b"route_id,route_type\nbus,3\n",
            "trips.txt": b"trip_id,route_id\nt,bus\n",
            "stop_times.txt": b"trip_id,stop_id,stop_sequence\nt,s1,\nt,s2,\n",
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-q")])
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    assert [(e["place_id"], e["tier"]) for e in edges] == [("Q-city", "unknown")]


def test_a_stop_in_two_members_counts_once_for_the_metro(tmp_path):
    # One stop inside two overlapping member cities is one stop inside the
    # metro — service, but a single stop of it.
    cache = tmp_path / "cache"
    places = PLACES[:-1] + [
        _place("Q-metro", "metro", member_ids=["Q-city", "Q-other"])
    ]
    feeds = [
        {"feed_id": "f-d", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-d",
        {
            "stops.txt": (
                b"stop_id,stop_lat,stop_lon\n"
                b"x,1.0,25.0\no1,1.0,30.0\no2,1.0,30.0\no3,1.0,30.0\no4,1.0,30.0\n"
            ),
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id\nt,tram\n",
            "stop_times.txt": (
                b"trip_id,stop_id,stop_sequence\n"
                b"t,x,1\nt,o1,2\nt,o2,3\nt,o3,4\nt,o4,5\n"
            ),
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-metro", "f-d")], places=places)
    manifest = classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    (edge,) = edges
    assert (edge["place_id"], edge["tier"]) == ("Q-metro", "local")
    assert edge["service"] == {"stops": 1, "routes": 1, "departures_per_day": None}
    assert manifest["edges_dropped_no_serving_route"] == 0


@pytest.mark.parametrize(
    "stop_times",
    [
        b"trip_id,stop_id,stop_sequence\nt,s1,1\nt,s2,2\nt,ghost,3\n",
        # The unlocated stop is traversal-only: outside the scheduled set,
        # but in the pattern, so the legs would be incomplete.
        b"trip_id,stop_id,stop_sequence,pickup_type,drop_off_type\n"
        b"t,s1,1,,\nt,s2,2,,\nt,ghost,3,1,1\n",
    ],
)
def test_a_route_with_an_unlocated_stop_has_no_geometry(tmp_path, stop_times):
    # One stop id without coordinates: the service denominator still counts
    # it, but span and median are missing signals, so the bus is unknown.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-u", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-u",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n",
            "routes.txt": b"route_id,route_type\nbus,3\n",
            "trips.txt": b"trip_id,route_id\nt,bus\n",
            "stop_times.txt": stop_times,
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-u")])
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    assert [(e["place_id"], e["tier"]) for e in edges] == [("Q-city", "unknown")]


def test_a_vanished_upstream_pointer_is_refused(tmp_path):
    cache, _, _ = _build(tmp_path)
    (cache / "gazetteer" / "expanded.json").unlink()
    with pytest.raises(classify.ClassifyError, match="re-run"):
        classify.read_edges(cache)


def test_a_corrupt_classify_generation_is_refused(tmp_path):
    cache, _, _ = _build(tmp_path)
    (cache / "classify" / "edges.json").write_text("not a pointer")
    with pytest.raises(classify.ClassifyError, match="unreadable"):
        classify.read_edges(cache)


def test_a_traversal_only_stop_is_no_service_and_the_metro_sums_members(tmp_path):
    # The tram's Q-other stop allows neither boarding nor alighting: the
    # feed is admitted there on stops.txt alone, no route serves it, so the
    # candidate is dropped and counted; the metro sums its served members.
    cache = tmp_path / "cache"
    places = PLACES[:-1] + [
        _place("Q-metro", "metro", member_ids=["Q-city", "Q-other"])
    ]
    feeds = [
        {"feed_id": "f-t", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-t",
        {
            "stops.txt": (
                b"stop_id,stop_lat,stop_lon\n"
                b"s1,1.0,10.0\nb3,1.0,20.0\no1,1.0,30.0\no2,1.0,30.0\no3,1.0,30.0\n"
            ),
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id\nt,tram\n",
            "stop_times.txt": (
                b"trip_id,stop_id,stop_sequence,pickup_type,drop_off_type\n"
                b"t,s1,1,0,0\nt,b3,2,1,1\nt,o1,3,,\nt,o2,4,,\nt,o3,5,,\n"
            ),
        },
        "complete",
    )
    _coverage(
        cache,
        feeds,
        [
            _candidate("Q-city", "f-t"),
            _candidate("Q-metro", "f-t"),
            _candidate("Q-other", "f-t"),
        ],
        places=places,
    )
    manifest = classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    by_place = {e["place_id"]: e for e in edges}
    assert [(e["place_id"], e["tier"]) for e in edges] == [
        ("Q-city", "local"),
        ("Q-metro", "local"),
    ]
    assert by_place["Q-metro"]["service"]["stops"] == 1
    assert manifest["edges_dropped_no_serving_route"] == 1


def test_calendar_days_are_counted_not_walked():
    import io

    calendar = io.BytesIO(
        b"service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        b"start_date,end_date\n"
        b"all,1,1,1,1,1,1,1,00010101,99991231\n"
        b"wk,1,1,1,1,1,0,0,20260901,20260914\n"
    )
    dates = io.BytesIO(
        b"service_id,date,exception_type\n"
        b"wk,20260907,2\n"  # a Monday: -1
        b"wk,20260905,2\n"  # a Saturday it never ran: no-op
        b"wk,20260901,1\n"  # a Tuesday it already runs: no-op
        b"wk,20260919,1\n"  # a date outside its window: +1
        b"wk,20260920,1\n"
        b"wk,20260920,2\n"  # added and removed: the removal wins
        b"lone,20260903,1\n"  # a service with exceptions only
    )
    active_days, span_days = classify._read_calendar(calendar, dates)
    assert span_days == 3652059
    assert active_days == {"all": 3652059, "wk": 10, "lone": 1}


def test_a_crawl_state_from_a_smaller_member_set_is_refused(tmp_path):
    # A cache the crawler never asked for calendar files cannot say whether
    # the feed has one: classifying it would publish null departures as if
    # measured, so it is refused until the crawl runs again.
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
    state_path = cache / "crawl" / crawl._dir_name("f-a") / "state.json"
    state = json.loads(state_path.read_text())
    del state["members_requested"]
    state_path.write_text(json.dumps(state))
    _coverage(cache, feeds, [_candidate("Q-city", "f-a")])
    with pytest.raises(classify.ClassifyError, match="re-run the crawl"):
        classify.classify(cache, lookup=LOOKUP)


def test_the_request_file_is_pinned_without_o_nofollow(tmp_path, monkeypatch):
    # The fallback for platforms without O_NOFOLLOW: a symlink is refused
    # by identity, a missing file is created, and an append deduplicates.
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    cache = tmp_path / "cache"
    cache.mkdir()
    assert classify._request_recrawl(cache, ["f-a", "f-a"]) == 1
    assert classify._request_recrawl(cache, ["f-a", "f-b"]) == 1
    lines = (cache / "recrawl_requests.jsonl").read_text().splitlines()
    assert [json.loads(line)["feed_id"] for line in lines] == ["f-a", "f-b"]
    aside = tmp_path / "aside.jsonl"
    aside.write_text("")
    (cache / "recrawl_requests.jsonl").unlink()
    try:
        (cache / "recrawl_requests.jsonl").symlink_to(aside)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(classify.ClassifyError, match="not a regular file"):
        classify._request_recrawl(cache, ["f-c"])
    assert aside.read_text() == ""


def test_edges_without_the_service_key_are_refused(tmp_path):
    # Artifacts written before the service level (they carried a membership
    # confidence instead) must not publish as "no service everywhere".
    cache = tmp_path / "cache"
    feeds = [
        {
            "feed_id": "f-old",
            "spec": "gtfs",
            "coverage_source": "declared",
            "aliases": [],
        }
    ]
    legacy = {k: v for k, v in _candidate("Q-other", "f-old").items() if k != "service"}
    _coverage(cache, feeds, [{**legacy, "confidence": 0.5}])
    with pytest.raises(classify.ClassifyError, match="predate the service level"):
        classify.classify(cache, lookup=LOOKUP)
    with pytest.raises(classify.ClassifyError, match="predate the service level"):
        classify.read_edges(cache)


def test_a_metro_polygon_places_stops_alongside_its_members(tmp_path):
    # An official metro has its own boundary: a stop only that polygon
    # places joins the member cities' stops in the metro's service.
    cache = tmp_path / "cache"
    places = PLACES[:-1] + [_place("Q-metro", "metro", member_ids=["Q-city"])]
    feeds = [
        {"feed_id": "f-m", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-m",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\nm1,1.0,40.0\n",
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id\nt,tram\n",
            "stop_times.txt": b"trip_id,stop_id,stop_sequence\nt,s1,1\nt,m1,2\n",
        },
        "complete",
    )
    _coverage(
        cache,
        feeds,
        [_candidate("Q-city", "f-m"), _candidate("Q-metro", "f-m")],
        places=places,
    )
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    stops = {e["place_id"]: e["service"]["stops"] for e in edges}
    assert stops == {"Q-city": 1, "Q-metro": 2}


def test_departures_per_day_are_weighted_by_the_calendar(tmp_path):
    # Two weeks: a weekday service minus one removed day (9 dates) and a
    # Sunday service plus one added date (3 dates); each trip visits both
    # stops once, so the place sees 2 * (9 + 3) / 14 stop-events a day.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-cal", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-cal",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n",
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id,service_id\nt1,tram,wk\nt2,tram,sun\n",
            "calendar.txt": (
                b"service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
                b"sunday,start_date,end_date\n"
                b"wk,1,1,1,1,1,0,0,20260901,20260914\n"
                b"sun,0,0,0,0,0,0,1,20260901,20260914\n"
            ),
            "calendar_dates.txt": (
                b"service_id,date,exception_type\nwk,20260907,2\nsun,20260901,1\n"
            ),
            "stop_times.txt": (
                b"trip_id,stop_id,stop_sequence\nt1,s1,1\nt1,s2,2\nt2,s1,1\nt2,s2,2\n"
            ),
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-cal", 2)])
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    (edge,) = edges
    assert edge["service"] == {
        "stops": 2,
        "routes": 1,
        "departures_per_day": pytest.approx(24 / 14),
    }


def test_publish_refuses_edges_from_another_places_generation(tmp_path):
    cache, _, _ = _build(tmp_path)
    _publish(
        cache,
        "gazetteer",
        "expanded.json",
        {"places_expanded.jsonl": PLACES},
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "sources": SOURCES,
            "overture_release": RELEASE,
        },
    )
    with pytest.raises(publish.PublishError, match="re-run"):
        publish.publish(cache)


def test_a_corrupt_expanded_generation_is_refused_by_publish(tmp_path):
    cache, _, _ = _build(tmp_path)
    (cache / "gazetteer" / "expanded.json").write_text("not a pointer")
    with pytest.raises(publish.PublishError, match="unreadable"):
        publish._read_places(cache)


def test_feeds_yaml_edited_after_the_resolve_stage_refuses_to_classify(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    cache = tmp_path / "cache"
    _coverage(cache, [], [])
    directory = write_overrides(
        tmp_path, feeds=[{"feed": "f", "mark_uncrawlable": True}]
    )
    with pytest.raises(overrides.OverrideError, match="re-run the coverage"):
        classify.classify(cache, lookup=LOOKUP, overrides_dir=directory)
