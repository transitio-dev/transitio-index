"""The rank stage: relevance category, score and cross-border flag per edge."""

import pytest

from transitio_index import rank

PLACES = {
    "c1": {"place_id": "c1", "kind": "city", "country_code": "FI"},
    "c2": {"place_id": "c2", "kind": "city", "country_code": "FI"},
    "r1": {"place_id": "r1", "kind": "region", "country_code": "FI"},
    "fi": {"place_id": "fi", "kind": "country", "country_code": "FI"},
    "ee": {"place_id": "ee", "kind": "city", "country_code": "EE"},
}
FEEDS = [
    {"feed_id": "bus", "home_country": "FI", "country_stops": {"FI": 100}},
    {"feed_id": "rail", "home_country": "FI", "country_stops": {"FI": 40, "EE": 2}},
    {"feed_id": "ferry", "home_country": None, "country_stops": {"FI": 5, "EE": 5}},
    {"feed_id": "declared", "home_country": None, "country_stops": {}},
    {"feed_id": "nobody", "home_country": None, "country_stops": {}},
]


def _edge(feed_id, place_id, tier, stops, departures, **more):
    return {
        "feed_id": feed_id,
        "place_id": place_id,
        "tier": tier,
        "service": {"stops": stops, "routes": 1, "departures_per_day": departures},
        "evidence": {"stops_in_place": stops},
        "needs_review": tier == "unknown",
        **more,
    }


def test_relevance_is_scored_per_pair_and_category_follows_the_tier():
    edges = [
        # c1: bus 80 dep/day over 50 stops, rail 20 dep/day over 10 stops; the
        # rail pair has two tiers sharing one service struct.
        _edge("bus", "c1", "local", 50, 80.0),
        _edge("rail", "c1", "regional", 10, 20.0),
        _edge("rail", "c1", "national", 10, 20.0),
        # c2 and the region: rail alone.
        _edge("rail", "c2", "national", 20, 10.0),
        _edge("rail", "r1", "national", 30, 30.0),
        # the country: bus serves 1 of 2 served cities, rail both.
        _edge("bus", "fi", "national", 50, 80.0),
        _edge("rail", "fi", "national", 40, 30.0),
        # the ferry has no home country; its Estonian city edge crosses.
        _edge("ferry", "ee", "international", 5, 4.0),
        _edge("ferry", "c1", "international", 5, 4.0),
        _edge("ferry", "r1", "international", 5, 4.0),
        # no crawled stops at all (declared and unknown scope): tier unknown.
        _edge("declared", "c2", "unknown", 3, None),
        _edge("nobody", "c2", "unknown", 2, None, needs_review=False),
    ]
    ranked, report = rank.rank_edges(edges, FEEDS, PLACES)
    by_key = {(e["feed_id"], e["place_id"], e["tier"]): e for e in ranked}
    bus_c1 = by_key[("bus", "c1", "local")]
    # 0.7 × 80/104 + 0.3 × 50/100 (departures basis: every pair of c1 has one).
    assert bus_c1["relevance_category"] == "primary"
    assert bus_c1["relevance"] == pytest.approx(0.7 * 80 / 104 + 0.3 * 0.5)
    assert bus_c1["evidence"]["share_basis"] == "departures"
    assert bus_c1["cross_border"] is False
    # Both rail tiers at c1 carry the pair's one score; nothing is summed.
    rail_c1 = by_key[("rail", "c1", "regional")]
    assert rail_c1["relevance"] == pytest.approx(0.7 * 20 / 104 + 0.3 * 10 / 40)
    assert rail_c1["relevance"] == by_key[("rail", "c1", "national")]["relevance"]
    assert rail_c1["relevance_category"] == "secondary"
    # The country view scores breadth: rail serves both served cities.
    rail_fi = by_key[("rail", "fi", "national")]
    assert rail_fi["relevance_category"] == "tertiary"
    assert rail_fi["evidence"]["breadth"] == 1.0
    assert rail_fi["relevance"] == pytest.approx(0.7 * 30 / 110 + 0.3 * 1.0)
    assert by_key[("bus", "fi", "national")]["evidence"]["breadth"] == 0.5
    # c2 mixes calendar-less pairs in: the whole place falls back to stops.
    rail_c2 = by_key[("rail", "c2", "national")]
    assert rail_c2["evidence"]["share_basis"] == "stops"
    assert rail_c2["relevance"] == pytest.approx(0.7 * 20 / 25 + 0.3 * 20 / 40)
    # An international feed's city edges score in [0, 1] over all its stops
    # and cross the border everywhere.
    ferry_ee = by_key[("ferry", "ee", "international")]
    assert ferry_ee["relevance_category"] == "international"
    assert ferry_ee["cross_border"] is True
    assert ferry_ee["relevance"] == pytest.approx(0.7 * 1.0 + 0.3 * 5 / 10)
    assert by_key[("ferry", "c1", "international")]["cross_border"] is True
    ferry_r1 = by_key[("ferry", "r1", "international")]
    assert ferry_r1["cross_border"] is True
    assert ferry_r1["relevance"] == pytest.approx(0.7 * 4 / 34 + 0.3 * 5 / 10)
    # No crawled stops (declared or unknown scope): unknown category,
    # relevance 0, needs_review forced, no stop evidence.
    for feed_id in ("declared", "nobody"):
        edge = by_key[(feed_id, "c2", "unknown")]
        assert edge["relevance_category"] == "unknown"
        assert edge["relevance"] == 0.0 and edge["needs_review"] is True
        assert edge["evidence"]["relevance_note"] == "no_country_stops"
        assert edge["evidence"]["share_of_feed"] == 0.0
    assert report["edges_by_category"] == {
        "primary": 1,
        "secondary": 1,
        "tertiary": 5,
        "international": 3,
        "unknown": 2,
    }
    assert report["cross_border_edges"] == 5
    assert report["share_basis_by_place"] == {"departures": 4, "stops": 1}
    # Two-value kinds keep their quartiles inside [min, max].
    region = report["relevance_by_kind"]["region"]
    assert region["edges"] == 2 and region["min"] <= region["p25"] <= region["max"]
    assert set(report["relevance_by_kind"]) == {"city", "region", "country"}


def test_inconsistent_inputs_are_refused():
    edges = [
        _edge("bus", "c1", "local", 5, 1.0),
        _edge("bus", "c1", "regional", 6, 1.0),
    ]
    with pytest.raises(rank.RankError, match="different service structs"):
        rank.rank_edges(edges, FEEDS, PLACES)
    for edge, message in (
        (_edge("bus", "zz", "local", 5, 1.0), "unknown place"),
        (_edge("ghost", "c1", "local", 5, 1.0), "unknown feed"),
        (_edge("bus", "c1", "express", 5, 1.0), "unknown tier"),
    ):
        with pytest.raises(rank.RankError, match=message):
            rank.rank_edges([edge], FEEDS, PLACES)
    with pytest.raises(rank.RankError, match="duplicate feed"):
        rank.rank_edges([], FEEDS + [FEEDS[0]], PLACES)
