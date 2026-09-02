"""The curate stage: edge overrides applied to classified edges."""

import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import (  # noqa: E402
    classify,
    curate,
    golden,
    overrides,
    publish,
    store,
)
from test_index_classify import (  # noqa: E402
    LOOKUP,
    _build,
    _candidate,
    _coverage,
    _write_crawl,
)


def _overrides(tmp_path, entries):
    directory = tmp_path / "overrides"
    directory.mkdir(parents=True, exist_ok=True)
    import yaml

    (directory / "edges.yaml").write_text(yaml.safe_dump(entries, sort_keys=False))
    return directory


def _curate(tmp_path, entries, **kw):
    cache, _, _ = _build(tmp_path)
    manifest = curate.curate(cache, overrides_dir=_overrides(tmp_path, entries), **kw)
    edges, _ = store.read_jsonl(
        cache / "curate", "edges_final.json", "edges_final.jsonl"
    )
    grouped = {}
    for edge in edges:
        grouped.setdefault(edge["feed_id"], {})[(edge["place_id"], edge["tier"])] = edge
    return cache, manifest, grouped


def _report(cache):
    path = cache / "override_staleness_report.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


ENTRY = {"reason": "curated", "author": "HT", "date": "2026-09-02"}
STAMP = {**ENTRY, "evidence_hash": None, "stale": False}


def test_set_tiers_redefines_the_pair_and_stamps_it(tmp_path):
    # f-a in Q-city is local + national by the machine; the curator says
    # local + regional at 0.8: national goes, regional arrives unavailable
    # with the pair's service, local keeps its own selector and confidence
    # but takes the stamp — the whole pair is the curator's decision now.
    cache, manifest, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-a",
                "place": "Q-city",
                "set_tiers": ["local", "regional"],
                "tier_confidence": 0.8,
                **ENTRY,
            }
        ],
    )
    a = edges["f-a"]
    assert {t for p, t in a if p == "Q-city"} == {"local", "regional"}
    regional = a[("Q-city", "regional")]
    assert regional["method"] == "human" and regional["curation"] == STAMP
    assert regional["tier_confidence"] == 0.8 and regional["needs_review"] is False
    assert regional["selector_state"] == "unavailable" and regional["selector"] is None
    assert regional["service"] == a[("Q-city", "local")]["service"]
    assert regional["fingerprint_kind"] == "route_stops"
    assert regional["evidence"]["curator_reason"] == "curated"
    # The pair's coverage facts and every classified tier's own evidence.
    assert regional["evidence"]["stops_in_place"] == 3
    assert set(regional["evidence"]["classified"]) == {"local", "national"}
    assert regional["evidence"]["classified"]["national"]["spread_km"] > 1000
    local = a[("Q-city", "local")]
    assert local["method"] == "human" and local["curation"] == STAMP
    assert local["tier_confidence"] == pytest.approx(0.90)
    assert local["selector"] == {"route_id": ["tram"]}
    assert manifest["edges_added"] == 1 and manifest["edges_removed"] == 1
    assert manifest["source"] == "curate"
    # The other places of the feed are untouched.
    assert a[("Q-other", "national")]["method"] == "crawl"


def test_selectors_are_explicit_ids_or_expanded_predicates(tmp_path):
    _, _, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "local",
                "set_selector": {"route_id": ["bus", "tram"]},
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "national",
                "set_selector": {"route_type": [3]},
                **ENTRY,
            },
            # A skipped feed keeps its route evidence: a predicate resolves.
            {
                "feed": "f-skip",
                "place": "Q-other",
                "tier": "local",
                "set_selector": {"route_type": [1]},
                **ENTRY,
            },
        ],
    )
    local = edges["f-a"][("Q-city", "local")]
    assert local["selector"] == {"route_id": ["bus", "tram"]}
    assert local["tier_confidence"] == pytest.approx(0.90)  # unchanged
    assert local["method"] == "human"
    national = edges["f-a"][("Q-city", "national")]
    assert national["selector"] == {
        "route_id": ["bus"],
        "declared_as": {"route_type": [3]},
    }
    assert edges["f-skip"][("Q-other", "local")]["selector"] == {
        "route_id": ["m1"],
        "declared_as": {"route_type": [1]},
    }


def test_add_edge_needs_evidence_or_a_reason_and_remove_edge_deletes(tmp_path):
    cache, manifest, edges = _curate(
        tmp_path,
        [
            # A pair with coverage evidence: service and fingerprint follow;
            # a crawled feed says which of its routes the edge means.
            {
                "feed": "f-a",
                "place": "Q-other",
                "tier": "regional",
                "add_edge": {"tier_confidence": 0.6, "selector": {"route_id": ["bus"]}},
                **ENTRY,
            },
            # A pair nobody measured: null service, the reason carries it.
            {
                "feed": "f-declared",
                "place": "Q-city",
                "tier": "local",
                "add_edge": True,
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "Q-metro",
                "tier": "national",
                "remove_edge": True,
                **ENTRY,
            },
        ],
    )
    added = edges["f-a"][("Q-other", "regional")]
    assert added["service"] == edges["f-a"][("Q-other", "national")]["service"]
    assert added["tier_confidence"] == 0.6 and added["needs_review"] is True
    assert added["fingerprint_kind"] == "route_stops"
    assert added["selector"] == {"route_id": ["bus"]}
    declared = edges["f-declared"][("Q-city", "local")]
    assert declared["service"] is None and declared["fingerprint_kind"] == "none"
    assert declared["selector_state"] == "unavailable"
    assert ("Q-metro", "national") not in edges["f-a"]
    assert manifest["edges_added"] == 2 and manifest["edges_removed"] == 1


def test_an_unmeasured_pair_needs_a_reason_on_every_addition(tmp_path):
    # The first reasoned edge on an unmeasured pair is not evidence for the
    # next one; and an unknown tier is added at confidence 0.0.
    entries = [
        {
            "feed": "f-declared",
            "place": "Q-city",
            "tier": "local",
            "add_edge": True,
            **ENTRY,
        },
        {"feed": "f-declared", "place": "Q-city", "tier": "unknown", "add_edge": True},
    ]
    with pytest.raises(curate.CurateError, match="needs a reason"):
        _curate(tmp_path, entries)
    _, _, edges = _curate(tmp_path / "ok", [entries[0], {**entries[1], **ENTRY}])
    unknown = edges["f-declared"][("Q-city", "unknown")]
    assert unknown["tier_confidence"] == 0.0 and unknown["needs_review"] is True


def test_a_trusted_selector_brings_its_fingerprint(tmp_path):
    # A skipped feed whose whole-feed claim went stale is classified
    # unknown with no fingerprint, yet its crawl artifact still holds route
    # evidence: a curator's selector on it must ship with that evidence's
    # fingerprint, or fetch-time validation has nothing to check.
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
    classify.classify(cache, lookup=LOOKUP)
    entries = [
        {
            "feed": "f-k",
            "place": "Q-city",
            "tier": "unknown",
            "set_selector": {"route_id": ["m1"]},
            **ENTRY,
        }
    ]
    curate.curate(cache, overrides_dir=_overrides(tmp_path, entries))
    (edge,) = store.read_jsonl(
        cache / "curate", "edges_final.json", "edges_final.jsonl"
    )[0]
    assert edge["selector"] == {"route_id": ["m1"]}
    assert edge["fingerprint_kind"] == "feed_stops"
    assert len(edge["classification_fingerprint"]) == 64
    # The unknown edge carried no fingerprint, so the hash the curator
    # records covers the feed's route evidence directly: a hash over the
    # edge alone is stale, the report's value is fresh.
    (unknown,) = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")[0]
    bare = {
        **entries[0],
        "evidence_hash": curate.evidence_hash(
            [unknown], [("Q-city", "unknown")], "f-k"
        ),
    }
    manifest = curate.curate(cache, overrides_dir=_overrides(tmp_path / "bare", [bare]))
    assert manifest["stale_overrides"] == 1
    (row,) = _report(cache)
    fresh = {**entries[0], "evidence_hash": row["current_evidence_hash"]}
    manifest = curate.curate(
        cache, overrides_dir=_overrides(tmp_path / "fresh", [fresh])
    )
    assert manifest["stale_overrides"] == 0
    # A later phase judges against the machine's edge, not the selector's
    # work on it: a removal recorded against the pristine unknown edge is
    # fresh even though set_selector stamped a fingerprint first.
    removal = {
        "feed": "f-k",
        "place": "Q-city",
        "tier": "unknown",
        "remove_edge": True,
        "evidence_hash": curate.evidence_hash(
            [unknown], [("Q-city", "unknown")], "f-k"
        ),
    }
    manifest = curate.curate(
        cache, overrides_dir=_overrides(tmp_path / "pristine", [fresh, removal])
    )
    assert manifest["stale_overrides"] == 0 and manifest["edges_removed"] == 1


def test_a_pair_coverage_measured_but_no_route_served_is_still_measured(tmp_path):
    # Coverage admitted f-t to Q-other on a stop no route schedules, so the
    # classifier kept no edge there: an added edge still inherits the
    # coverage facts and service, and needs no reason.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-t", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-t",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\nb3,1.0,20.0\n",
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id\nt,tram\n",
            "stop_times.txt": (
                b"trip_id,stop_id,stop_sequence,pickup_type,drop_off_type\n"
                b"t,s1,1,0,0\nt,b3,2,1,1\n"
            ),
        },
        "complete",
    )
    _coverage(
        cache, feeds, [_candidate("Q-city", "f-t"), _candidate("Q-other", "f-t", 1)]
    )
    assert (
        classify.classify(cache, lookup=LOOKUP)["edges_dropped_no_serving_route"] == 1
    )
    entries = [
        {
            "feed": "f-t",
            "place": "Q-other",
            "tier": "local",
            "add_edge": {"selector": {"route_id": ["tram"]}},
        }
    ]
    curate.curate(cache, overrides_dir=_overrides(tmp_path, entries))
    edges, _ = store.read_jsonl(
        cache / "curate", "edges_final.json", "edges_final.jsonl"
    )
    (added,) = [e for e in edges if e["place_id"] == "Q-other"]
    assert added["service"] == {"stops": 1, "routes": None, "departures_per_day": None}
    assert (
        added["evidence"]["stops_in_place"] == 1 and "classified" in added["evidence"]
    )
    assert added["selector"] == {"route_id": ["tram"]}
    # A wildcard addition reaches that pair too: Q-city has the tram's
    # local edge already, Q-other has nothing, both lack regional.
    wildcard = [
        {
            "feed": "f-t",
            "place": "*",
            "tier": "regional",
            "add_edge": {"selector": {"route_id": ["tram"]}},
        }
    ]
    manifest = curate.curate(
        cache, overrides_dir=_overrides(tmp_path / "wild", wildcard)
    )
    assert manifest["edges_added"] == 2
    # The coverage candidate is what the addition consumed, so its evidence
    # is what the hash covers (with the feed's route evidence, since the
    # candidate carries no fingerprint): the report says what to record,
    # fresh against one stop, stale at two.
    entries[0]["evidence_hash"] = "0" * 64
    curate.curate(cache, overrides_dir=_overrides(tmp_path / "stale", entries))
    (row,) = _report(cache)
    entries[0]["evidence_hash"] = row["current_evidence_hash"]
    manifest = curate.curate(
        cache, overrides_dir=_overrides(tmp_path / "fresh", entries)
    )
    assert manifest["stale_overrides"] == 0
    _coverage(
        cache, feeds, [_candidate("Q-city", "f-t"), _candidate("Q-other", "f-t", 2)]
    )
    classify.classify(cache, lookup=LOOKUP)
    manifest = curate.curate(
        cache, overrides_dir=_overrides(tmp_path / "moved", entries)
    )
    assert manifest["stale_overrides"] == 1
    edges, _ = store.read_jsonl(
        cache / "curate", "edges_final.json", "edges_final.jsonl"
    )
    (added,) = [e for e in edges if e["place_id"] == "Q-other"]
    assert added["curation"]["stale"] is True and added["service"]["stops"] == 2
    # A bare addition to a crawled feed is refused: its routes are known.
    with pytest.raises(curate.CurateError, match="needs a selector"):
        curate.curate(
            cache,
            overrides_dir=_overrides(
                tmp_path / "bare",
                [
                    {
                        "feed": "f-t",
                        "place": "Q-other",
                        "tier": "local",
                        "add_edge": True,
                    }
                ],
            ),
        )


def test_a_single_agency_feed_matches_its_agency_with_blank_route_agencies(tmp_path):
    # GTFS lets routes.txt omit agency_id when agency.txt has one agency:
    # a predicate naming that agency selects those routes.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-one", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-one",
        {
            "agency.txt": b"agency_id,agency_name\nA1,One\n",
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\n",
            "routes.txt": b"route_id,route_type\ntram,0\n",
        },
        "skipped",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-one", 1)])
    classify.classify(cache, lookup=LOOKUP)
    entries = [
        {
            "feed": "f-one",
            "place": "Q-city",
            "tier": "local",
            "set_selector": {"agency_id": ["A1"]},
            **ENTRY,
        }
    ]
    curate.curate(cache, overrides_dir=_overrides(tmp_path, entries))
    (edge,) = store.read_jsonl(
        cache / "curate", "edges_final.json", "edges_final.jsonl"
    )[0]
    assert edge["selector"] == {
        "route_id": ["tram"],
        "declared_as": {"agency_id": ["A1"]},
    }


def test_a_predicate_with_both_clauses_selects_their_intersection(tmp_path):
    # The plan's AND-versus-OR fixture: one agency runs a tram and a bus, a
    # second agency a coach; agency A1 AND route type 3 is the bus alone,
    # and both clauses stay visible in declared_as.
    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-two", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    _write_crawl(
        cache,
        "f-two",
        {
            "agency.txt": b"agency_id,agency_name\nA1,One\nA2,Two\n",
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\n",
            "routes.txt": (
                b"route_id,agency_id,route_type\ntram,A1,0\nbus,A1,3\ncoach,A2,3\n"
            ),
        },
        "skipped",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-two", 1)])
    classify.classify(cache, lookup=LOOKUP)
    entries = [
        {
            "feed": "f-two",
            "place": "Q-city",
            "tier": "unknown",
            "set_selector": {"agency_id": ["A1"], "route_type": [3]},
            **ENTRY,
        }
    ]
    curate.curate(cache, overrides_dir=_overrides(tmp_path, entries))
    (edge,) = store.read_jsonl(
        cache / "curate", "edges_final.json", "edges_final.jsonl"
    )[0]
    assert edge["selector"] == {
        "route_id": ["bus"],
        "declared_as": {"agency_id": ["A1"], "route_type": [3]},
    }


def test_a_selector_on_a_tier_another_entry_creates_hashes_the_route_evidence(tmp_path):
    # No pre-override row exists for the new tier, so the selector's hash
    # must cover the route evidence it resolves against: a hash over the
    # empty target alone is stale, the report's value is fresh.
    creator = {
        "feed": "f-a",
        "place": "Q-city",
        "set_tiers": ["local", "regional"],
        **ENTRY,
    }
    selector = {
        "feed": "f-a",
        "place": "Q-city",
        "tier": "regional",
        "set_selector": {"route_id": ["tram"]},
        "evidence_hash": curate.evidence_hash([], [("Q-city", "regional")], "f-a"),
        **ENTRY,
    }
    cache, manifest, _ = _curate(tmp_path, [creator, selector])
    assert manifest["stale_overrides"] == 1
    (row,) = _report(cache)
    fresh = {**selector, "evidence_hash": row["current_evidence_hash"]}
    _, manifest, _ = _curate(tmp_path / "fresh", [creator, fresh])
    assert manifest["stale_overrides"] == 0


def test_a_whole_feed_claim_needs_route_evidence(tmp_path):
    # An uncrawled feed cannot be validated whole: the claim is unavailable.
    _, _, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-declared",
                "place": "Q-other",
                "tier": "unknown",
                "set_selector": "whole_feed",
                **ENTRY,
            }
        ],
    )
    assert (
        edges["f-declared"][("Q-other", "unknown")]["selector_state"] == "unavailable"
    )


def test_staleness_is_sticky_and_shadowed_entries_are_judged(tmp_path):
    # A stale set_tiers shapes the edge; a fresh set_selector afterwards
    # must not clear the flag. An entry a more specific one shadows entirely
    # is still judged: it targets nothing now, and says so in the report.
    stale = {
        "feed": "f-a",
        "place": "Q-city",
        "set_tiers": ["local"],
        "evidence_hash": "0" * 64,
        **ENTRY,
    }
    fresh = {
        "feed": "f-a",
        "place": "Q-city",
        "tier": "local",
        "set_selector": {"route_id": ["tram"]},
        **ENTRY,
    }
    shadowed = {
        "feed": "f-a",
        "place": "Q-other",
        "tier": "*",
        "set_selector": {"route_type": [3]},
        "evidence_hash": "2" * 64,
        **ENTRY,
    }
    specific = {
        "feed": "f-a",
        "place": "Q-other",
        "tier": "national",
        "set_selector": {"route_id": ["bus"]},
        **ENTRY,
    }
    cache, manifest, edges = _curate(tmp_path, [stale, fresh, shadowed, specific])
    local = edges["f-a"][("Q-city", "local")]
    assert local["selector"] == {"route_id": ["tram"]}
    assert local["curation"]["stale"] is True
    assert manifest["stale_overrides"] == 2
    assert [r["operation"] for r in _report(cache)] == ["set_tiers", "set_selector"]
    assert manifest["overrides_applied"] == 3


def test_operations_compose_in_a_fixed_order_not_file_order(tmp_path):
    # The selector for a tier another entry creates sits FIRST in the file
    # and still applies; the removal listed first still runs last.
    _, manifest, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "regional",
                "set_selector": {"route_id": ["tram"]},
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "local",
                "remove_edge": True,
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "Q-city",
                "set_tiers": ["local", "regional"],
                **ENTRY,
            },
        ],
    )
    a = edges["f-a"]
    assert {t for p, t in a if p == "Q-city"} == {"regional"}
    assert a[("Q-city", "regional")]["selector"] == {"route_id": ["tram"]}
    assert manifest["overrides_applied"] == 3


def test_the_whole_feed_invariant_holds_over_the_final_state(tmp_path):
    # Local becomes whole-feed while the conflicting national tier is
    # removed in the same batch: valid, whatever phase runs first.
    _, _, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "local",
                "set_selector": "whole_feed",
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "national",
                "remove_edge": True,
                **ENTRY,
            },
        ],
    )
    assert {t for p, t in edges["f-a"] if p == "Q-city"} == {"local"}
    assert edges["f-a"][("Q-city", "local")]["selector_state"] == "whole_feed"
    # An unavailable second tier contradicts the claim just as route ids do.
    with pytest.raises(curate.CurateError, match="also carries"):
        _curate(
            tmp_path / "second",
            [
                {"feed": "f-a", "place": "Q-city", "set_tiers": ["local", "regional"]},
                {
                    "feed": "f-a",
                    "place": "Q-city",
                    "tier": "local",
                    "set_selector": "whole_feed",
                    **ENTRY,
                },
            ],
        )


def test_wildcards_apply_everywhere_and_specific_entries_win(tmp_path):
    # The specific entry comes FIRST, so a wildcard applied afterwards must
    # still leave its target alone.
    _, manifest, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-a",
                "place": "Q-other",
                "tier": "national",
                "set_selector": {"route_id": ["bus"]},
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "*",
                "tier": "*",
                "set_selector": {"route_type": [0, 3]},
                **ENTRY,
            },
        ],
    )
    a = edges["f-a"]
    assert a[("Q-other", "national")]["selector"] == {"route_id": ["bus"]}
    assert a[("Q-reg", "local")]["selector"] == {
        "route_id": ["bus", "tram"],
        "declared_as": {"route_type": [0, 3]},
    }
    assert manifest["overrides_applied"] == 2


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"feed": "f-zzz", "place": "Q-city", "set_tiers": ["local"]}, "index lacks"),
        (
            {"feed": "f-a", "place": "Q-nowhere", "set_tiers": ["local"]},
            "gazetteer lacks",
        ),
        # A well-formed QID the gazetteer never published.
        (
            {"feed": "f-a", "place": "Q99", "tier": "local", "add_edge": True, **ENTRY},
            "gazetteer lacks",
        ),
        (
            {"feed": "f-a", "place": "Q-city", "tier": "local", "add_edge": True},
            "exists",
        ),
        (
            {"feed": "f-a", "place": "Q-city", "tier": "regional", "remove_edge": True},
            "no such edge",
        ),
        # A pair nobody measured needs a reason.
        (
            {
                "feed": "f-declared",
                "place": "Q-city",
                "tier": "local",
                "add_edge": True,
            },
            "needs a reason",
        ),
        (
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "local",
                "set_selector": {"route_id": ["ghost"]},
            },
            "feed lacks",
        ),
        (
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "local",
                "set_selector": {"route_type": [99]},
            },
            "matches no route",
        ),
        # whole_feed on one tier while routes are classified into another.
        (
            {
                "feed": "f-a",
                "place": "Q-city",
                "tier": "local",
                "set_selector": "whole_feed",
            },
            "also carries",
        ),
        # An exact place with a wildcard tier must hold edges of the feed.
        (
            {
                "feed": "f-skip",
                "place": "Q-city",
                "tier": "*",
                "set_selector": {"route_type": [1]},
            },
            "no edges for this pair",
        ),
        # An unknown tier has no confidence to give — added or retained.
        (
            {
                "feed": "f-a",
                "place": "Q-other",
                "tier": "unknown",
                "add_edge": {"tier_confidence": 0.5, "selector": {"route_id": ["bus"]}},
                **ENTRY,
            },
            "must be 0",
        ),
        (
            {
                "feed": "f-none",
                "place": "Q-city",
                "set_tiers": ["unknown"],
                "tier_confidence": 0.5,
                **ENTRY,
            },
            "must be 0",
        ),
    ],
)
def test_impossible_overrides_are_build_errors(tmp_path, entry, message):
    with pytest.raises(curate.CurateError, match=message):
        _curate(tmp_path, [entry])


def test_equal_specificity_is_a_conflict(tmp_path):
    with pytest.raises(curate.CurateError, match="same specificity"):
        _curate(
            tmp_path,
            [
                {"feed": "f-a", "place": "*", "tier": "local", "remove_edge": True},
                {"feed": "f-a", "place": "Q-city", "tier": "*", "remove_edge": True},
            ],
        )


def test_a_feed_without_route_evidence_gets_no_selector(tmp_path):
    _, _, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-none",
                "place": "Q-city",
                "tier": "unknown",
                "set_selector": {"route_type": [3]},
                **ENTRY,
            }
        ],
    )
    assert edges["f-none"][("Q-city", "unknown")]["selector_state"] == "unavailable"


def test_a_stale_override_is_applied_flagged_reported_and_fails_strict(tmp_path):
    # A set_tiers that adds nothing: the retained edge is what carries the
    # stale flag. A wildcard entry is one hash over everything it targets
    # and one report row.
    entry = {
        "feed": "f-a",
        "place": "Q-city",
        "set_tiers": ["local"],
        "evidence_hash": "0" * 64,
        **ENTRY,
    }
    wildcard = {
        "feed": "f-a",
        "place": "*",
        "tier": "*",
        "set_selector": {"route_type": [0, 3]},
        "evidence_hash": "1" * 64,
        **ENTRY,
    }
    cache, manifest, edges = _curate(tmp_path, [entry, wildcard])
    local = edges["f-a"][("Q-city", "local")]
    assert {t for p, t in edges["f-a"] if p == "Q-city"} == {"local"}
    assert local["curation"]["stale"] is True
    assert manifest["stale_overrides"] == 2
    rows = _report(cache)
    assert [(r["place"], r["tier"], r["operation"]) for r in rows] == [
        ("Q-city", None, "set_tiers"),
        ("*", "*", "set_selector"),
    ]
    # The report prints what to record: with it, the override is fresh.
    fresh = [
        {**entry, "evidence_hash": rows[0]["current_evidence_hash"]},
        {**wildcard, "evidence_hash": rows[1]["current_evidence_hash"]},
    ]
    fresh_cache, manifest, edges = _curate(tmp_path / "fresh", fresh)
    assert manifest["stale_overrides"] == 0 and _report(fresh_cache) == []
    assert edges["f-a"][("Q-city", "local")]["curation"]["stale"] is False
    with pytest.raises(curate.CurateError, match="stale override"):
        _curate(tmp_path / "strict", [entry], strict=True)
    assert _report(tmp_path / "strict" / "cache")


def test_the_evidence_hash_covers_targets_and_pair_evidence(tmp_path):
    # An addition consumes the pair's evidence, so its hash must move with
    # it; and a wildcard's hash names the targets it resolved to.
    edge = {"feed_id": "f-a", "place_id": "Q1", "tier": "local", "evidence": {"x": 1}}
    assert curate.evidence_hash([], [("Q1", "local")]) != curate.evidence_hash([])
    # The feed is part of the target: the same override on another feed is
    # another decision, never a fresh copy.
    assert curate.evidence_hash([], [("Q1", "local")], "f-a") != curate.evidence_hash(
        [], [("Q1", "local")], "f-b"
    )
    assert curate.evidence_hash([edge], [("Q1", "local")]) != curate.evidence_hash(
        [], [("Q1", "local")]
    )
    entry = {
        "feed": "f-a",
        "place": "Q-other",
        "tier": "regional",
        "add_edge": {"selector": {"route_id": ["bus"]}},
        "evidence_hash": curate.evidence_hash([], [("Q-other", "regional")]),
        **ENTRY,
    }
    cache, manifest, _ = _curate(tmp_path, [entry])
    # Recorded as if the pair had no evidence: the measured pair makes it stale.
    assert manifest["stale_overrides"] == 1
    (row,) = _report(cache)
    assert row["current_evidence_hash"] != entry["evidence_hash"]


def test_the_curated_edges_are_the_final_edge_stage(tmp_path):
    entry = {"feed": "f-a", "place": "Q-metro", "tier": "national", "remove_edge": True}
    cache, manifest, _ = _curate(tmp_path, [entry])
    overrides_dir = tmp_path / "overrides"
    _, edges, read_manifest = classify.read_edges(cache)
    assert read_manifest["source"] == "curate"
    assert ("Q-metro", "f-a", "national") not in {
        (e["place_id"], e["feed_id"], e["tier"]) for e in edges
    }
    _, read, read_manifest, digest = publish._read_coverage(
        cache, overrides_dir=overrides_dir
    )
    assert read_manifest["source"] == "curate" and len(read) == len(edges)
    assert (
        manifest["overrides_sha256"] == digest == overrides.edges_digest(overrides_dir)
    )
    # An edited override file, or one publish cannot see, means the
    # generation no longer reflects the curator's file: refused.
    (overrides_dir / "edges.yaml").write_text("[]\n")
    with pytest.raises(publish.PublishError, match="re-run the curate"):
        publish._read_coverage(cache, overrides_dir=overrides_dir)
    with pytest.raises(publish.PublishError, match="re-run the curate"):
        publish._read_coverage(cache, overrides_dir=None)
    # The standalone golden gate judges the same file.
    with pytest.raises(golden.GoldenError, match="re-run the curate"):
        golden._actual(cache, overrides_dir)
    # A reclassification leaves the curated edges behind; they are refused.
    classify.classify(cache, lookup=LOOKUP)
    with pytest.raises(classify.ClassifyError, match="re-run the curate"):
        classify.read_edges(cache)
    # A curate pointer whose classify generation vanished is corruption.
    (cache / "classify" / "edges.json").unlink()
    with pytest.raises(classify.ClassifyError, match="without its classify"):
        classify.read_edges(cache)


def test_unapplied_overrides_refuse_to_publish(tmp_path):
    # Overrides on disk with only a classify generation: curate never ran.
    cache, _, _ = _build(tmp_path)
    overrides_dir = _overrides(
        tmp_path, [{"feed": "f-a", "place": "Q-city", "set_tiers": ["local"]}]
    )
    with pytest.raises(publish.PublishError, match="run the curate stage"):
        publish._read_coverage(cache, overrides_dir=overrides_dir)
    # No overrides and no curate generation: nothing to apply, nothing owed.
    _, _, manifest, digest = publish._read_coverage(cache, overrides_dir=None)
    assert manifest["source"] == "classify" and digest is None


def test_a_wildcard_place_with_an_exact_tier_addresses_the_pairs_that_fit(tmp_path):
    # f-a is local in four places and national-only in Q-other: a wildcard
    # set_selector on local touches the four, add_edge fills the gaps, and
    # a wildcard remove_edge takes the four away.
    _, manifest, edges = _curate(
        tmp_path,
        [
            {
                "feed": "f-a",
                "place": "*",
                "tier": "local",
                "set_selector": {"route_id": ["tram"]},
                **ENTRY,
            },
            {
                "feed": "f-a",
                "place": "*",
                "tier": "regional",
                "add_edge": {"selector": {"route_id": ["tram"]}},
                **ENTRY,
            },
            {"feed": "f-a", "place": "*", "tier": "national", "remove_edge": True},
        ],
    )
    a = edges["f-a"]
    assert {p for p, t in a if t == "local"} == {"Q-city", "Q-reg", "Q-c", "Q-metro"}
    assert all(a[k]["selector"] == {"route_id": ["tram"]} for k in a if k[1] == "local")
    assert {p for p, t in a if t == "regional"} == {
        "Q-city",
        "Q-reg",
        "Q-c",
        "Q-metro",
        "Q-other",
    }
    assert not [k for k in a if k[1] == "national"]
    assert manifest["edges_added"] == 5 and manifest["edges_removed"] == 5


def test_the_cli_runs_a_stage_and_everything_downstream(monkeypatch):
    import build_index

    calls = []
    fake = {
        name: (lambda n: lambda a: calls.append(n) or [])(name)
        for name in build_index.STAGES
    }
    monkeypatch.setattr(build_index, "STAGES", fake)
    assert build_index.main(["--stage", "classify", "--downstream"]) == 0
    assert calls == ["classify", "curate", "publish"]
    calls.clear()
    assert build_index.main(["--stage", "curate"]) == 0
    assert calls == ["curate"]
    assert build_index.stages_from("ingest", True) == list(build_index.STAGES)


def test_a_curate_stage_without_classified_edges_is_refused(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    with pytest.raises(curate.CurateError, match="run classify"):
        curate.curate(cache, overrides_dir=None)


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"feed": "f", "place": "Q", "tier": "local"}, "exactly one operation"),
        (
            {"feed": "f", "place": "Q", "tier": "local", "set_tiers": ["local"]},
            "pair-scoped",
        ),
        ({"feed": "f", "place": "Q", "set_tiers": ["bogus"]}, "list of tiers"),
        (
            {"feed": "f", "place": "Q", "set_selector": {"route_id": ["r"]}},
            "needs a tier",
        ),
        (
            {
                "feed": "f",
                "place": "Q",
                "tier": "local",
                "set_selector": {"route_id": ["r"], "route_type": [3]},
            },
            "pick one",
        ),
        (
            {
                "feed": "f",
                "place": "Q",
                "tier": "local",
                "set_selector": {"route_type": ["3"]},
            },
            "list of int",
        ),
        ({"feed": "f", "place": "Q", "tier": "*", "add_edge": True}, "names one tier"),
        (
            {
                "feed": "f",
                "place": "Q",
                "tier": "local",
                "add_edge": {"tier_confidence": 2},
            },
            "lie in",
        ),
        (
            {
                "feed": "f",
                "place": "Q",
                "tier": "local",
                "add_edge": {"tier_confidence": 0},
                "tier_confidence": 0.5,
            },
            "declared twice",
        ),
        (
            {
                "feed": "f",
                "place": "Q",
                "tier": "local",
                "remove_edge": True,
                "tier_confidence": 1,
            },
            "takes no tier_confidence",
        ),
        (
            {"feed": "f", "place": "Q", "tier": "local", "remove_edge": "yes"},
            "must be true",
        ),
        (
            {
                "feed": "f",
                "place": "Q",
                "tier": "local",
                "remove_edge": True,
                "bogus": 1,
            },
            "unknown keys",
        ),
    ],
)
def test_malformed_edge_overrides_are_refused(tmp_path, entry, message):
    with pytest.raises(overrides.OverrideError, match=message):
        overrides.load_edge_overrides(_overrides(tmp_path, [entry]))


def test_a_symlinked_overrides_directory_is_refused(tmp_path):
    real = _overrides(
        tmp_path / "real", [{"feed": "f", "place": "Q", "set_tiers": ["local"]}]
    )
    link = tmp_path / "overrides"
    try:
        link.symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(overrides.OverrideError):
        overrides.load_edge_overrides(link)
    with pytest.raises(overrides.OverrideError):
        overrides.edges_digest(link)


def test_a_fifo_under_the_override_name_is_refused_not_awaited(tmp_path):
    directory = tmp_path / "overrides"
    directory.mkdir()
    try:
        os.mkfifo(directory / "edges.yaml")
    except (AttributeError, OSError):
        pytest.skip("named pipes unavailable")
    with pytest.raises(overrides.OverrideError, match="not a regular file"):
        overrides.load_edge_overrides(directory)


def test_duplicate_edge_overrides_are_refused(tmp_path):
    entry = {"feed": "f", "place": "Q", "tier": "local", "remove_edge": True}
    with pytest.raises(overrides.OverrideError, match="duplicate"):
        overrides.load_edge_overrides(_overrides(tmp_path, [entry, dict(entry)]))
    assert overrides.load_edge_overrides(None) == ([], None)
    assert overrides.edges_digest(None) is None


@pytest.mark.parametrize("nofollow", [True, False])
def test_an_override_file_is_read_once_and_never_through_a_symlink(
    tmp_path, monkeypatch, nofollow
):
    # With O_NOFOLLOW the refusal is atomic; without it the entry's identity
    # is pinned by lstat and fstat — either way a symlink never reads.
    if not nofollow:
        monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    directory = _overrides(
        tmp_path, [{"feed": "f", "place": "Q", "set_tiers": ["local"]}]
    )
    entries, digest = overrides.load_edge_overrides(directory)
    assert [e["operation"] for e in entries] == ["set_tiers"]
    assert digest == overrides.edges_digest(directory)
    aside = tmp_path / "aside.yaml"
    aside.write_text("[]\n")
    (directory / "edges.yaml").unlink()
    try:
        (directory / "edges.yaml").symlink_to(aside)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(overrides.OverrideError):
        overrides.load_edge_overrides(directory)
    with pytest.raises(overrides.OverrideError):
        overrides.edges_digest(directory)
