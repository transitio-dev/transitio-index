"""Tests of the merge rules: which run of each label is a source, and how the
sources' tables become one set with one row per id."""

import json

from builds_fixture import (
    BOX,
    PLACES,
    _edge7,
    _feed7,
    _notice_listed_but_gone,
    _place,
    _rewrite_snapshot,
    _rt,
    _run,
    write_build,
)
from transitio_index import builds, merge


def test_the_newest_run_of_each_label_is_a_source_or_is_skipped_with_a_reason(
    tmp_path,
):
    cache = tmp_path / "cache"
    archived = cache / "builds"
    day = "2026-09-{:02d}T00:00:00+00:00".format
    _run(archived, "fi", 1, built_at=day(13))
    fi = _run(archived, "fi", 2, built_at=day(14))
    de = _run(archived, "de", 3, built_at=day(15))
    se = _run(archived, "se", 6, built_at=day(15))  # a tie: the lower id wins
    _run(archived, "se", 7, built_at=day(15))
    nl = _run(archived, "nl", 4, feeds=[], edges={})
    gone = _run(archived, "gone", 5)
    _notice_listed_but_gone(archived / gone / "index")
    six = _run(archived, "six", 8)  # partitioned, yet claiming schema 6
    _rewrite_snapshot(archived / six / "index", lambda s: s.update(schema_version=6))
    _run(archived, "bad", 9, built_at=day(13))  # a run whose snapshot cannot be
    bad = _run(archived, "bad", 10)  # read ranks first, so the label is skipped
    (archived / bad / "index" / "snapshot.json").write_text("{")
    _run(archived, "when", 11, built_at=day(13))  # so does one without a date
    when = _run(archived, "when", 12, built_at="yesterday")
    nodate = _run(archived, "nodate", 13)
    _rewrite_snapshot(archived / nodate / "index", lambda s: s.pop("built_at"))
    _run(archived, "raw", 14, built_at=day(13))  # a run without its index yet
    (archived / "raw-000000000000000f").mkdir()
    write_build(archived / "old" / "index")
    write_build(cache / "index")
    write_build(archived / builds.CATALOGUE / "index")  # a reserved id: not a build
    sources, skipped = merge.select_sources(cache)
    assert [(b, s["built_at"][8:10]) for b, _, s in sources] == [
        (de, "15"),
        (fi, "14"),
        (se, "15"),
    ]
    assert all(path == archived / b / "index" for b, path, _ in sources)
    assert skipped == [
        {"id": bad, "reason": "incomplete"},
        {"id": gone, "reason": "incomplete"},
        {"id": builds.LATEST, "reason": "not partitioned"},
        {"id": nl, "reason": "no feeds"},
        {"id": nodate, "reason": "undated"},
        {"id": "old", "reason": "not partitioned"},
        {"id": "raw-000000000000000f", "reason": "incomplete"},
        {"id": six, "reason": "not partitioned"},
        {"id": when, "reason": "undated"},
    ]


def _frame(tables, name, index):
    return tables[name].to_pandas().set_index(index)


def test_feeds_edges_and_places_merge_by_source(tmp_path):
    spill = json.dumps({"feeds": 2})  # the German build's own view of Finland
    a = _run(
        tmp_path,
        "fi",
        1,
        feeds=[
            _feed7("hsl", "HSL", "FI", "domestic"),
            _feed7("tram", "Tram", "FI", "d"),
            _feed7("nat", "Nat", "FI", "domestic"),
        ],
        edges={
            "FI": [
                _edge7("hel", "hsl", "local", "primary", 0.9, False),
                _edge7("hel", "tram", "local", "primary", 0.5, False),
                _edge7("esp", "tram", "local", "primary", 0.5, False),
                _edge7("uus", "nat", "regional", "secondary", 0.5, False),
            ]
        },
        realtime={
            "FI": [
                _rt("nat-rt", "nat", {"realtime_trip_updates": "https://a"}),
                _rt("nat-rt2", "nat", {"realtime_alerts": "https://a2"}),
                _rt("lost", None, {}, method="none"),
            ]
        },
        built_at="2026-09-14T00:00:00+00:00",
    )
    b = _run(
        tmp_path,
        "de",
        2,
        places=[
            _place("de", "country", "Germany", None, BOX(5, 47, 15, 55), country="DE"),
            _place("ber", "city", "Berlin", "de", BOX(13, 52, 14, 53), country="DE"),
            {**PLACES[0], "service": spill},
            {**PLACES[2], "service": spill},  # unserved in both runs
            {**PLACES[3], "service": spill},
            {**PLACES[4], "service": spill},  # one edge in both runs
        ],
        feeds=[
            _feed7("flix", "Flix", "DE", "domestic"),
            _feed7("nat", "Nat 2", "FI", "d"),
        ],
        edges={
            "DE": [_edge7("ber", "flix", "local", "primary", 0.9, False)],
            "FI": [_edge7("fi", "nat", "national", "tertiary", 0.4, False)],
            "links": [
                _edge7(
                    "hel", "flix", "international", "international", 0.3, True, "DE"
                ),
                _edge7(
                    "esp", "flix", "international", "international", 0.3, True, "DE"
                ),
            ],
        },
        realtime={"FI": [_rt("nat-rt", "nat", {"realtime_trip_updates": "https://b"})]},
        built_at="2026-09-15T00:00:00+00:00",
    )
    sources = []
    for run in (a, b):
        snapshot, _, tables = builds.load_tables(tmp_path / run / "index")
        sources.append((run, snapshot, tables))
    snapshot, tables = merge.merge_tables(
        sources, skipped=[{"id": "nl", "reason": "no feeds"}]
    )
    # A feed from the newest run carrying it, its edges from that run only.
    feeds = _frame(tables, "feeds.parquet", "feed_id")
    assert feeds["build_id"].to_dict() == {"hsl": a, "tram": a, "flix": b, "nat": b}
    assert feeds.loc["nat", "name"] == "Nat 2"
    edges = tables["edges.parquet"].to_pandas()
    assert set(map(tuple, edges[["place_id", "feed_id", "build_id"]].to_numpy())) == {
        ("hel", "hsl", a),
        ("hel", "tram", a),
        ("esp", "tram", a),
        ("ber", "flix", b),
        ("fi", "nat", b),
        ("hel", "flix", b),
        ("esp", "flix", b),
    }
    # A place from the run serving it most (two edges beat one), the newest
    # run on a tie (esp: one each) or when nothing serves it (lap); the row's
    # service is that run's.
    places = _frame(tables, "places.parquet", "place_id")
    ids = ["hel", "fi", "uus", "ber", "esp", "lap"]
    assert places.loc[ids, "build_id"].to_list() == [a, b, a, b, b, b]
    assert json.loads(places.loc["hel", "service"]) == {"feeds": 1}
    assert json.loads(places.loc["lap", "service"]) == {"feeds": 2}
    assert (edges["place_id"] == "hel").sum() == 3
    # A companion of a won feed comes with that feed's run or not at all
    # (nat-rt2 is the older run's); an unlinked one from the newest run.
    realtime = _frame(tables, "realtime.parquet", "feed_id")
    assert realtime["build_id"].to_dict() == {"nat-rt": b, "lost": a}
    assert json.loads(realtime.loc["nat-rt", "urls"]) == {
        "realtime_trip_updates": "https://b"
    }
    assert snapshot["counts"] == {
        "places": 8,
        "places_by_kind": {"city": 4, "region": 2, "country": 2},
        "feeds": 4,
        "edges": 7,
        "edges_by_tier": {"local": 4, "international": 2, "national": 1},
        "realtime": 2,
    }
    assert snapshot["built_at"][8:10] == "15" and snapshot["schema_version"] == 8
    assert snapshot["licensed"] is True and snapshot["catalogue"] is True
    assert [(s["label"], s["partitions"]) for s in snapshot["sources"]] == [
        ("de", ["DE", "FI", "links"]),
        ("fi", ["FI"]),
    ]
    assert snapshot["skipped"] == [{"id": "nl", "reason": "no feeds"}]
