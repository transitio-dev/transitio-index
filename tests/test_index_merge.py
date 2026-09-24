"""Tests of the merge rules: which run of each label is a source, how the
sources' tables become one set with one row per id, and what a merge
refuses to load."""

import hashlib
import io
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
from transitio_index import builds, classify, merge

BUILT = "2026-09-{:02d}T00:00:00+00:00".format


def test_the_newest_run_of_each_label_is_a_source_or_is_skipped_with_a_reason(
    tmp_path,
):
    cache = tmp_path / "cache"
    archived = cache / "builds"
    day = BUILT
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
    write_build(cache / "index")  # the cache's own index is not an archived run
    write_build(archived / builds.CATALOGUE / "index")  # a reserved id: not a build
    sources, skipped = merge.select_sources(archived)
    assert [(b, s["built_at"][8:10]) for b, _, s in sources] == [
        (de, "15"),
        (fi, "14"),
        (se, "15"),
    ]
    assert all(path == archived / b / "index" for b, path, _ in sources)
    assert skipped == [
        {"id": bad, "reason": "incomplete"},
        {"id": gone, "reason": "incomplete"},
        {"id": nl, "reason": "no feeds"},
        {"id": nodate, "reason": "undated"},
        {"id": "old", "reason": "not partitioned"},
        {"id": "raw-000000000000000f", "reason": "incomplete"},
        {"id": six, "reason": "not partitioned"},
        {"id": when, "reason": "undated"},
    ]


def test_a_run_whose_index_is_a_link_is_skipped_unread_and_not_replaced(tmp_path):
    archived = tmp_path / "builds"
    real = _run(archived, "fi", 1, built_at=BUILT(14))
    _run(archived, "se", 2, built_at=BUILT(13))  # an older, complete run of se
    linked = archived / "se-0000000000000003"
    linked.mkdir()
    try:
        os.symlink(archived / real / "index", linked / "index")
    except OSError:
        pytest.skip("this platform cannot create symlinks")
    reads = []

    def reading(path):
        reads.append(path)
        return builds._read_file(path)

    sources, skipped = merge.select_sources(archived, reading)
    assert [build_id for build_id, _, _ in sources] == [real]
    assert skipped == [{"id": linked.name, "reason": "incomplete"}]
    assert not any(linked in path.parents for path in reads)


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


# ---- loading a selection for a merge: the reader's schema-9 fixture ----

# What a schema-9 build's manifest carries that a merge checks or copies.
MANIFEST_9 = {
    "overture_release": "2026-08-19.0",
    "simplify_tolerance_deg": 0.0005,
    "classifier": classify.classifier_settings(),
    "coverage_mode": "crawled",
    "sources": {"atlas": {"archive_sha256": "a" * 64}, "mdb": {"csv_sha256": "b" * 64}},
    "stale_place_overrides": 0,
    "stale_feed_overrides": 1,
    "stale_edge_overrides": 0,
    "overrides_sha256": None,
    "feeds_overrides_sha256": None,
    "places_overrides_sha256": None,
}


def _archive(fx, archived, label, digit, *, built_at, notice=b"NOTICE\n", **fields):
    """A schema-9 run archived as ``<label>-<16 hex>``: the reader fixture's
    partitioned index with the manifest fields a merge checks."""
    path = archived / f"{label}-{digit:016x}" / "index"
    fx.write_partitioned_index(
        path,
        feeds=fields.pop("feeds"),
        places=fields.pop("places"),
        edges=fields.pop("edges"),
        realtime=fields.pop("realtime", []),
        validity={},
        snapshot_id=f"{digit:016x}",
        notice=notice,
    )
    _rewrite_snapshot(
        path, lambda s: s.update({**MANIFEST_9, "built_at": built_at, **fields})
    )
    return path.parent.name


def _two_runs(fx, archived):
    """A Finnish run and a newer German one that also carries Helsinki."""
    fi = _archive(
        fx,
        archived,
        "fi",
        1,
        built_at=BUILT(14),
        feeds=[
            {**fx.covered_feed("hsl"), "home_country": "FI", "scope": "domestic"},
            {**fx.covered_feed("nat"), "home_country": "FI", "scope": "domestic"},
        ],
        places=[
            fx.place("fi", "country", country_code="FI"),
            fx.place("hel", "city", country_code="FI", parent_id="fi"),
        ],
        edges=[
            fx.edge("hel", "hsl", tier="local", relevance_category="primary"),
            fx.edge("fi", "nat", tier="national", relevance_category="secondary"),
        ],
        realtime=[fx.realtime_feed("hsl-rt", "hsl"), fx.realtime_feed("lost", None)],
    )
    de = _archive(
        fx,
        archived,
        "de",
        2,
        built_at=BUILT(15),
        feeds=[
            {**fx.covered_feed("flix"), "home_country": "DE", "scope": "domestic"},
            {
                **fx.covered_feed("ferry"),
                "home_country": None,
                "scope": "international",
            },
        ],
        places=[
            fx.place("ber", "city", country_code="DE"),
            fx.place("hel", "city", country_code="FI", parent_id="fi"),
        ],
        edges=[
            fx.edge("ber", "flix", tier="local", relevance_category="primary"),
            fx.edge("hel", "flix", tier="international", cross_border=True),
            fx.edge("hel", "ferry", tier="international", cross_border=True),
        ],
    )
    return fi, de


def test_a_selection_loads_verified_in_label_order_with_its_digests(tmp_path):
    fx = pytest.importorskip("index_fixture")
    fi, de = _two_runs(fx, tmp_path)
    sources, skipped = merge.select_sources(tmp_path)
    assert skipped == []
    loaded = merge.load_sources(sources)
    assert [(s["label"], s["build_id"]) for s in loaded] == [("de", de), ("fi", fi)]
    for source in loaded:
        index = tmp_path / source["build_id"] / "index"
        assert source["path"] == index
        assert source["snapshot"] == json.loads((index / "snapshot.json").read_text())
        for name, file in (
            ("snapshot_sha256", "snapshot.json"),
            ("notice_sha256", "NOTICE"),
        ):
            assert (
                source[name] == hashlib.sha256((index / file).read_bytes()).hexdigest()
            )
        assert source["notice"] == b"NOTICE\n"
        assert {"feeds.parquet", "places.parquet", "edges.parquet"} <= set(
            source["tables"]
        )
    # The companions ride with the run that has them.
    assert "realtime.parquet" in loaded[1]["tables"]
    assert "realtime.parquet" not in loaded[0]["tables"]
    assert loaded[1]["tables"]["feeds.parquet"]["feed_id"].to_pylist() == ["hsl", "nat"]


def _unlicensed(archived, fi, de):
    _rewrite_snapshot(archived / fi / "index", lambda s: s.update(licensed=False))


def _mixed_overture(archived, fi, de):
    _rewrite_snapshot(
        archived / de / "index", lambda s: s.update(overture_release="2026-09-01.0")
    )


def _below_schema_9(archived, fi, de):
    _rewrite_snapshot(archived / fi / "index", lambda s: s.update(schema_version=8))


def _without_a_release(archived, fi, de):
    _rewrite_snapshot(archived / fi / "index", lambda s: s.pop("overture_release"))


def _another_classifier(archived, fi, de):
    settings = {**classify.classifier_settings(), "margin": 0.25}
    _rewrite_snapshot(archived / de / "index", lambda s: s.update(classifier=settings))


def _a_malformed_classifier_everywhere(archived, fi, de):
    # Agreement cannot mask it: ``true`` is not a threshold in either run.
    settings = {**classify.classifier_settings(), "rules_version": True}
    for run in (fi, de):
        _rewrite_snapshot(
            archived / run / "index", lambda s: s.update(classifier=settings)
        )


def _a_negative_tolerance(archived, fi, de):
    _rewrite_snapshot(
        archived / de / "index", lambda s: s.update(simplify_tolerance_deg=-0.1)
    )


def _feeds_without_service_spans(archived, fi, de):
    # Schema-8-shaped feeds under a schema-9 manifest, digests intact.
    file = archived / fi / "index" / "FI" / "feeds.parquet"
    table = pq.read_table(file).drop_columns(["service_start", "service_end"])
    sink = io.BytesIO()
    pq.write_table(table, sink)
    file.write_bytes(sink.getvalue())
    digest = hashlib.sha256(sink.getvalue()).hexdigest()
    _rewrite_snapshot(
        archived / fi / "index",
        lambda s: s["partitions"]["FI"]["feeds"].update(sha256=digest),
    )


def _with_an_override_digest(archived, fi, de):
    _rewrite_snapshot(archived / de / "index", lambda s: s.update(overrides_sha256="x"))


def _tampered_table(archived, fi, de):
    file = archived / fi / "index" / "FI" / "edges.parquet"
    data = file.read_bytes()
    file.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))  # same size, other bytes


def _rewritten_notice(archived, fi, de):
    (archived / de / "index" / "NOTICE").write_bytes(b"another attribution\n")


@pytest.mark.parametrize(
    "tamper, message",
    [
        (_unlicensed, "not a licensed build"),
        (_mixed_overture, "overture_release differs"),
        (_below_schema_9, "schema_version 8"),
        (_without_a_release, "no usable overture_release"),
        (_another_classifier, "classifier differs"),
        (_a_malformed_classifier_everywhere, "no usable classifier"),
        (_a_negative_tolerance, "no usable simplify_tolerance_deg"),
        (_feeds_without_service_spans, "does not verify"),
        (_with_an_override_digest, "carries no overrides"),
        (_tampered_table, "does not verify"),
        (_rewritten_notice, "does not verify"),
    ],
)
def test_a_selection_a_merge_cannot_ship_is_refused(tmp_path, tamper, message):
    fx = pytest.importorskip("index_fixture")
    fi, de = _two_runs(fx, tmp_path)
    tamper(tmp_path, fi, de)
    sources, _ = merge.select_sources(tmp_path)
    with pytest.raises(merge.MergeError, match=message):
        merge.load_sources(sources)


def test_a_manifest_rewritten_after_selection_is_refused_even_when_equal_in_python(
    tmp_path,
):
    fx = pytest.importorskip("index_fixture")
    fi, _ = _two_runs(fx, tmp_path)
    sources, _ = merge.select_sources(tmp_path)
    # ``1 == True`` in Python; in JSON the manifest changed.
    _rewrite_snapshot(tmp_path / fi / "index", lambda s: s.update(licensed=1))
    with pytest.raises(merge.MergeError, match="changed since it was selected"):
        merge.load_sources(sources)


def test_the_recorded_manifest_digest_is_of_the_bytes_the_tables_were_checked_against(
    tmp_path,
):
    fx = pytest.importorskip("index_fixture")
    fi, _ = _two_runs(fx, tmp_path)
    reads = []

    def reading(path):
        data = builds._read_file(path)
        if path.name != "snapshot.json":
            return data
        reads.append(path)
        if reads.count(path) == 2:  # the load's one read: the disk moves on after it
            _rewrite_snapshot(path.parent, lambda s: s.update(built_at=BUILT(20)))
        return json.dumps(json.loads(data), indent=1).encode()  # same JSON, other bytes

    sources, _ = merge.select_sources(tmp_path, reading)
    loaded = merge.load_sources(sources, reading)
    served = json.dumps(loaded[1]["snapshot"], indent=1).encode()
    assert loaded[1]["build_id"] == fi
    assert loaded[1]["snapshot"]["built_at"] == BUILT(14)
    assert loaded[1]["snapshot_sha256"] == hashlib.sha256(served).hexdigest()
    disk = json.loads((tmp_path / fi / "index" / "snapshot.json").read_text())
    assert disk["built_at"] == BUILT(20)
    assert len(reads) == 4  # each manifest once for the selection, once for the load


def test_a_large_integer_tolerance_is_a_number_not_an_error(tmp_path):
    fx = pytest.importorskip("index_fixture")
    for run in _two_runs(fx, tmp_path):
        _rewrite_snapshot(
            tmp_path / run / "index", lambda s: s.update(simplify_tolerance_deg=10**400)
        )
    sources, _ = merge.select_sources(tmp_path)
    assert len(merge.load_sources(sources)) == 2


# ---- routing the merged tables into the partitions of one snapshot ----


def _merged(archived):
    sources, skipped = merge.select_sources(archived)
    assert skipped == []
    loaded = merge.load_sources(sources)
    _, tables = merge.merge_tables(
        [(s["build_id"], s["snapshot"], s["tables"]) for s in loaded]
    )
    return loaded, tables


def _pairs(rows):
    return list(zip(rows["place_id"].to_pylist(), rows["feed_id"].to_pylist()))


def test_every_merged_row_lands_in_its_partition_sorted_without_the_merge_columns(
    tmp_path,
):
    fx = pytest.importorskip("index_fixture")
    _two_runs(fx, tmp_path)
    _, tables = _merged(tmp_path)
    routed = merge._route(tables)
    assert list(routed) == [
        ("DE", "edges"),
        ("DE", "feeds"),
        ("DE", "places"),
        ("FI", "edges"),
        ("FI", "feeds"),
        ("FI", "places"),
        ("FI", "realtime"),
        ("international", "feeds"),
        ("international", "realtime"),
        ("links", "edges"),
    ]
    # Feeds under their home country, ``international`` without one.
    assert routed[("FI", "feeds")]["feed_id"].to_pylist() == ["hsl", "nat"]
    assert routed[("DE", "feeds")]["feed_id"].to_pylist() == ["flix"]
    assert routed[("international", "feeds")]["feed_id"].to_pylist() == ["ferry"]
    # Places under their country, whichever run they came from.
    assert routed[("FI", "places")]["place_id"].to_pylist() == ["fi", "hel"]
    assert routed[("DE", "places")]["place_id"].to_pylist() == ["ber"]
    # Domestic edges with their feed, the rest under ``links`` naming the
    # feed's partition; every table sorted by its ids.
    assert _pairs(routed[("FI", "edges")]) == [("fi", "nat"), ("hel", "hsl")]
    assert _pairs(routed[("DE", "edges")]) == [("ber", "flix")]
    links = routed[("links", "edges")]
    assert _pairs(links) == [("hel", "ferry"), ("hel", "flix")]
    assert links["feed_partition"].to_pylist() == ["international", "DE"]
    # A companion with its static feed; an unlinked one under ``international``.
    assert routed[("FI", "realtime")]["feed_id"].to_pylist() == ["hsl-rt"]
    assert routed[("international", "realtime")]["feed_id"].to_pylist() == ["lost"]
    for (partition, table), rows in routed.items():
        assert "build_id" not in rows.column_names
        assert "partition" not in rows.column_names
        if table == "edges":
            assert ("feed_partition" in rows.column_names) == (partition == "links")


def _tiny(country="FI", edge_feed="f", edge_place="p", home="FI"):
    return {
        "feeds.parquet": pa.table(
            {"feed_id": ["f"], "home_country": [home], "snapshot": ["x"]}
        ),
        "places.parquet": pa.table(
            {"place_id": ["p"], "country_code": [country], "snapshot": ["x"]}
        ),
        "edges.parquet": pa.table(
            {"place_id": [edge_place], "feed_id": [edge_feed], "snapshot": ["x"]}
        ),
    }


@pytest.mark.parametrize(
    "tables, message",
    [
        (_tiny(country=None), "has no country_code"),
        (_tiny(country=""), "has no country_code"),
        (_tiny(country="links"), "not a country partition"),
        (_tiny(home="../x"), "not a country partition"),
        (_tiny(edge_feed="g"), "edge of a feed the index lacks"),
        (_tiny(edge_place="q"), "edge to a place the index lacks"),
    ],
)
def test_routing_refuses_what_publish_refuses(tables, message):
    with pytest.raises(merge.MergeError, match=message):
        merge._route(tables)


@pytest.mark.parametrize("home", [None, ""])
def test_a_feed_without_a_home_country_is_international(home):
    routed = merge._route(_tiny(home=home))
    assert routed[("international", "feeds")]["feed_id"].to_pylist() == ["f"]
    assert routed[("links", "edges")]["feed_partition"].to_pylist() == ["international"]


def test_partition_files_carry_the_snapshot_id_their_digests_and_the_geo_metadata(
    tmp_path,
):
    fx = pytest.importorskip("index_fixture")
    _two_runs(fx, tmp_path)
    _, tables = _merged(tmp_path)
    files, listing = merge._partition_files(merge._route(tables), "feedcafefeedcafe")
    assert set(files) == {(p, t) for p, tables_ in listing.items() for t in tables_}
    for (partition, table), data in files.items():
        read = pq.read_table(io.BytesIO(data))
        assert set(read["snapshot"].to_pylist()) == {"feedcafefeedcafe"}
        assert listing[partition][table] == {
            "rows": len(read),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    assert listing["links"]["edges"]["rows"] == 2
    assert listing["FI"]["realtime"]["rows"] == 1
    assert b"geo" in pq.read_schema(io.BytesIO(files[("FI", "places")])).metadata
    # The same tables give the same bytes again.
    again, _ = merge._partition_files(merge._route(tables), "feedcafefeedcafe")
    assert again == files
