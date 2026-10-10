"""Tests of the merge rules: which run of each label is a source, how the
sources' tables become one set with one row per id, and what a merge
refuses to load."""

import copy
import hashlib
import io
import json
import os
import shutil
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

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
from test_index_publish import _read_on_demand
from transitio.index import fingerprint

from transitio_index import builds, classify, licensing, merge, store

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
    # Claiming schema 11 without the providers table it lists.
    eleven = _run(archived, "eleven", 16)
    _rewrite_snapshot(
        archived / eleven / "index", lambda s: s.update(schema_version=11)
    )
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
        {"id": eleven, "reason": "incomplete"},
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


def test_an_id_another_build_folded_into_a_feed_is_that_feed(tmp_path):
    def feed(feed_id, *aliases, contained_in=(), companions=()):
        row = _feed7(feed_id, feed_id, "FI", "domestic")
        return {
            **row,
            "aliases": list(aliases),
            "contained_in": list(contained_in),
            "realtime_feed_ids": list(companions),
        }

    a = _run(
        tmp_path,
        "fi",
        1,
        feeds=[
            feed("c", "x"),
            feed("m", "n", contained_in=["x"]),
            feed("p", "q", contained_in=["p", "gone"]),
            feed("z", companions=["z-rt"]),
        ],
        edges={"FI": [_edge7("hel", "c", "local", "primary", 0.5, False)]},
        realtime={"FI": [_rt("z-rt", "z", {"realtime_alerts": "https://z"})]},
        built_at=BUILT(14),
    )
    # The newer build did not fold x into c but folded z, whose companion is
    # in the older build; n claims o (a chain m->n->o), and r claims q, which
    # p claims too.
    b = _run(
        tmp_path,
        "se",
        2,
        feeds=[
            feed("c", "y", "z"),
            feed("x", companions=["x-rt"]),
            feed("n", "o"),
            feed("r", "q"),
        ],
        edges={
            "FI": [
                _edge7("esp", "c", "local", "primary", 0.5, False),
                _edge7("hel", "x", "local", "primary", 0.5, False),
            ]
        },
        realtime={"FI": [_rt("x-rt", "x", {"realtime_alerts": "https://x"})]},
        built_at=BUILT(15),
    )
    sources = []
    for run in (a, b):
        snapshot, _, tables = builds.load_tables(tmp_path / run / "index")
        sources.append((run, snapshot, tables))
    snapshot, tables = merge.merge_tables(sources)
    feeds = _frame(tables, "feeds.parquet", "feed_id")
    # x is c: its row and edges go, c keeps every source's aliases and
    # x's companion follows c; contradicting claims leave their rows alone.
    assert set(feeds.index) == {"c", "m", "n", "p", "r"}
    assert list(feeds.loc["c", "aliases"]) == ["y", "z", "x"]
    # A container is named by its merged id; itself and absent ids go.
    assert list(feeds.loc["m", "contained_in"]) == ["c"]
    assert list(feeds.loc["p", "contained_in"]) == []
    edges = tables["edges.parquet"].to_pandas()
    assert set(map(tuple, edges[["place_id", "feed_id"]].to_numpy())) == {("esp", "c")}
    realtime = _frame(tables, "realtime.parquet", "feed_id")
    assert realtime.loc["x-rt", "static_feed_id"] == "c"
    # A companion follows its re-keyed feed from any build carrying the id,
    # and the feed lists every companion now linked to it.
    assert realtime.loc["z-rt", "static_feed_id"] == "c"
    assert list(feeds.loc["c", "realtime_feed_ids"]) == ["x-rt", "z-rt"]
    assert snapshot["alias_conflicts"] == [["m", "n", "o"], ["p", "q", "r"]]


IDENTITY = {"stops.txt": "s", "routes.txt": "r", "trips.txt": "t", "calendar.txt": "c"}


@pytest.mark.parametrize(
    "other, stops, version, folded",
    [
        (IDENTITY, 5, None, True),
        ({**IDENTITY, "trips.txt": "t2"}, 5, None, False),
        (IDENTITY, 1, None, False),
        (IDENTITY, 5, 0, False),
        (IDENTITY, 5, True, False),
    ],
    ids=["same-content", "other-trips", "one-stop", "stale-version", "bool-version"],
)
def test_feeds_another_build_kept_with_the_same_content_fold(
    tmp_path, other, stops, version, folded
):
    def feed(feed_id, source, **kw):
        row = _feed7(feed_id, kw.pop("name", feed_id), "AT", "domestic")
        return {
            **row,
            "source": source,
            "stop_count": stops,
            "aliases": kw.pop("aliases", []),
            "contained_in": [],
            "mdb_id": None,
            "mdb": None,
            "crosswalk_method": "none",
            "crosswalk_confidence": 0.0,
            "realtime_feed_ids": [],
            **kw,
        }

    runs = [
        _run(
            tmp_path,
            "at",
            1,
            feeds=[
                feed(
                    "f-mdb-648",
                    "mdb",
                    mdb_id="648",
                    mdb='{"id": "648"}',
                    aliases=["f-old"],
                    realtime_feed_ids=["wl-rt"],
                )
            ],
            edges={"AT": [_edge7("wien", "f-mdb-648", "local", "primary", 0.5, False)]},
            realtime={
                "AT": [_rt("wl-rt", "f-mdb-648", {"realtime_alerts": "https://x"})]
            },
            built_at=BUILT(14),
        ),
        _run(
            tmp_path,
            "atlas6",
            2,
            feeds=[feed("f-wl", "atlas", name="Wiener Linien")],
            edges={"AT": [_edge7("wien", "f-wl", "local", "primary", 0.5, False)]},
            built_at=BUILT(15),
        ),
    ]
    recorded = {"f-mdb-648": other, "f-wl": IDENTITY}
    sources = []
    for run in runs:
        snapshot, _, tables = builds.load_tables(tmp_path / run / "index")
        feed_id = tables["feeds.parquet"]["feed_id"][0].as_py()
        snapshot = {
            **snapshot,
            "feed_identities": {feed_id: recorded[feed_id]},
            "identity_version": (
                fingerprint.IDENTITY_VERSION if version is None else version
            ),
        }
        sources.append((run, snapshot, tables))
    snapshot, tables = merge.merge_tables(sources)
    feeds = _frame(tables, "feeds.parquet", "feed_id")
    realtime = _frame(tables, "realtime.parquet", "feed_id")
    if not folded:
        assert set(feeds.index) == {"f-mdb-648", "f-wl"}
        assert snapshot["content_folds"] == {}
        return
    # The Atlas feed ranks first and takes the MDB copy's record, its id and
    # aliases; the copy's edges go and its companion follows the kept feed.
    assert set(feeds.index) == {"f-wl"}
    kept = feeds.loc["f-wl"]
    assert list(kept["aliases"]) == ["f-mdb-648", "f-old"]
    assert (kept["source"], kept["mdb_id"], kept["crosswalk_method"]) == (
        "both",
        "648",
        "content",
    )
    assert kept["name"] == "f-mdb-648"
    edges = tables["edges.parquet"].to_pandas()
    assert set(edges["feed_id"]) == {"f-wl"}
    assert realtime.loc["wl-rt", "static_feed_id"] == "f-wl"
    assert list(kept["realtime_feed_ids"]) == ["wl-rt"]
    assert snapshot["content_folds"] == {"f-mdb-648": "f-wl"}


# ---- loading a selection for a merge: the reader's schema-11 fixture ----

# What a build's manifest carries that a merge checks or copies.
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
    "license_policy": licensing.POLICY_VERSION,
}


def _archive(
    fx, archived, label, digit, *, built_at, notice=b"NOTICE\n", split=False, **fields
):
    """A schema-11 run archived as ``<label>-<16 hex>``: the reader fixture's
    partitioned index (``access`` its providers, none by default) with the
    manifest fields a merge checks; with ``split`` a schema-12 run."""
    path = archived / f"{label}-{digit:016x}" / "index"
    fx.write_partitioned_index(
        path,
        feeds=fields.pop("feeds"),
        places=fields.pop("places"),
        edges=fields.pop("edges"),
        realtime=fields.pop("realtime", []),
        contained=fields.pop("contained", {}),
        access=fields.pop("access", {"providers": []}),
        snapshot_id=f"{digit:016x}",
        notice=notice,
        split=split,
    )
    _rewrite_snapshot(
        path, lambda s: s.update({**MANIFEST_9, "built_at": built_at, **fields})
    )
    return path.parent.name


HSL_LICENCE = {"spdx_identifier": "CC-BY-4.0", "url": "https://hsl.example/licence"}
NAT_LICENCE = {"url": "https://nat.example/terms", "attribution_text": "Data by Nat"}
FLIX = {
    "provider_id": "flix",
    "name": "FlixBus",
    "registration_url": "https://flix.example/register",
    "docs_url": None,
    "terms_url": None,
    "credential_fields": ["key"],
    "free": None,
}
HEL_CENTRE = shapely.to_wkb(shapely.Point(24.94, 60.17), hex=True)
HEL_BOUNDARY = shapely.to_wkb(BOX(24.8, 60.1, 25.3, 60.35), hex=True)
BER_BOUNDARY = shapely.to_wkb(
    shapely.MultiPolygon([BOX(13.1, 52.3, 13.7, 52.7), BOX(13.8, 52.3, 13.9, 52.4)]),
    hex=True,
)
DE_SOURCES = {
    "atlas": {"archive_sha256": "d" * 64},
    "mdb": {"csv_sha256": "e" * 64},
    "gbfs": {"csv_sha256": "f" * 64},
}


# The cut of the two runs: each label's Atlas ids and the digests its build
# read (``MANIFEST_9``'s for fi, ``DE_SOURCES`` for de); nl holds none.
PARTITION = {
    "catalogues": {"atlas_commit": None},
    "labels": {
        "de": {"mdb_sha256": "e" * 64, "atlas_sha256": "d" * 64},
        "fi": {"mdb_sha256": "b" * 64, "atlas_sha256": "a" * 64},
        "nl": {"mdb_sha256": "0" * 64, "atlas_sha256": "0" * 64},
    },
    "mdb": {},
    "atlas": {"hsl": "fi", "nat": "fi", "flix": "de", "ferry": "de"},
}


def _partition(directory, change=None):
    """A ``partition.json`` of ``PARTITION`` as ``change`` edits a copy."""
    partition = copy.deepcopy(PARTITION)
    if change is not None:
        change(partition)
    path = directory / "partition.json"
    path.write_text(json.dumps(partition))
    return path


def _two_runs(fx, archived, fi_notice=b"NOTICE\n", de_notice=b"NOTICE\n", split=False):
    """A Finnish run and a newer German one that also carries Helsinki; the
    German run read other catalogue samples. With ``split`` both are
    schema-12 runs."""
    fi = _archive(
        fx,
        archived,
        "fi",
        1,
        built_at=BUILT(14),
        notice=fi_notice,
        split=split,
        feeds=[
            {
                **fx.covered_feed("hsl"),
                "home_country": "FI",
                "scope": "domestic",
                "service_start": "2026-01-01",
                "service_end": "2026-12-31",
                "atlas": {"license": HSL_LICENCE},
            },
            # A curated feed: its licence rides in its Atlas block.
            {
                **fx.covered_feed("nat"),
                "source": "curated",
                "home_country": "FI",
                "scope": "domestic",
                "atlas": {"license": NAT_LICENCE},
            },
        ],
        places=[
            fx.place("fi", "country", country_code="FI"),
            fx.place(
                "hel", "city", country_code="FI", parent_id="fi", geometry=HEL_BOUNDARY
            ),
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
        notice=de_notice,
        split=split,
        sources=DE_SOURCES,
        access={"providers": [FLIX]},
        feeds=[
            {
                **fx.covered_feed("flix"),
                "home_country": "DE",
                "scope": "domestic",
                "access": "key",
                "access_provider": "flix",
            },
            {
                **fx.covered_feed("ferry"),
                "home_country": None,
                "scope": "international",
            },
        ],
        places=[
            fx.place("ber", "city", country_code="DE", geometry=BER_BOUNDARY),
            fx.place(
                "hel",
                "city",
                country_code="FI",
                parent_id="fi",
                centre=HEL_CENTRE,
                geometry=HEL_BOUNDARY,
            ),
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
    # The same runs in the schema-12 layout load into the same tables.
    _two_runs(fx, tmp_path / "split", split=True)
    split = merge.load_sources(merge.select_sources(tmp_path / "split")[0])
    for old, new in zip(loaded, split):
        assert new["snapshot"]["schema_version"] == 12
        assert set(new["tables"]) == set(old["tables"])
        for name, table in old["tables"].items():
            assert sorted(new["tables"][name].column_names) == sorted(
                table.column_names
            )
            assert new["tables"][name].select(table.column_names).equals(table)
    # A table's listed size is checked: without one, or a wrong one, the
    # build is refused.
    index = tmp_path / "split" / split[0]["build_id"] / "index"
    partition = next(
        p for p, t in split[0]["snapshot"]["partitions"].items() if "places" in t
    )
    for size in (None, 1):
        _rewrite_snapshot(
            index,
            lambda s, size=size: s["partitions"][partition]["places"].update(
                bytes=size
            ),
        )
        assert builds.load_tables(index) is None


def _unlicensed(archived, fi, de):
    _rewrite_snapshot(archived / fi / "index", lambda s: s.update(licensed=False))


def _mixed_overture(archived, fi, de):
    _rewrite_snapshot(
        archived / de / "index", lambda s: s.update(overture_release="2026-09-01.0")
    )


def _below_schema_11(archived, fi, de):
    _rewrite_snapshot(archived / fi / "index", lambda s: s.update(schema_version=10))


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
    _drop_feed_columns(archived, fi, ["service_start", "service_end"])


def _feeds_without_containers(archived, fi, de):
    _drop_feed_columns(archived, fi, ["contained_in"])


def _feeds_without_access_details(archived, fi, de):
    _drop_feed_columns(archived, fi, ["download_url"])


def _drop_feed_columns(archived, fi, columns):
    # Feeds lacking columns their manifest's schema requires, digests intact.
    file = archived / fi / "index" / "FI" / "feeds.parquet"
    table = pq.read_table(file).drop_columns(columns)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    file.write_bytes(sink.getvalue())
    digest = hashlib.sha256(sink.getvalue()).hexdigest()
    _rewrite_snapshot(
        archived / fi / "index",
        lambda s: s["partitions"]["FI"]["feeds"].update(sha256=digest),
    )


def _an_older_licence_policy(archived, fi, de):
    _rewrite_snapshot(archived / de / "index", lambda s: s.update(license_policy=5))


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
        (_an_older_licence_policy, "licensed under policy 5"),
        (_mixed_overture, "overture_release differs"),
        (_below_schema_11, "schema_version 10"),
        (_without_a_release, "no usable overture_release"),
        (_another_classifier, "classifier differs"),
        (_a_malformed_classifier_everywhere, "no usable classifier"),
        (_a_negative_tolerance, "no usable simplify_tolerance_deg"),
        (_feeds_without_service_spans, "does not verify"),
        (_feeds_without_containers, "does not verify"),
        (_feeds_without_access_details, "does not verify"),
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


def _provider_runs(fx, archived, fi_providers, de_providers):
    """An older Finnish and a newer German run listing the given providers;
    the German feed takes ``flix``."""
    for label, digit, providers in (("fi", 1, fi_providers), ("de", 2, de_providers)):
        code = label.upper()
        _archive(
            fx,
            archived,
            label,
            digit,
            built_at=BUILT(13 + digit),
            access={"providers": providers},
            feeds=[
                {
                    **fx.covered_feed(f"{label}-bus"),
                    "home_country": code,
                    "access_provider": "flix" if label == "de" else None,
                }
            ],
            places=[fx.place(label, "country", country_code=code)],
            edges=[fx.edge(label, f"{label}-bus", tier="national")],
        )


TOKEN_FLIX = {**FLIX, "credential_fields": ["token"]}


@pytest.mark.parametrize(
    "fi_providers, de_providers, expected",
    [
        ([FLIX], [FLIX], [FLIX]),
        # A label with no feed of the provider ships no row of it, so a newer
        # definition elsewhere stands alone.
        ([], [TOKEN_FLIX], [TOKEN_FLIX]),
        # A feed's auth_params were bound to its own build's fields.
        ([TOKEN_FLIX], [FLIX], "access provider flix differs between"),
        # Another build's row cannot stand in for the feed's own.
        ([FLIX], [], "their build does not list: \\[\\('flix', 'de-"),
    ],
)
def test_the_sources_providers_merge_only_when_they_agree(
    tmp_path, fi_providers, de_providers, expected
):
    fx = pytest.importorskip("index_fixture")
    _provider_runs(fx, tmp_path, fi_providers, de_providers)
    if isinstance(expected, str):
        with pytest.raises(merge.MergeError, match=expected):
            _merged(tmp_path)
        return
    _, tables = _merged(tmp_path)
    assert tables["access_providers.parquet"].to_pylist() == expected


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
        (None, "access_providers"),
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


def test_partition_files_carry_the_snapshot_id_their_digests_and_sizes(tmp_path):
    fx = pytest.importorskip("index_fixture")
    _two_runs(fx, tmp_path)
    _, tables = _merged(tmp_path)
    files, listing = merge._partition_files(merge._route(tables), "feedcafefeedcafe")
    # The providers table at the root, outside the listing.
    root_bytes = files.pop((None, "access_providers"))
    root = pq.read_table(io.BytesIO(root_bytes))
    assert root.to_pylist() == [FLIX]
    assert set(files) == {(p, t) for p, tables_ in listing.items() for t in tables_}
    for (partition, table), data in files.items():
        read = pq.read_table(io.BytesIO(data))
        assert set(read["snapshot"].to_pylist()) == {"feedcafefeedcafe"}
        assert listing[partition][table] == {
            "rows": len(read),
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
        }
    assert listing["links"]["edges"]["rows"] == listing["links"]["details"]["rows"] == 2
    assert listing["FI"]["realtime"]["rows"] == 1
    # Each places table beside its boundaries, each edges table its details.
    for tables_ in listing.values():
        assert ("places" in tables_) == ("boundaries" in tables_)
        assert ("edges" in tables_) == ("details" in tables_)
    # The same tables give the same bytes again.
    again, _ = merge._partition_files(merge._route(tables), "feedcafefeedcafe")
    assert again.pop((None, "access_providers")) == root_bytes and again == files


# ---- the merged snapshot: its id and its manifest ----


def test_assemble_names_the_snapshot_by_its_sources_and_records_them(
    tmp_path, monkeypatch
):
    fx = pytest.importorskip("index_fixture")
    import transitio
    from transitio.index import DISCOVERY_SEMANTICS_VERSION, MIN_READER_VERSIONS

    fi, de = _two_runs(fx, tmp_path)
    _with_an_override_digest(tmp_path, fi, de)  # a source that applied overrides
    loaded, tables = _merged(tmp_path)
    manifest, files = merge.assemble(loaded, tables, b"NOTICE\n")
    snapshot_id = manifest["snapshot_id"]
    assert len(snapshot_id) == 16 and int(snapshot_id, 16) >= 0
    read = pq.read_table(io.BytesIO(files[("FI", "feeds")]))
    assert set(read["snapshot"].to_pylist()) == {snapshot_id}
    assert manifest["schema_version"] == 12 and merge.MERGE_FORMAT == 17
    assert manifest["discovery_semantics_version"] == DISCOVERY_SEMANTICS_VERSION
    assert manifest["min_reader_version"] == MIN_READER_VERSIONS[12]
    assert manifest["built_with"] == transitio.__version__
    assert manifest["built_at"] == BUILT(15)  # the newest source's, not the clock
    assert manifest["counts"] == {
        "feeds": 4,
        "by_source": {"atlas": 3, "curated": 1},
        "feeds_dated": 1,
        "realtime": 2,
        "realtime_linked": 1,
        "realtime_unlinked": 1,
        "places": 3,
        "places_by_kind": {"city": 2, "country": 1},
        "edges": 5,
        "edges_by_tier": {"international": 2, "local": 2, "national": 1},
        "access_providers": 1,
    }
    assert {
        p: {t: e["rows"] for t, e in ts.items()}
        for p, ts in manifest["partitions"].items()
    } == {
        "DE": {"boundaries": 1, "details": 1, "edges": 1, "feeds": 1, "places": 1},
        "FI": {
            "boundaries": 2,
            "details": 2,
            "edges": 2,
            "feeds": 2,
            "places": 2,
            "realtime": 1,
        },
        "international": {"feeds": 1, "realtime": 1},
        "links": {"details": 2, "edges": 2},
    }
    for (partition, table), data in files.items():
        entry = (
            manifest["partitions"][partition][table]
            if partition is not None
            else {"sha256": manifest["access_providers_sha256"]}
        )
        assert entry["sha256"] == hashlib.sha256(data).hexdigest()
    assert manifest["licensed"] is True
    assert manifest["license_policy"] == licensing.POLICY_VERSION
    assert manifest["notice_sha256"] == hashlib.sha256(b"NOTICE\n").hexdigest()
    assert manifest["overture_release"] == "2026-08-19.0"
    assert manifest["simplify_tolerance_deg"] == 0.0005
    assert manifest["classifier"] == classify.classifier_settings()
    assert manifest["coverage_mode"] == "crawled"
    assert manifest["unknown_share"] == 0.0 and manifest["margin_share"] == 0.0
    assert all(manifest[field] is None for field in merge.OVERRIDE_FIELDS)
    assert manifest["stale_feed_overrides"] == 2 and manifest["stale_overrides"] == 2
    assert manifest["alias_conflicts"] == []
    conflicted, _ = merge.assemble(
        loaded, tables, b"NOTICE\n", alias_conflicts=[("p", "q", "r")]
    )
    assert conflicted["alias_conflicts"] == [["p", "q", "r"]]
    assert manifest["catalogue_check"] is None
    # A check against a partition is recorded, and the partition names the id.
    check = {"partition_sha256": "p" * 64}
    checked, _ = merge.assemble(loaded, tables, b"NOTICE\n", catalogue_check=check)
    assert checked["catalogue_check"] == check
    assert checked["snapshot_id"] != snapshot_id
    # The sources by portable identities only, in label order.
    assert [(s["label"], s["build_id"]) for s in manifest["merged"]] == [
        ("de", de),
        ("fi", fi),
    ]
    for record, source in zip(manifest["merged"], loaded):
        assert record["snapshot_id"] == source["snapshot"]["snapshot_id"]
        assert record["built_at"] == source["snapshot"]["built_at"]
        assert record["coverage_mode"] == "crawled"
        assert record["sources"] == merge._pins(source["snapshot"], source["build_id"])
        assert record["partitions"] == source["snapshot"]["partitions"]
        assert record["snapshot_sha256"] == source["snapshot_sha256"]
        assert record["notice_sha256"] == source["notice_sha256"]
    assert str(tmp_path) not in json.dumps(manifest)
    assert "generations" not in manifest and "leaves" not in manifest
    # The same sources give the same id, manifest and bytes; another
    # NOTICE changes only its digest; another merge format, the id; so
    # does any change to a source's manifest, and nothing else does.
    monkeypatch.setattr(time, "time", lambda: 4102444800.0)  # another day
    again, files_again = merge.assemble(*_merged(tmp_path), b"NOTICE\n")
    assert again == manifest and files_again == files
    other, _ = merge.assemble(loaded, tables, b"other\n")
    assert (
        other["snapshot_id"] == snapshot_id
        and other["notice_sha256"] != manifest["notice_sha256"]
    )
    monkeypatch.setattr(merge, "MERGE_FORMAT", merge.MERGE_FORMAT + 1)
    assert merge.assemble(loaded, tables, b"NOTICE\n")[0]["snapshot_id"] != snapshot_id
    monkeypatch.undo()
    _rewrite_snapshot(
        tmp_path / de / "index", lambda s: s.update(stale_edge_overrides=1)
    )
    assert (
        merge.assemble(*_merged(tmp_path), b"NOTICE\n")[0]["snapshot_id"] != snapshot_id
    )
    _rewrite_snapshot(
        tmp_path / de / "index", lambda s: s.update(stale_edge_overrides=0)
    )
    assert (
        merge.assemble(*_merged(tmp_path), b"NOTICE\n")[0]["snapshot_id"] == snapshot_id
    )


def test_assemble_recounts_the_shares_and_marks_a_mixed_coverage(tmp_path):
    fx = pytest.importorskip("index_fixture")
    _two_runs(fx, tmp_path)
    _archive(
        fx,
        tmp_path,
        "se",
        3,
        built_at=BUILT(13),
        coverage_mode="declared",
        feeds=[{**fx.covered_feed("sl"), "home_country": "SE", "scope": "domestic"}],
        places=[fx.place("sto", "city", country_code="SE")],
        edges=[
            {
                **fx.edge("sto", "sl", tier="unknown"),
                "evidence": {"near_threshold": True},
            }
        ],
    )
    loaded, tables = _merged(tmp_path)
    manifest, _ = merge.assemble(loaded, tables, b"NOTICE\n")
    assert manifest["coverage_mode"] == "mixed"
    assert manifest["counts"]["edges"] == 6
    assert manifest["unknown_share"] == pytest.approx(1 / 6)
    assert manifest["margin_share"] == pytest.approx(1 / 6)
    assert manifest["built_at"] == BUILT(15)
    assert [s["coverage_mode"] for s in manifest["merged"]] == [
        "crawled",
        "crawled",
        "declared",
    ]


def test_the_merged_block_carries_portable_identities_only(tmp_path):
    fx = pytest.importorskip("index_fixture")
    fi, _ = _two_runs(fx, tmp_path)
    sources = {
        "atlas": {"commit": "c" * 40, "archive_sha256": "a" * 64, "path": "/tmp/atlas"},
        "mdb": {
            "csv_label": "2026-08-28",
            "csv_sha256": "b" * 64,
            "file": "/tmp/mdb.csv",
        },
    }
    _rewrite_snapshot(tmp_path / fi / "index", lambda s: s.update(sources=sources))
    _rewrite_snapshot(
        tmp_path / fi / "index",
        lambda s: s["partitions"]["FI"]["feeds"].update(path="/tmp/feeds.parquet"),
    )
    manifest, _ = merge.assemble(*_merged(tmp_path), b"NOTICE\n")
    record = manifest["merged"][1]
    assert record["sources"] == {
        "atlas": {
            "commit": "c" * 40,
            "archive_sha256": "a" * 64,
            "commit_verified": None,
        },
        "mdb": {"csv_label": "2026-08-28", "csv_sha256": "b" * 64},
    }
    assert set(record["partitions"]["FI"]["feeds"]) == {"rows", "sha256"}
    assert "/tmp" not in json.dumps(manifest)
    for sources, message in (
        ({}, "no catalogue sources"),
        ({"other": {"x": 1}}, "no catalogue sources"),
        ({"atlas": "a4d0204"}, "catalogue atlas is not a record"),
    ):
        _rewrite_snapshot(
            tmp_path / fi / "index",
            lambda s, sources=sources: s.update(sources=sources),
        )
        with pytest.raises(merge.MergeError, match=message):
            merge.assemble(*_merged(tmp_path), b"NOTICE\n")


@pytest.mark.parametrize(
    "evidence, message",
    [
        pytest.param("not json", "is not JSON", id="text"),
        pytest.param("", "is not JSON", id="empty"),
        pytest.param(1, "is not JSON", id="number"),
        # Past the recursion limit where the decoder has one, a list elsewhere.
        pytest.param(
            "[" * 100_000 + "]" * 100_000, "is not (JSON|a record)", id="deep"
        ),
        pytest.param('"text"', "is not a record", id="string"),
        pytest.param("[1]", "is not a record", id="list"),
    ],
)
def test_edges_whose_evidence_is_not_a_record_are_refused(evidence, message):
    edges = pa.table({"tier": ["local"], "evidence": [evidence]})
    with pytest.raises(merge.MergeError, match=message):
        merge._shares(edges)


@pytest.mark.parametrize(
    "others, named",
    [
        pytest.param(
            {"f-old": {"bus": 0.7}, "f-new": {"bus": 0.6, "rail": 0.2}},
            {"f-new": {"bus": 0.7, "rail": 0.2}},
            id="alias",
        ),
        pytest.param({"f-a": {"bus": 1.0}, "f-a-old": {"bus": 0.5}}, {}, id="own"),
        pytest.param({"f-gone": {"bus": 0.5}}, {}, id="unknown"),
        pytest.param(None, None, id="no-block"),
        pytest.param({"f-new": [0.5]}, merge.MergeError, id="not-a-record"),
    ],
)
def test_overlap_evidence_names_the_merged_feeds(others, named):
    evidence = {"near_threshold": False}
    if others is not None:
        evidence["overlap"] = {"departures": {"bus": 2.0}, "with": others}
    edges = pa.table(
        {"feed_id": ["f-a", "f-a"], "evidence": [json.dumps(evidence), None]}
    )
    mapping = {"f-old": "f-new", "f-a-old": "f-a"}
    if named is merge.MergeError:
        with pytest.raises(named, match="overlap is not a record"):
            merge._merged_overlaps(edges, mapping, ["f-a", "f-new"])
        return
    merged = merge._merged_overlaps(edges, mapping, ["f-a", "f-new"])
    first, second = merged["evidence"].to_pylist()
    assert second is None
    if named is None:
        assert first == edges["evidence"][0].as_py()
    else:
        assert json.loads(first)["overlap"]["with"] == named


def test_overlap_evidence_lists_the_feeds_its_build_measured():
    # MVV's feed came from another build than DELFI's: neither names the
    # other because nothing compared them, which the block now records.
    block = {"overlap": {"departures": {"bus": 2.0}, "with": {}}}
    edges = pa.table(
        {
            "place_id": ["muc", "muc", "muc"],
            "feed_id": ["f-delfi", "f-mvg", "f-mvv"],
            "build_id": ["de-1", "de-1", "atlas2-1"],
            "evidence": [json.dumps(block)] * 3,
        }
    )
    measured = {
        ("muc", "de-1"): {"f-delfi", "f-mvg", "f-gone"},
        ("muc", "atlas2-1"): {"f-mvv", "f-delfi"},
    }
    feeds = ["f-delfi", "f-mvg", "f-mvv"]
    merged = merge._merged_overlaps(edges, {}, feeds, measured)
    evidence = merged["evidence"].to_pylist()
    compared = [json.loads(e)["overlap"]["compared"] for e in evidence]
    assert compared == [["f-mvg"], ["f-delfi"], ["f-delfi"]]


def test_an_edge_without_evidence_counts_as_not_near_a_threshold():
    edges = pa.table(
        {"tier": ["unknown", "local"], "evidence": [None, '{"near_threshold": true}']}
    )
    assert merge._shares(edges) == (0.5, 0.5)


@pytest.mark.parametrize(
    "value, outcome", [(None, 0), (3, 3), (-1, None), (True, None)]
)
def test_a_stale_count_is_a_non_negative_integer_or_nothing(value, outcome):
    snapshot = {"stale_feed_overrides": value}
    if outcome is None:
        with pytest.raises(merge.MergeError, match="is not a count"):
            merge._count(snapshot, "stale_feed_overrides", "b")
    else:
        assert merge._count(snapshot, "stale_feed_overrides", "b") == outcome


def test_the_snapshot_id_tells_apart_fields_a_delimiter_would_blur():
    def loaded(label, build_id):
        return [
            {
                "label": label,
                "build_id": build_id,
                "snapshot_sha256": "0" * 64,
                "notice_sha256": "1" * 64,
            }
        ]

    assert merge._merged_id(loaded("a|b", "c")) != merge._merged_id(loaded("a", "b|c"))
    assert merge._merged_id(loaded("a", "b")) == merge._merged_id(loaded("a", "b"))


# ---- the merged NOTICE ----

GEOMETRY = [
    "This index includes place boundary geometry from the Overture Maps",
    "divisions theme (release 2026-08-19.0), provided under CDLA-Permissive-2.0",
    "(https://cdla.dev/permissive-2-0/) and derived from:",
]
ODBL = [
    "Geometry derived from OpenStreetMap is a Derived Database under the",
    "Open Database License (ODbL 1.0) and is made available under that same",
    "licence; its share-alike terms apply.",
]
METRO = [
    "Metro memberships were derived at build time from these sources;",
    "the derived use ships no boundary data of its own:",
]
ESRI = (
    "  - Esri Community Maps — CC0 1.0 (https://creativecommons.org/publicdomain/zero/)"
)
OSM = (
    "  - OpenStreetMap, © OpenStreetMap contributors — ODbL 1.0 (https://odbl.example/)"
)
GEOB = "  - geoBoundaries — CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/)"
EUROSTAT = (
    "  - Source: Eurostat, metropolitan regions (NUTS 2021) — Eurostat copyright notice"
)
GHS = "  - GHS Urban Centre Database 2025 (GHS-UCDB R2024A) — CC BY 4.0"
CATALOGUES = [
    "Feed identities and coverage were compiled from:",
    "  - Transitland Atlas, commit 84829aaf23e7f07a9633fbed1a4a7d3e44ae9362",
]
LICENCES = [
    "Feed licences declared by the catalogues (feeds per licence):",
    "  - none declared: 2",
]


def _paragraphs(paragraphs):
    return ("\n\n".join("\n".join(lines) for lines in paragraphs) + "\n").encode()


PROVIDERS = "Feeds from credential providers, under their terms:"
ODPT = ["      notice: CC BY 4.0", "      notice: feed: a notice reading like a feed"]


def _odpt(first, last, feed):
    head = f"  - odpt: ODPT; data obtained {first} to {last}"
    return [head, *ODPT, f"      feed: {feed}; CC-BY-4.0; https://o.example/{feed}"]


def _notice_text(derived=(), odbl=ODBL, metro=(), geometry=GEOMETRY, providers=()):
    """A NOTICE as the license stage writes it for one build: the geometry
    credit's source list is a block of its own after a blank line, the way
    the geometry audit formats it."""
    paragraphs = [geometry]
    if derived:
        paragraphs.append(list(derived))
    if odbl:
        paragraphs.append(odbl)
    if metro:
        paragraphs.append([*METRO, *metro])
    paragraphs.append(CATALOGUES)
    if providers:
        paragraphs.append([PROVIDERS, *providers])
    return _paragraphs([*paragraphs, LICENCES])


def test_the_merged_notice_credits_every_source_once_and_recounts_the_licences(
    tmp_path,
):
    fx = pytest.importorskip("index_fixture")
    _two_runs(
        fx,
        tmp_path,
        fi_notice=_notice_text(
            odbl=None,
            metro=[EUROSTAT],
            providers=_odpt("2026-10-02", "2026-10-03", "a"),
        ),  # a feeds-only style run
        de_notice=_notice_text(
            [OSM, ESRI, GEOB],
            metro=[GHS, EUROSTAT],
            providers=[
                "  - nsw: TfNSW; data obtained 2026-09-30 to 2026-09-30",
                "      feed: n; no licence declared; https://n.example/n",
                *_odpt("2026-10-01", "2026-10-02", "b"),
            ],
        ),
    )
    loaded, tables = _merged(tmp_path)
    notice = merge.compose_notice(loaded, tables["feeds.parquet"])
    assert notice == _paragraphs(
        [
            [*GEOMETRY, *sorted([ESRI, OSM, GEOB])],
            ODBL,
            [*METRO, *sorted([EUROSTAT, GHS])],
            [
                "Feed identities and coverage were compiled from:",
                "  - Transitland Atlas, archive sha256 " + "a" * 64,
                "  - Transitland Atlas, archive sha256 " + "d" * 64,
                "  - Mobility Database catalog, sha256 " + "b" * 64,
                "  - Mobility Database catalog, sha256 " + "e" * 64,
                "  - GBFS systems.csv, sha256 " + "f" * 64,
            ],
            # Each provider once: the widest span, the union of its feeds.
            [
                PROVIDERS,
                "  - nsw: TfNSW; data obtained 2026-09-30 to 2026-09-30",
                "      feed: n; no licence declared; https://n.example/n",
                *_odpt("2026-10-01", "2026-10-03", "a"),
                "      feed: b; CC-BY-4.0; https://o.example/b",
            ],
            [
                "Feed licences declared by the catalogues (feeds per licence):",
                "  - CC-BY-4.0: 1",
                "      url: https://hsl.example/licence",
                "  - none declared: 2",
                # The curated feed's licence and the attribution it requires.
                "  - no identifier: 1",
                "      url: https://nat.example/terms",
                "      attribution: Data by Nat",
            ],
        ]
    )
    assert b"84829aaf" not in notice  # the commit no build verified is not named
    assert merge.compose_notice(loaded, tables["feeds.parquet"]) == notice


@pytest.mark.parametrize(
    "de_notice, message",
    [
        (b"NOTICE\n", "paragraph unknown"),
        # A non-indented block with no known opening is unknown; an indented
        # block with no section before it has nothing to continue.
        (
            _paragraphs([GEOMETRY, ["An extra clause."], CATALOGUES, LICENCES]),
            "paragraph unknown",
        ),
        (
            _paragraphs([["  - orphan bullet"], GEOMETRY, CATALOGUES, LICENCES]),
            "paragraph unknown",
        ),
        (_paragraphs([GEOMETRY, LICENCES]), "lacks its catalogue paragraph"),
        (
            _paragraphs([GEOMETRY, CATALOGUES, LICENCES, LICENCES]),
            "repeats its licence",
        ),
        (
            _notice_text(geometry=[*GEOMETRY[:1], "another release", GEOMETRY[2]]),
            "geometry notice differs",
        ),
        (_notice_text(odbl=[*ODBL[:2], "other terms"]), "ODbL notice differs"),
        (
            _notice_text(providers=[*_odpt("2026-10-01", "2026-10-01", "a")[:2]]),
            "provider odpt notice differs",
        ),
        (_notice_text(providers=["  * odpt"]), "provider line unknown"),
        (b"\xff\xfe", "not UTF-8"),
    ],
)
def test_a_source_notice_the_merge_cannot_compose_from_is_refused(
    tmp_path, de_notice, message
):
    fx = pytest.importorskip("index_fixture")
    fi_notice = _notice_text(
        [ESRI, OSM], providers=_odpt("2026-10-01", "2026-10-01", "a")
    )
    _two_runs(fx, tmp_path, fi_notice=fi_notice, de_notice=de_notice)
    loaded, tables = _merged(tmp_path)
    with pytest.raises(merge.MergeError, match=message):
        merge.compose_notice(loaded, tables["feeds.parquet"])


def _feeds_with(atlas):
    return pa.table({"atlas": [atlas], "mdb": [None], "redistribution_allowed": [None]})


@pytest.mark.parametrize(
    "feeds, message",
    [
        (pa.table({"feed_id": ["f"]}), "without atlas"),
        (_feeds_with("[1]"), "block is not a record"),
        (_feeds_with("{"), "is not JSON"),
        (_feeds_with(""), "is not JSON"),
        (_feeds_with('{"license": [1]}'), "licence block is not a record"),
        (_feeds_with('{"license": {"url": ["x"]}}'), "cannot be inventoried"),
    ],
)
def test_feeds_whose_catalogue_blocks_cannot_be_inventoried_are_refused(feeds, message):
    with pytest.raises(merge.MergeError, match=message):
        merge._licence_rows(feeds)


@pytest.mark.parametrize(
    "pins, message",
    [
        ({"atlas": {}}, "atlas archive_sha256 is not a SHA-256: None"),
        ({"mdb": {"csv_sha256": "short"}}, "mdb csv_sha256 is not a SHA-256"),
        ({"gbfs": {"csv_sha256": ["x"]}}, "gbfs csv_sha256 is not a SHA-256"),
    ],
)
def test_a_catalogue_without_its_digest_is_refused(pins, message):
    loaded = [{"build_id": "b", "snapshot": {"sources": pins}}]
    with pytest.raises(merge.MergeError, match=message):
        merge._catalogue_lines(loaded)


# ---- the catalogue check against a partition ----


def _cut_read(mdb, atlas):
    return {"sources": {"mdb": {"csv_sha256": mdb}, "atlas": {"archive_sha256": atlas}}}


def test_the_catalogue_check_looks_each_id_up_as_a_feed_or_an_alias():
    from transitio_index import overrides

    feeds = pa.table(
        {
            "feed_id": ["f-mdb-1", "f-abc", "f-xyz"],
            "aliases": [[], ["f-mdb-2"], ["f-old", "f-curated-folded"]],
        }
    )
    cut = {"mdb_sha256": "m" * 64, "atlas_sha256": "a" * 64}
    curated = {
        ref: {"feed": ref, "add_feed": {"location": {"country_code": code}}}
        for ref, code in (
            ("f-curated-folded", "FI"),
            ("f-curated-lost", "FI"),
            ("f-curated-nowhere", "AR"),
        )
    }
    curated["f-abc"] = {"feed": "f-abc", "mark_uncrawlable": True}
    partition = {
        "catalogues": {"atlas_commit": "c0ffee"},
        "labels": {"fi": {**cut, "countries": ["FI"]}, "se": cut, "zw": cut},
        "mdb": {"mdb-1": "fi", "mdb-2": "fi", "mdb-3": "fi", "mdb-9": "zw"},
        "atlas": {"f-abc": "fi", "f-old": "se", "f-gone": "se"},
    }
    loaded = [
        {"label": "fi", "build_id": "fi-1", "snapshot": _cut_read("m" * 64, "a" * 64)},
        {"label": "se", "build_id": "se-2", "snapshot": _cut_read("m" * 64, "0" * 64)},
        {"label": "us", "build_id": "us-3", "snapshot": _cut_read("m" * 64, "a" * 64)},
    ]
    check = merge.catalogue_check(partition, "p" * 64, feeds, loaded, curated)
    assert check == {
        "partition_sha256": "p" * 64,
        "catalogues": {"atlas_commit": "c0ffee"},
        "expected": {"mdb": 4, "atlas": 3, "curated": 3},
        "curated_sha256": overrides.phase_digest(
            curated, overrides.CROSSWALK_OPERATIONS
        ),
        "missing": [
            {"catalogue": "curated", "id": "f-curated-nowhere", "label": None},
            {"catalogue": "curated", "id": "f-curated-lost", "label": "fi"},
            {"catalogue": "mdb", "id": "mdb-3", "label": "fi"},
            {"catalogue": "atlas", "id": "f-gone", "label": "se"},
            {"catalogue": "mdb", "id": "mdb-9", "label": "zw"},
        ],
        "labels_not_merged": ["zw"],
        "labels_outside": ["us"],
        "labels_from_another_cut": ["se"],
    }
    lines = []
    merge._report_check(check, lines.append)
    assert lines == [
        "label zw not merged: 1 catalogue feeds missing",
        "missing curated f-curated-nowhere (no label holds its country)",
        "missing curated f-curated-lost (label fi)",
        "missing mdb mdb-3 (label fi)",
        "missing atlas f-gone (label se)",
        "label us is not in the partition",
        "label se was built from another cut",
    ]


@pytest.mark.parametrize(
    "partition, message",
    [
        ("{not json", "not JSON"),
        ({"labels": {}, "mdb": {}}, "needs catalogues, labels, mdb and atlas"),
        (
            {"labels": {"fi": {"mdb_sha256": "m"}}, "mdb": {}, "atlas": {}},
            "label fi records no cut digests",
        ),
        (
            {"labels": {}, "mdb": {"mdb-1": "zw"}, "atlas": {}},
            "mdb mdb-1 is in 'zw', a label the partition does not list",
        ),
        (
            {
                "labels": {
                    "fi": {"mdb_sha256": "", "atlas_sha256": "", "countries": "FI"}
                },
                "mdb": {},
                "atlas": {},
            },
            "label fi lists no country codes",
        ),
    ],
)
def test_a_partition_the_merge_cannot_check_against_is_refused(
    tmp_path, partition, message
):
    path = tmp_path / "partition.json"
    if not isinstance(partition, str):
        partition = json.dumps({"catalogues": {}, **partition})
    path.write_text(partition)
    with pytest.raises(merge.MergeError, match=message):
        merge.read_partition(path)


# ---- writing the merged snapshot and the command ----


def _reader():
    reader = pytest.importorskip("transitio.index")
    return reader.read_index


def _files_under(index):
    return {
        p.relative_to(index): p.read_bytes()
        for p in sorted(index.rglob("*"))
        if p.is_file()
    }


def test_the_merge_command_writes_an_index_the_reader_reads_back(tmp_path, capsys):
    fx = pytest.importorskip("index_fixture")
    read_index = _reader()
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    fi, de = _two_runs(
        fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM])
    )
    _run(builds, "nl", 3, feeds=[], edges={})  # no feeds: skipped, reported
    partition = _partition(tmp_path, lambda p: p["atlas"].update(lost="fi"))
    assert (
        merge.main(
            [
                *("--builds", str(builds), "--cache-dir", str(cache)),
                *("--partition", str(partition), "--overrides-dir", str(tmp_path)),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "skipped nl-0000000000000003: no feeds" in out
    assert "missing atlas lost (label fi)" in out
    assert (
        "merged 2 builds into" in out
        and "4 feeds, 3 places, 5 edges, 2 companions; 1 catalogue feeds missing" in out
    )
    index = read_index(cache / "index")
    assert index.snapshot["merged"][0]["build_id"] == de
    assert len(index.feeds) == 4 and len(index.places) == 3 and len(index.edges) == 5
    assert len(index.links) == 2 and len(index.realtime) == 2
    assert set(index.feeds["snapshot"]) == {index.snapshot["snapshot_id"]}
    assert index.access_provider("flix").name == "FlixBus"
    centre = index.places.set_index("place_id").loc["hel", "centre"]
    assert centre.equals(shapely.from_wkb(HEL_CENTRE))
    # The schema-11 sources' boundaries read back bit for bit, and the
    # edges' evidence as the merge gave it.
    boundaries = _read_on_demand(index, "boundaries")
    assert shapely.to_wkb(boundaries["hel"], hex=True) == HEL_BOUNDARY
    assert shapely.to_wkb(boundaries["ber"], hex=True) == BER_BOUNDARY
    assert boundaries["fi"] is None
    # The core places are plain Parquet, without the sources' GeoParquet
    # metadata.
    for path in (cache / "index").glob("*/places.parquet"):
        assert b"geo" not in (pq.read_schema(path).metadata or {})
    loaded = merge.load_sources(merge.select_sources(builds)[0])
    _, tables = merge.merge_tables(
        [(s["build_id"], s["snapshot"], s["tables"]) for s in loaded]
    )
    edges = tables["edges.parquet"]
    details = _read_on_demand(index, "details").reset_index()
    assert dict(zip(_pairs(edges), edges["evidence"].to_pylist())) == dict(
        zip(zip(details["place_id"], details["feed_id"]), details["evidence"])
    )
    notice = (cache / "index" / "NOTICE").read_bytes()
    assert notice.startswith(b"This index includes place boundary geometry")
    assert hashlib.sha256(notice).hexdigest() == index.snapshot["notice_sha256"]
    assert not list(cache.glob("index.*.tmp"))
    # Nothing to merge is refused, and said so.
    assert (
        merge.main(["--builds", str(tmp_path / "empty"), "--cache-dir", str(cache)])
        == 1
    )
    assert "no build to merge" in capsys.readouterr().err


def test_a_merge_is_refused_while_another_holds_the_index(tmp_path):
    fx = pytest.importorskip("index_fixture")
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    _two_runs(fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM]))
    for held in (store.open_directory(cache), store.open_subdir(cache, "index")):
        try:  # the cache's lock guards the staging, the index's the commit
            with store.exclusive_writer(held):
                with pytest.raises(
                    store.StoreError, match="another build is publishing"
                ):
                    merge.merge_builds(builds, cache, log=lambda line: None)
        finally:
            held.close()
        assert not list(cache.glob("index.*.tmp"))


def test_a_staging_path_that_cannot_be_cleared_is_refused(tmp_path):
    fx = pytest.importorskip("index_fixture")
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    _two_runs(fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM]))
    loaded, tables = _merged(builds)
    manifest, _ = merge.assemble(loaded, tables, compose_notice_for(loaded, tables))
    cache.mkdir()
    (cache / f"index.{manifest['snapshot_id']}.tmp").write_text("not a directory")
    with pytest.raises(merge.MergeError, match="cannot remove the staging directory"):
        merge.merge_builds(builds, cache, log=lambda line: None)
    assert not (cache / "index" / "snapshot.json").exists()


def compose_notice_for(loaded, tables):
    return merge.compose_notice(loaded, tables["feeds.parquet"])


def test_a_build_of_places_without_feeds_merges_with_fed_ones(tmp_path):
    fx = pytest.importorskip("index_fixture")
    pytest.importorskip("geopandas")
    from test_index_license import _feedless_index

    from transitio_index import geometry, ucdb

    read_index = _reader()
    builds, merged = tmp_path / "builds", tmp_path / "merged"
    _two_runs(fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM]))
    feedless = _feedless_index(tmp_path)
    shutil.copytree(feedless / "index", builds / "cities-0000000000000003" / "index")
    manifest = merge.merge_builds(builds, merged, log=lambda line: None)
    assert len(manifest["merged"]) == 3
    credit = geometry.DERIVED_SOURCES[ucdb.DERIVED]["credit"]
    assert (merged / "index" / "NOTICE").read_text().count(credit) == 1
    index = read_index(merged / "index")
    assert len(index.feeds) == 4 and len(index.places) == 5
    peru = read_index(merged / "index", country="PE")
    assert sorted(peru.places["place_id"]) == ["tp_lima", "tp_pe"]


def test_a_second_merge_replaces_the_live_index_and_drops_what_it_lacks(tmp_path):
    fx = pytest.importorskip("index_fixture")
    read_index = _reader()
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    fi, de = _two_runs(
        fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM])
    )
    first = merge.merge_builds(builds, cache, log=lambda line: None)
    shutil.rmtree(builds / de)
    second = merge.merge_builds(builds, cache, log=lambda line: None)
    assert second["snapshot_id"] != first["snapshot_id"]
    assert sorted(second["partitions"]) == ["FI", "international"]
    assert (
        not (cache / "index" / "DE").exists()
        and not (cache / "index" / "links").exists()
    )
    index = read_index(cache / "index")
    assert index.snapshot["snapshot_id"] == second["snapshot_id"]
    assert len(index.feeds) == 2 and len(index.places) == 2 and index.links is None


# A core table, and an on-demand one that only the full check reads.
@pytest.mark.parametrize("table", ["feeds", "boundaries"])
def test_a_snapshot_the_reader_rejects_leaves_the_live_index_untouched(
    tmp_path, monkeypatch, table
):
    fx = pytest.importorskip("index_fixture")
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    _two_runs(fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM]))
    merge.merge_builds(builds, cache, log=lambda line: None)
    before = _files_under(cache / "index")
    assemble = merge.assemble

    def tampered(loaded, tables, notice, **options):  # a digest the reader refuses
        manifest, files = assemble(loaded, tables, notice, **options)
        manifest["partitions"]["FI"][table]["sha256"] = "0" * 64
        return manifest, files

    monkeypatch.setattr(merge, "assemble", tampered)
    with pytest.raises(merge.MergeError, match="does not read back"):
        merge.merge_builds(builds, cache, log=lambda line: None)
    assert _files_under(cache / "index") == before
    assert not list(cache.glob("index.*.tmp"))
    # A cache that had no index has none afterwards either.
    fresh = tmp_path / "fresh"
    with pytest.raises(merge.MergeError, match="does not read back"):
        merge.merge_builds(builds, fresh, log=lambda line: None)
    assert not (fresh / "index").exists() and not list(fresh.glob("index.*.tmp"))


def test_a_failure_mid_commit_leaves_an_index_the_reader_refuses_and_the_next_merge_completes(
    tmp_path, monkeypatch
):
    fx = pytest.importorskip("index_fixture")
    read_index = _reader()
    exceptions = pytest.importorskip("transitio.exceptions")
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    fi, _ = _two_runs(
        fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM])
    )
    merge.merge_builds(builds, cache, log=lambda line: None)
    _archive(
        fx,
        builds,
        "se",
        3,
        built_at=BUILT(16),
        notice=_notice_text([GEOB]),
        feeds=[{**fx.covered_feed("sl"), "home_country": "SE", "scope": "domestic"}],
        places=[fx.place("sto", "city", country_code="SE")],
        edges=[fx.edge("sto", "sl", tier="local", relevance_category="primary")],
    )
    write_bytes = store.write_bytes

    def failing(directory, name, data):  # the live NOTICE write dies after the tables
        if name == "NOTICE" and directory.path.name == "index":
            raise OSError("disk full")
        return write_bytes(directory, name, data)

    monkeypatch.setattr(store, "write_bytes", failing)
    with pytest.raises(OSError, match="disk full"):
        merge.merge_builds(builds, cache, log=lambda line: None)
    with pytest.raises(exceptions.IncompatibleIndexError):
        read_index(cache / "index")  # new tables under the old manifest
    assert not list(cache.glob("index.*.tmp"))
    monkeypatch.undo()
    manifest = merge.merge_builds(builds, cache, log=lambda line: None)
    index = read_index(cache / "index")
    assert index.snapshot["snapshot_id"] == manifest["snapshot_id"]
    assert sorted(index.partitions) == ["DE", "FI", "SE", "international", "links"]


# ---- releasing a merged snapshot ----


def _small_run(fx, archived, label, digit, built_at):
    """A run of one Swedish feed, place and edge."""
    return _archive(
        fx,
        archived,
        label,
        digit,
        built_at=built_at,
        notice=_notice_text([GEOB]),
        feeds=[{**fx.covered_feed("sl"), "home_country": "SE", "scope": "domestic"}],
        places=[fx.place("sto", "city", country_code="SE")],
        edges=[fx.edge("sto", "sl", tier="local", relevance_category="primary")],
    )


def _merged_cache(fx, tmp_path, partition=None, overrides_dir=None):
    """The two runs and a skipped one merged, checked against ``PARTITION``
    as ``partition`` edits it and the add_feed entries of ``overrides_dir``;
    ``False`` merges without a check."""
    builds, cache = tmp_path / "builds", tmp_path / "cache"
    fi, de = _two_runs(
        fx, builds, fi_notice=_notice_text([ESRI]), de_notice=_notice_text([OSM])
    )
    _run(builds, "nl", 3, feeds=[], edges={})  # skipped: no feeds
    manifest = merge.merge_builds(
        builds,
        cache,
        log=lambda line: None,
        partition=None if partition is False else _partition(tmp_path, partition),
        overrides_dir=overrides_dir,
    )
    return builds, cache, fi, de, manifest


def test_the_publisher_packs_a_merged_snapshot_with_its_lineage_checked(tmp_path):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import publisher

    builds, cache, fi, de, manifest = _merged_cache(fx, tmp_path)
    assets, release = publisher.pack(
        cache / "index", cache_dir=cache, builds_dir=builds
    )
    assert release["snapshot_id"] == manifest["snapshot_id"]
    assert release["lineage"]["merged"] == manifest["merged"]
    assert release["lineage"]["catalogue_check"] == manifest["catalogue_check"]
    assert (
        release["lineage"]["licensed"] is True
        and release["lineage"]["generations"] is None
    )
    archive = f"transitio-index-{manifest['snapshot_id']}.tar.gz"
    assert {archive, archive + ".sha256"} < set(assets) and len(assets) == 3
    # Naming the cache but not the archived builds cannot check a merged
    # index, so it is refused rather than packed unchecked.
    with pytest.raises(publisher.PublishIndexError, match="name their directory"):
        publisher.pack(cache / "index", cache_dir=cache)
    # builds_dir alone checks it, no cache named.
    assets_again, _ = publisher.pack(cache / "index", builds_dir=builds)
    assert set(assets_again) == set(assets)
    # Neither directory is the assets-only escape hatch: built, not checked.
    assets_bare, _ = publisher.pack(cache / "index")
    assert set(assets_bare) == set(assets)
    # The merge's commit lock is the one the pack takes.
    index = store.open_subdir(cache, "index")
    try:
        with store.exclusive_writer(index):
            with pytest.raises(store.StoreError, match="another build is publishing"):
                publisher.pack(cache / "index", cache_dir=cache, builds_dir=builds)
    finally:
        index.close()


def _a_source_manifest_changed(fx, builds, fi, de):
    _rewrite_snapshot(builds / de / "index", lambda s: s.update(stale_edge_overrides=1))


def _a_source_notice_changed(fx, builds, fi, de):
    (builds / de / "index" / "NOTICE").write_bytes(_notice_text([GEOB]))


def _a_label_with_a_newer_run(fx, builds, fi, de):
    _small_run(fx, builds, "de", 9, BUILT(16))


def _a_label_added(fx, builds, fi, de):
    _small_run(fx, builds, "se", 5, BUILT(16))


def _a_skipped_label_now_valid(fx, builds, fi, de):
    _small_run(fx, builds, "nl", 4, BUILT(16))


def _a_label_gone(fx, builds, fi, de):
    shutil.rmtree(builds / fi)


@pytest.mark.parametrize(
    "change, message",
    [
        (_a_source_manifest_changed, "snapshot.json changed since the merge"),
        (_a_source_notice_changed, "NOTICE changed since the merge"),
        (_a_label_with_a_newer_run, "labels with another newest run: de"),
        (_a_label_added, "labels not merged: se"),
        (_a_skipped_label_now_valid, "labels not merged: nl"),
        (_a_label_gone, "labels no longer archived: fi"),
    ],
)
def test_a_merged_snapshot_whose_lineage_moved_is_refused(tmp_path, change, message):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import publisher

    builds, cache, fi, de, _ = _merged_cache(fx, tmp_path)
    change(fx, builds, fi, de)
    with pytest.raises(publisher.PublishIndexError, match=message):
        publisher.pack(cache / "index", cache_dir=cache, builds_dir=builds)


def test_a_manifest_recording_both_lineages_is_refused(tmp_path):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import publisher

    builds, cache, fi, de, _ = _merged_cache(fx, tmp_path)
    _rewrite_snapshot(
        cache / "index",
        lambda s: s.update(
            generations={"crosswalk/latest": "gen-0"},
            leaves={"feeds": "crosswalk/latest"},
        ),
    )
    with pytest.raises(publisher.PublishIndexError, match="one index is one"):
        publisher.pack(cache / "index", cache_dir=cache, builds_dir=builds)


def test_a_manifest_with_an_empty_stage_lineage_beside_a_merge_is_refused(tmp_path):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import publisher

    builds, cache, fi, de, _ = _merged_cache(fx, tmp_path)
    _rewrite_snapshot(cache / "index", lambda s: s.update(generations={}, leaves={}))
    with pytest.raises(publisher.PublishIndexError, match="one index is one"):
        publisher.pack(cache / "index", cache_dir=cache, builds_dir=builds)


def test_a_merged_manifest_recording_a_label_twice_is_refused(tmp_path):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import publisher

    builds, cache, fi, de, _ = _merged_cache(fx, tmp_path)
    _rewrite_snapshot(
        cache / "index",
        lambda s: s.__setitem__("merged", [s["merged"][0], s["merged"][0]]),
    )
    with pytest.raises(publisher.PublishIndexError, match="records a label twice"):
        publisher.pack(cache / "index", cache_dir=cache, builds_dir=builds)


def test_a_builds_directory_that_cannot_be_read_is_a_publish_error(
    tmp_path, monkeypatch
):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import merge as merge_module, publisher

    builds, cache, fi, de, _ = _merged_cache(fx, tmp_path)

    def failing(builds_dir):
        raise OSError("permission denied")

    monkeypatch.setattr(merge_module, "select_sources", failing)
    with pytest.raises(
        publisher.PublishIndexError, match="cannot read the archived builds"
    ):
        publisher.pack(cache / "index", cache_dir=cache, builds_dir=builds)


def _a_feed_lost(partition):
    partition["atlas"]["lost"] = "fi"


def _de_not_listed(partition):
    del partition["labels"]["de"]
    partition["atlas"] = {"hsl": "fi", "nat": "fi"}


def _de_cut_again(partition):
    partition["labels"]["de"]["atlas_sha256"] = "9" * 64


EXCEPTED = "- feed: lost\n  reason: withdrawn by its operator\n"
CURATED = (
    "- feed: f-curated-lost\n  add_feed: {name: Lost, url: 'https://lost.example/',"
    " spec: gtfs, license: {url: 'https://lost.example/'}, location: {country_code: ZW}}\n"
)


@pytest.mark.parametrize(
    "partition, feeds, exceptions, message",
    [
        (False, None, None, "no catalogue check; re-run the merge with --partition"),
        (_a_feed_lost, None, None, r"1 catalogue feeds missing .*: lost \(fi\)$"),
        (_a_feed_lost, None, EXCEPTED, None),
        (
            None,
            CURATED,
            None,
            r"1 catalogue feeds missing .*: f-curated-lost \(no label\)$",
        ),
        (None, CURATED, EXCEPTED.replace("lost", "f-curated-lost"), None),
        (_de_not_listed, None, None, "merges labels outside the partition: de"),
        (_de_cut_again, None, None, "merges labels built from another cut: de"),
    ],
)
def test_a_merged_snapshot_is_released_only_with_its_whole_catalogue_cut(
    tmp_path, partition, feeds, exceptions, message
):
    fx = pytest.importorskip("index_fixture")
    from transitio_index import publisher

    excepted = tmp_path / "overrides" / "catalogue_exceptions.yaml"
    excepted.parent.mkdir()
    if feeds is not None:
        (excepted.parent / "feeds.yaml").write_text(feeds)
    builds, cache, *_ = _merged_cache(fx, tmp_path, partition, excepted.parent)
    if exceptions is not None:
        excepted.write_text(exceptions)
    options = {"builds_dir": builds, "overrides_dir": excepted.parent}
    if message is not None:
        with pytest.raises(publisher.PublishIndexError, match=message):
            publisher.pack(cache / "index", **options)
        return
    _, release = publisher.pack(cache / "index", **options)
    # The flip-time check reads the exceptions and the add_feed entries again.
    excepted.unlink()
    with pytest.raises(publisher.PublishIndexError, match=r"lost \("):
        publisher._check_lineage(cache, release["lineage"], excepted.parent, builds)
    (excepted.parent / "feeds.yaml").write_text(CURATED.replace("lost", "new"))
    with pytest.raises(publisher.PublishIndexError, match="other add_feed entries"):
        publisher._check_lineage(cache, release["lineage"], excepted.parent, builds)


CHECKED = {
    "partition_sha256": "0" * 64,
    "curated_sha256": None,
    "catalogues": {},
    "expected": {"mdb": 0, "atlas": 1, "curated": 0},
    "missing": [],
    "labels_not_merged": [],
    "labels_outside": [],
    "labels_from_another_cut": [],
}


@pytest.mark.parametrize(
    "check",
    [
        {key: [] for key in ("missing", "labels_outside", "labels_from_another_cut")},
        {**CHECKED, "partition_sha256": "p" * 64},
        {**CHECKED, "expected": {"mdb": 0}},
        {**CHECKED, "labels_not_merged": None},
        {**CHECKED, "missing": [{"id": "lost", "label": "fi"}]},
        {**CHECKED, "missing": [{"catalogue": "mdb", "id": "lost", "label": None}]},
        {key: value for key, value in CHECKED.items() if key != "curated_sha256"},
    ],
)
def test_a_catalogue_check_not_shaped_as_the_merge_writes_it_is_refused(check):
    from transitio_index import publisher

    publisher._catalogue_complete({"catalogue_check": CHECKED}, None)
    with pytest.raises(publisher.PublishIndexError, match="no catalogue check"):
        publisher._catalogue_complete({"catalogue_check": check}, None)


@pytest.mark.parametrize(
    "text, message",
    [
        ("feed: lost\n", "expected a list"),
        ("- feed: lost\n", "'lost' needs a reason"),
        ("- {feed: lost, reason: ' '}\n", "'lost' needs a reason"),
        ("- {feed: lost, reason: a}\n- {feed: lost, reason: b}\n", "duplicate"),
        ("- {feed: lost, reason: a, set_identity: {}}\n", "unknown keys"),
    ],
)
def test_a_catalogue_exceptions_file_without_a_reason_per_id_is_refused(
    tmp_path, text, message
):
    from transitio_index import overrides

    (tmp_path / "catalogue_exceptions.yaml").write_text(text)
    with pytest.raises(overrides.OverrideError, match=message):
        overrides.load_catalogue_exceptions(tmp_path)
