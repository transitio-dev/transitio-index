"""Tests for the index viewer's data layer: loading, the build cache, slices and the API.

A publish-shaped build is written into a temp cache with real parquet and a
snapshot whose digests match, then tampered with per case.
"""

import datetime
import hashlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import threading
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "index_viewer.py"
_spec = importlib.util.spec_from_file_location("index_viewer", _SCRIPT)
iv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(iv)

BOX = shapely.box


def _place(place_id, kind, name, parent_id, geom, country="FI"):
    return {
        "place_id": place_id,
        "kind": kind,
        "name": name,
        "parent_id": parent_id,
        "country_code": country,
        "service": json.dumps({"feeds": 1}),
        "geometry": shapely.to_wkb(geom),
    }


# A country, two regions and three cities; only Helsinki is served, by feed f1.
PLACES = [
    _place("fi", "country", "Finland", None, BOX(19, 59, 32, 71)),
    _place("uus", "region", "Uusimaa", "fi", BOX(23, 59.8, 26.5, 60.9)),
    _place("lap", "region", "Lapland", "fi", BOX(20, 66, 30, 70)),
    _place("hel", "city", "Helsinki", "uus", BOX(24.8, 60.1, 25.3, 60.35)),
    _place("esp", "city", "Espoo", "uus", BOX(24.4, 60.1, 24.9, 60.3)),
    _place("rov", "city", "Rovaniemi", "lap", BOX(25.5, 66.4, 26.0, 66.6)),
]
EDGES = [{"place_id": "hel", "feed_id": "f1", "tier": "local"}]
FEEDS = [{"feed_id": "f1", "name": "HSL", "coverage": None}]


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def write_build(path, places=PLACES, edges=EDGES, feeds=FEEDS, notice=b"NOTICE\n"):
    """A publish-shaped build directory with digests that match its files."""
    path.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "built_at": "2026-09-11T00:00:00+00:00",
        "counts": {"places": len(places)},
    }
    tables = {
        "places.parquet": pa.Table.from_pylist(places),
        "edges.parquet": pa.Table.from_pylist(edges),
        "feeds.parquet": pa.Table.from_pylist(feeds),
    }
    for name, table in tables.items():
        sink = io.BytesIO()
        pq.write_table(table, sink)
        (path / name).write_bytes(sink.getvalue())
        snapshot[iv.DIGEST_KEYS[name]] = _sha(sink.getvalue())
    (path / "NOTICE").write_bytes(notice)
    snapshot[iv.DIGEST_KEYS["NOTICE"]] = _sha(notice)
    (path / "snapshot.json").write_text(json.dumps(snapshot))
    return snapshot


def _tamper_places(path):
    (path / "places.parquet").write_bytes(b"not the bytes the snapshot hashed")


def _drop_notice(path):
    (path / "NOTICE").unlink()


def _drop_digest(path):
    snapshot = json.loads((path / "snapshot.json").read_text())
    del snapshot["edges_sha256"]
    (path / "snapshot.json").write_text(json.dumps(snapshot))


def _replace_places(path, table):
    # Rewrites places.parquet AND its digest: the build verifies but is
    # not publish-shaped, so the schema/geometry guard must refuse it.
    sink = io.BytesIO()
    pq.write_table(table, sink)
    (path / "places.parquet").write_bytes(sink.getvalue())
    snapshot = json.loads((path / "snapshot.json").read_text())
    snapshot["places_sha256"] = _sha(sink.getvalue())
    (path / "snapshot.json").write_text(json.dumps(snapshot))


def _drop_geometry_column(path):
    _replace_places(path, pq.read_table(path / "places.parquet").drop(["geometry"]))


def _duplicate_geometry_column(path):
    table = pq.read_table(path / "places.parquet")
    _replace_places(path, table.append_column("geometry", table["geometry"]))


def _corrupt_wkb(path):
    _replace_places(
        path, pa.Table.from_pylist([dict(p, geometry=b"not wkb") for p in PLACES])
    )


def _garbage_parquet(path):
    # Verifies (the digest is updated) but is not parquet at all: the parser,
    # not the digest check, has to refuse it.
    data = b"PAR1 but no parquet footer follows"
    (path / "places.parquet").write_bytes(data)
    snapshot = json.loads((path / "snapshot.json").read_text())
    snapshot["places_sha256"] = _sha(data)
    (path / "snapshot.json").write_text(json.dumps(snapshot))


DEEP_JSON = "[" * 100_000 + "]" * 100_000  # legal JSON past the recursion limit


def _nested_snapshot(path):
    (path / "snapshot.json").write_text(DEEP_JSON)


def _notice_is_a_fifo(path):
    # Not a regular file: the open must not block on it, and the type check
    # must refuse it.
    if not hasattr(os, "mkfifo"):
        pytest.skip("no FIFOs on this platform")
    (path / "NOTICE").unlink()
    os.mkfifo(path / "NOTICE")


@pytest.mark.parametrize(
    ("tamper", "loads"),
    [
        (None, True),
        (_tamper_places, False),
        (_drop_notice, False),
        (_drop_digest, False),
        (_drop_geometry_column, False),
        (_duplicate_geometry_column, False),
        (_corrupt_wkb, False),
        (_garbage_parquet, False),
        (_nested_snapshot, False),
        (_notice_is_a_fifo, False),
    ],
    ids=[
        "intact",
        "tampered-parquet",
        "missing-notice",
        "missing-digest",
        "missing-column",
        "duplicate-column",
        "corrupt-wkb",
        "unreadable-parquet",
        "nested-snapshot",
        "notice-is-a-fifo",
    ],
)
def test_a_build_loads_only_when_every_file_verifies(tmp_path, tamper, loads):
    write_build(tmp_path)
    if tamper:
        tamper(tmp_path)
    build = iv.load_build("b", tmp_path)
    assert (build is not None) is loads
    if loads:
        assert len(build.places) == 6 and build.served.sum() == 1
        assert build.feed_count[list(build.places["place_id"]).index("hel")] == 1


def test_a_file_swapped_between_reads_is_refused_and_not_cached(tmp_path):
    # The snapshot describes generation A; a republish lands generation B's
    # places before that file is read. Hashing the bytes that are parsed makes
    # the swap a mismatch, so nothing mixed is cached.
    cache = tmp_path / "cache"
    write_build(cache / "index")
    other = tmp_path / "other"
    write_build(other, places=PLACES[:2])

    def swapped(path):
        path = Path(path)
        if path.name == "places.parquet":
            return (other / "places.parquet").read_bytes()
        return path.read_bytes()

    builds = iv.BuildCache(cache, read_bytes=swapped)
    assert builds.get(iv.LATEST) is None
    assert builds._builds == {}


def _symlink(target, link):
    try:
        os.symlink(target, link)
    except (OSError, NotImplementedError):
        return False
    return True


def test_discovery_reserves_latest_and_skips_what_is_not_a_real_build(
    tmp_path, request
):
    cache = tmp_path / "cache"
    write_build(cache / "index")
    write_build(cache / "builds" / "fi-abc" / "index")
    write_build(cache / "builds" / "latest" / "index", places=PLACES[:2])
    (cache / "builds" / "half").mkdir(parents=True)  # no index: not a build
    # Snapshots that are not a JSON object, or do not parse at all: listed as
    # incomplete, never opened.
    for name, snapshot in (("null", "null"), ("deep", DEEP_JSON)):
        (cache / "builds" / name / "index").mkdir(parents=True)
        (cache / "builds" / name / "index" / "snapshot.json").write_text(snapshot)
    write_build(tmp_path / "outside" / "index")  # a build outside the cache
    linked = _symlink(tmp_path / "outside", cache / "builds" / "link")
    locked = cache / "builds" / "locked"  # cannot be inspected: not a build
    locked.mkdir()
    if getattr(os, "geteuid", lambda: 1)() != 0:
        locked.chmod(0)
        request.addfinalizer(lambda: locked.chmod(0o700))
    found = iv.discover(cache)
    assert found[iv.LATEST] == cache / "index"  # the reserved name: cache/index wins
    assert list(found) == [iv.LATEST, "deep", "fi-abc", "null"]  # no latest child/link
    builds = iv.BuildCache(cache)
    rows = {row["id"]: row for row in builds.summaries()}
    assert rows["fi-abc"]["complete"]
    assert not rows["null"]["complete"] and not rows["deep"]["complete"]
    assert builds.get("null") is None and builds.get("deep") is None
    assert builds._builds == {}  # nothing unavailable is cached
    if linked:
        # A symlinked file is refused too, not just a symlinked directory.
        real = cache / "builds" / "fi-abc" / "index" / "NOTICE"
        real.rename(cache / "elsewhere")
        assert _symlink(cache / "elsewhere", real)
        assert iv.load_build("fi-abc", real.parent) is None
        assert not iv.describe("fi-abc", real.parent)["complete"]


def test_the_cache_reloads_on_digest_change_and_evicts_a_vanished_build(tmp_path):
    cache = tmp_path / "cache"
    write_build(cache / "index")
    write_build(cache / "builds" / "fi-abc" / "index")
    builds = iv.BuildCache(cache)
    country = builds.get("fi-abc")
    assert builds.get("fi-abc") is country  # unchanged: served from the cache
    first = builds.get(iv.LATEST)
    write_build(cache / "index", places=PLACES[:3])  # a republish
    second = builds.get(iv.LATEST)
    assert second is not first and len(second.places) == 3
    shutil.rmtree(cache / "builds" / "fi-abc")  # the build vanishes
    assert builds.get("fi-abc") is None and "fi-abc" not in builds._builds
    assert builds.get("nope") is None


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ({}, {"fi", "uus", "lap"}),  # default kinds: countries and regions
        ({"kinds": None, "bbox": (24, 60, 25, 60.5)}, {"fi", "uus", "hel", "esp"}),
        ({"kinds": {"city"}, "parent_id": "uus"}, {"hel", "esp"}),
        ({"served": True}, {"fi", "uus", "lap"} & set()),  # no served region
        ({"kinds": None, "bbox": (19, 59, 32, 71), "served": True}, {"hel"}),
        ({"q": "lap"}, {"lap"}),
        ({"kinds": None, "bbox": (19, 59, 32, 71), "feed_id": "f1"}, {"hel"}),
        ({"kinds": None}, ValueError),  # cities without a bound
        ({"kinds": {"city"}}, ValueError),
    ],
    ids=[
        "default",
        "all-in-bbox",
        "cities-of-region",
        "served-regions",
        "served-anywhere",
        "name-search",
        "feed-serves-a-city",
        "unbounded-all",
        "unbounded-cities",
    ],
)
def test_filter_places(tmp_path, query, expected):
    build = iv.load_build("b", write_build(tmp_path) and tmp_path)
    if expected is ValueError:
        with pytest.raises(ValueError, match="parent_id or bbox"):
            iv.filter_places(build, **query)
        return
    mask = iv.filter_places(build, **query)
    assert set(build.places["place_id"][mask]) == expected


def test_slices_are_capped_by_count_then_bytes(tmp_path):
    build = iv.load_build("b", write_build(tmp_path) and tmp_path)
    mask = iv.filter_places(build)  # 3 features
    body, overflow = iv.places_geojson(build, mask, max_features=2)
    assert body is None and overflow == {"overflow": True, "matched": 3, "limit": 2}
    body, overflow = iv.places_geojson(build, mask, max_bytes=50)
    assert body is None and overflow["bytes"] > 50 and overflow["byte_limit"] == 50
    body, overflow = iv.places_geojson(build, mask)
    assert overflow is None
    collection = json.loads(body)
    assert (
        collection["type"] == "FeatureCollection" and len(collection["features"]) == 3
    )
    feature = next(f for f in collection["features"] if f["id"] == "uus")
    assert feature["properties"] == {
        "place_id": "uus",
        "name": "Uusimaa",
        "kind": "region",
        "parent_id": "fi",
        "country_code": "FI",
        "service": '{"feeds": 1}',
        "tier": None,  # unserved: no class (``category`` on schema 7)
        "feed_count": 0,
        "served": False,
    }
    assert feature["geometry"]["type"] == "Polygon"


@pytest.mark.parametrize(
    ("zoom", "tolerance"), [(None, None), (5, 0.02), (6, 0.02), (8, 0.005), (12, None)]
)
def test_tolerance_for_zoom(zoom, tolerance):
    assert iv.tolerance_for_zoom(zoom) == tolerance


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("19,59,32,71", (19.0, 59.0, 32.0, 71.0)),
        ("1,2,3", ValueError),  # three numbers
        ("nan,59,32,71", ValueError),  # not finite
        ("19,59,-inf,71", ValueError),
        ("32,59,19,71", ValueError),  # west past east
        ("19,71,32,59", ValueError),  # south past north
    ],
)
def test_parse_bbox(value, expected):
    if expected is ValueError:
        with pytest.raises(ValueError, match="bbox needs"):
            iv.parse_bbox(value)
    else:
        assert iv.parse_bbox(value) == expected


def test_generalization_shrinks_a_slice_and_keeps_a_tiny_place(tmp_path):
    dense = shapely.Point(25, 65).buffer(2, quad_segs=200)  # ~800 vertices
    tiny = BOX(20, 60, 20.001, 60.001)  # smaller than the coarse tolerance
    places = PLACES[:1] + [
        _place("dense", "region", "Dense", "fi", dense),
        _place("tiny", "region", "Tiny", "fi", tiny),
    ]
    build = iv.load_build("b", write_build(tmp_path, places=places) and tmp_path)
    mask = iv.filter_places(build, kinds={"region"})
    stored, _ = iv.places_geojson(build, mask)
    coarse, _ = iv.places_geojson(build, mask, tolerance=0.02)
    assert len(coarse) < len(stored) / 4
    features = {f["id"]: f for f in json.loads(coarse)["features"]}
    # Plain Douglas-Peucker would collapse the tiny box to nothing; the
    # topology-preserving fallback keeps a ring.
    assert features["tiny"]["geometry"]["type"] == "Polygon"
    assert len(features["tiny"]["geometry"]["coordinates"][0]) >= 4


def test_the_api_serves_the_listing_summaries_and_bounded_slices(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    cache = tmp_path / "cache"
    write_build(cache / "index")
    write_build(cache / "builds" / "fi-abc" / "index")
    _tamper_places(cache / "builds" / "fi-abc" / "index")
    client = TestClient(iv.create_app(cache))
    page = client.get("/")
    assert page.headers["content-type"].startswith("text/html")
    assert '<script type="module" src="/static/index_viewer.mjs">' in page.text
    module = client.get("/static/index_viewer.mjs")
    assert module.headers["content-type"].startswith("text/javascript")
    assert module.text.startswith("//")
    assert [row["id"] for row in client.get("/api/builds").json()] == [
        iv.LATEST,
        "fi-abc",
    ]
    summary = client.get(f"/api/builds/{iv.LATEST}/summary").json()
    assert summary["counts"] == {"places": 6} and summary["served_places"] == 1
    assert summary["built_at"] == "2026-09-11T00:00:00+00:00"
    assert client.get("/api/builds/fi-abc/summary").status_code == 404  # tampered
    assert client.get("/api/builds/nope/summary").status_code == 404
    base = f"/api/builds/{iv.LATEST}/places"
    overview = client.get(base)
    assert overview.headers["content-type"].startswith("application/geo+json")
    assert overview.headers["x-snapshot"] == summary["snapshot_id"]
    # The snapshot id covers every file: new edges over identical places
    # change it, so a republish can never pass as the same snapshot.
    other_edges = [{"place_id": "esp", "feed_id": "f2", "tier": "local"}]
    write_build(cache / "builds" / "fi-edges" / "index", edges=other_edges)
    other = client.get("/api/builds/fi-edges/summary").json()
    assert other["places_sha256"] == summary["places_sha256"]
    assert other["snapshot_id"] != summary["snapshot_id"]
    assert {f["id"] for f in overview.json()["features"]} == {"fi", "uus", "lap"}
    cities = client.get(base, params={"kind": "city", "parent_id": "uus"}).json()
    assert {f["id"] for f in cities["features"]} == {"hel", "esp"}
    fed = client.get(base, params={"feed_id": "f1", "bbox": "19,59,32,71"}).json()
    assert [f["id"] for f in fed["features"]] == ["hel"]  # kind defaulted to all
    assert fed["features"][0]["properties"]["tier"] == "local"  # this feed's tier
    clipped = client.get(base, params={"bbox": "24,60,25,60.5"}).json()["features"]
    assert {f["id"] for f in clipped} == {"fi", "uus"}  # cut to the box
    assert all(
        24 <= x <= 25 and 60 <= y <= 60.5
        for f in clipped
        for x, y in f["geometry"]["coordinates"][0]
    )
    table = client.get(f"/api/builds/{iv.LATEST}/places/table")
    assert table.status_code == 200 and table.json()["total"] == 6
    assert table.headers["x-snapshot"] == summary["snapshot_id"]  # pages, one snapshot
    page = client.get(
        f"/api/builds/{iv.LATEST}/places/table",
        params={"sort": "feed_count", "order": "desc", "limit": 999},
    ).json()
    assert page["rows"][0]["place_id"] == "hel" and page["limit"] == 200
    assert (
        client.get(f"/api/builds/{iv.LATEST}/places/table?sort=nope").status_code == 400
    )
    record = client.get(f"/api/builds/{iv.LATEST}/places/hel")
    assert record.headers["content-type"].startswith("application/geo+json")
    assert record.headers["x-snapshot"] == summary["snapshot_id"]
    assert [a["place_id"] for a in record.json()["properties"]["ancestors"]] == [
        "fi",
        "uus",
    ]
    assert client.get(f"/api/builds/{iv.LATEST}/places/nope").status_code == 404
    tree = client.get(f"/api/builds/{iv.LATEST}/tree", params={"depth": 2}).json()
    assert [c["place_id"] for c in tree["nodes"][0]["children"]] == ["lap", "uus"]
    assert client.get(f"/api/builds/{iv.LATEST}/tree?root=nope").status_code == 404
    assert client.get(f"/api/builds/{iv.LATEST}/tree?depth=x").status_code == 400
    roots = client.get(f"/api/builds/{iv.LATEST}/tree")
    assert roots.headers["x-snapshot"] == summary["snapshot_id"]
    feeds_reply = client.get(f"/api/builds/{iv.LATEST}/feeds")
    feeds = feeds_reply.json()
    assert feeds["total"] == 1 and feeds["rows"][0]["feed_id"] == "f1"
    feed = client.get(f"/api/builds/{iv.LATEST}/feeds/f1")
    assert feed.headers["content-type"].startswith("application/geo+json")
    assert feed.json()["properties"]["places_served"] == 1
    assert client.get(f"/api/builds/{iv.LATEST}/feeds/nope").status_code == 404
    edges = client.get(f"/api/builds/{iv.LATEST}/edges", params={"feed_id": "f1"})
    assert [e["place_name"] for e in edges.json()["rows"]] == ["Helsinki"]
    for reply in (feeds_reply, feed, edges):  # one snapshot, like every route
        assert reply.headers["x-snapshot"] == summary["snapshot_id"]
    assert client.get(f"/api/builds/{iv.LATEST}/edges").status_code == 400
    both = {"place_id": "hel", "feed_id": "f1"}
    assert client.get(f"/api/builds/{iv.LATEST}/edges", params=both).status_code == 400
    for bad in ({"kind": "city"}, {"bbox": "1,2,3"}, {"served": "maybe"}):
        assert client.get(base, params=bad).status_code == 400, bad
    monkeypatch.setattr(iv, "MAX_FEATURES", 2)
    overflow = client.get(base)  # the answer, not an error: 200 with the record
    assert overflow.status_code == 200
    assert overflow.json() == {"overflow": True, "matched": 3, "limit": 2}


def test_a_clipped_slice_keeps_only_what_lies_inside_the_box(tmp_path):
    triangle = shapely.Polygon([(26, 66), (30, 66), (26, 70)])
    places = PLACES[:1] + [_place("tri", "region", "Triangle", "fi", triangle)]
    build = iv.load_build("b", write_build(tmp_path, places=places) and tmp_path)
    box = (29, 69, 30, 70)  # touches the triangle's bounds, not the triangle
    mask = iv.filter_places(build, bbox=box)
    assert set(build.places["place_id"][mask]) == {"fi", "tri"}
    body, _ = iv.places_geojson(build, mask, clip=box)
    features = json.loads(body)["features"]
    assert [f["id"] for f in features] == ["fi"]  # nothing of the triangle shows
    assert set(map(tuple, features[0]["geometry"]["coordinates"][0])) == {
        (29.0, 69.0),
        (30.0, 69.0),
        (30.0, 70.0),
        (29.0, 70.0),
    }


def test_the_cache_serves_concurrent_requests_without_losing_entries(tmp_path):
    cache = tmp_path / "cache"
    write_build(cache / "index")
    write_build(cache / "builds" / "fi-abc" / "index")
    builds = iv.BuildCache(cache, size=1)  # every other get evicts the other id
    failures = []

    def worker(build_id):
        try:
            for _ in range(20):
                assert builds.get(build_id).id == build_id
        except Exception as error:  # collected: a thread's failure must surface
            failures.append(error)

    threads = [
        threading.Thread(target=worker, args=(build_id,))
        for build_id in (iv.LATEST, "fi-abc") * 4
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == [] and len(builds._builds) == 1


def test_the_browser_url_and_the_wait_for_this_server_to_start():
    assert iv._browser_url("127.0.0.1", 8765) == "http://127.0.0.1:8765/"
    assert iv._browser_url("localhost", 1) == "http://localhost:1/"
    assert iv._browser_url("0.0.0.0", 1) == "http://127.0.0.1:1/"  # wildcard
    assert iv._browser_url("::", 1) == "http://127.0.0.1:1/"
    assert iv._browser_url("::1", 1) == "http://[::1]:1/"  # bracketed

    class Later:  # reports started on the third poll
        should_exit = False
        polls = 0

        @property
        def started(self):
            self.polls += 1
            return self.polls >= 3

    class Failed:  # the bind failed: uvicorn asks to exit
        started = False
        should_exit = True

    class Never:
        started = False
        should_exit = False

    opened = []
    assert iv._open_when_started(Later(), "http://x/", opener=opened.append, delay=0)
    assert not iv._open_when_started(
        Failed(), "http://x/", opener=opened.append, delay=0
    )
    assert not iv._open_when_started(
        Never(), "http://x/", opener=opened.append, attempts=2, delay=0
    )
    assert opened == ["http://x/"]  # once, and never for a server that is not up


def test_the_page_module_parses_as_esm_and_its_unit_tests_pass():
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    root = _SCRIPT.parent.parent
    subprocess.run([node, "--check", str(_SCRIPT.with_suffix(".mjs"))], check=True)
    result = subprocess.run(
        [node, "--test", "--test-reporter=tap", "tests/index_viewer_client.test.mjs"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_places_table_is_sorted_paged_and_geometry_free(tmp_path):
    build = iv.load_build("b", write_build(tmp_path) and tmp_path)
    everything = iv.filter_places(build, kinds=None, bounded=False)
    page = iv.places_table(build, everything)
    assert page["total"] == 6 and [r["name"] for r in page["rows"]][:2] == [
        "Espoo",
        "Finland",
    ]
    assert set(page["rows"][0]) == {
        "place_id",
        "name",
        "kind",
        "parent_id",
        "parent_name",
        "country_code",
        "served",
        "feed_count",
        "tier",
        "stops",
        "routes",
        "departures_per_day",
    }
    busiest = iv.places_table(build, everything, sort="feed_count", order="desc")
    assert busiest["rows"][0]["place_id"] == "hel" and busiest["rows"][0]["served"]
    assert busiest["rows"][0]["tier"] == "local"  # the highest class per place
    assert busiest["rows"][0]["parent_name"] == "Uusimaa"
    assert busiest["rows"][0]["stops"] is None  # the fixture's service has none
    paged = iv.places_table(build, everything, limit=2, offset=2)
    assert paged["total"] == 6 and [r["name"] for r in paged["rows"]] == [
        "Helsinki",
        "Lapland",
    ]
    served = iv.places_table(
        build, iv.filter_places(build, kinds=None, served=True, bounded=False)
    )
    assert [r["place_id"] for r in served["rows"]] == ["hel"]
    with pytest.raises(ValueError, match="sort must be"):
        iv.places_table(build, everything, sort="geometry")


def test_a_place_record_carries_ancestors_children_and_feeds(tmp_path):
    build = iv.load_build("b", write_build(tmp_path) and tmp_path)
    feature = json.loads(iv.place_record(build, "hel"))
    props = feature["properties"]
    assert feature["type"] == "Feature" and feature["id"] == "hel"
    stored = shapely.from_geojson(json.dumps(feature["geometry"]))
    assert stored.equals(BOX(24.8, 60.1, 25.3, 60.35))  # the stored box, as is
    assert [a["place_id"] for a in props["ancestors"]] == ["fi", "uus"]
    assert props["served"] and props["feed_count"] == 1
    assert props["bbox"] == [24.8, 60.1, 25.3, 60.35]
    assert props["edges"] == [{"feed_id": "f1", "tier": "local", "feed_name": "HSL"}]
    assert props["service"] == {"feeds": 1}
    region = json.loads(iv.place_record(build, "uus"))["properties"]
    assert region["children"] == {"count": 2, "by_kind": {"city": 2}, "served": 1}
    assert iv.place_record(build, "nope") is None
    # Arrow maps arrive as lists of pairs, empty or not; the record makes dicts.
    assert iv._as_map([("en", "Helsinki"), ("fi", "Helsinki")]) == {
        "en": "Helsinki",
        "fi": "Helsinki",
    }
    assert iv._as_map([]) == {} and iv._as_map(None) is None
    # Every missing scalar pandas can hand back becomes null, not a 500.
    assert [iv._cell(v) for v in (pd.NA, pd.NaT, float("nan"), np.float64("inf"))] == [
        None,
        None,
        None,
        None,
    ]
    assert iv._cell(np.int64(3)) == 3 and iv._cell(np.array(["a", "b"])) == ["a", "b"]


def test_non_finite_service_numbers_never_reach_the_json(tmp_path):
    # ``1e400`` is legal JSON that parses to infinity; it must not break the
    # table's serialization or leak as a non-standard token in the record.
    odd = dict(_place("odd", "region", "Odd", "fi", BOX(0, 0, 1, 1)))
    odd["service"] = '{"stops": 1e400, "routes": -1e400, "feeds": 2}'
    places = PLACES[:1] + [odd]
    build = iv.load_build("b", write_build(tmp_path, places=places) and tmp_path)
    row = iv.places_table(build, iv.filter_places(build, kinds={"region"}))["rows"][0]
    assert row["stops"] is None and row["routes"] is None
    record = json.loads(iv.place_record(build, "odd"))
    assert record["properties"]["service"] == {
        "stops": None,
        "routes": None,
        "feeds": 2,
    }
    # Service JSON is parsed once at load: a malformed edge service makes the
    # build unavailable instead of failing a later request.
    broken = tmp_path / "broken"
    write_build(broken, edges=[EDGES[0] | {"service": "{not json"}])
    assert iv.load_build("b", broken) is None


def test_the_tree_lists_roots_and_nests_children_by_level(tmp_path):
    # A region whose parent is not in the build is a root too.
    orphan = _place("orphan", "region", "Orphan", "gone", BOX(0, 0, 1, 1))
    build = iv.load_build(
        "b", write_build(tmp_path, places=PLACES + [orphan]) and tmp_path
    )
    roots = iv.tree_nodes(build)
    assert [n["place_id"] for n in roots["nodes"]] == ["fi", "orphan"]  # kind rank
    assert roots["nodes"][0]["child_count"] == 2 and "children" not in roots["nodes"][0]
    assert roots["depth"] == 1 and not roots["truncated"]
    nested = iv.tree_nodes(build, depth=9)  # clamped to three levels
    finland = nested["nodes"][0]
    assert nested["depth"] == 3
    assert [c["place_id"] for c in finland["children"]] == ["lap", "uus"]
    uusimaa = finland["children"][1]
    assert [(c["place_id"], c["served"]) for c in uusimaa["children"]] == [
        ("esp", False),
        ("hel", True),
    ]
    assert iv.tree_nodes(build, root="uus")["nodes"] == uusimaa["children"]
    assert iv.tree_nodes(build, root="hel")["nodes"] == []
    assert iv.tree_nodes(build, root="nope") is None


def test_feeds_have_a_table_a_record_with_a_hull_and_edges_both_ways(
    tmp_path, monkeypatch
):
    hull = shapely.to_wkb(BOX(24, 60, 26, 61))
    crawled = datetime.datetime(2026, 9, 10, 12, 0, tzinfo=datetime.timezone.utc)
    feeds = [  # a timestamp column, as parquet may carry: it must serialize
        FEEDS[0] | {"coverage": hull, "last_crawled": crawled},
        {"feed_id": "f2", "name": None, "coverage": None, "last_crawled": None},
    ]
    # f1 serves two places over three edges: Helsinki twice (local first).
    edges = EDGES + [
        {"place_id": "esp", "feed_id": "f1", "tier": "regional"},
        {"place_id": "hel", "feed_id": "f1", "tier": "regional"},
    ]
    path = write_build(tmp_path, feeds=feeds, edges=edges) and tmp_path
    build = iv.load_build("b", path)
    table = iv.feeds_table(build)
    assert table["total"] == 2
    assert table["rows"][0] == {
        "feed_id": "f1",
        "name": "HSL",
        "has_coverage": True,
        "places_served": 2,
        "realtime": 0,
        "tier_local": 1,
        "tier_regional": 2,
        "tier_national": 0,
        "tier_international": 0,
        "tier_unknown": 0,
    }
    assert (
        table["rows"][1]["places_served"] == 0 and not table["rows"][1]["has_coverage"]
    )
    record = json.loads(iv.feed_record(build, "f1"))
    assert record["id"] == "f1" and record["geometry"]["type"] == "Polygon"
    props = record["properties"]
    # One row per place (the first edge in table order), not one per edge.
    assert props["places"] == [
        {"place_id": "hel", "name": "Helsinki", "kind": "city", "tier": "local"},
        {"place_id": "esp", "name": "Espoo", "kind": "city", "tier": "regional"},
    ]
    assert props["places_total"] == 2 and not props["places_truncated"]
    assert props["places_served"] == 2
    assert props["last_crawled"].startswith("2026-09-10T12:00:00")  # ISO, not a 500
    assert json.loads(iv.feed_record(build, "f2"))["properties"]["last_crawled"] is None
    assert props["tiers"] == {  # tiers count edges
        "local": 1,
        "regional": 2,
        "national": 0,
        "international": 0,
        "unknown": 0,
    }
    assert json.loads(iv.feed_record(build, "f2"))["geometry"] is None
    assert iv.feed_record(build, "nope") is None
    by_place = iv.edges_of(build, place_id="hel")
    assert by_place["total"] == 2 and not by_place["truncated"]
    assert by_place["rows"][0] == {
        "place_id": "hel",
        "feed_id": "f1",
        "tier": "local",
        "place_name": "Helsinki",
        "kind": "city",
        "feed_name": "HSL",
    }
    by_feed = iv.edges_of(build, feed_id="f1")
    assert [r["place_id"] for r in by_feed["rows"]] == ["hel", "esp", "hel"]
    assert iv.edges_of(build, feed_id="f2") == {
        "total": 0,
        "truncated": False,
        "rows": [],
    }
    for bad in ({}, {"place_id": "hel", "feed_id": "f1"}):
        with pytest.raises(ValueError, match="exactly one"):
            iv.edges_of(build, **bad)
    # Both lists are bounded: the totals stay true, the rows stop at the limit.
    monkeypatch.setattr(iv, "FEED_PLACES_LIMIT", 1)
    monkeypatch.setattr(iv, "EDGES_LIMIT", 1)
    cut = json.loads(iv.feed_record(build, "f1"))["properties"]
    assert (
        len(cut["places"]) == 1 and cut["places_total"] == 2 and cut["places_truncated"]
    )
    cut_edges = iv.edges_of(build, feed_id="f1")
    assert (
        cut_edges["total"] == 3
        and cut_edges["truncated"]
        and len(cut_edges["rows"]) == 1
    )


# ---- schema 7: a partitioned build ----


def _feed7(feed_id, name, home, scope, spec="gtfs"):
    return {
        "feed_id": feed_id,
        "name": name,
        "spec": spec,
        "coverage": None,
        "home_country": home,
        "scope": scope,
        "declared_countries": ["FI"],
    }


def _edge7(place_id, feed_id, tier, category, relevance, cross, partition=None):
    row = {
        "place_id": place_id,
        "feed_id": feed_id,
        "tier": tier,
        "relevance_category": category,
        "relevance": relevance,
        "cross_border": cross,
    }
    if partition is not None:
        row["feed_partition"] = partition
    return row


PARTITIONED_FEEDS = [
    _feed7("f1", "HSL", "FI", "domestic"),
    _feed7("f2", "Ferry", None, "international"),
    _feed7("f3", "Bikes", "FI", "domestic", spec="gbfs"),
]


def _rt(feed_id, static, urls, method="declared"):
    return {
        "feed_id": feed_id,
        "name": None,
        "source": "atlas",
        "static_feed_id": static,
        "static_link_method": method,
        "entity_types": sorted(k.removeprefix("realtime_") for k in urls),
        "urls": json.dumps(urls),
    }


REALTIME_ROWS = {
    "FI": [_rt("f1-rt", "f1", {"realtime_trip_updates": "https://rt/tu"})],
    "international": [_rt("f-rt-lost", None, {}, method="none")],
}
PARTITIONED_EDGES = {
    "FI": [
        _edge7("hel", "f1", "local", "primary", 0.9, False),
        _edge7("hel", "f3", "local", "primary", 0.2, False),
    ],
    "links": [
        _edge7(
            "hel", "f2", "international", "international", 0.3, True, "international"
        )
    ],
}


def write_partitioned_build(
    path,
    places=PLACES,
    feeds=PARTITIONED_FEEDS,
    edges=PARTITIONED_EDGES,
    notice=b"NOTICE\n",
    realtime=None,
):
    """A schema-7 build: FI places and domestic edges, feeds by home country
    (``international`` without one), the cross-border edges under ``links``,
    every table listed with its rows and digest; ``notice=None`` publishes
    it unlicensed."""
    path.mkdir(parents=True, exist_ok=True)
    tables = {"FI/places.parquet": pa.Table.from_pylist(places)}
    for feed in feeds:
        partition = feed["home_country"] or "international"
        key = f"{partition}/feeds.parquet"
        tables.setdefault(key, []).append(feed)
    for partition, rows in edges.items():
        tables[f"{partition}/edges.parquet"] = pa.Table.from_pylist(rows)
    for partition, rows in (realtime or {}).items():  # schema 8
        tables[f"{partition}/realtime.parquet"] = pa.Table.from_pylist(rows)
    listing = {}
    for name, table in tables.items():
        if isinstance(table, list):
            table = pa.Table.from_pylist(table)
        partition, _, file = name.partition("/")
        (path / partition).mkdir(exist_ok=True)
        sink = io.BytesIO()
        pq.write_table(table, sink)
        (path / name).write_bytes(sink.getvalue())
        listing.setdefault(partition, {})[file[: -len(".parquet")]] = {
            "rows": len(table),
            "sha256": _sha(sink.getvalue()),
        }
    snapshot = {
        "schema_version": 8 if realtime else 7,
        "built_at": "2026-09-12T00:00:00+00:00",
        "counts": {"places": len(places)},
        "partitions": listing,
        "licensed": notice is not None,
        "notice_sha256": None if notice is None else _sha(notice),
    }
    if notice is not None:
        (path / "NOTICE").write_bytes(notice)
    (path / "snapshot.json").write_text(json.dumps(snapshot))
    return snapshot


def _rewrite_snapshot(path, change):
    snapshot = json.loads((path / "snapshot.json").read_text())
    change(snapshot)
    (path / "snapshot.json").write_text(json.dumps(snapshot))


def _tamper_partition(path):
    (path / "FI" / "edges.parquet").write_bytes(b"not the bytes the snapshot hashed")


def _wrong_rows(path):
    _rewrite_snapshot(path, lambda s: s["partitions"]["FI"]["edges"].update(rows=9))


def _bad_partition_name(path):
    def change(s):
        s["partitions"]["../x"] = s["partitions"].pop("international")

    _rewrite_snapshot(path, change)


def _realtime_in_links(path):
    # The layout: companions ride with feeds, never with the links.
    write_partitioned_build(path, realtime={"links": REALTIME_ROWS["FI"]})


def _links_holding_places(path):
    # The file exists and verifies: only the layout check can refuse it.
    (path / "links" / "places.parquet").write_bytes(
        (path / "FI" / "places.parquet").read_bytes()
    )

    def change(s):
        s["partitions"]["links"]["places"] = s["partitions"]["FI"]["places"]

    _rewrite_snapshot(path, change)


def _edges_without_relevance(path):
    table = pq.read_table(path / "FI" / "edges.parquet").drop_columns(
        ["relevance_category", "relevance", "cross_border"]
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    (path / "FI" / "edges.parquet").write_bytes(sink.getvalue())
    _rewrite_snapshot(
        path,
        lambda s: s["partitions"]["FI"]["edges"].update(sha256=_sha(sink.getvalue())),
    )


def _symlinked_partition(path):
    real = (path / "FI").rename(path / "FI-real")
    if not _symlink(real, path / "FI"):
        pytest.skip("this platform cannot create symlinks")


def _notice_listed_but_gone(path):
    (path / "NOTICE").unlink()


def _licensed_without_notice(path):
    _rewrite_snapshot(path, lambda s: s.update(notice_sha256=None))


@pytest.mark.parametrize(
    ("tamper", "loads"),
    [
        (None, True),
        (_tamper_partition, False),
        (_wrong_rows, False),
        (_bad_partition_name, False),
        (_links_holding_places, False),
        (_realtime_in_links, False),
        (_edges_without_relevance, False),
        (_symlinked_partition, False),
        (_notice_listed_but_gone, False),
        (_licensed_without_notice, False),
    ],
    ids=[
        "intact",
        "tampered-partition",
        "wrong-row-count",
        "bad-partition-name",
        "tables-outside-the-layout",
        "realtime-in-links",
        "schema-7-columns-missing",
        "symlinked-partition",
        "notice-missing",
        "licensed-without-notice",
    ],
)
def test_a_partitioned_build_loads_only_when_every_partition_verifies(
    tmp_path, tamper, loads
):
    write_partitioned_build(tmp_path)
    if tamper:
        tamper(tmp_path)
    build = iv.load_build("b", tmp_path)
    assert (build is not None) is loads
    if tamper is _symlinked_partition:
        # The listing agrees with the loader: a linked partition is not complete.
        assert iv.describe("b", tmp_path)["complete"] is False
    if loads:
        # The frames the viewer reads: every feed with its partition, the
        # domestic edges and the links joined, feed_partition on the links.
        assert list(build.feeds["partition"]) == ["FI", "FI", "international"]
        assert len(build.edges) == 3
        assert sorted(build.edges["feed_partition"].fillna("-")) == [
            "-",
            "-",
            "international",
        ]
        assert list(build.edges["relevance_category"]).count("primary") == 2
        assert build.served.sum() == 1
        assert build.feed_count[list(build.places["place_id"]).index("hel")] == 3
        assert set(build.digests) == {
            "FI/places.parquet",
            "FI/feeds.parquet",
            "FI/edges.parquet",
            "international/feeds.parquet",
            "links/edges.parquet",
            "NOTICE",
        }


def test_an_unlicensed_partitioned_build_has_no_notice_to_verify(tmp_path):
    write_partitioned_build(tmp_path, notice=None)
    build = iv.load_build("b", tmp_path)
    assert build is not None and "NOTICE" not in build.digests
    # A NOTICE listed as null but present is not read; one listed and missing is.
    row = iv.describe("b", tmp_path)
    assert row["complete"] is True and row["schema_version"] == 7
    assert row["partitions"] == {
        "FI": {"places": 6, "feeds": 2, "edges": 2},
        "international": {"feeds": 1},
        "links": {"edges": 1},
    }


def test_the_cache_and_the_api_serve_a_partitioned_build(tmp_path):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    cache = tmp_path / "cache"
    write_partitioned_build(cache / "index")
    builds = iv.BuildCache(cache)
    first = builds.get(iv.LATEST)
    assert builds.get(iv.LATEST) is first
    # A republish that changes one partition's digest reloads the build.
    write_partitioned_build(
        cache / "index",
        feeds=PARTITIONED_FEEDS[:1],
        edges={"FI": PARTITIONED_EDGES["FI"][:1]},
    )
    second = builds.get(iv.LATEST)
    assert second is not first and second.feed_count.max() == 1
    client = TestClient(iv.create_app(cache))
    listing = client.get("/api/builds").json()
    assert listing[0]["complete"] is True and listing[0]["schema_version"] == 7
    summary = client.get(f"/api/builds/{iv.LATEST}/summary").json()
    assert summary["schema_version"] == 7 and set(summary["partitions"]) == {"FI"}
    assert client.get(f"/api/builds/{iv.LATEST}/places").status_code == 200


@pytest.mark.parametrize(
    ("spec", "level", "feed_count", "category"),
    [
        ("all", None, 3, "primary"),
        ("gbfs", None, 1, "primary"),
        ("gtfs", "city", 1, "primary"),
        ("gtfs", "international", 1, "international"),
        ("gtfs", "national", 0, None),
        ("gbfs", "international", 0, None),
    ],
)
def test_a_view_keeps_the_edges_of_one_spec_at_one_level(
    tmp_path, spec, level, feed_count, category
):
    # A domestic edge in the international category is not a border crossing.
    domestic = _edge7("hel", "f1", "national", "international", 0.1, False)
    edges = dict(PARTITIONED_EDGES, FI=PARTITIONED_EDGES["FI"] + [domestic])
    write_partitioned_build(tmp_path, edges=edges)
    build = iv.load_build("b", tmp_path)
    view = build.view(spec, level)
    assert build.view(spec, level) is view  # derived once per combination
    hel = list(build.places["place_id"]).index("hel")
    assert view.feed_count[hel] == feed_count
    assert bool(view.served[hel]) is (feed_count > 0)
    assert view.category[hel] == category  # the highest class among kept edges
    assert view.served.sum() == (1 if feed_count else 0)
    mask = np.ones(len(build.places), dtype=bool)
    rows = iv.places_table(build, mask, view=view)["rows"]
    row = next(r for r in rows if r["place_id"] == "hel")
    assert row["category"] == category and row["feed_count"] == feed_count


def test_the_api_filters_places_by_level_and_spec(tmp_path):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    cache = tmp_path / "cache"
    write_partitioned_build(cache / "index")
    client = TestClient(iv.create_app(cache))
    url = f"/api/builds/{iv.LATEST}"
    # A level names its kinds and the classes that count; a spec its feeds.
    params = {"level": "city", "spec": "gbfs"}
    rows = client.get(f"{url}/places/table", params=params).json()["rows"]
    assert {r["kind"] for r in rows} == {"city"} and len(rows) == 3
    hel = next(r for r in rows if r["place_id"] == "hel")
    assert hel["served"] and hel["feed_count"] == 1 and hel["category"] == "primary"
    params = {"level": "city", "served": "true", "sort": "category"}
    rows = client.get(f"{url}/places/table", params=params).json()["rows"]
    assert [r["place_id"] for r in rows] == ["hel"]
    params = {"level": "city", "spec": "gtfs", "parent_id": "uus"}
    features = client.get(f"{url}/places", params=params).json()["features"]
    props = {f["id"]: f["properties"] for f in features}
    assert props["hel"]["category"] == "primary" and props["hel"]["feed_count"] == 1
    assert props["esp"]["category"] is None and not props["esp"]["served"]
    # A feed's slice classes each place by that feed's own edges the view keeps.
    params = {"feed_id": "f3", "parent_id": "uus", "spec": "gbfs"}
    features = client.get(f"{url}/places", params=params).json()["features"]
    assert [(f["id"], f["properties"]["category"]) for f in features] == [
        ("hel", "primary")
    ]
    params["feed_id"] = "f1"  # a gtfs feed under the gbfs spec serves nothing
    assert client.get(f"{url}/places", params=params).json()["features"] == []
    national = client.get(f"{url}/places", params={"level": "national"}).json()
    assert [f["id"] for f in national["features"]] == ["fi"]
    assert national["features"][0]["properties"]["feed_count"] == 0
    assert client.get(f"{url}/summary").json()["category_field"] == "category"
    for bad in ({"spec": "bikes"}, {"level": "galactic"}):
        assert client.get(f"{url}/places/table", params=bad).status_code == 400


def test_the_feed_side_api_filters_by_spec_level_and_country(tmp_path):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    cache = tmp_path / "cache"
    write_partitioned_build(cache / "index")
    write_build(cache / "builds" / "flat" / "index")  # schema 6, beside it
    client = TestClient(iv.create_app(cache))
    url = f"/api/builds/{iv.LATEST}"

    def feed_ids(**params):
        return [
            r["feed_id"]
            for r in client.get(f"{url}/feeds", params=params).json()["rows"]
        ]

    rows = client.get(f"{url}/feeds").json()["rows"]
    assert [r["feed_id"] for r in rows] == ["f1", "f3", "f2"]  # partition order
    f1 = rows[0]
    assert (f1["home_country"], f1["scope"], f1["partition"]) == (
        "FI",
        "domestic",
        "FI",
    )
    assert f1["places_served"] == 1 and f1["tier_local"] == 1
    assert f1["category_primary"] == 1 and f1["category_international"] == 0
    # A spec keeps its feeds, a level the feeds with an edge it counts, a
    # country the feeds of that partition.
    assert feed_ids(spec="gbfs") == ["f3"]
    assert feed_ids(level="international") == ["f2"]
    assert feed_ids(country="international") == ["f2"]
    assert feed_ids(level="city", spec="gtfs") == ["f1"]
    assert feed_ids(level="international", country="FI") == []
    assert client.get(f"{url}/feeds", params={"spec": "bikes"}).status_code == 400
    # Edges, the place record and the feed record carry the relevance fields.
    edges = client.get(f"{url}/edges", params={"place_id": "hel"}).json()["rows"]
    link = next(e for e in edges if e["feed_id"] == "f2")
    assert link["relevance_category"] == "international" and link["relevance"] == 0.3
    assert link["spec"] == "gtfs" and link["feed_name"] == "Ferry"
    assert link["cross_border"] is True and link["feed_partition"] == "international"
    params = {"place_id": "hel", "spec": "gtfs", "level": "city"}
    kept = client.get(f"{url}/edges", params=params).json()
    assert kept["total"] == 1 and kept["rows"][0]["feed_id"] == "f1"
    record = client.get(f"{url}/places/hel").json()["properties"]
    assert record["feeds_by_spec"] == {"gtfs": 2, "gbfs": 1}
    assert record["reached_from"] == {"international": 1}  # over the border
    assert {e["feed_id"]: e["partition"] for e in record["edges"]}[
        "f2"
    ] == "international"
    assert {e["feed_id"]: e["relevance_category"] for e in record["edges"]} == {
        "f1": "primary",
        "f2": "international",
        "f3": "primary",
    }
    feed = client.get(f"{url}/feeds/f1").json()["properties"]
    assert feed["categories"]["primary"] == 1
    assert feed["places"][0]["relevance_category"] == "primary"
    summary = client.get(f"{url}/summary").json()
    assert summary["edges_by_category"] == {"primary": 2, "international": 1}
    assert summary["feeds_by_spec"] == {"gtfs": 2, "gbfs": 1}
    # Schema 6: tiers stand in for categories, and there is no spec to count.
    flat = client.get("/api/builds/flat/summary").json()
    assert flat["category_field"] == "tier" and flat["edges_by_category"] == {
        "local": 1
    }
    assert flat["feeds_by_spec"] == {}
    assert (
        "category_primary" not in client.get("/api/builds/flat/feeds").json()["rows"][0]
    )


def test_a_schema_8_build_carries_its_realtime_companions(tmp_path):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    cache = tmp_path / "cache"
    # Publish-shaped: GTFS feeds only, each naming its companions.
    feeds = [
        {**PARTITIONED_FEEDS[0], "realtime_feed_ids": ["f1-rt"]},
        {**PARTITIONED_FEEDS[1], "realtime_feed_ids": []},
    ]
    edges = {"FI": PARTITIONED_EDGES["FI"][:1], "links": PARTITIONED_EDGES["links"]}
    write_partitioned_build(
        cache / "index", feeds=feeds, edges=edges, realtime=REALTIME_ROWS
    )
    build = iv.load_build("b", cache / "index")
    assert list(build.realtime["partition"]) == ["FI", "international"]
    assert set(build.feeds["spec"]) == {"gtfs"}
    # A feed row counts its companions; a record lists them with their
    # endpoints parsed, the ids agreeing with what the feed names; an
    # unlinked companion belongs to no feed.
    rows = iv.feeds_table(build)["rows"]
    assert {r["feed_id"]: r["realtime"] for r in rows} == {"f1": 1, "f2": 0}
    record = json.loads(iv.feed_record(build, "f1"))["properties"]
    assert record["realtime_feed_ids"] == [c["feed_id"] for c in record["realtime"]]
    assert record["realtime"] == [
        {
            "feed_id": "f1-rt",
            "name": None,
            "source": "atlas",
            "static_link_method": "declared",
            "entity_types": ["trip_updates"],
            "urls": {"realtime_trip_updates": "https://rt/tu"},
            "partition": "FI",
        }
    ]
    assert json.loads(iv.feed_record(build, "f2"))["properties"]["realtime"] == []
    client = TestClient(iv.create_app(cache))
    summary = client.get(f"/api/builds/{iv.LATEST}/summary").json()
    assert summary["schema_version"] == 8
    assert summary["realtime"] == {"feeds": 2, "linked": 1}
    assert summary["partitions"]["FI"]["realtime"]["rows"] == 1
    # A schema-7 build (no realtime tables) has none, and still loads.
    write_partitioned_build(tmp_path / "seven")
    seven = iv.load_build("s", tmp_path / "seven")
    assert len(seven.realtime) == 0 and iv.realtime_of(seven, "f1") == []
    # A companion whose endpoints are not JSON, or whose entity types are
    # not a list, or a table missing a consumed column, is refused at load.
    broken = dict(REALTIME_ROWS["FI"][0], urls="{not json")
    write_partitioned_build(tmp_path / "bad-urls", realtime={"FI": [broken]})
    assert iv.load_build("x", tmp_path / "bad-urls") is None
    broken = dict(REALTIME_ROWS["FI"][0], entity_types="trip_updates")
    write_partitioned_build(tmp_path / "bad-types", realtime={"FI": [broken]})
    assert iv.load_build("x", tmp_path / "bad-types") is None
    broken = {k: v for k, v in REALTIME_ROWS["FI"][0].items() if k != "urls"}
    write_partitioned_build(tmp_path / "no-urls", realtime={"FI": [broken]})
    assert iv.load_build("x", tmp_path / "no-urls") is None
