"""Tests for the index viewer's data layer: loading, the build cache, slices and the API.

A publish-shaped build is written into a temp cache with real parquet and a
snapshot whose digests match, then tampered with per case.
"""

import hashlib
import importlib.util
import io
import json
import os
import shutil
import threading
from pathlib import Path

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
    assert {f["id"] for f in overview.json()["features"]} == {"fi", "uus", "lap"}
    cities = client.get(base, params={"kind": "city", "parent_id": "uus"}).json()
    assert {f["id"] for f in cities["features"]} == {"hel", "esp"}
    fed = client.get(base, params={"feed_id": "f1", "bbox": "19,59,32,71"}).json()
    assert [f["id"] for f in fed["features"]] == ["hel"]  # kind defaulted to all
    clipped = client.get(base, params={"bbox": "24,60,25,60.5"}).json()["features"]
    assert {f["id"] for f in clipped} == {"fi", "uus"}  # cut to the box
    assert all(
        24 <= x <= 25 and 60 <= y <= 60.5
        for f in clipped
        for x, y in f["geometry"]["coordinates"][0]
    )
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
