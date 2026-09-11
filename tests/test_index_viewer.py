"""Tests for the index viewer's data layer: verified loading and the build cache.

A publish-shaped build is written into a temp cache with real parquet and a
snapshot whose digests match, then tampered with per case.
"""

import hashlib
import importlib.util
import io
import json
import os
import shutil
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
