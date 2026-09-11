#!/usr/bin/env python3

"""Inspect a built index in the browser: the viewer's read-only data layer.

A maintainer tool, not part of the package. A build is a directory holding
the published index — ``places.parquet``, ``edges.parquet``, ``feeds.parquet``,
``snapshot.json`` and ``NOTICE``, what the ``publish`` stage writes. Builds
are discovered under a cache: the latest at ``cache/index`` and the country
loop's under ``cache/builds/<name>/index``.

A build is loaded as a *verified snapshot*. ``snapshot.json`` records a
SHA-256 for each of the four files; each file is read into memory exactly
once, hashed, and — for the parquet files — parsed from those same bytes, so
a republish landing between two reads can never pair one generation's places
with another's edges: a digest that does not match means the build is
mid-publish, and it is reported unavailable rather than cached. Per-country
builds are written once and never rewritten; only ``cache/index`` churns.

The bounded map slices and the web app that serves them build on this
loader; ``python scripts/index_viewer.py --cache cache`` runs the app.
"""

import argparse
import collections
import hashlib
import io
import json
import math
import os
import stat
import threading
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
import shapely.errors

INDEX_FILES = ("places.parquet", "edges.parquet", "feeds.parquet", "NOTICE")
DIGEST_KEYS = {
    "places.parquet": "places_sha256",
    "edges.parquet": "edges_sha256",
    "feeds.parquet": "feeds_sha256",
    "NOTICE": "notice_sha256",
}
LATEST = "latest"  # the id of the build at cache/index
CACHED_BUILDS = 4  # verified builds kept in memory
# What a published index carries and the viewer reads; a verified set of
# files that lacks any of these is not a build.
REQUIRED_COLUMNS = {
    "places.parquet": {
        "place_id",
        "kind",
        "name",
        "parent_id",
        "country_code",
        "service",
        "geometry",
    },
    "edges.parquet": {"place_id", "feed_id", "tier"},
    "feeds.parquet": {"feed_id", "name", "coverage"},
}
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
# A snapshot that cannot be read or parsed (nesting past the recursion limit
# included) is treated like a missing one.
_SNAPSHOT_ERRORS = (OSError, ValueError, RecursionError)
# ... and a set of files that verifies but is not a build: parquet that does
# not decode (any Arrow error), a table of the wrong shape, undecodable WKB.
_BUILD_ERRORS = _SNAPSHOT_ERRORS + (
    KeyError,
    TypeError,
    pa.ArrowException,
    shapely.errors.ShapelyError,
)
# Read-only, never following a symlink, and non-blocking so that a FIFO
# planted in the cache is refused by the type check instead of waited on.
_OPEN_FLAGS = (
    os.O_RDONLY
    | _O_NOFOLLOW
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_BINARY", 0)
)
DEFAULT_KINDS = ("country", "region")
# A slice is bounded twice: by feature count and by serialized size. 3,000 is
# above any build's countries + regions (ES: 1,563), and 8 MB is above the
# measured overviews at the coarsest zoom (ES 2.7 MB, AU 2.5, CA 2.5) and
# 3,000 cities as stored (ES: 4.9 MB).
MAX_FEATURES = 3000
MAX_BYTES = 8 * 1024 * 1024
# (highest zoom, tolerance in degrees) for the coarser views; a zoom past the
# last entry keeps the stored geometry.
ZOOM_TOLERANCES = ((6, 0.02), (9, 0.005))
PROPERTY_COLUMNS = ("place_id", "name", "kind", "parent_id", "country_code", "service")


def _is_regular_file(path):
    """A regular file that is not a symlink; False if it cannot be inspected."""
    try:
        return stat.S_ISREG(os.lstat(path).st_mode)
    except OSError:
        return False


def _read_file(path):
    """The bytes of a regular file, refusing a symlink or any other node.

    Opened ``O_NOFOLLOW`` where the platform has it (else checked with ``lstat``
    first) and confirmed a regular file on the open descriptor; with
    discovery's containment check this keeps reads inside the cache. Accepted,
    deliberately: a directory swapped between that check and the open. The
    viewer is a read-only, localhost tool over a maintainer-owned cache, so the
    race can only show the maintainer a file they placed there themselves —
    the same residual the build's store accepts on its Windows path.
    """
    path = Path(path)
    if not _O_NOFOLLOW and path.is_symlink():
        raise OSError(f"{path}: cache entry is a symlink")
    handle = os.open(path, _OPEN_FLAGS)
    with os.fdopen(handle, "rb") as opened:
        if not stat.S_ISREG(os.fstat(opened.fileno()).st_mode):
            raise OSError(f"{path}: not a regular file")
        return opened.read()


def snapshot_digests(snapshot):
    """The digest a snapshot records for each index file, or None if any is missing."""
    digests = {}
    for name, key in DIGEST_KEYS.items():
        value = snapshot.get(key) if isinstance(snapshot, dict) else None
        if not isinstance(value, str) or not value:
            return None
        digests[name] = value
    return digests


class Build:
    """One verified build: its tables, its geometry and per-place summaries."""

    def __init__(self, build_id, path, snapshot, digests, tables):
        self.id = build_id
        self.path = path
        self.snapshot = snapshot
        self.digests = digests
        places = tables["places.parquet"].to_pandas()
        self.geoms = shapely.from_wkb(places["geometry"].to_numpy())
        self.bounds = shapely.bounds(self.geoms)
        self.places = places.drop(columns=["geometry"])
        self.edges = tables["edges.parquet"].to_pandas()
        self.feeds = tables["feeds.parquet"].to_pandas()
        self.served = (
            self.places["place_id"].isin(set(self.edges["place_id"])).to_numpy()
        )
        per_place = self.edges.groupby("place_id")["feed_id"].nunique()
        self.feed_count = (
            self.places["place_id"].map(per_place).fillna(0).astype(int).to_numpy()
        )


def load_build(build_id, path, read_bytes=_read_file):
    """The verified build at ``path``, or None while it is mid-publish.

    Every file is read once through ``read_bytes``; its digest is checked and a
    table is parsed from those same bytes, so a file swapped between two reads
    surfaces as a digest mismatch, never as a mixed generation.
    """
    path = Path(path)
    try:
        snapshot = json.loads(read_bytes(path / "snapshot.json"))
        digests = snapshot_digests(snapshot)
        if digests is None:
            return None
        tables = {}
        for name in INDEX_FILES:
            data = read_bytes(path / name)
            if hashlib.sha256(data).hexdigest() != digests[name]:
                return None
            if name.endswith(".parquet"):
                tables[name] = pq.read_table(io.BytesIO(data))
        for name, required in REQUIRED_COLUMNS.items():
            columns = tables[name].column_names
            # Every required column, and no column twice (Arrow allows it; a
            # duplicated column would select a two-dimensional geometry).
            if len(set(columns)) != len(columns) or not required <= set(columns):
                return None
        return Build(build_id, path, snapshot, digests, tables)
    except _BUILD_ERRORS:
        return None


def _is_build_dir(index, root):
    """A real directory inside ``root`` holding a regular ``snapshot.json``.

    Neither the directory nor the snapshot may be a symlink, and the directory
    must *resolve* under the resolved cache root, so a link anywhere in the
    path cannot point discovery outside the cache. An entry that cannot be
    inspected (unreadable, or gone mid-scan) is not a build.
    """
    try:
        return (
            index.is_dir()
            and not index.is_symlink()
            and _is_regular_file(index / "snapshot.json")
            and index.resolve().is_relative_to(root)
        )
    except OSError:
        return False


def discover(cache):
    """``{build_id: index directory}`` for every build under ``cache``."""
    cache = Path(cache)
    root = cache.resolve()
    found = {}
    if _is_build_dir(cache / "index", root):
        found[LATEST] = cache / "index"
    builds = cache / "builds"
    try:
        entries = sorted(builds.iterdir()) if not builds.is_symlink() else []
    except OSError:  # no builds/ directory, or it went away mid-scan
        entries = []
    for entry in entries:
        # ``latest`` is reserved for cache/index, and a symlinked entry
        # could redirect discovery outside the cache.
        if entry.name == LATEST or entry.is_symlink():
            continue
        if _is_build_dir(entry / "index", root):
            found[entry.name] = entry / "index"
    return found


def describe(build_id, path, read_bytes=_read_file):
    """A build's listing row from its snapshot alone: cheap, no hashing.

    ``complete`` says the snapshot records every digest and every file is a
    regular, unlinked file; full verification happens when the build is opened.
    """
    row = {"id": build_id, "path": str(path), "complete": False}
    try:
        snapshot = json.loads(read_bytes(Path(path) / "snapshot.json"))
    except _SNAPSHOT_ERRORS:
        return row
    if not isinstance(snapshot, dict):
        return row
    row["built_at"] = snapshot.get("built_at")
    row["counts"] = snapshot.get("counts")
    row["complete"] = snapshot_digests(snapshot) is not None and all(
        _is_regular_file(Path(path) / name) for name in INDEX_FILES
    )
    return row


class BuildCache:
    """Verified builds by id, reloaded exactly when a snapshot's digests change.

    ``get`` re-reads the small ``snapshot.json`` on every call and compares its
    digests with the cached build's: the churning ``cache/index`` is reloaded
    when it changes, a per-country build is hashed once. The most recent
    ``size`` builds are kept.
    """

    def __init__(self, cache, size=CACHED_BUILDS, read_bytes=_read_file):
        self.cache = Path(cache)
        self.size = size
        self.read_bytes = read_bytes
        self._builds = collections.OrderedDict()
        # The web app's handlers run in worker threads; a get is one
        # lookup-load-evict transaction, so it holds the lock throughout.
        self._lock = threading.Lock()

    def summaries(self):
        return [
            describe(build_id, path, self.read_bytes)
            for build_id, path in discover(self.cache).items()
        ]

    def get(self, build_id):
        with self._lock:
            return self._get(build_id)

    def _get(self, build_id):
        path = discover(self.cache).get(build_id)
        if path is None:
            self._builds.pop(build_id, None)  # gone: never serve the stale copy
            return None
        try:
            current = snapshot_digests(
                json.loads(self.read_bytes(path / "snapshot.json"))
            )
        except _SNAPSHOT_ERRORS:
            current = None
        cached = self._builds.get(build_id)
        if cached is not None and current is not None and cached.digests == current:
            self._builds.move_to_end(build_id)
            return cached
        build = load_build(build_id, path, self.read_bytes) if current else None
        if build is None:
            self._builds.pop(build_id, None)
            return None
        self._builds[build_id] = build
        self._builds.move_to_end(build_id)
        while len(self._builds) > self.size:
            self._builds.popitem(last=False)
        return build


def parse_kinds(value):
    """The kinds a ``kind=`` parameter names: the default pair, all, or a list."""
    if value is None:
        return set(DEFAULT_KINDS)
    if value == "all":
        return None
    return {kind.strip() for kind in value.split(",") if kind.strip()}


def parse_bbox(value):
    """``minx,miny,maxx,maxy``: four finite numbers with min <= max on each axis."""
    parts = [float(part) for part in value.split(",")]
    if len(parts) != 4 or not all(math.isfinite(part) for part in parts):
        raise ValueError("bbox needs four finite numbers: minx,miny,maxx,maxy")
    if parts[0] > parts[2] or parts[1] > parts[3]:
        raise ValueError("bbox needs minx <= maxx and miny <= maxy")
    return tuple(parts)


def filter_places(
    build,
    kinds=DEFAULT_KINDS,
    parent_id=None,
    bbox=None,
    served=None,
    q=None,
    feed_id=None,
):
    """A boolean mask over the build's places for one slice.

    ``kinds`` is a collection of kinds (the default pair when omitted), or
    None for every kind. A slice that can include
    cities must be bounded by ``parent_id`` or ``bbox`` (``ValueError``
    otherwise). ``feed_id`` keeps the places that feed serves, through the
    edges. Every test is a vectorized mask over the build's arrays.
    """
    places = build.places
    if (kinds is None or "city" in kinds) and parent_id is None and bbox is None:
        raise ValueError("a slice that includes cities needs parent_id or bbox")
    mask = np.ones(len(places), dtype=bool)
    if kinds is not None:
        mask &= places["kind"].isin(set(kinds)).to_numpy()
    if parent_id is not None:
        mask &= (places["parent_id"] == parent_id).to_numpy()
    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        bounds = build.bounds
        mask &= (
            (bounds[:, 0] <= maxx)
            & (bounds[:, 2] >= minx)
            & (bounds[:, 1] <= maxy)
            & (bounds[:, 3] >= miny)
        )
    if served is not None:
        mask &= build.served if served else ~build.served
    if q:
        mask &= places["name"].str.contains(q, case=False, na=False, regex=False)
    if feed_id is not None:
        edges = build.edges
        serving = set(edges.loc[edges["feed_id"] == feed_id, "place_id"])
        mask &= places["place_id"].isin(serving).to_numpy()
    return np.asarray(mask, dtype=bool)


def tolerance_for_zoom(zoom):
    """The simplification tolerance for a map zoom; None keeps the stored geometry."""
    if zoom is None:
        return None
    for highest, tolerance in ZOOM_TOLERANCES:
        if zoom <= highest:
            return tolerance
    return None


def generalize(geoms, tolerance):
    """``geoms`` simplified to ``tolerance`` for a coarser map view.

    Douglas-Peucker over the whole array, and the slower topology-preserving
    simplifier only for the few results that come out invalid or empty (a
    place smaller than the tolerance keeps a valid ring instead of vanishing).
    Measured on the Spain build's 1,563 countries and regions at 0.02°: 0.14 s
    and 2.7 MB, against 2.7 s and 7.7 MB with topology preserved throughout —
    that simplifier keeps most vertices of a many-island multipolygon.
    """
    simplified = shapely.simplify(geoms, tolerance, preserve_topology=False)
    broken = ~shapely.is_valid(simplified) | shapely.is_empty(simplified)
    if broken.any():
        simplified[broken] = shapely.simplify(
            geoms[broken], tolerance, preserve_topology=True
        )
    return simplified


def _overflow(matched, max_features, **extra):
    return {"overflow": True, "matched": matched, "limit": max_features, **extra}


def places_geojson(
    build,
    mask,
    tolerance=None,
    max_features=MAX_FEATURES,
    max_bytes=MAX_BYTES,
    clip=None,
):
    """``(body, overflow)`` for the masked places.

    ``body`` is the UTF-8 GeoJSON FeatureCollection and ``overflow`` None, or
    ``body`` is None and ``overflow`` the record to send instead. The count is
    checked before any geometry work; with ``clip`` (a bbox) every geometry is
    cut to the box and a place whose geometry does not reach into it is
    dropped; the geometry is generalized over the whole array, serialized in
    one pass, and the byte budget measured on exactly what would be sent.
    Properties stay compact; ``service`` is the row's JSON string.
    """
    matched = int(np.count_nonzero(mask))
    if matched > max_features:  # decided before anything is materialized
        return None, _overflow(matched, max_features)
    index = np.flatnonzero(mask)
    geoms = build.geoms[index]
    if clip is not None:
        geoms = shapely.clip_by_rect(geoms, *clip)
        visible = ~shapely.is_empty(geoms)
        index, geoms = index[visible], geoms[visible]
    if tolerance is not None:
        geoms = generalize(geoms, tolerance)
    geometry = shapely.to_geojson(geoms)
    props = build.places.iloc[index][list(PROPERTY_COLUMNS)]
    props = props.astype(object).where(props.notna(), None)
    features = []
    for record, feed_count, is_served, geom in zip(
        props.to_dict(orient="records"),
        build.feed_count[index].tolist(),
        build.served[index].tolist(),
        geometry,
    ):
        record["feed_count"] = feed_count
        record["served"] = is_served
        features.append(
            '{"type":"Feature","id":%s,"properties":%s,"geometry":%s}'
            % (
                json.dumps(record["place_id"]),
                json.dumps(record, ensure_ascii=False),
                geom if geom is not None else "null",
            )
        )
    body = (
        '{"type":"FeatureCollection","features":[' + ",".join(features) + "]}"
    ).encode("utf-8")
    if len(body) > max_bytes:
        return None, _overflow(
            matched, max_features, bytes=len(body), byte_limit=max_bytes
        )
    return body, None


def _parse_served(value):
    """The ``served=`` parameter: ``true``/``false`` (or ``1``/``0``), else None."""
    if value is None:
        return None
    if value in ("true", "1"):
        return True
    if value in ("false", "0"):
        return False
    raise ValueError("served must be true or false")


def create_app(cache, size=CACHED_BUILDS):
    """The viewer's web app over one build cache.

    FastAPI is imported here, not at module level, so the loader and the
    slices import and test without the ``viewer`` extra.
    """
    from fastapi import FastAPI, HTTPException, Response

    builds = BuildCache(cache, size)
    app = FastAPI(title="transitio index viewer", docs_url=None, redoc_url=None)

    def opened(build_id):
        build = builds.get(build_id)
        if build is None:
            raise HTTPException(404, f"{build_id}: not an available build")
        return build

    @app.get("/api/builds")
    def list_builds():
        return builds.summaries()

    @app.get("/api/builds/{build_id}/summary")
    def summary(build_id: str):
        build = opened(build_id)
        return {
            **build.snapshot,
            "id": build.id,
            "served_places": int(build.served.sum()),
        }

    @app.get("/api/builds/{build_id}/places")
    def places(
        build_id: str,
        kind: str | None = None,
        parent_id: str | None = None,
        bbox: str | None = None,
        served: str | None = None,
        q: str | None = None,
        feed_id: str | None = None,
        zoom: float | None = None,
    ):
        build = opened(build_id)
        try:
            # A feed's served places are mostly cities: with ``feed_id`` and
            # no ``kind`` the slice covers every kind.
            kinds = None if kind is None and feed_id is not None else parse_kinds(kind)
            box = parse_bbox(bbox) if bbox is not None else None
            mask = filter_places(
                build,
                kinds=kinds,
                parent_id=parent_id,
                bbox=box,
                served=_parse_served(served),
                q=q,
                feed_id=feed_id,
            )
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        body, overflow = places_geojson(
            build, mask, tolerance_for_zoom(zoom), MAX_FEATURES, MAX_BYTES, clip=box
        )
        if overflow is not None:
            return overflow
        return Response(body, media_type="application/geo+json")

    return app


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Serve a built index for inspection in the browser."
    )
    parser.add_argument("--cache", default="cache", help="the build cache directory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    import uvicorn

    print(f"index viewer at http://{args.host}:{args.port}/ over {args.cache}")
    uvicorn.run(create_app(args.cache), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
