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

The bounded map slices, and the web app and page that serve them, build on
this loader.
"""

import collections
import hashlib
import io
import json
import os
import stat
from pathlib import Path

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

    def summaries(self):
        return [
            describe(build_id, path, self.read_bytes)
            for build_id, path in discover(self.cache).items()
        ]

    def get(self, build_id):
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
