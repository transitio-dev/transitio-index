#!/usr/bin/env python3

"""Inspect a built index in the browser: the viewer's read-only data layer.

A maintainer tool, not part of the package. A build is a directory holding
the published index, what the ``publish`` stage writes: before schema 7 the
flat ``places.parquet``, ``edges.parquet``, ``feeds.parquet``, ``NOTICE`` and
``snapshot.json``; from schema 7 a directory of partitions — one per country
code with its feeds, places and domestic edges, ``international/feeds.parquet``
and ``links/edges.parquet`` (the cross-border edges, with ``feed_partition``)
— listed in ``snapshot.json`` with each table's rows and digest. Builds are
discovered under a cache: the latest at ``cache/index`` and the country
loop's under ``cache/builds/<name>/index``.

A build is loaded as a *verified snapshot*. Each listed file is read into
memory exactly once, hashed, and — for the parquet files — parsed from those
same bytes, so a republish landing between two reads can never pair one
generation's places with another's edges: a digest that does not match means
the build is mid-publish, and it is reported unavailable rather than cached.
A partitioned build's tables are joined into the three frames the viewer
reads (feeds with their ``partition``, places, edges with ``feed_partition``
on the links). Per-country builds are written once and never rewritten; only
``cache/index`` churns.

The bounded map slices and the web app that serves them build on this
loader; ``python scripts/index_viewer.py --cache cache`` runs the app.
"""

import argparse
import collections
import datetime
import hashlib
import io
import json
import math
import os
import re
import stat
import threading
import time
import webbrowser
from pathlib import Path

import numpy as np
import pandas as pd
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
# Schema 7: a partition is a country code, ``international`` (feeds without a
# home country) or ``links`` (the cross-border edges); its tables are listed
# under ``partitions`` in the snapshot with their rows and digest.
PARTITION_NAME = re.compile(r"[A-Z]{2}|international|links")
PARTITION_TABLES = ("feeds", "places", "edges")
# The tables a partition kind may carry; a country partition any of the three.
PARTITION_LAYOUT = {"international": {"feeds"}, "links": {"edges"}}
LINKS = "links"
# What a schema-7 build carries on top of the flat columns: the classify
# stage's country fields and the rank stage's relevance.
SCHEMA_7_COLUMNS = {
    "feeds.parquet": {"home_country", "scope", "declared_countries"},
    "edges.parquet": {"relevance_category", "relevance", "cross_border"},
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
_HERE = Path(__file__).resolve().parent  # the page and its module live here
TABLE_LIMIT = 200  # rows per page of the places table, at most
TREE_LIMIT = 2000  # nodes per tree level, at most
KIND_RANK = {"country": 0, "region": 1, "city": 2}  # the tree's order
SERVICE_STATS = ("stops", "routes", "departures_per_day")
TABLE_SORT_COLUMNS = (
    "place_id",
    "name",
    "kind",
    "parent_name",
    "country_code",
    "served",
    "feed_count",
) + SERVICE_STATS
# The descriptive columns a place record carries when the build has them.
RECORD_COLUMNS = (
    "place_id",
    "kind",
    "source_subtype",
    "name",
    "names",
    "aliases",
    "parent_id",
    "country_code",
    "overture_id",
    "osm_relation_id",
    "wikidata_id",
    "geonames_id",
    "statistical_area_id",
    "resolution_method",
    "geometry_source",
    "curated",
    "metro_ids",
    "member_ids",
)
EDGE_COLUMNS = (
    "feed_id",
    "tier",
    "tier_confidence",
    "method",
    "needs_review",
    "service",
    "relevance_category",
    "relevance",
    "cross_border",
    "feed_partition",
)
EDGE_FEED_COLUMNS = ("name", "spec", "source", "crawl_status", "stop_count")
# An edge's class: the rank stage's relevance category (schema 7) or, before
# it, the classify stage's tier; each in rank order, highest first.
TIERS = ("local", "regional", "national", "international", "unknown")
CATEGORIES = ("primary", "secondary", "tertiary", "international", "unknown")
SPECS = ("gtfs", "gtfs-rt", "gbfs")
# A level: the place kinds it shows, and the categories (schema 7) or tiers
# (schema 6) of the edges that count as serving a place at that level.
LEVELS = {
    "city": (("city", "metro"), ("primary", "secondary"), ("local", "regional")),
    "regional": (("region",), ("secondary", "tertiary"), ("regional", "national")),
    "national": (("country",), ("tertiary",), ("national",)),
    "international": (("country",), ("international",), ("international",)),
}
# A feed record lists at most this many served places, and an edges reply at
# most this many rows: Germany's busiest feed serves 23,126 places over
# 33,668 edges (3.9 MB and 9.4 MB unbounded), which nobody reads as a list.
FEED_PLACES_LIMIT = 2000
EDGES_LIMIT = 2000
FEED_TABLE_COLUMNS = (
    "feed_id",
    "name",
    "spec",
    "source",
    "crawl_status",
    "stop_count",
    "home_country",
    "scope",
    "partition",
)
# The descriptive columns a feed record carries when the build has them.
FEED_RECORD_COLUMNS = (
    "feed_id",
    "onestop_id",
    "mdb_id",
    "name",
    "spec",
    "source",
    "aliases",
    "crawl_status",
    "last_crawled",
    "stop_count",
    "coverage_source",
    "redistribution_allowed",
    "uncrawlable_reason",
    "home_country",
    "scope",
    "declared_countries",
    "partition",
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


def snapshot_files(snapshot):
    """``{relative path: (digest, rows)}`` for every file a snapshot lists, or
    None when it lists none or lists them badly.

    Before schema 7 the four flat files, each with a digest and no row count.
    From schema 7 every partition table (a partition name of the layout, a
    table of the layout, a string digest and an integer row count) and the
    ``NOTICE`` when the build is licensed (``notice_sha256`` a string; an
    unlicensed build has none).
    """
    if not isinstance(snapshot, dict):
        return None
    listing = snapshot.get("partitions")
    if listing is None:
        digests = snapshot_digests(snapshot)
        return None if digests is None else {n: (d, None) for n, d in digests.items()}
    if not isinstance(listing, dict) or not listing:
        return None
    files = {}
    for partition, tables in listing.items():
        if not isinstance(partition, str) or not PARTITION_NAME.fullmatch(partition):
            return None
        if not isinstance(tables, dict) or not tables:
            return None
        allowed = PARTITION_LAYOUT.get(partition, set(PARTITION_TABLES))
        for table, entry in tables.items():
            if table not in allowed or not isinstance(entry, dict):
                return None
            digest, rows = entry.get("sha256"), entry.get("rows")
            if not isinstance(digest, str) or not digest:
                return None
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                return None
            files[f"{partition}/{table}.parquet"] = (digest, rows)
    notice = snapshot.get("notice_sha256")
    if notice is None and snapshot.get("licensed"):
        return None  # a licensed build ships its NOTICE
    if notice is not None:
        if not isinstance(notice, str) or not notice:
            return None
        files["NOTICE"] = (notice, None)
    return files


def _files_present(path, files):
    """Every listed file is a regular file inside a plain partition directory."""
    for name in files:
        partition = name.rpartition("/")[0]
        if partition and not _plain_directory(path / partition):
            return False
        if not _is_regular_file(path / name):
            return False
    return True


def _join_partitions(tables):
    """The three viewer frames of a partitioned build from its parquet
    tables by path: feeds with their ``partition``, places, and the domestic
    edges with the links (``feed_partition`` on the links, null elsewhere)."""
    parts = {name: [] for name in PARTITION_TABLES}
    for path, table in tables.items():
        partition, _, file = path.partition("/")
        kind = file[: -len(".parquet")]
        if kind == "feeds":
            column = pa.array([partition] * len(table), pa.string())
            table = table.append_column("partition", column)
        elif kind == "edges" and "feed_partition" not in table.column_names:
            table = table.append_column(
                "feed_partition", pa.nulls(len(table), pa.string())
            )
        parts[kind].append(table)
    joined = {}
    for kind, found in parts.items():
        if not found:
            return None  # a feeds-only build is not a build the viewer shows
        joined[f"{kind}.parquet"] = pa.concat_tables(found, promote_options="default")
    return joined


def _plain_directory(path):
    """A directory that is not a symlink; False if it cannot be inspected."""
    try:
        info = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode)


class Build:
    """One verified build: its tables, its geometry and per-place summaries."""

    def __init__(self, build_id, path, snapshot, digests, tables):
        self.id = build_id
        self.path = path
        self.snapshot = snapshot
        self.digests = digests
        # One id for the whole verified snapshot: every file's digest, so a
        # republish that changes only the edges still changes it.
        self.snapshot_id = hashlib.sha256(
            "".join(f"{name}:{digests[name]}" for name in sorted(digests)).encode()
        ).hexdigest()
        places = tables["places.parquet"].to_pandas()
        self.geoms = shapely.from_wkb(places["geometry"].to_numpy())
        self.bounds = shapely.bounds(self.geoms)
        # Positional frames: a parquet written with a pandas index would
        # restore it, and every lookup here is by row position.
        self.places = places.drop(columns=["geometry"]).reset_index(drop=True)
        self.edges = tables["edges.parquet"].to_pandas().reset_index(drop=True)
        self.feeds = tables["feeds.parquet"].to_pandas().reset_index(drop=True)
        self._row_of = pd.Series(
            np.arange(len(self.places)), index=self.places["place_id"].to_numpy()
        )
        # The ``service`` JSON of places and edges, parsed once here and
        # normalized (non-finite → None); nothing parses it again per request,
        # and malformed JSON makes the build unavailable rather than a 500.
        self.service = [_loads(v) for v in self.places["service"].to_numpy()]
        self.edge_service = (
            [_loads(v) for v in self.edges["service"].to_numpy()]
            if "service" in self.edges.columns
            else None
        )
        self.ranked = "relevance_category" in self.edges.columns
        self.class_column = "category" if self.ranked else "tier"
        self.classes = self.edges["relevance_category" if self.ranked else "tier"]
        self.class_names = CATEGORIES if self.ranked else TIERS
        self._views = {}
        default = self.view()
        self.served, self.feed_count = default.served, default.feed_count
        self.table = _place_table(self, default)
        self.feed_table = default.feed_table
        children = self.places["parent_id"].value_counts()
        self.child_count = (
            self.places["place_id"].map(children).fillna(0).astype(int).to_numpy()
        )

    def view(self, spec="all", level=None):
        """The per-place summaries under one spec and level, derived once."""
        key = (spec, level)
        if key not in self._views:
            self._views[key] = View(self, spec, level)
        return self._views[key]


class View:
    """What ``spec`` and ``level`` keep of a build's edges, summarized per place.

    ``spec`` keeps the edges of the feeds of that spec (``all`` keeps every
    feed); ``level`` keeps the edges whose class is one the level counts (None
    keeps every class). From those edges: the served flag, the distinct feeds
    per place and the highest class per place (None when unserved): four
    arrays, kept with the build, and the feeds table counted over the kept
    edges.
    """

    def __init__(self, build, spec, level):
        self.spec, self.level = spec, level
        edges, feeds, places = build.edges, build.feeds, build.places
        keep = np.ones(len(edges), dtype=bool)
        if spec != "all":
            ids = (
                set(feeds.loc[feeds["spec"] == spec, "feed_id"])
                if "spec" in feeds
                else ()
            )
            keep &= edges["feed_id"].isin(ids).to_numpy()
        if level is not None:
            keep &= build.classes.isin(
                LEVELS[level][1 if build.ranked else 2]
            ).to_numpy()
            if level == "international" and build.ranked:
                # The category alone is not enough: the level shows the
                # edges that cross a border.
                keep &= edges["cross_border"].fillna(False).astype(bool).to_numpy()
        self.edge_mask = keep
        kept = edges[keep]
        ids = places["place_id"]
        self.served = ids.isin(set(kept["place_id"])).to_numpy()
        per_place = kept.groupby("place_id")["feed_id"].nunique()
        self.feed_count = ids.map(per_place).fillna(0).astype(int).to_numpy()
        names = build.class_names
        rank = build.classes[keep].map({name: i for i, name in enumerate(names)})
        best = ids.map(rank.groupby(kept["place_id"].to_numpy()).min())
        lookup = np.array(names + (None,), dtype=object)
        self.category = lookup[best.fillna(len(names)).astype(int).to_numpy()]
        self.feed_table = _feed_table(build, keep)


def _has_columns(table, name, required):
    """Every column ``required`` lists for ``name``, and no column twice.

    Arrow allows a duplicated column; one would select a two-dimensional
    geometry.
    """
    columns = table.column_names
    if len(set(columns)) != len(columns):
        return False
    return required.get(name, set()) <= set(columns)


def load_build(build_id, path, read_bytes=_read_file):
    """The verified build at ``path``, or None while it is mid-publish.

    Every file is read once through ``read_bytes``; its digest is checked and a
    table is parsed from those same bytes, so a file swapped between two reads
    surfaces as a digest mismatch, never as a mixed generation.
    """
    path = Path(path)
    try:
        snapshot = json.loads(read_bytes(path / "snapshot.json"))
        files = snapshot_files(snapshot)
        if files is None:
            return None
        digests, tables = {}, {}
        for name, (digest, rows) in files.items():
            partition = name.rpartition("/")[0]
            if partition and not _plain_directory(path / partition):
                return None
            data = read_bytes(path / name)
            if hashlib.sha256(data).hexdigest() != digest:
                return None
            digests[name] = digest
            if name.endswith(".parquet"):
                table = pq.read_table(io.BytesIO(data))
                if rows is not None and len(table) != rows:
                    return None
                base = name.rpartition("/")[2]
                # Each partition on its own: a join promotes a column one
                # partition lacks to nulls, which would hide the gap.
                if "partitions" in snapshot and not (
                    _has_columns(table, base, REQUIRED_COLUMNS)
                    and _has_columns(table, base, SCHEMA_7_COLUMNS)
                ):
                    return None
                tables[name] = table
        if "partitions" in snapshot:
            tables = _join_partitions(tables)
            if tables is None:
                return None
        for name in REQUIRED_COLUMNS:
            if not _has_columns(tables[name], name, REQUIRED_COLUMNS):
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
    row["schema_version"] = snapshot.get("schema_version")
    files = snapshot_files(snapshot)
    row["complete"] = files is not None and _files_present(Path(path), files)
    if isinstance(snapshot.get("partitions"), dict):
        row["partitions"] = {
            name: {
                table: entry.get("rows") if isinstance(entry, dict) else None
                for table, entry in tables.items()
            }
            for name, tables in snapshot["partitions"].items()
            if isinstance(tables, dict)
        }
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
            snapshot = json.loads(self.read_bytes(path / "snapshot.json"))
            listed = snapshot_files(snapshot)
        except _SNAPSHOT_ERRORS:
            listed = None
        cached = self._builds.get(build_id)
        # The whole snapshot: one rewritten with the same digests but other
        # row counts or metadata is revalidated, not served from memory.
        if cached is not None and listed is not None and cached.snapshot == snapshot:
            self._builds.move_to_end(build_id)
            return cached
        build = load_build(build_id, path, self.read_bytes) if listed else None
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
    bounded=True,
    view=None,
):
    """A boolean mask over the build's places for one slice.

    ``kinds`` is a collection of kinds (the default pair when omitted), or
    None for every kind. With ``bounded`` (the default, for map slices) a
    slice that can include cities must be bounded by ``parent_id`` or
    ``bbox`` (``ValueError`` otherwise); the geometry-free table passes
    ``bounded=False``. ``feed_id`` keeps the places that feed serves, through the
    edges. ``served`` and ``feed_id`` read the ``view`` (the build's default
    when omitted). Every test is a vectorized mask over the build's arrays.
    """
    places = build.places
    view = view or build.view()
    unbounded = parent_id is None and bbox is None
    if bounded and unbounded and (kinds is None or "city" in kinds):
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
        mask &= view.served if served else ~view.served
    if q:
        mask &= places["name"].str.contains(q, case=False, na=False, regex=False)
    if feed_id is not None:
        edges = build.edges[view.edge_mask]
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


def _service_frame(parsed):
    """The parsed ``service`` dicts as a numeric frame, NaN where a stat is absent."""
    records = [stats if isinstance(stats, dict) else {} for stats in parsed]
    frame = pd.DataFrame.from_records(records).reindex(columns=SERVICE_STATS)
    return frame.apply(pd.to_numeric, errors="coerce")


def _place_table(build, view):
    """The geometry-free table of a build's places under ``view``, one row each."""
    places = build.places
    names = pd.Series(places["name"].to_numpy(), index=places["place_id"].to_numpy())
    table = places[["place_id", "name", "kind", "parent_id", "country_code"]].copy()
    table["parent_name"] = places["parent_id"].map(names).to_numpy()
    table["served"] = view.served
    table["feed_count"] = view.feed_count
    table[build.class_column] = view.category
    stats = _service_frame(build.service)
    for column in SERVICE_STATS:
        table[column] = stats[column].to_numpy()
    return table.reset_index(drop=True)


def _json_ready(frame):
    """A frame's rows as plain JSON values: Python scalars, None for NaN and ±inf."""
    frame = frame.replace([np.inf, -np.inf], np.nan).astype(object)
    return frame.where(frame.notna(), None).to_dict(orient="records")


def places_table(build, mask, sort="name", order="asc", offset=0, limit=50, view=None):
    """``{"total", "offset", "limit", "rows"}``: a page of the masked places."""
    view = view or build.view()
    sortable = TABLE_SORT_COLUMNS + (build.class_column,)
    if sort not in sortable:
        raise ValueError(f"sort must be one of {', '.join(sortable)}")
    if order not in ("asc", "desc"):
        raise ValueError("order must be asc or desc")
    if offset < 0 or limit < 1:
        raise ValueError("offset must be at least 0 and limit at least 1")
    limit = min(limit, TABLE_LIMIT)
    table = build.table
    if view is not build.view():  # the default view's columns are the table's
        table = table.assign(
            served=view.served,
            feed_count=view.feed_count,
            **{build.class_column: view.category},
        )
    rows = table[mask].sort_values(
        sort, ascending=order == "asc", kind="stable", na_position="last"
    )
    page = rows.iloc[offset : offset + limit]
    return {
        "total": int(len(rows)),
        "offset": offset,
        "limit": limit,
        "rows": _json_ready(page),
    }


def _cell(value):
    """One pandas cell as a JSON value (Arrow maps arrive as lists of pairs)."""
    if isinstance(value, np.ndarray):
        return [_cell(item) for item in value.tolist()]
    if isinstance(value, list):
        if value and all(isinstance(item, tuple) and len(item) == 2 for item in value):
            return dict(value)
        return [_cell(item) for item in value]
    if isinstance(value, np.datetime64):  # before .item(): ns precision gives an int
        value = pd.Timestamp(value)
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or value is pd.NaT:  # pandas' own missing scalars
        return None
    if isinstance(value, (datetime.date, datetime.datetime)):  # incl. pd.Timestamp
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _as_map(value):
    """An Arrow map cell (a list of pairs, or empty) as a dict; None stays None."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return dict(value.tolist() if isinstance(value, np.ndarray) else value)


def _finite(value):
    """Parsed JSON with every non-finite number (``1e400`` → inf) made None."""
    if isinstance(value, dict):
        return {key: _finite(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_finite(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _loads(value):
    """A ``service`` JSON string parsed and normalized (at load), None when empty."""
    return _finite(json.loads(value)) if isinstance(value, str) and value else None


def place_record(build, place_id):
    """One place as a GeoJSON Feature in UTF-8 bytes, or None when unknown.

    The properties carry the row's descriptive columns present in this build,
    its parsed ``service``, ``served`` and ``feed_count``, its ``bbox``, the
    ``ancestors`` root first, a ``children`` summary and its ``edges`` joined
    with the feed table.
    """
    if place_id not in build._row_of.index:
        return None
    row = int(build._row_of[place_id])
    places = build.places
    place = places.iloc[row]
    props = {c: _cell(place[c]) for c in RECORD_COLUMNS if c in places.columns}
    if "names" in props:  # an Arrow map: a dict even when empty
        props["names"] = _as_map(place["names"])
    props["service"] = build.service[row]
    props["served"] = bool(build.served[row])
    props["feed_count"] = int(build.feed_count[row])
    props["bbox"] = [_cell(v) for v in build.bounds[row]]
    ancestors = []
    parent = place["parent_id"]
    for _ in range(8):  # a bounded walk: a cycle in the data cannot loop
        if not isinstance(parent, str) or parent not in build._row_of.index:
            break
        ancestor = places.iloc[int(build._row_of[parent])]
        ancestors.append(
            {"place_id": parent, "name": ancestor["name"], "kind": ancestor["kind"]}
        )
        parent = ancestor["parent_id"]
    props["ancestors"] = ancestors[::-1]
    children = (places["parent_id"] == place_id).to_numpy()
    props["children"] = {
        "count": int(children.sum()),
        "by_kind": {
            k: int(v) for k, v in places.loc[children, "kind"].value_counts().items()
        },
        "served": int(build.served[children].sum()),
    }
    positions = np.flatnonzero((build.edges["place_id"] == place_id).to_numpy())
    edges = build.edges.iloc[positions]
    if build.edge_service is not None:  # the parsed copies, by edge position
        edges = edges.assign(service=[build.edge_service[i] for i in positions])
    edges = edges[[c for c in EDGE_COLUMNS if c in edges.columns]]
    feeds = build.feeds[
        [c for c in ("feed_id",) + EDGE_FEED_COLUMNS if c in build.feeds]
    ]
    merged = edges.merge(
        feeds.rename(columns={"name": "feed_name"}), on="feed_id", how="left"
    )
    props["edges"] = [{k: _cell(v) for k, v in r.items()} for r in _json_ready(merged)]
    distinct = merged.drop_duplicates("feed_id")
    props["feeds_by_spec"] = (
        {k: int(v) for k, v in distinct["spec"].value_counts().items()}
        if "spec" in distinct.columns
        else {}
    )
    geometry = shapely.to_geojson(build.geoms[row])
    body = '{"type":"Feature","id":%s,"geometry":%s,"properties":%s}' % (
        json.dumps(place_id),
        geometry if geometry is not None else "null",
        json.dumps(props, ensure_ascii=False, allow_nan=False),
    )
    return body.encode("utf-8")


def _tree_level(build, mask):
    """The nodes for the masked places, kind rank then name, cut at TREE_LIMIT."""
    columns = ["place_id", "name", "kind", "parent_id", "served", "feed_count"]
    frame = build.table.loc[mask, columns].copy()
    frame["child_count"] = build.child_count[mask]
    frame["rank"] = frame["kind"].map(KIND_RANK).fillna(len(KIND_RANK))
    frame = frame.sort_values(["rank", "name"], kind="stable").drop(columns="rank")
    return _json_ready(frame.iloc[:TREE_LIMIT]), len(frame) > TREE_LIMIT


def tree_nodes(build, root=None, depth=1):
    """``{"root", "depth", "nodes", "truncated"}``: the roots, or ``root``'s children.

    A root is a place whose parent is null or not in the build. With ``depth``
    above 1 (clamped to 3) each node carries its ``children`` one level down;
    every level is one ``isin`` mask over the parent column, nested in Python
    over the few hundred nodes. None when ``root`` is not a place.
    """
    depth = max(1, min(int(depth), 3))
    parents = build.places["parent_id"]
    if root is None:
        mask = (parents.isna() | ~parents.isin(build._row_of.index)).to_numpy()
    elif root in build._row_of.index:
        mask = (parents == root).to_numpy()
    else:
        return None
    nodes, truncated = _tree_level(build, mask)
    level = nodes
    for _ in range(depth - 1):
        ids = [node["place_id"] for node in level if node["child_count"]]
        if not ids:
            break
        children, cut = _tree_level(build, parents.isin(ids).to_numpy())
        truncated = truncated or cut
        by_parent = {}
        for child in children:
            by_parent.setdefault(child["parent_id"], []).append(child)
        for node in level:
            node["children"] = by_parent.get(node["place_id"], [])
        level = children
    return {"root": root, "depth": depth, "nodes": nodes, "truncated": truncated}


def _feed_table(build, keep):
    """The geometry-free table of a build's feeds, one row per feed, in feed order.

    Its counts (places served, edges per tier and, on schema 7, per category)
    run over the edges ``keep`` masks.
    """
    feeds, edges = build.feeds, build.edges[keep]
    table = feeds[[c for c in FEED_TABLE_COLUMNS if c in feeds.columns]].copy()
    table["has_coverage"] = feeds["coverage"].notna().to_numpy()
    served = edges.groupby("feed_id")["place_id"].nunique()
    table["places_served"] = (
        table["feed_id"].map(served).fillna(0).astype(int).to_numpy()
    )
    counted = [("tier", "tier", TIERS)]
    if build.ranked:
        counted.append(("category", "relevance_category", CATEGORIES))
    for prefix, column, names in counted:
        by_class = pd.crosstab(edges["feed_id"], edges[column]) if len(edges) else {}
        for name in names:
            if name in by_class:
                counts = table["feed_id"].map(by_class[name]).fillna(0).astype(int)
                table[f"{prefix}_{name}"] = counts.to_numpy()
            else:
                table[f"{prefix}_{name}"] = 0
    return table.reset_index(drop=True)


def feeds_table(build, view=None, country=None):
    """``{"total", "rows"}``: the build's feeds under ``view``, geometry-free.

    A spec keeps the feeds of that spec, a level the feeds with an edge the
    level counts, ``country`` the feeds of that partition.
    """
    view = view or build.view()
    table = view.feed_table
    keep = np.ones(len(table), dtype=bool)
    if view.spec != "all":
        keep &= (table["spec"] == view.spec).to_numpy() if "spec" in table else False
    if view.level is not None:
        keep &= (table["places_served"] > 0).to_numpy()
    if country is not None:
        keep &= (
            (table["partition"] == country).to_numpy()
            if "partition" in table
            else False
        )
    rows = table[keep]
    return {"total": int(len(rows)), "rows": _json_ready(rows)}


def _hull_geojson(wkb):
    if not isinstance(wkb, (bytes, bytearray)):
        return None
    try:
        return shapely.to_geojson(shapely.from_wkb(wkb))
    except shapely.errors.ShapelyError:
        return None


def feed_record(build, feed_id):
    """One feed as a GeoJSON Feature in UTF-8 bytes (its hull as geometry), or None.

    The properties carry the feed's descriptive columns present in this build,
    the counts from the feed table, and the places it serves (one per place,
    with the tier of its first edge) up to ``FEED_PLACES_LIMIT``.
    """
    feeds = build.feeds
    matches = np.flatnonzero((feeds["feed_id"] == feed_id).to_numpy())
    if not matches.size:
        return None
    row = int(matches[0])
    feed = feeds.iloc[row]
    props = {c: _cell(feed[c]) for c in FEED_RECORD_COLUMNS if c in feeds.columns}
    counts = build.feed_table.iloc[row]
    props["places_served"] = int(counts["places_served"])
    props["tiers"] = {tier: int(counts[f"tier_{tier}"]) for tier in TIERS}
    if build.ranked:
        props["categories"] = {c: int(counts[f"category_{c}"]) for c in CATEGORIES}
    served = build.edges[(build.edges["feed_id"] == feed_id).to_numpy()].merge(
        build.table[["place_id", "name", "kind"]], on="place_id", how="left"
    )
    # One row per place: a feed with several edges to one place contributes
    # its first edge in table order, so the total counts places, not edges.
    served = served.drop_duplicates("place_id")
    columns = [
        c
        for c in ("place_id", "name", "kind", "tier", "relevance_category")
        if c in served
    ]
    props["places_total"] = int(len(served))
    props["places_truncated"] = len(served) > FEED_PLACES_LIMIT
    props["places"] = _json_ready(served[columns].iloc[:FEED_PLACES_LIMIT])
    geometry = _hull_geojson(feed["coverage"])
    body = '{"type":"Feature","id":%s,"geometry":%s,"properties":%s}' % (
        json.dumps(feed_id),
        geometry if geometry is not None else "null",
        json.dumps(props, ensure_ascii=False),
    )
    return body.encode("utf-8")


def edges_of(build, place_id=None, feed_id=None, view=None):
    """``{"total", "truncated", "rows"}``: the edges of one place or one feed.

    Exactly one of the two ids; rows carry the other side's name, and at most
    ``EDGES_LIMIT`` of them are returned (the total says how many there are).
    ``view`` keeps only the edges its spec and level keep.
    """
    if (place_id is None) == (feed_id is None):
        raise ValueError("give exactly one of place_id or feed_id")
    edges = build.edges
    view = view or build.view()
    column, value = (
        ("place_id", place_id) if place_id is not None else ("feed_id", feed_id)
    )
    positions = np.flatnonzero((edges[column] == value).to_numpy() & view.edge_mask)
    total = int(positions.size)
    positions = positions[:EDGES_LIMIT]
    rows = edges.iloc[positions][
        [c for c in ("place_id",) + EDGE_COLUMNS if c in edges.columns]
    ]
    if build.edge_service is not None:  # the parsed copies, by edge position
        rows = rows.assign(service=[build.edge_service[i] for i in positions])
    places = build.table[["place_id", "name", "kind"]].rename(
        columns={"name": "place_name"}
    )
    rows = rows.merge(places, on="place_id", how="left")
    if "name" in build.feeds.columns:
        feeds = build.feeds[["feed_id", "name"]].rename(columns={"name": "feed_name"})
        rows = rows.merge(feeds, on="feed_id", how="left")
    return {"total": total, "truncated": total > EDGES_LIMIT, "rows": _json_ready(rows)}


def _overflow(matched, max_features, **extra):
    return {"overflow": True, "matched": matched, "limit": max_features, **extra}


def places_geojson(
    build,
    mask,
    tolerance=None,
    max_features=MAX_FEATURES,
    max_bytes=MAX_BYTES,
    clip=None,
    extra=None,
    view=None,
):
    """``(body, overflow)`` for the masked places.

    ``body`` is the UTF-8 GeoJSON FeatureCollection and ``overflow`` None, or
    ``body`` is None and ``overflow`` the record to send instead. The count is
    checked before any geometry work; with ``clip`` (a bbox) every geometry is
    cut to the box and a place whose geometry does not reach into it is
    dropped; the geometry is generalized over the whole array, serialized in
    one pass, and the byte budget measured on exactly what would be sent.
    Properties stay compact; ``service`` is the row's JSON string, ``served``,
    ``feed_count`` and the class (``category``, or ``tier`` on schema 6) come
    from ``view`` (the build's default when omitted); ``extra`` maps a property
    name to a ``place_id``-indexed Series to add.
    """
    view = view or build.view()
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
    props[build.class_column] = view.category[index]
    for key, by_place in (extra or {}).items():  # e.g. a feed's tier per place
        props[key] = props["place_id"].map(by_place)
    props = props.astype(object).where(props.notna(), None)
    features = []
    for record, feed_count, is_served, geom in zip(
        props.to_dict(orient="records"),
        view.feed_count[index].tolist(),
        view.served[index].tolist(),
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


def parse_spec(value):
    """The ``spec=`` parameter: one of SPECS, or ``all`` (the default)."""
    if value is None or value == "all":
        return "all"
    if value not in SPECS:
        raise ValueError(f"spec must be all or one of {', '.join(SPECS)}")
    return value


def parse_level(value):
    """The ``level=`` parameter: one of LEVELS, or None (no level filter)."""
    if value is not None and value not in LEVELS:
        raise ValueError(f"level must be one of {', '.join(LEVELS)}")
    return value


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
    from fastapi.responses import JSONResponse

    builds = BuildCache(cache, size)
    app = FastAPI(title="transitio index viewer", docs_url=None, redoc_url=None)
    page = (_HERE / "index_viewer.html").read_bytes()
    module = (_HERE / "index_viewer.mjs").read_bytes()

    def opened(build_id):
        build = builds.get(build_id)
        if build is None:
            raise HTTPException(404, f"{build_id}: not an available build")
        return build

    def viewed(build, spec, level):
        try:
            return build.view(parse_spec(spec), parse_level(level))
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.get("/")
    def index():
        return Response(page, media_type="text/html; charset=utf-8")

    @app.get("/static/index_viewer.mjs")
    def client_module():
        # Explicit: the platform's mimetypes table may not know .mjs.
        return Response(module, media_type="text/javascript; charset=utf-8")

    @app.get("/api/builds")
    def list_builds():
        return builds.summaries()

    @app.get("/api/builds/{build_id}/summary")
    def summary(build_id: str):
        build = opened(build_id)
        return {
            **build.snapshot,
            "id": build.id,
            "snapshot_id": build.snapshot_id,
            "served_places": int(build.served.sum()),
            "category_field": build.class_column,
            "edges_by_category": {
                k: int(v) for k, v in build.classes.value_counts().items()
            },
            "feeds_by_spec": (
                {k: int(v) for k, v in build.feeds["spec"].value_counts().items()}
                if "spec" in build.feeds.columns
                else {}
            ),
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
        spec: str | None = None,
        level: str | None = None,
    ):
        build = opened(build_id)
        view = viewed(build, spec, level)
        try:
            # A feed's served places are mostly cities: with ``feed_id`` and
            # no ``kind`` the slice covers every kind; a level names its kinds.
            if kind is None and level is not None:
                kinds = set(LEVELS[level][0])
            elif kind is None and feed_id is not None:
                kinds = None
            else:
                kinds = parse_kinds(kind)
            box = parse_bbox(bbox) if bbox is not None else None
            mask = filter_places(
                build,
                kinds=kinds,
                parent_id=parent_id,
                bbox=box,
                served=_parse_served(served),
                q=q,
                feed_id=feed_id,
                view=view,
            )
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        extra = None
        if feed_id is not None:  # the tier at which this feed serves each place
            edges = build.edges[view.edge_mask]
            mine = edges.loc[
                (edges["feed_id"] == feed_id).to_numpy(), ["place_id", "tier"]
            ]
            extra = {
                "tier": mine.drop_duplicates("place_id").set_index("place_id")["tier"]
            }
        body, overflow = places_geojson(
            build,
            mask,
            tolerance_for_zoom(zoom),
            MAX_FEATURES,
            MAX_BYTES,
            clip=box,
            extra=extra,
            view=view,
        )
        # The slice names the snapshot it came from, so the page can tell
        # when ``latest`` was republished between its summary and a slice.
        headers = {"X-Snapshot": build.snapshot_id}
        if overflow is not None:
            return JSONResponse(overflow, headers=headers)
        return Response(body, media_type="application/geo+json", headers=headers)

    @app.get("/api/builds/{build_id}/places/table")
    def table(
        build_id: str,
        kind: str | None = None,
        parent_id: str | None = None,
        served: str | None = None,
        q: str | None = None,
        sort: str = "name",
        order: str = "asc",
        offset: int = 0,
        limit: int = 50,
        spec: str | None = None,
        level: str | None = None,
    ):
        # Geometry-free, so every kind by default and no bound required.
        build = opened(build_id)
        view = viewed(build, spec, level)
        try:
            if kind is not None:
                kinds = parse_kinds(kind)
            else:
                kinds = set(LEVELS[level][0]) if level is not None else None
            mask = filter_places(
                build,
                kinds=kinds,
                parent_id=parent_id,
                served=_parse_served(served),
                q=q,
                bounded=False,
                view=view,
            )
            page = places_table(build, mask, sort, order, offset, limit, view)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return JSONResponse(page, headers={"X-Snapshot": build.snapshot_id})

    @app.get("/api/builds/{build_id}/places/{place_id}")
    def place(build_id: str, place_id: str):
        build = opened(build_id)
        body = place_record(build, place_id)
        if body is None:
            raise HTTPException(404, f"{place_id}: not a place in {build_id}")
        return Response(
            body,
            media_type="application/geo+json",
            headers={"X-Snapshot": build.snapshot_id},
        )

    @app.get("/api/builds/{build_id}/tree")
    def tree(build_id: str, root: str | None = None, depth: str = "1"):
        build = opened(build_id)
        try:
            levels = int(depth)
        except ValueError as error:
            raise HTTPException(400, "depth must be an integer") from error
        result = tree_nodes(build, root, levels)
        if result is None:
            raise HTTPException(404, f"{root}: not a place in {build_id}")
        return JSONResponse(result, headers={"X-Snapshot": build.snapshot_id})

    @app.get("/api/builds/{build_id}/feeds")
    def feeds(
        build_id: str,
        spec: str | None = None,
        level: str | None = None,
        country: str | None = None,
    ):
        build = opened(build_id)
        view = viewed(build, spec, level)
        return JSONResponse(
            feeds_table(build, view, country), headers={"X-Snapshot": build.snapshot_id}
        )

    @app.get("/api/builds/{build_id}/feeds/{feed_id}")
    def feed(build_id: str, feed_id: str):
        build = opened(build_id)
        body = feed_record(build, feed_id)
        if body is None:
            raise HTTPException(404, f"{feed_id}: not a feed in {build_id}")
        return Response(
            body,
            media_type="application/geo+json",
            headers={"X-Snapshot": build.snapshot_id},
        )

    @app.get("/api/builds/{build_id}/edges")
    def edges(
        build_id: str,
        place_id: str | None = None,
        feed_id: str | None = None,
        spec: str | None = None,
        level: str | None = None,
    ):
        build = opened(build_id)
        view = viewed(build, spec, level)
        try:
            reply = edges_of(build, place_id, feed_id, view)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return JSONResponse(reply, headers={"X-Snapshot": build.snapshot_id})

    return app


def _browser_url(host, port):
    """A URL a browser can open for a bind address.

    A wildcard bind (``0.0.0.0``, ``::``, empty) is reached through loopback,
    and an IPv6 literal is bracketed.
    """
    if host in ("", "0.0.0.0", "::"):
        host = "127.0.0.1"
    elif ":" in host:
        host = f"[{host}]"
    return f"http://{host}:{port}/"


def _open_when_started(server, url, opener=webbrowser.open, attempts=40, delay=0.25):
    """Open ``url`` once *this* uvicorn server reports it has started.

    Polls ``server.started`` and gives up quietly when the server asks to exit
    (a failed bind) or never starts within ``attempts`` polls — so another
    service already on the port can never make the browser open.
    """
    for _ in range(attempts):
        if server.started:
            opener(url)
            return True
        if server.should_exit:
            return False
        time.sleep(delay)
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Serve a built index for inspection in the browser."
    )
    parser.add_argument("--cache", default="cache", help="the build cache directory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--open", action="store_true", help="open the page in the browser"
    )
    args = parser.parse_args(argv)
    import uvicorn

    url = _browser_url(args.host, args.port)
    print(f"index viewer at {url} over {args.cache}")
    server = uvicorn.Server(
        uvicorn.Config(create_app(args.cache), host=args.host, port=args.port)
    )
    if args.open:
        threading.Thread(
            target=_open_when_started, args=(server, url), daemon=True
        ).start()
    server.run()


if __name__ == "__main__":
    main()
