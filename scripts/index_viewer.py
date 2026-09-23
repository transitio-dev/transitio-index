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
A partitioned build's tables are joined into the frames the viewer reads
(feeds with their ``partition``, places, edges with ``feed_partition`` on the
links, and from schema 8 the realtime companions keyed by static feed).
Per-country builds are written once and never rewritten; only ``cache/index``
churns.

The bounded map slices and the web app that serves them build on this
loader; ``python scripts/index_viewer.py --cache cache`` runs the app.
"""

import argparse
import collections
import datetime
import json
import math
import re
import threading
import time
import webbrowser
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import shapely
import shapely.errors

from transitio_index.builds import (  # noqa: F401  (two names for the tests)
    CATALOGUE,
    _SNAPSHOT_ERRORS,
    _BUILD_ERRORS,
    _read_file,
    _snapshot_digest,
    snapshot_files,
    _files_present,
    load_tables,
    discover,
    label_of,
    _EPOCH,
    _built_at,
    run_signature,
    DIGEST_KEYS,
    LATEST,
)
from transitio_index.merge import merge_tables, select_sources

REALTIME_COLUMNS = (
    "feed_id",
    "name",
    "source",
    "static_link_method",
    "entity_types",
    "urls",
    "partition",
)
CACHED_BUILDS = 4  # verified builds kept in memory
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
SEARCH_LIMIT = 50  # search rows returned by default ...
SEARCH_MAX = 200  # ... and at most
MEMBERS_LIMIT = 2000  # a metro record lists at most this many members
# What a search row and a record's metro and member rows carry.
SEARCH_COLUMNS = (
    "place_id",
    "name",
    "kind",
    "source_subtype",
    "statistical_area_id",
    "parent_id",
    "country_code",
    "build_id",
)
METRO_COLUMNS = ("place_id", "name", "source_subtype", "statistical_area_id")
# The metro definitions a build publishes, by ``source_subtype``; a slice can
# keep the metros of some of them.
METRO_SUBTYPES = (
    "metropolitan statistical area",
    "metropolitan region",
    "functional urban area",
    "city-region (FAO)",
)
MEMBER_COLUMNS = ("place_id", "name", "kind", "served", "feed_count")
KIND_RANK = {"country": 0, "region": 1, "city": 2}  # the tree's order
SERVICE_STATS = ("stops", "routes", "departures_per_day")
# The places table's columns; ``build_id`` in the catalogue only.
TABLE_COLUMNS = ("place_id", "name", "kind", "parent_id", "country_code", "build_id")
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
    "build_id",
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
EDGE_FEED_COLUMNS = (
    "name",
    "spec",
    "source",
    "crawl_status",
    "stop_count",
    "partition",
)
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
    "service_start",
    "service_end",
    "build_id",
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
    "realtime_feed_ids",
    "partition",
    "service_start",
    "service_end",
    "build_id",
)


def _one_line(values):
    """String values with any line break (LF, CRLF or CR) made a space: one
    value is one line."""
    values = pc.fill_null(values.cast(pa.string()), "")
    return pc.replace_substring_regex(values, pattern=r"\r\n|\r|\n", replacement=" ")


def _flat(values):
    """String values as the search text holds them: one line, lower-cased
    by Arrow — the one case mapping the text, the query and the ranks share."""
    return pc.utf8_lower(_one_line(values))


def _lines(offsets, values):
    """The rows of a list column joined with newlines, from the ``offsets``
    into ``values`` the array exposes; a null value joins as nothing."""
    values = pa.ListArray.from_arrays(offsets, _one_line(values))
    return pc.binary_join(values, "\n")


def _search_text(places):
    """What a query matches, one string per place: the name, the aliases and
    the names in other languages, one per line, lower-cased. Arrow list
    operations throughout; a build without aliases or names has the name."""
    parts = [_one_line(places["name"].combine_chunks())]
    if "aliases" in places.column_names:
        aliases = places["aliases"].combine_chunks()
        if pa.types.is_list(aliases.type):
            parts.append(_lines(aliases.offsets, aliases.values))
    if "names" in places.column_names:
        names = places["names"].combine_chunks()
        if pa.types.is_map(names.type):
            parts.append(_lines(names.offsets, names.items))
    joined = pc.binary_join_element_wise(*parts, "\n", null_handling="skip")
    return pc.utf8_lower(joined)  # the values are one line each already


class Build:
    """One verified build: its tables, its geometry and per-place summaries."""

    def __init__(self, build_id, path, snapshot, digests, tables):
        self.id = build_id
        self.path = path
        self.snapshot = snapshot
        self.digests = digests
        self.snapshot_id = _snapshot_digest(snapshot)
        places = tables["places.parquet"].to_pandas()
        self.search_text = _search_text(tables["places.parquet"])
        self.geoms = shapely.from_wkb(places["geometry"].to_numpy())
        self.bounds = shapely.bounds(self.geoms)
        # Positional frames: a parquet written with a pandas index would
        # restore it, and every lookup here is by row position.
        self.places = places.drop(columns=["geometry"]).reset_index(drop=True)
        self.edges = tables["edges.parquet"].to_pandas().reset_index(drop=True)
        self.feeds = tables["feeds.parquet"].to_pandas().reset_index(drop=True)
        realtime = tables.get("realtime.parquet")
        self.realtime = (
            realtime.to_pandas().reset_index(drop=True)
            if realtime is not None
            else pd.DataFrame(columns=list(REALTIME_COLUMNS) + ["static_feed_id"])
        )
        # The companions' endpoints parsed once, like ``service``: malformed
        # JSON or a non-list entity_types makes the build unavailable.
        self.realtime_urls = [_loads(v) for v in self.realtime["urls"].to_numpy()]
        for types in self.realtime["entity_types"].to_numpy():
            if not isinstance(types, (list, np.ndarray)):
                raise TypeError("entity_types is not a list")
        self._row_of = pd.Series(
            np.arange(len(self.places)), index=self.places["place_id"].to_numpy()
        )
        self.metros_of = _metros_of(self.places)
        # The ``service`` JSON of places and edges, parsed once here and
        # normalized (non-finite → None); nothing parses it again per request,
        # and malformed JSON makes the build unavailable rather than a 500.
        self.service = [_loads(v) for v in self.places["service"].to_numpy()]
        # Schema 9: the validity of each place's feeds, parsed once like the
        # service; None before it.
        self.validity = (
            [_loads(v) for v in self.places["validity"].to_numpy()]
            if "validity" in self.places.columns
            else None
        )
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
        top = ids.map(best_class(build, keep)).astype(object)
        self.category = top.where(top.notna(), None).to_numpy()
        self.feed_table = _feed_table(build, keep)


def best_class(build, mask):
    """The highest class per served place over the edges ``mask`` keeps.

    A Series indexed by place_id; a place with edges of no known class is
    None.
    """
    names = build.class_names
    rank = build.classes[mask].map({name: i for i, name in enumerate(names)})
    best = rank.groupby(build.edges.loc[mask, "place_id"].to_numpy()).min()
    lookup = np.array(names + (None,), dtype=object)
    return pd.Series(
        lookup[best.fillna(len(names)).astype(int).to_numpy()], index=best.index
    )


def load_build(build_id, path, read_bytes=_read_file):
    """The verified build at ``path``, or None while it is mid-publish or
    when its tables are not a build's (undecodable WKB, malformed service)."""
    loaded = load_tables(path, read_bytes)
    if loaded is None:
        return None
    try:
        return Build(build_id, Path(path), *loaded)
    except _BUILD_ERRORS:
        return None


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


def catalogue_row(build, skipped):
    """The catalogue's listing row from its assembled snapshot; incomplete,
    with the runs skipped, when nothing could be assembled."""
    if build is None:
        return {
            "id": CATALOGUE,
            "complete": False,
            "built_at": None,
            "sources": 0,
            "skipped": skipped,
        }
    snapshot = build.snapshot
    return {
        "id": CATALOGUE,
        "complete": True,
        "built_at": snapshot["built_at"],
        "counts": snapshot["counts"],
        "sources": len(snapshot["sources"]),
        "skipped": snapshot["skipped"],
    }


def _merged_build(cache, loaded, skipped):
    if not loaded:
        return None
    try:
        sources = [(build_id, s, t) for build_id, _, s, t, _ in loaded]
        snapshot, tables = merge_tables(sources, skipped)
        return Build(CATALOGUE, Path(cache), snapshot, {}, tables)
    except _BUILD_ERRORS:
        return None


def assemble_catalogue(cache, sources, skipped, read_bytes=_read_file):
    """``(build, skipped, unverified)``: the catalogue ``Build`` over
    ``sources`` (None when none of them loads); every run skipped — the
    selection's, plus a source that does not verify when read: a snapshot
    rewritten since it was selected, a digest mismatch, or tables that are
    not a build's (undecodable WKB, malformed service), found source by
    source only when the merged build fails; and, for those unverified
    runs, ``{build_id: (path, snapshot, signature)}``, the file signature a
    repair changes, taken before the run was read so that a repair landing
    right after the read is not missed."""
    loaded, skipped, unverified = [], list(skipped), {}

    def refuse(build_id, path, snapshot, signature):
        skipped.append({"id": build_id, "reason": "does not verify"})
        unverified[build_id] = (path, snapshot, signature)

    for build_id, path, snapshot in sources:
        signature = run_signature(path, snapshot)
        verified = load_tables(path, read_bytes, expected=snapshot)
        if verified is None:
            refuse(build_id, path, snapshot, signature)
        else:
            loaded.append((build_id, path, verified[0], verified[2], signature))
    build = _merged_build(cache, loaded, skipped)
    if build is None and loaded:
        sound = []
        for source in loaded:
            build_id, path, snapshot, tables, signature = source
            try:
                Build(build_id, Path(cache), snapshot, {}, tables)
                sound.append(source)
            except _BUILD_ERRORS:
                refuse(build_id, path, snapshot, signature)
        build = _merged_build(cache, sound, skipped)
    return build, skipped, unverified


class BuildCache:
    """Verified builds by id, reloaded exactly when a snapshot's digests change.

    ``get`` re-reads the small ``snapshot.json`` on every call and compares its
    digests with the cached build's: the churning ``cache/index`` is reloaded
    when it changes, a per-country build is hashed once. The catalogue is
    assembled on its first request and again when the set of its sources or
    any source's snapshot changes. The most recent ``size`` builds are kept,
    the catalogue among them.
    """

    def __init__(self, cache, size=CACHED_BUILDS, read_bytes=_read_file):
        self.cache = Path(cache)
        self.size = size
        self.read_bytes = read_bytes
        self._builds = collections.OrderedDict()
        # What the last catalogue assembly saw: its key (the sources and the
        # skipped runs; None once the catalogue is evicted), the runs it
        # skipped, and the file signatures of those that did not verify.
        self._catalogue_key = None
        self._catalogue_skipped = []
        self._catalogue_unverified = {}
        # The web app's handlers run in worker threads; a get is one
        # lookup-load-evict transaction, so it holds the lock throughout.
        self._lock = threading.Lock()

    def summaries(self):
        """The catalogue's row first (assembled now if need be), then every
        discovered build's."""
        rows = [
            describe(build_id, path, self.read_bytes)
            for build_id, path in discover(self.cache).items()
        ]
        with self._lock:
            catalogue = self._catalogue()
            skipped = self._catalogue_skipped
        return [catalogue_row(catalogue, skipped), *rows]

    def get(self, build_id):
        with self._lock:
            if build_id == CATALOGUE:
                return self._catalogue()
            return self._get(build_id)

    def _keep(self, build_id, build):
        self._builds[build_id] = build
        self._builds.move_to_end(build_id)
        while len(self._builds) > self.size:
            evicted, _ = self._builds.popitem(last=False)
            if evicted == CATALOGUE:
                self._catalogue_key = None
        return build

    def _catalogue(self):
        sources, skipped = select_sources(self.cache / "builds", self.read_bytes)
        key = [(build_id, _snapshot_digest(s)) for build_id, _, s in sources]
        key += [(run["id"], run["reason"]) for run in skipped]
        if self._catalogue_key == key and not self._repaired():
            cached = self._builds.get(CATALOGUE)
            if cached is not None:
                self._builds.move_to_end(CATALOGUE)
            return cached
        build, skipped, unverified = assemble_catalogue(
            self.cache, sources, skipped, self.read_bytes
        )
        self._catalogue_key, self._catalogue_skipped = key, skipped
        self._catalogue_unverified = unverified
        if build is None:
            self._builds.pop(CATALOGUE, None)
            return None
        return self._keep(CATALOGUE, build)

    def _repaired(self):
        """A run that did not verify at the last assembly has changed since."""
        return any(
            run_signature(path, snapshot) != signature
            for path, snapshot, signature in self._catalogue_unverified.values()
        )

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
        return self._keep(build_id, build)


def parse_kinds(value):
    """The kinds a ``kind=`` parameter names: the default pair, all, or a list."""
    if value is None:
        return set(DEFAULT_KINDS)
    if value == "all":
        return None
    return {kind.strip() for kind in value.split(",") if kind.strip()}


def parse_subtypes(value):
    """The metro definitions a ``subtype=`` parameter names, as a set of
    ``source_subtype`` values: None (every definition) when omitted, empty
    when blank."""
    if value is None:
        return None
    subtypes = {part.strip() for part in value.split(",") if part.strip()}
    unknown = subtypes - set(METRO_SUBTYPES)
    if unknown:
        raise ValueError(f"subtype must be one of {', '.join(METRO_SUBTYPES)}")
    return subtypes


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
    subtypes=None,
):
    """A boolean mask over the build's places for one slice.

    ``kinds`` is a collection of kinds (the default pair when omitted), or
    None for every kind. With ``bounded`` (the default, for map slices) a
    slice that can include cities must be bounded by ``parent_id`` or
    ``bbox`` (``ValueError`` otherwise); the geometry-free table passes
    ``bounded=False``. ``q`` matches the search text (name, aliases and names
    in other languages, any case). ``feed_id`` keeps the places that feed
    serves, through the edges. ``served`` and ``feed_id`` read the ``view``
    (the build's default when omitted). ``subtypes`` keeps the metros of the
    definitions named (their ``source_subtype``) and every other kind as it
    is; None keeps every metro. Every test is a vectorized mask over the
    build's arrays.
    """
    places = build.places
    view = view or build.view()
    unbounded = parent_id is None and bbox is None
    if bounded and unbounded and (kinds is None or "city" in kinds):
        raise ValueError("a slice that includes cities needs parent_id or bbox")
    mask = np.ones(len(places), dtype=bool)
    if kinds is not None:
        mask &= places["kind"].isin(set(kinds)).to_numpy()
    if subtypes is not None:
        metro = (places["kind"] == "metro").to_numpy()
        named = np.zeros(len(places), dtype=bool)
        if "source_subtype" in places.columns:
            named = places["source_subtype"].isin(set(subtypes)).to_numpy()
        mask &= ~metro | named
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
        hits = pc.match_substring(build.search_text, q, ignore_case=True)
        mask &= pc.fill_null(hits, False).to_numpy(zero_copy_only=False)
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
    table = places[[c for c in TABLE_COLUMNS if c in places.columns]].copy()
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


def _ancestors(build, parent):
    """The chain above a place, root first, as ``{place_id, name, kind}``."""
    ancestors = []
    for _ in range(8):  # a bounded walk: a cycle in the data cannot loop
        if not isinstance(parent, str) or parent not in build._row_of.index:
            break
        ancestor = build.places.iloc[int(build._row_of[parent])]
        ancestors.append(
            {"place_id": parent, "name": ancestor["name"], "kind": ancestor["kind"]}
        )
        parent = ancestor["parent_id"]
    return ancestors[::-1]


def _list(value):
    """An Arrow list cell as a list; a null cell is empty."""
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value if isinstance(value, list) else []


def _matched(place, needle):
    """The alias or other-language name of ``place`` containing ``needle``
    when its name does not, else None; the value as stored."""
    names = _as_map(place.get("names")) or {}
    candidates = [*_list(place.get("aliases")), *names.values()]
    candidates = [c for c in candidates if isinstance(c, str)]
    flat = _flat(pa.array([place["name"], *candidates], pa.string())).to_pylist()
    if needle in flat[0]:
        return None
    for candidate, line in zip(candidates, flat[1:]):
        if needle in line:
            return candidate
    return None


def search_places(build, q, limit=SEARCH_LIMIT, view=None, subtypes=None):
    """``{"total", "rows"}``: the places ``q`` matches, the best ``limit`` first.

    Ranked: the name equals ``q`` (any case); an alias or a name in another
    language does; a word of the name starts with ``q``; ``q`` is a substring
    of any of them. Within a rank served places first, then more feeds, then
    the name. A row carries the place's ``chain`` (its ancestors' names, root
    first), the view's served flag, feed count and class, and ``matched``:
    the alias or other name that matched when the name itself does not
    contain ``q``. ``subtypes`` keeps the metros of the definitions named, as
    in :func:`filter_places`.
    """
    q = " ".join(q.split())  # one space between words, none around, no newline
    if len(q) < 2:
        raise ValueError("q needs at least two characters")
    if limit < 1:
        raise ValueError("limit must be at least 1")
    view = view or build.view()
    hits = np.flatnonzero(
        filter_places(build, None, q=q, bounded=False, view=view, subtypes=subtypes)
    )
    if not hits.size:
        return {"total": 0, "rows": []}
    needle = _flat(pa.array([q]))[0].as_py()
    pattern = re.escape(needle)
    text = build.search_text.take(pa.array(hits))
    # The name as the text holds it (its first line), so that a name with a
    # newline in it ranks as the text matched it.
    names = pd.Series(pc.list_element(pc.split_pattern(text, "\n", max_splits=1), 0))
    exact_line = pc.fill_null(pc.match_substring_regex(text, f"(?m)^{pattern}$"), False)
    rank = np.select(
        [
            (names == needle).to_numpy(),
            exact_line.to_numpy(zero_copy_only=False),
            names.str.contains(rf"\b{pattern}", regex=True, na=False).to_numpy(),
        ],
        [0, 1, 2],
        default=3,
    )
    order = pd.DataFrame(
        {
            "rank": rank,
            "unserved": ~view.served[hits],
            "feeds": -view.feed_count[hits],
            "name": names.to_numpy(),
        }
    ).sort_values(["rank", "unserved", "feeds", "name"], kind="stable")
    top = hits[order.index[: min(limit, SEARCH_MAX)]]
    places = build.places.iloc[top]
    rows = _json_ready(places[[c for c in SEARCH_COLUMNS if c in places.columns]])
    for row, (_, place), position in zip(rows, places.iterrows(), top):
        row["chain"] = [a["name"] for a in _ancestors(build, place["parent_id"])]
        row["served"] = bool(view.served[position])
        row["feed_count"] = int(view.feed_count[position])
        row[build.class_column] = view.category[position]
        row["matched"] = _matched(place, needle)
    return {"total": int(hits.size), "rows": rows}


def _metros_of(places):
    """``{member place_id: [metro place_ids]}`` from the metros' member lists;
    a member's own ``metro_ids`` is empty in the published index."""
    if "member_ids" not in places.columns:
        return {}
    members = places[["place_id", "member_ids"]].explode("member_ids").dropna()
    return members.groupby("member_ids")["place_id"].agg(list).to_dict()


def _rows_of(build, ids):
    """The row positions of the places ``ids`` names, those the build holds."""
    present = [i for i in _list(ids) if i in build._row_of.index]
    return build._row_of[present].to_numpy() if present else np.array([], dtype=int)


def _metro_rows(build, place_id, ids):
    """The metros a place belongs to — those listing it as a member, and
    ``ids`` — as ``METRO_COLUMNS`` records."""
    ids = dict.fromkeys([*build.metros_of.get(place_id, ()), *_list(ids)])
    places = build.places.iloc[_rows_of(build, list(ids))]
    return _json_ready(places[[c for c in METRO_COLUMNS if c in places.columns]])


def _member_rows(build, ids):
    """``{"count", "served", "rows"}``: how many members ``ids`` names, and
    of those the build holds, how many are served and their rows, the most
    served first, at most ``MEMBERS_LIMIT`` of them."""
    rows = build.table.iloc[_rows_of(build, ids)][list(MEMBER_COLUMNS)]
    rows = rows.sort_values(
        ["feed_count", "name"], ascending=[False, True], kind="stable"
    )
    return {
        "count": len(_list(ids)),
        "served": int(rows["served"].sum()),
        "rows": _json_ready(rows.iloc[:MEMBERS_LIMIT]),
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
    ``ancestors`` root first, a ``children`` summary, its ``metros`` and
    ``members`` (the metro rows it belongs to; the member rows it holds), its
    ``edges`` joined with the feed table, the distinct feeds by spec and, on
    schema 7, the feeds reaching it over a border by partition
    (``reached_from``).
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
    if build.validity is not None:
        props["validity"] = build.validity[row]
    props["served"] = bool(build.served[row])
    props["feed_count"] = int(build.feed_count[row])
    props["bbox"] = [_cell(v) for v in build.bounds[row]]
    props["ancestors"] = _ancestors(build, place["parent_id"])
    props["metros"] = _metro_rows(build, place_id, place.get("metro_ids"))
    props["members"] = _member_rows(build, place.get("member_ids"))
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
    if "cross_border" in distinct.columns:
        # The feeds reaching the place over a border, by the partition
        # holding each (the links carry it; a domestic edge has none).
        foreign = distinct[distinct["cross_border"].fillna(False).astype(bool)]
        by_partition = foreign["feed_partition"].dropna().value_counts()
        props["reached_from"] = {k: int(v) for k, v in by_partition.items()}
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
    companions = build.realtime.groupby("static_feed_id").size()
    table["realtime"] = (
        table["feed_id"].map(companions).fillna(0).astype(int).to_numpy()
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
    props["realtime"] = realtime_of(build, feed_id)
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


def realtime_of(build, feed_id):
    """The GTFS-RT companions of a static feed, as records with their
    endpoints (``urls`` parsed) and entity types; empty before schema 8."""
    positions = np.flatnonzero((build.realtime["static_feed_id"] == feed_id).to_numpy())
    rows = build.realtime.iloc[positions][list(REALTIME_COLUMNS)]
    records = [{k: _cell(v) for k, v in r.items()} for r in _json_ready(rows)]
    for record, position in zip(records, positions):
        record["urls"] = build.realtime_urls[position] or {}
    return records


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
    columns = [c for c in ("feed_id", "name", "spec") if c in build.feeds.columns]
    if len(columns) > 1:
        feeds = build.feeds[columns].rename(columns={"name": "feed_name"})
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
    for key, by_place in (extra or {}).items():  # e.g. a feed's class per place
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
            "realtime": {
                "feeds": int(len(build.realtime)),
                "linked": int(
                    build.realtime["static_feed_id"]
                    .isin(set(build.feeds["feed_id"]))
                    .sum()
                ),
            },
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
        subtype: str | None = None,
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
                subtypes=parse_subtypes(subtype),
            )
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        extra = None
        if feed_id is not None:  # this feed's own highest class at each place
            mine = view.edge_mask & (build.edges["feed_id"] == feed_id).to_numpy()
            extra = {build.class_column: best_class(build, mine)}
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
        subtype: str | None = None,
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
                subtypes=parse_subtypes(subtype),
            )
            page = places_table(build, mask, sort, order, offset, limit, view)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return JSONResponse(page, headers={"X-Snapshot": build.snapshot_id})

    @app.get("/api/builds/{build_id}/search")
    def search(
        build_id: str,
        q: str = "",
        limit: int = SEARCH_LIMIT,
        spec: str | None = None,
        level: str | None = None,
        subtype: str | None = None,
    ):
        build = opened(build_id)
        view = viewed(build, spec, level)
        try:
            reply = search_places(build, q, limit, view, parse_subtypes(subtype))
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return JSONResponse(reply, headers={"X-Snapshot": build.snapshot_id})

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
