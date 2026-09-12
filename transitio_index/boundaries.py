"""The boundary lookup: which admin divisions contain a point.

The gazetteer's expansion pass and the coverage stage both ask "which admin
unit contains this stop?", and neither stop coordinates nor the seeded rows can
answer that. This module answers it against the full pinned Overture release
without mirroring it: ``division_area`` is cloud-native GeoParquet whose row
groups carry ``bbox`` statistics, so a spatial query reads the file footers and
only the row groups whose boxes intersect the query — the COG access pattern —
and the divisions theme supplies the matching subtype/hierarchy metadata.

What a query touches is memoized under
``cache/boundary_lookup/<release>-t<tolerance>/`` as
GeoParquet parts of the division polygons (simplified to the shipping
tolerance) plus their metadata, keyed by the release, so repeated queries
within and across builds read locally and both consumers see the same
geometry. The memo accumulates across builds and only ever grows by appending
a part; the coverage file names the parts it was computed with. Containment
runs locally over the memoized polygons; the cloud filter only selects
candidates by bounding box.
"""

import functools
import io
import itertools
import json
import math
import os
import re

import numpy as np
import pyarrow.dataset as ds
import shapely
from shapely.strtree import STRtree

from transitio_index import geometry, overture, store
from transitio_index.progress import progress

# The single-file memo written before parts existed; read as one part.
DIVISIONS_FILE = "divisions.parquet"
PART_PATTERN = re.compile(r"divisions-(\d{4,})\.parquet")
# A part is split until it is well under the store's artifact ceiling.
PART_BYTES = store.MAX_ARTIFACT_BYTES // 2
COVERED_FILE = "covered.jsonl"
MEMO_CRS = "EPSG:4326"

# The memo's non-geometry columns: plain scalars, and the nested fields as JSON
# strings (light supportive data beside the GeoParquet geometry column).
_SCALAR_FIELDS = (
    "overture_id",
    "subtype",
    "source_subtype",
    "kind",
    "admin_level",
    "country",
    "name",
    "wikidata",
)
_JSON_FIELDS = ("names", "ancestors", "osm_relation_ids", "sources")
_MEMO_COLUMNS = ["division_id", *_SCALAR_FIELDS, *_JSON_FIELDS]

AREA_COLUMNS = ["division_id", "geometry", "country", "is_land"]
# The ``bbox`` struct's fields, in the order the footprint and row-box readers
# take them: the footer statistics of these columns bound each row group.
_BBOX_FIELDS = ("xmin", "xmax", "ymin", "ymax")

# Most specific first: the order ``divisions_at`` returns containing divisions.
_SPECIFICITY = {"locality": 0, "localadmin": 1, "county": 2, "region": 3, "country": 4}


def _memo_fields(record):
    """The memo's non-geometry columns for a division record."""
    fields = {"division_id": record["division_id"]}
    for name in _SCALAR_FIELDS:
        fields[name] = record.get(name)
    for name in _JSON_FIELDS:
        fields[name] = json.dumps(record.get(name), ensure_ascii=False)
    return fields


def _record_from_row(attrs):
    """A division record's metadata rebuilt from a memo row's columns."""
    record = {"division_id": attrs["division_id"]}
    for name in _SCALAR_FIELDS:
        record[name] = attrs.get(name)
    for name in _JSON_FIELDS:
        record[name] = json.loads(attrs[name])
    record["geoms"] = []
    return record


def _read_memo(directory, name):
    """``(records, damaged)`` from the GeoParquet memo part ``name``.

    Opened through the store like any cache entry — a symlink or anything but
    a regular file is refused — and parsed from that descriptor. A row with a
    missing id or a geometry that is not a valid polygon is dropped and flags
    the memo damaged, the same validation the fresh scan applies, so a memo
    written before validation existed cannot seed the index.
    """
    import geopandas as gpd

    # No size ceiling: a memo from before parts existed can exceed the store's
    # artifact limit, which is what the parts are for.
    handle = store.open_regular(directory, name, limit=None)
    with os.fdopen(handle, "rb") as opened:
        frame = gpd.read_parquet(opened)
    geoms = list(frame.geometry)
    non_geom = frame.drop(columns=frame.geometry.name)
    # A null scalar column reads back as NaN, and a NaN wikidata or name is
    # truthy — it would be minted as identity rather than falling to the
    # Overture-id path. Restore the None the memo was written with.
    non_geom = non_geom.astype(object).where(non_geom.notna(), None)
    attrs = non_geom.to_dict(orient="records")
    records = {}
    damaged = False
    for row_attrs, geom in zip(attrs, geoms):
        division_id = row_attrs.get("division_id")
        if not division_id or geom is None or not geometry._valid_polygon(geom):
            damaged = True
            continue
        record = records.get(division_id)
        if record is None:
            try:
                record = _record_from_row(row_attrs)
            except (TypeError, ValueError):
                damaged = True
                continue
            records[division_id] = record
        record["geoms"].append(geom)
    return records, damaged


def memo_name(release, tolerance=None):
    """The memo directory for ``release`` at the simplification ``tolerance``
    (the shipping tolerance by default): a memo holds polygons simplified at
    one tolerance, so another tolerance is another memo, never a stale read."""
    tolerance = geometry.SIMPLIFY_TOLERANCE_DEG if tolerance is None else tolerance
    return f"{release}-t{tolerance!r}"


def _part_name(part):
    return part == DIVISIONS_FILE or bool(PART_PATTERN.fullmatch(part))


def _next_part_name(parts):
    """The part name after the highest numbered one in ``parts``."""
    numbers = (PART_PATTERN.fullmatch(name) for name in parts)
    last = max((int(match.group(1)) for match in numbers if match), default=0)
    return f"divisions-{last + 1:04d}.parquet"


def _part_chunks(gdf):
    """``gdf`` serialized as GeoParquet in pieces under ``PART_BYTES``.

    The rows are halved until a piece fits; a single row is written as it is.
    """
    sink = io.BytesIO()
    gdf.to_parquet(sink)
    data = sink.getvalue()
    if len(data) <= PART_BYTES or len(gdf) <= 1:
        yield data
        return
    half = len(gdf) // 2
    yield from _part_chunks(gdf.iloc[:half])
    yield from _part_chunks(gdf.iloc[half:])


def _wkb_set(geoms):
    return set(shapely.to_wkb(np.asarray(geoms, dtype=object)))


def _valid_box(box):
    return (
        isinstance(box, list)
        and len(box) == 4
        and all(isinstance(v, (int, float)) and math.isfinite(v) for v in box)
    )


def _boxes(rectangles):
    """``(xmin, ymin, xmax, ymax)`` tuples as shapely boxes, vectorized."""
    xmin, ymin, xmax, ymax = np.asarray(rectangles, dtype=float).T
    return shapely.box(xmin, ymin, xmax, ymax)


def _row_boxes(bbox):
    """The rows' own bounding boxes from a ``bbox`` struct column."""
    fields = [bbox.field(name).to_numpy(zero_copy_only=False) for name in _BBOX_FIELDS]
    xmin, xmax, ymin, ymax = fields
    return shapely.box(xmin, ymin, xmax, ymax)


def _footprint(row_group, columns):
    """A row group's spatial footprint from its ``bbox`` column statistics as a
    shapely box, or ``None`` when a statistic is missing."""
    bounds = []
    for name, side in zip(_BBOX_FIELDS, ("min", "max", "min", "max")):
        if f"bbox.{name}" not in columns:
            return None
        statistics = row_group.column(columns[f"bbox.{name}"]).statistics
        if statistics is None or not statistics.has_min_max:
            return None
        bounds.append(getattr(statistics, side))
    xmin, xmax, ymin, ymax = bounds
    return shapely.box(xmin, ymin, xmax, ymax)


class BoundaryLookup:
    """Point-in-division queries over the pinned release, locally memoized.

    ``ensure`` makes a set of bounding boxes queryable (fetching what the memo
    lacks); ``divisions_at`` then answers exactly, most specific first. The
    datasets are injectable for tests; a lookup opened without them still
    answers anything the memo already covers.
    """

    def __init__(
        self,
        cache_dir,
        *,
        release=overture.OVERTURE_RELEASE,
        area_dataset=None,
        division_dataset=None,
        reopen_area=None,
        reopen_division=None,
    ):
        self.release = release
        self._area_dataset = area_dataset
        self._division_dataset = division_dataset
        # Fresh-connection openers for a scan that stalls (see geometry.retrying);
        # a reopened dataset replaces the instance's for the boxes that follow.
        self._reopen_area = reopen_area
        self._reopen_division = reopen_division
        root = store.open_subdir(cache_dir, "boundary_lookup")
        try:
            self._dir = store.open_subdir(root.path, memo_name(release))
        finally:
            root.close()
        self._records = {}
        self._covered = []
        self._covered_tree = None
        self._parts = []
        self._listed = []
        self._pending = {}
        self._tree = None
        self._tree_entries = []
        self._load()

    def close(self):
        self._dir.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def _load(self):
        # Read under the writer lock so the divisions and coverage files (which
        # a writer replaces one after the other) are always observed as a
        # coherent pair, never an old-divisions / new-coverage mix.
        with store.exclusive_writer(self._dir):
            self._read_state()

    def _read_state(self):
        """Read the memo from disk into ``_records`` / ``_covered`` / ``_parts``.

        Resets the in-memory state first, so it doubles as a reload. Must be
        called while holding the writer lock (see :meth:`_load`, :meth:`ensure`).
        """
        self._records = {}
        self._covered = []
        self._covered_tree = None
        self._parts = []
        self._listed = []
        self._pending = {}
        damaged = False
        listed = None
        rows = []
        try:
            handle = store.open_regular(self._dir, COVERED_FILE)
        except store.MissingEntry:
            pass
        except store.StoreError:
            damaged = True
        else:
            try:
                rows = store.parse_jsonl(store.read_all(handle))
            finally:
                os.close(handle)
        for row in rows:
            if not isinstance(row, dict):
                damaged = True
            elif "parts" in row:
                parts = row["parts"]
                if (
                    listed is None
                    and isinstance(parts, list)
                    and all(isinstance(p, str) and _part_name(p) for p in parts)
                ):
                    listed = parts
                else:
                    damaged = True
            elif _valid_box(row.get("box")):
                self._covered.append(tuple(row["box"]))
            else:
                damaged = True
        if listed is None:
            # A memo from before parts existed is the one file, named nowhere;
            # coverage without it is damage like any listed part gone. (An
            # EMPTY memo is legitimate: an all-ocean box finds no divisions.)
            listed = [DIVISIONS_FILE]
        self._listed = listed
        loaded = []
        for name in listed:
            try:
                loaded.append((name, *_read_memo(self._dir, name)))
            except store.MissingEntry:
                # A listed part that is gone would leave its boxes covered
                # with nothing to answer them.
                damaged = True
            except Exception:  # noqa: B902 - geopandas/pyarrow raise their own
                # An unreadable part (a corrupt or partial parquet, a symlink)
                # is damage, not fatal: coverage is cleared below and the
                # next ensure() refetches.
                loaded.append((name, {}, True))
        # The healthy parts first, so what a damaged part still holds is
        # deduplicated against them; it is dropped from the list, written
        # into a fresh part at the next persist, and the file reclaimed.
        for name, records, bad in sorted(loaded, key=lambda item: item[2]):
            if bad:
                damaged = True
            else:
                self._parts.append(name)
            self._merge(records, pending=bad)
        if damaged:
            # Damage must not count as covered: the dropped entries would
            # become permanent false negatives, so coverage is cleared and
            # the next ensure() refetches and repairs the memo.
            self._covered = []
        self._tree = None

    def _merge(self, records, pending=False):
        """Add a part's records to ``_records``, each polygon once by WKB;
        with ``pending`` the polygons taken are also queued for re-writing."""
        for division_id, record in records.items():
            known = self._records.get(division_id)
            if known is None:
                self._records[division_id] = record
                new = record["geoms"]
            else:
                existing = _wkb_set(known["geoms"])
                keys = shapely.to_wkb(np.asarray(record["geoms"], dtype=object))
                new = [g for g, k in zip(record["geoms"], keys) if k not in existing]
                known["geoms"].extend(new)
            if pending and new:
                self._pending.setdefault(division_id, []).extend(new)

    def _uncovered(self, boxes):
        """The boxes not already inside a covered box (an STRtree over the
        covered ones, rebuilt when coverage changes)."""
        if not boxes or not self._covered:
            return list(boxes)
        if self._covered_tree is None:
            self._covered_tree = STRtree(_boxes(self._covered))
        hits = self._covered_tree.query(_boxes(boxes), predicate="covered_by")
        inside = set(hits[0].tolist())
        return [box for i, box in enumerate(boxes) if i not in inside]

    def _persist(self, added):
        """Append the geometry ``added`` (``{division_id: [polygon]}``) as new
        parts, then rewrite the coverage file naming every part.

        The parts already on disk are never rewritten, so a persist costs what
        it adds, not the size of the memo; what a damaged part still held is
        appended along with it.
        """
        import geopandas as gpd
        import pandas as pd

        for division_id, geoms in self._pending.items():
            added.setdefault(division_id, []).extend(geoms)
        self._pending = {}
        rows, geoms = [], []
        for division_id, new in added.items():
            rows.extend([_memo_fields(self._records[division_id])] * len(new))
            geoms.extend(new)
        if rows:
            frame = pd.DataFrame(rows, columns=_MEMO_COLUMNS)
            gdf = gpd.GeoDataFrame(frame, geometry=gpd.GeoSeries(geoms, crs=MEMO_CRS))
            # Named above every part the coverage file names or the directory
            # holds: a part that file still names — damaged, or gone — is never
            # given new content before the file stops naming it.
            taken = set(self._listed).union(
                self._parts, (n for n in self._dir.listdir() if _part_name(n))
            )
            for data in _part_chunks(gdf):
                name = _next_part_name(taken)
                store.write_bytes(self._dir, name, data)
                self._parts.append(name)
                taken.add(name)
        store.write_file(
            self._dir,
            COVERED_FILE,
            lambda: itertools.chain(
                [json.dumps({"parts": self._parts}) + "\n"],
                (json.dumps({"box": list(b)}) + "\n" for b in self._covered),
            ),
        )
        # A part no list names — written by a persist that never reached the
        # coverage file, or dropped as damaged above — is reclaimed. What
        # cannot be unlinked (a directory under a part's name) stays, and so
        # does its claim on the name.
        for name in self._dir.listdir():
            if _part_name(name) and name not in self._parts:
                try:
                    self._dir.unlink(name)
                except OSError:
                    pass

    def ensure(self, boxes):
        """Make every box queryable; returns how many new divisions arrived.

        Boxes already inside a covered box cost nothing. The rest select, from
        the files' footer statistics, the row groups whose footprint they
        touch; each is read once, its land polygons whose own bbox touches a
        box parsed (malformed WKB skipped) and their divisions' metadata
        resolved, then memoized, and every box is recorded as covered.
        """
        boxes = [tuple(box) for box in boxes]
        if not self._uncovered(boxes):
            return 0
        if self._area_dataset is None or self._division_dataset is None:
            raise store.StoreError(
                "boundary lookup needs its datasets to fetch uncovered boxes"
            )
        with store.exclusive_writer(self._dir):
            # Reload under the lock so a concurrent writer's additions are seen
            # and never overwritten, then recompute what is still uncovered.
            self._read_state()
            needed = self._uncovered(boxes)
            if not needed:
                return 0
            cells = STRtree(_boxes(needed))
            # The row groups whose footprint touches a needed rectangle, each
            # read once whatever rectangles touch it: the rectangles are never
            # widened into their bounding box, so a chain of cells across a
            # continent reads the geometry under the cells, not the continent.
            # Deduplicated by canonical WKB: a division already cached may
            # surface a further component in a later read — merged in, never
            # discarded. Geometry is processed per batch with the vectorized
            # shapely API, not one polygon at a time.
            self._area_dataset, groups = geometry.retrying(
                self._open_area,
                self._reopen_area,
                functools.partial(self._row_groups, cells=cells),
            )
            polygons = {}
            for group in progress(groups, "boundary"):
                # One row group per attempt, restartable: a stalled read is
                # retried on a fresh connection and its partial result
                # discarded. The dataset the successful attempt opened serves
                # the reads that follow — adopted here, on this thread, never
                # from inside an attempt a deadline may have abandoned.
                self._area_dataset, found = geometry.retrying(
                    self._open_area,
                    self._reopen_area,
                    functools.partial(self._group_polygons, group=group, cells=cells),
                )
                for division_id, geoms in found.items():
                    polygons.setdefault(division_id, {}).update(geoms)
            new_ids = sorted(set(polygons) - set(self._records))
            metadata = self._division_metadata(new_ids)
            added = {}
            for division_id, found in polygons.items():
                record = self._records.get(division_id)
                if record is None:
                    record = metadata.get(division_id) or {
                        "overture_id": division_id,
                        "subtype": None,
                        "kind": None,
                        "country": None,
                        "name": None,
                        "names": {},
                        "wikidata": None,
                        "osm_relation_ids": [],
                        "ancestors": [],
                    }
                    record["division_id"] = division_id
                    record["geoms"] = []
                    self._records[division_id] = record
                existing = _wkb_set(record["geoms"])
                for key, geom in found.items():
                    if key not in existing:
                        record["geoms"].append(geom)
                        added.setdefault(division_id, []).append(geom)
                        existing.add(key)
            self._covered.extend(needed)
            self._covered_tree = None
            self._persist(added)
        self._tree = None
        return len(new_ids)

    def _open_area(self):
        return self._area_dataset

    def _open_division(self):
        return self._division_dataset

    def division_dataset(self):
        """The divisions dataset the lookup reads metadata from, for the stage
        that owns the lookup (``None`` for a memo-only lookup)."""
        return self._division_dataset

    @staticmethod
    def _row_groups(open_dataset, cells):
        """``(dataset, [(fragment path, row group index), ...])``: the row
        groups whose footprint — the ``bbox`` columns' min/max in the file's
        footer — intersects one of the ``cells``, the footers read under the
        stall deadline. A row group without statistics is kept: it cannot be
        ruled out."""
        opened = []

        def scan():
            opened.append(open_dataset())
            for fragment in opened[0].get_fragments():
                yield fragment.path, fragment.metadata

        groups = []
        for path, metadata in geometry.with_deadline(scan, geometry.AREA_READ_DEADLINE):
            columns = {
                metadata.schema.column(i).path: i for i in range(metadata.num_columns)
            }
            for index in range(metadata.num_row_groups):
                footprint = _footprint(metadata.row_group(index), columns)
                if (
                    footprint is None
                    or cells.query(footprint, predicate="intersects").size
                ):
                    groups.append((path, index))
        return opened[0], groups

    @staticmethod
    def _group_polygons(open_dataset, group, cells):
        """``(dataset, {division_id: {wkb: simplified polygon}})`` for the land
        areas of one row group whose own bbox touches a cell — the dataset the
        attempt opened and read, both under the stall deadline — so the caller
        can adopt that dataset once the whole attempt has succeeded."""
        path, index = group
        opened = []

        def scan():
            opened.append(open_dataset())
            fragment = next(f for f in opened[0].get_fragments() if f.path == path)
            return fragment.subset(row_group_ids=[index]).to_batches(
                columns=AREA_COLUMNS + ["bbox"],
                filter=ds.field("is_land"),
                **geometry.scan_options(),
            )

        polygons = {}
        for batch in geometry.with_deadline(scan, geometry.AREA_READ_DEADLINE):
            if batch.num_rows == 0:
                continue
            division_ids = batch.column("division_id").to_pylist()
            wkb = batch.column("geometry").to_numpy(zero_copy_only=False)
            geoms = shapely.force_2d(shapely.from_wkb(wkb, on_invalid="ignore"))
            # Empty, invalid or non-polygon geometry, or a row naming no
            # division, must not enter the containment index as evidence; nor
            # does a row whose bbox touches no cell — the rest of the row group.
            named = np.fromiter(map(bool, division_ids), bool, len(division_ids))
            touching = np.zeros(batch.num_rows, dtype=bool)
            hits = cells.query(_row_boxes(batch.column("bbox")), predicate="intersects")
            touching[hits[0]] = True
            index = np.nonzero(geometry._valid_polygons(geoms) & named & touching)[0]
            if index.size == 0:
                continue
            # Memoize the simplified boundaries (the shipping tolerance): exact
            # coastline detail is needless for stop containment and would bloat
            # the memo.
            simplified = geometry._simplify(geoms[index])
            good = geometry._valid_polygons(simplified)
            kept = simplified[good]
            for row, geom, key in zip(index[good].tolist(), kept, shapely.to_wkb(kept)):
                polygons.setdefault(division_ids[row], {})[key] = geom
        return opened[0], polygons

    def _division_metadata(self, division_ids):
        if not division_ids:
            return {}
        predicate = ds.field("id").isin(list(division_ids))

        def read(open_dataset):
            opened = []

            def scan():
                opened.append(open_dataset())
                return opened[0].to_batches(
                    columns=overture.PROJECT,
                    filter=predicate,
                    **geometry.scan_options(),
                )

            found = {}
            for batch in geometry.with_deadline(scan, geometry.AREA_READ_DEADLINE):
                for row in batch.to_pylist():
                    record = overture.normalize_division(row)
                    found[record["overture_id"]] = record
            return opened[0], found

        # Adopted on this thread once the whole attempt succeeded (see ensure).
        self._division_dataset, found = geometry.retrying(
            self._open_division, self._reopen_division, read
        )
        return found

    def _build_tree(self):
        entries = []
        geoms = []
        for record in self._records.values():
            for geom in record["geoms"]:
                entries.append(record)
                geoms.append(geom)
        self._tree_entries = entries
        self._tree = STRtree(geoms) if geoms else STRtree([shapely.Point()])
        self._tree_empty = not geoms

    def divisions_at(self, x, y):
        """The divisions whose polygons contain the point, most specific first.

        Containment (``covered_by``, so boundary points count) over the
        memoized polygons (simplified to the shipping tolerance); call
        :meth:`ensure` for the area first.
        """
        if self._tree is None:
            self._build_tree()
        if self._tree_empty:
            return []
        point = shapely.Point(x, y)
        found = {}
        for index in self._tree.query(point, predicate="covered_by"):
            record = self._tree_entries[index]
            found[record["division_id"]] = record
        return sorted(
            found.values(),
            key=lambda r: (_SPECIFICITY.get(r.get("subtype"), 8), r["division_id"]),
        )
