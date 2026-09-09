"""The boundary lookup: which admin divisions contain a point.

The gazetteer's expansion pass and the coverage stage both ask "which admin
unit contains this stop?", and neither stop coordinates nor the seeded rows can
answer that. This module answers it against the full pinned Overture release
without mirroring it: ``division_area`` is cloud-native GeoParquet whose row
groups carry ``bbox`` statistics, so a spatial query reads the file footers and
only the row groups whose boxes intersect the query — the COG access pattern —
and the divisions theme supplies the matching subtype/hierarchy metadata.

What a query touches is memoized under ``cache/boundary_lookup/<release>/`` as a
GeoParquet of the division polygons (simplified to the shipping tolerance) plus
their metadata, keyed by the release, so repeated queries within and across
builds read locally and both consumers see the same geometry. Containment runs
locally over the memoized polygons; the cloud filter only selects candidates by
bounding box.
"""

import io
import json
import math

import numpy as np
import pyarrow.dataset as ds
import shapely
from shapely.strtree import STRtree

from transitio_index import geometry, overture, store

DIVISIONS_FILE = "divisions.parquet"
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


def _read_memo(path):
    """``(records, damaged)`` from the GeoParquet memo at ``path``.

    A row with a missing id or a geometry that is not a valid polygon is
    dropped and flags the memo damaged, the same validation the fresh scan
    applies, so a memo written before validation existed cannot seed the index.
    """
    import geopandas as gpd

    frame = gpd.read_parquet(path)
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


def _box_contains(outer, inner):
    return (
        outer[0] <= inner[0]
        and outer[1] <= inner[1]
        and outer[2] >= inner[2]
        and outer[3] >= inner[3]
    )


def _merge_boxes(boxes):
    """Union intersecting boxes so overlapping queries become one scan."""
    merged = [tuple(box) for box in boxes]
    changed = True
    while changed:
        changed = False
        result = []
        for box in merged:
            for i, other in enumerate(result):
                if not (
                    box[2] < other[0]
                    or other[2] < box[0]
                    or box[3] < other[1]
                    or other[3] < box[1]
                ):
                    result[i] = (
                        min(box[0], other[0]),
                        min(box[1], other[1]),
                        max(box[2], other[2]),
                        max(box[3], other[3]),
                    )
                    changed = True
                    break
            else:
                result.append(box)
        merged = result
    return merged


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
    ):
        self.release = release
        self._area_dataset = area_dataset
        self._division_dataset = division_dataset
        root = store.open_subdir(cache_dir, "boundary_lookup")
        try:
            self._dir = store.open_subdir(root.path, release)
        finally:
            root.close()
        self._records = {}
        self._covered = []
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
        """Read the memo from disk into ``_records`` / ``_covered``.

        Resets the in-memory state first, so it doubles as a reload. Must be
        called while holding the writer lock (see :meth:`_load`, :meth:`ensure`).
        """
        self._records = {}
        self._covered = []
        damaged = False
        path = self._dir.path / DIVISIONS_FILE
        divisions_present = path.is_file()
        if divisions_present:
            # An unreadable memo (a corrupt or partial parquet) is damage, not
            # fatal: coverage is cleared below and the next ensure() refetches.
            try:
                self._records, damaged = _read_memo(path)
            except Exception:  # noqa: B902 - geopandas/pyarrow raise their own
                self._records, damaged = {}, True
        path = self._dir.path / COVERED_FILE
        if path.is_file():
            for row in store.parse_jsonl(path.read_bytes()):
                box = row.get("box") if isinstance(row, dict) else None
                if (
                    isinstance(box, list)
                    and len(box) == 4
                    and all(
                        isinstance(v, (int, float)) and math.isfinite(v) for v in box
                    )
                ):
                    self._covered.append(tuple(box))
                else:
                    damaged = True
        if self._covered and not divisions_present:
            # The two files persist together; coverage without a divisions
            # file is not a valid memo. (Coverage with an EMPTY divisions
            # file is legitimate: an all-ocean box finds no divisions.)
            damaged = True
        if damaged:
            # Damage must not count as covered: the dropped entries would
            # become permanent false negatives, so coverage is cleared and
            # the next ensure() refetches and repairs the memo.
            self._covered = []
        self._tree = None

    def _uncovered(self, boxes):
        """The boxes not already inside a covered box."""
        return [
            box
            for box in boxes
            if not any(_box_contains(done, box) for done in self._covered)
        ]

    def _persist(self):
        import geopandas as gpd
        import pandas as pd

        rows, geoms = [], []
        for record in self._records.values():
            fields = _memo_fields(record)
            for geom in record["geoms"]:
                rows.append(fields)
                geoms.append(geom)
        frame = pd.DataFrame(rows, columns=_MEMO_COLUMNS)
        gdf = gpd.GeoDataFrame(frame, geometry=gpd.GeoSeries(geoms, crs=MEMO_CRS))
        sink = io.BytesIO()
        gdf.to_parquet(sink)
        store.write_bytes(self._dir, DIVISIONS_FILE, sink.getvalue())
        store.write_file(
            self._dir,
            COVERED_FILE,
            lambda: (json.dumps({"box": list(b)}) + "\n" for b in self._covered),
        )

    def ensure(self, boxes):
        """Make every box queryable; returns how many new divisions arrived.

        Boxes already inside a covered box cost nothing. The rest are merged
        and scanned against the release with a bbox filter, their land
        polygons parsed (malformed WKB skipped) and their divisions' metadata
        resolved, then memoized.
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
            merged = _merge_boxes(needed)
            # Deduplicated by canonical WKB: disjoint boxes each return the
            # same country polygon, and a division already cached may surface a
            # further component in a later box — merged in, never discarded.
            # Geometry is processed per batch with the vectorized shapely API,
            # not one polygon at a time.
            polygons = {}
            for xmin, ymin, xmax, ymax in merged:
                predicate = (
                    (ds.field(("bbox", "xmin")) <= xmax)
                    & (ds.field(("bbox", "xmax")) >= xmin)
                    & (ds.field(("bbox", "ymin")) <= ymax)
                    & (ds.field(("bbox", "ymax")) >= ymin)
                    & ds.field("is_land")
                )
                for batch in self._area_dataset.to_batches(
                    columns=AREA_COLUMNS, filter=predicate
                ):
                    if batch.num_rows == 0:
                        continue
                    division_ids = batch.column("division_id").to_pylist()
                    wkb = batch.column("geometry").to_numpy(zero_copy_only=False)
                    geoms = shapely.force_2d(shapely.from_wkb(wkb, on_invalid="ignore"))
                    # Empty, invalid or non-polygon geometry must not enter the
                    # containment index as division evidence.
                    index = np.nonzero(geometry._valid_polygons(geoms))[0]
                    if index.size == 0:
                        continue
                    # Memoize the simplified boundaries (the shipping tolerance):
                    # exact coastline detail is needless for stop containment and
                    # would bloat the memo.
                    simplified = geometry._simplify(geoms[index])
                    good = geometry._valid_polygons(simplified)
                    kept = simplified[good]
                    for row, geom, key in zip(
                        index[good].tolist(), kept, shapely.to_wkb(kept)
                    ):
                        polygons.setdefault(division_ids[row], {})[key] = geom
            new_ids = sorted(set(polygons) - set(self._records))
            metadata = self._division_metadata(new_ids)
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
                existing = (
                    set(shapely.to_wkb(np.asarray(record["geoms"], dtype=object)))
                    if record["geoms"]
                    else set()
                )
                for key, geom in found.items():
                    if key not in existing:
                        record["geoms"].append(geom)
                        existing.add(key)
            self._covered.extend(merged)
            self._persist()
        self._tree = None
        return len(new_ids)

    def _division_metadata(self, division_ids):
        if not division_ids:
            return {}
        predicate = ds.field("id").isin(list(division_ids))
        found = {}
        for batch in self._division_dataset.to_batches(
            columns=overture.PROJECT, filter=predicate
        ):
            for row in batch.to_pylist():
                record = overture.normalize_division(row)
                found[record["overture_id"]] = record
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
