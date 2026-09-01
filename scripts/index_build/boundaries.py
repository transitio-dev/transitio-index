"""The boundary lookup: which admin divisions contain a point.

The gazetteer's expansion pass and the coverage stage both ask "which admin
unit contains this stop?", and neither stop coordinates nor the seeded rows can
answer that. This module answers it against the full pinned Overture release
without mirroring it: ``division_area`` is cloud-native GeoParquet whose row
groups carry ``bbox`` statistics, so a spatial query reads the file footers and
only the row groups whose boxes intersect the query — the COG access pattern —
and the divisions theme supplies the matching subtype/hierarchy metadata.

What a query touches is memoized under ``cache/boundary_lookup/<release>/``
(full-resolution WKB plus division metadata, keyed by the release), so repeated
queries within and across builds read locally and both consumers see
byte-identical geometry. Exact containment always runs locally over the
memoized polygons; the cloud filter only selects candidates by bounding box.
"""

import json

import pyarrow.dataset as ds
import shapely
from shapely.strtree import STRtree

from index_build import overture, store

DIVISIONS_FILE = "divisions.jsonl"
COVERED_FILE = "covered.jsonl"

AREA_COLUMNS = ["division_id", "geometry", "country", "is_land"]

# Most specific first: the order ``divisions_at`` returns containing divisions.
_SPECIFICITY = {"locality": 0, "localadmin": 1, "county": 2, "region": 3, "country": 4}


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
        path = self._dir.path / DIVISIONS_FILE
        if path.is_file():
            for record in store.parse_jsonl(path.read_bytes()):
                record["geoms"] = [
                    shapely.from_wkb(bytes.fromhex(entry))
                    for entry in record.get("geometries") or []
                ]
                self._records[record["division_id"]] = record
        path = self._dir.path / COVERED_FILE
        if path.is_file():
            self._covered = [
                tuple(row["box"]) for row in store.parse_jsonl(path.read_bytes())
            ]
        self._tree = None

    def _persist(self):
        def division_lines():
            for record in self._records.values():
                row = {k: v for k, v in record.items() if k != "geoms"}
                yield json.dumps(row, ensure_ascii=False) + "\n"

        store.write_file(self._dir, DIVISIONS_FILE, division_lines)
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
        needed = [
            tuple(box)
            for box in boxes
            if not any(_box_contains(done, tuple(box)) for done in self._covered)
        ]
        if not needed:
            return 0
        if self._area_dataset is None or self._division_dataset is None:
            raise store.StoreError(
                "boundary lookup needs its datasets to fetch uncovered boxes"
            )
        merged = _merge_boxes(needed)
        with store.exclusive_writer(self._dir):
            # Deduplicated by canonical WKB: disjoint boxes each return the
            # same country polygon, and a division already cached may surface
            # a further component in a later box — merged in, never discarded.
            polygons = {}
            for xmin, ymin, xmax, ymax in merged:
                predicate = (
                    (ds.field(("bbox", "xmin")) <= xmax)
                    & (ds.field(("bbox", "xmax")) >= xmin)
                    & (ds.field(("bbox", "ymin")) <= ymax)
                    & (ds.field(("bbox", "ymax")) >= ymin)
                )
                for batch in self._area_dataset.to_batches(
                    columns=AREA_COLUMNS, filter=predicate
                ):
                    for row in batch.to_pylist():
                        if not row.get("is_land"):
                            continue
                        try:
                            geom = shapely.force_2d(shapely.from_wkb(row["geometry"]))
                        except Exception:
                            continue
                        key = shapely.to_wkb(geom).hex()
                        polygons.setdefault(row["division_id"], {})[key] = geom
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
                    record["geometries"] = []
                    record["geoms"] = []
                    self._records[division_id] = record
                for key, geom in found.items():
                    if key not in record["geometries"]:
                        record["geometries"].append(key)
                        record["geoms"].append(geom)
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

        Exact containment (``covered_by``, so boundary points count) over the
        memoized full-resolution polygons; call :meth:`ensure` for the area
        first.
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
