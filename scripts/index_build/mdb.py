"""Mobility Database catalogue ingest, into the shared normalized shape.

The no-token CSV export (``feeds_v2.csv``) is the whole catalogue, GTFS and
GTFS-RT — an always-latest download, so the manifest's content SHA-256 is its
identity (6,505 rows as observed on 2026-08-28). Each row is normalized and
published as a generation; the crosswalk against Atlas is a later stage.
"""

import collections
import math

from index_build import csv_source
from index_build.csv_source import IngestError

CSV_URL = "https://files.mobilitydatabase.org/feeds_v2.csv"
# A human tag for the fetch; the manifest's csv_sha256 is the real identity
# (feeds_v2.csv is always-latest, carrying no immutable version of its own).
DEFAULT_LABEL = "2026-08-28"

# ``data_type`` in the CSV to the spec vocabulary the index uses.
SPEC_BY_DATA_TYPE = {"gtfs": "gtfs", "gtfs_rt": "gtfs-rt"}

BOX_KEYS = (
    "location.bounding_box.minimum_latitude",
    "location.bounding_box.maximum_latitude",
    "location.bounding_box.minimum_longitude",
    "location.bounding_box.maximum_longitude",
)

# Every column ``normalize_row`` reads: a rename of any of them would
# otherwise be read as null rather than caught.
REQUIRED_HEADERS = {
    "id",
    "data_type",
    "provider",
    "name",
    "status",
    "is_official",
    "location.country_code",
    "location.subdivision_name",
    "location.municipality",
    "urls.direct_download",
    "urls.latest",
    "urls.license",
    "urls.authentication_type",
    "features",
    "static_reference",
    "redirect.id",
    *BOX_KEYS,
}


def _bounding_box(row):
    """A valid box, or None (with the reason left to the caller to count).

    An out-of-range or misordered box would otherwise flow into declared
    placement and IoU crosswalking as if it were real geography. Latitudes
    must be ordered; a min_lon > max_lon box is kept — that is how a box
    crossing the antimeridian is expressed, not an error.
    """
    values = [csv_source.blank_to_none(row.get(key)) for key in BOX_KEYS]
    if any(value is None for value in values):
        return None
    try:
        south, north, west, east = (float(value) for value in values)
    except ValueError:
        return None
    if not all(math.isfinite(v) for v in (south, north, west, east)):
        return None
    if not (-90.0 <= south <= north <= 90.0):
        return None
    if not (-180.0 <= west <= 180.0 and -180.0 <= east <= 180.0):
        return None
    return {"min_lat": south, "max_lat": north, "min_lon": west, "max_lon": east}


def normalize_row(row, source_file, position):
    """One MDB catalogue row as an ingest record.

    ``static_references`` (a GTFS-RT feed's static feed(s)) and ``redirect_ids``
    (a deprecated feed's replacement(s)) are pipe-joined lists upstream — one
    feed can reference several — and are preserved as validated id lists.
    They are linkage evidence later stages need; nothing here requires them
    to resolve, since a reference can legitimately dangle.
    """
    mdb_id = csv_source.require_id(row.get("id"), "feed", source_file, position)
    data_type = csv_source.blank_to_none(row.get("data_type"))
    spec = SPEC_BY_DATA_TYPE.get(data_type)
    if spec is None:
        raise IngestError(
            f"{source_file}: feed {mdb_id}: unsupported data_type {data_type!r}"
        )
    auth_type = csv_source.blank_to_none(row.get("urls.authentication_type"))
    features = csv_source.blank_to_none(row.get("features"))
    box = _bounding_box(row)
    box_present = any(
        csv_source.blank_to_none(row.get(key)) is not None for key in BOX_KEYS
    )
    return {
        "source": "mdb",
        "mdb_id": mdb_id,
        "spec": spec,
        "provider": csv_source.blank_to_none(row.get("provider")),
        "name": csv_source.blank_to_none(row.get("name")),
        "status": csv_source.blank_to_none(row.get("status")),
        "official": _as_bool(row.get("is_official"), source_file, mdb_id),
        "location": {
            "country_code": csv_source.blank_to_none(row.get("location.country_code")),
            "subdivision_name": csv_source.blank_to_none(
                row.get("location.subdivision_name")
            ),
            "municipality": csv_source.blank_to_none(row.get("location.municipality")),
        },
        "bounding_box": box,
        "bounding_box_invalid": box_present and box is None,
        "urls": {
            "direct_download": csv_source.blank_to_none(
                row.get("urls.direct_download")
            ),
            "latest": csv_source.blank_to_none(row.get("urls.latest")),
            "license": csv_source.blank_to_none(row.get("urls.license")),
        },
        "authentication_type": auth_type,
        "requires_auth": bool(auth_type) and auth_type != "0",
        "features": features.split("|") if features else [],
        "static_references": csv_source.id_list(
            row.get("static_reference"), "static_reference", source_file, mdb_id
        ),
        "redirect_ids": csv_source.id_list(
            row.get("redirect.id"), "redirect", source_file, mdb_id
        ),
    }


_TRUE = {"true", "1", "yes"}
_FALSE = {"false", "0", "no"}


def _as_bool(value, source_file, position):
    value = csv_source.blank_to_none(value)
    if value is None:
        return None
    lowered = value.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise IngestError(
        f"{source_file}: feed {position}: is_official {value!r} is not a boolean"
    )


def parse_rows(rows, source_file):
    """Normalize every catalogue row; MDB ids must be unique.

    Returns ``(records, summary)``. A duplicate id is an error — the whole
    catalogue keys on it — but a dangling ``static_reference`` is not, since
    the referenced feed may simply have been removed.
    """
    records = []
    seen = {}
    for position, row in enumerate(rows):
        record = normalize_row(row, source_file, position)
        mdb_id = record["mdb_id"]
        first = seen.get(mdb_id)
        if first is not None:
            raise IngestError(
                f"{source_file}: mdb id {mdb_id!r} appears at rows {first} and {position}"
            )
        seen[mdb_id] = position
        records.append(record)

    by_spec = collections.Counter(record["spec"] for record in records)
    # A blank status must be a string key: a None mixed with string keys
    # would raise when the manifest is serialized with sort_keys.
    by_status = collections.Counter(record["status"] or "unknown" for record in records)
    invalid_boxes = sum(1 for record in records if record["bounding_box_invalid"])
    for record in records:
        del record["bounding_box_invalid"]
    return records, {
        "records_by_spec": dict(by_spec),
        "records_by_status": dict(by_status),
        "invalid_bounding_boxes": invalid_boxes,
    }


def ingest(cache_dir, *, csv_path=None, label=DEFAULT_LABEL, expected_sha256=None):
    """Run the MDB ingest, publishing a ``mdb.json`` generation."""
    return csv_source.ingest_csv(
        cache_dir,
        source="mdb",
        label=label,
        url=CSV_URL,
        pointer="mdb.json",
        artifact="mdb_feeds.jsonl",
        parse_rows=parse_rows,
        required_headers=REQUIRED_HEADERS,
        csv_path=csv_path,
        expected_sha256=expected_sha256,
    )
