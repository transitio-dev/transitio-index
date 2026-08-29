"""Publish stage: the crosswalk feeds as a shippable index.

Reads the crosswalk ``feeds.json`` generation and writes ``<cache>/index/`` —
``feeds.parquet`` (one row per feed) and ``snapshot.json`` (the manifest: a
deterministic snapshot id, the schema version, the source versions the crosswalk
recorded, the counts, and the Parquet's SHA-256). This is the "feeds only"
index; places and edges are later stages, and their columns are added then.

The flat identity and crosswalk fields are their own columns; the verbatim Atlas,
MDB and GBFS source rows are kept as JSON-string columns, so nothing is lost and
the field-level columns a query surface needs can be derived later.
"""

import collections
import datetime
import hashlib
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

from index_build import store

SCHEMA_VERSION = 1
FEEDS_FILE = "feeds.parquet"
SNAPSHOT_FILE = "snapshot.json"


class PublishError(RuntimeError):
    """The index could not be built from the crosswalk output."""


_SCHEMA = pa.schema(
    [
        ("feed_id", pa.string()),
        ("onestop_id", pa.string()),
        ("mdb_id", pa.string()),
        ("id_minted", pa.bool_()),
        ("source", pa.string()),
        ("spec", pa.string()),
        ("name", pa.string()),
        ("aliases", pa.list_(pa.string())),
        ("crosswalk_method", pa.string()),
        ("crosswalk_confidence", pa.float64()),
        ("static_feed_id", pa.string()),
        ("static_link_method", pa.string()),
        ("atlas", pa.string()),
        ("mdb", pa.string()),
        ("gbfs", pa.string()),
        ("snapshot", pa.string()),
    ]
)


def _json_block(block):
    if block is None:
        return None
    return json.dumps(block, ensure_ascii=False, sort_keys=True, allow_nan=False)


def _row(record, snapshot_id):
    return {
        "feed_id": record["feed_id"],
        "onestop_id": record.get("onestop_id"),
        "mdb_id": record.get("mdb_id"),
        "id_minted": record["id_minted"],
        "source": record["source"],
        "spec": record["spec"],
        "name": record.get("name"),
        "aliases": record.get("aliases") or [],
        "crosswalk_method": record["crosswalk_method"],
        "crosswalk_confidence": record["crosswalk_confidence"],
        "static_feed_id": record.get("static_feed_id"),
        "static_link_method": record.get("static_link_method"),
        "atlas": _json_block(record.get("atlas")),
        "mdb": _json_block(record.get("mdb")),
        "gbfs": _json_block(record.get("gbfs")),
        "snapshot": snapshot_id,
    }


def _snapshot_id(sources):
    # Deterministic in the source versions, so the same inputs always produce
    # the same snapshot id; the build time is metadata and is left out of it.
    atlas = sources["atlas"]
    seed = "|".join(
        [
            str(SCHEMA_VERSION),
            atlas.get("commit") or "",
            atlas.get("archive_sha256") or "",
            sources["mdb"].get("csv_sha256") or "",
            sources["gbfs"].get("csv_sha256") or "",
        ]
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _counts(records):
    return {
        "feeds": len(records),
        "by_source": dict(collections.Counter(r["source"] for r in records)),
        "by_spec": dict(collections.Counter(r["spec"] for r in records)),
    }


def _parquet_bytes(records, snapshot_id):
    table = pa.Table.from_pylist(
        [_row(record, snapshot_id) for record in records], schema=_SCHEMA
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def publish(cache_dir):
    """Build ``<cache>/index`` from the crosswalk output. Returns the manifest.

    The source versions come from the crosswalk manifest — the ones it actually
    read — rather than re-resolving the ingests, so a republished ingest cannot
    label the snapshot with versions the feeds were not built from. The
    ``feeds.parquet`` is hashed into the manifest and written first, the manifest
    (carrying the matching digest) last, both under ``exclusive_writer``. A
    reader overlapping a publish can therefore only ever see a new Parquet with
    the old manifest — a digest mismatch it refuses — never a mismatched pair
    read as valid.
    """
    records, crosswalk = store.read_jsonl(
        cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
    )
    if not records:
        raise PublishError("crosswalk produced no feeds to publish")
    sources = crosswalk.get("sources")
    if not sources:
        raise PublishError("crosswalk manifest records no source versions")

    snapshot_id = _snapshot_id(sources)
    data = _parquet_bytes(records, snapshot_id)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "sources": sources,
        "counts": _counts(records),
        "feeds_sha256": hashlib.sha256(data).hexdigest(),
    }

    directory = store.open_subdir(cache_dir, "index")
    try:
        with store.exclusive_writer(directory):
            store.write_bytes(directory, FEEDS_FILE, data)
            store.write_file(
                directory,
                SNAPSHOT_FILE,
                lambda: [json.dumps(manifest, indent=2, sort_keys=True)],
            )
    finally:
        directory.close()
    return manifest
