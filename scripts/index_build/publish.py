"""Publish stage: the crosswalk feeds and gazetteer places as a shippable index.

Reads the crosswalk ``feeds.json`` generation and, when the gazetteer has run,
its ``names.json`` places generation, and writes ``<cache>/index/`` —
``feeds.parquet`` (one row per feed), ``places.parquet`` (one row per place, a
GeoParquet with the simplified boundary), and ``snapshot.json`` (the manifest: a
deterministic snapshot id, the schema version, the source versions, the counts,
and each Parquet's SHA-256). Places are optional: an index built before the
gazetteer ran is feeds only, and the reader treats places the same way.

The flat identity and crosswalk fields are their own columns; the verbatim Atlas,
MDB and GBFS source rows are kept as JSON-string columns, so nothing is lost and
the field-level columns a query surface needs can be derived later. A place's
``names`` is a ``map<string, string>`` column (language to label), as the plan
defines it.
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
PLACES_FILE = "places.parquet"
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


def _place_row(record, snapshot_id):
    metro_ids = record.get("metro_ids") or []
    return {
        "place_id": record["place_id"],
        "kind": record["kind"],
        "source_subtype": record.get("source_subtype"),
        "name": record.get("name"),
        "names": dict(sorted((record.get("names") or {}).items())),
        "aliases": record.get("aliases") or [],
        # An explicit default set upstream (a curated override) wins; otherwise a
        # place in exactly one metro promotes to it, and choosing among several
        # is the resolver's metro-default rule, so it stays null.
        "default_metro_id": record.get("default_metro_id")
        or (metro_ids[0] if len(metro_ids) == 1 else None),
        "resolution_method": record.get("resolution_method"),
        "curated": bool(record.get("curated", False)),
        "parent_id": record.get("parent_id"),
        "metro_ids": metro_ids,
        "member_ids": record.get("member_ids") or [],
        "country_code": record.get("country_code"),
        "overture_id": record.get("overture_id"),
        "osm_relation_id": record.get("osm_relation_id"),
        "statistical_area_id": record.get("statistical_area_id"),
        "geonames_id": record.get("geonames_id"),
        "geometry_source": record.get("geometry_source"),
        "snapshot": snapshot_id,
    }


def _places_digest(places):
    """A stable digest of the raw place records the gazetteer produced."""
    canonical = json.dumps(places, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _snapshot_id(sources, overture_release=None, places_digest=None):
    # Deterministic in the source versions, so the same inputs always produce the
    # same snapshot id; the build time is metadata and is left out of it. Feeds
    # are fully determined by those versions. Places also draw on live Wikidata,
    # which no release pins, so the places' own digest joins the id — otherwise
    # two builds with the same Overture release but edited Wikidata would collide.
    atlas = sources["atlas"]
    parts = [
        str(SCHEMA_VERSION),
        atlas.get("commit") or "",
        atlas.get("archive_sha256") or "",
        sources["mdb"].get("csv_sha256") or "",
        sources["gbfs"].get("csv_sha256") or "",
    ]
    if overture_release:
        parts.append(overture_release)
    if places_digest:
        parts.append(places_digest)
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


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


_PLACES_SCHEMA = pa.schema(
    [
        ("place_id", pa.string()),
        ("kind", pa.string()),
        ("source_subtype", pa.string()),
        ("name", pa.string()),
        ("names", pa.map_(pa.string(), pa.string())),
        ("aliases", pa.list_(pa.string())),
        ("default_metro_id", pa.string()),
        ("resolution_method", pa.string()),
        ("curated", pa.bool_()),
        ("parent_id", pa.string()),
        ("metro_ids", pa.list_(pa.string())),
        ("member_ids", pa.list_(pa.string())),
        ("country_code", pa.string()),
        ("overture_id", pa.string()),
        ("osm_relation_id", pa.string()),
        ("statistical_area_id", pa.string()),
        ("geonames_id", pa.string()),
        ("geometry_source", pa.string()),
        ("snapshot", pa.string()),
        ("geometry", pa.binary()),
    ]
)


def _geo_metadata():
    """The GeoParquet ``geo`` metadata so a reader treats geometry as WKB."""
    import pyproj

    crs = json.loads(pyproj.CRS.from_epsg(4326).to_json())
    return json.dumps(
        {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": [], "crs": crs}
            },
        }
    ).encode("utf-8")


def _places_parquet_bytes(places, snapshot_id):
    """The places as GeoParquet bytes: declared columns plus the WKB boundary.

    The schema is declared, not inferred, so an all-null column (``geonames_id``,
    say) or an all-empty list column keeps its type across builds rather than
    collapsing to ``null`` or ``list<null>``.
    """
    rows = []
    for place in places:
        row = _place_row(place, snapshot_id)
        wkb = place.get("geometry")
        row["geometry"] = bytes.fromhex(wkb) if wkb else None
        rows.append(row)
    schema = _PLACES_SCHEMA.with_metadata({b"geo": _geo_metadata()})
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def _read_places(cache_dir):
    """The gazetteer places and the Overture release, or ``(None, None)``.

    The release is taken from the ``names.json`` generation's own manifest — the
    same generation the places come from — so it cannot be labelled with a
    different Overture pointer read separately.
    """
    if not (cache_dir / "gazetteer").is_dir():
        return None, None
    try:
        places, manifest = store.read_jsonl(
            cache_dir / "gazetteer", "names.json", "places_seed.jsonl"
        )
    except store.StoreError:
        # No published places generation: the index is feeds only.
        return None, None
    return places, manifest.get("overture_release")


def publish(cache_dir):
    """Build ``<cache>/index`` from the crosswalk (and gazetteer). Returns the manifest.

    The source versions come from the crosswalk manifest — the ones it actually
    read — rather than re-resolving the ingests, so a republished ingest cannot
    label the snapshot with versions the feeds were not built from. Each Parquet
    is hashed into the manifest and written first, the manifest (carrying the
    matching digests) last, all under ``exclusive_writer``. A reader overlapping a
    publish can therefore only ever see a new Parquet with the old manifest — a
    digest mismatch it refuses — never a mismatched pair read as valid.
    """
    records, crosswalk = store.read_jsonl(
        cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
    )
    if not records:
        raise PublishError("crosswalk produced no feeds to publish")
    sources = crosswalk.get("sources")
    if not sources:
        raise PublishError("crosswalk manifest records no source versions")

    # A gazetteer that ran but produced no places is a places index of zero
    # places, distinct from a feeds-only build (no gazetteer at all) — hence
    # ``is not None`` throughout, never a truthiness test that folds the two.
    places, overture_release = _read_places(cache_dir)
    if places is not None and not overture_release:
        # The release folds into the snapshot id; without it a places index would
        # share the feeds-only id for the same feeds.
        raise PublishError("gazetteer places carry no overture_release")
    places_digest = _places_digest(places) if places is not None else None
    snapshot_id = _snapshot_id(sources, overture_release, places_digest)
    feeds_data = _parquet_bytes(records, snapshot_id)
    counts = _counts(records)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "sources": sources,
        "counts": counts,
        "feeds_sha256": hashlib.sha256(feeds_data).hexdigest(),
    }

    places_data = None
    if places is not None:
        places_data = _places_parquet_bytes(places, snapshot_id)
        manifest["places_sha256"] = hashlib.sha256(places_data).hexdigest()
        manifest["overture_release"] = overture_release
        counts["places"] = len(places)
        counts["places_by_kind"] = dict(collections.Counter(p["kind"] for p in places))

    directory = store.open_subdir(cache_dir, "index")
    try:
        with store.exclusive_writer(directory):
            store.write_bytes(directory, FEEDS_FILE, feeds_data)
            if places_data is not None:
                store.write_bytes(directory, PLACES_FILE, places_data)
            store.write_file(
                directory,
                SNAPSHOT_FILE,
                lambda: [json.dumps(manifest, indent=2, sort_keys=True)],
            )
    finally:
        directory.close()
    return manifest
