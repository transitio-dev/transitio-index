"""Stage: statistics about the catalogue rows a build saw, after publish.

The article's tables come from a build, not from hand-counted exports: this
stage reads the ingest generations (every Mobility Database, Transitland Atlas
and GBFS row the build ingested) and the crosswalk generation (the feeds they
became) and writes a ``stats`` generation with ``catalogue.parquet`` — one row
per catalogue row, whether or not it became a feed — and ``summary.json``, the
totals the report renders, keyed by section (the build's provenance here; the
declared-place and identity sections follow). Nothing is re-run and
no network is touched; for a sample build the rows are the sample's, and the
summary records the build's snapshot id and source versions.
"""

import collections
import contextlib
import datetime
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

from transitio_index import store
from transitio_index.crosswalk import _clean_url, _host

STATS_POINTER = "stats.json"
CATALOGUE_ARTIFACT = "catalogue.parquet"
SUMMARY_ARTIFACT = "summary.json"

# The ingest pointers and artifacts the catalogue rows come from.
RAW_SOURCES = {
    "mdb": ("mdb.json", "mdb_feeds.jsonl"),
    "atlas": ("atlas.json", "atlas_feeds.jsonl"),
    "gbfs": ("gbfs.json", "gbfs_systems.jsonl"),
}

CATALOGUE_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("source_id", pa.string()),
        ("spec", pa.string()),
        ("status", pa.string()),
        ("declared_country", pa.string()),
        ("declared_subdivision", pa.string()),
        ("declared_municipality", pa.string()),
        ("has_name", pa.bool_()),
        ("has_license_url", pa.bool_()),
        ("has_bbox", pa.bool_()),
        ("has_latest_url", pa.bool_()),
        ("requires_auth", pa.bool_()),
        ("bbox_lon_span", pa.float64()),
        ("bbox_lat_span", pa.float64()),
        ("bbox_extracted_year", pa.int64()),
        ("redirect_targets", pa.list_(pa.string())),
        ("redirect_target_status", pa.string()),
        ("download_url", pa.string()),
        ("download_host", pa.string()),
        ("id_namespace", pa.string()),
        ("feed_id", pa.string()),
        ("drop_reason", pa.string()),
        ("snapshot_id", pa.string()),
    ]
)


class StatsError(RuntimeError):
    """The statistics inputs do not describe one build."""


def _lon_span(box):
    """The box's width in degrees; a box crossing the antimeridian
    (min_lon > max_lon) wraps rather than spanning the globe."""
    span = box["max_lon"] - box["min_lon"]
    return span if span >= 0 else span + 360.0


def _year(value):
    if isinstance(value, str) and value[:4].isdigit():
        return int(value[:4])
    return None


def _mdb_row(record, status_by_id):
    location = record.get("location") or {}
    urls = record.get("urls") or {}
    box = record.get("bounding_box")
    # Every replacement a deprecated row names; the status is the first's.
    redirects = list(record.get("redirect_ids") or [])
    target = redirects[0] if redirects else None
    url = _clean_url(urls.get("direct_download"))
    return {
        "source": "mdb",
        "source_id": record["mdb_id"],
        "spec": record.get("spec"),
        "status": record.get("status"),
        "declared_country": location.get("country_code"),
        "declared_subdivision": location.get("subdivision_name"),
        "declared_municipality": location.get("municipality"),
        "has_name": bool(record.get("name")),
        "has_license_url": bool(urls.get("license")),
        "has_bbox": box is not None,
        "has_latest_url": bool(urls.get("latest")),
        "requires_auth": bool(record.get("requires_auth")),
        "bbox_lon_span": _lon_span(box) if box else None,
        "bbox_lat_span": box["max_lat"] - box["min_lat"] if box else None,
        "bbox_extracted_year": _year(record.get("bounding_box_extracted_on")),
        "redirect_targets": redirects,
        "redirect_target_status": status_by_id.get(target) if target else None,
        "download_url": url,
        "download_host": _host(url) if url else None,
        "id_namespace": record["mdb_id"].split("-", 1)[0],
    }


def _atlas_row(record):
    url = _clean_url((record.get("urls") or {}).get("static_current"))
    return {
        "source": "atlas",
        "source_id": record["onestop_id"],
        "spec": record.get("spec"),
        "has_name": bool(record.get("name")),
        "has_license_url": bool((record.get("license") or {}).get("url")),
        "has_bbox": False,
        "has_latest_url": False,
        "requires_auth": bool(record.get("requires_auth")),
        "download_url": url,
        "download_host": _host(url) if url else None,
    }


def _gbfs_row(record, duplicated):
    """A GBFS system row; a system id the list repeats is keyed with its
    country, as the crosswalk mints it, so the stable key stays unique."""
    url = _clean_url(record.get("auto_discovery_url"))
    system_id = record["system_id"]
    if system_id in duplicated:
        system_id = f"{system_id}/{record.get('country_code') or '?'}"
    return {
        "source": "gbfs",
        "source_id": system_id,
        "spec": record.get("spec"),
        "declared_country": record.get("country_code"),
        "declared_municipality": record.get("location"),
        "has_name": bool(record.get("name")),
        "has_license_url": False,
        "has_bbox": False,
        "has_latest_url": False,
        "requires_auth": bool(record.get("requires_auth")),
        "download_url": url,
        "download_host": _host(url) if url else None,
    }


def _feed_lookup(feeds):
    """``{(source, key): feed_id}`` for every catalogue row a feed carries:
    MDB rows by id, Atlas feeds by Onestop ID, GBFS systems by id and country
    (a duplicated system id is only unambiguous with its country)."""
    lookup = {}
    for feed in feeds:
        if feed.get("mdb_id"):
            lookup[("mdb", feed["mdb_id"])] = feed["feed_id"]
        if feed.get("onestop_id"):
            lookup[("atlas", feed["onestop_id"])] = feed["feed_id"]
        system = feed.get("gbfs") or {}
        if system.get("system_id"):
            key = ("gbfs", system["system_id"], system.get("country_code"))
            lookup[key] = feed["feed_id"]
    return lookup


def catalogue_rows(raw, feeds, snapshot_id=None):
    """One row per catalogue row of ``raw`` (``{source: records}``), with the
    index feed it became, or the reason it did not."""
    lookup = _feed_lookup(feeds)
    mdb = raw.get("mdb") or []
    status_by_id = {record["mdb_id"]: record.get("status") for record in mdb}
    gbfs = raw.get("gbfs") or []
    gbfs_counts = collections.Counter(record["system_id"] for record in gbfs)
    duplicated = {system_id for system_id, n in gbfs_counts.items() if n > 1}
    rows = []
    for record in mdb:
        row = _mdb_row(record, status_by_id)
        row["feed_id"] = lookup.get(("mdb", record["mdb_id"]))
        rows.append(row)
    for record in raw.get("atlas") or []:
        row = _atlas_row(record)
        row["feed_id"] = lookup.get(("atlas", record["onestop_id"]))
        rows.append(row)
    for record in gbfs:
        row = _gbfs_row(record, duplicated)
        key = ("gbfs", record["system_id"], record.get("country_code"))
        row["feed_id"] = lookup.get(key)
        if row["feed_id"] is None and record["system_id"] in duplicated:
            if not record.get("country_code"):
                row["drop_reason"] = "ambiguous_id"
        rows.append(row)
    keys = collections.Counter((row["source"], row["source_id"]) for row in rows)
    repeated = sorted(key for key, n in keys.items() if n > 1)
    if repeated:
        raise StatsError(f"catalogue rows share a key: {repeated[:5]}")
    for row in rows:
        row.setdefault("drop_reason", None if row["feed_id"] else "not_in_index")
        row["snapshot_id"] = snapshot_id
        for field in CATALOGUE_SCHEMA.names:
            row.setdefault(field, None)
    return rows


def _read_raw(cache_dir):
    """``({source: records}, {source: manifest})`` for the ingests that ran."""
    records, manifests = {}, {}
    for source, (pointer, artifact) in RAW_SOURCES.items():
        if store.current_generation(cache_dir / "raw", pointer) is None:
            continue
        records[source], manifests[source] = store.read_jsonl(
            cache_dir / "raw", pointer, artifact
        )
    return records, manifests


def _snapshot(cache_dir, crosswalk, manifests):
    """The published snapshot the statistics describe. The stage runs after
    publish, and the ingests and the crosswalk it reads must be the ones the
    snapshot records, else the rows would be labelled with another build."""
    path = cache_dir / "index" / "snapshot.json"
    if not path.is_file():
        raise StatsError("no published index; run the publish stage first")
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    recorded = snapshot.get("generations") or {}
    current = {"crosswalk/feeds.json": crosswalk.get("generation")}
    for source, (pointer, _) in RAW_SOURCES.items():
        if source in manifests:
            current[f"raw/{pointer}"] = manifests[source].get("generation")
    missing = [k for k in recorded if k.startswith("raw/") and k not in current]
    stale = sorted(missing + [k for k, v in current.items() if recorded.get(k) != v])
    if stale:
        raise StatsError(
            f"the published index was not built from the current {stale}; "
            "re-run the pipeline in stage order"
        )
    return snapshot


def _parquet(rows):
    table = pa.Table.from_pylist(rows, schema=CATALOGUE_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def stats(cache_dir):
    """Gather the build's statistics; publish the ``stats`` generation.
    Returns the manifest."""
    with contextlib.ExitStack() as stack:
        directory = store.open_subdir(cache_dir, "stats")
        stack.callback(directory.close)
        stack.enter_context(store.exclusive_writer(directory))
        if store.current_generation(cache_dir / "crosswalk", "feeds.json") is None:
            raise StatsError("no crosswalk generation to gather statistics from")
        feeds, crosswalk = store.read_jsonl(
            cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
        )
        raw, manifests = _read_raw(cache_dir)
        if not raw:
            raise StatsError("no ingest generation to gather statistics from")
        snapshot = _snapshot(cache_dir, crosswalk, manifests)
        snapshot_id = snapshot.get("snapshot_id")
        rows = catalogue_rows(raw, feeds, snapshot_id)
        # An ingest that read a local file (no URL) is a cut sample.
        local = [
            s
            for s, m in manifests.items()
            if not (m.get("csv_url") or m.get("archive_url"))
        ]
        summary = {
            "build": {
                "snapshot_id": snapshot_id,
                "schema_version": snapshot.get("schema_version"),
                "overture_release": snapshot.get("overture_release"),
                "sources": crosswalk.get("sources"),
                "catalogue_dates": {
                    s: m.get("csv_label") or m.get("commit")
                    for s, m in manifests.items()
                },
                "catalogue_rows": {s: m.get("rows") for s, m in manifests.items()},
                "sample": "sample" if local else "full",
                "sample_sources": local,
            },
        }
        manifest = {
            "source": "stats",
            "snapshot_id": snapshot_id,
            "crosswalk_generation": crosswalk.get("generation"),
            "raw_generations": {s: m.get("generation") for s, m in manifests.items()},
            "catalogue_rows": len(rows),
            "sections": sorted(summary),
            "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        data = _parquet(rows)
        text = json.dumps(summary, indent=2, sort_keys=True)
        return store.publish(
            cache_dir / "stats",
            STATS_POINTER,
            {CATALOGUE_ARTIFACT: lambda: [data], SUMMARY_ARTIFACT: lambda: [text]},
            manifest,
            held=directory,
        )
