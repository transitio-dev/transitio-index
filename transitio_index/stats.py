"""Stage: statistics about the catalogue rows a build saw, after publish.

The article's tables come from a build, not from hand-counted exports: this
stage reads the ingest generations (every Mobility Database, Transitland Atlas
and GBFS row the build ingested) and the crosswalk generation (the feeds they
became) and writes a ``stats`` generation with ``catalogue.parquet`` — one row
per catalogue row, whether or not it became a feed — and ``summary.json``, the
totals and shares the report renders, keyed by section: the build's
provenance, the declared places and identity and duplication. Nothing is re-run and
no network is touched; for a sample build the rows are the sample's, and the
summary records the build's snapshot id and source versions.
"""

import collections
import contextlib
import datetime
import io
import json
import re

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

# A declared bounding box wider than these, in degrees, is not a place.
WIDE_BOX_DEGREES = (15.0, 40.0)
# A municipality field naming several places at once.
_SEVERAL = re.compile(r"[,/;]")

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
        ("redirect_target", pa.string()),
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
    # Every replacement a deprecated row names; the status is that of the
    # first one the catalogue holds, else of the first named.
    redirects = list(record.get("redirect_ids") or [])
    present = [t for t in redirects if t in status_by_id]
    target = present[0] if present else (redirects[0] if redirects else None)
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
        "redirect_target": target,
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


def declared_places(rows):
    """The declared-place section: what the MDB location fields leave out or
    get wrong, and what the other catalogues never carry."""
    mdb = [row for row in rows if row["source"] == "mdb"]
    wide, very_wide = WIDE_BOX_DEGREES

    def span(row):
        return max(row["bbox_lon_span"] or 0.0, row["bbox_lat_span"] or 0.0)

    def several(row):
        return bool(row["declared_municipality"]) and bool(
            _SEVERAL.search(row["declared_municipality"])
        )

    def repeats(row):
        return bool(row["declared_municipality"]) and (
            row["declared_municipality"].strip().casefold()
            == (row["declared_subdivision"] or "").strip().casefold()
        )

    return {
        "mdb_rows": len(mdb),
        "missing_country": sum(1 for r in mdb if not r["declared_country"]),
        "missing_subdivision": sum(1 for r in mdb if not r["declared_subdivision"]),
        "missing_municipality": sum(1 for r in mdb if not r["declared_municipality"]),
        "subdivision_without_municipality": sum(
            1
            for r in mdb
            if r["declared_subdivision"] and not r["declared_municipality"]
        ),
        "municipality_repeats_subdivision": sum(1 for r in mdb if repeats(r)),
        "municipality_lists_several": sum(1 for r in mdb if several(r)),
        "missing_bbox": sum(1 for r in mdb if not r["has_bbox"]),
        f"bbox_over_{wide:g}_degrees": sum(
            1 for r in mdb if r["has_bbox"] and span(r) > wide
        ),
        f"bbox_over_{very_wide:g}_degrees": sum(
            1 for r in mdb if r["has_bbox"] and span(r) > very_wide
        ),
        "bbox_by_extracted_year": dict(
            collections.Counter(
                str(r["bbox_extracted_year"])
                for r in mdb
                if r["bbox_extracted_year"] is not None
            )
        ),
        "atlas_rows_without_location": sum(1 for r in rows if r["source"] == "atlas"),
        "gbfs_rows_with_free_text_location": sum(
            1 for r in rows if r["source"] == "gbfs" and r["declared_municipality"]
        ),
    }


def identity(rows, feeds):
    """The identity-and-duplication section: id namespaces, deprecated rows
    and their redirects, nameless rows, duplicated GBFS ids, and what the
    crosswalk made of the rows."""
    mdb = [row for row in rows if row["source"] == "mdb"]
    deprecated = [row for row in mdb if row["status"] == "deprecated"]
    url_by_id = {row["source_id"]: row["download_url"] for row in mdb}
    gbfs_ids = collections.Counter(
        row["source_id"] for row in rows if row["source"] == "gbfs"
    )
    return {
        "rows_by_source": dict(collections.Counter(row["source"] for row in rows)),
        "id_namespaces": dict(collections.Counter(row["id_namespace"] for row in mdb)),
        "deprecated_rows": len(deprecated),
        "deprecated_with_redirect": sum(1 for r in deprecated if r["redirect_targets"]),
        "redirect_target_present": sum(
            1 for r in deprecated if r["redirect_target_status"] is not None
        ),
        "redirect_target_status": dict(
            collections.Counter(
                r["redirect_target_status"]
                for r in deprecated
                if r["redirect_target_status"] is not None
            )
        ),
        "redirect_shares_target_url": sum(
            1
            for r in deprecated
            if r["download_url"]
            and any(
                r["download_url"] == url_by_id.get(t) for t in r["redirect_targets"]
            )
        ),
        "mdb_rows_without_name": sum(1 for r in mdb if not r["has_name"]),
        "gbfs_duplicate_system_ids": sum(
            1
            for n in collections.Counter(k.split("/")[0] for k in gbfs_ids).values()
            if n > 1
        ),
        "rows_into_feeds": sum(1 for r in rows if r["feed_id"]),
        "rows_dropped_by_reason": dict(
            collections.Counter(r["drop_reason"] for r in rows if r["drop_reason"])
        ),
        "feeds": len(feeds),
        "feeds_by_source": dict(collections.Counter(f["source"] for f in feeds)),
        "feeds_by_crosswalk_method": dict(
            collections.Counter(f.get("crosswalk_method") for f in feeds)
        ),
    }


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
        # The index lock publish takes to write: the snapshot read here is
        # the one the statistics are published against.
        for subdir in ("index", "stats"):
            directory = store.open_subdir(cache_dir, subdir)
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
            "declared_places": declared_places(rows),
            "identity": identity(rows, feeds),
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
