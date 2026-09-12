"""Stage: statistics about the catalogue rows a build saw, after publish.

The article's tables come from a build, not from hand-counted exports: this
stage reads the ingest generations (every Mobility Database, Transitland Atlas
and GBFS row the build ingested) and the crosswalk generation (the feeds they
became) and writes a ``stats`` generation with ``catalogue.parquet`` — one row
per catalogue row, whether or not it became a feed — ``feeds.parquet`` — one
row per published feed, joined with the crawl log, the seed placements and
the published edges — and ``summary.json``, the totals the report renders,
keyed by section: the build's provenance, the declared places, identity and
duplication, availability, licensing, scale, the declared-versus-observed
country agreement and the declared-municipality outcomes. Nothing is re-run and
no network is touched; for a sample build the rows are the sample's, and the
summary records the build's snapshot id and source versions.
"""

import collections
import hashlib
import contextlib
import datetime
import io
import json
import re

import pyarrow as pa
import pyarrow.parquet as pq

from transitio_index import store
from transitio_index.crosswalk import GBFS_ARTIFACT, _clean_url, _host

STATS_POINTER = "stats.json"
# The shape of the stats artifacts; the aggregation script refuses a mismatch.
STATS_SCHEMA_VERSION = 2  # 2: the realtime section
# A partition of the published index: a country code, international or links.
PARTITION_NAME = re.compile(r"[A-Z]{2}|international|links")
# The tables a partition kind may carry; a country partition any of them.
PARTITION_LAYOUT = {"international": {"feeds", "realtime"}, "links": {"edges"}}
CATALOGUE_ARTIFACT = "catalogue.parquet"
FEEDS_ARTIFACT = "feeds.parquet"
PLACES_ARTIFACT = "places.parquet"
SUMMARY_ARTIFACT = "summary.json"
REPORT_ARTIFACT = "report.md"

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


def _feed_lookup(feeds, systems=()):
    """``{(source, key): feed_id}`` for every catalogue row a record carries:
    MDB rows by id, Atlas feeds by Onestop ID, GBFS systems by id and country
    (a duplicated system id is only unambiguous with its country)."""
    lookup = {}
    for feed in feeds:
        if feed.get("mdb_id"):
            lookup[("mdb", feed["mdb_id"])] = feed["feed_id"]
        if feed.get("onestop_id"):
            lookup[("atlas", feed["onestop_id"])] = feed["feed_id"]
    for record in systems:
        system = record.get("gbfs") or {}
        if system.get("system_id"):
            key = ("gbfs", system["system_id"], system.get("country_code"))
            lookup[key] = record["feed_id"]
    return lookup


def catalogue_rows(raw, feeds, snapshot_id=None, systems=()):
    """One row per catalogue row of ``raw`` (``{source: records}``), with the
    index feed it became, or the reason it did not. ``systems`` are the GBFS
    records the crosswalk kept apart: a system it resolved is ``not_transit``,
    one it could not tell apart ``ambiguous_id``; neither is a feed."""
    lookup = _feed_lookup(feeds, systems)
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
        if record.get("spec") == "gbfs":  # an Atlas GBFS feed is a system too
            row["feed_id"], row["drop_reason"] = None, "not_transit"
        else:
            row["feed_id"] = lookup.get(("atlas", record["onestop_id"]))
        rows.append(row)
    for record in gbfs:
        row = _gbfs_row(record, duplicated)
        key = ("gbfs", record["system_id"], record.get("country_code"))
        row["feed_id"] = None
        # The crosswalk mints no id for a system the country does not tell
        # apart (none, or two systems sharing id and country).
        row["drop_reason"] = "not_transit" if key in lookup else "ambiguous_id"
        rows.append(row)
    # Two systems sharing id and country still need distinct keys: the
    # ordinal in catalogue order tells them apart.
    seen = collections.Counter()
    repeated = {
        key
        for key, n in collections.Counter(
            (row["source"], row["source_id"]) for row in rows
        ).items()
        if n > 1
    }
    for row in rows:
        key = (row["source"], row["source_id"])
        if key in repeated:
            seen[key] += 1
            row["source_id"] = f"{row['source_id']}#{seen[key]}"
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


def identity(rows, feeds, systems=None):
    """The identity-and-duplication section: id namespaces, deprecated rows
    and their redirects, nameless rows, duplicated GBFS ids, and what the
    crosswalk made of the rows: feeds, and the GBFS systems it kept apart
    (``systems``, a per-build count; None when aggregating archives)."""
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
        "gbfs_systems_kept": None if systems is None else len(systems),
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


def _parquet(rows, schema=CATALOGUE_SCHEMA):
    table = pa.Table.from_pylist(rows, schema=schema)
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
        generation, crosswalk = store.resolve(cache_dir / "crosswalk", "feeds.json")
        with generation:
            # Both artifacts from the one verified generation; one written
            # before the crosswalk kept the systems apart has no systems.
            feeds = store.parse_jsonl(generation.read_bytes("feeds.jsonl"))
            systems = (
                store.parse_jsonl(generation.read_bytes(GBFS_ARTIFACT))
                if generation.has(GBFS_ARTIFACT)
                else []
            )
        raw, manifests = _read_raw(cache_dir)
        if not raw:
            raise StatsError("no ingest generation to gather statistics from")
        snapshot = _snapshot(cache_dir, crosswalk, manifests)
        snapshot_id = snapshot.get("snapshot_id")
        rows = catalogue_rows(raw, feeds, snapshot_id, systems)
        # An ingest that read a local file (no URL) is a cut sample.
        local = [
            s
            for s, m in manifests.items()
            if not (m.get("csv_url") or m.get("archive_url"))
        ]
        summary = {
            "build": {
                "snapshot_id": snapshot_id,
                "stats_schema_version": STATS_SCHEMA_VERSION,
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
            "identity": identity(rows, feeds, systems),
        }
        from transitio_index import crawl

        with crawl.reading(cache_dir):
            # The crawl the published edges were measured against.
            if crawl.states_digest(cache_dir) != snapshot.get("crawl_digest"):
                raise StatsError(
                    "the crawl changed since the index was published; re-run "
                    "the pipeline in stage order"
                )
            published, realtime, edges, places = _index_tables(cache_dir, snapshot)
            by_place = {place["place_id"]: place for place in places}
            statuses = {
                r["source_id"]: r["status"] for r in rows if r["source"] == "mdb"
            }
            # The companions are transit feeds too: one row each, never
            # crawled, so the table matches the crosswalk's feeds.
            feed_table = feed_rows(
                published + [{**r, "spec": "gtfs-rt"} for r in realtime],
                edges,
                by_place,
                _crawl_log(cache_dir),
                _placements(cache_dir, snapshot),
                statuses,
                snapshot_id,
            )
        summary.update(feed_sections(feed_table))
        summary["realtime"] = realtime_section(realtime, published)
        place_table = place_rows(places, edges, snapshot_id)
        summary["duplicate_coverage"] = duplicate_coverage(edges, rows)
        summary["distributions"] = distributions(edges, places)
        manifest = {
            "source": "stats",
            "stats_schema_version": STATS_SCHEMA_VERSION,
            "snapshot_id": snapshot_id,
            "crosswalk_generation": crosswalk.get("generation"),
            "raw_generations": {s: m.get("generation") for s, m in manifests.items()},
            "catalogue_rows": len(rows),
            "sections": sorted(summary),
            "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        manifest["feeds"] = len(feed_table)
        manifest["realtime"] = len(realtime)
        manifest["places"] = len(place_table)
        manifest["sections"] = sorted(summary)
        data = _parquet(rows)
        feed_data = _parquet(feed_table, FEED_SCHEMA)
        place_data = _parquet(place_table, PLACE_SCHEMA)
        text = json.dumps(summary, indent=2, sort_keys=True)
        report = render_report(summary)
        artifacts = {
            CATALOGUE_ARTIFACT: lambda: [data],
            FEEDS_ARTIFACT: lambda: [feed_data],
            PLACES_ARTIFACT: lambda: [place_data],
            SUMMARY_ARTIFACT: lambda: [text],
            REPORT_ARTIFACT: lambda: [report],
        }
        return store.publish(
            cache_dir / "stats", STATS_POINTER, artifacts, manifest, held=directory
        )


# ---- feed level ----

FEED_SCHEMA = pa.schema(
    [
        ("feed_id", pa.string()),
        ("source", pa.string()),
        ("spec", pa.string()),
        ("crosswalk_method", pa.string()),
        ("crosswalk_confidence", pa.float64()),
        ("catalogue_status", pa.string()),
        ("crawl_outcome", pa.string()),
        ("failure_class", pa.string()),
        ("stop_count", pa.int64()),
        ("route_count", pa.int64()),
        ("has_calendar", pa.bool_()),
        ("home_country", pa.string()),
        ("scope", pa.string()),
        ("country_shares", pa.string()),
        ("declared_countries", pa.list_(pa.string())),
        ("country_agreement", pa.string()),
        ("declared_place_id", pa.string()),
        ("declared_level", pa.string()),
        ("municipality_outcome", pa.string()),
        ("download_url", pa.string()),
        ("places_served", pa.int64()),
        ("cities_served", pa.int64()),
        ("countries_served", pa.int64()),
        ("edges_by_tier", pa.string()),
        ("edges_by_category", pa.string()),
        ("relevance_max", pa.float64()),
        ("relevance_median", pa.float64()),
        ("licence_state", pa.string()),
        ("redistribution_allowed", pa.bool_()),
        ("snapshot_id", pa.string()),
    ]
)
# The crawl log's method for a feed, as the availability section counts it.
CRAWL_OUTCOMES = {
    "download": "ok",
    "not_modified": "not_modified",
    "range": "range",
    "failed": "failed",
    "skipped": "skipped",
}
_HTTP = re.compile(r"HTTP (\d{3})")


def failure_class(reason):
    """The class of a crawl failure from the fetcher's recorded reason."""
    if not reason:
        return None
    text = reason.lower()
    status = _HTTP.search(reason)
    if status:
        code = status.group(1)
        if code in ("401", "403", "404"):
            return code
        return "5xx" if code.startswith("5") else "other_http"
    if "not a zip file" in text or "not an archive" in text:
        return "not-an-archive"
    if "ceiling" in text or "over the" in text:
        return "oversize"
    if "certificate" in text or "ssl" in text or "tls" in text:
        return "tls"
    if "timed out" in text or "timeout" in text:
        return "timeout"
    if "getaddrinfo" in text or "name or service" in text or "nodename" in text:
        return "dns"
    if "scheme" in text or "blocked" in text or "not fetched" in text:
        return "blocked_url"
    if "unreachable" in text or "transport" in text:
        return "transport"
    return "other"


def _median(values):
    values = sorted(values)
    if not values:
        return None
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2


def _municipality_outcome(placement, served, places):
    """How the crawled places relate to the declared municipality: in the
    place, in its region (a served place's parent chain reaches the declared
    place or its parent), elsewhere, or unplaceable when the declared place
    left the index."""
    if placement is None or not served:
        return None
    declared = placement["place_id"]
    if declared not in places:
        return "unplaceable"
    if declared in served:
        return "in_place"
    targets = {declared, _region_of(declared, places)} - {None}
    for place_id in served:
        seen = set()
        current = place_id
        while current is not None and current not in seen:
            if current in targets:
                return "in_region"
            seen.add(current)
            current = (places.get(current) or {}).get("parent_id")
    return "elsewhere"


def _region_of(place_id, places):
    """The first region in a place's ancestry (itself when it is one), or
    None — a district's parent may be a city, and a city parented straight
    to its country has no region to judge by."""
    seen, current = set(), place_id
    while current is not None and current not in seen:
        place = places.get(current) or {}
        if place.get("kind") == "region":
            return current
        seen.add(current)
        current = place.get("parent_id")
    return None


def _licence_state(feed):
    """Whether the catalogues declare a licence for the feed."""
    atlas = (feed.get("atlas") or {}).get("license") or {}
    if atlas.get("spdx_identifier") or atlas.get("url"):
        return "declared"
    if ((feed.get("mdb") or {}).get("urls") or {}).get("license"):
        return "declared"
    return "none"


def _download_url(feed):
    mdb = ((feed.get("mdb") or {}).get("urls") or {}).get("direct_download")
    atlas = ((feed.get("atlas") or {}).get("urls") or {}).get("static_current")
    gbfs = (feed.get("gbfs") or {}).get("auto_discovery_url")
    return _clean_url(mdb) or _clean_url(atlas) or _clean_url(gbfs)


def feed_rows(
    feeds, edges, places, crawl_log, placements, status_by_mdb_id, snapshot_id=None
):
    """One row per index feed from the published tables, the crawl log
    (keyed by the crawl-time id or an alias), the seed placements and the
    catalogue statuses."""
    from transitio_index import classify, coverage

    canonical = coverage._canonical_ids(feeds)
    log = {}
    for record in crawl_log:
        feed_id = canonical.get(record.get("feed_id"))
        if feed_id is not None:
            log[feed_id] = record
    placed = {}
    for placement in placements:
        feed_id = canonical.get(placement.get("feed_id"))
        if feed_id is not None:
            placed.setdefault(feed_id, placement)
    by_feed = collections.defaultdict(list)
    for edge in edges:
        by_feed[edge["feed_id"]].append(edge)
    rows = []
    for feed in feeds:
        feed_id = feed["feed_id"]
        mine = by_feed.get(feed_id, [])
        served = {e["place_id"] for e in mine}
        crawled = {e["place_id"] for e in mine if e.get("method") == "crawl"}
        record = log.get(feed_id)
        outcome = CRAWL_OUTCOMES.get((record or {}).get("method"), "other")
        relevance = [e["relevance"] for e in mine if e.get("relevance") is not None]
        files = set(feed.get("files") or (record or {}).get("files") or [])
        home = feed.get("home_country")
        declared = feed.get("declared_countries") or []
        rows.append(
            {
                "feed_id": feed_id,
                "source": feed.get("source"),
                "spec": feed.get("spec"),
                "crosswalk_method": feed.get("crosswalk_method"),
                "crosswalk_confidence": feed.get("crosswalk_confidence"),
                "catalogue_status": status_by_mdb_id.get(feed.get("mdb_id")),
                "crawl_outcome": outcome if record else "not_crawled",
                "failure_class": (
                    failure_class(record.get("fallback_reason"))
                    if record and record.get("method") == "failed"
                    else None
                ),
                "stop_count": feed.get("stop_count"),
                "route_count": (record or {}).get("route_count"),
                "has_calendar": bool(files & {"calendar.txt", "calendar_dates.txt"}),
                "home_country": home,
                "scope": feed.get("scope"),
                "country_shares": json.dumps(
                    feed.get("country_shares") or {}, sort_keys=True
                ),
                "declared_countries": list(declared),
                "country_agreement": classify._agreement(home, declared),
                "declared_place_id": (placed.get(feed_id) or {}).get("place_id"),
                "declared_level": (placed.get(feed_id) or {}).get("level"),
                "municipality_outcome": _municipality_outcome(
                    placed.get(feed_id), crawled, places
                ),
                "download_url": _download_url(feed),
                "places_served": len(served),
                "cities_served": sum(
                    1 for p in served if (places.get(p) or {}).get("kind") == "city"
                ),
                "countries_served": len(
                    {(places.get(p) or {}).get("country_code") for p in served} - {None}
                ),
                "edges_by_tier": json.dumps(
                    dict(collections.Counter(e["tier"] for e in mine)), sort_keys=True
                ),
                "edges_by_category": json.dumps(
                    dict(
                        collections.Counter(
                            e.get("relevance_category")
                            for e in mine
                            if e.get("relevance_category")
                        )
                    ),
                    sort_keys=True,
                ),
                "relevance_max": max(relevance) if relevance else None,
                "relevance_median": _median(relevance),
                "licence_state": _licence_state(feed),
                "redistribution_allowed": feed.get("redistribution_allowed"),
                "snapshot_id": snapshot_id,
            }
        )
    return rows


# The feed sources that belong to each catalogue (``both`` is in two).
CATALOGUE_SOURCES = {
    "mdb": ("mdb", "both"),
    "atlas": ("atlas", "both"),
    "gbfs": ("systems_csv", "gbfs"),
}


def _agreement_share(rows):
    """Agreement counts for one catalogue's feeds and the share of the
    judged feeds (declared and observed) whose declaration is wrong."""
    counts = collections.Counter(r["country_agreement"] for r in rows)
    judged = counts["agree"] + counts["disagree"]
    return {
        **dict(counts),
        "disagreement_share": counts["disagree"] / judged if judged else None,
    }


def _quantiles(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "median": _median(values),
        "p95": values[min(len(values) - 1, int(round(0.95 * (len(values) - 1))))],
        "max": values[-1],
    }


def feed_sections(rows):
    """The summary sections the feed rows answer: availability by outcome,
    failure class and catalogue status; licensing; scale; the declared-versus-
    observed country agreement; the declared-municipality outcomes."""

    def counts(key, subset=None):
        return dict(
            collections.Counter(
                r[key] for r in (subset if subset is not None else rows) if r[key]
            )
        )

    crawled = [r for r in rows if r["crawl_outcome"] != "not_crawled"]
    by_status = collections.defaultdict(collections.Counter)
    for r in crawled:
        by_status[r["catalogue_status"] or "unknown"][r["crawl_outcome"]] += 1
    return {
        "availability": {
            "feeds": len(rows),
            "crawled": len(crawled),
            "by_outcome": counts("crawl_outcome"),
            "failures_by_class": counts("failure_class"),
            "outcome_by_catalogue_status": {
                status: dict(c) for status, c in sorted(by_status.items())
            },
            "with_calendar": sum(1 for r in crawled if r["has_calendar"]),
        },
        "licensing": {
            "licence_state": counts("licence_state"),
            "redistribution_allowed": dict(
                collections.Counter(
                    {True: "yes", False: "no", None: "unknown"}[
                        r["redistribution_allowed"]
                    ]
                    for r in rows
                )
            ),
        },
        "scale": {
            # Stops over the crawled feeds; places and countries over every
            # feed, a feed without published edges counting as zero.
            "stop_count": _quantiles([r["stop_count"] for r in rows]),
            "places_served": _quantiles([r["places_served"] for r in rows]),
            "countries_served": dict(
                collections.Counter(str(r["countries_served"]) for r in rows)
            ),
        },
        "country_agreement": {
            "by_agreement": counts("country_agreement"),
            "by_catalogue": {
                catalogue: _agreement_share([r for r in rows if r["source"] in sources])
                for catalogue, sources in CATALOGUE_SOURCES.items()
            },
            "scope": counts("scope"),
        },
        "declared_municipality": {
            "placed": sum(1 for r in rows if r["declared_place_id"]),
            "by_level": counts("declared_level"),
            "outcome": counts("municipality_outcome"),
        },
    }


def _index_tables(cache_dir, snapshot):
    """``(feeds, realtime, edges, places)`` of the published index, its
    partitions joined; JSON columns decoded. Every file is read once and
    checked against the snapshot's digest and row count, and only the
    layout's own partition and table names are opened."""
    tables = {"feeds": [], "realtime": [], "edges": [], "places": []}
    for partition, listed in (snapshot.get("partitions") or {}).items():
        if not PARTITION_NAME.fullmatch(partition):
            raise StatsError(f"{partition!r}: not a partition of the index")
        allowed = PARTITION_LAYOUT.get(partition, set(tables))
        for table, entry in listed.items():
            if table not in allowed:
                raise StatsError(f"{partition}/{table}: not a table of the index")
            path = cache_dir / "index" / partition / f"{table}.parquet"
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != (entry or {}).get("sha256"):
                raise StatsError(f"{path}: does not match the snapshot's digest")
            rows = pq.read_table(io.BytesIO(data)).to_pylist()
            if len(rows) != (entry or {}).get("rows"):
                raise StatsError(
                    f"{path}: {len(rows)} rows, the snapshot lists {entry}"
                )
            tables[table].extend(rows)
    for feed in tables["feeds"] + tables["realtime"]:
        for key in ("atlas", "mdb", "gbfs", "country_shares", "urls"):
            if isinstance(feed.get(key), str):
                feed[key] = json.loads(feed[key])
    return tables["feeds"], tables["realtime"], tables["edges"], tables["places"]


def realtime_section(realtime, feeds):
    """The GTFS-RT companions: how many, linked to a static feed of the index
    or not, by source and by the entity types their endpoints carry, and
    the static feeds with at least one companion."""
    static = {feed["feed_id"] for feed in feeds}
    linked = sum(1 for r in realtime if r.get("static_feed_id") in static)
    return {
        "feeds": len(realtime),
        "linked": linked,
        "unlinked": len(realtime) - linked,
        "by_source": dict(collections.Counter(r["source"] for r in realtime)),
        "by_entity_type": dict(
            collections.Counter(
                t for r in realtime for t in r.get("entity_types") or ()
            )
        ),
        "by_link_method": dict(
            collections.Counter(r.get("static_link_method") or "none" for r in realtime)
        ),
        "static_feeds_with_realtime": sum(
            1 for feed in feeds if feed.get("realtime_feed_ids")
        ),
    }


def _crawl_log(cache_dir):
    """The crawl log's records, each with the feed's route count when its
    routes were crawled; empty without a crawl."""
    from transitio_index import classify, crawl

    path = cache_dir / "crawl" / crawl.LOG_FILE
    records = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                records.append(json.loads(line))
    routes = {}
    for feed_dir, state in crawl.crawled_feeds(cache_dir):
        with crawl.verified_member(feed_dir, state, "routes.txt") as opened:
            if opened is not None:
                routes[state.get("feed_id")] = len(classify._read_routes(opened)[0])
    for record in records:
        record["route_count"] = routes.get(record.get("feed_id"))
    return records


def _placements(cache_dir, snapshot):
    """The seed placements the snapshot descends from, or none."""
    recorded = (snapshot.get("generations") or {}).get("gazetteer/seed.json")
    if recorded is None:
        return []
    placements, manifest = store.read_jsonl(
        cache_dir / "gazetteer", "seed.json", "feed_places.jsonl"
    )
    if manifest.get("generation") != recorded:
        raise StatsError(
            "the published index was not built from the current seed placements; "
            "re-run the pipeline in stage order"
        )
    return placements


# ---- place level, duplicate coverage, distributions and the report ----

PLACE_SCHEMA = pa.schema(
    [
        ("place_id", pa.string()),
        ("kind", pa.string()),
        ("country_code", pa.string()),
        ("feeds", pa.int64()),
        ("feeds_by_category", pa.string()),
        ("departures_per_day", pa.float64()),
        ("has_primary", pa.bool_()),
        ("snapshot_id", pa.string()),
    ]
)
# Two feeds whose served places overlap by at least this containment (the
# shared places over the smaller set) cover the same ground.
DUPLICATE_CONTAINMENT = 0.9
# One line per section, rendered under its heading.
DEFINITIONS = {
    "build": "The snapshot the statistics describe and the catalogues it read.",
    "declared_places": (
        "What the catalogue rows say about where a feed is, counted over the "
        "MDB rows the build ingested; boxes are the declared bounding boxes."
    ),
    "identity": (
        "Catalogue ids, deprecated rows and their redirects, and what the "
        "crosswalk made of the rows (feeds by source and match method)."
    ),
    "availability": (
        "Crawl outcomes per feed, the failure classes from the fetcher's "
        "recorded reason, and outcomes by the row's catalogue status."
    ),
    "licensing": "Licence declarations and the redistribution judgement per feed.",
    "realtime": (
        "The GTFS-RT companions shipped beside the GTFS feeds: linked to a "
        "static feed of the index or not, by source, endpoint entity type "
        "and link method, and the static feeds that have one."
    ),
    "scale": (
        "Stops per crawled feed and places per feed (count, median, 95th "
        "percentile, maximum), and countries served per feed."
    ),
    "country_agreement": (
        "The catalogues' declared country against the home country classify "
        "found from the stops; the share is disagree over agree plus disagree."
    ),
    "declared_municipality": (
        "Where a crawled feed's stops fall relative to the municipality the "
        "catalogue declared: in the place, in its region, elsewhere, or "
        "unplaceable when the declared place left the index."
    ),
    "duplicate_coverage": (
        "Feed pairs whose served place sets overlap at the containment "
        "threshold (shared places over the smaller set): a catalogue duplicate "
        "when one row redirects to the other or they share a download URL."
    ),
    "distributions": (
        "Edges by tier and relevance category, and relevance quantiles, per "
        "country and per place kind."
    ),
    "places": (
        "The published places per kind, the share with a primary feed, feeds "
        "per place and their departures per day (aggregated builds)."
    ),
}
REPORT_SECTIONS = (
    "build",
    "declared_places",
    "identity",
    "availability",
    "licensing",
    "realtime",
    "scale",
    "country_agreement",
    "declared_municipality",
    "duplicate_coverage",
    "distributions",
)


def _service(edge):
    service = edge.get("service")
    if isinstance(service, str):
        service = json.loads(service)
    return service or {}


def place_rows(places, edges, snapshot_id=None):
    """One row per published place: the feeds serving it (per relevance
    category, counted once per feed), its departures per day summed over the
    pairs that report them, and whether a primary feed serves it."""
    by_place = collections.defaultdict(list)
    for edge in edges:
        by_place[edge["place_id"]].append(edge)
    rows = []
    for place in places:
        mine = by_place.get(place["place_id"], [])
        feeds = {e["feed_id"] for e in mine}
        categories = collections.defaultdict(set)
        for e in mine:
            if e.get("relevance_category"):
                categories[e["relevance_category"]].add(e["feed_id"])
        per_feed = {}
        for e in mine:
            value = _service(e).get("departures_per_day")
            if per_feed.get(e["feed_id"]) is None:
                per_feed[e["feed_id"]] = value
        reported = [d for d in per_feed.values() if d is not None]
        rows.append(
            {
                "place_id": place["place_id"],
                "kind": place.get("kind"),
                "country_code": place.get("country_code"),
                "feeds": len(feeds),
                "feeds_by_category": json.dumps(
                    {c: len(f) for c, f in sorted(categories.items())}
                ),
                "departures_per_day": sum(reported) if reported else None,
                "has_primary": bool(categories.get("primary")),
                "snapshot_id": snapshot_id,
            }
        )
    return rows


def duplicate_coverage(edges, catalogue):
    """Pairs of feeds whose served place sets overlap by at least
    ``DUPLICATE_CONTAINMENT``, each unordered pair once: a
    ``catalogue_duplicate`` when one feed's MDB row redirects to the other's
    or the two share a download URL, else a ``genuine_overlap``."""
    served = collections.defaultdict(set)
    for edge in edges:
        served[edge["feed_id"]].add(edge["place_id"])
    by_place = collections.defaultdict(list)
    for feed_id, places in served.items():
        for place_id in places:
            by_place[place_id].append(feed_id)
    shared = collections.Counter()
    for feeds in by_place.values():
        feeds = sorted(feeds)
        for i, a in enumerate(feeds):
            for b in feeds[i + 1 :]:
                shared[(a, b)] += 1
    feed_of_row = {
        (row["source"], row["source_id"]): row["feed_id"] for row in catalogue
    }
    urls, redirects = collections.defaultdict(set), collections.defaultdict(set)
    for row in catalogue:
        if row["feed_id"] and row.get("download_url"):
            urls[row["feed_id"]].add(row["download_url"])
        for target in row.get("redirect_targets") or []:
            other = feed_of_row.get(("mdb", target))
            if row["feed_id"] and other:
                redirects[row["feed_id"]].add(other)
    pairs = []
    for (a, b), common in sorted(shared.items()):
        containment = common / min(len(served[a]), len(served[b]))
        if containment < DUPLICATE_CONTAINMENT:
            continue
        linked = b in redirects[a] or a in redirects[b] or bool(urls[a] & urls[b])
        pairs.append(
            {
                "feeds": [a, b],
                "shared_places": common,
                "containment": round(containment, 3),
                "kind": "catalogue_duplicate" if linked else "genuine_overlap",
            }
        )
    return {
        "pairs": len(pairs),
        "by_kind": dict(collections.Counter(p["kind"] for p in pairs)),
        "containment": DUPLICATE_CONTAINMENT,
        "list": pairs,
    }


def distributions(edges, places):
    """Tier and relevance-category counts per country and per place kind,
    and the relevance quantiles per place kind."""
    by_id = {place["place_id"]: place for place in places}
    tiers = {"country": collections.defaultdict(collections.Counter)}
    tiers["kind"] = collections.defaultdict(collections.Counter)
    categories = {key: collections.defaultdict(collections.Counter) for key in tiers}
    relevance = {key: collections.defaultdict(list) for key in tiers}
    for edge in edges:
        place = by_id.get(edge["place_id"]) or {}
        keys = {
            "country": place.get("country_code") or "?",
            "kind": place.get("kind") or "?",
        }
        for axis, key in keys.items():
            tiers[axis][key][edge["tier"]] += 1
            if edge.get("relevance_category"):
                categories[axis][key][edge["relevance_category"]] += 1
            if edge.get("relevance") is not None:
                relevance[axis][key].append(edge["relevance"])
    out = {}
    for axis in ("country", "kind"):
        out[f"tiers_by_{axis}"] = {k: dict(t) for k, t in sorted(tiers[axis].items())}
        out[f"categories_by_{axis}"] = {
            k: dict(c) for k, c in sorted(categories[axis].items())
        }
        out[f"relevance_by_{axis}"] = {
            k: _quantiles(v) for k, v in sorted(relevance[axis].items())
        }
    return out


def _cell(value):
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value)
    # A pipe or a line break in a value (an archive label, a URL) would
    # break the table: escaped and folded to one line.
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _table(rows, columns):
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for row in rows:
        lines.append("| " + " | ".join(_cell(row.get(c)) for c in columns) + " |")
    return lines


def render_report(summary):
    """``report.md`` from the summary: one section per summary section, in
    the report's order, scalars as a key/value table, nested counts as a
    table per key, lists of records as a table."""
    lines = ["# Build statistics", ""]
    build = summary.get("build") or {}
    lines.append(
        f"Snapshot `{build.get('snapshot_id')}` ({build.get('sample')}); "
        f"schema {build.get('schema_version')}, Overture "
        f"{build.get('overture_release')}."
    )
    extra = sorted(key for key in summary if key not in REPORT_SECTIONS)
    for section in (*REPORT_SECTIONS, *extra):
        content = summary.get(section)
        if not content:
            continue
        lines += ["", f"## {section.replace('_', ' ').capitalize()}", ""]
        if section in DEFINITIONS:
            lines += [DEFINITIONS[section], ""]
        scalars = [
            {"metric": k, "value": v}
            for k, v in content.items()
            if not isinstance(v, (dict, list))
        ]
        if scalars:
            lines += _table(scalars, ["metric", "value"])
        for key, value in content.items():
            if isinstance(value, dict) and value:
                lines += ["", f"### {key.replace('_', ' ')}", ""]
                if all(isinstance(v, dict) for v in value.values()):
                    columns = sorted({c for v in value.values() for c in v})
                    rows = [{"key": k, **v} for k, v in value.items()]
                    lines += _table(rows, ["key", *columns])
                else:
                    rows = [{"key": k, "value": v} for k, v in value.items()]
                    lines += _table(rows, ["key", "value"])
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                lines += ["", f"### {key.replace('_', ' ')}", ""]
                lines += _table(value, list(value[0]))
    return "\n".join(lines) + "\n"
