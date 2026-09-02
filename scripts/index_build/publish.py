"""Publish stage: the feeds, places and membership edges as a shippable index.

Writes ``<cache>/index/`` — ``feeds.parquet`` (one row per feed),
``places.parquet`` (one row per place, a GeoParquet with the simplified
boundary), ``edges.parquet`` (one membership row per place/feed/tier) and
``snapshot.json`` (the manifest: a deterministic snapshot id, the schema
version, the source versions, the counts, and each Parquet's SHA-256). The
feeds come from the coverage generation when it exists (stamped with
``coverage_source`` and ``crawlable``, alongside the candidate edges), else from
the crosswalk; the places from the expanded generation, else the names one.
Places and edges are optional: an index built before those stages ran is feeds
only, and the reader treats the missing tables the same way.

The flat identity and crosswalk fields are their own columns; the verbatim Atlas,
MDB and GBFS source rows are kept as JSON-string columns, so nothing is lost and
the field-level columns a query surface needs can be derived later. A place's
``names`` is a ``map<string, string>`` column (language to label), as the plan
defines it.
"""

import collections
import contextlib
import datetime
import hashlib
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

from index_build import store

SCHEMA_VERSION = 2
FEEDS_FILE = "feeds.parquet"
PLACES_FILE = "places.parquet"
EDGES_FILE = "edges.parquet"
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
        ("crawlable", pa.bool_()),
        ("uncrawlable_reason", pa.string()),
        ("coverage_source", pa.string()),
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
        "crawlable": record.get("crawlable"),
        "uncrawlable_reason": record.get("uncrawlable_reason"),
        "coverage_source": record.get("coverage_source"),
        "snapshot": snapshot_id,
    }


def _place_row(record, snapshot_id, service=None):
    metro_ids = record.get("metro_ids") or []
    return {
        "service": _json_block(service),
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


def _content_digest(records):
    """A stable digest of raw records whose content no source version pins."""
    canonical = json.dumps(records, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _snapshot_id(sources, overture_release=None, digests=()):
    # Deterministic in the source versions, so the same inputs always produce the
    # same snapshot id; the build time is metadata and is left out of it.
    # Crosswalk feeds are fully determined by those versions. Places also draw on
    # live Wikidata, and covered feeds and edges on the override files — neither
    # pinned by any source version — so their content digests join the id;
    # otherwise two builds with the same pins but different content would collide.
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
    parts.extend(digests)
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
        ("service", pa.string()),
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


def _service_by_place(edges):
    """Each place's service level summed over the feeds serving it.

    Every tier edge of a (place, feed) pair carries the same struct, so pairs
    are counted once. Each number sums over the feeds that report it and
    stays null when none does: a place served only by declared feeds has
    unknown counts, not zero.
    """
    per_pair = {}
    for edge in edges or []:
        per_pair.setdefault((edge["place_id"], edge["feed_id"]), edge.get("service"))
    totals = {}
    for (place_id, _), service in per_pair.items():
        service = service or {}
        total = totals.setdefault(
            place_id,
            {"feeds": 0, "stops": None, "routes": None, "departures_per_day": None},
        )
        total["feeds"] += 1
        for field in ("stops", "routes", "departures_per_day"):
            if service.get(field) is not None:
                total[field] = (total[field] or 0) + service[field]
    return totals


def _places_parquet_bytes(places, snapshot_id, service_by_place=None):
    """The places as GeoParquet bytes: declared columns plus the WKB boundary.

    The schema is declared, not inferred, so an all-null column (``geonames_id``,
    say) or an all-empty list column keeps its type across builds rather than
    collapsing to ``null`` or ``list<null>``.
    """
    rows = []
    for place in places:
        row = _place_row(
            place, snapshot_id, (service_by_place or {}).get(place["place_id"])
        )
        wkb = place.get("geometry")
        row["geometry"] = bytes.fromhex(wkb) if wkb else None
        rows.append(row)
    schema = _PLACES_SCHEMA.with_metadata({b"geo": _geo_metadata()})
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


_EDGES_SCHEMA = pa.schema(
    [
        ("place_id", pa.string()),
        ("feed_id", pa.string()),
        ("tier", pa.string()),
        ("service", pa.string()),
        ("tier_confidence", pa.float64()),
        ("method", pa.string()),
        ("rehomed_from", pa.list_(pa.string())),
        ("evidence", pa.string()),
        ("curation", pa.string()),
        ("merged_evidence", pa.string()),
        ("curation_history", pa.string()),
        ("classification_fingerprint", pa.string()),
        ("fingerprint_kind", pa.string()),
        ("selector_state", pa.string()),
        ("selector", pa.string()),
        ("needs_review", pa.bool_()),
        ("snapshot", pa.string()),
    ]
)


def _edge_row(record, snapshot_id):
    return {
        "place_id": record["place_id"],
        "feed_id": record["feed_id"],
        "tier": record["tier"],
        "service": _json_block(record.get("service")),
        "tier_confidence": record["tier_confidence"],
        "method": record["method"],
        "rehomed_from": record.get("rehomed_from") or [],
        "evidence": _json_block(record.get("evidence")),
        "curation": _json_block(record.get("curation")),
        "merged_evidence": _json_block(record.get("merged_evidence") or []),
        "curation_history": _json_block(record.get("curation_history") or []),
        "classification_fingerprint": record.get("classification_fingerprint"),
        "fingerprint_kind": record["fingerprint_kind"],
        "selector_state": record["selector_state"],
        "selector": _json_block(record.get("selector")),
        "needs_review": record["needs_review"],
        "snapshot": snapshot_id,
    }


def _edges_parquet_bytes(edges, snapshot_id):
    table = pa.Table.from_pylist(
        [_edge_row(record, snapshot_id) for record in edges], schema=_EDGES_SCHEMA
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def _read_places(cache_dir):
    """The gazetteer places, the Overture release and the generation read,
    or ``(None, None, None)``.

    The expanded generation is preferred (it is what coverage derived edges
    from), falling back to the names one for a build that has not run the
    expand stage. The release is taken from the same generation's own manifest,
    so the places cannot be labelled with a different pointer read separately.
    """
    if not (cache_dir / "gazetteer").is_dir():
        return None, None, None
    for pointer, artifact in (
        ("expanded.json", "places_expanded.jsonl"),
        ("names.json", "places_seed.jsonl"),
    ):
        path = cache_dir / "gazetteer" / pointer
        if not (path.is_symlink() or path.exists()):
            continue
        try:
            places, manifest = store.read_jsonl(
                cache_dir / "gazetteer", pointer, artifact
            )
        except (store.StoreError, ValueError) as error:
            # A pointer that exists but will not resolve is corruption, and
            # corruption must never fall back to an older generation.
            raise PublishError(
                f"the {pointer} generation is unreadable: {error}"
            ) from error
        if pointer == "expanded.json":
            _check_names_lineage(cache_dir, manifest)
        return places, manifest.get("overture_release"), manifest.get("generation")
    # No published places generation: the index is feeds only.
    return None, None, None


def _check_names_lineage(cache_dir, expanded_manifest):
    """The expanded places must have been derived from the CURRENT seed and
    names generations: a gazetteer rerun without a following expand leaves
    an old expanded/coverage/classify chain that is mutually consistent and
    still stale."""
    for pointer, key in (
        ("seed.json", "seed_generation"),
        ("names.json", "names_generation"),
    ):
        path = cache_dir / "gazetteer" / pointer
        recorded = expanded_manifest.get(key)
        if not (path.is_symlink() or path.exists()):
            if recorded is not None:
                # The ancestor these places descend from is gone: nothing
                # can verify them any more.
                raise PublishError(
                    f"the {pointer} generation the expanded places descend "
                    "from no longer exists; re-run the expand stage"
                )
            continue
        try:
            generation, manifest = store.resolve(cache_dir / "gazetteer", pointer)
        except (store.StoreError, ValueError) as error:
            raise PublishError(
                f"the {pointer} generation is unreadable: {error}"
            ) from error
        with generation:
            pass
        if recorded != manifest.get("generation"):
            raise PublishError(
                f"the expanded places do not descend from the current {pointer} "
                "generation; re-run the expand stage"
            )


def _read_coverage(cache_dir, *, locked=False, overrides_dir=None):
    """The feeds, edges, manifest and override digest of the latest edge
    stage, or ``(None, None, None, None)``; stale classified edges are a
    publish error.

    The override file is an input like any other: curated edges must come
    from the ``edges.yaml`` on disk now (by digest), and an ``edges.yaml``
    that no curate generation applied is a stage that has not run yet. The
    digest returned is the one the edges were checked against — the
    baseline every later comparison must use.
    """
    from index_build import classify, overrides

    try:
        feeds, edges, manifest = classify.read_edges(cache_dir, locked=locked)
        current = overrides.applied_digest(manifest, overrides_dir)
    except (classify.ClassifyError, overrides.OverrideError) as error:
        raise PublishError(str(error)) from error
    return feeds, edges, manifest, current


def _golden_gate(cache_dir, golden_path, edges, manifest):
    """The golden diff over the very edges about to ship, run before
    anything is written: a violation fails the publish loudly rather than
    shipping a regression."""
    from index_build import golden

    try:
        report = golden.check(cache_dir, golden_path, edges=edges, manifest=manifest)
    except golden.GoldenError as error:
        raise PublishError(f"golden diff could not run: {error}") from error
    if not report["passed"]:
        problems = "; ".join(
            f"{v.get('feed_id') or 'index'}: {v['problem']}"
            for v in report["violations"]
        )
        raise PublishError(f"golden diff failed: {problems}")
    return report


def publish(cache_dir, *, golden_path=None, overrides_dir=None):
    """Build ``<cache>/index`` from the crosswalk (and gazetteer). Returns the manifest.

    The source versions come from the crosswalk manifest — the ones it actually
    read — rather than re-resolving the ingests, so a republished ingest cannot
    label the snapshot with versions the feeds were not built from. Each Parquet
    is hashed into the manifest and written first, the manifest (carrying the
    matching digests) last, all under ``exclusive_writer``. A reader overlapping a
    publish can therefore only ever see a new Parquet with the old manifest — a
    digest mismatch it refuses — never a mismatched pair read as valid.
    """
    # The coverage generation supersedes the crosswalk as the feed source: its
    # feeds carry coverage_source and crawlability, and its edges ship alongside.
    from index_build import crawl

    # Every upstream stage's writer lock is held from the reads through the
    # commit, so no crawl, resolve, expand, coverage, classify or curate run
    # can republish between the lineage checks, the gate's verdict and
    # snapshot.json.
    with contextlib.ExitStack() as stack:
        # Stage locks first, the crawl lock last — the order every stage
        # uses (its own lock, then the crawl's), so no lock-order inversion.
        for subdir in (
            "crosswalk",
            "resolve",
            "gazetteer",
            "coverage",
            "classify",
            "curate",
        ):
            # Created when absent, so a stage that has not run yet cannot
            # slip its first publication in between: the lock exists first.
            held = store.open_subdir(cache_dir, subdir)
            stack.callback(held.close)
            stack.enter_context(store.exclusive_writer(held))
        stack.enter_context(crawl.reading(cache_dir))
        # The override digest the curated edges were checked against is the
        # baseline: it is re-read once more right before activation, so an
        # edit during publication cannot ship through a generation built
        # before it — and never re-established from a later read.
        records, edges, coverage, override_digest = _read_coverage(
            cache_dir, locked=True, overrides_dir=overrides_dir
        )
        from index_build import overrides

        if records is None:
            records, crosswalk = store.read_jsonl(
                cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
            )
            sources = crosswalk.get("sources")
        else:
            sources = coverage.get("sources")
        if not records:
            raise PublishError("no feeds to publish")
        if not sources:
            raise PublishError("the feed manifest records no source versions")
        golden_report = None
        if golden_path is not None:
            if edges is None:
                # The gate is mandatory; a feeds-only snapshot must say so.
                raise PublishError(
                    "the golden diff needs classified edges; publish a feeds-only "
                    "snapshot with --no-golden"
                )
            golden_report = _golden_gate(cache_dir, golden_path, edges, coverage)

        # A gazetteer that ran but produced no places is a places index of zero
        # places, distinct from a feeds-only build (no gazetteer at all) — hence
        # ``is not None`` throughout, never a truthiness test that folds the two.
        places, overture_release, places_generation = _read_places(cache_dir)
        if places is not None and not overture_release:
            # The release folds into the snapshot id; without it a places index would
            # share the feeds-only id for the same feeds.
            raise PublishError("gazetteer places carry no overture_release")
        if edges is not None:
            if places is None:
                raise PublishError("coverage edges exist but no places generation does")
            if coverage.get("overture_release") != overture_release:
                raise PublishError(
                    "coverage and places come from different Overture releases; "
                    "re-run the pipeline in stage order"
                )
            # The exact generation, not just the release: the edges must
            # reference the places generation this snapshot ships.
            if coverage.get("expanded_generation") != places_generation:
                raise PublishError(
                    "the edges were derived from a different places generation "
                    "than the one being published; re-run the pipeline in stage order"
                )
            if coverage.get("source") not in ("classify", "curate"):
                # Candidate edges carry no tiers; shipping them would publish
                # every edge as unknown with the tier gate silently off.
                raise PublishError(
                    "the edges are unclassified; run the classify stage before publishing"
                )
        digests = []
        if places is not None:
            digests.append(_content_digest(places))
        if edges is not None:
            # Covered feeds and edges also fold in: the override files shape them.
            digests.append(_content_digest(records))
            digests.append(_content_digest(edges))
        snapshot_id = _snapshot_id(sources, overture_release, digests)
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
            places_data = _places_parquet_bytes(
                places, snapshot_id, _service_by_place(edges)
            )
            manifest["places_sha256"] = hashlib.sha256(places_data).hexdigest()
            manifest["overture_release"] = overture_release
            counts["places"] = len(places)
            counts["places_by_kind"] = dict(
                collections.Counter(p["kind"] for p in places)
            )

        edges_data = None
        if edges is not None:
            edges_data = _edges_parquet_bytes(edges, snapshot_id)
            manifest["edges_sha256"] = hashlib.sha256(edges_data).hexdigest()
            manifest["coverage_mode"] = coverage.get("mode")
            # Curation's third staleness signal: the count travels with the
            # snapshot, zero when clean and when nothing was curated.
            manifest["stale_overrides"] = int(coverage.get("stale_overrides") or 0)
            manifest["overrides_sha256"] = override_digest
            if coverage.get("unknown_share") is not None:
                # Recorded so the next build's golden diff can measure drift.
                manifest["unknown_share"] = coverage["unknown_share"]
            if golden_report is not None:
                manifest["golden_entries"] = golden_report["entries"]
            counts["edges"] = len(edges)
            counts["edges_by_tier"] = dict(
                collections.Counter(e["tier"] for e in edges)
            )

        directory = store.open_subdir(cache_dir, "index")
        try:
            with store.exclusive_writer(directory):
                # Before the first file is replaced: an abort here leaves the
                # previous index whole, never a new table under an old manifest.
                if overrides.edges_digest(overrides_dir) != override_digest:
                    raise PublishError(
                        "edges.yaml changed during publication; re-run the curate stage"
                    )
                store.write_bytes(directory, FEEDS_FILE, feeds_data)
                if places_data is not None:
                    store.write_bytes(directory, PLACES_FILE, places_data)
                if edges_data is not None:
                    store.write_bytes(directory, EDGES_FILE, edges_data)
                store.write_file(
                    directory,
                    SNAPSHOT_FILE,
                    lambda: [json.dumps(manifest, indent=2, sort_keys=True)],
                )
        finally:
            directory.close()
    return manifest
