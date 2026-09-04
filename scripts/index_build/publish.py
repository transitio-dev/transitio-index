"""Publish stage: the feeds, places and membership edges as a shippable index.

Writes ``<cache>/index/`` — ``feeds.parquet`` (one row per feed),
``places.parquet`` (one row per place, a GeoParquet with the simplified
boundary), ``edges.parquet`` (one membership row per place/feed/tier) and
``snapshot.json`` (the manifest: a deterministic snapshot id, the schema
version, the source versions, the counts, and each Parquet's SHA-256). The
feeds come from the latest edge stage when one exists (curated, classified or
coverage edges, with the feeds stamped ``coverage_source`` and ``crawlable``),
else from the resolved feeds, else from the crosswalk; the places from the
pruned generation for a curated build, else the expanded generation, else the
names one. Places and edges are optional: an index built before those stages
ran is feeds only, and the reader treats the missing tables the same way.

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

SCHEMA_VERSION = 4
FEEDS_FILE = "feeds.parquet"
PLACES_FILE = "places.parquet"
EDGES_FILE = "edges.parquet"
SNAPSHOT_FILE = "snapshot.json"
NOTICE_FILE = "NOTICE"


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
        # The crawl's evidence and bookkeeping (schema_version 3).
        ("coverage", pa.binary()),
        ("stop_count", pa.int64()),
        ("etag", pa.string()),
        ("last_modified", pa.string()),
        ("last_crawled", pa.string()),
        ("crawl_status", pa.string()),
        # Whether the feed's licence permits redistributing derived data;
        # null when unknown. Set by the license stage.
        ("redistribution_allowed", pa.bool_()),
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
        "coverage": _wkb(record.get("coverage")),
        "stop_count": record.get("stop_count"),
        "etag": record.get("etag"),
        "last_modified": record.get("last_modified"),
        "last_crawled": record.get("last_crawled"),
        "crawl_status": record.get("crawl_status"),
        "redistribution_allowed": record.get("redistribution_allowed"),
        "snapshot": snapshot_id,
    }


def _wkb(value):
    """A hex-encoded WKB column value as bytes, None passing through."""
    return None if value is None else bytes.fromhex(value)


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
        # is the resolver's metro-default rule, so it stays null. A default the
        # prune stage cleared stays cleared, whatever metro survived.
        "default_metro_id": record.get("default_metro_id")
        or (
            metro_ids[0]
            if len(metro_ids) == 1 and not record.get("default_metro_cleared")
            else None
        ),
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


def _read_places(cache_dir, edge_manifest=None, overrides_dir=None):
    """The gazetteer places, the Overture release, the expanded generation
    they descend from and that generation's manifest, or four Nones.

    The pruned generation is what a curated build ships: it must descend
    from the current expanded places and from the very curate generation
    whose edges are being published (``edge_manifest``), and a curated build
    without one is a stage that has not run. Without curation the expanded
    generation is used (it is what coverage derived edges from), falling
    back to the names one for a build that has not run the expand stage.
    The release is taken from the same generation's own manifest, so the
    places cannot be labelled with a different pointer read separately.
    """
    curated = edge_manifest is not None and edge_manifest.get("source") == "curate"
    pruned = cache_dir / "prune" / "places_pruned.json"
    if pruned.is_symlink() or pruned.exists():
        try:
            places, manifest = store.read_jsonl(
                cache_dir / "prune", "places_pruned.json", "places_pruned.jsonl"
            )
        except (store.StoreError, ValueError) as error:
            raise PublishError(f"the pruned places are unreadable: {error}") from error
        if not curated:
            raise PublishError(
                "pruned places exist without a curate generation to have pruned "
                "against; re-run the pipeline in stage order"
            )
        if manifest.get("curate_generation") != edge_manifest.get("generation"):
            raise PublishError(
                "the pruned places were not derived from the edges being "
                "published; re-run the prune stage"
            )
        expanded = _current_expanded(cache_dir)
        if manifest.get("expanded_generation") != expanded.get("generation"):
            raise PublishError(
                "the pruned places do not descend from the current expanded "
                "places; re-run the pipeline in stage order"
            )
        _check_names_lineage(cache_dir, expanded)
        _check_places_overrides(expanded, overrides_dir)
        # The pruned generation is part of the lineage the snapshot records.
        expanded = {**expanded, "pruned_generation": manifest.get("generation")}
        return (
            places,
            manifest.get("overture_release"),
            manifest.get("expanded_generation"),
            expanded,
        )
    if curated:
        raise PublishError(
            "curated edges exist but no pruned places; run the prune stage"
        )
    if not (cache_dir / "gazetteer").is_dir():
        return _no_places(overrides_dir)
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
        else:
            _check_lineage(
                cache_dir,
                manifest,
                (("gazetteer", "seed.json", "seed_generation"),),
                "names places",
                "gazetteer",
            )
        _check_places_overrides(manifest, overrides_dir)
        return (
            places,
            manifest.get("overture_release"),
            manifest.get("generation"),
            manifest,
        )
    # No published places generation: the index is feeds only.
    return _no_places(overrides_dir)


# The catalogue ingests' pointers.
RAW_POINTERS = ("atlas.json", "mdb.json", "gbfs.json")
# The edge pointer each edge stage publishes.
EDGE_POINTERS = {
    "curate": ("curate", "edges_final.json"),
    "classify": ("classify", "edges.json"),
    "coverage": ("coverage", "coverage.json"),
}


def _generations(cache_dir, edges, resolved, places):
    """Every generation the index descends from, leaves and their ancestors,
    as ``({"subdir/pointer": generation}, {table: leaf pointer})``: the
    crosswalk from its pointer (held locked), the rest from the manifests
    that recorded them. A re-run of any one of them makes the index stale."""
    from index_build import classify

    found = {}
    leaves = {}
    # The ingest and crosswalk pointers as they stand now, held locked.
    for name in RAW_POINTERS:
        found[f"raw/{name}"] = classify._current_generation(cache_dir, "raw", name)
    found["crosswalk/feeds.json"] = classify._current_generation(
        cache_dir, "crosswalk", "feeds.json"
    )
    leaves["feeds"] = "crosswalk/feeds.json"
    if resolved is not None:
        found["resolve/feeds_resolved.json"] = resolved.get("generation")
        leaves["feeds"] = "resolve/feeds_resolved.json"
    elif edges is not None:
        found["resolve/feeds_resolved.json"] = edges.get("resolve_generation")
    if places is not None:
        pointer = "expanded.json" if places.get("source") == "expand" else "names.json"
        found[f"gazetteer/{pointer}"] = places.get("generation")
        leaves["places"] = f"gazetteer/{pointer}"
        # The geometry generation the places descend from, whose audit the
        # licence inventory and NOTICE come from.
        found["gazetteer/geometry.json"] = places.get("geometry_generation")
        found["gazetteer/seed.json"] = places.get("seed_generation")
        if pointer == "expanded.json":
            found["gazetteer/names.json"] = places.get("names_generation")
        if places.get("pruned_generation") is not None:
            found["prune/places_pruned.json"] = places["pruned_generation"]
            leaves["places"] = "prune/places_pruned.json"
    if edges is not None and edges.get("source") in EDGE_POINTERS:
        subdir, pointer = EDGE_POINTERS[edges["source"]]
        found[f"{subdir}/{pointer}"] = edges.get("generation")
        leaves["edges"] = leaves["feeds"] = f"{subdir}/{pointer}"
        found["classify/edges.json"] = edges.get("classify_generation")
        found["coverage/coverage.json"] = edges.get("coverage_generation")
        if edges.get("source") == "classify":
            found["classify/edges.json"] = edges.get("generation")
        if edges.get("source") == "coverage":
            found["coverage/coverage.json"] = edges.get("generation")
    found = {key: value for key, value in found.items() if value is not None}
    return found, leaves


def _no_places(overrides_dir):
    """A feeds-only index; a places.yaml that no gazetteer generation
    applied is a stage that has not run yet."""
    from index_build import overrides

    if overrides.places_digest(overrides_dir) is not None:
        raise PublishError(
            "places.yaml exists but no gazetteer generation applied it; run the "
            "gazetteer stage"
        )
    return None, None, None, None


def _check_places_overrides(expanded_manifest, overrides_dir):
    """The places.yaml on disk must be the one the gazetteer applied; a
    generation written before override tracking cannot say which one."""
    from index_build import overrides

    if "places_overrides_sha256" not in expanded_manifest:
        raise PublishError(
            "the places generation predates override tracking; re-run the "
            "gazetteer stage"
        )
    try:
        overrides.expect_digest(
            expanded_manifest["places_overrides_sha256"],
            overrides.places_digest(overrides_dir),
            "places.yaml",
            "gazetteer",
        )
    except overrides.OverrideError as error:
        raise PublishError(str(error)) from error


def _current_expanded(cache_dir):
    """The current expanded generation's manifest; its absence under a
    pruned generation is corruption, never a feeds-only build."""
    try:
        generation, manifest = store.resolve(cache_dir / "gazetteer", "expanded.json")
    except (store.StoreError, ValueError) as error:
        raise PublishError(
            f"the expanded generation the pruned places descend from is "
            f"unreadable: {error}"
        ) from error
    with generation:
        pass
    return manifest


def _check_names_lineage(cache_dir, expanded_manifest):
    """The expanded places must have been derived from the CURRENT seed and
    names generations: a gazetteer rerun without a following expand leaves
    an old expanded/coverage/classify chain that is mutually consistent and
    still stale."""
    _check_lineage(
        cache_dir,
        expanded_manifest,
        (
            ("gazetteer", "seed.json", "seed_generation"),
            ("gazetteer", "names.json", "names_generation"),
        ),
        "expanded places",
        "expand",
    )


def _read_resolved(cache_dir, overrides_dir, *, check_file=True):
    """The resolved feeds and their manifest, or ``(None, None)`` without a
    resolve generation. They must descend from the current crosswalk (a
    re-run crosswalk without a following resolve leaves a feed chain that
    is mutually consistent and still stale) and, when they are what ships
    (``check_file``), from the ``feeds.yaml`` on disk now — a coverage
    generation checks the file itself, and a ``set_coverage`` edit must not
    send the resolve stage back. A ``feeds.yaml`` that no resolve generation
    applied is a stage that has not run yet."""
    from index_build import overrides

    pointer = cache_dir / "resolve" / "feeds_resolved.json"
    if not (pointer.is_symlink() or pointer.exists()):
        if overrides.feeds_digest(overrides_dir) is not None:
            raise PublishError(
                "feeds.yaml exists but no resolve generation applied it; run the "
                "resolve stage"
            )
        return None, None
    try:
        feeds, manifest = store.read_jsonl(
            cache_dir / "resolve", pointer.name, "feeds_resolved.jsonl"
        )
    except (store.StoreError, ValueError) as error:
        raise PublishError(f"the resolve generation is unreadable: {error}") from error
    if manifest.get("crosswalk_generation") is None:
        # A resolve generation from before the crosswalk was recorded
        # cannot demonstrate descent at all.
        raise PublishError(
            "the resolved feeds record no crosswalk generation; re-run the "
            "resolve stage"
        )
    _check_lineage(
        cache_dir,
        manifest,
        (("crosswalk", "feeds.json", "crosswalk_generation"),),
        "resolved feeds",
        "resolve",
    )
    if "feeds_overrides_sha256" not in manifest:
        # Written before override tracking: it cannot say which feeds.yaml
        # shaped it, and an absent file must not read as "none applied".
        raise PublishError(
            "the resolve generation predates override tracking; re-run the "
            "resolve stage"
        )
    if check_file:
        try:
            overrides.expect_digest(
                manifest["feeds_overrides_sha256"],
                overrides.feeds_digest(overrides_dir),
                "feeds.yaml",
                "resolve",
            )
        except overrides.OverrideError as error:
            raise PublishError(str(error)) from error
    return feeds, manifest


def _check_lineage(cache_dir, manifest, ancestors, what, rerun):
    """The ``what`` must descend from the current ``ancestors`` generations
    (subdirectory, pointer, manifest key), else ``rerun`` is the stage to run."""
    for subdir, pointer, key in ancestors:
        path = cache_dir / subdir / pointer
        recorded = manifest.get(key)
        if not (path.is_symlink() or path.exists()):
            if recorded is not None:
                # The ancestor these descend from is gone: nothing can
                # verify them any more.
                raise PublishError(
                    f"the {pointer} generation the {what} descend from no "
                    f"longer exists; re-run the {rerun} stage"
                )
            continue
        try:
            generation, current = store.resolve(cache_dir / subdir, pointer)
        except (store.StoreError, ValueError) as error:
            raise PublishError(
                f"the {pointer} generation is unreadable: {error}"
            ) from error
        with generation:
            pass
        if recorded != current.get("generation"):
            raise PublishError(
                f"the {what} do not descend from the current {pointer} "
                f"generation; re-run the {rerun} stage"
            )


# The stage locks publication holds, in order; the crawl lock follows them.
STAGE_LOCKS = (
    "raw",
    "crosswalk",
    "resolve",
    "gazetteer",
    "coverage",
    "classify",
    "curate",
    "prune",
)


def read_inputs(cache_dir, overrides_dir):
    """Everything a publication reads, lineage-checked, under the locks the
    caller holds: the feeds (from the edge stage, else the resolved feeds,
    else the crosswalk), the edges and their manifest, the places and the
    generations all of it descends from. The license stage reads exactly
    this, so what it licenses is what would ship."""
    # The override digest the curated edges were checked against is the
    # baseline: it is re-read once more right before activation, so an
    # edit during publication cannot ship through a generation built
    # before it — and never re-established from a later read.
    records, edges, coverage, override_digest = _read_coverage(
        cache_dir, locked=True, overrides_dir=overrides_dir
    )
    # Without coverage the resolved feeds ship (identity and crawlability
    # applied), and only without those the crosswalk's.
    resolved, resolve_manifest = _read_resolved(
        cache_dir, overrides_dir, check_file=records is None
    )
    if records is not None:
        sources = coverage.get("sources")
    elif resolved is not None:
        records, sources = resolved, resolve_manifest.get("sources")
    else:
        records, crosswalk = store.read_jsonl(
            cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
        )
        sources = crosswalk.get("sources")
    if not records:
        raise PublishError("no feeds to publish")
    if not sources:
        raise PublishError("the feed manifest records no source versions")
    # A gazetteer that ran but produced no places is a places index of zero
    # places, distinct from a feeds-only build (no gazetteer at all) — hence
    # ``is not None`` throughout, never a truthiness test that folds the two.
    places, overture_release, places_generation, places_manifest = _read_places(
        cache_dir, coverage, overrides_dir
    )
    generations, leaves = _generations(
        cache_dir, coverage, resolve_manifest, places_manifest
    )
    if places is not None:
        # The audit the places descend from must be the current one: a
        # regenerated geometry cannot relabel places built before it.
        from index_build import classify

        consumed = (places_manifest or {}).get("geometry_generation")
        current = classify._current_generation(cache_dir, "gazetteer", "geometry.json")
        if consumed != current:
            raise PublishError(
                "the geometry audit moved since the places were built; re-run the "
                "gazetteer stage"
            )
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
    return {
        "records": records,
        "edges": edges,
        "coverage": coverage,
        "override_digest": override_digest,
        "resolved": resolved,
        "resolve_manifest": resolve_manifest,
        "sources": sources,
        "places": places,
        "overture_release": overture_release,
        "places_generation": places_generation,
        "places_manifest": places_manifest,
        "generations": generations,
        "leaves": leaves,
    }


def _read_licensed(cache_dir, inputs):
    """Swap the license stage's artifacts into ``inputs`` when a license
    generation exists: it must descend from exactly the generations the
    inputs do, else it is stale. Returns the NOTICE bytes, or None when the
    build is not licensed (publication then ships the inputs as they are
    and says so)."""
    from index_build import licensing

    pointer = cache_dir / "license" / licensing.POINTER
    if not (pointer.is_symlink() or pointer.exists()):
        return None
    try:
        generation, manifest = store.resolve(cache_dir / "license", licensing.POINTER)
    except (store.StoreError, ValueError) as error:
        raise PublishError(f"the license generation is unreadable: {error}") from error
    with generation:
        if manifest.get("policy") != licensing.POLICY_VERSION:
            raise PublishError(
                "the licensed artifacts were built under an older licensing policy; "
                "re-run the license stage"
            )
        if manifest.get("generations") != inputs["generations"]:
            raise PublishError(
                "the licensed artifacts do not descend from the current inputs; "
                "re-run the license stage"
            )
        inputs["records"] = store.parse_jsonl(
            generation.read_bytes(licensing.FEEDS_ARTIFACT)
        )
        if inputs["places"] is not None:
            inputs["places"] = store.parse_jsonl(
                generation.read_bytes(licensing.PLACES_ARTIFACT)
            )
        if inputs["edges"] is not None:
            inputs["edges"] = store.parse_jsonl(
                generation.read_bytes(licensing.EDGES_ARTIFACT)
            )
        notice = generation.read_bytes(licensing.NOTICE_ARTIFACT)
    key = f"license/{licensing.POINTER}"
    inputs["generations"] = {**inputs["generations"], key: manifest.get("generation")}
    inputs["leaves"] = {table: key for table in inputs["leaves"]}
    return notice


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
    if manifest is not None:
        if "feeds_overrides_sha256" not in manifest:
            raise PublishError(
                "the edge generation predates override tracking; re-run the "
                "coverage stage"
            )
        try:
            overrides.expect_digest(
                manifest["feeds_overrides_sha256"],
                overrides.feeds_digest(overrides_dir),
                "feeds.yaml",
                "coverage",
            )
        except overrides.OverrideError as error:
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
    from transitio import __version__ as built_with
    from transitio.index import DISCOVERY_SEMANTICS_VERSION, MIN_READER_VERSIONS

    # Every upstream stage's writer lock is held from the reads through the
    # commit, so no crawl, resolve, expand, coverage, classify or curate run
    # can republish between the lineage checks, the gate's verdict and
    # snapshot.json.
    with contextlib.ExitStack() as stack:
        # Stage locks first, the crawl lock last — the order every stage
        # uses (its own lock, then the crawl's), so no lock-order inversion.
        for subdir in (*STAGE_LOCKS, "license"):
            # Created when absent, so a stage that has not run yet cannot
            # slip its first publication in between: the lock exists first.
            held = store.open_subdir(cache_dir, subdir)
            stack.callback(held.close)
            stack.enter_context(store.exclusive_writer(held))
        stack.enter_context(crawl.reading(cache_dir))
        from index_build import overrides

        inputs = read_inputs(cache_dir, overrides_dir)
        licensed = _read_licensed(cache_dir, inputs)
        records = inputs["records"]
        edges = inputs["edges"]
        coverage = inputs["coverage"]
        override_digest = inputs["override_digest"]
        resolved = inputs["resolved"]
        resolve_manifest = inputs["resolve_manifest"]
        sources = inputs["sources"]
        places = inputs["places"]
        overture_release = inputs["overture_release"]
        places_manifest = inputs["places_manifest"]
        generations = inputs["generations"]
        leaves = inputs["leaves"]
        golden_report = None
        if golden_path is not None:
            if edges is None:
                # The gate is mandatory; a feeds-only snapshot must say so.
                raise PublishError(
                    "the golden diff needs classified edges; publish a feeds-only "
                    "snapshot with --no-golden"
                )
            golden_report = _golden_gate(cache_dir, golden_path, edges, coverage)
        digests = []
        if places is not None:
            digests.append(_content_digest(places))
        if resolved is not None or edges is not None or licensed is not None:
            # Resolved, covered and licensed feeds fold in, and edges: the
            # override files and the licensing policy shape them, and no
            # source version pins those.
            digests.append(_content_digest(records))
        if edges is not None:
            digests.append(_content_digest(edges))
        if licensed is not None:
            # The NOTICE ships too: a corrected attribution is a new snapshot.
            digests.append(hashlib.sha256(licensed).hexdigest())
        snapshot_id = _snapshot_id(sources, overture_release, digests)
        feeds_data = _parquet_bytes(records, snapshot_id)
        counts = _counts(records)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            # The snapshot pins the data; the discovery semantics and the
            # build's version are recorded so a reproduction can say whether
            # it is exact, and a reader below the schema's floor refuses it.
            "discovery_semantics_version": DISCOVERY_SEMANTICS_VERSION,
            "min_reader_version": MIN_READER_VERSIONS[SCHEMA_VERSION],
            "built_with": built_with,
            "snapshot_id": snapshot_id,
            "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "sources": sources,
            "counts": counts,
            "feeds_sha256": hashlib.sha256(feeds_data).hexdigest(),
            # The stage generations this index was built from, by pointer,
            # the leaf that produced each table, and the override files
            # applied, so a release can check they are all still current.
            "generations": generations,
            "leaves": leaves,
            "feeds_overrides_sha256": (
                coverage if coverage is not None else resolve_manifest or {}
            ).get("feeds_overrides_sha256"),
            "places_overrides_sha256": (places_manifest or {}).get(
                "places_overrides_sha256"
            ),
            # The crawl the edges and expanded places were measured against.
            "crawl_digest": crawl.states_digest(cache_dir),
            # Whether the license stage's artifacts are what ships, and the
            # NOTICE that ships with them.
            "licensed": licensed is not None,
            "notice_sha256": (
                None if licensed is None else hashlib.sha256(licensed).hexdigest()
            ),
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

        # Curation's third staleness signal: every override file's stale
        # count travels with the snapshot, zero when clean and when nothing
        # was curated. Edge staleness is the curate stage's own count.
        stale = {
            "stale_place_overrides": (places_manifest or {}).get(
                "stale_place_overrides"
            ),
            "stale_feed_overrides": (coverage or {}).get("stale_feed_overrides"),
            "stale_edge_overrides": (
                coverage.get("stale_overrides")
                if coverage is not None and coverage.get("source") == "curate"
                else 0
            ),
        }
        stale = {key: int(value or 0) for key, value in stale.items()}
        manifest.update(stale, stale_overrides=sum(stale.values()))
        # The digests the inputs were checked against, re-read before the
        # first file is replaced: an override file edited during publication
        # must not activate a snapshot built from its predecessor.
        checks = [("edges.yaml", override_digest, overrides.edges_digest, "curate")]
        # feeds.yaml is rechecked even for a crosswalk-only index (recorded
        # None): a file created during publication must not ship unapplied.
        feed_source = coverage if coverage is not None else resolve_manifest
        checks.append(
            (
                "feeds.yaml",
                (feed_source or {}).get("feeds_overrides_sha256"),
                overrides.feeds_digest,
                "coverage" if coverage is not None else "resolve",
            )
        )
        checks.append(
            (
                "places.yaml",
                (places_manifest or {}).get("places_overrides_sha256"),
                overrides.places_digest,
                "gazetteer",
            )
        )

        directory = store.open_subdir(cache_dir, "index")
        try:
            with store.exclusive_writer(directory):
                # An abort here leaves the previous index whole, never a new
                # table under an old manifest.
                for what, recorded, current, rerun in checks:
                    if current(overrides_dir) != recorded:
                        raise PublishError(
                            f"{what} changed during publication; re-run the "
                            f"{rerun} stage"
                        )
                store.write_bytes(directory, FEEDS_FILE, feeds_data)
                # A table this build lacks must not linger from an earlier one,
                # nor a NOTICE from a licensed build under an unlicensed one.
                for name, data in (
                    (PLACES_FILE, places_data),
                    (EDGES_FILE, edges_data),
                    (NOTICE_FILE, licensed),
                ):
                    if data is not None:
                        store.write_bytes(directory, name, data)
                    else:
                        store.unlink(directory, name)
                store.write_file(
                    directory,
                    SNAPSHOT_FILE,
                    lambda: [json.dumps(manifest, indent=2, sort_keys=True)],
                )
        finally:
            directory.close()
    return manifest
