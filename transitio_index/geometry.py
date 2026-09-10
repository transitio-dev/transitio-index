"""Attach simplified place geometry and audit its source licences.

Reads the Overture ``division_area`` polygons for the seeded admin places,
simplifies them to a shipping tolerance, and audits each area's sources against
an allowlist of audited ``(dataset, licence)`` pairs: geometry ships (as
hex-encoded WKB) only when every source that built it is on the allowlist, and
its attribution goes into ``NOTICE``; geometry with any unaudited or unlicensed
source is omitted and recorded in the licence inventory. A metro's geometry is
the union of its member cities' shipped polygons, so it never carries what a
member may not. The shipped geometry is simplified to a tolerance; the boundary
lookup used for point-in-polygon (the expand and coverage stages) memoizes
geometry at that same tolerance.
"""

import collections
import datetime
import os
import uuid

import numpy as np
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import shapely

from transitio_index import overrides, overture, store
from transitio_index.progress import progress

DIVISION_AREA_PATH = "release/{release}/theme=divisions/type=division_area"
AREA_PROJECT = ["division_id", "geometry", "sources", "is_land"]

# How many divisions' areas to resolve at once in ``attach_geometry``. Holding
# every seeded division's polygons in memory at once peaks high enough to OOM a
# feed-dense country's build on a memory-tight host; a positive value caps the
# working set to that many divisions per read, at the cost of extra dataset
# scans. Unset or non-positive resolves them in a single pass (no overhead),
# which is the default for a normally-resourced build.
AREA_CHUNK = int(os.environ.get("TRANSITIO_AREA_CHUNK", "0") or "0")

# Explicit allowlist of AUDITED geometry sources, keyed by ``(dataset, licence)``
# so a new upstream dataset is not shipped on a familiar licence until it is
# audited. Each records the NOTICE credit, the licence's canonical URL, and
# whether the licence is share-alike (ODbL), which the plan flags as a recorded
# policy decision. The pairs are those the pinned release carries.
SOURCE_ALLOWLIST = {
    ("OpenStreetMap", "ODbL-1.0"): {
        "credit": "OpenStreetMap, © OpenStreetMap contributors",
        "licence": "ODbL 1.0",
        "url": "https://opendatacommons.org/licenses/odbl/1-0/",
        "share_alike": True,
    },
    ("Esri Community Maps", "CC0-1.0"): {
        "credit": "Esri Community Maps",
        "licence": "CC0 1.0",
        "url": "https://creativecommons.org/publicdomain/zero/1.0/",
        "share_alike": False,
    },
    ("geoBoundaries", "CC-BY-4.0"): {
        "credit": "geoBoundaries",
        "licence": "CC BY 4.0",
        "url": "https://creativecommons.org/licenses/by/4.0/",
        "share_alike": False,
    },
    ("Maps Entity Variant Names", "CC0-1.0"): {
        "credit": "Maps Entity Variant Names",
        "licence": "CC0 1.0",
        "url": "https://creativecommons.org/publicdomain/zero/1.0/",
        "share_alike": False,
    },
}

# Overture aggregates the sources above under its own permissive licence; the
# inventory records it as the aggregator, versioned by the pinned release. The URL
# is the licence's canonical URL, as the inventory records licence URLs.
OVERTURE_AGGREGATOR = {
    "dataset": "Overture Maps divisions",
    "license": "CDLA-Permissive-2.0",
    "url": "https://cdla.dev/permissive-2-0/",
}

# The audited DERIVED-data sources — memberships and codes computed from them
# at build time, as opposed to shipped geometry — with the credit and licence
# URL their terms require, kept apart from the permission to use them so that
# a closed source still names its terms. Keyed like the geometry allowlist.
DERIVED_SOURCES = {
    ("Overture Maps divisions", "CDLA-Permissive-2.0"): {
        "credit": "Overture Maps Foundation, divisions theme",
        "licence": "CDLA-Permissive-2.0",
        "url": "https://cdla.dev/permissive-2-0/",
        "share_alike": False,
    },
    ("Eurostat metropolitan regions", "Eurostat-2011/833/EU"): {
        "credit": "Source: Eurostat, metropolitan regions (NUTS 2021)",
        "licence": "Eurostat copyright notice (Commission Decision 2011/833/EU)",
        "url": "https://ec.europa.eu/eurostat/help/copyright-notice",
        "share_alike": False,
    },
    ("FAO city-regions 2024", "CC-BY-4.0"): {
        "credit": (
            "Girgin, Cattaneo, de By, McMenomy, Nelson and Vaz (2024), Worldwide "
            "Delineation of Multi-Tier City-Regions (Zenodo), CC BY 4.0"
        ),
        "licence": "CC-BY-4.0",
        "url": "https://doi.org/10.5281/zenodo.11187634",
        "share_alike": False,
    },
    # Non-commercial terms: used for point-in-polygon at build time only,
    # never shipped; the credit is the one the terms require.
    ("GISCO NUTS 2021", "EuroGeographics-NC"): {
        "credit": "© EuroGeographics for the administrative boundaries",
        "licence": "Eurostat/GISCO conditions of use (non-commercial)",
        "url": "https://ec.europa.eu/eurostat/en/web/gisco/geodata/statistical-units",
        "share_alike": False,
    },
    ("GHS-UCDB R2024A", "CC-BY-4.0"): {
        "credit": (
            "GHS Urban Centre Database 2025 (GHS-UCDB R2024A), European "
            "Commission, Joint Research Centre, "
            "doi:10.2905/1a338be6-7eaf-480c-9664-3a8ade88cbcd"
        ),
        "licence": "CC BY 4.0 (Commission Decision 2011/833/EU)",
        "url": (
            "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/"
            "GHS_UCDB_GLOBE_R2024A/copyright.txt"
        ),
        "share_alike": False,
    },
}

# The sources approved to contribute derived data — an explicit set, never
# derived from the registry, so registering a source to name its terms does
# not approve it; a branch whose inputs are not all approved runs report-only.
DERIVED_SOURCE_ALLOWLIST = frozenset(
    {
        ("Overture Maps divisions", "CDLA-Permissive-2.0"),
        ("Eurostat metropolitan regions", "Eurostat-2011/833/EU"),
        ("GISCO NUTS 2021", "EuroGeographics-NC"),
        ("GHS-UCDB R2024A", "CC-BY-4.0"),
        ("FAO city-regions 2024", "CC-BY-4.0"),
    }
)

# The FAO city-regions as a derived input: a curated FAO metro publishes
# only with this row and Overture's allowlisted.
FAO_DERIVED = ("FAO city-regions 2024", "CC-BY-4.0")

# ~100 m near the equator; the deviation in metres shrinks toward the poles, so
# this never over-simplifies much beyond that. The boundary lookup used for
# point-in-polygon memoizes geometry at this same tolerance — ample for locating
# transit stops, which never sit on a border to the metre.
SIMPLIFY_TOLERANCE_DEG = 0.001


def division_area_dataset(release=overture.OVERTURE_RELEASE):
    """The pinned Overture ``division_area`` theme as a dataset over S3."""
    path = f"{overture.OVERTURE_BUCKET}/{DIVISION_AREA_PATH.format(release=release)}"
    return ds.dataset(path, filesystem=overture.s3_filesystem(), format="parquet")


def read_areas(dataset, division_ids, *, simplify=None, cache=None):
    """``{division_id: [{"geom", "sources"}, ...]}`` land-area rows for the ids.

    One row per land area is kept with its own sources — never flattened across a
    division's areas — so each area is audited on its own provenance; maritime
    (non-land) areas are dropped. Geometry is forced to 2D and malformed WKB is
    skipped (stored as ``None``) rather than aborting the stage.

    ``simplify`` (a tolerance in degrees) simplifies each area as it is read, so
    a full-resolution country or region polygon — hundreds of thousands to
    millions of vertices — is reduced to the shipping resolution before it is
    held or unioned, instead of materialising every seeded division's raw
    geometry at once (the peak that OOMs a feed-dense build). The caller ships
    at this same tolerance anyway, so the result is unchanged within it.

    ``cache`` — a ``(cache_dir, release)`` pair — memoizes the fetched rows under
    ``cache_dir/overture_areas/<release>/`` so each division's raw geometry is
    read from S3 at most once; the release is immutable, so the cache is valid
    until it is bumped. With ``None`` the areas are read straight from S3.
    """
    if not division_ids:
        return {}
    ids = sorted(set(division_ids))
    areas = {}
    for batch in _area_batches(dataset, ids, cache):
        for row in batch.to_pylist():
            if not row.get("is_land"):
                continue
            try:
                geom = shapely.from_wkb(row["geometry"])
            except Exception:
                geom = None
            if geom is not None:
                geom = shapely.force_2d(geom)
                # Only simplify an already-valid boundary: an empty,
                # self-intersecting, non-polygonal or non-finite area is left
                # raw so the caller's own validity check still rejects it, and
                # ``simplify`` never runs on geometry it could turn into an
                # apparently-valid shape or raise on.
                if simplify is not None and _valid_polygon(geom):
                    geom = shapely.simplify(geom, simplify, preserve_topology=True)
            areas.setdefault(row["division_id"], []).append(
                {"geom": geom, "sources": row.get("sources") or []}
            )
    return areas


def _area_predicate(ids):
    return ds.field("division_id").isin(ids)


def _area_batches(dataset, ids, cache):
    """Yield projected ``division_area`` record-batches for ``ids``, one batch in
    memory at a time (never the whole set at once). With ``cache`` a
    ``(cache_dir, release)`` pair the rows are memoized under a release-keyed
    local parquet — ids not already stored are streamed in from S3 first — so
    each division is read from S3 at most once; the release is immutable, so the
    cache stays valid until it is bumped. Builds run one at a time, so the
    unique per-fetch filenames need no lock."""
    if cache is None:
        yield from progress(
            dataset.to_batches(columns=AREA_PROJECT, filter=_area_predicate(ids)),
            "areas",
        )
        return
    cache_dir, release = cache
    rows_dir = _cache_rows_dir(cache_dir, release)
    cached = _cached_area_ids(rows_dir)
    missing = [i for i in ids if i not in cached]
    if missing:
        _fetch_into_cache(dataset, missing, rows_dir)
    if _has_cache(rows_dir):
        yield from ds.dataset(rows_dir, format="parquet").to_batches(
            filter=_area_predicate(ids)
        )


def _cache_rows_dir(cache_dir, release):
    """The release's cache rows directory, refusing a release that is not a safe
    path component and a path a symlink has redirected outside the cache root.

    The cache lives under the build's own ``cache_dir`` (maintainer-owned, not
    attacker input); this rejects a statically planted symlink escape. A
    concurrent symlink race is out of scope — pyarrow reads and writes by path,
    not by directory descriptor, so an ``openat``-relative implementation is not
    available here.
    """
    if not store.safe_component(str(release)):
        raise ValueError(f"unsafe Overture release for the area cache: {release!r}")
    rows_dir = cache_dir / "overture_areas" / str(release) / "rows"
    root = os.path.realpath(cache_dir)
    resolved = os.path.realpath(rows_dir)
    if os.path.commonpath([root, resolved]) != root:
        raise ValueError(f"area cache path escapes the cache root: {rows_dir}")
    return rows_dir


def _fetch_into_cache(dataset, ids, rows_dir):
    """Stream the S3 area rows for ``ids`` into one new parquet file in the cache
    — written to a hidden temp then atomically renamed, one batch at a time, so
    the whole set is never materialised and a crash leaves no partial file the
    reader chokes on (pyarrow skips dotfiles during discovery)."""
    rows_dir.mkdir(parents=True, exist_ok=True)
    tmp = rows_dir / f".{uuid.uuid4().hex}.parquet.tmp"
    writer = None
    try:
        for batch in progress(
            dataset.to_batches(columns=AREA_PROJECT, filter=_area_predicate(ids)),
            "areas",
        ):
            if writer is None:
                writer = pq.ParquetWriter(tmp, batch.schema)
            writer.write_batch(batch)
    finally:
        if writer is not None:
            writer.close()
    if writer is not None:
        os.replace(tmp, rows_dir / f"{uuid.uuid4().hex}.parquet")


def _has_cache(rows_dir):
    return rows_dir.exists() and any(rows_dir.glob("*.parquet"))


def _cached_area_ids(rows_dir):
    """Division ids already fetched into the local area cache — those with any
    stored row, land or maritime. A division absent from the theme entirely (no
    rows at all) is not recorded, so it is re-scanned next time (a cheap empty
    S3 read) rather than complicating the cache with negative entries."""
    if not _has_cache(rows_dir):
        return set()
    column = (
        ds.dataset(rows_dir, format="parquet")
        .to_table(columns=["division_id"])
        .column("division_id")
    )
    return set(column.to_pylist())


def _source_key(source):
    """The ``(dataset, licence)`` of a source; a null source keys to neither."""
    if not source:
        return (None, None)
    return (source.get("dataset"), source.get("license"))


def _is_shippable(sources):
    """True only when every source is an audited, allowlisted ``(dataset, licence)``.

    A source with a missing licence, or a dataset not yet audited, is not on the
    allowlist and makes the area unshippable rather than being ignored — a
    boundary is redistributed only when every source that built it permits it.
    """
    return bool(sources) and all(_source_key(s) in SOURCE_ALLOWLIST for s in sources)


def _valid_polygons(geoms):
    """A boolean mask over a geometry array: which are shippable boundaries.

    True where a geometry is a non-empty, valid, finite (multi)polygon; ``None``
    and other-typed entries are False. Vectorized over the shapely array API so
    a batch of geometries is checked in one pass rather than one at a time;
    ``_valid_polygon`` is the scalar form.
    """
    bounds = shapely.bounds(geoms)
    return (
        np.isin(shapely.get_type_id(geoms), (3, 6))  # Polygon / MultiPolygon
        & ~shapely.is_empty(geoms)
        & np.isfinite(bounds).all(axis=1)
        & shapely.is_valid(geoms)
    )


def _valid_polygon(geom):
    """True for a non-empty, valid, finite (multi)polygon; a shippable boundary.

    External WKB may be empty, self-intersecting, non-polygonal or carry
    non-finite coordinates; none of those may ship, so each is rejected rather
    than committed as a place's geometry.
    """
    return bool(_valid_polygons(np.asarray([geom], dtype=object))[0])


def _repaired_polygon(geom):
    """A valid, non-empty (multi)polygon for ``geom``, or None.

    An already-valid geometry is returned unchanged. A self-intersecting
    (multi)polygon (generalised boundaries occasionally are) is repaired with
    ``make_valid``, keeping only the polygonal part of the result. Returns None
    when ``geom`` is empty, not a (multi)polygon, or cannot be made into a valid
    polygon — so a non-polygonal input is rejected, not silently reduced.
    """
    if _valid_polygon(geom):
        return geom
    if geom is None or geom.is_empty:
        return None
    if geom.geom_type not in ("Polygon", "MultiPolygon"):
        return None
    try:
        parts = [
            part
            for part in shapely.get_parts(shapely.make_valid(geom))
            if part.geom_type in ("Polygon", "MultiPolygon")
        ]
        merged = shapely.unary_union(parts) if parts else None
    except Exception:  # noqa: B902 - shapely raises its own hierarchy
        return None
    return merged if merged is not None and _valid_polygon(merged) else None


def _simplify(geom):
    """The geometry simplified to the shipping tolerance."""
    return shapely.simplify(geom, SIMPLIFY_TOLERANCE_DEG, preserve_topology=True)


def _notice(shipped, release, derived=()):
    """The NOTICE text: the release, each shipped source, share-alike terms, and
    the credits of the derived-data sources that were allowed."""
    lines = [
        "This index includes place boundary geometry from the Overture Maps",
        f"divisions theme (release {release}), provided under "
        f"{OVERTURE_AGGREGATOR['license']}",
        f"({OVERTURE_AGGREGATOR['url']}) and derived from:",
        "",
    ]
    for pair in sorted(shipped):
        meta = SOURCE_ALLOWLIST[pair]
        lines.append(f"  - {meta['credit']} — {meta['licence']} ({meta['url']})")
    if any(SOURCE_ALLOWLIST[pair]["share_alike"] for pair in shipped):
        lines += [
            "",
            "Geometry derived from OpenStreetMap is a Derived Database under the",
            "Open Database License (ODbL 1.0) and is made available under that same",
            "licence; its share-alike terms apply.",
        ]
    credited = [row for row in derived if row.get("allowed")]
    if credited:
        lines += [
            "",
            "Metro memberships were derived at build time from these sources;",
            "the derived use ships no boundary data of its own:",
        ]
        for row in credited:
            lines.append(f"  - {row['credit']} — {row['terms']} ({row['url']})")
    return "\n".join(lines) + "\n"


def _inventory_rows(inventory, shipped_count, derived=()):
    """The licence inventory: the Overture aggregator, each component source,
    then the derived-data rows an earlier stage recorded.

    Every row carries the licence URL and the pinned release as its version, so
    the record answers "what shipped, under what licence, from where, at which
    version" without reading back the source data.
    """
    rows = [
        {
            "role": "aggregator",
            "use": "geometry",
            "dataset": OVERTURE_AGGREGATOR["dataset"],
            "license": OVERTURE_AGGREGATOR["license"],
            "url": OVERTURE_AGGREGATOR["url"],
            "version": overture.OVERTURE_RELEASE,
            "allowed": True,
            "geometries": shipped_count,
        }
    ]
    for (dataset_name, licence, allowed), count in sorted(
        inventory.items(), key=lambda item: tuple(str(part) for part in item[0])
    ):
        meta = SOURCE_ALLOWLIST.get((dataset_name, licence))
        rows.append(
            {
                "role": "component",
                "use": "geometry",
                "dataset": dataset_name,
                "license": licence,
                "url": meta["url"] if meta else None,
                "version": overture.OVERTURE_RELEASE,
                "allowed": allowed,
                "geometries": count,
            }
        )
    return rows + list(derived)


def _curated_geometry(place, wkt):
    """A curator-supplied boundary: WKT parsed, validated and simplified like
    any other, shipped with ``geometry_source = "curated"`` — the curator, not
    a licence audit, vouches for it."""
    try:
        geom = shapely.from_wkt(wkt)
    except Exception as error:  # noqa: B902 - shapely raises its own hierarchy
        raise overrides.OverrideError(
            f"place {place['place_id']!r}: boundary is not valid WKT: {error}"
        ) from None
    geom = shapely.force_2d(geom)
    simplified = _simplify(geom)
    if not _valid_polygon(geom) or not _valid_polygon(simplified):
        raise overrides.OverrideError(
            f"place {place['place_id']!r}: boundary is not a valid polygon"
        )
    minx, miny, maxx, maxy = geom.bounds
    if not (-180 <= minx <= maxx <= 180 and -90 <= miny <= maxy <= 90):
        raise overrides.OverrideError(
            f"place {place['place_id']!r}: boundary is not in WGS84 degrees"
        )
    place["geometry"] = shapely.to_wkb(simplified).hex()
    place["geometry_source"] = "curated"


def attach_geometry(
    cache_dir,
    *,
    dataset=None,
    overrides_dir=None,
    strict=False,
    registry=None,
    run=None,
    area_chunk=None,
):
    """Attach simplified geometry to the seeded places and write the NOTICE.

    Resolves each seeded place's ``division_area`` polygon(s) and ships the
    simplified (unioned) geometry only where every land area's sources are
    allowlisted, recording the audit in the licence inventory and NOTICE; a place
    with a disallowed source, no source, or invalid geometry keeps a null
    geometry; a metro gets the union of its members' shipped polygons, or none.
    One writer lock spans the read, the geometry read and the publish.
    Returns the generation manifest.
    """
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, metros_manifest = store.read_jsonl(
                cache_dir / "gazetteer",
                "metros.json",
                "places_seed.jsonl",
                generations=run,
            )
            wanted = sorted({p["overture_id"] for p in places if p.get("overture_id")})
            if dataset is None:
                dataset = division_area_dataset()

            for place in places:
                place.setdefault("geometry", None)
                place.setdefault("geometry_source", None)
            by_overture = collections.defaultdict(list)
            for place in places:
                if place.get("overture_id"):
                    by_overture[place["overture_id"]].append(place)

            shipped = set()
            inventory = collections.Counter()
            with_geometry = 0
            omitted = 0
            invalid = 0
            # Resolve the seeded places' areas in bounded chunks so the whole
            # set's polygons are never held at once (that peak OOMs a feed-dense
            # country's build on a memory-tight host). The accumulators below are
            # chunk-order independent, so the result matches a single-pass read.
            chunk = area_chunk if area_chunk is not None else AREA_CHUNK
            if not chunk or chunk <= 0:
                chunk = len(wanted) or 1
            for start in progress(range(0, len(wanted) or 1, chunk), "geometry"):
                batch_ids = wanted[start : start + chunk]
                areas = read_areas(
                    dataset,
                    set(batch_ids),
                    simplify=SIMPLIFY_TOLERANCE_DEG,
                    cache=(cache_dir, overture.OVERTURE_RELEASE),
                )
                for overture_id in batch_ids:
                    rows = areas.get(overture_id)
                    if not rows:
                        continue
                    for place in by_overture[overture_id]:
                        for row in rows:
                            # A land area with no sources still records one row,
                            # keyed to the null source, so its omission is
                            # auditable, not silent.
                            for source in row["sources"] or [None]:
                                key = _source_key(source)
                                inventory[(*key, key in SOURCE_ALLOWLIST)] += 1
                        if not all(_is_shippable(row["sources"]) for row in rows):
                            omitted += 1
                            continue
                        geoms = [row["geom"] for row in rows]
                        if not all(_valid_polygon(geom) for geom in geoms):
                            invalid += 1
                            continue
                        merged = (
                            geoms[0] if len(geoms) == 1 else shapely.unary_union(geoms)
                        )
                        simplified = _simplify(merged)
                        if not _valid_polygon(simplified):
                            invalid += 1
                            continue
                        place["geometry"] = shapely.to_wkb(simplified).hex()
                        place["geometry_source"] = "overture"
                        with_geometry += 1
                        for row in rows:
                            for source in row["sources"]:
                                shipped.add(_source_key(source))
                del areas

            by_id = {p["place_id"]: p for p in places}
            member_union = 0
            for place in places:
                if place.get("kind") != "metro" or place.get("geometry"):
                    continue
                # Every member must have shipped its own polygon: a metro is
                # drawn only from what its members already redistribute.
                members = [by_id.get(qid) for qid in place.get("member_ids") or []]
                if not members or any(not (m and m.get("geometry")) for m in members):
                    continue
                merged = shapely.unary_union(
                    [shapely.from_wkb(m["geometry"]) for m in members]
                )
                simplified = _simplify(merged)
                if not _valid_polygon(simplified):
                    invalid += 1
                    continue
                place["geometry"] = shapely.to_wkb(simplified).hex()
                place["geometry_source"] = "member_union"
                member_union += 1

            place_overrides, places_digest = overrides.load_place_overrides(
                overrides_dir, registry=registry
            )
            overrides.expect_digest(
                metros_manifest.get("places_overrides_sha256"),
                places_digest,
                "places.yaml",
                "gazetteer",
            )
            override_report = []
            curated = 0
            for place in places:
                # A curated place's own boundary, judged already by the seed
                # stage that upserted it: attached here, not judged again.
                if place.get("boundary_wkt"):
                    _curated_geometry(place, place.pop("boundary_wkt"))
                    curated += 1
            for entry in overrides.by_operation(place_overrides, "set_boundary"):
                place = by_id.get(entry["place"])
                if place is None:
                    raise overrides.OverrideError(
                        f"place {entry['place']!r}: set_boundary needs a seeded place"
                    )
                # Judged against the geometry the place has now.
                overrides.judge(
                    entry,
                    {
                        "geometry_source": place.get("geometry_source"),
                        "geometry": place.get("geometry"),
                    },
                    override_report,
                    "geometry",
                )
                _curated_geometry(place, entry["set_boundary"])
                curated += 1
            derived = metros_manifest.get("derived_inventory") or []
            inventory_rows = _inventory_rows(inventory, with_geometry, derived)
            notice = _notice(shipped, overture.OVERTURE_RELEASE, derived)
            manifest = {
                "source": "geometry",
                "sources": metros_manifest.get("sources"),
                "seed_generation": metros_manifest.get("seed_generation"),
                "overture_release": overture.OVERTURE_RELEASE,
                "with_geometry": with_geometry,
                "omitted_by_licence": omitted,
                "invalid_geometry": invalid,
                "curated_geometry": curated,
                "member_union_geometry": member_union,
                "places_overrides_sha256": places_digest,
                "stale_overrides": len(override_report),
                "stale_place_overrides": (
                    metros_manifest.get("stale_place_overrides") or 0
                )
                + len(override_report),
                "licence_sources": sorted("|".join(pair) for pair in shipped),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            published = store.publish(
                cache_dir / "gazetteer",
                "geometry.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(places),
                    "licence_inventory.jsonl": store.jsonl_chunks(inventory_rows),
                    "override_report.jsonl": store.jsonl_chunks(override_report),
                    "NOTICE": lambda: [notice],
                },
                manifest,
                held=directory,
                staged=run is not None,
            )
            if run is not None:
                run["geometry.json"] = published["generation"]
            overrides.strict_check(strict, override_report, "geometry")
            return published
    finally:
        directory.close()
