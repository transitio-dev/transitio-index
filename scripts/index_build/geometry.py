"""Attach simplified place geometry and audit its source licences.

Reads the Overture ``division_area`` polygons for the seeded admin places,
simplifies them to a shipping tolerance, and audits each area's sources against
an allowlist of audited ``(dataset, licence)`` pairs: geometry ships (as
hex-encoded WKB) only when every source that built it is on the allowlist, and
its attribution goes into ``NOTICE``; geometry with any unaudited or unlicensed
source is omitted and recorded in the licence inventory. Metros carry no geometry
here — their hulls are a statistical/member-union stage. Only the simplified
geometry is meant to ship; the full-resolution boundary lookup used for
point-in-polygon is built by the coverage stage that consumes it.
"""

import collections
import datetime
import math

import pyarrow.dataset as ds
import shapely

from index_build import overture, store

DIVISION_AREA_PATH = "release/{release}/theme=divisions/type=division_area"
AREA_PROJECT = ["division_id", "geometry", "sources", "is_land"]

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

# ~100 m near the equator; the deviation in metres shrinks toward the poles, so
# this never over-simplifies much beyond that. Point-in-polygon runs against the
# full-resolution geometry in the coverage stage, so shipped geometry can be this
# coarse.
SIMPLIFY_TOLERANCE_DEG = 0.001


def division_area_dataset(release=overture.OVERTURE_RELEASE):
    """The pinned Overture ``division_area`` theme as a dataset over S3."""
    from pyarrow.fs import S3FileSystem

    filesystem = S3FileSystem(anonymous=True, region=overture.OVERTURE_REGION)
    path = f"{overture.OVERTURE_BUCKET}/{DIVISION_AREA_PATH.format(release=release)}"
    return ds.dataset(path, filesystem=filesystem, format="parquet")


def read_areas(dataset, division_ids):
    """``{division_id: [{"geom", "sources"}, ...]}`` land-area rows for the ids.

    One row per land area is kept with its own sources — never flattened across a
    division's areas — so each area is audited on its own provenance; maritime
    (non-land) areas are dropped. Geometry is forced to 2D and malformed WKB is
    skipped (stored as ``None``) rather than aborting the stage.
    """
    if not division_ids:
        return {}
    predicate = ds.field("division_id").isin(sorted(division_ids))
    areas = {}
    for batch in dataset.to_batches(columns=AREA_PROJECT, filter=predicate):
        for row in batch.to_pylist():
            if not row.get("is_land"):
                continue
            try:
                geom = shapely.from_wkb(row["geometry"])
            except Exception:
                geom = None
            if geom is not None:
                geom = shapely.force_2d(geom)
            areas.setdefault(row["division_id"], []).append(
                {"geom": geom, "sources": row.get("sources") or []}
            )
    return areas


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


def _valid_polygon(geom):
    """True for a non-empty, valid, finite (multi)polygon; a shippable boundary.

    External WKB may be empty, self-intersecting, non-polygonal or carry
    non-finite coordinates; none of those may ship, so each is rejected rather
    than committed as a place's geometry.
    """
    if geom is None or geom.is_empty:
        return False
    if geom.geom_type not in ("Polygon", "MultiPolygon"):
        return False
    if not all(math.isfinite(bound) for bound in geom.bounds):
        return False
    return geom.is_valid


def _simplify(geom):
    """The geometry simplified to the shipping tolerance."""
    return shapely.simplify(geom, SIMPLIFY_TOLERANCE_DEG, preserve_topology=True)


def _notice(shipped, release):
    """The NOTICE text: the release, each shipped source, and share-alike terms."""
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
    return "\n".join(lines) + "\n"


def _inventory_rows(inventory, shipped_count):
    """The licence inventory: the Overture aggregator, then each component source.

    Every row carries the licence URL and the pinned release as its version, so
    the record answers "what shipped, under what licence, from where, at which
    version" without reading back the source data.
    """
    rows = [
        {
            "role": "aggregator",
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
                "dataset": dataset_name,
                "license": licence,
                "url": meta["url"] if meta else None,
                "version": overture.OVERTURE_RELEASE,
                "allowed": allowed,
                "geometries": count,
            }
        )
    return rows


def attach_geometry(cache_dir, *, dataset=None):
    """Attach simplified geometry to the seeded places and write the NOTICE.

    Resolves each seeded place's ``division_area`` polygon(s) and ships the
    simplified (unioned) geometry only where every land area's sources are
    allowlisted, recording the audit in the licence inventory and NOTICE; a place
    with a disallowed source, no source, or invalid geometry keeps a null
    geometry. One writer lock spans the read, the geometry read and the publish.
    Returns the generation manifest.
    """
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, _ = store.read_jsonl(
                cache_dir / "gazetteer", "metros.json", "places_seed.jsonl"
            )
            wanted = {p["overture_id"] for p in places if p.get("overture_id")}
            if dataset is None:
                dataset = division_area_dataset()
            areas = read_areas(dataset, wanted)

            shipped = set()
            inventory = collections.Counter()
            with_geometry = 0
            omitted = 0
            invalid = 0
            for place in places:
                place.setdefault("geometry", None)
                place.setdefault("geometry_source", None)
                rows = areas.get(place.get("overture_id"))
                if not rows:
                    continue
                for row in rows:
                    # A land area with no sources still records one row, keyed to
                    # the null source, so its omission is auditable, not silent.
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
                merged = geoms[0] if len(geoms) == 1 else shapely.unary_union(geoms)
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

            inventory_rows = _inventory_rows(inventory, with_geometry)
            notice = _notice(shipped, overture.OVERTURE_RELEASE)
            manifest = {
                "source": "geometry",
                "overture_release": overture.OVERTURE_RELEASE,
                "with_geometry": with_geometry,
                "omitted_by_licence": omitted,
                "invalid_geometry": invalid,
                "sources": sorted("|".join(pair) for pair in shipped),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "gazetteer",
                "geometry.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(places),
                    "licence_inventory.jsonl": store.jsonl_chunks(inventory_rows),
                    "NOTICE": lambda: [notice],
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
