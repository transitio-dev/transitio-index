#!/usr/bin/env python3

"""Export a produced index as an editor-ready geospatial layer.

A maintainer tool, not part of the package: it reads a produced index directory
(``places.parquet``, ``edges.parquet`` and ``feeds.parquet`` — what the
``publish`` stage writes) and re-emits the places as GeoParquet and GeoJSON for
transitio-editor. Each place keeps its boundary geometry, name, identity and
hierarchy from the index and gains the feeds serving it (each feed's id, name
and tier(s), joined from the membership edges) plus a ``served`` flag and a feed
count. Every place is kept — the served places and their region/country
ancestors and metros — so the whole hierarchy is on the map and the editor can
filter on ``served``.

Run the build through ``publish`` first (the sample build writes ``cache/index``,
the default here); outputs go into a fresh per-run subdirectory of ``--out-dir``
(default ``cache/index-layer``, gitignored). The heavy dependencies (pyarrow,
shapely, geopandas) are imported lazily so the join and row-shaping stay
unit-testable without them.
"""

import argparse
import json
import tempfile
from pathlib import Path

DEFAULT_INDEX = Path("cache/index")


def _feed_names(feeds):
    """``{feed_id: display name}``, falling back to the id when unnamed."""
    return {feed["feed_id"]: (feed.get("name") or feed["feed_id"]) for feed in feeds}


def _feeds_by_place(edges, feed_names):
    """``{place_id: [{feed_id, name, tiers, categories}]}`` from the
    membership edges.

    A (place, feed) pair may have several tier edges, so its tiers and their
    relevance categories are gathered into one entry; feeds are sorted by id
    for a stable layer.
    """
    tiers, categories = {}, {}
    for edge in edges:
        key = (edge["place_id"], edge["feed_id"])
        tiers.setdefault(key, set()).add(edge.get("tier"))
        categories.setdefault(key, set()).add(edge.get("relevance_category"))
    by_place = {}
    for (place_id, feed_id), feed_tiers in tiers.items():
        by_place.setdefault(place_id, []).append(
            {
                "feed_id": feed_id,
                "name": feed_names.get(feed_id, feed_id),
                "tiers": sorted(tier for tier in feed_tiers if tier is not None),
                "categories": sorted(
                    c for c in categories[(place_id, feed_id)] if c is not None
                ),
            }
        )
    for feeds in by_place.values():
        feeds.sort(key=lambda feed: feed["feed_id"])
    return by_place


def _enrich(places, feeds_by_place):
    """Editor-ready attribute rows: index fields plus the feeds serving each place.

    Pure and free of the geospatial dependencies — geometry is re-attached when
    the layer is written — so it is unit-tested directly. List and map fields are
    rendered as JSON strings so both GeoParquet and GeoJSON carry them cleanly.
    """
    rows = []
    for place in places:
        feeds = feeds_by_place.get(place["place_id"], [])
        rows.append(
            {
                "place_id": place["place_id"],
                "name": place.get("name"),
                "names": json.dumps(
                    dict(place.get("names") or {}), ensure_ascii=False, sort_keys=True
                ),
                "kind": place.get("kind"),
                "country_code": place.get("country_code"),
                "wikidata_id": place.get("wikidata_id"),
                "overture_id": place.get("overture_id"),
                "parent_id": place.get("parent_id"),
                "metro_ids": json.dumps(list(place.get("metro_ids") or [])),
                "member_ids": json.dumps(list(place.get("member_ids") or [])),
                "served": bool(feeds),
                "feed_count": len(feeds),
                "feeds": json.dumps(feeds, ensure_ascii=False),
                "service": place.get("service"),
                "geometry_source": place.get("geometry_source"),
            }
        )
    return rows


def _read_index(index_dir):
    """The place, edge and feed records of a produced index directory."""
    import pyarrow.parquet as pq

    def rows(name):
        return pq.read_table(index_dir / name).to_pylist()

    return rows("places.parquet"), rows("edges.parquet"), rows("feeds.parquet")


def _write(rows, geom_by_place, out_dir):
    """Write the enriched rows + boundary geometry as GeoParquet and GeoJSON."""
    import geopandas as gpd
    import shapely

    geometries = [
        (
            shapely.from_wkb(geom_by_place[row["place_id"]])
            if geom_by_place.get(row["place_id"])
            else None
        )
        for row in rows
    ]
    gdf = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")
    parquet = out_dir / "index_layer.parquet"
    geojson = out_dir / "index_layer.geojson"
    gdf.to_parquet(parquet)
    gdf.to_file(geojson, driver="GeoJSON")
    return parquet, geojson


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python scripts/export_index_layer.py",
        description="Export a produced index as an editor-ready geospatial layer",
    )
    parser.add_argument(
        "--index",
        type=Path,
        default=DEFAULT_INDEX,
        help="a produced index directory (default: cache/index)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("cache/index-layer"),
        help="parent for the per-run output directory (default: cache/index-layer)",
    )
    args = parser.parse_args(argv)

    places, edges, feeds = _read_index(args.index)
    if not places:
        raise SystemExit(f"no places in {args.index}")
    rows = _enrich(places, _feeds_by_place(edges, _feed_names(feeds)))
    geom_by_place = {place["place_id"]: place.get("geometry") for place in places}

    args.out_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=args.out_dir))
    parquet, geojson = _write(rows, geom_by_place, run_dir)

    served = sum(1 for row in rows if row["served"])
    print(f"places: {len(rows)} ({served} served)")
    print(f"wrote: {parquet}")
    print(f"wrote: {geojson}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
