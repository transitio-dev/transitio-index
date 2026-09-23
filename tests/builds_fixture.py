"""Build directories for tests: the flat layout before schema 7 and the
partitioned layout from it, written with digests that match their files."""

import hashlib
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

BOX = shapely.box


def _place(place_id, kind, name, parent_id, geom, country="FI", **extra):
    return {
        "place_id": place_id,
        "kind": kind,
        "name": name,
        "parent_id": parent_id,
        "country_code": country,
        "service": json.dumps({"feeds": 1}),
        "geometry": shapely.to_wkb(geom),
        **extra,
    }


def _places_table(places):
    """The places as Arrow: every key any row has (``from_pylist`` reads the
    first row's), ``names`` typed as the map the publisher writes."""
    keys = [k for k in dict.fromkeys(k for p in places for k in p) if k != "names"]
    table = pa.Table.from_pylist([{k: p.get(k) for k in keys} for p in places])
    if any("names" in p for p in places):
        names = [list((p.get("names") or {}).items()) for p in places]
        table = table.append_column(
            "names", pa.array(names, pa.map_(pa.string(), pa.string()))
        )
    return table


# A country, two regions and three cities; only Helsinki is served, by feed f1.
PLACES = [
    _place("fi", "country", "Finland", None, BOX(19, 59, 32, 71)),
    _place("uus", "region", "Uusimaa", "fi", BOX(23, 59.8, 26.5, 60.9)),
    _place("lap", "region", "Lapland", "fi", BOX(20, 66, 30, 70)),
    _place(
        "hel",
        "city",
        "Helsinki",
        "uus",
        BOX(24.8, 60.1, 25.3, 60.35),
        names={"sv": "Helsingfors"},
        aliases=["Hki"],
    ),
    _place("esp", "city", "Espoo", "uus", BOX(24.4, 60.1, 24.9, 60.3)),
    _place("rov", "city", "Rovaniemi", "lap", BOX(25.5, 66.4, 26.0, 66.6)),
]


EDGES = [{"place_id": "hel", "feed_id": "f1", "tier": "local"}]


FEEDS = [{"feed_id": "f1", "name": "HSL", "coverage": None}]


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _digest_key(name):
    """The snapshot key holding a flat file's digest: ``places_sha256`` for
    ``places.parquet``, ``notice_sha256`` for ``NOTICE``."""
    return name.partition(".")[0].lower() + "_sha256"


def write_build(path, places=PLACES, edges=EDGES, feeds=FEEDS, notice=b"NOTICE\n"):
    """A publish-shaped build directory with digests that match its files."""
    path.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "built_at": "2026-09-11T00:00:00+00:00",
        "counts": {"places": len(places)},
    }
    tables = {
        "places.parquet": _places_table(places),
        "edges.parquet": pa.Table.from_pylist(edges),
        "feeds.parquet": pa.Table.from_pylist(feeds),
    }
    for name, table in tables.items():
        sink = io.BytesIO()
        pq.write_table(table, sink)
        (path / name).write_bytes(sink.getvalue())
        snapshot[_digest_key(name)] = _sha(sink.getvalue())
    (path / "NOTICE").write_bytes(notice)
    snapshot[_digest_key("NOTICE")] = _sha(notice)
    (path / "snapshot.json").write_text(json.dumps(snapshot))
    return snapshot


def _feed7(feed_id, name, home, scope, spec="gtfs", start=None, end=None):
    return {
        "feed_id": feed_id,
        "name": name,
        "spec": spec,
        "coverage": None,
        "home_country": home,
        "scope": scope,
        "declared_countries": ["FI"],
        "service_start": start,
        "service_end": end,
    }


def _edge7(place_id, feed_id, tier, category, relevance, cross, partition=None):
    row = {
        "place_id": place_id,
        "feed_id": feed_id,
        "tier": tier,
        "relevance_category": category,
        "relevance": relevance,
        "cross_border": cross,
    }
    if partition is not None:
        row["feed_partition"] = partition
    return row


PARTITIONED_FEEDS = [
    _feed7("f1", "HSL", "FI", "domestic"),
    _feed7("f2", "Ferry", None, "international"),
    _feed7("f3", "Bikes", "FI", "domestic", spec="gbfs"),
]


def _rt(feed_id, static, urls, method="declared"):
    return {
        "feed_id": feed_id,
        "name": None,
        "source": "atlas",
        "static_feed_id": static,
        "static_link_method": method,
        "entity_types": sorted(k.removeprefix("realtime_") for k in urls),
        "urls": json.dumps(urls),
    }


REALTIME_ROWS = {
    "FI": [_rt("f1-rt", "f1", {"realtime_trip_updates": "https://rt/tu"})],
    "international": [_rt("f-rt-lost", None, {}, method="none")],
}


PARTITIONED_EDGES = {
    "FI": [
        _edge7("hel", "f1", "local", "primary", 0.9, False),
        _edge7("hel", "f3", "local", "primary", 0.2, False),
    ],
    "links": [
        _edge7(
            "hel", "f2", "international", "international", 0.3, True, "international"
        )
    ],
}


def write_partitioned_build(
    path,
    places=PLACES,
    feeds=PARTITIONED_FEEDS,
    edges=PARTITIONED_EDGES,
    notice=b"NOTICE\n",
    realtime=None,
    built_at="2026-09-12T00:00:00+00:00",
):
    """A schema-7 build: places by country, feeds by home country
    (``international`` without one), domestic edges by partition and the
    cross-border edges under ``links``, every table listed with its rows and
    digest; ``notice=None`` publishes it unlicensed."""
    path.mkdir(parents=True, exist_ok=True)
    tables = {}
    for place in places:
        tables.setdefault(f"{place['country_code']}/places.parquet", []).append(place)
    for feed in feeds:
        partition = feed["home_country"] or "international"
        key = f"{partition}/feeds.parquet"
        tables.setdefault(key, []).append(feed)
    for partition, rows in edges.items():
        tables[f"{partition}/edges.parquet"] = pa.Table.from_pylist(rows)
    for partition, rows in (realtime or {}).items():  # schema 8
        tables[f"{partition}/realtime.parquet"] = pa.Table.from_pylist(rows)
    listing = {}
    for name, table in tables.items():
        if isinstance(table, list):
            rows = table
            table = (
                _places_table(rows) if "places" in name else pa.Table.from_pylist(rows)
            )
        partition, _, file = name.partition("/")
        (path / partition).mkdir(exist_ok=True)
        sink = io.BytesIO()
        pq.write_table(table, sink)
        (path / name).write_bytes(sink.getvalue())
        listing.setdefault(partition, {})[file[: -len(".parquet")]] = {
            "rows": len(table),
            "sha256": _sha(sink.getvalue()),
        }
    snapshot = {
        "schema_version": 8 if realtime else 7,
        "built_at": built_at,
        "counts": {"places": len(places)},
        "partitions": listing,
        "licensed": notice is not None,
        "notice_sha256": None if notice is None else _sha(notice),
    }
    if notice is not None:
        (path / "NOTICE").write_bytes(notice)
    (path / "snapshot.json").write_text(json.dumps(snapshot))
    return snapshot


def _rewrite_snapshot(path, change):
    snapshot = json.loads((path / "snapshot.json").read_text())
    change(snapshot)
    (path / "snapshot.json").write_text(json.dumps(snapshot))


def _notice_listed_but_gone(path):
    (path / "NOTICE").unlink()


def _run(builds, label, digit, **kwargs):
    """A partitioned build archived as ``<label>-<16 hex>`` under ``builds``."""
    path = builds / f"{label}-{digit:016x}" / "index"
    write_partitioned_build(path, **kwargs)
    return path.parent.name
