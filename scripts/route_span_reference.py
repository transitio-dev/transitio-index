#!/usr/bin/env python3

"""Write ``transitio_index/route_span_reference.csv``, the 90th percentile of
route spans per home country and basic mode, which the tier rules compare a
route's span with.

A maintainer tool, not part of the package: the build reads the CSV. It reads
the feeds of each parked crawl given as ``--crawl`` (``cache/crawl.<label>.bak``,
which no crawl writes again) with the classify stage's readers, once per set
of route member digests from the first copy that reads, and files each route
of a basic mode whose span is measured under its feed's home country in the
merged index at ``--index``. Routes of one name over the same stops
(positions rounded to ``ROUND`` decimals) count once per country and mode,
and once per mode in the ``*`` rows, which hold every country and the feeds
without a home country. A country has a row for a mode with at least
``classify.MIN_REFERENCE_ROUTES`` routes. Header comments name the crawl
directories, the index snapshot and the date.
"""

import argparse
import collections
import csv
import datetime
import hashlib
import pathlib

import numpy as np

from transitio_index import builds, classify, crawl

ROUND = 5
ROUTE_MEMBERS = ("stops.txt", "routes.txt", "trips.txt", "stop_times.txt")


def _crawls(roots):
    """``(feed_dir, state, key)`` of each committed crawl in the ``roots``
    with complete stop times, ``key`` its route member digests."""
    for root in roots:
        if not crawl.PARKED_CRAWL.fullmatch(root.name):
            raise SystemExit(f"{root}: not a parked crawl")
        for feed_dir, state in crawl.crawled_feeds(root.parent, name=root.name):
            if (state.get("stop_times") or {}).get("state") != "complete":
                continue
            digests = state.get("member_sha256") or {}
            key = tuple(digests.get(name) for name in ROUTE_MEMBERS)
            if None not in key:
                yield feed_dir, state, key


def measured_feeds(roots):
    """``(state, routes)`` of :func:`feed_routes` per set of route member
    digests, from its first copy whose members verify and parse."""
    seen = set()
    for feed_dir, state, key in _crawls(roots):
        if key in seen:
            continue
        routes = feed_routes(feed_dir, state)
        if routes is not None:
            seen.add(key)
            yield state, routes


def feed_routes(feed_dir, state):
    """``[(mode, identity, span_km)]`` for each route of a crawled feed whose
    span is measured; ``identity`` is the line name and a digest of the
    route's rounded stop positions. None when the members do not verify or
    parse."""
    try:
        parsed = classify._members(feed_dir, state, ("trips.txt",))
    except classify.ClassifyError:
        return None
    if parsed is None:
        return None
    coords = parsed["coords"]
    routes = []
    for route_id, info in parsed["routes"].items():
        stop_ids = parsed["stops"].get(route_id, set())
        span, _, _ = classify._route_geography(
            stop_ids, parsed["sequences"].get(route_id, []), coords
        )
        if span is None:
            continue
        # Whole units of 10**-ROUND degrees: no signed zero.
        scale = 10**ROUND
        positions = sorted(
            {
                (round(coords[s][0] * scale), round(coords[s][1] * scale))
                for s in stop_ids
            }
        )
        digest = hashlib.sha256(repr(positions).encode("ascii")).digest()
        mode = classify.basic_mode(info["route_type"])
        routes.append((mode, (info["name"], digest), span))
    return routes


def reference_rows(routes, minimum=classify.MIN_REFERENCE_ROUTES):
    """The table's rows from ``(country, mode, identity, span_km)`` routes,
    ``country`` None without a home country: each identity counts once per
    bucket, with the span it was first seen with; a route of a mode outside
    ``classify.BASIC_MODES`` is left out."""
    buckets = collections.defaultdict(dict)
    for country, mode, identity, span in routes:
        if mode not in classify.BASIC_MODES:
            continue
        if country:
            buckets[country, mode].setdefault(identity, span)
        buckets["*", mode].setdefault(identity, span)
    rows = []
    for (country, mode), spans in sorted(buckets.items()):
        if country != "*" and len(spans) < minimum:
            continue
        p90 = float(np.quantile(list(spans.values()), 0.9))
        rows.append(
            {
                "country": country,
                "mode": mode,
                "routes": len(spans),
                "p90_km": round(p90, 1),
            }
        )
    return rows


def missing_modes(rows):
    """The basic modes the ``*`` rows leave out, sorted."""
    covered = {row["mode"] for row in rows if row["country"] == "*"}
    return sorted(set(classify.BASIC_MODES) - covered)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--index", type=pathlib.Path, required=True)
    parser.add_argument("--crawl", type=pathlib.Path, nargs="+", required=True)
    parser.add_argument("--out", type=pathlib.Path, default=classify.REFERENCE_TABLE)
    args = parser.parse_args()
    loaded = builds.load_tables(args.index)
    if loaded is None:
        raise SystemExit(f"{args.index}: not a verified index")
    snapshot, _, tables = loaded
    feeds = tables["feeds.parquet"]
    homes = dict(
        zip(
            feeds.column("feed_id").to_pylist(),
            feeds.column("home_country").to_pylist(),
        )
    )
    counted = collections.Counter()

    def routes():
        for state, measured in measured_feeds(args.crawl):
            counted["feeds"] += 1
            for mode, identity, span in measured:
                counted["routes"] += 1
                yield homes.get(state.get("feed_id")), mode, identity, span

    rows = reference_rows(routes())
    if missing_modes(rows):
        raise SystemExit(f"no route of {missing_modes(rows)} in the crawls")
    with open(args.out, "w", encoding="utf-8", newline="") as handle:
        handle.write(f"# crawls: {' '.join(p.name for p in args.crawl)}\n")
        handle.write(f"# index: {snapshot['snapshot_id']}\n")
        handle.write(f"# written: {datetime.date.today().isoformat()}\n")
        writer = csv.DictWriter(handle, classify.REFERENCE_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(
        f"{len(rows)} rows from {counted['routes']} routes of {counted['feeds']} feeds"
    )


if __name__ == "__main__":
    main()
