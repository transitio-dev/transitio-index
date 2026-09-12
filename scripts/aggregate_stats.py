#!/usr/bin/env python3

"""Aggregate the ``stats`` artifacts of several archived builds into one report.

A maintainer recipe for the country samples: each archive under
``cache/builds/<label>-<snapshot>/stats/`` holds the row-level tables the
stats stage wrote (``catalogue.parquet``, ``feeds.parquet``,
``places.parquet``) and its ``summary.json``. The script concatenates the
rows, deduplicates them on their stable keys — ``(source, source_id)`` for
catalogue rows, ``feed_id`` for feeds, ``place_id`` for places — recomputes
the summary from the deduplicated rows (archived summaries are never summed)
and renders one ``report.md``. The per-build measures a summary carries
(duplicate coverage, distributions) are repeated per archive.

Archives must agree on the stats schema, the Overture release and the
catalogue dates; a catalogue row seen twice must be identical apart from its
snapshot id (feed ids are minted from the catalogue ids, so they agree); a
feed or place seen twice is refused, since its edge counts depend on the
sample's other feeds and merging either row would undercount the other
archive's edges. With the full index there is one build and nothing to
aggregate.
"""

import argparse
import collections
import json
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq

from transitio_index import stats

TABLES = ("catalogue", "feeds", "places")
KEYS = {
    "catalogue": lambda row: (row["source"], row["source_id"]),
    "feeds": lambda row: row["feed_id"],
    "places": lambda row: row["place_id"],
}
# The summary fields every archive must share.
COMPATIBILITY = (
    "stats_schema_version",
    "schema_version",
    "overture_release",
    "catalogue_dates",
)
# The per-build sections repeated per archive rather than recomputed (the
# realtime companions have no archived row table of their own).
PER_BUILD = ("duplicate_coverage", "distributions", "realtime")


def read_archive(path):
    """``(summary, {table: rows})`` of one archive's ``stats`` directory;
    every row must carry the summary's snapshot id, else the directory mixes
    generations, and the per-build sections must be there to repeat."""
    summary = json.loads((path / "summary.json").read_text(encoding="utf-8"))
    snapshot = summary.get("build", {}).get("snapshot_id")
    if not snapshot:
        raise SystemExit(f"{path}: summary.json names no snapshot")
    for field in COMPATIBILITY:
        if summary["build"].get(field) is None:
            raise SystemExit(f"{path}: summary.json lacks {field}")
    for section in PER_BUILD:
        if not isinstance(summary.get(section), dict):
            raise SystemExit(f"{path}: summary.json lacks the {section} section")
    tables = {
        table: pq.read_table(path / f"{table}.parquet").to_pylist() for table in TABLES
    }
    for table, rows in tables.items():
        found = {row.get("snapshot_id") for row in rows}
        if rows and found != {snapshot}:
            raise SystemExit(
                f"{path / table}.parquet: rows of snapshot {sorted(map(str, found))} "
                f"under a summary of {snapshot!r}"
            )
    return summary, tables


def check_compatible(summaries):
    """Refuse archives that differ in the fields the rows must share."""
    first_label, first_summary = summaries[0]
    first = first_summary["build"]
    for label, summary in summaries[1:]:
        for field in COMPATIBILITY:
            if summary["build"].get(field) != first.get(field):
                raise SystemExit(
                    f"{label}: {field} {summary['build'].get(field)!r} differs from "
                    f"{first_label}'s {first.get(field)!r}"
                )


def place_section(rows):
    """The place-level section from the deduplicated place rows: places per
    kind, the share with a primary feed, and the feeds and departures they
    carry."""
    by_kind = collections.Counter(row["kind"] for row in rows)
    departures = [
        row["departures_per_day"]
        for row in rows
        if row.get("departures_per_day") is not None
    ]
    return {
        "places": len(rows),
        "by_kind": dict(by_kind),
        "with_primary": sum(1 for row in rows if row.get("has_primary")),
        "with_primary_share": (
            sum(1 for row in rows if row.get("has_primary")) / len(rows)
            if rows
            else None
        ),
        "feeds_per_place": stats._quantiles([row.get("feeds") for row in rows]),
        "departures_per_day": sum(departures) if departures else None,
    }


def merge(archives):
    """The deduplicated rows per table over ``[(label, tables)]``."""
    merged = {table: {} for table in TABLES}
    seen = {table: {} for table in TABLES}
    overlaps = []
    for label, tables in archives:
        for table in TABLES:
            key_of = KEYS[table]
            for row in tables[table]:
                key = key_of(row)
                previous = merged[table].get(key)
                if previous is None:
                    merged[table][key] = row
                    seen[table][key] = label
                    continue
                if table == "catalogue":
                    if _without_snapshot(previous) != _without_snapshot(row):
                        raise SystemExit(
                            f"catalogue row {key} differs between "
                            f"{seen[table][key]} and {label}"
                        )
                    continue
                overlaps.append(f"{table} {key!r} ({seen[table][key]}, {label})")
    if overlaps:
        raise SystemExit(
            "the archives must be disjoint in feeds and places; overlapping: "
            + ", ".join(sorted(overlaps))
        )
    return {table: list(rows.values()) for table, rows in merged.items()}


def _without_snapshot(row):
    return {k: v for k, v in row.items() if k != "snapshot_id"}


def aggregate(archives):
    """``(summary, report)`` over ``[(label, summary, tables)]``: the summary
    recomputed from the deduplicated rows, the per-build sections per archive."""
    check_compatible([(label, summary) for label, summary, _ in archives])
    rows = merge([(label, tables) for label, _, tables in archives])
    first = archives[0][1]["build"]
    summary = {
        "build": {
            "snapshot_id": "aggregate",
            "stats_schema_version": first.get("stats_schema_version"),
            "schema_version": first.get("schema_version"),
            "overture_release": first.get("overture_release"),
            "catalogue_dates": first.get("catalogue_dates"),
            "sample": "aggregate",
            "archives": {
                label: summary["build"].get("snapshot_id")
                for label, summary, _ in archives
            },
            "catalogue_rows": {
                source: sum(1 for r in rows["catalogue"] if r["source"] == source)
                for source in ("mdb", "atlas", "gbfs")
            },
        },
        "declared_places": stats.declared_places(rows["catalogue"]),
        "identity": stats.identity(rows["catalogue"], rows["feeds"]),
        **stats.feed_sections(rows["feeds"]),
        "places": place_section(rows["places"]),
    }
    for section in PER_BUILD:
        summary[section] = {
            label: archive_summary.get(section) or {}
            for label, archive_summary, _ in archives
        }
    return summary, stats.render_report(summary)


def _write(path, text):
    """Write ``text`` at ``path`` through a fresh file replaced into place, so
    a symlink planted there is replaced, never followed."""
    tmp = path.with_name(f".{path.name}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
    except BaseException:
        os.unlink(tmp)
        raise
    os.replace(tmp, path)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python scripts/aggregate_stats.py",
        description="Aggregate the stats artifacts of several archived builds",
    )
    parser.add_argument(
        "archives",
        nargs="+",
        type=Path,
        help="archive directories (each holding a stats/ directory, or a stats "
        "directory itself)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("cache/stats-aggregate"),
        help="where summary.json and report.md are written "
        "(default: cache/stats-aggregate)",
    )
    args = parser.parse_args(argv)
    archives, inputs = [], set()
    for path in args.archives:
        directory = path / "stats" if (path / "stats").is_dir() else path
        if not (directory / "summary.json").is_file():
            raise SystemExit(f"{path}: no stats artifacts")
        inputs.add(directory.resolve())
        # The archive's label: its directory, or the parent of a stats/ path.
        label = (path.parent if path.name == "stats" else path).resolve().name
        if label in {existing for existing, _, _ in archives}:
            raise SystemExit(f"{path}: archive label {label!r} is not unique")
        summary, tables = read_archive(directory)
        archives.append((label, summary, tables))
    summary, report = aggregate(archives)
    out_dir = args.out_dir.resolve()
    if out_dir in inputs:
        raise SystemExit(f"{args.out_dir}: the output directory is an input archive")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    _write(args.out_dir / "summary.json", json.dumps(summary, indent=2, sort_keys=True))
    _write(args.out_dir / "report.md", report)
    print(f"archives: {len(archives)}", file=sys.stderr)
    print(f"wrote: {args.out_dir / 'summary.json'}")
    print(f"wrote: {args.out_dir / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
