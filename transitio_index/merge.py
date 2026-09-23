"""The rules that merge the builds of every label into one index.

``select_sources`` picks the newest complete run of every label under a
cache and ``merge_tables`` joins the selected builds' tables into one set
with one row per id. The viewer's catalogue page and the merged snapshot
both use them, so the page and the release cannot disagree on what the
merged index holds.
"""

import json

import numpy as np
import pandas as pd
import pyarrow as pa

from .builds import (
    _EPOCH,
    _SNAPSHOT_ERRORS,
    _built_at,
    _files_present,
    _read_file,
    _snapshot_digest,
    discover,
    label_of,
    snapshot_files,
)


def select_sources(cache, read_bytes=_read_file):
    """``(sources, skipped)``: the newest build of every label under ``cache``.

    ``sources`` lists ``(build_id, path, snapshot)`` by label; ``skipped``
    the labels whose newest run cannot be a source, with the reason: not
    complete, undated, not partitioned (before schema 7), or without a feeds
    table. An older run of a skipped label is never consulted: it would show
    stale data as current. Newest is by build date, ties to the lower id; a
    run whose snapshot or date cannot be read — one still being written —
    ranks first and is skipped.
    """
    runs = {}
    for build_id, path in discover(cache, listed=False).items():
        try:
            snapshot = json.loads(read_bytes(path / "snapshot.json"))
        except _SNAPSHOT_ERRORS:
            snapshot = None
        runs.setdefault(label_of(build_id), []).append(
            (_built_at(snapshot), build_id, path, snapshot)
        )
    sources, skipped = [], []
    for label in sorted(runs):
        ranked = sorted(runs[label], key=lambda run: run[1])
        ranked.sort(key=lambda run: (run[0] is None, run[0] or _EPOCH), reverse=True)
        stamp, build_id, path, snapshot = ranked[0]
        files = snapshot_files(snapshot)
        if files is None or not _files_present(path, files):
            reason = "incomplete"
        elif stamp is None:
            reason = "undated"
        elif "partitions" not in snapshot or not (
            isinstance(snapshot.get("schema_version"), int)
            and snapshot["schema_version"] >= 7
        ):
            reason = "not partitioned"
        elif not any(name.endswith("/feeds.parquet") for name in files):
            reason = "no feeds"
        else:
            sources.append((build_id, path, snapshot))
            continue
        skipped.append({"id": build_id, "reason": reason})
    return sources, skipped


def _keys(table, columns, order):
    """The key columns of a stacked table as a frame, with each row's
    position (``row``) and the rank of its source (``order``)."""
    frame = table.select([*columns, "build_id"]).to_pandas()
    frame["order"] = frame["build_id"].map(order)
    frame["row"] = np.arange(len(frame))
    return frame


def _value_counts(column):
    return {k: int(v) for k, v in column.to_pandas().value_counts().items()}


def merge_tables(sources, skipped=()):
    """The catalogue's ``(snapshot, tables)`` over loaded ``sources``, each a
    ``(build_id, snapshot, tables)`` as ``load_tables`` returns them.

    One row per id: a feed from the newest source carrying it (ties to the
    lower id); a feed's edges from the source that won the feed, so one
    build's classification is never mixed with another's; a place from the
    source contributing the most kept edges to it, from the newest when none
    does. A realtime companion follows its static feed, an unlinked one the
    newest source. Every table gains ``build_id``. The snapshot recounts the
    merged tables and lists the ``sources`` and the ``skipped`` runs.
    """
    ranked = sorted(sources, key=lambda source: source[0])
    ranked.sort(key=lambda source: _built_at(source[1]) or _EPOCH, reverse=True)
    order = {build_id: rank for rank, (build_id, _, _) in enumerate(ranked)}
    stacked = {}
    for build_id, _, tables in ranked:
        for name, table in tables.items():
            column = pa.repeat(build_id, len(table))
            stacked.setdefault(name, []).append(table.append_column("build_id", column))
    merged = {
        name: pa.concat_tables(parts, promote_options="default")
        for name, parts in stacked.items()
    }
    feeds = _keys(merged["feeds.parquet"], ["feed_id"], order)
    winners = feeds.sort_values("order", kind="stable").drop_duplicates("feed_id")
    won = winners[["feed_id", "build_id"]]
    edges = _keys(merged["edges.parquet"], ["place_id", "feed_id"], order)
    kept = edges.merge(won, on=["feed_id", "build_id"])
    places = _keys(merged["places.parquet"], ["place_id"], order)
    contributed = kept.groupby(["place_id", "build_id"]).size().rename("kept")
    places = places.merge(contributed.reset_index(), how="left")
    places["kept"] = places["kept"].fillna(0)
    chosen = places.sort_values(
        ["kept", "order"], ascending=[False, True], kind="stable"
    ).drop_duplicates("place_id")
    taken = {
        "feeds.parquet": winners["row"],
        "edges.parquet": kept["row"],
        "places.parquet": chosen["row"],
    }
    if "realtime.parquet" in merged:
        realtime = _keys(
            merged["realtime.parquet"], ["feed_id", "static_feed_id"], order
        )
        # A companion of a won feed comes with that feed's source or not at
        # all; one linked to no won feed comes from the newest source.
        static = won.rename(columns={"feed_id": "static_feed_id"})
        follows = realtime.merge(static, on=["static_feed_id", "build_id"])
        loose = realtime[~realtime["static_feed_id"].isin(static["static_feed_id"])]
        loose = loose.sort_values("order", kind="stable")
        rows = pd.concat([follows["row"], loose["row"]])
        taken["realtime.parquet"] = rows[
            ~pd.concat([follows["feed_id"], loose["feed_id"]]).duplicated().to_numpy()
        ]
    tables = {
        name: merged[name].take(pa.array(np.sort(rows.to_numpy())))
        for name, rows in taken.items()
    }
    counts = {
        "places": len(tables["places.parquet"]),
        "places_by_kind": _value_counts(tables["places.parquet"]["kind"]),
        "feeds": len(tables["feeds.parquet"]),
        "edges": len(tables["edges.parquet"]),
        "edges_by_tier": _value_counts(tables["edges.parquet"]["tier"]),
    }
    if "realtime.parquet" in tables:
        counts["realtime"] = len(tables["realtime.parquet"])
    versions = [
        snapshot["schema_version"]
        for _, snapshot, _ in ranked
        if isinstance(snapshot.get("schema_version"), int)
    ]
    snapshot = {
        "catalogue": True,
        "built_at": ranked[0][1].get("built_at"),
        "schema_version": min(versions) if versions else None,
        "licensed": all(snapshot.get("licensed") for _, snapshot, _ in ranked),
        "counts": counts,
        "sources": [
            {
                "id": build_id,
                "label": label_of(build_id),
                "built_at": snapshot.get("built_at"),
                "snapshot_id": _snapshot_digest(snapshot),
                "schema_version": snapshot.get("schema_version"),
                "partitions": sorted(snapshot.get("partitions") or ()),
            }
            for build_id, snapshot, _ in sorted(ranked, key=lambda s: label_of(s[0]))
        ],
        "skipped": list(skipped),
    }
    return snapshot, tables
