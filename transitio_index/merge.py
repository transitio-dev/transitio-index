"""The rules that merge the builds of every label into one index.

``select_sources`` picks the newest complete run of every label archived
under a builds directory and ``merge_tables`` joins the selected builds'
tables into one set with one row per id. The viewer's catalogue page and
the merged snapshot both use them, so the page and the release cannot
disagree on what the merged index holds. ``load_sources`` verifies and
loads the selection for a merge, refusing what a merged snapshot could
not ship, and ``_route`` and ``_partition_files`` turn the merged tables
into the partition tables of a schema-9 snapshot.
"""

import hashlib
import io
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import classify, publish
from .builds import (
    _EPOCH,
    _SNAPSHOT_ERRORS,
    _built_at,
    _files_present,
    _plain_directory,
    _read_file,
    _snapshot_digest,
    archived,
    label_of,
    load_tables,
    snapshot_files,
)

# Manifest fields every source must agree on: a disagreement means builds
# of different code or data would be mixed into one index.
AGREED_FIELDS = ("overture_release", "simplify_tolerance_deg", "classifier")
# The override digests; a merged index carries none, so every source's
# must be null.
OVERRIDE_FIELDS = (
    "overrides_sha256",
    "feeds_overrides_sha256",
    "places_overrides_sha256",
)


class MergeError(RuntimeError):
    """The selected builds cannot be merged into one snapshot."""


def _canonical(value):
    """A JSON value as its canonical text, so that comparisons are as strict
    as JSON: ``true`` is not ``1`` and ``9`` is not ``9.0``."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def select_sources(builds, read_bytes=_read_file):
    """``(sources, skipped)``: the newest run of every label archived under
    ``builds``.

    ``sources`` lists ``(build_id, path, snapshot)`` by label; ``skipped``
    the labels whose newest run cannot be a source, with the reason: not
    complete, undated, not partitioned (before schema 7), or without a feeds
    table. An older run of a skipped label is never consulted: it would show
    stale data as current. Newest is by build date, ties to the lower id; a
    run whose snapshot or date cannot be read — one still being written, or
    one whose index is not a plain directory and is not read through —
    ranks first and is skipped.
    """
    runs = {}
    for build_id, path in archived(builds, listed=False).items():
        snapshot = None
        if _plain_directory(path):
            try:
                snapshot = json.loads(read_bytes(path / "snapshot.json"))
            except _SNAPSHOT_ERRORS:
                pass
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


def _check_sources(snapshots):
    """Refuse a selection the merge cannot ship: a source below schema 9, an
    unlicensed one, one carrying an override digest, one without a field of
    ``AGREED_FIELDS``, or sources disagreeing on one. Returns the agreed
    values."""
    agreed = {}
    for build_id, snapshot in snapshots:
        version = snapshot.get("schema_version")
        if version != publish.SCHEMA_VERSION:
            raise MergeError(
                f"{build_id}: schema_version {version!r}; the merge takes schema "
                f"{publish.SCHEMA_VERSION} builds only"
            )
        if snapshot.get("licensed") is not True or not isinstance(
            snapshot.get("notice_sha256"), str
        ):
            raise MergeError(f"{build_id}: not a licensed build")
        for field in OVERRIDE_FIELDS:
            if snapshot.get(field) is not None:
                raise MergeError(
                    f"{build_id}: {field} is set; a merged index carries no overrides"
                )
        for field in AGREED_FIELDS:
            value = snapshot.get(field)
            if not _well_formed(field, value):
                raise MergeError(f"{build_id}: no usable {field}: {value!r}")
            if field in agreed and _canonical(agreed[field][1]) != _canonical(value):
                first, other = agreed[field]
                raise MergeError(
                    f"{field} differs: {first} has {other!r}, {build_id} has {value!r}"
                )
            agreed.setdefault(field, (build_id, value))
    return {field: value for field, (_, value) in agreed.items()}


def _number(value):
    """A finite JSON number that is not a boolean."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return isinstance(value, int) or math.isfinite(value)


def _well_formed(field, value):
    """Whether an agreed field holds what publish records there: a release
    name, a tolerance in degrees, the classifier's settings — every setting
    ``classify.classifier_settings`` records, each a number."""
    if field == "overture_release":
        return isinstance(value, str) and bool(value)
    if field == "simplify_tolerance_deg":
        return _number(value) and value >= 0
    return (
        isinstance(value, dict)
        and set(value) == set(classify.classifier_settings())
        and all(_number(setting) for setting in value.values())
    )


def load_sources(sources, read_bytes=_read_file):
    """The selection verified and loaded for a merge, in label order: per
    source its ``label``, ``build_id``, ``path``, ``snapshot``, the SHA-256
    of its ``snapshot.json`` and ``NOTICE`` bytes, the ``notice`` itself and
    its ``tables`` as ``load_tables`` joins them.

    The manifests are checked first (``_check_sources``). Each manifest is
    read once, and the tables are verified against those same bytes, so the
    recorded digest names exactly the manifest generation that vouched for
    the tables, and the ``snapshot`` kept is what those bytes say. A source
    that does not verify — a manifest rewritten since it was selected (even
    to an equal value of another JSON type), a digest mismatch, tables that
    are not a build's — is refused: the merge ships every selected label or
    nothing.
    """
    _check_sources([(build_id, snapshot) for build_id, _, snapshot in sources])
    loaded = []
    for build_id, path, snapshot in sources:
        try:
            manifest = read_bytes(path / "snapshot.json")
        except OSError as error:
            raise MergeError(f"{build_id}: snapshot.json: {error}") from error

        def reading(file, manifest=manifest, path=path):
            return (
                manifest if Path(file) == path / "snapshot.json" else read_bytes(file)
            )

        verified = load_tables(path, reading, expected=snapshot)
        if verified is None:
            raise MergeError(f"{build_id}: the build does not verify")
        current, digests, tables = verified
        if _canonical(current) != _canonical(snapshot):
            raise MergeError(f"{build_id}: snapshot.json changed since it was selected")
        try:
            notice = read_bytes(path / "NOTICE")
        except OSError as error:
            raise MergeError(f"{build_id}: NOTICE: {error}") from error
        if hashlib.sha256(notice).hexdigest() != digests["NOTICE"]:
            raise MergeError(f"{build_id}: NOTICE does not match its digest")
        loaded.append(
            {
                "label": label_of(build_id),
                "build_id": build_id,
                "path": path,
                "snapshot": current,
                "snapshot_sha256": hashlib.sha256(manifest).hexdigest(),
                "notice": notice,
                "notice_sha256": digests["NOTICE"],
                "tables": tables,
            }
        )
    loaded.sort(key=lambda source: source["label"])
    return loaded


def _without(table, names):
    return table.drop_columns([name for name in names if name in table.column_names])


def _split(table, names):
    """``{partition: rows}`` of ``table`` by each row's partition name in
    ``names``, the rows kept in table order."""
    groups = pd.Series(range(len(names))).groupby(names.to_numpy()).indices
    return {name: table.take(pa.array(rows)) for name, rows in groups.items()}


def _countries(table, key, column):
    """The country partition of each row of ``table``, keyed by ``key``: the
    two-letter code in ``column``, None for a null or empty value (what
    ``publish.partition`` treats as no country). Any other value is refused:
    a reserved name or a path fragment is not a country partition."""
    frame = table.select([key, column]).to_pandas().set_index(key)[column]
    frame = frame.mask(frame == "")
    codes = frame.dropna()
    odd = codes[~codes.astype(str).str.fullmatch(r"[A-Z]{2}")]
    if len(odd):
        raise MergeError(
            f"{key} {odd.index[0]} has {column} {odd.iloc[0]!r}; not a country partition"
        )
    return frame


def _route(tables):
    """The merged tables as ``{(partition, table): rows}``, the rules of
    ``publish.partition`` applied by column values: a feed under its home
    country, else ``international``; a companion under its static feed's
    partition, ``international`` when the merged index has no such feed; a
    place under its country, refused without one; an edge under the feed's
    home country when the place lies there, else under ``links`` with
    ``feed_partition``. The columns the merge and the viewer's join added
    (``build_id``, ``partition``, a domestic edge's ``feed_partition``) go,
    and every table is sorted by its ids.
    """
    feeds = _without(tables["feeds.parquet"], ("partition", "build_id"))
    feeds = feeds.sort_by("feed_id")
    places = _without(tables["places.parquet"], ("build_id",)).sort_by("place_id")
    edges = _without(tables["edges.parquet"], ("feed_partition", "build_id"))
    edges = edges.sort_by([("place_id", "ascending"), ("feed_id", "ascending")])
    home = _countries(feeds, "feed_id", "home_country")
    country = _countries(places, "place_id", "country_code")
    if country.isna().any():
        raise MergeError(
            f"place {country.index[country.isna()][0]} has no country_code; every "
            "place belongs to one country partition"
        )
    routed = {}
    for name, rows in _split(
        feeds, home.fillna(publish.INTERNATIONAL_PARTITION)
    ).items():
        routed[(name, "feeds")] = rows
    for name, rows in _split(places, country).items():
        routed[(name, "places")] = rows
    keys = edges.select(["place_id", "feed_id"]).to_pandas()
    for column, known, what in (
        ("feed_id", home, "of a feed"),
        ("place_id", country, "to a place"),
    ):
        unknown = keys.loc[~keys[column].isin(known.index), column]
        if len(unknown):
            raise MergeError(f"edge {what} the index lacks: {unknown.iloc[0]}")
    feed_home = keys["feed_id"].map(home)
    domestic = feed_home.notna() & (feed_home == keys["place_id"].map(country))
    split = _split(edges.filter(pa.array(domestic)), feed_home[domestic])
    for name, rows in split.items():
        routed[(name, "edges")] = rows
    if not domestic.all():
        links = edges.filter(pa.array(~domestic))
        partition = feed_home[~domestic].fillna(publish.INTERNATIONAL_PARTITION)
        routed[(publish.LINKS_PARTITION, "edges")] = links.append_column(
            "feed_partition", pa.array(partition, pa.string())
        )
    if "realtime.parquet" in tables:
        realtime = _without(tables["realtime.parquet"], ("partition", "build_id"))
        realtime = realtime.sort_by("feed_id")
        static = realtime["static_feed_id"].to_pandas()
        partition = static.map(home).where(static.isin(home.index))
        partition = partition.fillna(publish.INTERNATIONAL_PARTITION)
        for name, rows in _split(realtime, partition).items():
            routed[(name, "realtime")] = rows
    return dict(sorted(routed.items()))


def _partition_files(routed, snapshot_id):
    """``(files, listing)``: every routed table with its ``snapshot`` column
    set to the merged id, as Parquet bytes keyed ``(partition, table)``, and
    the manifest listing of each table's rows and digest."""
    files, listing = {}, {}
    for (partition, table), rows in routed.items():
        column = pa.array([snapshot_id] * len(rows), pa.string())
        rows = rows.set_column(
            rows.schema.get_field_index("snapshot"), "snapshot", column
        )
        sink = io.BytesIO()
        pq.write_table(rows, sink)
        data = sink.getvalue()
        files[(partition, table)] = data
        listing.setdefault(partition, {})[table] = {
            "rows": len(rows),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    return files, listing
