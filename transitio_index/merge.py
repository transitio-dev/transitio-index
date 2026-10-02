"""Merge the newest build of every label into one releasable snapshot.

    python -m transitio_index.merge --builds ~/.cache/transitio-index/builds \\
        --cache-dir cache/merged --partition cache/sample/partition-<id>/partition.json

``select_sources`` picks the newest complete run of every label archived
under a builds directory and ``merge_tables`` joins the selected builds'
tables into one set with one row per id. The viewer's catalogue page and
the merged snapshot both use them, so the page and the release cannot
disagree on what the merged index holds. ``load_sources`` verifies and
loads the selection for a merge, refusing what a merged snapshot could
not ship, ``compose_notice`` writes the merged index's NOTICE from the
sources' NOTICEs and the merged feeds, and ``assemble`` turns the merged
tables into a schema-11 snapshot: the partition tables, the providers
table at the root, their manifest and a snapshot id that names exactly the
sources, the merge format and the
toolchain they were merged with. ``write_snapshot`` reads the assembled
snapshot back through the reader before committing it into the cache's
``index/`` as publish commits its own. Given the ``partition.json`` of the
catalogue cut the builds came from, ``catalogue_check`` records which of its
feeds and labels, and which ``add_feed`` feeds of ``overrides/feeds.yaml``, the
merged index lacks; the publisher refuses the gaps.
"""

import argparse
import collections
import hashlib
import io
import json
import math
import re
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from transitio.index import fingerprint

from . import classify, coverage, crosswalk, licensing, overrides, publish, rank, store
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
# The override digests: a merged index records none of its own; each
# source's stays pinned by its manifest's digest in ``merged``.
OVERRIDE_FIELDS = (
    "overrides_sha256",
    "feeds_overrides_sha256",
    "places_overrides_sha256",
    "access_providers_overrides_sha256",
)


# Bumped by every change to the merge rules, the routing, the NOTICE
# composition or the serialisation, so a new implementation never reuses
# an old snapshot id. 2: the NOTICE parser accepts the geometry credit's
# source list as an indented continuation block. 3: an id another build
# folded into a feed is that feed. 4: a feed's containers are named by the
# merged ids. 5: a re-keyed id keeps its companions, and a feed lists the
# companions linked to it. 6: feeds of different builds with one content
# identity fold. 7: relevance is rescored over the merged edges. 8: the
# manifest records the catalogue check. 9: the check covers the curated feeds.
# 10: schema 11, the providers table and the places' populations.
MERGE_FORMAT = 10

STALE_FIELDS = ("stale_place_overrides", "stale_feed_overrides", "stale_edge_overrides")


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


def _alias_map(feeds):
    """``(mapping, conflicts)`` over stacked feed rows: ``{alias: feed_id}``
    for the ids another build lists as a feed's alias, and the id groups
    whose claims contradict each other.

    The claims (alias to the feed listing it) form a graph judged one
    connected component at a time, never followed transitively: a component
    maps only when it is a star — one feed claiming every other id and
    claimed by none. Two claimants, a chain or a cycle leave the whole
    component unmapped and listed in ``conflicts``.
    """
    if "aliases" not in feeds.column_names:
        return {}, []
    claims = {}
    for feed_id, aliases in zip(
        feeds["feed_id"].to_pylist(), feeds["aliases"].to_pylist()
    ):
        for alias in aliases or ():
            if alias != feed_id:
                claims.setdefault(alias, set()).add(feed_id)
    parent = {}

    def root(node):
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for alias, claimants in claims.items():
        for feed_id in claimants:
            parent[root(alias)] = root(feed_id)
    components = {}
    for node in parent:
        components.setdefault(root(node), set()).add(node)
    mapping, conflicts = {}, []
    for component in components.values():
        claimants = {f for alias in component & claims.keys() for f in claims[alias]}
        (canonical,) = claimants if len(claimants) == 1 else (None,)
        if canonical is None or canonical in claims:
            conflicts.append(sorted(component))
            continue
        mapping.update((alias, canonical) for alias in component - {canonical})
    return mapping, sorted(conflicts)


def _recorded_identities(snapshot):
    """The content identities a build recorded, when made by this identity
    version and well formed; otherwise none."""
    recorded = snapshot.get("feed_identities")
    version = snapshot.get("identity_version")
    current = type(version) is int and version == fingerprint.IDENTITY_VERSION
    if not current or not isinstance(recorded, dict):
        return {}
    return {
        feed_id: found
        for feed_id, found in recorded.items()
        if isinstance(found, dict)
        and all(isinstance(k, str) and isinstance(v, str) for k, v in found.items())
    }


def _content_folds(ranked, feeds, mapping, order):
    """``({folded id: canonical id}, best)`` for merged feeds whose content
    identities match (:func:`fingerprint.identical_groups`), each judged by
    the row and identity of the newest build carrying it and folded only with
    at least two stops; ``best`` is each id's ``(build_id, source)``. The
    canonical feed is the first by :data:`coverage.SOURCE_RANK`, then id, as
    in a build's own fold. Ids already re-keyed by ``mapping`` take no part.
    """
    columns = feeds.column_names
    if "source" not in columns or "stop_count" not in columns:
        return {}, {}
    recorded = {build_id: _recorded_identities(snap) for build_id, snap, _ in ranked}
    best = {}
    for feed_id, build_id, source, stops in zip(
        feeds["feed_id"].to_pylist(),
        feeds["build_id"].to_pylist(),
        feeds["source"].to_pylist(),
        feeds["stop_count"].to_pylist(),
    ):
        if feed_id in mapping:
            continue
        if feed_id not in best or order[build_id] < order[best[feed_id][0]]:
            best[feed_id] = (build_id, source, stops)
    identities = {
        feed_id: recorded[build_id].get(feed_id)
        for feed_id, (build_id, _, stops) in best.items()
        if stops is not None and stops >= 2
    }
    rank = coverage.SOURCE_RANK
    folds = {}
    for group in fingerprint.identical_groups(identities):
        group.sort(key=lambda f: (rank.get(best[f][1], len(rank)), f))
        folds.update((feed_id, group[0]) for feed_id in group[1:])
    return folds, {f: (build_id, source) for f, (build_id, source, _) in best.items()}


def _take_mdb_records(table, stacked, folds, best):
    """``table`` with each Atlas-only canonical taking the record of the
    first MDB feed folded into it, as a feed both catalogues carry matched by
    ``content`` — what a build's own fold does."""
    takes = {}
    for folded, canonical in sorted(folds.items()):
        if best[canonical][1] == "atlas" and best[folded][1] == "mdb":
            takes.setdefault(canonical, folded)
    if not takes:
        return table
    wanted = {(f, best[f][0]): canonical for canonical, f in takes.items()}
    fields = [c for c in ("mdb_id", "mdb", "name") if c in stacked.column_names]
    records = {}
    for row in stacked.select(["feed_id", "build_id", *fields]).to_pylist():
        canonical = wanted.get((row["feed_id"], row["build_id"]))
        if canonical is not None:
            records[canonical] = row
    ids = table["feed_id"].to_pylist()
    for column in (
        "source",
        "mdb_id",
        "mdb",
        "name",
        "crosswalk_method",
        "crosswalk_confidence",
    ):
        if column not in table.column_names:
            continue
        values = table[column].to_pylist()
        for i, feed_id in enumerate(ids):
            if feed_id not in records:
                continue
            record = records[feed_id]
            values[i] = {
                "source": "both",
                "mdb_id": record.get("mdb_id"),
                "mdb": record.get("mdb"),
                "name": values[i] or record.get("name"),
                "crosswalk_method": "content",
                "crosswalk_confidence": 1.0,
            }[column]
        index = table.schema.get_field_index(column)
        table = table.set_column(
            index, table.field(index), pa.array(values, table.field(index).type)
        )
    return table


def _union_aliases(table, stacked, mapping):
    """``table`` with each re-keyed feed's aliases the union of every source
    row's for it, the winning row's own first; a row of an id re-keyed into
    it adds that id and its aliases."""
    canonicals = set(mapping.values())
    union = {}
    for feed_id, aliases in zip(
        stacked["feed_id"].to_pylist(), stacked["aliases"].to_pylist()
    ):
        target = mapping.get(feed_id, feed_id)
        if target in canonicals:
            own = [] if target == feed_id else [feed_id]
            union.setdefault(target, []).extend([*own, *(aliases or ())])
    rows = [
        list(dict.fromkeys([*(aliases or ()), *union.get(feed_id, ())]))
        for feed_id, aliases in zip(
            table["feed_id"].to_pylist(), table["aliases"].to_pylist()
        )
    ]
    index = table.schema.get_field_index("aliases")
    return table.set_column(
        index, table.field(index), pa.array(rows, table.field(index).type)
    )


def _companion_lists(feeds, realtime):
    """``feeds`` with each feed's ``realtime_feed_ids`` the companions the
    merged realtime table links to it, as the publish stage derives them."""
    linked = {}
    for feed_id, static in zip(
        realtime["feed_id"].to_pylist(), realtime["static_feed_id"].to_pylist()
    ):
        if static is not None:
            linked.setdefault(static, set()).add(feed_id)
    rows = [sorted(linked.get(feed_id, ())) for feed_id in feeds["feed_id"].to_pylist()]
    index = feeds.schema.get_field_index("realtime_feed_ids")
    return feeds.set_column(
        index, feeds.field(index), pa.array(rows, feeds.field(index).type)
    )


def _merged_containers(table, mapping):
    """``table`` with each feed's ``contained_in`` named by the merged ids:
    re-keyed through ``mapping``, without the feed itself, repeats, or an id
    no merged feed carries."""
    ids = table["feed_id"].to_pylist()
    kept = set(ids)
    rows = [
        sorted({mapping.get(c, c) for c in containers or ()} & kept - {feed_id})
        for feed_id, containers in zip(ids, table["contained_in"].to_pylist())
    ]
    index = table.schema.get_field_index("contained_in")
    return table.set_column(
        index, table.field(index), pa.array(rows, table.field(index).type)
    )


def merge_tables(sources, skipped=()):
    """The catalogue's ``(snapshot, tables)`` over loaded ``sources``, each a
    ``(build_id, snapshot, tables)`` as ``load_tables`` returns them.

    One row per id: a feed from the newest source carrying it (ties to the
    lower id); a feed's edges from the source that won the feed, so one
    build's classification is never mixed with another's; a place from the
    source contributing the most kept edges to it, from the newest when none
    does. A realtime companion follows its static feed, an unlinked one the
    newest source. An id another build folded into a feed (see
    :func:`_alias_map`) is that feed, and so is a feed another build kept
    with the same content identity (see :func:`_content_folds`): its own
    rows are dropped, the feed's aliases gather every source's, and its
    companions follow the feed from the newest build carrying the id. A feed
    lists the companions linked to it. Every table gains ``build_id``. The
    edges' relevance is scored again over the merged edges (see
    :func:`_rescored`). The snapshot recounts the merged tables and lists the
    ``sources``, the ``skipped`` runs, the ``alias_conflicts``, the
    ``content_folds`` and the ``relevance`` rescore.
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
    mapping, conflicts = _alias_map(merged["feeds.parquet"])
    folds, best = _content_folds(ranked, merged["feeds.parquet"], mapping, order)
    mapping = {alias: folds.get(f, f) for alias, f in mapping.items()} | folds
    feeds = _keys(merged["feeds.parquet"], ["feed_id"], order)
    # Each re-keyed id's newest build, whose companions follow its feed.
    rekeyed = (
        feeds[feeds["feed_id"].isin(mapping.keys())]
        .sort_values("order", kind="stable")
        .drop_duplicates("feed_id")
    )
    feeds = feeds[~feeds["feed_id"].isin(mapping.keys())]
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
        realtime["static_feed_id"] = realtime["static_feed_id"].replace(mapping)
        # A companion of a won feed comes with that feed's source or not at
        # all; one linked to no won feed comes from the newest source.
        static = won.rename(columns={"feed_id": "static_feed_id"})
        # A re-keyed id keeps its companions: they follow its feed from the
        # newest build carrying the id.
        static = pd.concat(
            [
                static,
                pd.DataFrame(
                    {
                        "static_feed_id": rekeyed["feed_id"].map(mapping),
                        "build_id": rekeyed["build_id"],
                    }
                ),
            ],
            ignore_index=True,
        )
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
    if mapping:
        tables["feeds.parquet"] = _union_aliases(
            tables["feeds.parquet"], merged["feeds.parquet"], mapping
        )
        tables["feeds.parquet"] = _take_mdb_records(
            tables["feeds.parquet"], merged["feeds.parquet"], folds, best
        )
        if "realtime.parquet" in tables:
            # A companion taken from a build that did not fold its static
            # feed still names the folded id.
            companions = tables["realtime.parquet"]
            index = companions.schema.get_field_index("static_feed_id")
            linked = [mapping.get(f, f) for f in companions[index].to_pylist()]
            tables["realtime.parquet"] = companions.set_column(
                index,
                companions.field(index),
                pa.array(linked, companions.field(index).type),
            )
    if "contained_in" in tables["feeds.parquet"].column_names:
        tables["feeds.parquet"] = _merged_containers(tables["feeds.parquet"], mapping)
    if (
        "realtime.parquet" in tables
        and "realtime_feed_ids" in tables["feeds.parquet"].column_names
    ):
        tables["feeds.parquet"] = _companion_lists(
            tables["feeds.parquet"], tables["realtime.parquet"]
        )
    if "population" in tables["places.parquet"].column_names:
        tables["places.parquet"] = _populations(
            tables["places.parquet"], merged["places.parquet"]
        )
    if "access_providers.parquet" in merged:
        tables["access_providers.parquet"] = _providers(
            merged["access_providers.parquet"], tables["feeds.parquet"]
        )
    tables["edges.parquet"], relevance = _rescored(
        tables["edges.parquet"], tables["places.parquet"]
    )
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
        "alias_conflicts": conflicts,
        "content_folds": dict(sorted(folds.items())),
        "relevance": relevance,
    }
    return snapshot, tables


def _populations(places, stacked):
    """``places`` with each place's population the largest any source row of
    it records, null when none does: only the build seeding a city records
    its urban centre's population, and another build may serve it more."""
    largest = stacked.group_by("place_id").aggregate([("population", "max")])
    by_id = dict(
        zip(largest["place_id"].to_pylist(), largest["population_max"].to_pylist())
    )
    values = [by_id.get(place_id) for place_id in places["place_id"].to_pylist()]
    return _replaced(places, "population", values, publish._PLACES_SCHEMA)


def _providers(stacked, feeds):
    """The merged providers table: one row per provider id, which every
    source listing it must give alike. A feed's ``auth_params`` were bound
    to its own build's provider, so the build each merged feed comes from
    must list the provider it names."""
    columns = publish._ACCESS_PROVIDERS_SCHEMA.names
    rows, listed = {}, set()
    for row in stacked.select([*columns, "build_id"]).to_pylist():
        build_id = row.pop("build_id")
        listed.add((row["provider_id"], build_id))
        first, kept = rows.setdefault(row["provider_id"], (build_id, row))
        if kept != row:
            raise MergeError(
                f"access provider {row['provider_id']} differs between {first} "
                f"and {build_id}"
            )
    named = zip(feeds["access_provider"].to_pylist(), feeds["build_id"].to_pylist())
    unlisted = sorted({pair for pair in named if pair[0] is not None} - listed)
    if unlisted:
        raise MergeError(
            f"merged feeds name access providers their build does not list: {unlisted}"
        )
    return pa.Table.from_pylist(
        [rows[provider_id][1] for provider_id in sorted(rows)],
        schema=publish._ACCESS_PROVIDERS_SCHEMA,
    )


def _check_sources(snapshots):
    """Refuse a selection the merge cannot ship: a source below schema 9, an
    unlicensed one, one without a field of ``AGREED_FIELDS``, or sources
    disagreeing on one. Returns the agreed values."""
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
    _refuse_curated_clashes(loaded)
    return loaded


def _refuse_curated_clashes(loaded):
    """Refuse a feed id that is a curated feed in one source and a catalogue
    feed, static or realtime, in another: each build checks a curated id
    against its own catalogue cut only. A catalogue feed lists a curated id
    as an alias only when it holds the same data (a content fold): catalogue
    mints never take the curated prefix, and ``set_identity`` refuses it."""
    curated = collections.defaultdict(set)
    for source in loaded:
        for name in ("feeds.parquet", "realtime.parquet"):
            table = source["tables"].get(name)
            if table is None or "source" not in table.column_names:
                continue
            for feed_id, origin in zip(
                table["feed_id"].to_pylist(), table["source"].to_pylist()
            ):
                curated[feed_id].add(origin == "curated")
    clashes = sorted(feed_id for feed_id, kinds in curated.items() if len(kinds) > 1)
    if clashes:
        raise MergeError(f"curated feed ids a catalogue feed also carries: {clashes}")


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
    and every table is sorted by its ids. The providers table goes to the
    root, under the partition ``None``.
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
    routed = dict(sorted(routed.items()))
    if "access_providers.parquet" in tables:
        routed[(None, "access_providers")] = tables["access_providers.parquet"]
    return routed


def _partition_files(routed, snapshot_id):
    """``(files, listing)``: every routed table with its ``snapshot`` column
    set to the merged id, as Parquet bytes keyed ``(partition, table)``, and
    the manifest listing of each table's rows and digest; a root table
    (partition ``None``) has neither."""
    files, listing = {}, {}
    for (partition, table), rows in routed.items():
        if partition is not None:
            column = pa.array([snapshot_id] * len(rows), pa.string())
            rows = rows.set_column(
                rows.schema.get_field_index("snapshot"), "snapshot", column
            )
        sink = io.BytesIO()
        pq.write_table(rows, sink)
        data = sink.getvalue()
        files[(partition, table)] = data
        if partition is None:
            continue
        listing.setdefault(partition, {})[table] = {
            "rows": len(rows),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    return files, listing


def _merged_id(loaded, partition_sha256=None, curated_sha256=None):
    """The snapshot id: the first sixteen hex digits of a SHA-256 over the
    schema version, the merge format, the transitio and pyarrow versions,
    the digests of the partition and of the curated feeds the merge was
    checked against (None without them) and, per source in label order, its
    label, build id and the digests of its manifest and NOTICE. The same
    sources merged the same way name the same artefact; a change in any
    source, the partition, the curated feeds, the format or the toolchain
    names another."""
    from transitio import __version__ as transitio_version

    parts = [
        publish.SCHEMA_VERSION,
        MERGE_FORMAT,
        transitio_version,
        pa.__version__,
        partition_sha256,
        curated_sha256,
    ]
    for source in sorted(loaded, key=lambda s: s["label"]):
        parts.append(
            [
                source["label"],
                source["build_id"],
                source["snapshot_sha256"],
                source["notice_sha256"],
            ]
        )
    # Canonical JSON: a typed list, unambiguous whatever a label contains.
    return hashlib.sha256(_canonical(parts).encode("utf-8")).hexdigest()[:16]


def _json_records(edges, column):
    """The edges' ``column`` decoded, one dict per row. Null is what publish
    writes for an absent block and reads as ``{}``; anything else must be a
    JSON object."""
    try:
        values = [{} if v is None else json.loads(v) for v in edges[column].to_pylist()]
    except (TypeError, ValueError, RecursionError) as error:
        raise MergeError(f"edge {column} is not JSON: {error}") from error
    if not all(isinstance(v, dict) for v in values):
        raise MergeError(f"edge {column} is not a record")
    return values


def _replaced(table, name, values, schema=publish._EDGES_SCHEMA):
    """``table`` with column ``name`` set to ``values``, typed as publish
    writes it in ``schema``."""
    field = schema.field(name)
    column = pa.array(values, field.type)
    index = table.schema.get_field_index(name)
    if index < 0:
        return table.append_column(field, column)
    return table.set_column(index, field, column)


def _rescored(edges, places):
    """``(edges, block)``: the merged edges with their relevance scored again
    over the merged edge set by :func:`rank.score_edges`, and the snapshot's
    ``relevance`` block. Tier and ``cross_border`` stay: a feed's row and its
    edges come from one build, and a place's country is fixed by its id. An
    edges table without ``service`` or ``evidence`` stays as taken, with no
    block."""
    if not {"service", "evidence"} <= set(edges.column_names):
        return edges, None
    by_id = {
        place["place_id"]: place
        for place in places.select(["place_id", "kind", "country_code"]).to_pylist()
    }
    rows = edges.select(["place_id", "feed_id", "tier"]).to_pylist()
    for row in rows:
        if row["place_id"] not in by_id:
            raise MergeError(f"edge to a place the index lacks: {row['place_id']}")
        if row["tier"] not in rank.CATEGORY_BY_TIER:
            raise MergeError(f"edge with an unknown tier {row['tier']!r}")
    for column in ("service", "evidence"):
        for row, value in zip(rows, _json_records(edges, column)):
            row[column] = value
    if "needs_review" in edges.column_names:
        for row, flag in zip(rows, edges["needs_review"].to_pylist()):
            row["needs_review"] = flag
    scored, basis, unscored = rank.score_edges(rows, by_id)
    before = (
        edges["relevance"].to_pylist()
        if "relevance" in edges.column_names
        else [None] * len(scored)
    )
    changed = sum(
        1
        for old, edge in zip(before, scored)
        if old is None or abs(old - edge["relevance"]) > 1e-9
    )
    for name in ("relevance_category", "relevance", "needs_review"):
        edges = _replaced(edges, name, [edge[name] for edge in scored])
    edges = _replaced(
        edges, "evidence", [publish._json_block(edge["evidence"]) for edge in scored]
    )
    block = {
        "edges": len(scored),
        "changed": changed,
        "share_basis_by_place": dict(
            sorted(collections.Counter(basis.values()).items())
        ),
        "no_share_of_feed": unscored,
    }
    return edges, block


def _shares(edges):
    """``(unknown_share, margin_share)`` over the merged edges: the share
    whose tier is unknown, and the share the classifier decided within its
    margin of a threshold, from each edge's evidence as the classify stage
    counts it."""
    if not len(edges):
        return 0.0, 0.0
    if "evidence" not in edges.column_names:
        raise MergeError("edges without evidence; not a published index")
    evidence = _json_records(edges, "evidence")
    flagged = classify.near_threshold_count({"evidence": e} for e in evidence)
    unknown = pc.sum(pc.equal(edges["tier"], "unknown")).as_py() or 0
    return unknown / len(edges), flagged / len(edges)


# The catalogue pins a source's manifest records, by catalogue: what the
# merged block carries of its ``sources``.
CATALOGUE_PINS = {
    "atlas": ("commit", "archive_sha256", "commit_verified"),
    "mdb": ("csv_label", "csv_sha256"),
    "gbfs": ("csv_label", "csv_sha256"),
}


def _pins(snapshot, build_id):
    """A source's catalogue pins, projected to ``CATALOGUE_PINS``: portable
    identities only, whatever else the manifest carries there. A manifest
    naming no catalogue, or one whose record is not an object, is refused."""
    sources = snapshot.get("sources")
    if not isinstance(sources, dict) or not set(sources) & set(CATALOGUE_PINS):
        raise MergeError(f"{build_id}: no catalogue sources in the manifest")
    pins = {}
    for catalogue, keys in CATALOGUE_PINS.items():
        if catalogue not in sources:
            continue
        pinned = sources[catalogue]
        if not isinstance(pinned, dict):
            raise MergeError(f"{build_id}: catalogue {catalogue} is not a record")
        pins[catalogue] = {key: pinned.get(key) for key in keys}
    return pins


def _listing(snapshot):
    """A source's partition listing projected to each table's rows and digest."""
    return {
        partition: {
            table: {"rows": entry.get("rows"), "sha256": entry.get("sha256")}
            for table, entry in tables.items()
        }
        for partition, tables in snapshot["partitions"].items()
    }


def _count(snapshot, field, build_id):
    """A source's stale count, zero when unrecorded."""
    value = snapshot.get(field)
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MergeError(f"{build_id}: {field} is not a count: {value!r}")
    return value


def _recount(tables):
    """The manifest counts over the merged tables, as publish counts them."""
    feeds, places = tables["feeds.parquet"], tables["places.parquet"]
    edges, realtime = tables["edges.parquet"], tables.get("realtime.parquet")
    static = set(feeds["feed_id"].to_pylist())
    companions = realtime["static_feed_id"].to_pylist() if realtime is not None else []
    linked = sum(1 for feed_id in companions if feed_id in static)
    return {
        "feeds": len(feeds),
        "by_source": _value_counts(feeds["source"]),
        "feeds_dated": len(feeds) - feeds["service_start"].null_count,
        "realtime": len(companions),
        "realtime_linked": linked,
        "realtime_unlinked": len(companions) - linked,
        "places": len(places),
        "places_by_kind": _value_counts(places["kind"]),
        "edges": len(edges),
        "edges_by_tier": _value_counts(edges["tier"]),
        "access_providers": len(tables["access_providers.parquet"]),
    }


def read_partition(path):
    """``(partition, sha256)`` of a catalogue cut's ``partition.json``, read
    once: its ``catalogues`` pins, its ``labels``, each recording the digests
    of its cut's MDB CSV and Atlas archive, and its ``mdb`` and ``atlas`` maps
    of GTFS id to label; a label may list the ``countries`` it owns. A file
    that is not such a JSON object, or that assigns an id to a label it does
    not list, is refused."""
    try:
        data = Path(path).read_bytes()
        partition = json.loads(data)
    except OSError as error:
        raise MergeError(f"{path}: {error}") from error
    except (ValueError, RecursionError) as error:
        raise MergeError(f"{path}: not JSON: {error}") from error
    if not isinstance(partition, dict) or not all(
        isinstance(partition.get(key), dict)
        for key in ("catalogues", "labels", "mdb", "atlas")
    ):
        raise MergeError(
            f"{path}: not a partition; it needs catalogues, labels, mdb and atlas"
        )
    labels = partition["labels"]
    for label, entry in labels.items():
        if not isinstance(entry, dict) or not all(
            isinstance(entry.get(key), str) for key in ("mdb_sha256", "atlas_sha256")
        ):
            raise MergeError(f"{path}: label {label} records no cut digests")
        countries = entry.get("countries", [])
        if not isinstance(countries, list) or not all(
            isinstance(code, str) for code in countries
        ):
            raise MergeError(f"{path}: label {label} lists no country codes")
    for catalogue in ("mdb", "atlas"):
        for feed_id, label in partition[catalogue].items():
            if not isinstance(label, str) or label not in labels:
                raise MergeError(
                    f"{path}: {catalogue} {feed_id} is in {label!r}, a label the "
                    "partition does not list"
                )
    return partition, hashlib.sha256(data).hexdigest()


def catalogue_check(partition, digest, feeds, loaded, feed_overrides=None):
    """The merged manifest's ``catalogue_check`` of the merged ``feeds``
    and the ``loaded`` sources against a cut's ``partition`` and its
    ``digest``, as :func:`read_partition` returns them, and against the
    ``add_feed`` entries of ``feed_overrides`` (``feeds.yaml`` by reference):
    the digest, the partition's catalogue pins, the id counts and the digest
    of the add_feed entries whose ids were looked up (the one the crosswalk
    records as ``feeds_add_sha256``); ``missing``, by label and id, every id
    that is neither a merged ``feed_id`` nor an alias of one (an MDB row is
    looked up by its minted ``f-mdb-`` id; a curated feed is filed under the
    label owning its country, None when no label does); the partition's
    labels no source was merged for; the merged labels it does not list; and
    the merged labels whose build read another MDB CSV or Atlas archive than
    the cut the partition records for them."""
    present = set(feeds["feed_id"].to_pylist())
    if "aliases" in feeds.column_names:
        present.update(pc.list_flatten(feeds["aliases"]).to_pylist())
    wanted = [
        ("mdb", feed_id, label, crosswalk._mint_mdb(feed_id))
        for feed_id, label in partition["mdb"].items()
    ] + [
        ("atlas", feed_id, label, feed_id)
        for feed_id, label in partition["atlas"].items()
    ]
    labels = partition["labels"]
    owner = {
        code: label
        for label, entry in labels.items()
        for code in entry.get("countries", [])
    }
    curated = {
        ref: entry["add_feed"]["location"]["country_code"]
        for ref, entry in (feed_overrides or {}).items()
        if "add_feed" in entry
    }
    wanted += [("curated", ref, owner.get(code), ref) for ref, code in curated.items()]
    wanted.sort(key=lambda row: (row[2] or "", row[1], row[0]))
    merged = {source["label"]: source for source in loaded}
    another_cut = []
    for label in sorted(labels.keys() & merged.keys()):
        pins = _pins(merged[label]["snapshot"], merged[label]["build_id"])
        read = (
            pins.get("mdb", {}).get("csv_sha256"),
            pins.get("atlas", {}).get("archive_sha256"),
        )
        if read != (labels[label]["mdb_sha256"], labels[label]["atlas_sha256"]):
            another_cut.append(label)
    return {
        "partition_sha256": digest,
        "catalogues": partition.get("catalogues"),
        "expected": {
            "mdb": len(partition["mdb"]),
            "atlas": len(partition["atlas"]),
            "curated": len(curated),
        },
        "curated_sha256": overrides.phase_digest(
            feed_overrides or {}, overrides.CROSSWALK_OPERATIONS
        ),
        "missing": [
            {"catalogue": catalogue, "id": feed_id, "label": label}
            for catalogue, feed_id, label, key in wanted
            if key not in present
        ],
        "labels_not_merged": sorted(labels.keys() - merged.keys()),
        "labels_outside": sorted(merged.keys() - labels.keys()),
        "labels_from_another_cut": another_cut,
    }


def _report_check(check, log):
    """Log a catalogue check: each label not merged with its missing count,
    each missing id of a merged label, each label outside the partition or
    built from another cut."""
    unmerged = set(check["labels_not_merged"])
    counts = collections.Counter(row["label"] for row in check["missing"])
    for label in check["labels_not_merged"]:
        log(f"label {label} not merged: {counts[label]} catalogue feeds missing")
    for row in check["missing"]:
        if row["label"] is None:
            log(f"missing {row['catalogue']} {row['id']} (no label holds its country)")
        elif row["label"] not in unmerged:
            log(f"missing {row['catalogue']} {row['id']} (label {row['label']})")
    for label in check["labels_outside"]:
        log(f"label {label} is not in the partition")
    for label in check["labels_from_another_cut"]:
        log(f"label {label} was built from another cut")


def assemble(loaded, tables, notice, alias_conflicts=(), catalogue_check=None):
    """``(manifest, files)`` of the merged snapshot: the partition tables as
    Parquet bytes keyed ``(partition, table)`` and the manifest that lists
    them, over the sources ``load_sources`` returned and the ``tables``
    ``merge_tables`` produced from them, with the composed ``notice`` bytes.

    The manifest records what publish records where a merged index has it
    — the schema and reader floors, the counts recounted, ``built_at`` the
    newest source's (never the wall clock), the fields the sources agree on,
    ``coverage_mode`` crawled when every source is and mixed otherwise, the
    shares over the merged edges, the stale counts summed — and, in place of
    stage generations, a ``merged`` block naming each source by portable
    identities only: its label, build id, snapshot id, build date, coverage
    mode, catalogue pins, partition digests and the digests of its manifest
    and NOTICE; ``alias_conflicts`` are the id groups ``merge_tables`` left
    unfolded, and ``catalogue_check`` the block :func:`catalogue_check`
    returned (None when the merge was checked against no partition).
    """
    from transitio import __version__ as built_with
    from transitio.index import DISCOVERY_SEMANTICS_VERSION, MIN_READER_VERSIONS

    loaded = sorted(loaded, key=lambda s: s["label"])
    agreed = _check_sources([(s["build_id"], s["snapshot"]) for s in loaded])
    check = catalogue_check or {}
    snapshot_id = _merged_id(
        loaded, check.get("partition_sha256"), check.get("curated_sha256")
    )
    files, listing = _partition_files(_route(tables), snapshot_id)
    unknown_share, margin_share = _shares(tables["edges.parquet"])
    newest = max(loaded, key=lambda s: _built_at(s["snapshot"]) or _EPOCH)
    stale = {
        field: sum(_count(s["snapshot"], field, s["build_id"]) for s in loaded)
        for field in STALE_FIELDS
    }
    manifest = {
        "schema_version": publish.SCHEMA_VERSION,
        "discovery_semantics_version": DISCOVERY_SEMANTICS_VERSION,
        "min_reader_version": MIN_READER_VERSIONS.get(
            publish.SCHEMA_VERSION, publish.MIN_READER_VERSION
        ),
        "built_with": built_with,
        "snapshot_id": snapshot_id,
        "built_at": newest["snapshot"]["built_at"],
        "counts": _recount(tables),
        "partitions": listing,
        "access_providers_sha256": hashlib.sha256(
            files[(None, "access_providers")]
        ).hexdigest(),
        "licensed": True,
        "notice_sha256": hashlib.sha256(notice).hexdigest(),
        **{field: agreed.get(field) for field in AGREED_FIELDS},
        "coverage_mode": (
            "crawled"
            if all(s["snapshot"].get("coverage_mode") == "crawled" for s in loaded)
            else "mixed"
        ),
        "unknown_share": unknown_share,
        "margin_share": margin_share,
        **{field: None for field in OVERRIDE_FIELDS},
        **stale,
        "stale_overrides": sum(stale.values()),
        "alias_conflicts": [list(group) for group in alias_conflicts],
        "catalogue_check": catalogue_check,
        "merged": [
            {
                "label": s["label"],
                "build_id": s["build_id"],
                "snapshot_id": s["snapshot"]["snapshot_id"],
                "built_at": s["snapshot"].get("built_at"),
                "coverage_mode": s["snapshot"].get("coverage_mode"),
                "sources": _pins(s["snapshot"], s["build_id"]),
                "partitions": _listing(s["snapshot"]),
                "snapshot_sha256": s["snapshot_sha256"],
                "notice_sha256": s["notice_sha256"],
            }
            for s in loaded
        ],
    }
    return manifest, files


# The paragraphs of a NOTICE as the license stage writes it, by their
# opening words: the geometry credit with its derived sources, the ODbL
# terms when OpenStreetMap is among them, the metro credits, the
# catalogues read and the feed-licence inventory.
GEOMETRY_OPENING = "This index includes place boundary geometry"
ODBL_OPENING = "Geometry derived from OpenStreetMap"
METRO_OPENING = "Metro memberships were derived"
CATALOGUE_OPENING = "Feed identities and coverage were compiled from:"
LICENCE_OPENING = "Feed licences declared by the catalogues"
# The catalogue lines of a merged NOTICE: the dataset in the license
# stage's words, the pin the merge names it by, and what the line calls
# that pin. The Atlas is named by the archive digest each build read, not
# by a commit no build verified.
NOTICE_CATALOGUES = (
    ("Transitland Atlas", "atlas", "archive_sha256", "archive sha256"),
    ("Mobility Database catalog", "mdb", "csv_sha256", "sha256"),
    ("GBFS systems.csv", "gbfs", "csv_sha256", "sha256"),
)


def _notice_sections(notice, build_id):
    """A source NOTICE parsed into its paragraphs, keyed ``geometry``,
    ``odbl``, ``metro``, ``catalogue`` and ``licence`` (the middle two
    optional), each a list of lines; refused when it is not UTF-8, holds
    a paragraph the merge does not know or repeats one, or lacks the
    geometry, catalogue or licence paragraph."""
    try:
        text = notice.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MergeError(f"{build_id}: NOTICE is not UTF-8") from error
    openings = (
        ("geometry", GEOMETRY_OPENING),
        ("odbl", ODBL_OPENING),
        ("metro", METRO_OPENING),
        ("catalogue", CATALOGUE_OPENING),
        ("licence", LICENCE_OPENING),
    )
    sections = {}
    current = None
    for block in text.split("\n\n"):
        lines = block.strip("\n").split("\n")
        if lines == [""]:
            continue
        key = next((k for k, opening in openings if lines[0].startswith(opening)), None)
        if key is None:
            # A block that opens with no known heading continues the section
            # before it when it is indented (the license stage sets the
            # geometry credit's source list off with a blank line); a
            # non-indented one is a paragraph the merge does not know.
            if current is None or not lines[0].startswith(" "):
                raise MergeError(
                    f"{build_id}: NOTICE paragraph unknown to the merge: {lines[0]!r}"
                )
            sections[current].extend(lines)
            continue
        if key in sections:
            raise MergeError(f"{build_id}: NOTICE repeats its {key} paragraph")
        sections[key] = lines
        current = key
    for key in ("geometry", "catalogue", "licence"):
        if key not in sections:
            raise MergeError(f"{build_id}: NOTICE lacks its {key} paragraph")
    return sections


def _items(lines):
    """``(text, items)`` of a paragraph: its ``  - `` lines and the rest."""
    items = [line for line in lines if line.startswith("  - ")]
    return [line for line in lines if not line.startswith("  - ")], items


def _agree(current, value, what, build_id):
    """``value`` when ``current`` is unset or equal; refused otherwise."""
    if current is not None and current != value:
        raise MergeError(f"{what} differs at {build_id}")
    return value


_SHA256 = re.compile(r"[0-9a-f]{64}")


def _catalogue_lines(loaded):
    """One line per catalogue archive or CSV the sources read, by digest. A
    catalogue a source names must carry its digest as a SHA-256 string."""
    digests = {catalogue: set() for _, catalogue, _, _ in NOTICE_CATALOGUES}
    for source in loaded:
        pins = _pins(source["snapshot"], source["build_id"])
        for _, catalogue, key, _ in NOTICE_CATALOGUES:
            if catalogue not in pins:
                continue
            digest = pins[catalogue].get(key)
            if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
                raise MergeError(
                    f"{source['build_id']}: {catalogue} {key} is not a SHA-256: {digest!r}"
                )
            digests[catalogue].add(digest)
    return [
        f"  - {dataset}, {called} {digest}"
        for dataset, catalogue, _, called in NOTICE_CATALOGUES
        for digest in sorted(digests[catalogue])
    ]


def _licence_records(feeds):
    """The merged feeds as the records ``licensing._feed_rows`` inventories:
    their catalogue blocks decoded and the licence judgement the stage
    applied."""
    for column in ("atlas", "mdb", "redistribution_allowed"):
        if column not in feeds.column_names:
            raise MergeError(f"feeds without {column}; not a published index")
    records = []
    columns = (
        feeds[name].to_pylist() for name in ("atlas", "mdb", "redistribution_allowed")
    )
    for atlas, mdb, judged in zip(*columns):
        try:
            blocks = [
                None if block is None else json.loads(block) for block in (atlas, mdb)
            ]
        except (TypeError, ValueError, RecursionError) as error:
            raise MergeError(f"feed catalogue block is not JSON: {error}") from error
        if not all(block is None or isinstance(block, dict) for block in blocks):
            raise MergeError("feed catalogue block is not a record")
        licence = (blocks[0] or {}).get("license")
        if licence is not None and not isinstance(licence, dict):
            raise MergeError("feed licence block is not a record")
        records.append(
            {"atlas": blocks[0], "mdb": blocks[1], "redistribution_allowed": judged}
        )
    return records


def _licence_rows(feeds):
    """The feed-licence inventory of the merged feeds, as the license stage
    writes it; a block the stage's inventory cannot take is refused."""
    try:
        return licensing._feed_rows(_licence_records(feeds))
    except (TypeError, AttributeError) as error:
        raise MergeError(f"feed licences cannot be inventoried: {error}") from error


def compose_notice(loaded, feeds):
    """The merged index's NOTICE, composed from the sources' NOTICEs as the
    license stage writes one: the geometry credit once (its text must agree
    across the sources) with the union of their derived source lines, the
    ODbL terms when any source carries them, the metro credits once with
    the union of their lines, the catalogues named by every archive and
    CSV digest the sources read, and the feed-licence inventory recounted
    over the merged ``feeds``."""
    opening = odbl = metro = None
    derived, credits = set(), set()
    for source in loaded:
        sections = _notice_sections(source["notice"], source["build_id"])
        text, items = _items(sections["geometry"])
        opening = _agree(opening, text, "geometry notice", source["build_id"])
        derived.update(items)
        if "odbl" in sections:
            odbl = _agree(odbl, sections["odbl"], "ODbL notice", source["build_id"])
        if "metro" in sections:
            header, items = _items(sections["metro"])
            metro = _agree(metro, header, "metro notice", source["build_id"])
            credits.update(items)
    paragraphs = [[*opening, *sorted(derived)]]
    if odbl is not None:
        paragraphs.append(odbl)
    if metro is not None:
        paragraphs.append([*metro, *sorted(credits)])
    paragraphs.append([CATALOGUE_OPENING, *_catalogue_lines(loaded)])
    paragraphs.append(licensing._licence_lines(_licence_rows(feeds)))
    return ("\n\n".join("\n".join(lines) for lines in paragraphs) + "\n").encode(
        "utf-8"
    )


DEFAULT_BUILDS_DIR = Path("~/.cache/transitio-index/builds").expanduser()
DEFAULT_CACHE_DIR = Path("cache/merged")


def _write_index(directory, manifest, files, notice):
    """Write a snapshot into ``directory`` as publish writes its own: every
    partition table (tables and partitions the snapshot lacks removed), the
    NOTICE, and the manifest last."""
    publish._write_partitions(directory, files, manifest["partitions"])
    store.write_bytes(directory, publish.NOTICE_FILE, notice)
    store.write_file(
        directory,
        publish.SNAPSHOT_FILE,
        lambda: [json.dumps(manifest, indent=2, sort_keys=True)],
    )


def _remove_staging(staged):
    """Remove the staging directory; failing to is a merge error, and a
    symlink or a plain file in its place is refused rather than followed."""
    try:
        shutil.rmtree(staged)
    except FileNotFoundError:
        pass
    except OSError as error:
        raise MergeError(
            f"{staged}: cannot remove the staging directory: {error}"
        ) from error


def write_snapshot(cache_dir, manifest, files, notice):
    """Write the assembled snapshot to ``<cache_dir>/index``.

    Under the cache's writer lock throughout, so two merges cannot stage
    at once: first into a temporary sibling, ``index.<snapshot id>.tmp``,
    where the reader reads it back; a snapshot the reader refuses is
    removed and the live index is left as it was, absent included. Then
    committed as publish commits, under the index's own lock (the one the
    publisher takes): every partition table through the store's atomic
    replacement, the NOTICE, the manifest last. A reader overlapping the
    commit sees at worst a new table under the old manifest, which its
    digest check refuses; a crash mid-commit leaves an index the reader
    refuses, and the next merge completes it. The staging directory is
    removed afterwards (checked on success, best effort when the merge
    itself failed), and a leftover one is removed before writing.
    """
    from transitio.exceptions import IncompatibleIndexError
    from transitio.index import read_index

    cache = Path(cache_dir)
    staged = cache / f"index.{manifest['snapshot_id']}.tmp"
    root = store.open_directory(cache)
    try:
        with store.exclusive_writer(root):
            _remove_staging(staged)
            try:
                directory = root.child(staged.name)
                try:
                    _write_index(directory, manifest, files, notice)
                finally:
                    directory.close()
                try:
                    read = read_index(staged)
                except (IncompatibleIndexError, OSError, ValueError) as error:
                    raise MergeError(
                        f"the assembled snapshot does not read back: {error}"
                    ) from error
                if read.snapshot.get("snapshot_id") != manifest["snapshot_id"]:
                    raise MergeError(
                        "the assembled snapshot reads back under another id"
                    )
                live = root.child("index")
                try:
                    with store.exclusive_writer(live):
                        _write_index(live, manifest, files, notice)
                finally:
                    live.close()
            except BaseException:
                shutil.rmtree(staged, ignore_errors=True)  # the merge's error stands
                raise
            _remove_staging(staged)
    finally:
        root.close()


def merge_builds(
    builds,
    cache_dir,
    read_bytes=_read_file,
    log=print,
    partition=None,
    overrides_dir=None,
):
    """Merge the newest run of every label archived under ``builds`` into
    ``<cache_dir>/index``; returns the manifest. The labels whose newest
    run cannot be a source are reported through ``log`` and left out; an
    older run never stands in for one. Nothing to merge is refused. With
    the path of a cut's ``partition`` the merged feeds are checked against
    it and the ``add_feed`` entries of ``overrides_dir``'s ``feeds.yaml``
    (:func:`catalogue_check`), the gaps reported through ``log`` and
    recorded in the manifest."""
    cut = None
    if partition is not None:
        cut = read_partition(partition)
        feed_overrides, _ = overrides.load_feed_overrides(overrides_dir)
    sources, skipped = select_sources(builds, read_bytes)
    for run in skipped:
        log(f"skipped {run['id']}: {run['reason']}")
    if not sources:
        raise MergeError(f"no build to merge under {builds}")
    loaded = load_sources(sources, read_bytes)
    catalogue, tables = merge_tables(
        [(s["build_id"], s["snapshot"], s["tables"]) for s in loaded]
    )
    for group in catalogue["alias_conflicts"]:
        log(f"not folded, contradicting alias claims: {', '.join(group)}")
    if catalogue["content_folds"]:
        log(f"folded {len(catalogue['content_folds'])} feeds with the same content")
    relevance = catalogue["relevance"]
    if relevance is not None:
        places = sum(relevance["share_basis_by_place"].values())
        log(
            f"rescored relevance at {places} places: {relevance['changed']} of "
            f"{relevance['edges']} edges changed"
        )
        if relevance["no_share_of_feed"]:
            log(f"{relevance['no_share_of_feed']} edges lack share_of_feed")
    check = None
    if cut is not None:
        check = catalogue_check(*cut, tables["feeds.parquet"], loaded, feed_overrides)
        _report_check(check, log)
    notice = compose_notice(loaded, tables["feeds.parquet"])
    manifest, files = assemble(
        loaded,
        tables,
        notice,
        alias_conflicts=catalogue["alias_conflicts"],
        catalogue_check=check,
    )
    write_snapshot(cache_dir, manifest, files, notice)
    return manifest


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--builds",
        type=Path,
        default=DEFAULT_BUILDS_DIR,
        help=f"the archived builds, one <label>-<snapshot>/index each "
        f"(default: {DEFAULT_BUILDS_DIR})",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"the cache whose index/ receives the merged snapshot "
        f"(default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--partition",
        type=Path,
        default=None,
        help="the partition.json of the catalogue cut the builds came from; the "
        "merge checks every GTFS feed it lists is in the merged index",
    )
    parser.add_argument(
        "--overrides-dir",
        type=Path,
        default=Path("overrides"),
        help="directory of override YAML files; with --partition the merge also "
        "checks every add_feed feed of its feeds.yaml (default: overrides)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_args(argv)
    try:
        manifest = merge_builds(
            arguments.builds,
            arguments.cache_dir,
            partition=arguments.partition,
            overrides_dir=arguments.overrides_dir,
        )
    except (MergeError, store.StoreError, overrides.OverrideError) as error:
        print(f"merge: {error}", file=sys.stderr)
        return 1
    counts, check = manifest["counts"], manifest["catalogue_check"]
    missing = (
        "" if check is None else f"; {len(check['missing'])} catalogue feeds missing"
    )
    print(
        f"merged {len(manifest['merged'])} builds into {arguments.cache_dir / 'index'}: "
        f"snapshot {manifest['snapshot_id']}, {counts['feeds']} feeds, "
        f"{counts['places']} places, {counts['edges']} edges, "
        f"{counts['realtime']} companions{missing}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
