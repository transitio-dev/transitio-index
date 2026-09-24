"""Merge the newest build of every label into one releasable snapshot.

    python -m transitio_index.merge --builds ~/.cache/transitio-index/builds \\
        --cache-dir cache/merged

``select_sources`` picks the newest complete run of every label archived
under a builds directory and ``merge_tables`` joins the selected builds'
tables into one set with one row per id. The viewer's catalogue page and
the merged snapshot both use them, so the page and the release cannot
disagree on what the merged index holds. ``load_sources`` verifies and
loads the selection for a merge, refusing what a merged snapshot could
not ship, ``compose_notice`` writes the merged index's NOTICE from the
sources' NOTICEs and the merged feeds, and ``assemble`` turns the merged
tables into a schema-9 snapshot: the partition tables, their manifest and
a snapshot id that names exactly the sources, the merge format and the
toolchain they were merged with. ``write_snapshot`` reads the assembled
snapshot back through the reader before committing it into the cache's
``index/`` as publish commits its own.
"""

import argparse
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

from . import classify, licensing, publish, store
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


# Bumped by every change to the merge rules, the routing, the NOTICE
# composition or the serialisation, so a new implementation never reuses
# an old snapshot id.
MERGE_FORMAT = 1

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


def _merged_id(loaded):
    """The snapshot id: the first sixteen hex digits of a SHA-256 over the
    schema version, the merge format, the transitio and pyarrow versions
    and, per source in label order, its label, build id and the digests of
    its manifest and NOTICE. The same sources merged the same way name the
    same artefact; a change in any source, the format or the toolchain
    names another."""
    from transitio import __version__ as transitio_version

    parts = [publish.SCHEMA_VERSION, MERGE_FORMAT, transitio_version, pa.__version__]
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


def _shares(edges):
    """``(unknown_share, margin_share)`` over the merged edges: the share
    whose tier is unknown, and the share the classifier decided within its
    margin of a threshold, from each edge's evidence as the classify stage
    counts it."""
    if not len(edges):
        return 0.0, 0.0
    if "evidence" not in edges.column_names:
        raise MergeError("edges without evidence; not a published index")
    # Null is what publish writes for an edge without evidence; anything
    # else must be a JSON object.
    try:
        evidence = [
            json.loads(e) for e in edges["evidence"].to_pylist() if e is not None
        ]
    except (TypeError, ValueError, RecursionError) as error:
        raise MergeError(f"edge evidence is not JSON: {error}") from error
    if not all(isinstance(e, dict) for e in evidence):
        raise MergeError("edge evidence is not a record")
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
    }


def assemble(loaded, tables, notice):
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
    and NOTICE.
    """
    from transitio import __version__ as built_with
    from transitio.index import DISCOVERY_SEMANTICS_VERSION, MIN_READER_VERSIONS

    loaded = sorted(loaded, key=lambda s: s["label"])
    agreed = _check_sources([(s["build_id"], s["snapshot"]) for s in loaded])
    snapshot_id = _merged_id(loaded)
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
    for block in text.split("\n\n"):
        lines = block.strip("\n").split("\n")
        if lines == [""]:
            continue
        key = next((k for k, opening in openings if lines[0].startswith(opening)), None)
        if key is None:
            raise MergeError(
                f"{build_id}: NOTICE paragraph unknown to the merge: {lines[0]!r}"
            )
        if key in sections:
            raise MergeError(f"{build_id}: NOTICE repeats its {key} paragraph")
        sections[key] = lines
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


def merge_builds(builds, cache_dir, read_bytes=_read_file, log=print):
    """Merge the newest run of every label archived under ``builds`` into
    ``<cache_dir>/index``; returns the manifest. The labels whose newest
    run cannot be a source are reported through ``log`` and left out; an
    older run never stands in for one. Nothing to merge is refused."""
    sources, skipped = select_sources(builds, read_bytes)
    for run in skipped:
        log(f"skipped {run['id']}: {run['reason']}")
    if not sources:
        raise MergeError(f"no build to merge under {builds}")
    loaded = load_sources(sources, read_bytes)
    _, tables = merge_tables(
        [(s["build_id"], s["snapshot"], s["tables"]) for s in loaded]
    )
    notice = compose_notice(loaded, tables["feeds.parquet"])
    manifest, files = assemble(loaded, tables, notice)
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
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_args(argv)
    try:
        manifest = merge_builds(arguments.builds, arguments.cache_dir)
    except (MergeError, store.StoreError) as error:
        print(f"merge: {error}", file=sys.stderr)
        return 1
    counts = manifest["counts"]
    print(
        f"merged {len(manifest['merged'])} builds into {arguments.cache_dir / 'index'}: "
        f"snapshot {manifest['snapshot_id']}, {counts['feeds']} feeds, "
        f"{counts['places']} places, {counts['edges']} edges, "
        f"{counts['realtime']} companions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
