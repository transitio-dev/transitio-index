"""The builds a cache holds and the verified loading of one.

A build is a directory holding a published index, what the ``publish``
stage writes: before schema 7 the flat ``places.parquet``,
``edges.parquet``, ``feeds.parquet``, ``NOTICE`` and ``snapshot.json``;
from schema 7 a directory of partitions — one per country code with its
feeds, places and domestic edges, ``international/feeds.parquet`` and
``links/edges.parquet`` (the cross-border edges, with ``feed_partition``)
— listed in ``snapshot.json`` with each table's rows and digest. Builds
are discovered under a cache: the latest at ``cache/index`` and the
archived runs under ``cache/builds/<label>-<snapshot>/index``.

A build is loaded as a *verified snapshot*. Each listed file is read into
memory exactly once, hashed, and — for the parquet files — parsed from
those same bytes, so a republish landing between two reads can never pair
one generation's places with another's edges: a digest that does not match
means the build is mid-publish, and it is reported unavailable rather than
read. A partitioned build's tables are joined into flat frames (feeds
with their ``partition``, places, edges with ``feed_partition`` on the
links, and from schema 8 the realtime companions keyed by static feed).
The viewer and the merge of builds both read through this module.
"""

import datetime
import hashlib
import io
import json
import os
import re
import stat
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely.errors

INDEX_FILES = ("places.parquet", "edges.parquet", "feeds.parquet", "NOTICE")


DIGEST_KEYS = {
    "places.parquet": "places_sha256",
    "edges.parquet": "edges_sha256",
    "feeds.parquet": "feeds_sha256",
    "NOTICE": "notice_sha256",
}


# Schema 7: a partition is a country code, ``international`` (feeds without a
# home country) or ``links`` (the cross-border edges); its tables are listed
# under ``partitions`` in the snapshot with their rows and digest.
PARTITION_NAME = re.compile(r"[A-Z]{2}|international|links")


PARTITION_TABLES = ("feeds", "places", "edges")


# Schema 8 adds ``realtime``: the GTFS-RT companions of a partition's feeds,
# keyed by ``static_feed_id``; optional, never under ``links``.
REALTIME = "realtime"


# The tables a partition kind may carry; a country partition any of them.
PARTITION_LAYOUT = {"international": {"feeds", REALTIME}, "links": {"edges"}}


LINKS = "links"


# What a schema-7 build carries on top of the flat columns: the classify
# stage's country fields and the rank stage's relevance.
SCHEMA_7_COLUMNS = {
    "feeds.parquet": {"spec", "home_country", "scope", "declared_countries"},
    "edges.parquet": {"relevance_category", "relevance", "cross_border"},
    "realtime.parquet": {
        "feed_id",
        "name",
        "source",
        "static_feed_id",
        "static_link_method",
        "entity_types",
        "urls",
    },
}
# What schema 9 adds: the feeds' service spans and the places' validity.
SCHEMA_9_COLUMNS = {
    "feeds.parquet": {"service_start", "service_end"},
    "places.parquet": {"validity"},
}


LATEST = "latest"  # the id of the build at cache/index


CATALOGUE = "catalogue"  # the id of every label's newest build, merged


# An archived run is ``<label>-<16 hex>``; any other build id is its own label.
LABEL_SUFFIX = re.compile(r"-[0-9a-f]{16}$")


# What a published index carries and the viewer reads; a verified set of
# files that lacks any of these is not a build.
REQUIRED_COLUMNS = {
    "places.parquet": {
        "place_id",
        "kind",
        "name",
        "parent_id",
        "country_code",
        "service",
        "geometry",
    },
    "edges.parquet": {"place_id", "feed_id", "tier"},
    "feeds.parquet": {"feed_id", "name", "coverage"},
}


_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


# A snapshot that cannot be read or parsed (nesting past the recursion limit
# included) is treated like a missing one.
_SNAPSHOT_ERRORS = (OSError, ValueError, RecursionError)


# ... and a set of files that verifies but is not a build: parquet that does
# not decode (any Arrow error), a table of the wrong shape, undecodable WKB.
_BUILD_ERRORS = _SNAPSHOT_ERRORS + (
    KeyError,
    TypeError,
    pa.ArrowException,
    shapely.errors.ShapelyError,
)


# Read-only, never following a symlink, and non-blocking so that a FIFO
# planted in the cache is refused by the type check instead of waited on.
_OPEN_FLAGS = (
    os.O_RDONLY
    | _O_NOFOLLOW
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_BINARY", 0)
)


def _is_regular_file(path):
    """A regular file that is not a symlink; False if it cannot be inspected."""
    try:
        return stat.S_ISREG(os.lstat(path).st_mode)
    except OSError:
        return False


def _read_file(path):
    """The bytes of a regular file, refusing a symlink or any other node.

    Opened ``O_NOFOLLOW`` where the platform has it (else checked with ``lstat``
    first) and confirmed a regular file on the open descriptor; with
    discovery's containment check this keeps reads inside the cache. Accepted,
    deliberately: a directory swapped between that check and the open. The
    viewer is a read-only, localhost tool over a maintainer-owned cache, so the
    race can only show the maintainer a file they placed there themselves —
    the same residual the build's store accepts on its Windows path.
    """
    path = Path(path)
    if not _O_NOFOLLOW and path.is_symlink():
        raise OSError(f"{path}: cache entry is a symlink")
    handle = os.open(path, _OPEN_FLAGS)
    with os.fdopen(handle, "rb") as opened:
        if not stat.S_ISREG(os.fstat(opened.fileno()).st_mode):
            raise OSError(f"{path}: not a regular file")
        return opened.read()


def _snapshot_digest(snapshot):
    """One id for a whole snapshot, metadata included: a republish that
    changes only the edges, or only a count, changes it."""
    return hashlib.sha256(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def snapshot_digests(snapshot):
    """The digest a snapshot records for each index file, or None if any is missing."""
    digests = {}
    for name, key in DIGEST_KEYS.items():
        value = snapshot.get(key) if isinstance(snapshot, dict) else None
        if not isinstance(value, str) or not value:
            return None
        digests[name] = value
    return digests


def snapshot_files(snapshot):
    """``{relative path: (digest, rows)}`` for every file a snapshot lists, or
    None when it lists none or lists them badly.

    Before schema 7 the four flat files, each with a digest and no row count.
    From schema 7 every partition table (a partition name of the layout, a
    table of the layout, a string digest and an integer row count) and the
    ``NOTICE`` when the build is licensed (``notice_sha256`` a string; an
    unlicensed build has none).
    """
    if not isinstance(snapshot, dict):
        return None
    listing = snapshot.get("partitions")
    if listing is None:
        digests = snapshot_digests(snapshot)
        return None if digests is None else {n: (d, None) for n, d in digests.items()}
    if not isinstance(listing, dict) or not listing:
        return None
    files = {}
    for partition, tables in listing.items():
        if not isinstance(partition, str) or not PARTITION_NAME.fullmatch(partition):
            return None
        if not isinstance(tables, dict) or not tables:
            return None
        allowed = PARTITION_LAYOUT.get(partition, {*PARTITION_TABLES, REALTIME})
        for table, entry in tables.items():
            if table not in allowed or not isinstance(entry, dict):
                return None
            digest, rows = entry.get("sha256"), entry.get("rows")
            if not isinstance(digest, str) or not digest:
                return None
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                return None
            files[f"{partition}/{table}.parquet"] = (digest, rows)
    notice = snapshot.get("notice_sha256")
    if notice is None and snapshot.get("licensed"):
        return None  # a licensed build ships its NOTICE
    if notice is not None:
        if not isinstance(notice, str) or not notice:
            return None
        files["NOTICE"] = (notice, None)
    return files


def _files_present(path, files):
    """Every listed file is a regular file inside a plain partition directory."""
    for name in files:
        partition = name.rpartition("/")[0]
        if partition and not _plain_directory(path / partition):
            return False
        if not _is_regular_file(path / name):
            return False
    return True


def _join_partitions(tables):
    """The three viewer frames of a partitioned build from its parquet
    tables by path: feeds with their ``partition``, places, and the domestic
    edges with the links (``feed_partition`` on the links, null elsewhere)."""
    parts = {name: [] for name in (*PARTITION_TABLES, REALTIME)}
    for path, table in tables.items():
        partition, _, file = path.partition("/")
        kind = file[: -len(".parquet")]
        if kind in ("feeds", REALTIME):
            column = pa.array([partition] * len(table), pa.string())
            table = table.append_column("partition", column)
        elif kind == "edges" and "feed_partition" not in table.column_names:
            table = table.append_column(
                "feed_partition", pa.nulls(len(table), pa.string())
            )
        parts[kind].append(table)
    joined = {}
    for kind, found in parts.items():
        if not found:
            if kind == REALTIME:
                continue  # before schema 8, or a build with no companions
            return None  # a feeds-only build is not a build the viewer shows
        joined[f"{kind}.parquet"] = pa.concat_tables(found, promote_options="default")
    return joined


def _plain_directory(path):
    """A directory that is not a symlink; False if it cannot be inspected."""
    try:
        info = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode)


def _has_columns(table, name, required):
    """Every column ``required`` lists for ``name``, and no column twice.

    Arrow allows a duplicated column; one would select a two-dimensional
    geometry.
    """
    columns = table.column_names
    if len(set(columns)) != len(columns):
        return False
    return required.get(name, set()) <= set(columns)


def load_tables(path, read_bytes=_read_file, expected=None):
    """``(snapshot, digests, tables)`` of the verified build at ``path``, or
    None while it is mid-publish.

    Every file is read once through ``read_bytes``; its digest is checked and a
    table is parsed from those same bytes, so a file swapped between two reads
    surfaces as a digest mismatch, never as a mixed generation. With
    ``expected``, the snapshot read must equal it: a caller that chose the
    build on a snapshot loads that generation or none.
    """
    path = Path(path)
    try:
        snapshot = json.loads(read_bytes(path / "snapshot.json"))
        if expected is not None and snapshot != expected:
            return None
        files = snapshot_files(snapshot)
        if files is None:
            return None
        version = snapshot.get("schema_version")
        dated = isinstance(version, int) and version >= 9
        digests, tables = {}, {}
        for name, (digest, rows) in files.items():
            partition = name.rpartition("/")[0]
            if partition and not _plain_directory(path / partition):
                return None
            data = read_bytes(path / name)
            if hashlib.sha256(data).hexdigest() != digest:
                return None
            digests[name] = digest
            if name.endswith(".parquet"):
                table = pq.read_table(io.BytesIO(data))
                if rows is not None and len(table) != rows:
                    return None
                base = name.rpartition("/")[2]
                # Each partition on its own: a join promotes a column one
                # partition lacks to nulls, which would hide the gap.
                if "partitions" in snapshot and not (
                    _has_columns(table, base, REQUIRED_COLUMNS)
                    and _has_columns(table, base, SCHEMA_7_COLUMNS)
                    and (not dated or _has_columns(table, base, SCHEMA_9_COLUMNS))
                ):
                    return None
                tables[name] = table
        if "partitions" in snapshot:
            tables = _join_partitions(tables)
            if tables is None:
                return None
        for name in REQUIRED_COLUMNS:
            if not _has_columns(tables[name], name, REQUIRED_COLUMNS):
                return None
        return snapshot, digests, tables
    except _BUILD_ERRORS:
        return None


def _is_build_dir(index, root, listed=True):
    """A real directory inside ``root`` holding a regular ``snapshot.json``
    (any such directory, snapshot or not, when ``listed`` is False).

    Neither the directory nor the snapshot may be a symlink, and the directory
    must *resolve* under the resolved cache root, so a link anywhere in the
    path cannot point discovery outside the cache. An entry that cannot be
    inspected (unreadable, or gone mid-scan) is not a build.
    """
    try:
        return (
            index.is_dir()
            and not index.is_symlink()
            and (not listed or _is_regular_file(index / "snapshot.json"))
            and index.resolve().is_relative_to(root)
        )
    except OSError:
        return False


def discover(cache, listed=True):
    """``{build_id: index directory}`` for every build under ``cache``.

    With ``listed`` False the plain entries without a snapshot, or without
    their index directory, yet are included too: the catalogue's
    candidates, where a run being written must rank ahead of an older run
    of its label.
    """
    cache = Path(cache)
    root = cache.resolve()
    found = {}
    if _is_build_dir(cache / "index", root, listed):
        found[LATEST] = cache / "index"
    found.update(archived(cache / "builds", listed, root))
    return found


def archived(builds, listed=True, root=None):
    """``{build_id: index directory}`` for the runs archived under ``builds``
    (``<label>-<snapshot>/index``), each resolving under ``root`` (the
    builds directory itself unless the caller names the cache around it).
    ``listed`` as for ``discover``."""
    builds = Path(builds)
    root = builds.resolve() if root is None else root
    try:
        entries = sorted(builds.iterdir()) if not builds.is_symlink() else []
    except OSError:  # no builds/ directory, or it went away mid-scan
        entries = []
    found = {}
    for entry in entries:
        # ``latest`` (cache/index) and ``catalogue`` are reserved ids, and a
        # symlinked entry could redirect discovery outside the cache.
        if entry.name in (LATEST, CATALOGUE) or entry.is_symlink():
            continue
        index = entry / "index"
        if _is_build_dir(index, root, listed) or (
            not listed and _is_build_dir(entry, root, False)
        ):
            found[entry.name] = index
    return found


def label_of(build_id):
    """A build's label: an archived run's id without its snapshot suffix."""
    return LABEL_SUFFIX.sub("", build_id)


_EPOCH = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


def _built_at(snapshot):
    """A snapshot's build date as an aware datetime, None when unreadable."""
    value = snapshot.get("built_at") if isinstance(snapshot, dict) else None
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=datetime.timezone.utc)


def run_signature(path, snapshot):
    """What repairing a run that did not verify changes while its snapshot
    stays: the identity (inode, size, mode, modification and change times)
    of every file the snapshot lists and of every partition directory. On
    Windows the change time is the creation time, so a repair there that
    keeps a file's size and modification time is not noticed until the
    snapshot or another file changes."""
    files = snapshot_files(snapshot) or ()
    nodes = {*files, *(name.rpartition("/")[0] for name in files if "/" in name)}
    signature = []
    for name in sorted(nodes):
        try:
            info = os.lstat(Path(path) / name)
        except OSError:
            signature.append((name,))
            continue
        signature.append(
            (
                name,
                info.st_ino,
                info.st_size,
                info.st_mode,
                info.st_mtime_ns,
                info.st_ctime_ns,
            )
        )
    return tuple(signature)
