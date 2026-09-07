"""The place registry: transitio's own place ids and their concordances.

A committed JSON Lines file — a header, then one row per minted id in
numeric order — is the durable authority on place identity. Ids are minted
from the header's counter and never reused; a place is found again through
the concordances (external ids, one ordered list per namespace) its row
carries; a merged row resolves to its successor and a retired one is
refused. A session holds the writer lock that lives beside the file and
saves once by atomic replacement, only when something changed and only if
the file is still the one it loaded. Identification (minting and
enrichment) follows. See ``plans/place-identity.md``.
"""

import contextlib
import hashlib
import json
import os
import re

from index_build import store

VERSION = 1
# Up to 18 digits: far beyond any counter, safely within int64 and the
# digit limit int() enforces, so a matching id always converts.
ID_PATTERN = re.compile(r"\Atp_[1-9][0-9]{0,17}\Z")
QID_PATTERN = re.compile(r"\AQ[1-9][0-9]*\Z")
NAMESPACES = (
    "wikidata",
    "overture",
    "osm_relation",
    "eurostat_metro",
    "cbsa",
    "fao_city_region",
    "ghs_ucdb",
    "geonames",
)
KINDS = ("country", "region", "city", "metro")
LIVE_FIELDS = frozenset(
    {
        "place_id",
        "kind",
        "concordances",
        "name",
        "country_code",
        "minted_from",
        "minted_in",
    }
)
MERGED_FIELDS = frozenset(
    {"place_id", "status", "into", "concordances", "at", "reason"}
)
RETIRED_FIELDS = frozenset({"place_id", "status", "at", "reason"})
MAX_REGISTRY_BYTES = 256 * 1024 * 1024


class RegistryError(RuntimeError):
    """The registry is malformed, or a change it forbids was attempted."""


def _number(place_id):
    return int(place_id[3:])


def _text(value, where, field, optional=False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        raise RegistryError(f"{where}: {field} must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        # A lone surrogate survives JSON decoding but could never be saved.
        raise RegistryError(f"{where}: {field} is not encodable") from None
    return value


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _pairs(pairs):
    """A JSON object whose keys are unique at every level."""
    record = {}
    for key, value in pairs:
        if key in record:
            raise ValueError(f"duplicate key {key!r}")
        record[key] = value
    return record


def _json(line, where):
    try:
        return json.loads(line, object_pairs_hook=_pairs)
    except ValueError as error:
        raise RegistryError(f"{where}: {error}") from None


def _concordances(value, where):
    """``{namespace: [values]}`` validated: known namespaces, non-empty
    unique strings, QIDs under ``wikidata``."""
    if not isinstance(value, dict):
        raise RegistryError(f"{where}: concordances must be a mapping")
    result = {}
    for namespace, values in value.items():
        if namespace not in NAMESPACES:
            raise RegistryError(f"{where}: unknown namespace {namespace!r}")
        if not isinstance(values, list) or not values:
            raise RegistryError(f"{where}: {namespace} must be a non-empty list")
        kept, seen = [], set()
        for item in values:
            _text(item, where, namespace)
            if namespace == "wikidata" and not QID_PATTERN.match(item):
                raise RegistryError(f"{where}: {item!r} is not a QID")
            if item in seen:
                raise RegistryError(f"{where}: {namespace}:{item} listed twice")
            seen.add(item)
            kept.append(item)
        result[namespace] = kept
    return result


def _row(record, where):
    if not isinstance(record, dict):
        raise RegistryError(f"{where}: not an object")
    place_id = record.get("place_id")
    if not isinstance(place_id, str) or not ID_PATTERN.match(place_id):
        raise RegistryError(f"{where}: place_id {place_id!r}")
    where = f"{where} ({place_id})"
    status = record.get("status")
    if status is None:
        unknown = set(record) - LIVE_FIELDS
        if unknown:
            raise RegistryError(f"{where}: unknown fields {sorted(unknown)}")
        if record.get("kind") not in KINDS:
            raise RegistryError(f"{where}: kind {record.get('kind')!r}")
        row = {
            "place_id": place_id,
            "kind": record["kind"],
            "concordances": _concordances(record.get("concordances"), where),
            "name": _text(record.get("name"), where, "name", optional=True),
            "country_code": _text(
                record.get("country_code"), where, "country_code", optional=True
            ),
            "minted_from": _text(record.get("minted_from"), where, "minted_from"),
            "minted_in": _text(record.get("minted_in"), where, "minted_in"),
        }
        if not row["concordances"]:
            raise RegistryError(f"{where}: a live place needs a concordance")
        return row
    if status == "merged":
        unknown = set(record) - MERGED_FIELDS
        if unknown:
            raise RegistryError(f"{where}: unknown fields {sorted(unknown)}")
        into = record.get("into")
        if not isinstance(into, str) or not ID_PATTERN.match(into) or into == place_id:
            raise RegistryError(f"{where}: merged row needs a different 'into' id")
        row = {"place_id": place_id, "status": status, "into": into}
        if "concordances" in record:
            row["concordances"] = _concordances(record["concordances"], where)
    elif status == "retired":
        unknown = set(record) - RETIRED_FIELDS
        if unknown:
            raise RegistryError(
                f"{where}: retired row carries {sorted(unknown)}; "
                "a retired row names no successor and no concordances"
            )
        row = {"place_id": place_id, "status": status}
    else:
        raise RegistryError(f"{where}: status {status!r}")
    row["at"] = _text(record.get("at"), where, "at")
    row["reason"] = _text(record.get("reason"), where, "reason")
    return row


def effective(row):
    """The row's concordances as every lookup sees them, order kept (the
    detachments a curator records arrive with identification)."""
    return {
        namespace: list(values)
        for namespace, values in row.get("concordances", {}).items()
    }


def parse(raw, where):
    """``(header, rows)`` from the file bytes, every row validated and the
    links between them checked: ids in order and below the counter, merge
    targets live with no chains, a concordance value on at most one place."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise RegistryError(f"{where}: not UTF-8") from None
    lines = [line for line in text.split("\n") if line]
    if not lines:
        raise RegistryError(f"{where}: no header")
    header = _json(lines[0], f"{where}: header")
    if (
        not isinstance(header, dict)
        or set(header) != {"registry", "next_id"}
        or not _integer(header["registry"])
        or header["registry"] != VERSION
        or not _integer(header["next_id"])
        or header["next_id"] < 1
    ):
        raise RegistryError(f"{where}: header must be {{registry: 1, next_id: n}}")
    rows = {}
    previous = 0
    for position, line in enumerate(lines[1:], 2):
        row = _row(
            _json(line, f"{where}: line {position}"), f"{where}: line {position}"
        )
        number = _number(row["place_id"])
        if number <= previous:
            raise RegistryError(f"{where}: line {position}: ids out of order")
        if number >= header["next_id"]:
            raise RegistryError(
                f"{where}: line {position}: {row['place_id']} is at or above next_id"
            )
        previous = number
        rows[row["place_id"]] = row
    for place_id, row in rows.items():
        if row.get("status") == "merged":
            target = rows.get(row["into"])
            if target is None or target.get("status") is not None:
                raise RegistryError(
                    f"{where}: {place_id} merged into {row['into']}, which is not live"
                )
    _index(rows, where)
    return header, rows


def _survivor(rows, place_id):
    row = rows[place_id]
    return row["into"] if row.get("status") == "merged" else place_id


def _index(rows, where):
    """``{(namespace, value): live id}`` over effective values, merged rows
    resolving to their successor; a value on two places is an error."""
    index = {}
    for place_id, row in rows.items():
        if row.get("status") == "retired":
            continue
        owner = _survivor(rows, place_id)
        for namespace, values in effective(row).items():
            for value in values:
                claimed = index.setdefault((namespace, value), owner)
                if claimed != owner:
                    raise RegistryError(
                        f"{where}: {namespace}:{value} is on both {claimed} and {owner}"
                    )
    return index


def _serialize(header, rows):
    ordered = sorted(rows.values(), key=lambda row: _number(row["place_id"]))
    lines = [json.dumps(header, sort_keys=True)]
    lines.extend(
        json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False)
        for row in ordered
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _read(path):
    try:
        handle = store.open_regular_path(path)
    except FileNotFoundError:
        raise RegistryError(f"{path}: no registry") from None
    except store.StoreError as error:
        raise RegistryError(str(error)) from None
    try:
        return store.read_all(handle, MAX_REGISTRY_BYTES)
    finally:
        os.close(handle)


class Registry:
    """The loaded registry: lookups and the one save."""

    def __init__(self, path, header, rows, base, *, read_only, locked=False):
        self.path = path
        self.header = header
        self.rows = rows
        # ``base`` is the digest of the file as loaded, kept for provenance;
        # ``digest`` follows the file through this session's saves.
        self.base = base
        self.read_only = read_only
        self._locked = locked
        self.digest = base
        self.minted = 0
        self.enriched = 0
        self._index = _index(rows, str(path))

    @property
    def next_id(self):
        return self.header["next_id"]

    def survivor(self, place_id):
        """The live id ``place_id`` stands for: itself, or the successor it
        was merged into; a retired or unknown id is refused."""
        row = self.rows.get(place_id)
        if row is None:
            raise RegistryError(f"{place_id}: no such place")
        if row.get("status") == "retired":
            raise RegistryError(f"{place_id}: retired ({row['reason']})")
        return _survivor(self.rows, place_id)

    def effective(self, place_id):
        return effective(self.rows[place_id])

    def canonical_qid(self, place_id):
        """The first effective QID of the live place ``place_id`` stands for
        (a merged id's survivor; a retired one refused), or None."""
        return (self.effective(self.survivor(place_id)).get("wikidata") or [None])[0]

    def lookup(self, namespace, value):
        return self._index.get((namespace, value))

    def resolve(self, reference):
        """The live id an override reference names: a ``tp_`` id, a bare
        QID, or ``namespace:value``; never a mint."""
        if not isinstance(reference, str) or not reference:
            raise RegistryError(f"{reference!r}: not a place reference")
        if ID_PATTERN.match(reference):
            return self.survivor(reference)
        if QID_PATTERN.match(reference):
            namespace, value = "wikidata", reference
        elif ":" in reference:
            namespace, value = reference.split(":", 1)
        else:
            raise RegistryError(f"{reference!r}: not a place reference")
        if namespace not in NAMESPACES or not value:
            raise RegistryError(f"{reference!r}: not a place reference")
        found = self._index.get((namespace, value))
        if found is None:
            raise RegistryError(f"{reference!r}: no place carries it")
        return found

    @property
    def changed(self):
        return bool(self.minted or self.enriched)

    def save(self):
        """Replace the file atomically with the current state, if anything
        changed; refused, changed or not, when the file is no longer the one
        this session last saw, so a run never commits against an edit made
        underneath it."""
        if hashlib.sha256(_read(self.path)).hexdigest() != self.digest:
            raise RegistryError(f"{self.path}: changed since it was loaded; not saved")
        if not self.changed:
            return self.digest
        if self.read_only:
            raise RegistryError("saving: the registry is read-only")
        if not self._locked:
            raise RegistryError("saving: only a session holding the lock may save")
        payload = _serialize(self.header, self.rows)
        directory = store.open_directory(self.path.parent)
        try:
            self.digest = store.write_bytes(directory, self.path.name, payload)
            directory.sync()
        finally:
            directory.close()
        return self.digest

    def manifest(self):
        return {
            "registry_base": self.base,
            "registry_digest": self.digest,
            "next_id": self.header["next_id"],
            "minted": self.minted,
            "enriched": self.enriched,
        }


def load(path):
    """The registry at ``path``, validated, for reading: a registry that may
    change comes only from :func:`session`, under the lock."""
    return _load(path, read_only=True)


def _load(path, *, read_only, locked=False):
    raw = _read(path)
    header, rows = parse(raw, str(path))
    return Registry(
        path,
        header,
        rows,
        hashlib.sha256(raw).hexdigest(),
        read_only=read_only,
        locked=locked,
    )


@contextlib.contextmanager
def session(path, *, read_only=False):
    """The registry loaded under the writer lock beside its file, held until
    the block ends; the caller saves explicitly, before it publishes what
    depends on the ids. A second writer is refused rather than queued."""
    directory = store.open_directory(path.parent)
    try:
        try:
            lock = store.exclusive_writer(directory, path.name + ".lock")
            lock.__enter__()
        except store.StoreError:
            raise RegistryError(f"{path}: another build holds the registry") from None
        loaded = None
        try:
            loaded = _load(path, read_only=read_only, locked=True)
            yield loaded
        finally:
            # A registry kept past its session can no longer save: the lock
            # it saved under is about to be released.
            if loaded is not None:
                loaded._locked = False
            lock.__exit__(None, None, None)
    finally:
        directory.close()
