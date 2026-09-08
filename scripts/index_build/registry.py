"""The place registry: transitio's own place ids and their concordances.

A committed JSON Lines file — a header, then one row per minted id in
numeric order — is the durable authority on place identity. Ids are minted
from the header's counter and never reused; a place is found again through
the concordances (external ids, one ordered list per namespace) its row
carries; a merged row resolves to its successor and a retired one is
refused. A session holds the writer lock that lives beside the file,
identifies places through the registry — a known place by any of its
concordances, gaining the ones it lacked; a new one minted from the counter
— and saves once by atomic replacement, only when something changed and
only if the file is still the one it loaded. Values a curator detached stay
in a row's history but count for nothing. See ``plans/place-identity.md``.
"""

import contextlib
import hashlib
import json
import os
import re

from index_build import store

VERSION = 1
FILE = "places_registry.jsonl"
# Up to 18 digits: far beyond any counter, safely within int64 and the
# digit limit int() enforces, so a matching id always converts.
ID_PATTERN = re.compile(r"\Atp_[1-9][0-9]{0,17}\Z")
# The last id the pattern admits; a header may name one past it, meaning
# the id space is exhausted, and minting then refuses.
MAX_ID = 10**18 - 1
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
        "detached",
    }
)
MERGED_FIELDS = frozenset(
    {"place_id", "status", "into", "concordances", "detached", "at", "reason"}
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


def _detached(value, concordances, where):
    """Detachments validated against the stored concordances: each names a
    value the same namespace still stores, once."""
    if not isinstance(value, dict):
        raise RegistryError(f"{where}: detached must be a mapping")
    result = {}
    for namespace, entries in value.items():
        if namespace not in NAMESPACES:
            raise RegistryError(f"{where}: unknown namespace {namespace!r}")
        if not isinstance(entries, list) or not entries:
            raise RegistryError(f"{where}: detached {namespace} must be a list")
        seen = set()
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"value", "at", "reason"}:
                raise RegistryError(f"{where}: a detached {namespace} entry")
            for field in ("value", "at", "reason"):
                _text(entry[field], where, f"detached {field}")
            if entry["value"] not in concordances.get(namespace, ()):
                raise RegistryError(
                    f"{where}: detached {namespace}:{entry['value']} is not stored"
                )
            if entry["value"] in seen:
                raise RegistryError(
                    f"{where}: {namespace}:{entry['value']} detached twice"
                )
            seen.add(entry["value"])
        result[namespace] = entries
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
        if "detached" in record:
            row["detached"] = _detached(record["detached"], row["concordances"], where)
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
        if "detached" in record:
            row["detached"] = _detached(
                record["detached"], row.get("concordances", {}), where
            )
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
    """The row's concordances net of its detached values, order kept."""
    detached = {
        namespace: {entry["value"] for entry in entries}
        for namespace, entries in row.get("detached", {}).items()
    }
    result = {}
    for namespace, values in row.get("concordances", {}).items():
        kept = [v for v in values if v not in detached.get(namespace, ())]
        if kept:
            result[namespace] = kept
    return result


def parse(raw, where):
    """``(header, rows)`` from the file bytes, every row validated and the
    links between them checked: ids in order and below the counter, merge
    targets live with no chains, an effective concordance value (stored minus
    detached) on at most one place."""
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
        or not 1 <= header["next_id"] <= MAX_ID + 1
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
    except (OSError, store.StoreError) as error:
        # A directory, a symlink or an unreadable file: Windows reports some
        # of these as a permission error rather than through the store.
        raise RegistryError(f"{path}: {error}") from None
    try:
        return store.read_all(handle, MAX_REGISTRY_BYTES)
    finally:
        os.close(handle)


class Registry:
    """The loaded registry: lookups, identification and the one save."""

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

    def key_for(self, reference, *, mint=False, internal=False, strict=False):
        """The current key of the row ``reference`` names — its own id, or
        with ``internal`` the canonical QID a QID-keyed stage joins on (the
        own id for a row without one) — or the reference itself when no row
        carries it and it may name a place still to be minted: a bare QID
        always — on a first build no row exists yet, and a QID that names
        nothing surfaces as a stale override where it is applied — and any
        concordance under ``mint``; ``strict``, for a consumer of the
        finished gazetteer, refuses an unknown QID too. Other unknown
        references are refused."""
        if isinstance(reference, str) and QID_PATTERN.match(reference):
            if self.lookup("wikidata", reference) is None:
                if strict:
                    raise RegistryError(f"{reference!r}: no place carries it")
                return reference
        try:
            place_id = self.resolve(reference)
        except RegistryError:
            if mint and _mintable(reference):
                return reference
            raise
        if internal:
            return self.canonical_qid(place_id) or place_id
        return place_id

    def _refuse_change(self, what):
        if self.read_only:
            raise RegistryError(f"{what}: the registry is read-only")

    def identify(
        self,
        concordances,
        *,
        kind,
        minted_from,
        minted_in,
        name=None,
        country_code=None,
    ):
        """The id of the place the concordances name: the one they all agree
        on, which gains any it lacked (an enrichment); a value detached from
        that place refuses the candidate; none of them known mints the next
        id. A candidate without any concordance is refused, and in a
        read-only session so is every change, at the point of discovery."""
        where = f"{minted_from}"
        if kind not in KINDS:
            raise RegistryError(f"{where}: kind {kind!r}")
        wanted = _concordances(concordances, where)
        pairs = [(ns, v) for ns, values in wanted.items() for v in values]
        if not pairs:
            raise RegistryError(
                f"{where}: a place needs a concordance to be identified"
            )
        hits = sorted({self._index[pair] for pair in pairs if pair in self._index})
        if len(hits) > 1:
            raise RegistryError(f"{where}: concordances name several places {hits}")
        if hits:
            (place_id,) = hits
            row = self.rows[place_id]
            if row["kind"] != kind:
                raise RegistryError(
                    f"{where}: {place_id} is a {row['kind']}, not a {kind}"
                )
            # A detachment on the survivor or on any alias merged into it
            # stands: enrichment must not undo a curated correction.
            for owner, detached_row in self.rows.items():
                if _survivor(self.rows, owner) != place_id:
                    continue
                for namespace, entries in detached_row.get("detached", {}).items():
                    for entry in entries:
                        if (namespace, entry["value"]) in pairs:
                            raise RegistryError(
                                f"{where}: {namespace}:{entry['value']} was detached "
                                f"from {owner} ({entry['reason']})"
                            )
            missing = [pair for pair in pairs if self._index.get(pair) != place_id]
            if missing:
                self._refuse_change(f"{place_id} would gain {missing}")
                for namespace, value in missing:
                    row["concordances"].setdefault(namespace, []).append(value)
                    self._index[(namespace, value)] = place_id
                self.enriched += 1
            return place_id
        self._refuse_change(f"{where}: a new place would be minted")
        # Everything is validated before anything is mutated, so a refused
        # candidate consumes no id and leaves the session unchanged.
        row = {
            "kind": kind,
            "concordances": wanted,
            "name": _text(name, where, "name", optional=True),
            "country_code": _text(country_code, where, "country_code", optional=True),
            "minted_from": _text(minted_from, where, "minted_from"),
            "minted_in": _text(minted_in, where, "minted_in"),
        }
        if self.header["next_id"] > MAX_ID:
            raise RegistryError(f"{where}: the id space is exhausted")
        place_id = f"tp_{self.header['next_id']}"
        self.header["next_id"] += 1
        self.rows[place_id] = {"place_id": place_id, **row}
        for pair in pairs:
            self._index[pair] = place_id
        self.minted += 1
        return place_id

    @property
    def changed(self):
        return bool(self.minted or self.enriched)

    def save(self):
        """Replace the file atomically with the current state, if anything
        changed; refused, changed or not, when the file is no longer the one
        this session last saw, so a run never commits against an edit made
        underneath it."""
        if file_digest(self.path) != self.digest:
            raise RegistryError(f"{self.path}: changed since it was loaded; not saved")
        if not self.changed:
            return self.digest
        self._refuse_change("saving")
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


def file_digest(path):
    """The SHA-256 of the registry file as it is on disk."""
    return hashlib.sha256(_read(path)).hexdigest()


def _mintable(reference):
    """A well-formed ``namespace:value`` no row carries: a place a curated
    ``add_place`` may mint from."""
    if not isinstance(reference, str) or ":" not in reference:
        return False
    namespace, value = reference.split(":", 1)
    return namespace in NAMESPACES and bool(value)


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
