"""A dated local copy of the Wikidata answers a build asks for.

Every Wikidata lookup of the build (:class:`overture.WikidataClient`: OSM
relations to items, US metros, metro candidates, county FIPS codes, names
and aliases, coordinates) is a batch of keys answered key by key. The copy
job (:func:`make_copy`) asks those lookups once for every key a build can
ask — derived from the Overture release's divisions and the CBSA delineation
(:func:`copy_keys`) — and keeps each key's answer, or that Wikidata had none,
in one SQLite file; :class:`WikidataCopy` answers from it. A copy is made in
a folder of its own under the copies' root and named the current one by
``current.json`` only once complete, so a build never reads half a copy.

    python -m transitio_index.wikidata_copy [--root DIR] [--overture-dir DIR]
        [--release NAME] [--inputs DIR]
"""

import argparse
import datetime
import http.client
import json
import logging
import pathlib
import re
import sqlite3
import sys
import time

import pyarrow.dataset as ds

from transitio_index import cbsa, overture, store
from transitio_index.progress import configure, progress

log = logging.getLogger(__name__)

DEFAULT_ROOT = pathlib.Path("~/.cache/transitio-index/wikidata").expanduser()
COPY_FILE = "wikidata.sqlite"
CURRENT = "current.json"
# Held by the copy job over the copies' root for its whole run.
LOCK = ".wikidata.lock"
# A copy's day, the name of its folder after ``copy-``.
DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
# The lookups a copy holds, in the order the job asks them: the names and
# coordinates come last, also for the items the others answered with.
LOOKUPS = (
    "p402",
    "statistical_metros",
    "metro_candidates",
    "fips_items",
    "labels_and_aliases",
    "coordinates",
)
# Keys per call while copying; each call's answers are committed together, so
# a job stopped midway resumes where it stopped.
CHUNK = 1000
# Tries of a call that fails (a dropped or cut-off reply, a server error), the
# wait after the first failure in seconds, doubled after each further one.
CALL_ATTEMPTS = 5
CALL_WAIT = 30
_CALL_ERRORS = (OSError, http.client.HTTPException, ValueError, overture.GazetteerError)
# Keys per query when reading, under SQLite's bound on query parameters.
_READ_BATCH = 900
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS covered "
    "(lookup TEXT, key TEXT, PRIMARY KEY (lookup, key))",
    "CREATE TABLE IF NOT EXISTS answers "
    "(lookup TEXT, key TEXT, value TEXT, PRIMARY KEY (lookup, key))",
    "CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT)",
)


class WikidataCopyError(RuntimeError):
    """A Wikidata copy is missing, incomplete or not one."""


def copy_keys(dataset, counties):
    """``{lookup: sorted keys}`` a build can ask, from the Overture divisions
    ``dataset`` of the kinds the gazetteer reads and ``counties`` (the CBSA
    delineation, ``{cbsa: county FIPS codes}``): the divisions' QIDs (names,
    coordinates), the OSM relations of those without one (P402), the city
    QIDs (metro candidates), the US ones among them (US metros) and every
    county code (FIPS). The QIDs and relations are read as
    :func:`overture.normalize_division` reads them."""
    qids, cities, us_cities, relations = set(), set(), set(), set()
    columns = ["subtype", "country", "wikidata", "sources"]
    kinds = ds.field("subtype").isin(list(overture.SUBTYPE_TO_KIND))
    for batch in dataset.to_batches(columns=columns, filter=kinds):
        for row in batch.to_pylist():
            qid = row["wikidata"]
            if not (qid and overture.QID_PATTERN.match(qid)):
                relations.update(overture._osm_relation_ids(row["sources"]))
                continue
            qids.add(qid)
            if overture.SUBTYPE_TO_KIND[row["subtype"]] == "city":
                cities.add(qid)
                if row["country"] == "US":
                    us_cities.add(qid)
    codes = {code for found in counties.values() for code in found}
    keys = {
        "p402": relations,
        "statistical_metros": us_cities,
        "metro_candidates": cities,
        "fips_items": codes,
        "labels_and_aliases": qids,
        "coordinates": qids,
    }
    return {lookup: sorted(found) for lookup, found in keys.items()}


def _connect(path):
    """The copy file at ``path``, its tables created."""
    db = sqlite3.connect(path)
    for statement in _SCHEMA:
        db.execute(statement)
    return db


def _answered_items(db):
    """The QIDs the OSM relation and metro lookups answered with."""
    found = set()
    for lookup, value in db.execute(
        "SELECT lookup, value FROM answers WHERE lookup IN "
        "('p402', 'statistical_metros', 'metro_candidates')"
    ):
        value = json.loads(value)
        if lookup == "p402":
            found.update([value] if value else [])
        else:
            found.update(metro["qid"] for metro in value)
    return found


def _read_only(path):
    """The SQLite file at ``path`` opened read-only, through an encoded URI."""
    return sqlite3.connect(
        pathlib.Path(path).absolute().as_uri() + "?mode=ro", uri=True
    )


def _valid_day(day):
    """Whether ``day`` is an ISO date, ``YYYY-MM-DD``."""
    try:
        return bool(DAY.fullmatch(day)) and bool(datetime.date.fromisoformat(day))
    except (TypeError, ValueError):
        return False


def _linked(folder):
    """Whether the copy ``folder`` or its file is a link. Checked before the
    file is opened by path; a link swapped in between is not noticed: the
    copies are the maintainer's own scratch, as the Overture copies are."""
    return store._redirects(folder) or store._redirects(folder / COPY_FILE)


def _copy_folder(root, day):
    """``root/copy-<day>`` for an ISO date ``day``; another value, a linked
    folder or a linked copy file is refused."""
    if not _valid_day(day):
        raise WikidataCopyError(f"not a copy day: {day!r}")
    folder = pathlib.Path(root) / f"copy-{day}"
    if _linked(folder):
        raise WikidataCopyError(f"{folder}: a linked copy")
    return folder


def _unfinished_day(root):
    """The day of the newest copy under ``root`` the job did not finish."""
    days = []
    for folder in sorted(pathlib.Path(root).glob("copy-*")):
        day = folder.name[len("copy-") :]
        if not _valid_day(day) or _linked(folder):
            continue
        try:
            db = _read_only(folder / COPY_FILE)
            try:
                meta = dict(db.execute("SELECT name, value FROM meta"))
            finally:
                db.close()
        except sqlite3.Error:
            continue
        if "finished" not in meta and meta.get("day") == day:
            days.append(day)
    return max(days, default=None)


def _pending(db, lookup, keys):
    """The sorted keys of ``lookup`` the copy ``db`` does not cover yet: its
    ``keys``, and for the names and coordinates the items the OSM relation
    and metro lookups answered with."""
    wanted = set(keys.get(lookup, ()))
    if lookup in ("labels_and_aliases", "coordinates"):
        wanted |= _answered_items(db)
    covered = {
        key
        for (key,) in db.execute("SELECT key FROM covered WHERE lookup = ?", (lookup,))
    }
    return sorted(wanted - covered)


def _asked(client, lookup, keys):
    """``client``'s ``lookup`` for ``keys``, tried again after a failure up to
    ``CALL_ATTEMPTS`` times, waiting ``CALL_WAIT`` seconds and doubling: a
    failed call ends the job only when Wikidata keeps failing."""
    for attempt in range(1, CALL_ATTEMPTS + 1):
        try:
            return getattr(client, lookup)(keys)
        except _CALL_ERRORS as error:
            if attempt == CALL_ATTEMPTS:
                raise
            wait = CALL_WAIT * 2 ** (attempt - 1)
            log.warning("wikidata copy %s: %s; again in %d s", lookup, error, wait)
            time.sleep(wait)


def make_copy(root, keys, client, *, day=None):
    """Make, or resume, the copy of ``day`` under ``root`` (by default the
    newest unfinished one, else today's, UTC), holding ``LOCK`` over ``root``
    so no second job runs beside it: each lookup of ``LOOKUPS`` asked through
    ``client`` for its keys not yet covered (:func:`_pending`), ``CHUNK`` at
    a time, each call's answers and keys committed together; then
    ``current.json`` names the copy. A finished copy is never changed: one
    that covers ``keys`` is named again, one that lacks some is refused.
    Returns its folder."""
    root = pathlib.Path(root)
    directory = store.open_directory(root)
    try:
        try:
            with store.exclusive_writer(directory, LOCK):
                folder = _fill_copy(root, keys, client, day)
                store.write_bytes(
                    directory, CURRENT, json.dumps({"copy": folder.name}).encode()
                )
        except store.LockHeld:
            raise WikidataCopyError(f"{root}: another copy job is running") from None
    finally:
        directory.close()
    return folder


def _fill_copy(root, keys, client, day):
    """The copy folder of ``day`` (see :func:`make_copy`), every lookup asked
    and the copy marked finished."""
    if day is None:
        today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        day = _unfinished_day(root) or today
    folder = _copy_folder(root, day)
    folder.mkdir(parents=True, exist_ok=True)
    db = _connect(folder / COPY_FILE)
    try:
        finished = db.execute("SELECT 1 FROM meta WHERE name = 'finished'").fetchone()
        db.execute("INSERT OR IGNORE INTO meta VALUES ('day', ?)", (day,))
        db.commit()
        for lookup in LOOKUPS:
            pending = _pending(db, lookup, keys)
            if pending and finished:
                raise WikidataCopyError(
                    f"{folder}: the finished copy lacks {len(pending)} {lookup} "
                    "keys; make another day's copy"
                )
            chunks = range(0, len(pending), CHUNK)
            for start in progress(chunks, f"wikidata copy {lookup}"):
                chunk = pending[start : start + CHUNK]
                answers = _asked(client, lookup, chunk)
                db.executemany(
                    "INSERT OR REPLACE INTO answers VALUES (?, ?, ?)",
                    [
                        (lookup, key, json.dumps(value, sort_keys=True))
                        for key, value in answers.items()
                    ],
                )
                db.executemany(
                    "INSERT OR IGNORE INTO covered VALUES (?, ?)",
                    [(lookup, key) for key in chunk],
                )
                db.commit()
        if not finished:
            db.execute(
                "INSERT INTO meta VALUES ('finished', ?)",
                (datetime.datetime.now(datetime.timezone.utc).isoformat(),),
            )
            db.commit()
    finally:
        db.close()
    return folder


class WikidataCopy:
    """A finished copy, opened read-only: :meth:`answers` gives a lookup's
    answers for the keys the copy covers and the keys it does not."""

    def __init__(self, folder):
        self.folder = pathlib.Path(folder)
        path = self.folder / COPY_FILE
        if _linked(self.folder) or not path.is_file():
            raise WikidataCopyError(f"{path}: no Wikidata copy")
        self._db = _read_only(path)
        meta = dict(self._db.execute("SELECT name, value FROM meta"))
        if "finished" not in meta:
            raise WikidataCopyError(f"{path}: the copy is not finished")
        self.day = meta["day"]

    def answers(self, lookup, keys):
        """``(found, uncovered)``: ``{key: answer}`` for the ``keys`` the copy
        covers and holds an answer for, as the live lookup gives it, and the
        sorted keys it does not cover. A covered key without an answer is in
        neither: Wikidata had none."""
        if lookup not in LOOKUPS:
            raise ValueError(f"not a Wikidata lookup: {lookup!r}")
        keys = sorted({str(key) for key in keys})
        found, covered = {}, set()
        for start in range(0, len(keys), _READ_BATCH):
            batch = keys[start : start + _READ_BATCH]
            marks = ",".join("?" * len(batch))
            covered.update(
                key
                for (key,) in self._db.execute(
                    f"SELECT key FROM covered WHERE lookup = ? AND key IN ({marks})",
                    (lookup, *batch),
                )
            )
            for key, value in self._db.execute(
                f"SELECT key, value FROM answers WHERE lookup = ? AND key IN ({marks})",
                (lookup, *batch),
            ):
                found[key] = _as_answered(lookup, json.loads(value))
        return found, [key for key in keys if key not in covered]

    def close(self):
        self._db.close()


def _as_answered(lookup, value):
    """A stored answer as the live lookup gives it: coordinates as pairs."""
    if lookup == "coordinates":
        return [tuple(point) for point in value]
    return value


def current_copy(root):
    """The finished copy ``root/current.json`` names, opened; None when there
    is none."""
    pointer = pathlib.Path(root) / CURRENT
    try:
        name = json.loads(pointer.read_text()).get("copy")
    except (OSError, ValueError, AttributeError):
        return None
    if not isinstance(name, str) or not name.startswith("copy-") or "/" in name:
        return None
    try:
        return WikidataCopy(pathlib.Path(root) / name)
    except (WikidataCopyError, sqlite3.Error):
        return None


def parse_args(argv):
    """The copy job's options."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", type=pathlib.Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--overture-dir",
        type=pathlib.Path,
        default=overture.LOCAL_ROOT,
    )
    parser.add_argument("--release", help="the Overture release (default: newest)")
    parser.add_argument(
        "--inputs",
        type=pathlib.Path,
        help="the cache the CBSA delineation is prepared in (default: ROOT/inputs)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Make, or resume, today's copy for the release's divisions."""
    arguments = parse_args(argv)
    configure()
    if arguments.release:
        overture.use_release(arguments.release)
    overture.use_local_copy(arguments.overture_dir)
    inputs = arguments.inputs or arguments.root / "inputs"
    cbsa.prepare_inputs(inputs)
    counties, _ = cbsa.load_inputs(inputs)
    keys = copy_keys(overture.overture_dataset(), counties)
    log.info(
        "wikidata copy: %s", {lookup: len(found) for lookup, found in keys.items()}
    )
    folder = make_copy(arguments.root, keys, overture.WikidataClient())
    print(folder)
    return 0


if __name__ == "__main__":
    sys.exit(main())
