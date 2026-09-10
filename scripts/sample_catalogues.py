#!/usr/bin/env python3

"""Cut a small, multi-place sample of the three source catalogues.

A maintainer recipe, not part of the package: it fetches the full Transitland
Atlas archive, the Mobility Database ``feeds_v2.csv`` and the GBFS
``systems.csv``, then writes a trimmed archive and two trimmed CSVs covering
only the requested countries — a set small enough to run every
``transitio_index.build`` stage end to end. The final line prints the exact,
shell-quoted build command — ``--stage ingest --downstream`` (ingest through
publish, minting the registry's first rows), pinned with ``--commit`` to the
Atlas revision the sample was cut at — for the outputs; releasing the snapshot
with ``transitio_index.publish_cli`` and reading it back with the reader are
the operator's separate acceptance steps.

MDB and GBFS rows carry a country field — usually an ISO ``country_code``,
sometimes a full country name — so a requested country is matched on either
form; a missing or renamed column stops the cut rather than silently matching
nothing. Atlas feed records carry no country — the crosswalk places an Atlas
feed by matching its GTFS download URL (exactly, then by host) against MDB — so
the archive is trimmed to the DMFR files that share a download URL or host with
a kept MDB feed, the same signal the crosswalk uses. That Atlas↔MDB overlap is
kept when it exists; a country served by a single national feed with no Atlas
match yields an MDB-only sample. Downloads and matching reuse the build's own
modules, so the cut sees the catalogues exactly as the build would.

Each run fetches the full catalogues into a fresh temporary directory (not
kept), selects and validates all three before publishing anything, then writes
the outputs into a uniquely named subdirectory of ``--out-dir`` (default
``cache/sample``, gitignored) — so concurrent runs never collide and an
interrupted run leaves no half-written set anything would consume. Nothing
here is committed. The cut requires at least one MDB feed for each requested
country; GBFS systems and Atlas overlap are optional, so a country served by a
single national feed with no Atlas crosswalk still yields an MDB-only sample.
"""

import argparse
import csv
import io
import json
import re
import shlex
import sys
import tarfile
import tempfile
from pathlib import Path

from transitio_index import atlas, csv_source, gbfs, mdb, store
from transitio_index.crosswalk import _clean_url, _host

DEFAULT_COUNTRIES = ("FI", "EE")

# ISO country_code lives in these columns of the two CSV exports.
MDB_COUNTRY = "location.country_code"
MDB_DOWNLOAD = "urls.direct_download"
GBFS_COUNTRY = "Country Code"

# Some catalogue rows record a place by its full country name rather than its
# ISO code, so each requested country matches either form. Names are matched
# upper-cased, like the codes.
COUNTRY_NAMES = {
    "FI": "FINLAND",
    "EE": "ESTONIA",
    "NL": "NETHERLANDS",
    "AT": "AUSTRIA",
    "ES": "SPAIN",
    "CA": "CANADA",
    "MX": "MEXICO",
    "BR": "BRAZIL",
}
_NAME_TO_CODE = {name: code for code, name in COUNTRY_NAMES.items()}


def _country_code(token):
    """The ISO code a requested token names — the token itself if already a
    code, or the code its full name maps to."""
    return _NAME_TO_CODE.get(token, token)


def _match_values(code):
    """Every country-field value that names the country ``code``: the code and,
    when known, its full name — so a catalogue recording either form matches."""
    values = {code}
    if code in COUNTRY_NAMES:
        values.add(COUNTRY_NAMES[code])
    return values


def _is_iso_code(token):
    """Whether ``token`` looks like an ISO 3166-1 alpha-2 country code — two
    ASCII letters (validity against the actual code set is judged later, against
    the downloaded catalogue rows)."""
    return len(token) == 2 and token.isascii() and token.isalpha()


def _unrecognized_countries(requested):
    """Raw requested tokens that are neither an ISO alpha-2 code nor a curated
    full name — a typo or an unsupported name. The check is on the raw token, so
    a non-ASCII character cannot case-fold into a code (e.g. ``ß`` -> ``SS``).
    Refusing these up front stops a literal like ``FRANCE`` being passed through
    as a match value that only the data's shape would (fail to) reject."""
    return sorted(
        token
        for token in requested
        if not _is_iso_code(token) and token.upper() not in _NAME_TO_CODE
    )


def _limit_per_country(rows, field, limit):
    """At most ``limit`` rows per country, in input order — a small, deterministic
    sample so a large country's full catalogue does not overwhelm the build. With
    ``limit`` None the rows pass through unchanged."""
    if limit is None:
        return rows
    kept, counts = [], {}
    for row in rows:
        code = _country_code((row.get(field) or "").strip().upper())
        if counts.get(code, 0) < limit:
            kept.append(row)
            counts[code] = counts.get(code, 0) + 1
    return kept


def _chunks(rows, size):
    """``rows`` in consecutive lists of at most ``size``."""
    return [rows[start : start + size] for start in range(0, len(rows), size)] or [[]]


def _even_split(rows, parts):
    """``rows`` shared across ``parts`` groups, as evenly as possible, in order."""
    if parts <= 1:
        return [rows]
    base, extra = divmod(len(rows), parts)
    out, start = [], 0
    for i in range(parts):
        take = base + (1 if i < extra else 0)
        out.append(rows[start : start + take])
        start += take
    return out


# Multi-tenant endpoints where the URL path, not the host, identifies a feed: a
# shared-host match there is no evidence a feed belongs to a sampled place, so
# host-matching skips them (an exact-URL match on such a host is still kept).
# The match is exact-host — a bucket-specific virtual host like
# ``agency.s3.amazonaws.com`` is feed-specific and stays — plus the path-style
# S3 endpoints, including regional forms (``s3.eu-west-1.amazonaws.com``).
SHARED_HOSTS = frozenset(
    {
        "s3.amazonaws.com",
        "storage.googleapis.com",
        "www.googleapis.com",
        "drive.google.com",
        "raw.githubusercontent.com",
        "github.com",
        "gitlab.com",
        "www.dropbox.com",
        "dl.dropboxusercontent.com",
    }
)
_S3_PATH_STYLE = re.compile(r"\As3[.-][a-z0-9-]+\.amazonaws\.com\Z")


def _is_shared(host):
    return host in SHARED_HOSTS or bool(_S3_PATH_STYLE.match(host))


def _download(work, commit):
    """Fetch the three full catalogues into ``work``; return their paths."""
    directory = store.open_subdir(work, "download")
    try:
        atlas.download_archive(directory, "atlas.tar.gz", commit=commit)
        csv_source.download_file(directory, "feeds_v2.csv", mdb.CSV_URL)
        csv_source.download_file(directory, "systems.csv", gbfs.CSV_URL)
        base = directory.path
    finally:
        directory.close()
    return base / "atlas.tar.gz", base / "feeds_v2.csv", base / "systems.csv"


def _select_csv(src, country_field, required, countries):
    """``(fieldnames, rows)`` for rows whose country is in ``countries``.

    A missing required column is upstream schema drift, not an empty result, so
    it stops the cut rather than silently matching nothing.
    """
    with open(src, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = [column for column in required if column not in fieldnames]
        if missing:
            raise SystemExit(f"{src}: missing columns {missing}")
        kept = [
            row
            for row in reader
            if (row.get(country_field) or "").strip().upper() in countries
        ]
    return fieldnames, kept


def _mdb_targets(rows):
    """The exact download URLs and the non-shared hosts of the kept MDB feeds."""
    urls, hosts = set(), set()
    for row in rows:
        url = _clean_url(row.get(MDB_DOWNLOAD))
        if url is None:
            continue
        urls.add(url)
        host = _host(url)
        if host is not None and not _is_shared(host):
            hosts.add(host)
    return urls, hosts


def _feed_matches(feed, urls, hosts):
    for value in (feed.get("urls") or {}).values():
        cleaned = _clean_url(value)
        if cleaned is None:
            continue
        if cleaned in urls:
            return True
        host = _host(cleaned)
        if host is not None and host in hosts:
            return True
    return False


def _select_atlas(archive, urls, hosts, limit=None):
    """``(member name, payload)`` for the DMFR files with a matching feed.

    With ``limit`` the sample is trimmed to only the feeds that match a kept MDB
    feed, capped to ``limit`` in total: a feed-dense file's other agencies would
    otherwise dominate the crawl and overwhelm the expand stage. Without a limit
    each matching file is kept whole (the crosswalk sees the full Atlas layout).
    """
    kept = []
    total = 0
    for source_file, payload in atlas.iter_dmfr(archive):
        feeds = payload.get("feeds") or []
        matching = [feed for feed in feeds if _feed_matches(feed, urls, hosts)]
        if not matching:
            continue
        if limit is None:
            kept.append((source_file, payload))
            continue
        if total >= limit:
            break
        matching = matching[: limit - total]
        kept.append((source_file, {**payload, "feeds": matching}))
        total += len(matching)
    return kept


def _write_csv(out, fieldnames, rows):
    with open(out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_atlas(out, files):
    """Write the kept DMFR files to a gzipped tarball at ``out``.

    Whole files are kept (the ingest reads them whole), re-emitted under a
    synthetic ``<root>/feeds/<name>`` path — the only shape the ingest accepts.
    ``iter_dmfr`` yields a bare basename, but a name with a path separator or a
    ``..`` component is refused before it becomes a tar member that could
    traverse on extraction.
    """
    with tarfile.open(out, "w:gz") as tar:
        for source_file, payload in files:
            if source_file in (".", "..") or "/" in source_file or "\\" in source_file:
                raise SystemExit(f"unsafe DMFR file name: {source_file!r}")
            data = json.dumps(payload).encode("utf-8")
            info = tarfile.TarInfo(f"transitland-atlas/feeds/{source_file}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def _by_country(rows, field):
    counts = {}
    for row in rows:
        code = (row.get(field) or "").strip().upper()
        counts[code] = counts.get(code, 0) + 1
    return counts


def _missing(rows, field, countries):
    """Requested countries with no row in ``rows`` — a typo or unsupported code.

    A country is present when a row carries its ISO code or its full name; a
    multi-country request must not silently drop one that matched nothing just
    because another matched.
    """
    present = {(row.get(field) or "").strip().upper() for row in rows}
    return sorted(code for code in countries if not (_match_values(code) & present))


def _build_command(atlas_out, mdb_out, gbfs_out, commit):
    """The build command for the sample: every stage, pinned to ``commit``.

    ``--downstream`` runs ingest through publish (the gazetteer mints and saves
    the registry's first rows on the way); ``--commit`` pins the Atlas revision
    to the one the sample was cut at, so the build records matching provenance.
    """
    return shlex.join(
        [
            "python",
            "-m",
            "transitio_index.build",
            "--stage",
            "ingest",
            "--downstream",
            "--commit",
            commit,
            "--archive",
            str(atlas_out),
            "--mdb-csv",
            str(mdb_out),
            "--gbfs-csv",
            str(gbfs_out),
        ]
    )


def _emit(out_dir, mdb_fields, gbfs_fields, sample, countries, commit):
    """Write one sample set (``(mdb_rows, gbfs_rows, atlas_files)``) to
    ``out_dir`` and print its counts and build command."""
    mdb_rows, gbfs_rows, atlas_files = sample
    mdb_out = out_dir / "mdb_sample.csv"
    gbfs_out = out_dir / "gbfs_sample.csv"
    atlas_out = out_dir / "atlas_sample.tar.gz"
    _write_csv(mdb_out, mdb_fields, mdb_rows)
    _write_csv(gbfs_out, gbfs_fields, gbfs_rows)
    _write_atlas(atlas_out, atlas_files)
    if not gbfs_rows:
        print(f"note: no GBFS systems for {sorted(countries)}", file=sys.stderr)
    if not atlas_files:
        print(
            f"note: no Atlas DMFR files overlap the {len(mdb_rows)} MDB feed(s); "
            "the sample is MDB-only",
            file=sys.stderr,
        )
    print(
        f"mdb feeds: {len(mdb_rows)} {_by_country(mdb_rows, MDB_COUNTRY)} -> {mdb_out}"
    )
    print(
        f"gbfs systems: {len(gbfs_rows)} {_by_country(gbfs_rows, GBFS_COUNTRY)} "
        f"-> {gbfs_out}"
    )
    print(f"atlas dmfr files: {len(atlas_files)} -> {atlas_out}")
    print(f"run: {_build_command(atlas_out, mdb_out, gbfs_out, commit)}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python scripts/sample_catalogues.py",
        description="Cut a small multi-place sample of the source catalogues",
    )
    parser.add_argument(
        "--country",
        dest="countries",
        action="append",
        metavar="CC",
        help="ISO country code to keep (repeatable; default: "
        + " ".join(DEFAULT_COUNTRIES)
        + ")",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("cache/sample"),
        help="parent for the per-run output directory (default: cache/sample)",
    )
    parser.add_argument(
        "--commit",
        default=atlas.ATLAS_COMMIT,
        help="Atlas commit to fetch (default: the commit the build is pinned to)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="keep at most this many MDB feeds and GBFS systems per country — a "
        "small sample for a large country (default: no cap)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="split the sample into consecutive batches of this many MDB feeds "
        "(GBFS systems split evenly, Atlas trimmed to each batch's matching "
        "feeds), each a self-contained set built on its own — process a large "
        "country in memory-safe pieces rather than all at once",
    )
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.batch_size is not None and args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    raw = [c.strip() for c in (args.countries or DEFAULT_COUNTRIES)]
    # Requests may be ISO codes or full names; a token that is neither an ASCII
    # alpha-2 code nor a curated full name is refused before any download,
    # rather than passed through as a literal match value. Checked on the raw
    # input so a non-ASCII character cannot case-fold into a code.
    unknown = _unrecognized_countries(raw)
    if unknown:
        supported = ", ".join(sorted(COUNTRY_NAMES.values()))
        raise SystemExit(
            f"unrecognized --country {unknown}: use an ISO alpha-2 code or a "
            f"full name ({supported})"
        )
    # Canonicalise to codes, and match rows carrying either form.
    countries = {_country_code(token.upper()) for token in raw}
    match_values = set().union(*(_match_values(code) for code in countries))

    # Fetch and validate everything before publishing any output, so a schema
    # drift or an unsupported country fails loudly instead of leaving a
    # plausible-looking but unusable sample.
    with tempfile.TemporaryDirectory(prefix="sample-catalogues-") as tmp:
        atlas_full, mdb_full, gbfs_full = _download(Path(tmp), args.commit)
        mdb_fields, mdb_rows = _select_csv(
            mdb_full, MDB_COUNTRY, (MDB_COUNTRY, MDB_DOWNLOAD), match_values
        )
        gbfs_fields, gbfs_rows = _select_csv(
            gbfs_full, GBFS_COUNTRY, (GBFS_COUNTRY,), match_values
        )
        missing = _missing(mdb_rows, MDB_COUNTRY, countries)
        if missing:
            raise SystemExit(f"no MDB feeds for {missing}")
        mdb_rows = _limit_per_country(mdb_rows, MDB_COUNTRY, args.limit)
        gbfs_rows = _limit_per_country(gbfs_rows, GBFS_COUNTRY, args.limit)
        # Plan the sets while the downloaded catalogues still exist: one set, or
        # consecutive MDB slices, each with a proportional GBFS share and the
        # Atlas trimmed to that slice's matching feeds.
        if args.batch_size is None:
            urls, hosts = _mdb_targets(mdb_rows)
            planned = [
                (
                    mdb_rows,
                    gbfs_rows,
                    _select_atlas(atlas_full, urls, hosts, limit=args.limit),
                )
            ]
        else:
            mdb_batches = _chunks(mdb_rows, args.batch_size)
            gbfs_batches = _even_split(gbfs_rows, len(mdb_batches))
            planned = []
            for mdb_chunk, gbfs_chunk in zip(mdb_batches, gbfs_batches):
                urls, hosts = _mdb_targets(mdb_chunk)
                atlas_chunk = _select_atlas(
                    atlas_full, urls, hosts, limit=args.batch_size
                )
                planned.append((mdb_chunk, gbfs_chunk, atlas_chunk))

    # A fresh per-run directory: concurrent runs never collide, and a set is
    # only advertised once every file is written.
    args.out_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=args.out_dir))
    print(f"countries: {', '.join(sorted(countries))}")
    if args.batch_size is None:
        _emit(run_dir, mdb_fields, gbfs_fields, planned[0], countries, args.commit)
    else:
        print(f"batches: {len(planned)} (up to {args.batch_size} MDB feeds each)")
        for index, batch in enumerate(planned):
            batch_dir = run_dir / f"batch-{index:03d}"
            batch_dir.mkdir()
            print(f"[batch {index}]")
            _emit(batch_dir, mdb_fields, gbfs_fields, batch, countries, args.commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
