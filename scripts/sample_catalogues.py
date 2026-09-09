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

MDB and GBFS rows carry an ISO ``country_code``, so those are filtered exactly;
a missing or renamed column stops the cut rather than silently matching
nothing. Atlas feed records carry no country — the crosswalk places an Atlas
feed by matching its GTFS download URL (exactly, then by host) against MDB — so
the archive is trimmed to the DMFR files that share a download URL or host with
a kept MDB feed, the same signal the crosswalk uses, which keeps real
Atlas↔MDB overlap in the sample. Downloads and matching reuse the build's own
modules, so the cut sees the catalogues exactly as the build would.

Each run fetches the full catalogues into a fresh temporary directory (not
kept), selects and validates all three before publishing anything, then writes
the outputs into a uniquely named subdirectory of ``--out-dir`` (default
``cache/sample``, gitignored) — so concurrent runs never collide and an
interrupted run leaves no half-written set anything would consume. Nothing
here is committed. The cut stops before publishing if the MDB selection or the
Atlas overlap is empty.
"""

import argparse
import csv
import io
import json
import re
import shlex
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


def _select_atlas(archive, urls, hosts):
    """``(member name, payload)`` for every DMFR file with a matching feed."""
    kept = []
    for source_file, payload in atlas.iter_dmfr(archive):
        feeds = payload.get("feeds") or []
        if any(_feed_matches(feed, urls, hosts) for feed in feeds):
            kept.append((source_file, payload))
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

    A multi-country request must not silently drop a country that matched
    nothing just because another matched.
    """
    present = {(row.get(field) or "").strip().upper() for row in rows}
    return sorted(countries - present)


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
    args = parser.parse_args(argv)
    countries = {c.strip().upper() for c in (args.countries or DEFAULT_COUNTRIES)}

    # Fetch and validate everything before publishing any output, so a schema
    # drift or an unsupported country fails loudly instead of leaving a
    # plausible-looking but unusable sample.
    with tempfile.TemporaryDirectory(prefix="sample-catalogues-") as tmp:
        atlas_full, mdb_full, gbfs_full = _download(Path(tmp), args.commit)
        mdb_fields, mdb_rows = _select_csv(
            mdb_full, MDB_COUNTRY, (MDB_COUNTRY, MDB_DOWNLOAD), countries
        )
        gbfs_fields, gbfs_rows = _select_csv(
            gbfs_full, GBFS_COUNTRY, (GBFS_COUNTRY,), countries
        )
        missing = _missing(mdb_rows, MDB_COUNTRY, countries)
        if missing:
            raise SystemExit(f"no MDB feeds for {missing}")
        if not gbfs_rows:
            raise SystemExit(f"no GBFS systems for {sorted(countries)}")
        urls, hosts = _mdb_targets(mdb_rows)
        atlas_files = _select_atlas(atlas_full, urls, hosts)
        if not atlas_files:
            raise SystemExit(
                f"no Atlas DMFR files overlap the {len(mdb_rows)} MDB feeds; "
                "the crosswalk would have nothing to match"
            )

    # A fresh per-run directory: concurrent runs never collide, and the set is
    # only advertised once every file is written.
    args.out_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=args.out_dir))
    mdb_out = run_dir / "mdb_sample.csv"
    gbfs_out = run_dir / "gbfs_sample.csv"
    atlas_out = run_dir / "atlas_sample.tar.gz"
    _write_csv(mdb_out, mdb_fields, mdb_rows)
    _write_csv(gbfs_out, gbfs_fields, gbfs_rows)
    _write_atlas(atlas_out, atlas_files)

    print(f"countries: {', '.join(sorted(countries))}")
    print(
        f"mdb feeds: {len(mdb_rows)} {_by_country(mdb_rows, MDB_COUNTRY)} -> {mdb_out}"
    )
    print(
        f"gbfs systems: {len(gbfs_rows)} {_by_country(gbfs_rows, GBFS_COUNTRY)} "
        f"-> {gbfs_out}"
    )
    print(f"atlas dmfr files: {len(atlas_files)} -> {atlas_out}")
    print(f"run: {_build_command(atlas_out, mdb_out, gbfs_out, args.commit)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
