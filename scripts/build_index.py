#!/usr/bin/env python3

"""Build the place-based feed index.

Stages read and write files in a build cache, so any stage can be run on
its own once its inputs exist:

    python scripts/build_index.py --stage ingest

The stages are described in plans/place-index.md. ``ingest`` reads the
Transitland Atlas, the Mobility Database catalogue and the GBFS systems
catalogue; ``crosswalk`` resolves the same feed across them into one table;
``publish`` writes that table as the shippable ``index/`` (Parquet + manifest).
"""

import argparse
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from index_build import atlas, crosswalk, gbfs, mdb, publish  # noqa: E402

DEFAULT_CACHE_DIR = pathlib.Path("cache")

SOURCES = ("atlas", "mdb", "gbfs")


def commit_sha(value):
    """A full 40-character hex SHA, so the pin cannot move.

    Branch and tag names would resolve to whatever they point at today,
    which is the opposite of pinning. The same rule is enforced inside the
    ingest itself; this only turns it into a clean CLI message.
    """
    if not atlas.is_commit_sha(value):
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a full 40-character commit SHA"
        )
    return value


def run_ingest(arguments):
    summaries = []
    if "atlas" in arguments.sources:
        summaries.append(
            atlas.ingest(
                arguments.cache_dir,
                archive=arguments.archive,
                commit=arguments.commit,
            )
        )
    if "mdb" in arguments.sources:
        summaries.append(mdb.ingest(arguments.cache_dir, csv_path=arguments.mdb_csv))
    if "gbfs" in arguments.sources:
        summaries.append(gbfs.ingest(arguments.cache_dir, csv_path=arguments.gbfs_csv))
    return summaries


def run_crosswalk(arguments):
    return [crosswalk.crosswalk(arguments.cache_dir)]


def run_publish(arguments):
    return [publish.publish(arguments.cache_dir)]


STAGES = {
    "ingest": run_ingest,
    "crosswalk": run_crosswalk,
    "publish": run_publish,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="build_index.py", description="Build the transitio feed index"
    )
    parser.add_argument(
        "--stage", required=True, choices=sorted(STAGES), help="build stage to run"
    )
    parser.add_argument(
        "--source",
        dest="sources",
        action="append",
        choices=SOURCES,
        help="limit the ingest to this source (repeatable; default: all)",
    )
    parser.add_argument(
        "--cache-dir",
        type=pathlib.Path,
        default=DEFAULT_CACHE_DIR,
        help=f"build cache directory (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--archive",
        type=pathlib.Path,
        help="ingest this local Atlas tarball instead of downloading one",
    )
    parser.add_argument(
        "--mdb-csv",
        type=pathlib.Path,
        help="ingest this local feeds_v2.csv instead of downloading it",
    )
    parser.add_argument(
        "--gbfs-csv",
        type=pathlib.Path,
        help="ingest this local systems.csv instead of downloading it",
    )
    parser.add_argument(
        "--commit",
        type=commit_sha,
        default=atlas.ATLAS_COMMIT,
        help="Atlas commit to pin (default: the commit this build is pinned to)",
    )
    arguments = parser.parse_args(argv)
    if not arguments.sources:
        arguments.sources = list(SOURCES)
    return arguments


def main(argv=None):
    arguments = parse_args(argv)
    try:
        summaries = STAGES[arguments.stage](arguments)
    except Exception as error:  # noqa: B902
        # Broad on purpose: this is the CLI boundary, where any stage
        # failure should read as one line rather than a traceback. Set
        # TRANSITIO_TRACEBACK to see the original instead.
        if os.environ.get("TRANSITIO_TRACEBACK"):
            raise
        print(f"{arguments.stage}: {error}", file=sys.stderr)
        return 1
    for summary in summaries:
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
