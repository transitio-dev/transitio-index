#!/usr/bin/env python3

"""Build the place-based feed index.

Stages read and write files in a build cache, so any stage can be run on
its own once its inputs exist:

    python scripts/build_index.py --stage ingest

The stages are described in plans/place-index.md. ``ingest`` reads the
Transitland Atlas, the Mobility Database catalogue and the GBFS systems
catalogue; ``crosswalk`` resolves the same feed across them into one table;
``gazetteer`` resolves the Overture administrative divisions to Wikidata QIDs and
seeds the feed cities from their declared locations; ``publish`` writes the feed
table as the shippable ``index/`` (Parquet + manifest).
"""

import argparse
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

DEFAULT_GOLDEN = (
    pathlib.Path(__file__).resolve().parent.parent / "golden" / "feeds.jsonl"
)

from index_build import (  # noqa: E402
    atlas,
    classify,
    coverage,
    crawl,
    crosswalk,
    curate,
    expand,
    gbfs,
    geometry,
    mdb,
    metros,
    names,
    overture,
    publish,
    resolve,
    seed,
)

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


def run_gazetteer(arguments):
    return [
        overture.resolve(arguments.cache_dir),
        seed.resolve_seed(arguments.cache_dir),
        metros.attach_metros(arguments.cache_dir),
        geometry.attach_geometry(arguments.cache_dir),
        names.merge_names(arguments.cache_dir),
    ]


def run_resolve(arguments):
    return [resolve.resolve(arguments.cache_dir, overrides_dir=arguments.overrides_dir)]


def run_expand(arguments):
    return [expand.expand(arguments.cache_dir)]


def run_crawl(arguments):
    return [crawl.crawl(arguments.cache_dir)]


def run_coverage(arguments):
    return [coverage.cover(arguments.cache_dir)]


def run_classify(arguments):
    return [classify.classify(arguments.cache_dir)]


def run_curate(arguments):
    return [
        curate.curate(
            arguments.cache_dir,
            overrides_dir=arguments.overrides_dir,
            strict=arguments.strict_overrides,
        )
    ]


def run_publish(arguments):
    golden_path = None if arguments.no_golden else arguments.golden
    if golden_path is not None and not golden_path.is_file():
        # The gate must never vanish because a file went missing.
        raise SystemExit(
            f"golden file {golden_path} is missing; pass --no-golden to publish "
            "without the golden diff"
        )
    return [
        publish.publish(
            arguments.cache_dir,
            golden_path=golden_path,
            overrides_dir=arguments.overrides_dir,
        )
    ]


STAGES = {
    "ingest": run_ingest,
    "crosswalk": run_crosswalk,
    "gazetteer": run_gazetteer,
    "resolve": run_resolve,
    "crawl": run_crawl,
    "expand": run_expand,
    "coverage": run_coverage,
    "classify": run_classify,
    "curate": run_curate,
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
    parser.add_argument(
        "--golden",
        type=pathlib.Path,
        default=DEFAULT_GOLDEN,
        help="golden set the publish stage must pass first (default: the "
        "repository's golden/feeds.jsonl)",
    )
    parser.add_argument(
        "--no-golden",
        action="store_true",
        help="publish without the golden diff (an explicit choice, never a default)",
    )
    parser.add_argument(
        "--overrides-dir",
        type=pathlib.Path,
        default=pathlib.Path("overrides"),
        help="directory of override YAML files (default: overrides)",
    )
    parser.add_argument(
        "--strict-overrides",
        action="store_true",
        help="fail the curate stage on a stale override instead of flagging it",
    )
    parser.add_argument(
        "--downstream",
        action="store_true",
        help="also run every later stage, in build order, so nothing downstream "
        "of a rerun is left stale",
    )
    arguments = parser.parse_args(argv)
    if not arguments.sources:
        arguments.sources = list(SOURCES)
    return arguments


def stages_from(stage, downstream):
    """The stages one invocation runs: the named one, and — with
    ``downstream`` — every later one in build order, which is the order
    ``STAGES`` lists them in."""
    order = list(STAGES)
    return order[order.index(stage) :] if downstream else [stage]


def main(argv=None):
    arguments = parse_args(argv)
    summaries = []
    stage = arguments.stage
    try:
        for stage in stages_from(arguments.stage, arguments.downstream):
            summaries.extend(STAGES[stage](arguments))
    except Exception as error:  # noqa: B902
        # Broad on purpose: this is the CLI boundary, where any stage
        # failure should read as one line rather than a traceback. Set
        # TRANSITIO_TRACEBACK to see the original instead.
        if os.environ.get("TRANSITIO_TRACEBACK"):
            raise
        print(f"{stage}: {error}", file=sys.stderr)
        return 1
    for summary in summaries:
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
