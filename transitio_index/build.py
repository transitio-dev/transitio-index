#!/usr/bin/env python3

"""Build the place-based feed index.

Stages read and write files in a build cache, so any stage can be run on
its own once its inputs exist:

    python -m transitio_index.build --stage ingest

The stages are described in the README's "Run a build" section. ``ingest`` reads the
Transitland Atlas, the Mobility Database catalogue and the GBFS systems
catalogue; ``crosswalk`` resolves the same feed across them into one table;
``gazetteer`` resolves the Overture administrative divisions to Wikidata QIDs and
seeds the feed cities from their declared locations; ``publish`` writes the
shippable ``index/`` in the cache — the Parquet tables and manifest the reader
installs.
"""

import argparse
import contextlib
import datetime
import functools
import json
import logging
import os
import pathlib
import time

DEFAULT_GOLDEN = (
    pathlib.Path(__file__).resolve().parent.parent / "golden" / "feeds.jsonl"
)

from transitio_index import (  # noqa: E402
    atlas,
    classify,
    coverage,
    crawl,
    crosswalk,
    curate,
    expand,
    fao,
    gbfs,
    geometry,
    licensing,
    mdb,
    metros,
    names,
    overture,
    progress,
    prune,
    publish,
    rank,
    registry,
    stats,
    resolve,
    seed,
    store,
)

log = logging.getLogger(__name__)

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


def registry_path(arguments):
    """The place registry: ``--registry``, else the overrides directory's."""
    return arguments.registry or arguments.overrides_dir / registry.FILE


def commit_run(cache_dir, path, *, read_only, stages):
    """Run the gazetteer stages as one transaction: every stage identifies
    places through one registry session and publishes its generation
    staged, the registry is saved once after the last stage, and the run
    manifest published last makes the whole set visible together — so a
    failure anywhere before that leaves the previous set current and, until
    the save, the registry untouched. The run lock is held throughout, so runs on
    one cache never interleave. Returns the stage summaries and the run
    manifest."""
    run = {}
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        # The run lock outlives every stage: a second run on this cache
        # cannot stage, prune or publish under this one.
        with (
            store.exclusive_writer(directory, store.RUN_LOCK),
            registry.session(path, read_only=read_only) as places,
        ):
            summaries = [stage(places, run) for stage in stages]
            digest = places.save()
            manifest = {
                "source": "gazetteer-run",
                "generations": dict(run),
                "registry_base": places.base,
                "registry_digest": digest,
                "next_id": places.next_id,
                "minted": places.minted,
                "enriched": places.enriched,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            rows = [{"pointer": p, "generation": g} for p, g in sorted(run.items())]
            with store.exclusive_writer(directory):
                summaries.append(
                    store.publish(
                        cache_dir / "gazetteer",
                        store.RUN_POINTER,
                        {"generations.jsonl": store.jsonl_chunks(rows)},
                        manifest,
                        held=directory,
                    )
                )
    finally:
        directory.close()
    return summaries


@contextlib.contextmanager
def check_registry_state(cache_dir, path):
    """Refuse to consume a gazetteer run whose registry is not the file on
    disk: after a pull, a branch switch or a curation edit, the gazetteer
    must run again before anything reads its places. The registry is held
    read-only for the block, so no run can save it under the consumer, and
    yields it for resolving override references."""
    gazetteer = cache_dir / "gazetteer"
    if not path.is_file():
        _refuse_missing_registry(gazetteer, path)
        yield None
        return
    # The manifests are read under the registry lock, so they and the file
    # are one consistent view: no run can commit between them.
    with registry.session(path, read_only=True) as places:
        expected = expand.expected_registry_digest(gazetteer, path)
        if expected is not None and expected != places.digest:
            raise registry.RegistryError(
                f"{path}: changed since the gazetteer ran; rerun the gazetteer"
            )
        yield places


def _refuse_missing_registry(gazetteer, path):
    if store.run_manifest(gazetteer) is not None:
        raise registry.RegistryError(
            f"{path}: changed since the gazetteer ran; rerun the gazetteer"
        )


def consumes_run(command):
    """A command that reads the gazetteer run, guarded for its whole
    duration by :func:`check_registry_state`."""

    @functools.wraps(command)
    def guarded(arguments):
        with check_registry_state(
            arguments.cache_dir, registry_path(arguments)
        ) as places:
            return command(arguments, places)

    return guarded


def run_gazetteer(arguments):
    cache_dir = arguments.cache_dir
    options = {
        "overrides_dir": arguments.overrides_dir,
        "strict": arguments.strict_overrides,
    }
    stages = [
        lambda places, run: overture.resolve(cache_dir, run=run),
        lambda places, run: seed.resolve_seed(
            cache_dir, registry=places, run=run, **options
        ),
        lambda places, run: metros.attach_metros(
            cache_dir, registry=places, run=run, **options
        ),
        lambda places, run: geometry.attach_geometry(
            cache_dir, registry=places, run=run, **options
        ),
        lambda places, run: names.merge_names(
            cache_dir, registry=places, run=run, **options
        ),
        lambda places, run: fao.suggest_metros(cache_dir, run=run),
    ]
    return commit_run(
        cache_dir,
        registry_path(arguments),
        read_only=arguments.registry_read_only,
        stages=stages,
    )


def run_resolve(arguments):
    return [resolve.resolve(arguments.cache_dir, overrides_dir=arguments.overrides_dir)]


def run_expand(arguments):
    """Expand under its own registry session: it identifies what it discovers,
    so it writes the registry the gazetteer run left, as a second transaction."""
    path = registry_path(arguments)
    gazetteer = arguments.cache_dir / "gazetteer"
    if not path.is_file():
        _refuse_missing_registry(gazetteer, path)
        return [
            expand.expand(arguments.cache_dir, overrides_dir=arguments.overrides_dir)
        ]
    with registry.session(path, read_only=arguments.registry_read_only) as places:
        return [
            expand.expand(
                arguments.cache_dir,
                overrides_dir=arguments.overrides_dir,
                registry=places,
            )
        ]


def run_crawl(arguments):
    return [crawl.crawl(arguments.cache_dir, workers=arguments.workers)]


@consumes_run
def run_coverage(arguments, places):
    return [
        coverage.cover(
            arguments.cache_dir,
            overrides_dir=arguments.overrides_dir,
            strict=arguments.strict_overrides,
            registry=places,
        )
    ]


@consumes_run
def run_classify(arguments, places):
    return [
        classify.classify(arguments.cache_dir, overrides_dir=arguments.overrides_dir)
    ]


@consumes_run
def run_curate(arguments, places):
    return [
        curate.curate(
            arguments.cache_dir,
            overrides_dir=arguments.overrides_dir,
            strict=arguments.strict_overrides,
            registry=places,
        )
    ]


@consumes_run
def run_rank(arguments, places):
    return [rank.rank(arguments.cache_dir)]


@consumes_run
def run_prune(arguments, places):
    return [prune.prune(arguments.cache_dir)]


@consumes_run
def run_license(arguments, places):
    return [
        licensing.license_index(
            arguments.cache_dir, overrides_dir=arguments.overrides_dir
        )
    ]


@consumes_run
def run_publish(arguments, places):
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
            registry=places,
        )
    ]


def run_stats(arguments):
    return [stats.stats(arguments.cache_dir)]


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
    "rank": run_rank,
    "prune": run_prune,
    "license": run_license,
    "publish": run_publish,
    "stats": run_stats,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m transitio_index.build",
        description="Build the transitio feed index",
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
        "--registry",
        type=pathlib.Path,
        default=None,
        help="the place registry (default: places_registry.jsonl in the overrides "
        "directory)",
    )
    parser.add_argument(
        "--registry-read-only",
        action="store_true",
        help="refuse to mint or enrich a place: the gazetteer fails at the first "
        "change the registry would need, and never writes it (CI, pinned builds)",
    )
    parser.add_argument(
        "--strict-overrides",
        action="store_true",
        help="fail the gazetteer, coverage and curate stages on a stale override "
        "instead of flagging it",
    )
    parser.add_argument(
        "--downstream",
        action="store_true",
        help="also run every later stage, in build order, so nothing downstream "
        "of a rerun is left stale",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="crawl this many feeds concurrently over one shared client "
        "(default: 1); the network, not cores, is the limit, so a modest cap "
        "(~8-32) saturates a home connection while staying polite. Each worker "
        "may buffer one feed's largest member, so lower the cap for feeds with "
        "very large stop_times.txt",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="log DEBUG-level detail",
    )
    verbosity.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="log warnings and errors only",
    )
    parser.add_argument(
        "--log-file",
        type=pathlib.Path,
        default=None,
        help="also write the log to this file (its directory is created and it "
        "is appended to); with --no-console, log only to the file",
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="do not log to the screen (use with --log-file)",
    )
    arguments = parser.parse_args(argv)
    if arguments.workers < 1:
        parser.error("--workers must be at least 1")
    if not arguments.sources:
        arguments.sources = list(SOURCES)
    return arguments


def stages_from(stage, downstream):
    """The stages one invocation runs: the named one, and — with
    ``downstream`` — every later one in build order, which is the order
    ``STAGES`` lists them in."""
    order = list(STAGES)
    return order[order.index(stage) :] if downstream else [stage]


def _highlights(summaries):
    """A compact ``key=count`` tail for a stage's numeric summary values."""
    parts = [
        f"{key}={value}"
        for summary in summaries
        for key, value in summary.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    ]
    return " - " + ", ".join(parts) if parts else ""


def main(argv=None):
    arguments = parse_args(argv)
    stage = arguments.stage
    try:
        progress.configure(
            progress.resolve_level(arguments.verbose, arguments.quiet),
            log_file=arguments.log_file,
            console=not arguments.no_console,
        )
    except OSError as error:
        progress.fatal(f"{stage}: cannot open log file {arguments.log_file}: {error}")
        return 1
    summaries = []
    try:
        for stage in stages_from(arguments.stage, arguments.downstream):
            log.info("%s: starting", stage)
            started = time.perf_counter()
            stage_summaries = STAGES[stage](arguments)
            summaries.extend(stage_summaries)
            log.info(
                "%s: done in %.1fs%s",
                stage,
                time.perf_counter() - started,
                _highlights(stage_summaries),
            )
    except SystemExit as error:
        # A stage that exits (e.g. a missing golden file) is a fatal build
        # error like any other; report it with the stage prefix and exit 1.
        progress.fatal(f"{stage}: {error}")
        return 1
    except Exception as error:  # noqa: B902
        # Broad on purpose: this is the CLI boundary, where any stage
        # failure should read as one line rather than a traceback. Set
        # TRANSITIO_TRACEBACK to see the original instead.
        if os.environ.get("TRANSITIO_TRACEBACK"):
            raise
        progress.fatal(f"{stage}: {error}")
        return 1
    try:
        for summary in summaries:
            print(json.dumps(summary, indent=2, sort_keys=True))
    except (TypeError, ValueError, OSError) as error:
        # A non-serialisable summary or a broken stdout (e.g. a closed pipe)
        # is a fatal build error too, reported the same controlled way.
        progress.fatal(f"writing the build summary failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
