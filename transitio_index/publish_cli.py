"""Publish the built feed index as a GitHub release.

    python -m transitio_index.publish_cli --cache-dir cache

The token comes from the environment (``GITHUB_TOKEN`` unless ``--token-env``
says otherwise) and is sent only to the API and upload hosts. The release
report is printed to stderr first: the index against the previous release
(``--previous``, by default the newest snapshot transitio has installed, so
run ``transitio.index.refresh()`` first) and transitio's reference checks on
both (``--reference-tests``, a transitio checkout's ``tests`` directory, by
default ``$TRANSITIO_TESTS``). A reference check that passed on the previous
release and fails on this one stops the release unless
``--accept-regressions``. The release is created as a draft, its assets
uploaded and verified, then published; the summary printed last is the
round trip a client would make.
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from transitio_index.build import DEFAULT_CACHE_DIR  # noqa: E402
from transitio_index.merge import DEFAULT_BUILDS_DIR  # noqa: E402
from transitio_index import publisher, release_report  # noqa: E402
from transitio.index import release as contract  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"build cache whose index/ is published (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--overrides-dir",
        type=Path,
        default=Path("overrides"),
        help="directory of override YAML files the build applied (default: overrides)",
    )
    parser.add_argument(
        "--builds-dir",
        type=Path,
        default=DEFAULT_BUILDS_DIR,
        help="the archived builds a merged index is checked against "
        f"(default: {DEFAULT_BUILDS_DIR})",
    )
    parser.add_argument(
        "--repository",
        default=contract.DEFAULT_REPOSITORY,
        help=f"GitHub owner/name to release under (default: {contract.DEFAULT_REPOSITORY})",
    )
    parser.add_argument(
        "--token-env",
        default="GITHUB_TOKEN",
        help="environment variable holding the token (default: GITHUB_TOKEN)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="where to write the release assets: the archive, its checksum, the "
        "on-demand files and the manifest (default: a temporary directory)",
    )
    parser.add_argument(
        "--api-url",
        default=contract.API_URL,
        help=f"GitHub API base URL (default: {contract.API_URL})",
    )
    parser.add_argument(
        "--previous",
        type=Path,
        default=None,
        help="the previous release's installed snapshot directory the report "
        "compares with (default: the newest snapshot transitio has installed)",
    )
    parser.add_argument(
        "--reference-tests",
        type=Path,
        default=os.environ.get("TRANSITIO_TESTS"),
        help="a transitio checkout's tests directory, whose reference checks the "
        "report runs on both indexes (default: $TRANSITIO_TESTS)",
    )
    parser.add_argument(
        "--accept-regressions",
        action="store_true",
        help="publish although reference checks that passed on the previous "
        "release fail on this one",
    )
    return parser.parse_args(argv)


def _previous(snapshot_id):
    """The newest snapshot transitio has installed other than
    ``snapshot_id``, or None."""
    from transitio.index import _refresh

    for found, _ in _refresh.installed():
        if found != snapshot_id:
            return _refresh._snapshots(_refresh.cache_root()) / found
    return None


def main(argv=None):
    arguments = parse_args(argv)
    token = os.environ.get(arguments.token_env)
    if not token:
        print(f"publish: no token in ${arguments.token_env}", file=sys.stderr)
        return 1
    if arguments.reference_tests is None:
        print(
            "publish: name transitio's tests directory with --reference-tests or "
            "$TRANSITIO_TESTS",
            file=sys.stderr,
        )
        return 1
    index_dir = arguments.cache_dir / "index"
    try:
        snapshot_id = publisher._peek(index_dir).get("snapshot_id")
        report = release_report.build_report(
            index_dir,
            previous_dir=arguments.previous or _previous(snapshot_id),
            tests_dir=arguments.reference_tests,
            withhold=(arguments.token_env,),
        )
        print(release_report.render(report), file=sys.stderr)
        options = dict(
            repository=arguments.repository,
            token=token,
            api_url=arguments.api_url,
            cache_dir=arguments.cache_dir,
            overrides_dir=arguments.overrides_dir,
            builds_dir=arguments.builds_dir,
            report=report,
            accept_regressions=arguments.accept_regressions,
        )
        if arguments.out_dir is None:
            with tempfile.TemporaryDirectory() as out_dir:
                summary = publisher.publish_index(index_dir, out_dir=out_dir, **options)
        else:
            summary = publisher.publish_index(
                index_dir, out_dir=arguments.out_dir, **options
            )
    except Exception as error:  # noqa: B902 - the CLI boundary reads as one line
        if os.environ.get("TRANSITIO_TRACEBACK"):
            raise
        print(f"publish: {error}", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
