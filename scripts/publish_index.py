"""Publish the built feed index as a GitHub release.

    python scripts/publish_index.py --cache-dir cache

The token comes from the environment (``GITHUB_TOKEN`` unless ``--token-env``
says otherwise) and is sent only to the API and upload hosts. The release is
created as a draft, its assets uploaded and verified, then published; the
summary printed last is the round trip a client would make.
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_index import DEFAULT_CACHE_DIR  # noqa: E402
from index_build import publisher  # noqa: E402
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
        help="where to write the archive, checksum and manifest (default: a "
        "temporary directory)",
    )
    parser.add_argument(
        "--api-url",
        default=contract.API_URL,
        help=f"GitHub API base URL (default: {contract.API_URL})",
    )
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_args(argv)
    token = os.environ.get(arguments.token_env)
    if not token:
        print(f"publish: no token in ${arguments.token_env}", file=sys.stderr)
        return 1
    try:
        if arguments.out_dir is None:
            with tempfile.TemporaryDirectory() as out_dir:
                summary = publisher.publish_index(
                    arguments.cache_dir / "index",
                    repository=arguments.repository,
                    token=token,
                    api_url=arguments.api_url,
                    out_dir=out_dir,
                    cache_dir=arguments.cache_dir,
                    overrides_dir=arguments.overrides_dir,
                )
        else:
            summary = publisher.publish_index(
                arguments.cache_dir / "index",
                repository=arguments.repository,
                token=token,
                api_url=arguments.api_url,
                out_dir=arguments.out_dir,
                cache_dir=arguments.cache_dir,
                overrides_dir=arguments.overrides_dir,
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
