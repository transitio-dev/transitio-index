# transitio-index

The feed index build for [transitio](https://github.com/cafein-py/transitio).
It gathers the Mobility Database, Transitland Atlas and GBFS catalogues,
crosswalks and de-duplicates their feeds, resolves the places they serve
against Overture divisions and boundary geometry, crawls the feeds for
coverage, classifies each feed's tier per place, and publishes a versioned
index snapshot as a GitHub release.

transitio's reader (`transitio.index`) installs that snapshot and answers
place and feed queries from it. The reader lives in transitio and is what
users install; this repository is the build behind it, run by maintainers
from a checkout.

## Layout

```
transitio_index/       the build package: the stages (ingest, crosswalk,
                       gazetteer, crawl, coverage, classify, curate, prune,
                       license, publish) and the registry
transitio_index/build.py              the pipeline entry point
transitio_index/publish_cli.py        release the built snapshot
transitio_index/registry_history.py   the CI registry-history guard
scripts/sample_catalogues.py  cut a small multi-place catalogue sample
scripts/export_divisions.py   export the Overture division hierarchy to inspect
scripts/export_index_layer.py export a produced index as a map layer (feeds/tiers)
overrides/             the place registry and curated override files
golden/                the golden feed set the publish stage diffs against
tests/                 the build's pytest suite and its fixtures
```

The entry points are modules, run with `python -m`:
`python -m transitio_index.build`, `python -m transitio_index.publish_cli`
and `python -m transitio_index.registry_history`.

## Develop

Requires Python >= 3.10. The build reads transitio's release contract, schema
floors and fingerprint, so it depends on the released reader alongside its own
libraries.

This build tool is not published to PyPI: users install transitio's reader, and
maintainers install this to produce the snapshots it reads. Get it either way:

- From a clone, editable, to work on the build. Its dependencies pull in the
  reader and the build's own libraries; then run the suite:

  ```
  pip install -e ".[test]"
  pytest
  ```

- Pinned, to run a released build without a working copy:

  ```
  pip install "git+https://github.com/transitio-dev/transitio-index@<tag>"
  ```

The publisher test imports the shared index fixture from transitio; point
`TRANSITIO_TESTS` at a transitio checkout's `tests` directory to run it.

- Format with black and lint with flake8 (config in `.flake8`), over
  `transitio_index`, `tests` and `scripts`.
- Small, staged pull requests against `main`; each describes what it does and
  how it was verified.
- Regression tests are consolidated in `tests/test_regressions.py` — one test
  per fixed defect.
- The place registry (`overrides/places_registry.jsonl`) is append-only; a CI
  job checks each pull request's registry against the base branch.
