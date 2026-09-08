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
scripts/index_build/   the build stages (ingest, crosswalk, gazetteer,
                       crawl, coverage, classify, curate, prune, license,
                       publish) and the registry
scripts/build_index.py the pipeline entry point
scripts/publish_index.py       release the built snapshot
scripts/check_registry_history.py  the CI registry-history guard
overrides/             the place registry and curated override files
golden/                the golden feed set the publish stage diffs against
tests/                 the build's pytest suite and its fixtures
```

## Develop

Requires Python >= 3.10. The build reads transitio's release contract, schema
floors and fingerprint, so it depends on the released reader alongside its own
libraries:

```
pip install "transitio>=0.11" geopandas httpx openpyxl pandas pyarrow pyyaml shapely pytest
PYTHONPATH=scripts pytest
```

The publisher test imports the shared index fixture from transitio; point
`TRANSITIO_TESTS` at a transitio checkout's `tests` directory to run it.

- Format with black and lint with flake8 (config in `.flake8`), over
  `scripts` and `tests`.
- Small, staged pull requests against `main`; each describes what it does and
  how it was verified.
- Regression tests are consolidated in `tests/test_regressions.py` — one test
  per fixed defect.
- The place registry (`overrides/places_registry.jsonl`) is append-only; a CI
  job checks each pull request's registry against the base branch.
