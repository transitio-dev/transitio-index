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

## Run a build

The build runs as ordered stages that read and write a build cache (`cache/`
by default, gitignored). Each stage consumes the earlier stages' cache files,
so a stage can be rerun on its own once its inputs exist. In build order:

1. `ingest` — read the three catalogues (the Transitland Atlas archive, the
   Mobility Database `feeds_v2.csv`, the GBFS `systems.csv`).
2. `crosswalk` — resolve the same feed across the three into one table.
3. `gazetteer` — resolve Overture divisions to Wikidata QIDs, seed the feed
   cities, attach metros, geometry and names, and mint the place registry.
4. `resolve` — settle each feed's identity and crawlability from the
   overrides, before any crawl.
5. `crawl` — fetch each crawlable feed.
6. `expand` — add the places a feed's crawled stops fall in that the seed
   missed.
7. `coverage` — derive the membership edges: which places each feed serves.
8. `classify` — assign each edge's tier.
9. `curate` — apply the curated edge overrides.
10. `prune` — drop the places no kept edge needs.
11. `license` — record each shipped feed's licence and lineage, and the NOTICE.
12. `publish` — write the shippable `cache/index/` (the GeoParquet tables and
    manifest the reader consumes).

Run one stage, or a stage and every later one, with `--stage` and
`--downstream`; pin the Atlas revision with `--commit`:

```
python -m transitio_index.build --stage gazetteer            # one stage
python -m transitio_index.build --stage ingest --downstream  # ingest -> publish
```

`--help` lists every flag. The build logs its progress to the screen by
default; `--verbose`/`--quiet`, `--log-file` and `--no-console` control it.

### A small sample end to end

`scripts/sample_catalogues.py` fetches the full catalogues and cuts a small,
multi-country sample (Finland and Estonia by default) — enough to run every
stage quickly. It writes trimmed inputs under `cache/sample/` and prints the
exact build command for them, pinned to the Atlas revision it cut at:

```
python scripts/sample_catalogues.py
python -m transitio_index.build --stage ingest --downstream --commit <sha> \
    --archive <sample>/atlas.tar.gz --mdb-csv <sample>/feeds_v2.csv \
    --gbfs-csv <sample>/systems.csv
```

Inspect the result with `scripts/export_index_layer.py`, which reads
`cache/index` and writes an editor-ready GeoParquet + GeoJSON — each place with
its geometry, a `served` flag and the feeds serving it — to `cache/index-layer/`.
`scripts/export_divisions.py` exports the raw Overture hierarchy for chosen
countries straight from public Overture data, for checking the geography before
a build.

### Publish a snapshot

`publish` writes `cache/index/`; releasing it to GitHub is a separate step and
needs a token (`GITHUB_TOKEN` unless `--token-env` says otherwise):

```
GITHUB_TOKEN=... python -m transitio_index.publish_cli --cache-dir cache
```

It creates a draft release, uploads and verifies the assets, then publishes,
and prints the round trip a client would make. The publisher refuses an
unlicensed or lineage-incomplete build.

## Conventions

- Format with black and lint with flake8 (config in `.flake8`), over
  `transitio_index`, `tests` and `scripts`.
- Small, staged pull requests against `main`; each describes what it does and
  how it was verified.
- Regression tests are consolidated in `tests/test_regressions.py` — one test
  per fixed defect.
- The place registry (`overrides/places_registry.jsonl`) is append-only; a CI
  job checks each pull request's registry against the base branch.
