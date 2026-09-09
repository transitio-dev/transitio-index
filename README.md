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
                       gazetteer, resolve, crawl, expand, coverage, classify,
                       curate, prune, license, publish) and the registry
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

A build is a fixed sequence of stages, run through `python -m
transitio_index.build`. Every stage reads the previous stages' output from a
build cache — `cache/` by default, gitignored — and writes its own output back
into it, so nothing is passed between stages in memory and a build can stop
after any stage and pick up later from the next. Each stage therefore needs the
output its predecessors already left in the cache.

One piece of state lives outside the cache: the place registry
(`overrides/places_registry.jsonl` by default, or `--registry`), which the
`gazetteer` and `expand` stages write and the later stages read. The later
stages refuse to run against a registry that no longer matches the cached
gazetteer run, so resuming or moving a build means keeping the cache and its
matching registry together.

### The stages, in order

1. `ingest` — download and read the three source catalogues: the Transitland
   Atlas archive, the Mobility Database `feeds_v2.csv` and the GBFS
   `systems.csv`.
2. `crosswalk` — match the same feed across the three catalogues into one
   de-duplicated table.
3. `gazetteer` — resolve Overture administrative divisions to Wikidata QIDs,
   seed the cities the feeds declare, attach metros, boundary geometry and
   names, and mint the place registry.
4. `resolve` — settle each feed's identity and whether it is crawlable (from
   the overrides), before anything is fetched.
5. `crawl` — fetch every crawlable feed.
6. `expand` — add the places a feed's crawled stops actually fall in that the
   declared seed missed.
7. `coverage` — derive the membership edges: which places each feed serves.
8. `classify` — give each candidate edge a tier (the feed's service level for
   that place).
9. `curate` — apply the curated edge overrides on top of the classified edges.
10. `prune` — drop the places that no kept edge needs.
11. `license` — record each shipped feed's licence and lineage, and write the
    NOTICE.
12. `publish` — write the shippable `cache/index/`: the GeoParquet tables and
    manifest the reader installs.

### Running the stages

`--stage` names the stage to run. What matters is how much it runs alongside
that stage — and, in particular, that it never runs the stages *before* it:

- **On its own, `--stage X` runs only X**, not its predecessors. The earlier
  stages are prerequisites: their output must already be in the cache. Running a
  stage against a cache that is missing an earlier stage's output fails — it
  does not quietly rebuild the missing input.
- **`--downstream` runs X and every stage after it**, in build order (never the
  ones before it). This carries a build forward from a chosen stage to the end.

So a full build from an empty cache starts at the first stage and runs the whole
pipeline through to `publish`:

```
python -m transitio_index.build --stage ingest --downstream   # ingest -> publish
```

To redo only part of a build, run the earliest stage you need to change with
`--downstream`, reusing the cache the earlier stages already left. For example,
after editing a curated override, re-apply it and rebuild the shipped index
without re-crawling:

```
python -m transitio_index.build --stage curate --downstream   # curate -> publish
```

Running a single stage (no `--downstream`) is for iterating on that one stage
once its inputs are built:

```
python -m transitio_index.build --stage gazetteer
```

Pin the Atlas revision with `--commit <sha>` so the ingest is reproducible.
`--help` lists every flag. The build logs its progress to the screen by default;
`--verbose` / `--quiet`, `--log-file` and `--no-console` control the log.

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
