# transitio-index

The feed index build for [transitio](https://github.com/cafein-py/transitio).
It gathers the Mobility Database and Transitland Atlas catalogues (and the
GBFS catalogue, to keep shared-mobility systems apart from the feeds),
crosswalks and de-duplicates their feeds, resolves the places they serve
against Overture divisions and boundary geometry, crawls the feeds for
coverage, classifies each feed's tier per place, ranks each place's feeds, and
publishes a versioned index snapshot as a GitHub release.

transitio's reader (`transitio.index`) installs that snapshot and answers
place and feed queries from it. The reader lives in transitio and is what
users install; this repository is the build behind it, run by maintainers
from a checkout.

## Layout

```
transitio_index/       the build package: the stages (ingest, crosswalk,
                       gazetteer, resolve, crawl, expand, coverage, classify,
                       curate, rank, prune, license, publish, stats), the
                       merge and the registry
transitio_index/build.py              the pipeline entry point
transitio_index/merge.py              merge the per-label builds into one snapshot
transitio_index/publish_cli.py        release the built snapshot
transitio_index/registry_history.py   the CI registry-history guard
scripts/sample_catalogues.py  cut a small multi-place catalogue sample
scripts/export_divisions.py   export the Overture division hierarchy to inspect
scripts/export_index_layer.py export a produced index as a map layer (feeds/tiers)
scripts/index_viewer.py       inspect a built index in the browser
scripts/check_place_geometry.py flag cities whose area disagrees with Wikidata
overrides/             the place registry and curated override files
golden/                the golden feed set the publish stage diffs against
tests/                 the build's pytest suite and its fixtures
```

The entry points are modules, run with `python -m`:
`python -m transitio_index.build`, `python -m transitio_index.merge`,
`python -m transitio_index.publish_cli` and
`python -m transitio_index.registry_history`.

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
2. `crosswalk` — match the same feed across the Transitland Atlas and the
   Mobility Database into one de-duplicated table. A deprecated Mobility
   Database row that redirects to a live row becomes an alias of that feed.
   A Mobility Database row the other matches leave unmatched is paired with
   the unmatched Atlas feed that lists its download URL among its past URLs.
   The GBFS systems go to a table of their own, which the index does not
   publish. The feeds no catalogue lists that `overrides/feeds.yaml` adds
   with `add_feed` join the table in every build whose catalogue feeds
   declare their country; later stages place, crawl and fold them like
   catalogue feeds, and a catalogue feed with the same data keeps its own id.
3. `gazetteer` — resolve Overture administrative divisions to Wikidata QIDs,
   seed the cities the feeds declare, attach metros, boundary geometry, centre
   points and names, and mint the place registry.
4. `resolve` — settle each feed's identity, its access details and whether
   it is crawlable (from the overrides), before anything is fetched. A feed
   that needs a key takes its provider from `overrides/access_providers.yaml`;
   the generation's `access_report.jsonl` lists the protected feeds no
   provider claims and the access curation errors.
5. `crawl` — fetch every crawlable feed, first without credentials. A feed's own
   URL is read first; when that read fails on the producer's side (a dead or
   refused link, a server error, a redirect loop, a file that is not the
   archive or a broken one), the crawl reads the feed's copy hosted by the
   Mobility Database instead. A GTFS feed the catalogues or overrides say
   needs a key is crawled the same way, and then with the maintainer's key
   where its provider approves (see below); one without a URL, or of which
   the crawl reads no copy, is marked uncrawlable,
   "requires authentication". Its access details stay as given either way.
   The crawl records each archive's size, every root file's uncompressed
   size and the `.txt` files holding no data row (a file over 64 KiB counts
   as having rows); a feed crawled before these were recorded is fetched
   again once.
6. `expand` — add the places a feed's crawled stops actually fall in that the
   declared seed missed.
7. `coverage` — derive the membership edges: which places each feed serves.
   A feed whose catalogue name is a note, such as "User registration
   required to download", takes its Mobility Database provider's name.
8. `classify` — give each candidate edge a tier. Tier is a property of routes,
   surfaced per place: the edge carries the tier of the routes serving the
   place, so a national coach stopping once in a town gives that town a
   national edge, and the edge's `service` struct says how much service that
   is. Its evidence also records, per other feed at the place, the share of
   its departures on lines that feed runs too: the same line name and mode
   family, with stops nearby.
9. `curate` — apply the curated edge overrides on top of the classified edges.
10. `rank` — give every edge its relevance: a category from its tier (local
    is primary, regional secondary, national tertiary, international stays
    international), a score within that category, and whether it crosses a
    border. A feed whose service ended more than 30 days before it was
    crawled scores 0, so it lists last in its category.
11. `prune` — drop the places that no kept edge needs.
12. `license` — record each shipped feed's licence and lineage, and write the
    NOTICE.
13. `publish` — write the shippable `cache/index/`: the GeoParquet tables,
    the table of the access providers its feeds name and the manifest the
    reader installs.
14. `stats` — write statistics about the catalogue rows the build saw and the
    feeds they became, for reporting; the shipped index does not depend on
    them.

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
pipeline through `publish` and `stats`:

```
python -m transitio_index.build --stage ingest --downstream   # ingest -> stats
```

To redo only part of a build, run the earliest stage you need to change with
`--downstream`, reusing the cache the earlier stages already left. For example,
after editing an edge override (`overrides/edges.yaml`), re-apply it and
rebuild the shipped index without re-crawling:

```
python -m transitio_index.build --stage curate --downstream   # curate -> stats
```

An `add_feed` entry in `overrides/feeds.yaml` enters at the crosswalk, so a
change to one starts there: `--stage crosswalk --downstream`.

Running a single stage (no `--downstream`) is for iterating on that one stage
once its inputs are built:

```
python -m transitio_index.build --stage gazetteer
```

Pin the Atlas revision with `--commit <sha>` so the ingest is reproducible.
`--help` lists every flag. The build logs its progress to the screen by default;
`--verbose` / `--quiet`, `--log-file` and `--no-console` control the log.

### Crawl key-protected feeds

A provider in `overrides/access_providers.yaml` lends the crawl the
maintainer's key once `crawl_approved: true` lands in a reviewed pull
request. The key goes over https alone, for a URL under the provider's
`url_prefixes`, and to that URL's host alone; an `http://` feed whose
`https://` form lies there is resolved, crawled and published in that form. Each credential field is
`TRANSITIO_KEY_<PROVIDER>__<FIELD>`, else read from the file
`TRANSITIO_INDEX_CREDENTIALS` names, else from transitio's own credentials
file. The crawl log is masked of every secret and says, per key-flagged
feed, how its key was used (`key_crawl`; the summary counts them in
`by_key_crawl`; the build statistics count them per provider). A provider's
optional `crawl_budget` caps its keyed requests per calendar month (UTC),
redirects and retries included, across every run on the machine; the tally is
`key_requests.json` in the user state directory, or the file
`TRANSITIO_INDEX_KEY_USAGE` names. Keyed reads run one at a time, after every
keyless read, in feed order, and a 401 stops a provider's key for the rest of
the run. A keyed read whose provider
no longer approves it, or no longer serves the feed, is removed at the next
crawl. Data read with a key are refreshed with each index release, on a
best-effort basis; there is no separate weekly rebuild. The NOTICE credits
each provider whose feeds' data the index holds, read with its key or from
the hosted copy of a feed bound to it, or from a feed's own URL its
`url_prefixes` claim: the days the data were obtained, the provider's
`notice` (the attribution its terms ask for, in `access_providers.yaml`) and
each feed's licence and URL.

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

### Cut the whole catalogue

`python scripts/sample_catalogues.py --partition` cuts every MDB row and every
Atlas GTFS feed into exactly one label: one per country, a country of more
than `--batch-size` rows (600 by default) split by subdivision, `other` for
rows without a country code or moved there with `--exclude`, and `atlas1..K`
for the Atlas feeds no label picks. A deprecated row stays with the live row
it folds into. Each cut's `countries.txt` names the countries its label owns:
`<cc>`, or `<cc>1` for a split country, and `cities`, a label without feeds,
for the `--country` codes that have no label of their own. The cuts go to
`cache/sample/run-<label>_*`; once all are written, a `partition-*` directory
gets `rebuild_map.txt` (a `<label> <cut>` line per label) and
`partition.json` (each label's cut, countries and digests, and the label of
every GTFS id).

### Inspect a build in the browser

`scripts/index_viewer.py` serves the built index — `cache/index` and every
per-country build under `cache/builds/` — to a page on localhost with a map
of the places. It needs the `viewer` extra:

```
pip install -e ".[viewer]"
python scripts/index_viewer.py --cache cache --open
```

Pick a build in the top bar; the map fits itself to the build's countries and
regions, hovering names a place, clicking opens its details, and from zoom 7
the loaded slice follows the viewport. The first entry, `catalogue`, merges
the newest run of every build under `cache/builds/` (and `cache/index`) into
one view: a feed comes from the newest build carrying it, its edges from that
same build, a place from the build serving it most, and every row names its
source build. Point `--cache` at `~/.cache/transitio-index` to browse the
archived runs that way; a run that does not verify, or holds no feeds, is
listed as skipped rather than replaced by an older run. Assembling the
catalogue over a hundred archived runs takes about ten seconds and several
gigabytes of memory, once per change to the runs. The viewer only reads the parquet and
JSON files a build publishes and verifies their digests before showing them,
so a build that is mid-publish is listed but reported unavailable until the
publish completes.

A city can belong to a metro of each of four definitions, one `metro` row
each, told apart by `source_subtype`: `metropolitan statistical area` (the
US Census areas, through Wikidata), `metropolitan region` (Eurostat's NUTS-3
approximation of a functional urban area), `functional urban area` (the
Eurostat Urban Audit's city plus its commuting zone) and `city-region (FAO)`
(the FAO one-hour city-regions, worldwide). The `Areas` boxes in the top bar
pick the definitions whose metros the map, the places table and the search
show; a city's details list every metro it is in.

### Merge the builds into one snapshot

The index is built one label at a time and each build is archived under
`~/.cache/transitio-index/builds/<label>-<snapshot>/index/`. The merge takes
the newest complete run of every label and writes one snapshot from them:

```
python -m transitio_index.merge --builds ~/.cache/transitio-index/builds --cache-dir cache/merged \
    --partition cache/sample/partition-<id>/partition.json
```

Every source must be a licensed schema-11 build, and the sources must agree on
the Overture release, the simplification tolerance and the classifier. A feed
comes from the newest build carrying it and its edges from that same build; a
place comes from the build serving it most, with the largest population any
build records for it and its service and validity summed over the merged
edges. The access providers are the union of the sources',
which must give a provider the same fields wherever they list it, and the
build a merged feed comes from must list the provider the feed names. The
merged NOTICE credits every source once, names the catalogues by the digests
each build read, and recounts the feed licences. The snapshot is read back
through the reader before it is
committed into `cache/merged/index/`, and its manifest records each source by
label, build id and digests, so the publisher can check the lineage. A label
whose newest run is incomplete or holds no feeds is skipped and reported; an
older run never stands in for it.

With `--partition`, the merge looks up every MDB and Atlas GTFS id of the cut,
and every `add_feed` id of `overrides/feeds.yaml` (`--overrides-dir`), among
the merged feed ids and aliases, logs each one missing, each label not merged,
outside the partition or built from another cut, and records the result in the
manifest. The publisher refuses a merged snapshot without that check, with a
label outside the partition or from another cut, checked against other
`add_feed` entries than the committed ones, or with a missing id that
`overrides/catalogue_exceptions.yaml` does not list (entries of `feed` and
`reason`, optionally `author` and `date`).

### Publish a snapshot

`publish` writes `cache/index/`; releasing it to GitHub is a separate step and
needs a token (`GITHUB_TOKEN` unless `--token-env` says otherwise):

```
GITHUB_TOKEN=... python -m transitio_index.publish_cli --cache-dir cache
```

It creates a draft release, uploads and verifies the assets, then publishes,
and prints the round trip a client would make. The publisher refuses an
unlicensed or lineage-incomplete build. A merged snapshot is released the same
way from its own cache (`--cache-dir cache/merged`); its lineage is checked
against the archived builds (`--builds-dir`, the merge's `--builds`), and a
merge that is no longer of the newest archive of every label is refused.

## Conventions

- Format with black and lint with flake8 (config in `.flake8`), over
  `transitio_index`, `tests` and `scripts`.
- Small, staged pull requests against `main`; each describes what it does and
  how it was verified.
- Regression tests are consolidated in `tests/test_regressions.py` — one test
  per fixed defect.
- The place registry (`overrides/places_registry.jsonl`) is append-only; a CI
  job checks each pull request's registry against the base branch.
