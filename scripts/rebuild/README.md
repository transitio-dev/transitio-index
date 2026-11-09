# Rebuild the whole index

A full rebuild builds every label of a partition of the catalogues again from
the current `main`, with a fresh crawl, then merges the label builds into one
snapshot and releases it. The scripts here run it from a terminal of the build
checkout; a rebuild of about a hundred labels takes about a day when the
labels are built one at a time.

## Prepare a run

1. Cut the catalogues into labels with
   `python scripts/sample_catalogues.py --partition` (see the main README).
   The cut is `cache/sample/partition-<id>/`, with its `rebuild_map.txt`.
2. Make the run's folder, `~/.cache/transitio-index/runs/<run>/` (`RUN_DIR`
   names another), with three files:
   - `partition`: one line, the cut (`cache/sample/partition-<id>`);
   - `map-check.txt`: one line of the cut's `rebuild_map.txt`, the label
     built first to check the code;
   - `map-rest.txt`: every other line of `rebuild_map.txt`.
3. Set `PYTHON` to the build environment's Python (default `python3`) and
   `MDB_CATALOGUE` to the absolute path of the whole MDB catalogue the cut was
   taken from. `READER`, a pip requirement such as `transitio==0.29.0`, is
   installed by `prepare`; `ARCHIVE_ROOT` (default
   `~/.cache/transitio-index/builds-<run>`) and `MIN_FREE_GB` (default 50) are
   optional.
4. Optionally set `SLOTS` (1 to 9, default 1): `rest`, `resume` and `fix`
   then build that many labels at a time, each in a cache folder of its own
   (`cache/slot-<n>`), taking turns on the place registry
   (`--wait-for-registry`); `first` always builds its label alone. `WORKERS`
   sets each crawl's threads (default 4 with one slot, 2 with more, so a
   feed host that several countries share is asked less often at once).
   Each slot needs the memory and disk of one label's build.

## Run it

```
R=scripts/rebuild
$R/rebuild.sh <run> prepare      # pull main, install READER, run every check
$R/rebuild.sh <run> first        # build the check label in the background
$R/rebuild.sh <run> rest         # after the check label is checked: the others
$R/rebuild.sh <run> resume       # build the labels not archived yet
$R/release.sh <run> merge        # merge the newest build of every label
$R/release.sh <run> publish      # after the merge log is checked: release it
```

`$R/rebuild.sh <run> check` runs the checks alone and writes nothing.

`first` backs up the place registry once (`places_registry.before.jsonl` in
the run folder), copies the MDB catalogue into the run's logs and records in
`started` the commit, the archive root, the catalogue's and the maps'
digests and the Overture release. It stops if the check label fails.

Every label reads one Overture release: the newest the bucket holds when
`first` starts. `rest` and `resume` reuse it; Overture keeps only recent
releases, and if the recorded one is removed before the labels are built,
they refuse, and the rebuild starts over with `first` and an empty archive
root. The release's divisions tables are downloaded once into
`~/.cache/transitio-index/overture/<release>/`.

When a fix merged to `main` must reach some labels, list their lines of the
maps in `map-fix.txt` and run `$R/rebuild.sh <run> fix`. The first `fix`
pulls `main`, which must descend from the first run's commit, backs up the
registry to `places_registry.before-fix.jsonl` and records the commit, the
map's digest and the start time in `fixed`. Run again, it keeps that commit
and rebuilds the labels of the map whose newest archive is older than the
fix. From then on `rest` and `resume` refuse; the merge and the release
accept the fix's commit once every label of the fix map is rebuilt at it.

The merge writes `~/.cache/transitio-index/merged-<run>/` (`MERGED` names
another) and refuses a label without an archived build unless
`ALLOW_MISSING=1`. Publishing needs `TRANSITIO_TESTS`, a transitio checkout's
`tests` folder, and `TOKEN_HELPER`, a command that prints a GitHub token
allowed to make releases. It installs the current release, writes the release
report with transitio's reference checks on both indexes, and refuses a check
that passed before and fails now unless `ACCEPT_REGRESSIONS=1`; the report
still lists those checks.

## While it runs

- Do not change the build checkout: every label reads it.
- Keep the machine on power. On macOS the driver keeps it awake while the run
  goes, but not with the lid closed on battery.
- Progress: `tail -f ~/.cache/transitio-index/runs/<run>/logs/progress.log`;
  one label's log is `logs/<label>.log` beside it.
- A status table (labels done and failed, places and feeds archived, free
  disk, the labels being built and an estimate of the time left):
  `${PYTHON:-python3} $R/status.py --run <run>`. With `--previous-archive` naming an
  earlier rebuild's archive root, the estimate uses its place counts for the
  labels not built yet. `$R/watch.sh <run>` prints the table every five
  minutes (`INTERVAL` seconds) and stops once the `rest`, `resume` or `fix`
  run has ended.
- Stop after the labels being built with `kill <pid>`: the driver prints the
  pid, which is also in `cache/.rebuild.lock/pid`. A `REBUILD ENDED` line
  then closes the progress log; continue later with `resume`.

## The run folder

| File | What it is |
| --- | --- |
| `partition`, `map-check.txt`, `map-rest.txt` | the cut and its labels, written by hand |
| `map-fix.txt` | the labels a fix rebuilds, written by hand |
| `started`, `fixed` | the first run's and the fix's records |
| `places_registry.before.jsonl`, `places_registry.before-fix.jsonl` | the registry as each started |
| `map-resume-*.txt`, `map-fix-*.txt` | the labels each resume and fix run built |
| `rebuild.out` | the background run's own output |
| `logs/` | `progress.log`, one log per label, the MDB catalogue copy, the merge and publish logs and the release assets |
