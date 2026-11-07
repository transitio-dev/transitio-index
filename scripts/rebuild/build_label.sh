#!/bin/sh
# build_label.sh <label> <sample-run-dir> — one label of a rebuild, end to
# end: refuse to start over a running build, build ingest→publish from the
# sample cut, archive the published index and map layer on success
# (archive_index.sh), then park the label's crawl cache as
# cache/crawl.<label>.bak so the next label starts from an empty crawl.
# REUSE_CRAWL=1 keeps an existing cache/crawl (a relaunch of the same label).
# A partition cut's countries.txt is passed as --seed-countries, and the Atlas
# commit its partition.json records as --commit (other cuts: the build's
# pinned commit).
# MDB_CATALOGUE, when set, is the whole MDB CSV the cut was taken from, passed
# as --mdb-catalogue; OVERTURE_RELEASE, when set, is passed as
# --overture-release (else the build reads the newest release).
# BUILD_CHECKOUT is the checkout to build in (default: this repository),
# PYTHON its Python (default: python3), LOGDIR where <label>.log goes
# (default: $TMPDIR).
# Exit: the build's code; 2 refused; 3 the build exited 0 without publishing;
# 4 archiving failed; 5 parking the crawl failed; 130/143 interrupted.
set -u
HERE=$(cd "$(dirname "$0")" && pwd) || exit 2
. "$HERE/checks.sh" || exit 2
REPO="${BUILD_CHECKOUT:-$HERE/../..}"
PY="${PYTHON:-python3}"
cd "$REPO" || { echo "refusing: cannot enter $REPO" >&2; exit 2; }
[ "$#" -eq 2 ] || { echo "usage: build_label.sh <label> <sample-run-dir>" >&2; exit 2; }
LABEL="$1"; B="$2"
case "$LABEL" in *[!$LOWER$DIGITS-]*|"") echo "refusing: bad label '$LABEL'" >&2; exit 2;; esac
case "$B" in
  cache/sample/run-*[!$UPPER$LOWER${DIGITS}_]*|cache/sample/run-) echo "refusing: bad run dir '$B'" >&2; exit 2;;
  cache/sample/run-*) ;;
  *) echo "refusing: bad run dir '$B'" >&2; exit 2;;
esac
# Nothing under cache is written or read through a symlink.
if [ -L cache ] || [ -L cache/sample ] || [ -L cache/crawl ] || [ -L "$B" ]; then
  echo "refusing: a symlink under cache" >&2; exit 2
fi
[ -d "$B" ] || { echo "refusing: no such run dir '$B'" >&2; exit 2; }
for f in atlas_sample.tar.gz mdb_sample.csv gbfs_sample.csv; do
  [ -f "$B/$f" ] && [ ! -L "$B/$f" ] || { echo "refusing: $B/$f missing" >&2; exit 2; }
done
# The countries this label owns: their capitals and large cities are seeded.
SEED=""
if [ -e "$B/countries.txt" ] || [ -L "$B/countries.txt" ]; then
  [ -f "$B/countries.txt" ] && [ ! -L "$B/countries.txt" ] || { echo "refusing: $B/countries.txt is not a regular file" >&2; exit 2; }
  while IFS= read -r code || [ -n "$code" ]; do
    [ -n "$code" ] || continue
    case "$code" in
      [$UPPER][$UPPER]) SEED="${SEED:+$SEED,}$code";;
      *) echo "refusing: bad country code '$code' in $B/countries.txt" >&2; exit 2;;
    esac
  done < "$B/countries.txt"
fi
if [ -n "${MDB_CATALOGUE:-}" ]; then
  [ -f "$MDB_CATALOGUE" ] && [ ! -L "$MDB_CATALOGUE" ] || { echo "refusing: MDB_CATALOGUE '$MDB_CATALOGUE' is not a regular file" >&2; exit 2; }
fi
case "${OVERTURE_RELEASE:-}" in
  *[!$DIGITS.-]*) echo "refusing: bad OVERTURE_RELEASE '$OVERTURE_RELEASE'" >&2; exit 2;;
esac
LOG="${LOGDIR:-${TMPDIR:-/tmp}}/$LABEL.log"
[ -L "$LOG" ] && { echo "refusing: $LOG is a symlink" >&2; exit 2; }
command -v lsof >/dev/null || { echo "refusing: cannot inspect the registry lock (no lsof)" >&2; exit 2; }
# One runner at a time, held through the build, the archive and the parking:
# an atomic mkdir lock, released on any exit.
LOCK="cache/.runner.lock"
[ -L "$LOCK" ] && { echo "refusing: $LOCK is a symlink" >&2; exit 2; }
if ! mkdir "$LOCK" 2>/dev/null; then
  # A lock whose recorded runner is gone (killed, crashed) is stale: reclaimed.
  # One without a pid yet may be a runner that has just taken it: asked again.
  holder=$(cat "$LOCK/pid" 2>/dev/null || echo)
  if [ -z "$holder" ]; then
    sleep 2; holder=$(cat "$LOCK/pid" 2>/dev/null || echo)
  fi
  if [ -n "$holder" ] && kill -0 "$holder" 2>/dev/null; then
    echo "refusing: runner $holder holds $LOCK" >&2; exit 2
  fi
  rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null
  mkdir "$LOCK" 2>/dev/null || { echo "refusing: cannot take $LOCK" >&2; exit 2; }
fi
if ! echo $$ > "$LOCK/pid" || [ ! -s "$LOCK/pid" ]; then
  rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null
  echo "refusing: cannot record the owner of $LOCK" >&2; exit 2
fi
trap 'rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null' EXIT
if lsof overrides/places_registry.jsonl.lock 2>/dev/null | grep -qi python; then
  echo "refusing: another build holds the registry" >&2; exit 2
fi
if [ -d cache/crawl ] && [ -z "${REUSE_CRAWL:-}" ]; then
  echo "refusing: cache/crawl exists — park the previous label's crawl first" >&2; exit 2
fi
# The Atlas commit of the partition whose cut this run dir is, when exactly one
# partition lists it (one commit across them); else none.
ATLAS=$("$PY" - "$LABEL" "$B" <<'PYEOF'
import glob, json, sys
label, cut = sys.argv[1:]
commits = set()
for path in glob.glob("cache/sample/partition-*/partition.json"):
    try:
        with open(path, encoding="utf-8") as handle:
            partition = json.load(handle)
        entry = partition["labels"][label]
        if entry["cut"] == cut:
            commits.add(partition["catalogues"]["atlas_commit"])
    except (OSError, ValueError, KeyError, TypeError):
        continue
print(commits.pop() if len(commits) == 1 else "")
PYEOF
) || ATLAS=""
case "$ATLAS" in
  *[!$HEX]*) ATLAS="";;
esac
snapshot_id() { "$PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['snapshot_id'])" cache/index/snapshot.json 2>/dev/null || echo; }
BEFORE=$(snapshot_id)
: > "$LOG" || exit 2
echo "log: $LOG"

# Finalize with the runner's status: archive a published index (the snapshot
# the build printed, so the archiver names and checks that one), park the
# crawl, log the outcome. Also the INT/TERM path, once the build has stopped.
finish() {
  code=$1
  echo "[exited with code $code]" >> "$LOG"
  if [ "$code" -eq 0 ]; then
    # Published means cache/index now names a snapshot it did not before.
    SNAP=$(snapshot_id)
    if [ -z "$SNAP" ] || [ "$SNAP" = "$BEFORE" ]; then
      echo "[build exited 0 without publishing a new snapshot]" >> "$LOG"; code=3
    elif "$HERE/archive_index.sh" "$LABEL" "$SNAP" >> "$LOG" 2>&1; then
      echo "archived $LABEL" >> "$LOG"
    else
      echo "[archiving failed]" >> "$LOG"; code=4
    fi
  fi
  if [ -d cache/crawl ] && [ ! -L cache/crawl ]; then
    if [ -e "cache/crawl.$LABEL.bak" ]; then
      echo "[cache/crawl.$LABEL.bak already exists; crawl left in place]" >> "$LOG"
      [ "$code" -eq 0 ] && code=5
    elif ! mv cache/crawl "cache/crawl.$LABEL.bak" >> "$LOG" 2>&1; then
      echo "[parking the crawl failed]" >> "$LOG"
      [ "$code" -eq 0 ] && code=5
    fi
  fi
  echo "[$LABEL done: runner exit $code]" >> "$LOG"
  exit "$code"
}
trap 'finish 130' INT
trap 'finish 143' TERM
set -- --stage ingest --downstream --no-golden --allow-empty-mdb --workers 4 \
  --archive "$B/atlas_sample.tar.gz" --mdb-csv "$B/mdb_sample.csv" --gbfs-csv "$B/gbfs_sample.csv"
[ -z "$ATLAS" ] || set -- "$@" --commit "$ATLAS"
[ -z "$SEED" ] || set -- "$@" --seed-countries "$SEED"
[ -z "${MDB_CATALOGUE:-}" ] || set -- "$@" --mdb-catalogue "$MDB_CATALOGUE"
[ -z "${OVERTURE_RELEASE:-}" ] || set -- "$@" --overture-release "$OVERTURE_RELEASE"
echo "build: $*" >> "$LOG"
OMP_NUM_THREADS=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="" \
  "$PY" -m transitio_index.build "$@" \
  >> "$LOG" 2>&1
finish $?
