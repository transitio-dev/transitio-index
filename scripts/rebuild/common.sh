# common.sh — settings and checks shared by rebuild.sh and release.sh, which
# source it after setting HERE (their folder) and RUN (the run's name). Not
# run on its own.
#
# A run keeps its files in RUN_DIR (default ~/.cache/transitio-index/runs/<run>):
# `partition` names the cut (cache/sample/partition-<id>), `map-check.txt`
# holds its check label and `map-rest.txt` the other labels (together the
# cut's rebuild_map.txt), `map-fix.txt` the labels a fix rebuilds. The scripts
# write the run's records, logs and registry backups beside them. The lock is
# the checkout's, whatever the run: one rebuild, merge or release at a time.
. "$HERE/checks.sh" || { echo "refusing: cannot read checks.sh" >&2; exit 2; }
REPO="$(cd "${BUILD_CHECKOUT:-$HERE/../..}" && pwd)" || { echo "refusing: no build checkout" >&2; exit 2; }
export BUILD_CHECKOUT="$REPO"
PY="${PYTHON:-python3}"
case "$RUN" in *[!$LOWER$DIGITS-]*|"") echo "refusing: bad run name '$RUN'" >&2; exit 2;; esac
RUN_DIR="${RUN_DIR:-$HOME/.cache/transitio-index/runs/$RUN}"
plain_path "$RUN_DIR" || { echo "refusing: RUN_DIR must be a plain absolute path" >&2; exit 2; }
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$HOME/.cache/transitio-index/builds-$RUN}"
STATE="$RUN_DIR/partition"
CHECK_MAP="$RUN_DIR/map-check.txt"
REST_MAP="$RUN_DIR/map-rest.txt"
STARTED="$RUN_DIR/started"
# The run's logs, always in its folder (a LOGDIR in the environment is not read).
LOGDIR="$RUN_DIR/logs"
# The run's own copy of the MDB catalogue, taken by the first run.
RUN_CATALOGUE="$LOGDIR/mdb_catalogue.csv"
LOCK="$REPO/cache/.rebuild.lock"
PROG="$LOGDIR/progress.log"
BACKUP="$RUN_DIR/places_registry.before.jsonl"
# The fix: labels rebuilt at a later commit of main than the first run's (the
# map lists them), recorded when the fix run starts — its commit, the map's
# SHA-256 and its start time (UTC) — with the registry as it was then.
FIX_MAP="$RUN_DIR/map-fix.txt"
FIXED="$RUN_DIR/fixed"
FIX_BACKUP="$RUN_DIR/places_registry.before-fix.jsonl"

stamp() { date '+%m-%d %H:%M:%S'; }
say() { echo "[$(stamp)] $*"; }
refuse() { echo "refusing: $*" >&2; exit 2; }
# The lines that hold more than blanks (of the files named, else of stdin),
# and how many there are: what the map readers read.
nonblank_lines() { awk 'NF' "$@"; }
count_nonblank() { awk 'NF { n++ } END { print n + 0 }' "$@"; }


# A path the scripts write: absent, or a regular file that is not a symlink.
writable_file() {
  [ ! -L "$1" ] || refuse "$1 is a symlink"
  [ ! -e "$1" ] || [ -f "$1" ] || refuse "$1 is not a regular file"
}

# Write $2 from $1 through a new temporary file beside it, never through a
# symlink.
copy_into() {
  local partial
  writable_file "$2"
  partial=$(mktemp "$2.XXXXXX") || refuse "cannot write $2"
  cp -p "$1" "$partial" && mv "$partial" "$2" || { rm -f "$partial"; refuse "cannot write $2"; }
  [ -f "$2" ] && [ ! -L "$2" ] || refuse "cannot write $2"
}

# The run's log directory, a real directory.
check_logdir() {
  [ ! -L "$LOGDIR" ] || refuse "$LOGDIR is a symlink"
  mkdir -p "$LOGDIR" || refuse "cannot create $LOGDIR"
  [ -d "$LOGDIR" ] && [ ! -L "$LOGDIR" ] || refuse "$LOGDIR is not a directory"
}

# The lock every state-changing step holds (rebuild starts, merge, publish): a
# directory with the owner's pid; only a lock whose recorded owner is gone is
# taken over. Released on exit unless the caller hands it on.
take_lock() {
  local holder
  [ ! -L "$LOCK" ] || refuse "$LOCK is a symlink"
  [ ! -L "$REPO/cache" ] && mkdir -p "$REPO/cache" || refuse "cannot create $REPO/cache"
  if ! mkdir "$LOCK" 2>/dev/null; then
    [ -d "$LOCK" ] && [ ! -L "$LOCK" ] || refuse "$LOCK is not a directory"
    holder=$(cat "$LOCK/pid" 2>/dev/null)
    if [ -n "$holder" ] && kill -0 "$holder" 2>/dev/null; then
      refuse "the rebuild, a merge or a release is running (pid $holder)"
    fi
    # One step at a time takes a stale lock over.
    mkdir "$LOCK.recover" 2>/dev/null || refuse "another step is taking $LOCK over (remove $LOCK.recover if none runs)"
    sleep 2
    if [ "$(cat "$LOCK/pid" 2>/dev/null)" != "$holder" ]; then
      rmdir "$LOCK.recover"; refuse "another step took $LOCK"
    fi
    rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null
    if ! mkdir "$LOCK" 2>/dev/null; then
      rmdir "$LOCK.recover"; refuse "cannot take $LOCK"
    fi
    rmdir "$LOCK.recover"
  fi
  if ! echo $$ > "$LOCK/pid"; then
    rmdir "$LOCK" 2>/dev/null; refuse "cannot record the owner of $LOCK"
  fi
  trap 'rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null' EXIT
}

# Sets INPUTS_DIGEST, the SHA-256 of the inputs every label reads besides the
# catalogue: the cut's partition.json and rebuild_map.txt and the two maps
# (after check_cut). Called in the script's own shell, so a refusal stops it.
set_inputs_digest() {
  local file sum
  for file in "$PART/partition.json" "$PART/rebuild_map.txt" "$CHECK_MAP" "$REST_MAP"; do
    [ -f "$file" ] && [ -r "$file" ] || refuse "cannot read $file"
  done
  sum=$(set -o pipefail; cat "$PART/partition.json" "$PART/rebuild_map.txt" "$CHECK_MAP" "$REST_MAP" | shasum -a 256 | cut -d' ' -f1) \
    || refuse "cannot hash the cut and the maps"
  case "$sum" in *[!$HEX]*|"") refuse "cannot hash the cut and the maps";; esac
  [ "${#sum}" -eq 64 ] || refuse "cannot hash the cut and the maps"
  INPUTS_DIGEST=$sum
}

# The checkout has no tracked change besides the place registry, which the
# builds append to; a failing git status refuses.
clean_checkout() {
  local status changed
  status=$(git -C "$REPO" status --porcelain --untracked-files=no) || refuse "git status failed in $REPO"
  changed=$(printf '%s\n' "$status" | grep -v '^ M overrides/places_registry\.jsonl$' | grep .)
  [ -z "$changed" ] || refuse "tracked changes besides the place registry: $changed"
}

# No rebuild, merge, release or build of any harness runs, and no crawl is
# left unparked.
no_running_build() {
  local running code
  if [ -f "$LOCK/pid" ] && kill -0 "$(cat "$LOCK/pid")" 2>/dev/null; then
    refuse "the rebuild, a merge or a release is running (pid $(cat "$LOCK/pid"))"
  fi
  if [ -f "$REPO/cache/.runner.lock/pid" ] && kill -0 "$(cat "$REPO/cache/.runner.lock/pid")" 2>/dev/null; then
    refuse "a build is running (pid $(cat "$REPO/cache/.runner.lock/pid"))"
  fi
  command -v lsof > /dev/null || refuse "cannot inspect the registry lock (no lsof)"
  if lsof "$REPO/overrides/places_registry.jsonl.lock" 2>/dev/null | grep -qi python; then
    refuse "another build holds the registry"
  fi
  running=$(pgrep -f -- '-m transitio_index\.(build|merge|publish_cli)|scripts/rebuild/build_labels\.sh')
  code=$?
  [ "$code" -le 1 ] || refuse "cannot list the running processes (pgrep exit $code)"
  [ -z "$running" ] || refuse "build processes are running: $(echo $running)"
  [ ! -e "$REPO/cache/crawl" ] || refuse "$REPO/cache/crawl exists (an unparked crawl): park or remove it first"
}

# The run's cut: sets PART and CUT_MDB_SHA (its MDB catalogue's SHA-256),
# from the checkout.
check_cut() {
  [ -f "$STATE" ] && [ ! -L "$STATE" ] || refuse "no cut recorded in $STATE"
  PART=$(cat "$STATE")
  case "$PART" in
    cache/sample/partition-*[!$UPPER$LOWER${DIGITS}_]*|cache/sample/partition-) refuse "bad cut '$PART'";;
    cache/sample/partition-*) ;;
    *) refuse "bad cut '$PART'";;
  esac
  [ -d "$PART" ] && [ ! -L "$PART" ] && [ -f "$PART/rebuild_map.txt" ] && [ -f "$PART/partition.json" ] \
    && [ ! -L "$PART/rebuild_map.txt" ] && [ ! -L "$PART/partition.json" ] \
    || refuse "the cut $PART is not a partition directory"
  CUT_MDB_SHA=$(PYTHONPATH="" "$PY" -c 'import json, sys; print(json.load(open(sys.argv[1]))["catalogues"]["mdb_sha256"])' "$PART/partition.json") \
    || refuse "cannot read the cut's MDB digest from $PART/partition.json"
  case "$CUT_MDB_SHA" in *[!$HEX]*|"") refuse "the cut records no MDB digest";; esac
  [ "${#CUT_MDB_SHA}" -eq 64 ] || refuse "the cut records no MDB digest"
}

# The SHA-256 of a regular file (not a symlink) that must be the cut's MDB
# catalogue.
check_catalogue() {
  local digest
  [ -f "$1" ] && [ ! -L "$1" ] || refuse "no MDB catalogue at $1"
  digest=$(shasum -a 256 "$1" | cut -d' ' -f1) || refuse "cannot read $1"
  [ "$digest" = "$CUT_MDB_SHA" ] || refuse "$1 is not the MDB catalogue the cut was taken from"
}

# The two maps hold every label of the cut exactly once with its own run dir,
# the check map one label; sets CHECK_LABEL and ENTRIES (both maps' lines).
check_maps() {
  local map maps same label run extra f
  for map in "$CHECK_MAP" "$REST_MAP"; do
    [ -f "$map" ] && [ ! -L "$map" ] || refuse "no map $map"
  done
  ENTRIES=$(nonblank_lines "$CHECK_MAP" "$REST_MAP") || refuse "cannot read the maps"
  maps=$(mktemp) || refuse "cannot create a temporary file"
  printf '%s\n' "$ENTRIES" | nonblank_lines | sort > "$maps"
  nonblank_lines "$PART/rebuild_map.txt" | sort | cmp -s - "$maps"
  same=$?
  rm -f "$maps"
  [ "$same" -eq 0 ] || refuse "the two maps and $PART/rebuild_map.txt list different labels or run dirs"
  [ "$(count_nonblank "$CHECK_MAP")" -eq 1 ] || refuse "$CHECK_MAP must hold exactly one label"
  CHECK_LABEL=$(awk 'NF { print $1; exit }' "$CHECK_MAP")
  while read -r label run extra; do
    [ -n "$label" ] || continue
    [ -z "$extra" ] || refuse "bad map line: $label $run $extra"
    case "$label" in *[!$LOWER$DIGITS-]*) refuse "bad label '$label'";; esac
    case "$run" in
      cache/sample/run-"$label"_*[!$UPPER$LOWER${DIGITS}_]*) refuse "bad run dir '$run' for $label";;
      cache/sample/run-"$label"_?*) ;;
      *) refuse "bad run dir '$run' for $label";;
    esac
    [ -d "$run" ] && [ ! -L "$run" ] || refuse "$run is not a directory"
    for f in atlas_sample.tar.gz mdb_sample.csv gbfs_sample.csv countries.txt; do
      [ -f "$run/$f" ] && [ ! -L "$run/$f" ] || refuse "$run/$f missing or a symlink"
    done
  done < <(printf "%s\n" "$ENTRIES")
}

# ARCHIVE_ROOT and its feeds-only root are absolute, and absent or real
# directories.
check_archive_root() {
  local root
  plain_path "$ARCHIVE_ROOT" || refuse "ARCHIVE_ROOT must be a plain absolute path: printable ASCII, no trailing slash, no . or .. components"
  for root in "$ARCHIVE_ROOT" "$ARCHIVE_ROOT-feeds-only"; do
    [ ! -L "$root" ] || refuse "$root is a symlink"
    [ ! -e "$root" ] || [ -d "$root" ] || refuse "$root is not a directory"
  done
}

# The Overture releases the bucket holds a division table for, oldest first,
# one per line, as the checkout's build lists them (from $REPO).
overture_releases() {
  PYTHONPATH="" "$PY" -c 'from transitio_index import overture; print("\n".join(overture.releases()))'
}

# True for an Overture release name: date, dot, that day's release number.
release_name() {
  case "$1" in *$'\n'*) return 1;; esac
  printf '%s\n' "$1" | grep -Eqx '[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]+'
}

# True when <label> has an archive: a real directory <root>/<label>-<16 hex>
# holding a regular index/snapshot.json, nothing on the way a symlink.
archived() {
  local d suffix
  for d in "$ARCHIVE_ROOT/$1"-* "$ARCHIVE_ROOT-feeds-only/$1"-*; do
    suffix="${d##*/$1-}"
    case "$suffix" in *[!$HEX]*) continue;; esac
    [ "${#suffix}" -eq 16 ] || continue
    [ -d "$d" ] && [ ! -L "$d" ] && [ -d "$d/index" ] && [ ! -L "$d/index" ] \
      && [ -f "$d/index/snapshot.json" ] && [ ! -L "$d/index/snapshot.json" ] && return 0
  done
  return 1
}

# The labels of both maps without an archive, one per line (after check_maps).
missing_labels() {
  local label run
  while read -r label run; do
    [ -n "$label" ] || continue
    archived "$label" || echo "$label $run"
  done < <(printf "%s\n" "$ENTRIES")
}

# The fix map: a regular file whose lines are lines of the two maps, each
# label once, at least one (after check_maps).
check_fix_map() {
  local label run extra
  [ -f "$FIX_MAP" ] && [ ! -L "$FIX_MAP" ] || refuse "no fix map $FIX_MAP"
  [ "$(count_nonblank "$FIX_MAP")" -ge 1 ] || refuse "$FIX_MAP lists no label"
  [ -z "$(awk 'NF { print $1 }' "$FIX_MAP" | sort | uniq -d)" ] || refuse "$FIX_MAP lists a label twice"
  while read -r label run extra || [ -n "$label" ]; do
    [ -n "$label" ] || continue
    [ -z "$extra" ] || refuse "bad fix map line: $label $run $extra"
    printf '%s\n' "$ENTRIES" | grep -qxF -- "$label $run" || refuse "$label $run of $FIX_MAP is not a line of the two maps"
  done < "$FIX_MAP"
}

# The SHA-256 of the fix map.
fix_map_digest() {
  local sum
  sum=$(shasum -a 256 "$FIX_MAP" | cut -d' ' -f1) || refuse "cannot hash $FIX_MAP"
  case "$sum" in *[!$HEX]*|"") refuse "cannot hash $FIX_MAP";; esac
  echo "$sum"
}

# The lines of the fix map whose label's newest archive was built before the
# fix run started (a label with no archive too), one per line.
unfixed_labels() {
  PYTHONPATH="" "$PY" - "$FIX_MAP" "$(sed -n 3p "$FIXED")" "$ARCHIVE_ROOT" <<'PYEOF'
import datetime as dt
import json
import re
import sys
from pathlib import Path

fix_map, started, root = sys.argv[1], dt.datetime.fromisoformat(sys.argv[2]), sys.argv[3]
for line in open(fix_map, encoding="utf-8"):
    if not line.split():
        continue
    label = line.split()[0]
    newest = None
    for base in (Path(root), Path(f"{root}-feeds-only")):
        for d in base.glob(f"{label}-*"):
            if d.is_symlink() or not re.fullmatch(rf"{re.escape(label)}-[0-9a-f]{{16}}", d.name):
                continue
            index, snapshot = d / "index", d / "index" / "snapshot.json"
            if index.is_symlink() or snapshot.is_symlink() or not snapshot.is_file():
                continue
            built = dt.datetime.fromisoformat(json.load(open(snapshot, encoding="utf-8"))["built_at"])
            newest = built if newest is None or built > newest else newest
    if newest is None or newest < started:
        print(line.rstrip("\n"))
PYEOF
}

# The checkout holds the code every label's newest build used: the first
# run's commit, or, once a fix has started, the fix's, with every label of the
# fix map rebuilt at it (after check_maps and check_archive_root).
check_built_code() {
  local head started unfixed
  head=$(git -C "$REPO" rev-parse HEAD) || refuse "cannot read HEAD in $REPO"
  started=$(sed -n 1p "$STARTED")
  if [ ! -e "$FIXED" ] && [ ! -L "$FIXED" ]; then
    [ "$head" = "$started" ] || refuse "the checkout is at ${head:0:7}, the labels were built at ${started:0:7}"
    return 0
  fi
  [ -f "$FIXED" ] && [ ! -L "$FIXED" ] || refuse "$FIXED is not a regular file"
  [ "$head" = "$(sed -n 1p "$FIXED")" ] \
    || refuse "the checkout is at ${head:0:7}; the labels were built at ${started:0:7}, the fix at $(sed -n 1p "$FIXED" | cut -c1-7)"
  check_fix_map
  [ "$(fix_map_digest)" = "$(sed -n 2p "$FIXED")" ] || refuse "$FIX_MAP changed since the fix run started"
  unfixed=$(set -o pipefail; unfixed_labels | awk '{ printf "%s ", $1 }') || refuse "cannot list the labels the fix has not rebuilt"
  [ -z "$unfixed" ] || refuse "labels of the fix not rebuilt at ${head:0:7}: ${unfixed}(run '$HERE/rebuild.sh $RUN fix' again)"
}

# True for a missing path or an empty real directory; an unreadable one, a
# symlink or a file is not empty.
empty_dir() {
  local entry
  [ -e "$1" ] || [ -L "$1" ] || return 0
  [ -d "$1" ] && [ ! -L "$1" ] || return 1
  entry=$(find "$1" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null) || return 1
  [ -z "$entry" ]
}
