#!/bin/bash
# rebuild.sh <run> <mode> — rebuild every label of the run's cut from the
# current main, with a fresh crawl, in two runs: the check label, then the
# rest. Run from a terminal:
#   rebuild.sh <run> prepare   pull main, install READER if given, then the checks
#   rebuild.sh <run> check     the checks only; writes nothing
#   rebuild.sh <run> first     build the check label (map-check.txt)
#   rebuild.sh <run> rest      build the other labels (map-rest.txt)
#   rebuild.sh <run> resume    build the labels of both maps not yet archived
#   rebuild.sh <run> fix       pull main (a later commit than the first run's)
#                              and rebuild the labels of map-fix.txt at it; run
#                              again, it rebuilds those not yet rebuilt
# The builds run in the background through build_labels.sh (build_label.sh per
# label: ingest -> publish, crawl included), which archives each index under
# ARCHIVE_ROOT and parks each crawl. Do not change the build checkout while a
# run goes: every label reads it. The merge and the release are release.sh,
# after the builds are checked. Every label reads the Overture release that
# was newest when the first run started. The run's files are described in
# common.sh.
# Environment: MDB_CATALOGUE (the whole MDB catalogue the cut was taken from,
# needed until the first run copies it into the run's logs); READER (a pip
# requirement such as transitio==0.29.0 that prepare installs); ARCHIVE_ROOT
# (default ~/.cache/transitio-index/builds-<run>); MIN_FREE_GB (default 50);
# RUN_DIR, BUILD_CHECKOUT and PYTHON (see common.sh).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
[ "$#" -eq 2 ] || { echo "usage: rebuild.sh <run> prepare | check | first | rest | resume | fix" >&2; exit 2; }
RUN="$1"
MODE="$2"
case "$MODE" in
  prepare|check|first|rest|resume|fix) ;;
  *) echo "usage: rebuild.sh <run> prepare | check | first | rest | resume | fix" >&2; exit 2;;
esac
. "$HERE/common.sh" || { echo "refusing: cannot read common.sh" >&2; exit 2; }
READER="${READER:-}"
MIN_FREE_GB="${MIN_FREE_GB:-50}"
MDB_CATALOGUE="${MDB_CATALOGUE:-}"
# Each resume gets a map of its own, named by its start time.
RESUME_MAP="$RUN_DIR/map-resume-$(date '+%Y%m%d-%H%M%S').txt"
FIX_RUN_MAP="$RUN_DIR/map-fix-$(date '+%Y%m%d-%H%M%S').txt"
OUT="$RUN_DIR/rebuild.out"
[ -d "$RUN_DIR" ] && [ ! -L "$RUN_DIR" ] || refuse "no run folder $RUN_DIR (it holds the run's partition and maps)"
cd "$REPO" || refuse "cannot enter $REPO"

# --- nothing else is building ------------------------------------------------
# Checked before anything else, so that prepare never pulls or installs under
# a running build.
no_running_build
# Every mode but check holds the lock from here, before it reads the checkout
# or the cut, so that no prepare can pull between its checks and its run.
[ "$MODE" = check ] || take_lock

# --- the checkout ------------------------------------------------------------
[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || refuse "the checkout is not on main"
clean_checkout
# The fix pulls main once, when it starts; run again, it keeps that commit.
if [ "$MODE" = prepare ] || { [ "$MODE" = fix ] && [ ! -e "$FIXED" ]; }; then
  say "pulling main into $REPO"
  before=$(git rev-parse HEAD)
  git pull --ff-only origin main || refuse "git pull --ff-only origin main failed"
  # These scripts are part of the checkout: carry on with the pulled ones,
  # which pull again (a no-op unless main moved once more).
  if [ "$(git rev-parse HEAD)" != "$before" ]; then
    trap - EXIT
    rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null
    exec "$REPO/scripts/rebuild/rebuild.sh" "$RUN" "$MODE"
  fi
fi
if [ "$MODE" = prepare ] && [ -n "$READER" ]; then
  have=$(PYTHONPATH="" "$PY" -c 'import importlib.metadata as m; print(m.version("transitio"))' 2>/dev/null)
  if [ "$have" != "${READER#transitio==}" ]; then
    say "installing $READER into the build environment (it has ${have:-no transitio})"
    PYTHONPATH="" "$PY" -m pip install "$READER" || refuse "pip install $READER failed"
  fi
fi
head=$(git rev-parse HEAD)
case "$MODE" in
  prepare|check|first)
    # A new rebuild builds GitHub's main (asked without writing git state).
    remote=$(git ls-remote --exit-code origin refs/heads/main | cut -f1) || refuse "cannot read GitHub's main (git ls-remote origin)"
    case "$remote" in *[!$HEX]*|"") refuse "cannot read GitHub's main (git ls-remote origin)";; esac
    [ "$head" = "$remote" ] || refuse "the checkout is at ${head:0:7}, GitHub's main at ${remote:0:7}: run '$0 $RUN prepare'"
    ;;
  rest|resume)
    # Every label of the rebuild builds from the code the check label built.
    [ -f "$STARTED" ] && [ ! -L "$STARTED" ] || refuse "no first run recorded in $STARTED: run '$0 $RUN first'"
    [ "$head" = "$(sed -n 1p "$STARTED")" ] || refuse "the checkout is at ${head:0:7}, the check label built $(sed -n 1p "$STARTED" | cut -c1-7): the labels would mix code versions"
    ;;
  fix)
    # The fix builds from a later commit of main than the first run's, the
    # same one for every label of the fix map.
    [ -f "$STARTED" ] && [ ! -L "$STARTED" ] || refuse "no first run recorded in $STARTED: run '$0 $RUN first'"
    started=$(sed -n 1p "$STARTED")
    [ "$head" != "$started" ] || refuse "the checkout is at ${head:0:7}, the code the labels were built with: nothing to fix"
    git merge-base --is-ancestor "$started" "$head" || refuse "the checkout (${head:0:7}) does not descend from ${started:0:7}, the code the labels were built with"
    if [ -e "$FIXED" ]; then
      [ -f "$FIXED" ] && [ ! -L "$FIXED" ] || refuse "$FIXED is not a regular file"
      [ "$head" = "$(sed -n 1p "$FIXED")" ] || refuse "the fix started at $(sed -n 1p "$FIXED" | cut -c1-7), the checkout is at ${head:0:7}: the fixed labels would mix code versions"
    else
      remote=$(git ls-remote --exit-code origin refs/heads/main | cut -f1) || refuse "cannot read GitHub's main (git ls-remote origin)"
      case "$remote" in *[!$HEX]*|"") refuse "cannot read GitHub's main (git ls-remote origin)";; esac
      [ "$head" = "$remote" ] || refuse "the checkout is at ${head:0:7}, GitHub's main at ${remote:0:7}"
    fi
    ;;
esac

# The build environment satisfies the checkout's pyproject.toml.
PYTHONPATH="" "$PY" - <<'PYEOF' || refuse "the build environment ($PY) does not meet pyproject.toml: run '$0 $RUN prepare'"
import sys
from importlib.metadata import PackageNotFoundError, version

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        sys.exit("reading pyproject.toml needs Python 3.11 or the tomli package")

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

with open("pyproject.toml", "rb") as handle:
    project = tomllib.load(handle)["project"]
bad = []
python = ".".join(map(str, sys.version_info[:3]))
if not SpecifierSet(project.get("requires-python", "")).contains(python, prereleases=True):
    bad.append(f"Python {python} does not satisfy {project['requires-python']}")
for dependency in project["dependencies"]:
    requirement = Requirement(dependency)
    if requirement.marker is not None and not requirement.marker.evaluate():
        continue
    try:
        found = version(requirement.name)
    except PackageNotFoundError:
        bad.append(f"{requirement.name} is not installed")
        continue
    if not requirement.specifier.contains(found, prereleases=True):
        bad.append(f"{requirement.name} {found} does not satisfy {requirement.specifier}")
for line in bad:
    print(line, file=sys.stderr)
sys.exit(1 if bad else 0)
PYEOF

# --- the cut, its catalogue and the maps -------------------------------------
check_cut
check_maps
set_inputs_digest
case "$MODE" in
  prepare|check|first)
    plain_path "$MDB_CATALOGUE" || refuse "MDB_CATALOGUE must name the MDB catalogue the cut was taken from, as a plain absolute path"
    check_catalogue "$MDB_CATALOGUE"
    ;;
  rest|resume|fix)
    # The first run recorded its commit, archive root, MDB digest and the
    # digest of the cut and maps, and copied the catalogue every label reads.
    [ "$(sed -n 2p "$STARTED")" = "$ARCHIVE_ROOT" ] || refuse "the check label was archived under '$(sed -n 2p "$STARTED")', not $ARCHIVE_ROOT"
    [ "$(sed -n 3p "$STARTED")" = "$CUT_MDB_SHA" ] || refuse "the first run recorded another MDB catalogue than the cut's"
    [ "$(sed -n 4p "$STARTED")" = "$INPUTS_DIGEST" ] || refuse "the cut or the maps changed since the first run"
    check_catalogue "$RUN_CATALOGUE"
    [ -s "$BACKUP" ] && [ -f "$BACKUP" ] && [ ! -L "$BACKUP" ] || refuse "no registry backup at $BACKUP"
    ;;
esac

# --- the Overture release ----------------------------------------------------
# Every label of a rebuild reads one Overture release (the merge refuses labels
# that differ): the newest when the first run starts, recorded for rest and
# resume. Overture keeps only recent releases, so a recorded one can go away.
held=$(overture_releases) || refuse "cannot list the Overture releases"
[ -n "$held" ] || refuse "the Overture bucket lists no release with a division table"
case "$MODE" in
  prepare|check|first)
    RELEASE=$(printf '%s\n' "$held" | tail -n 1)
    ;;
  rest|resume|fix)
    RELEASE=$(sed -n 5p "$STARTED")
    [ -n "$RELEASE" ] || refuse "the first run recorded no Overture release in $STARTED: start over with 'first'"
    printf '%s\n' "$held" | grep -qxF -- "$RELEASE" \
      || refuse "Overture no longer holds release $RELEASE, which the archived labels read: start over with 'first' and an empty ARCHIVE_ROOT"
    ;;
esac
release_name "$RELEASE" || refuse "bad Overture release name '$RELEASE'"

# --- there is room -----------------------------------------------------------
case "$MIN_FREE_GB" in
  [123456789]|[123456789][$DIGITS]|[123456789][$DIGITS][$DIGITS]|[123456789][$DIGITS][$DIGITS][$DIGITS]) ;;
  *) refuse "MIN_FREE_GB must be a whole number of GB from 1 to 9999, not '$MIN_FREE_GB'";;
esac
# Free space on the cache volume and on the archive root's volume (its nearest
# existing parent when it does not exist yet).
check_archive_root
archive_parent="$ARCHIVE_ROOT"
while [ ! -d "$archive_parent" ]; do archive_parent=$(dirname "$archive_parent"); done
free_gb=""
for target in cache "$archive_parent"; do
  kb=$(df -Pk "$target" | awk 'NR == 2 { print $4 }')
  case "$kb" in *[!$DIGITS]*|"") refuse "cannot read the free space of $target";; esac
  [ "$kb" -ge $((MIN_FREE_GB * 1024 * 1024)) ] || refuse "$((kb / 1024 / 1024)) GB free on the volume of $target, under MIN_FREE_GB=$MIN_FREE_GB"
  gb=$((kb / 1024 / 1024))
  [ -n "$free_gb" ] && [ "$gb" -ge "$free_gb" ] || free_gb=$gb
done

# --- what to build -----------------------------------------------------------
check_archive_root
stop_first=""
case "$MODE" in
  prepare|check|first)
    empty_dir "$ARCHIVE_ROOT" || refuse "$ARCHIVE_ROOT holds builds already: use 'rest' or 'resume', or choose another ARCHIVE_ROOT"
    empty_dir "$ARCHIVE_ROOT-feeds-only" || refuse "$ARCHIVE_ROOT-feeds-only is not empty: use 'rest' or 'resume', or choose another ARCHIVE_ROOT"
    RUNMAP="$CHECK_MAP"; stop_first=1
    ;;
  rest)
    archived "$CHECK_LABEL" || refuse "the check label $CHECK_LABEL is not archived under $ARCHIVE_ROOT: run 'first' (or 'resume')"
    [ "$(missing_labels | count_nonblank)" -eq "$(count_nonblank "$REST_MAP")" ] || refuse "labels of the rest are archived already: use 'resume'"
    RUNMAP="$REST_MAP"
    ;;
  resume)
    resume=$(mktemp) || refuse "cannot create a temporary file"
    missing_labels > "$resume" || refuse "cannot list the labels to resume"
    if [ ! -s "$resume" ]; then
      rm -f "$resume"
      say "every label of the cut is archived under $ARCHIVE_ROOT; nothing to resume"
      exit 0
    fi
    copy_into "$resume" "$RESUME_MAP"
    rm -f "$resume"
    RUNMAP="$RESUME_MAP"
    ;;
  fix)
    check_fix_map
    fixrun=$(mktemp) || refuse "cannot create a temporary file"
    if [ -e "$FIXED" ]; then
      [ "$(sed -n 2p "$FIXED")" = "$(fix_map_digest)" ] || refuse "$FIX_MAP changed since the fix started"
      unfixed_labels > "$fixrun" || refuse "cannot list the labels the fix has not rebuilt"
      if [ ! -s "$fixrun" ]; then
        rm -f "$fixrun"
        say "every label of $FIX_MAP is rebuilt at ${head:0:7}; nothing to fix"
        exit 0
      fi
    else
      nonblank_lines "$FIX_MAP" > "$fixrun" || refuse "cannot read $FIX_MAP"
    fi
    copy_into "$fixrun" "$FIX_RUN_MAP"
    rm -f "$fixrun"
    RUNMAP="$FIX_RUN_MAP"
    ;;
esac
count=$(count_nonblank "$RUNMAP")
say "checkout $REPO at ${head:0:7} ($(git log -1 --format=%s | cut -c1-60))"
say "reader: transitio $(PYTHONPATH="" "$PY" -c 'import importlib.metadata as m; print(m.version("transitio"))')"
say "cut $PART, MDB catalogue ${CUT_MDB_SHA:0:16}"
say "Overture release $RELEASE (the bucket holds: $(printf '%s\n' "$held" | paste -sd ' ' -))"
say "free disk: $free_gb GB on the fuller of the cache and archive volumes (MIN_FREE_GB $MIN_FREE_GB)"
say "would build $count label(s) from $(basename "$RUNMAP"), first $(awk 'NF { print $1; exit }' "$RUNMAP"), archive root $ARCHIVE_ROOT"
case "$MODE" in prepare|check) say "checks passed; start with: $0 $RUN first"; exit 0;; esac

# --- the label builds, in the background -------------------------------------
check_logdir
writable_file "$OUT"
writable_file "$PROG"
: >> "$OUT" 2>/dev/null || refuse "cannot write $OUT"
: >> "$PROG" 2>/dev/null || refuse "cannot write $PROG"
if [ "$MODE" = first ]; then
  writable_file "$BACKUP"
  [ -s "$BACKUP" ] || copy_into overrides/places_registry.jsonl "$BACKUP"
  copy_into "$MDB_CATALOGUE" "$RUN_CATALOGUE"
  check_catalogue "$RUN_CATALOGUE"
  record=$(mktemp) || refuse "cannot create a temporary file"
  printf '%s\n%s\n%s\n%s\n%s\n' "$head" "$ARCHIVE_ROOT" "$CUT_MDB_SHA" "$INPUTS_DIGEST" "$RELEASE" > "$record" || refuse "cannot record the run"
  copy_into "$record" "$STARTED"
  rm -f "$record"
fi
if [ "$MODE" = fix ] && [ ! -e "$FIXED" ]; then
  # The registry as the fix found it, and the fix's commit, map and start.
  writable_file "$FIX_BACKUP"
  [ -s "$FIX_BACKUP" ] || copy_into overrides/places_registry.jsonl "$FIX_BACKUP"
  started_at=$(PYTHONPATH="" "$PY" -c 'import datetime as dt; print(dt.datetime.now(dt.timezone.utc).isoformat())') || refuse "cannot read the time"
  record=$(mktemp) || refuse "cannot create a temporary file"
  printf '%s\n%s\n%s\n' "$head" "$(fix_map_digest)" "$started_at" > "$record" || refuse "cannot record the fix"
  copy_into "$record" "$FIXED"
  rm -f "$record"
fi
echo "[$(stamp)] REBUILD $RUN $MODE: map $RUNMAP, $count labels, code ${head:0:7}, archive root $ARCHIVE_ROOT, $free_gb GB free, Overture $RELEASE" >> "$PROG" \
  || refuse "cannot write $PROG"
nohup env ARCHIVE_ROOT="$ARCHIVE_ROOT" LOGDIR="$LOGDIR" STOP_IF_FIRST_FAILS="$stop_first" RUN_LOCK="$LOCK" \
  MDB_CATALOGUE="$RUN_CATALOGUE" OVERTURE_RELEASE="$RELEASE" \
  "$HERE/supervise.sh" "$PROG" "$HERE/build_labels.sh" "$RUNMAP" >> "$OUT" 2>&1 < /dev/null &
bg=$!
if ! echo "$bg" > "$LOCK/pid"; then
  # Without its pid in the lock the run would go unseen: stop it at once.
  kill -TERM "$bg" 2>/dev/null
  refuse "cannot hand $LOCK to the run (pid $bg); asked it to stop: the label it builds finishes first, so start nothing until $LOGDIR shows that label done"
fi
trap - EXIT
if command -v caffeinate > /dev/null; then
  nohup caffeinate -i -w "$bg" > /dev/null 2>&1 < /dev/null &
fi
sleep 5
if ! kill -0 "$bg" 2>/dev/null; then
  echo "[$(stamp)] REBUILD STOPPED: build_labels.sh stopped at once; see $OUT" >> "$PROG"
  tail -n 5 "$OUT" >&2
  refuse "build_labels.sh stopped at once; see $OUT"
fi
say "build_labels.sh runs under pid $bg (stop after the current label: kill $bg)"
say "progress: tail -f $PROG; label logs: $LOGDIR/<label>.log"
