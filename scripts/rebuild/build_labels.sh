#!/bin/bash
# build_labels.sh <map-file> — build every label a map lists, one after
# another (build_label.sh), reusing a label's parked crawl when there is one.
# A map line is "<label> <sample-run-dir>"; a map with any other line is
# refused before anything runs. One progress line per label goes
# to $LOGDIR/progress.log and each label's own log to $LOGDIR/<label>.log
# (LOGDIR default: ~/.cache/transitio-index/logs). BUILD_CHECKOUT, PYTHON and
# ARCHIVE_ROOT reach build_label.sh and archive_index.sh;
# STOP_IF_FIRST_FAILS=1 stops the run when the map's first label fails.
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/checks.sh" || exit 2
REPO="$(cd "${BUILD_CHECKOUT:-$HERE/../..}" && pwd)" || exit 2
export BUILD_CHECKOUT="$REPO"
[ "$#" -eq 1 ] || { echo "usage: build_labels.sh <map-file>" >&2; exit 2; }
[ -f "$1" ] && [ -r "$1" ] || { echo "no map file: $1" >&2; exit 2; }
MAP="$(cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")" || exit 2
[ "$(tr -d '\000' < "$MAP" | wc -c)" -eq "$(wc -c < "$MAP")" ] || { echo "refusing: $MAP holds NUL bytes" >&2; exit 2; }
# The map is read once; the run follows this copy of it.
LINES=()
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in *[![:space:]]*) LINES+=("$line");; esac
done < "$MAP" || exit 2
[ "${#LINES[@]}" -gt 0 ] || { echo "refusing: no labels in $MAP" >&2; exit 2; }
for line in "${LINES[@]}"; do
  read -r label run extra <<< "$line"
  case "$label" in *[!$LOWER$DIGITS-]*) label="";; esac
  case "$run" in
    cache/sample/run-*[!$UPPER$LOWER${DIGITS}_]*|cache/sample/run-) run="";;
    cache/sample/run-*) ;;
    *) run="";;
  esac
  if [ -z "$label" ] || [ -z "$run" ] || [ -n "$extra" ]; then
    echo "refusing: bad map line in $MAP" >&2; exit 2
  fi
done
LOGDIR="${LOGDIR:-$HOME/.cache/transitio-index/logs}"
[ -L "$LOGDIR" ] && { echo "refusing: $LOGDIR is a symlink" >&2; exit 2; }
mkdir -p "$LOGDIR" || exit 2
LOGDIR="$(cd "$LOGDIR" && pwd)" || exit 2
export LOGDIR
PROG="$LOGDIR/progress.log"
cd "$REPO" || exit 2
# Nothing below is deleted or written through a symlink.
if [ -L cache ] || [ -L cache/crawl ] || [ -L "$PROG" ]; then
  echo "refusing: a symlink under cache or at $PROG" >&2; exit 2
fi
[ ! -e "$PROG" ] || [ -f "$PROG" ] || { echo "refusing: $PROG is not a regular file" >&2; exit 2; }
export TMPDIR="$LOGDIR"

[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || { echo "refusing: the checkout is not on main" >&2; exit 2; }
if [ -f cache/.runner.lock/pid ] && kill -0 "$(cat cache/.runner.lock/pid)" 2>/dev/null; then
  echo "refusing: a build is running (pid $(cat cache/.runner.lock/pid))" >&2; exit 2
fi
command -v lsof >/dev/null || { echo "refusing: cannot inspect the registry lock (no lsof)" >&2; exit 2; }
if lsof overrides/places_registry.jsonl.lock 2>/dev/null | grep -qi python; then
  echo "refusing: another build holds the registry" >&2; exit 2
fi

total=${#LINES[@]}
echo "[$(date '+%m-%d %H:%M:%S')] REBUILD START: $total labels of $(basename "$MAP"), $(git rev-parse --short HEAD) on main" >> "$PROG"
n=0; ok=0; fail=0
for line in "${LINES[@]}"; do
  read -r label run <<< "$line"
  n=$((n+1))
  if ! rm -rf cache/crawl || [ -e cache/crawl ]; then
    echo "[$(date '+%m-%d %H:%M:%S')] REBUILD STOPPED: cannot clear cache/crawl before $label" >> "$PROG"; exit 1
  fi
  crawl="fresh crawl"
  if [ -d "cache/crawl.$label.bak" ]; then
    if ! mv "cache/crawl.$label.bak" cache/crawl; then
      echo "[$(date '+%m-%d %H:%M:%S')] REBUILD STOPPED: cannot restore the parked crawl of $label" >> "$PROG"; exit 1
    fi
    crawl="parked crawl"
  fi
  # A refused run writes no label log: its reason is in <label>.runner.out.
  rm -f "$LOGDIR/$label.log" "$LOGDIR/$label.runner.out"
  REUSE_CRAWL=1 "$HERE/build_label.sh" "$label" "$run" >"$LOGDIR/$label.runner.out" 2>&1 </dev/null
  code=$?
  # A label refused before it parked its crawl leaves the crawl in place:
  # parked here, so the next label's start does not delete it.
  if [ -d cache/crawl ] && [ ! -L cache/crawl ]; then
    if [ -e "cache/crawl.$label.bak" ] || ! mv cache/crawl "cache/crawl.$label.bak"; then
      echo "[$(date '+%m-%d %H:%M:%S')] REBUILD STOPPED: cannot park the crawl of $label" >> "$PROG"; exit 1
    fi
  fi
  last=$(tail -n1 "$LOGDIR/$label.log" 2>/dev/null | cut -c1-60)
  [ -n "$last" ] || last=$(tail -n1 "$LOGDIR/$label.runner.out" 2>/dev/null | cut -c1-80)
  if [ "$code" -eq 0 ]; then ok=$((ok+1)); st=OK; else fail=$((fail+1)); st=FAIL; fi
  echo "[$(date '+%m-%d %H:%M:%S')] ($n/$total) $st $label ($crawl) | $last" >> "$PROG"
  if [ "$n" -eq 1 ] && [ "$st" = FAIL ] && [ -n "${STOP_IF_FIRST_FAILS:-}" ]; then
    echo "[$(date '+%m-%d %H:%M:%S')] REBUILD STOPPED: the first label, $label, failed" >> "$PROG"; exit 1
  fi
done
echo "[$(date '+%m-%d %H:%M:%S')] REBUILD COMPLETE: ok=$ok fail=$fail of $n" >> "$PROG"
