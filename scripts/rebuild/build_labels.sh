#!/bin/bash
# build_labels.sh <map-file> — build every label a map lists (build_label.sh),
# reusing a label's parked crawl when there is one. A map line is "<label>
# <sample-run-dir>"; a map with any other line is refused before anything
# runs. One progress line per label goes to $LOGDIR/progress.log and each
# label's own log to $LOGDIR/<label>.log (LOGDIR default:
# ~/.cache/transitio-index/logs). BUILD_CHECKOUT, PYTHON and ARCHIVE_ROOT
# reach build_label.sh and archive_index.sh; STOP_IF_FIRST_FAILS=1 stops the
# run when the map's first label fails.
# SLOTS (default 1) labels build at a time. With one, in order, in cache; with
# more, each slot in a cache folder of its own (cache/slot-<n>), taking turns
# on the registry, its crawl on WORKERS threads (default 4 with one slot, 2
# with more); a TERM then lets the labels being built finish and starts no
# other. BUILD_LABEL names the per-label runner (default build_label.sh here).
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/checks.sh" || exit 2
REPO="$(cd "${BUILD_CHECKOUT:-$HERE/../..}" && pwd)" || exit 2
export BUILD_CHECKOUT="$REPO"
[ "$#" -eq 1 ] || { echo "usage: build_labels.sh <map-file>" >&2; exit 2; }
[ -f "$1" ] && [ -r "$1" ] || { echo "no map file: $1" >&2; exit 2; }
MAP="$(cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")" || exit 2
[ "$(tr -d '\000' < "$MAP" | wc -c)" -eq "$(wc -c < "$MAP")" ] || { echo "refusing: $MAP holds NUL bytes" >&2; exit 2; }
SLOTS="${SLOTS:-1}"
case "$SLOTS" in [123456789]) ;; *) echo "refusing: bad SLOTS '$SLOTS' (1 to 9)" >&2; exit 2;; esac
if [ "$SLOTS" -gt 1 ] && [ -n "${STOP_IF_FIRST_FAILS:-}" ]; then
  echo "refusing: STOP_IF_FIRST_FAILS needs SLOTS=1" >&2; exit 2
fi
if [ "$SLOTS" -gt 1 ]; then WORKERS="${WORKERS:-2}"; else WORKERS="${WORKERS:-4}"; fi
case "$WORKERS" in [123456789]|1[0123456]) ;; *) echo "refusing: bad WORKERS '$WORKERS'" >&2; exit 2;; esac
export WORKERS
BUILD_LABEL="${BUILD_LABEL:-$HERE/build_label.sh}"
# The map is read once; the run follows this copy of it.
LINES=()
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in *[![:space:]]*) LINES+=("$line");; esac
done < "$MAP" || exit 2
[ "${#LINES[@]}" -gt 0 ] || { echo "refusing: no labels in $MAP" >&2; exit 2; }
seen=" "
for line in "${LINES[@]}"; do
  read -r label run extra <<< "$line"
  case "$label" in *[!$LOWER$DIGITS-]*) label="";; esac
  # A label twice would build twice at once into the same log and archive.
  case "$seen" in *" $label "*) echo "refusing: $label is twice in $MAP" >&2; exit 2;; esac
  seen="$seen$label "
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
# The cache folder of each slot: cache alone, or cache/slot-1 … cache/slot-N.
CACHES=()
if [ "$SLOTS" -eq 1 ]; then
  CACHES[1]=cache
else
  s=1
  while [ "$s" -le "$SLOTS" ]; do CACHES[$s]="cache/slot-$s"; s=$((s+1)); done
fi
# Nothing below is deleted or written through a symlink.
if [ -L cache ] || [ -L "$PROG" ]; then
  echo "refusing: a symlink under cache or at $PROG" >&2; exit 2
fi
for cache in "${CACHES[@]}"; do
  if [ -L "$cache" ] || [ -L "$cache/crawl" ]; then
    echo "refusing: a symlink under cache" >&2; exit 2
  fi
done
[ ! -e "$PROG" ] || [ -f "$PROG" ] || { echo "refusing: $PROG is not a regular file" >&2; exit 2; }
export TMPDIR="$LOGDIR"

[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || { echo "refusing: the checkout is not on main" >&2; exit 2; }
# A build in any cache folder, whatever SLOTS it was started with.
for runner in cache/.runner.lock cache/slot-*/.runner.lock; do
  if [ -f "$runner/pid" ] && kill -0 "$(cat "$runner/pid")" 2>/dev/null; then
    echo "refusing: a build is running (pid $(cat "$runner/pid"))" >&2; exit 2
  fi
done
command -v lsof >/dev/null || { echo "refusing: cannot inspect the registry lock (no lsof)" >&2; exit 2; }
if lsof overrides/places_registry.jsonl.lock 2>/dev/null | grep -qi python; then
  echo "refusing: another build holds the registry" >&2; exit 2
fi

# Why the run stops, written by the label that stops it and logged once no
# slot builds.
STOP_NOTE=$(mktemp "$LOGDIR/.stop.XXXXXX") || exit 2
trap 'rm -f "$STOP_NOTE"' EXIT
stopped() {
  echo "[$(date '+%m-%d %H:%M:%S')] REBUILD STOPPED: $*" >> "$PROG"
}

# Build label $2 (number $1, run dir $3) in the cache folder $4 and log its
# outcome. Returns 0 when it built, 1 when it failed, 3 when the run must
# stop (a crawl that cannot be cleared, restored or parked: the reason in
# STOP_NOTE).
build_one() {
  local n=$1 label=$2 run=$3 cache=$4 crawl code last st
  # Checked again here: the run started hours ago.
  if [ -L cache ] || [ -L "$cache" ] || [ -L "$cache/crawl" ] || [ -L "cache/crawl.$label.bak" ]; then
    echo "a symlink under cache before $label" > "$STOP_NOTE"; return 3
  fi
  if ! rm -rf "$cache/crawl" || [ -e "$cache/crawl" ]; then
    echo "cannot clear $cache/crawl before $label" > "$STOP_NOTE"; return 3
  fi
  crawl="fresh crawl"
  if [ -d "cache/crawl.$label.bak" ]; then
    if ! mv "cache/crawl.$label.bak" "$cache/crawl"; then
      echo "cannot restore the parked crawl of $label" > "$STOP_NOTE"; return 3
    fi
    crawl="parked crawl"
  fi
  # A refused run writes no label log: its reason is in <label>.runner.out.
  rm -f "$LOGDIR/$label.log" "$LOGDIR/$label.runner.out"
  CACHE="$cache" REUSE_CRAWL=1 "$BUILD_LABEL" "$label" "$run" >"$LOGDIR/$label.runner.out" 2>&1 </dev/null
  code=$?
  # A label refused before it parked its crawl leaves the crawl in place:
  # parked here, so the slot's next label does not delete it.
  if [ -L cache ] || [ -L "$cache" ] || [ -L "$cache/crawl" ]; then
    echo "a symlink under cache after $label" > "$STOP_NOTE"; return 3
  fi
  if [ -d "$cache/crawl" ] && [ ! -L "$cache/crawl" ]; then
    if [ -e "cache/crawl.$label.bak" ] || ! mv "$cache/crawl" "cache/crawl.$label.bak"; then
      echo "cannot park the crawl of $label" > "$STOP_NOTE"; return 3
    fi
  fi
  last=$(tail -n1 "$LOGDIR/$label.log" 2>/dev/null | cut -c1-60)
  [ -n "$last" ] || last=$(tail -n1 "$LOGDIR/$label.runner.out" 2>/dev/null | cut -c1-80)
  if [ "$code" -eq 0 ]; then st=OK; else st=FAIL; fi
  echo "[$(date '+%m-%d %H:%M:%S')] ($n/$total) $st $label ($crawl) | $last" >> "$PROG"
  [ "$st" = OK ]
}

total=${#LINES[@]}
echo "[$(date '+%m-%d %H:%M:%S')] REBUILD START: $total labels of $(basename "$MAP"), $(git rev-parse --short HEAD) on main" >> "$PROG"
ok=0; fail=0
if [ "$SLOTS" -eq 1 ]; then
  n=0
  for line in "${LINES[@]}"; do
    read -r label run <<< "$line"
    n=$((n+1))
    build_one "$n" "$label" "$run" cache
    case $? in
      0) ok=$((ok+1)); st=OK;;
      3) stopped "$(cat "$STOP_NOTE")"; exit 1;;
      *) fail=$((fail+1)); st=FAIL;;
    esac
    if [ "$n" -eq 1 ] && [ "$st" = FAIL ] && [ -n "${STOP_IF_FIRST_FAILS:-}" ]; then
      stopped "the first label, $label, failed"; exit 1
    fi
  done
  echo "[$(date '+%m-%d %H:%M:%S')] REBUILD COMPLETE: ok=$ok fail=$fail of $n" >> "$PROG"
  exit 0
fi

# The slots: every finished label's slot is collected first, so a stop one
# of them asks for starts no label in another; then each free slot takes the
# next label of the map. The run ends when no slot builds.
export WAIT_FOR_REGISTRY=1
PIDS=()
next=0; halted=""; stopping=""
trap 'stopping=1' TERM INT
while :; do
  s=1
  while [ "$s" -le "$SLOTS" ]; do
    pid="${PIDS[$s]:-}"
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      case $? in 0) ok=$((ok+1));; 3) halted=1;; *) fail=$((fail+1));; esac
      PIDS[$s]=""
    fi
    s=$((s+1))
  done
  busy=0; s=1
  while [ "$s" -le "$SLOTS" ]; do
    # A stop a slot wrote since the collection starts no label either.
    if [ -s "$STOP_NOTE" ]; then halted=1; fi
    if [ -z "${PIDS[$s]:-}" ] && [ -z "$halted$stopping" ] && [ "$next" -lt "$total" ]; then
      read -r label run <<< "${LINES[$next]}"
      next=$((next+1))
      if mkdir -p "${CACHES[$s]}"; then
        build_one "$next" "$label" "$run" "${CACHES[$s]}" &
        PIDS[$s]=$!
      else
        echo "cannot create ${CACHES[$s]}" > "$STOP_NOTE"; halted=1
      fi
    fi
    [ -z "${PIDS[$s]:-}" ] || busy=$((busy+1))
    s=$((s+1))
  done
  [ "$busy" -gt 0 ] || break
  sleep 1
done
if [ -n "$halted" ]; then
  stopped "$(cat "$STOP_NOTE")"; exit 1
fi
if [ -n "$stopping" ]; then
  stopped "asked to stop; the labels being built finished"; exit 1
fi
echo "[$(date '+%m-%d %H:%M:%S')] REBUILD COMPLETE: ok=$ok fail=$fail of $total" >> "$PROG"
