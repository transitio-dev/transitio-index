#!/bin/bash
# watch.sh <run> — print the rebuild's status (status.py) every INTERVAL
# seconds (default 300) while a run of it is going, for a monitor. Says once
# that it is waiting while no run has started, prints one last status when a
# run ends, and exits once the rest (or a resume or fix) run has ended.
# PYTHON and RUN_DIR as for rebuild.sh.
HERE="$(cd "$(dirname "$0")" && pwd)"
[ "$#" -eq 1 ] || { echo "usage: watch.sh <run>" >&2; exit 2; }
RUN="$1"
PY="${PYTHON:-python3}"
INTERVAL="${INTERVAL:-300}"
set -- --run "$RUN"
[ -z "${RUN_DIR:-}" ] || set -- "$@" --run-dir "$RUN_DIR"
last=""
err=$(mktemp) || exit 2
trap 'rm -f "$err"' EXIT
while true; do
  out="$("$PY" "$HERE/status.py" "$@" 2>"$err")"
  code=$?
  if [ "$code" -ne 0 ]; then
    state="error: $(tail -n 5 "$err")"
  else
    final=$(printf '%s\n' "$out" | tail -n 1)
    case "$out" in
      "No $RUN run"*) state=waiting;;
      *) case "$final" in "This run ("*) state="over: $final";; *) state=running;; esac;;
    esac
  fi
  if [ "$state" = running ] || [ "$state" != "$last" ]; then
    case "$state" in
      waiting) echo "waiting for the $RUN run to start";;
      error:*) printf 'status.py failed (exit %s):\n%s\n\n' "$code" "${state#error: }";;
      *) printf '%s\n\n' "$out";;
    esac
  fi
  case "$state" in
    "over: This run (rest)"*|"over: This run (resume)"*|"over: This run (fix)"*) exit 0;;
  esac
  last="$state"
  sleep "$INTERVAL"
done
