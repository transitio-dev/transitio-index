#!/bin/bash
# supervise.sh <progress-log> <command> [args...] — run the command
# (build_labels.sh on a map) and, however it exits, append a "REBUILD ENDED"
# line with its exit code to the progress log, so a run that dies partway is
# seen to have ended. A TERM (the stop rebuild.sh prints: kill <pid>) is
# passed on to the command: as with build_labels.sh itself, the label being
# built finishes. Started in the background by rebuild.sh, which hands it the
# run lock (RUN_LOCK): released at the end while it still names this process.
PROG="$1"
shift
"$@" &
child=$!
interrupted=""
trap 'interrupted=1; kill -TERM "$child" 2>/dev/null' TERM INT
wait "$child"
code=$?
# A trapped signal ends a wait early; wait again until the command itself has
# ended, whose status the shell keeps (127: already collected above).
while [ -n "$interrupted" ]; do
  interrupted=""
  wait "$child"
  again=$?
  [ "$again" -eq 127 ] || code=$again
done
line=$(printf '[%s] REBUILD ENDED: %s exited with code %s' "$(date '+%m-%d %H:%M:%S')" "$(basename "$1")" "$code")
# Checked again here: the log was checked hours ago, when the run started.
if [ -L "$PROG" ] || { [ -e "$PROG" ] && [ ! -f "$PROG" ]; }; then
  echo "$line (not written to $PROG: not a regular file)" >&2
else
  echo "$line" >> "$PROG"
fi
if [ -n "${RUN_LOCK:-}" ] && [ "$(cat "$RUN_LOCK/pid" 2>/dev/null)" = "$$" ]; then
  rm -f "$RUN_LOCK/pid"; rmdir "$RUN_LOCK" 2>/dev/null
fi
exit "$code"
