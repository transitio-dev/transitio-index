#!/bin/bash
# release.sh <run> <mode> — merge the run's label builds into one snapshot
# and release it, from a terminal, once the builds are done and checked:
#   release.sh <run> merge     merge the newest archived build of every label
#   release.sh <run> publish   write the release report, then publish the merged snapshot
# merge writes $MERGED/index (default ~/.cache/transitio-index/merged-<run>)
# and logs to the run's logs/merge.log; it refuses a label without an archived
# build unless ALLOW_MISSING=1. publish installs the current release with
# transitio (the snapshot the report compares with), runs transitio's
# reference checks on both indexes from $TRANSITIO_TESTS (a transitio
# checkout's tests folder), and refuses a check that passed on the current
# release and fails on the new one unless ACCEPT_REGRESSIONS=1 (the
# publisher's --accept-regressions); the publisher itself refuses a merge that
# is no longer of the newest archive of every label. The GitHub token comes
# from the command TOKEN_HELPER names. It logs to logs/publish.log and keeps
# the release assets in logs/release-assets.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
[ "$#" -eq 2 ] || { echo "usage: release.sh <run> merge | publish" >&2; exit 2; }
RUN="$1"
MODE="$2"
case "$MODE" in
  merge|publish) ;;
  *) echo "usage: release.sh <run> merge | publish" >&2; exit 2;;
esac
. "$HERE/common.sh" || { echo "refusing: cannot read common.sh" >&2; exit 2; }
MERGED="${MERGED:-$HOME/.cache/transitio-index/merged-$RUN}"
TRANSITIO_TESTS="${TRANSITIO_TESTS:-}"
TOKEN_HELPER="${TOKEN_HELPER:-}"
[ -d "$RUN_DIR" ] && [ ! -L "$RUN_DIR" ] || refuse "no run folder $RUN_DIR"
cd "$REPO" || refuse "cannot enter $REPO"
no_running_build
# Held from here through the merge or the release, before the checkout and the
# run record are read: no rebuild start, prepare or second release runs
# meanwhile.
take_lock

# The merge and the release use the code, the cut and the archive root every
# label's newest build used: the first run's, or the fix's for the labels it
# rebuilt.
[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || refuse "the checkout is not on main"
clean_checkout
[ -f "$STARTED" ] && [ ! -L "$STARTED" ] || refuse "no rebuild recorded in $STARTED"
[ "$(sed -n 2p "$STARTED")" = "$ARCHIVE_ROOT" ] || refuse "the labels were archived under '$(sed -n 2p "$STARTED")', not $ARCHIVE_ROOT"
check_cut
[ "$(sed -n 3p "$STARTED")" = "$CUT_MDB_SHA" ] || refuse "the labels were built from another MDB catalogue than the cut's"
check_maps
set_inputs_digest
[ "$(sed -n 4p "$STARTED")" = "$INPUTS_DIGEST" ] || refuse "the cut or the maps changed since the first run"
check_archive_root
# The checkout is the first run's commit, or the fix's with every label of the
# fix rebuilt at it.
check_built_code
plain_path "$MERGED" || refuse "MERGED must be a plain absolute path: printable ASCII, no trailing slash, no . or .. components"
# The merge writes a directory of its own, apart from what it reads and the
# run's records.
# Compared as real paths, case-folded: macOS volumes ignore case by default.
physical() { PYTHONPATH="" "$PY" -c 'import os, sys; print(os.path.realpath(sys.argv[1]).casefold())' "$1"; }
merged_path=$(physical "$MERGED") || refuse "cannot resolve $MERGED"
for kept in "$ARCHIVE_ROOT" "$ARCHIVE_ROOT-feeds-only" "$REPO" "$RUN_DIR"; do
  kept_path=$(physical "$kept") || refuse "cannot resolve $kept"
  case "$merged_path/" in "$kept_path"/*) refuse "MERGED ($MERGED) is or lies inside $kept";; esac
  case "$kept_path/" in "$merged_path"/*) refuse "MERGED ($MERGED) holds $kept";; esac
done
home_path=$(physical "$HOME") || refuse "cannot resolve $HOME"
case "$home_path/" in "$merged_path"/*) refuse "MERGED ($MERGED) holds your home directory";; esac
# The last line of a log (read from its last 64 KB), at most 160 characters,
# with every C0 and C1 control replaced: safe to append to the progress log.
log_tail() {
  PYTHONPATH="" "$PY" -c '
import os, re, sys
with open(sys.argv[1], "rb") as handle:
    handle.seek(max(0, os.fstat(handle.fileno()).st_size - 65536))
    lines = handle.read().decode("utf-8", "replace").split("\n")
last = next((line for line in reversed(lines) if line.strip()), "")
print(re.sub(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]", "?", last[:160]))
' "$1"
}
[ ! -L "$MERGED" ] || refuse "$MERGED is a symlink"
[ ! -L "$MERGED/index" ] || refuse "$MERGED/index is a symlink"
check_logdir
writable_file "$PROG"
writable_file "$LOGDIR/$MODE.log"
[ ! -L "$LOGDIR/release-assets" ] || refuse "$LOGDIR/release-assets is a symlink"

if [ "$MODE" = merge ]; then
  missing=$(missing_labels | awk '{ printf "%s ", $1 }')
  if [ -n "$missing" ]; then
    # A label without a build leaves its catalogue feeds out of the snapshot.
    [ "${ALLOW_MISSING:-}" = 1 ] || refuse "labels without an archived build: $missing(build them with '$HERE/rebuild.sh $RUN resume', or set ALLOW_MISSING=1 to merge without them)"
    say "merging without the builds of: $missing"
  fi
  say "merging the builds under $ARCHIVE_ROOT into $MERGED"
  PYTHONPATH="" OMP_NUM_THREADS=1 "$PY" -m transitio_index.merge \
    --builds "$ARCHIVE_ROOT" --cache-dir "$MERGED" --partition "$PART/partition.json" \
    2>&1 | tee "$LOGDIR/merge.log"
  codes=("${PIPESTATUS[@]}")
  status=${codes[0]}
  printf '[%s] MERGE %s: exit %s | %s\n' "$(stamp)" "$RUN" "$status" "$(log_tail "$LOGDIR/merge.log")" >> "$PROG"
  [ "$status" -eq 0 ] || refuse "the merge failed (exit $status): $LOGDIR/merge.log"
  [ "${codes[1]}" -eq 0 ] || refuse "the merge ran, but writing $LOGDIR/merge.log failed"
  say "merged; next, after checking $LOGDIR/merge.log: $0 $RUN publish"
  exit 0
fi

# --- publish -----------------------------------------------------------------
[ -f "$MERGED/index/snapshot.json" ] && [ ! -L "$MERGED/index/snapshot.json" ] || refuse "no merged snapshot in $MERGED/index: run '$0 $RUN merge'"
[ -f "$LOGDIR/merge.log" ] || refuse "no merge log $LOGDIR/merge.log"
[ -n "$TRANSITIO_TESTS" ] && [ -f "$TRANSITIO_TESTS/test_reference_places.py" ] || refuse "no reference checks in '$TRANSITIO_TESTS' (set TRANSITIO_TESTS to a transitio checkout's tests)"
[ -n "$TOKEN_HELPER" ] && [ -x "$TOKEN_HELPER" ] || refuse "no token helper '$TOKEN_HELPER' (set TOKEN_HELPER to a command that prints the GitHub token)"
say "installing the current release with transitio, for the report's comparison"
PYTHONPATH="" "$PY" -c 'import transitio; transitio.index.refresh()' || refuse "transitio.index.refresh() failed"
# No shell trace from here on: it would print the token. Neither name may
# arrive exported: the token goes only to the publisher.
{ set +x; } 2>/dev/null
unset token GITHUB_TOKEN
token=$("$TOKEN_HELPER") || refuse "the token helper failed"
[ -n "$token" ] || refuse "the token helper printed no token"
accept=""
if [ "${ACCEPT_REGRESSIONS:-}" = 1 ]; then
  accept=1
  say "accepting reference checks that passed on the current release and fail on the new one"
fi
say "publishing $MERGED/index (report, draft release, upload, verify, publish)"
GITHUB_TOKEN="$token" PYTHONPATH="" OMP_NUM_THREADS=1 TRANSITIO_TESTS="$TRANSITIO_TESTS" "$PY" -m transitio_index.publish_cli \
  --cache-dir "$MERGED" --builds-dir "$ARCHIVE_ROOT" --out-dir "$LOGDIR/release-assets" \
  ${accept:+--accept-regressions} 2>&1 | tee "$LOGDIR/publish.log"
codes=("${PIPESTATUS[@]}")
status=${codes[0]}
unset token
printf '[%s] PUBLISH %s: exit %s | %s\n' "$(stamp)" "$RUN" "$status" "$(log_tail "$LOGDIR/publish.log")" >> "$PROG"
[ "$status" -eq 0 ] || refuse "the release failed (exit $status): $LOGDIR/publish.log"
[ "${codes[1]}" -eq 0 ] || refuse "the release ran, but writing $LOGDIR/publish.log failed"
say "published; the release's round trip is at the end of $LOGDIR/publish.log"
