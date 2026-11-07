#!/bin/sh
# archive_index.sh <label> [expected-snapshot-id] — keep the just-published
# index: copy cache/index to $ARCHIVE_ROOT/<label>-<snapshot_id>/index
# (default root ~/.cache/transitio-index/builds) with its stats and the map
# layer. Built in a staging directory beside the root ($ARCHIVE_ROOT.staging,
# so the merge never reads a half-copied run) and renamed into place, so a
# destination either does not exist or is complete; an existing one is
# refused. An index with feeds but no places is archived without the map layer
# under $ARCHIVE_ROOT-feeds-only: the merge cannot load a build without places
# and would refuse the whole merge over one.
# BUILD_CHECKOUT is the checkout whose index is archived (default: this
# repository), PYTHON its Python (default: python3).
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/checks.sh"
REPO="${BUILD_CHECKOUT:-$HERE/../..}"
PY="${PYTHON:-python3}"
cd "$REPO" || { echo "cannot enter $REPO" >&2; exit 2; }
[ "$#" -ge 1 ] && [ "$#" -le 2 ] || { echo "usage: archive_index.sh <label> [expected-snapshot-id]" >&2; exit 2; }
LABEL="$1"; EXPECT="${2:-}"
case "$LABEL" in *[!$LOWER$DIGITS-]*|"") echo "bad label '$LABEL'" >&2; exit 2;; esac
# A plain absolute path, so the staging and feeds-only folders beside it are
# never inside it.
ROOT="${ARCHIVE_ROOT:-$HOME/.cache/transitio-index/builds}"
ROOT="${ROOT%/}"
plain_path "$ROOT" || { echo "ARCHIVE_ROOT must be a plain absolute path" >&2; exit 2; }
STAGING="$ROOT.staging"
FEEDS_ONLY="$ROOT-feeds-only"
# Nothing under cache is written through a symlink.
if [ -L cache ] || [ -L cache/index ] || [ -L "$ROOT" ] || [ -L "$STAGING" ] || [ -L "$FEEDS_ONLY" ]; then
  echo "a symlink under cache" >&2; exit 2
fi
snapshot_id() { "$PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['snapshot_id'])" "$1"; }
SNAP=$(snapshot_id cache/index/snapshot.json)
case "$SNAP" in *[!$HEX]*|"") echo "bad snapshot id '$SNAP'" >&2; exit 2;; esac
# The index must be the snapshot the caller's build published, not a later one.
[ -z "$EXPECT" ] || [ "$SNAP" = "$EXPECT" ] || { echo "cache/index holds $SNAP, expected $EXPECT" >&2; exit 2; }
mkdir -p "$ROOT" "$STAGING"
# The final rename must not turn into a copy across file systems.
same_device() {
  "$PY" -c "import os,sys;sys.exit(os.stat(sys.argv[1]).st_dev != os.stat(sys.argv[2]).st_dev)" "$1" "$2" \
    || { echo "$1 and $2 are on different file systems" >&2; exit 2; }
}
same_device "$STAGING" "$ROOT"
for DEST in "$ROOT/$LABEL-$SNAP" "$FEEDS_ONLY/$LABEL-$SNAP"; do
  if [ -e "$DEST" ] || [ -L "$DEST" ]; then echo "$DEST already exists" >&2; exit 2; fi
done
STAGE=$(mktemp -d "$STAGING/$LABEL-$SNAP.XXXXXX")
trap 'rm -rf "$STAGE"' EXIT
mkdir "$STAGE/index"
cp -R cache/index/. "$STAGE/index/"
rm -f "$STAGE/index/.lock"
LINKS=$(find "$STAGE/index" -type l)
[ -z "$LINKS" ] || { echo "a symlink inside cache/index" >&2; exit 2; }
# The copy must be the snapshot the name says: a publish landing mid-copy
# would leave a mixed archive.
STAGED=$(snapshot_id "$STAGE/index/snapshot.json")
[ "$STAGED" = "$SNAP" ] || { echo "index changed during the copy ($SNAP -> $STAGED)" >&2; exit 2; }
# An index with places goes to the merge root with its map layer; one with
# feeds but no places to the feeds-only root, without a layer (the export
# reads places); one with neither has nothing to archive. What it holds is
# read from the counts its manifest records.
HOLDS=$("$PY" -c "import json,sys;c=json.load(open(sys.argv[1])).get('counts') or {};print('places' if c.get('places') else 'feeds' if c.get('feeds') else '')" "$STAGE/index/snapshot.json")
if [ "$HOLDS" = places ]; then
  DEST="$ROOT/$LABEL-$SNAP"
  OUT=$(mktemp "${TMPDIR:-/tmp}/export-layer.XXXXXX")
  status=0
  PYTHONPATH="" OMP_NUM_THREADS=1 PYTHONDONTWRITEBYTECODE=1 \
    "$PY" scripts/export_index_layer.py --index "$STAGE/index" --out-dir "$STAGE/layer" > "$OUT" 2>&1 || status=$?
  grep -vE "sysctlbyname|cpu_info|InitGoogle|WARNING: Logging|OMP:" "$OUT" || true
  rm -f "$OUT"
  [ "$status" -eq 0 ] || { echo "map layer export failed (exit $status)" >&2; exit "$status"; }
elif [ "$HOLDS" = feeds ]; then
  DEST="$FEEDS_ONLY/$LABEL-$SNAP"
  mkdir -p "$FEEDS_ONLY"
  same_device "$STAGING" "$FEEDS_ONLY"
  echo "note: feeds but no places in the index; map layer skipped, archived under $FEEDS_ONLY, outside the merge root"
else
  echo "no places and no feeds in the index; not archived" >&2; exit 2
fi
# The stats generation the build wrote for this snapshot, beside the index.
STATS=$("$PY" - "$SNAP" <<'PYEOF'
import json, sys
from pathlib import Path
from transitio_index import store
try:
    generation, manifest = store.resolve(Path("cache/stats"), "stats.json")
except Exception:
    sys.exit(0)
with generation:
    if manifest.get("snapshot_id") == sys.argv[1]:
        print(Path("cache/stats") / manifest["generation"])
PYEOF
)
if [ -n "$STATS" ] && [ -d "$STATS" ]; then
  mkdir "$STAGE/stats"
  for f in catalogue.parquet feeds.parquet places.parquet summary.json report.md manifest.json; do
    [ -f "$STATS/$f" ] && [ ! -L "$STATS/$f" ] && cp "$STATS/$f" "$STAGE/stats/"
  done
else
  echo "no stats generation for $SNAP; archived without stats/" >&2
fi
# Re-checked at publication: mv onto an existing directory would move inside it.
if [ -e "$DEST" ] || [ -L "$DEST" ]; then echo "$DEST appeared during archiving" >&2; exit 2; fi
mv "$STAGE" "$DEST"
trap - EXIT
echo "archived -> $DEST"; du -sh "$DEST"/* || true
