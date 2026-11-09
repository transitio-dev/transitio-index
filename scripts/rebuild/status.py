"""Print a rebuild's progress as Markdown tables, from its progress log, the
run's map and its label logs.

    python scripts/rebuild/status.py --run 2026-11 [--previous-archive DIR]

The run's files are in its run folder (--run-dir, default
~/.cache/transitio-index/runs/<run>; see common.sh): logs/progress.log,
logs/<label>.log, the maps, and the record of the first run, which names the
archive root (default ~/.cache/transitio-index/builds-<run>). A run starts at
a "REBUILD <run> <mode>: map <file>, ..." line of the progress log.
--previous-archive names an earlier rebuild's archive root, whose place
counts the time estimate uses for the labels not built yet."""

import argparse
import collections
import datetime as dt
import os
import re
import shutil
from pathlib import Path

# The run folder, its label logs, its archive root, the build checkout and an
# earlier rebuild's archive root; main() sets them for --run.
RUN_DIR = LOGS = ARCHIVE = PREVIOUS = None
REPO = Path(os.environ.get("BUILD_CHECKOUT") or Path(__file__).resolve().parents[2])
# The timestamp every progress-log line starts with.
STAMP = r"\[\d\d-\d\d \d\d:\d\d:\d\d\] "
ROW = re.compile(
    r"^\[(\d\d-\d\d \d\d:\d\d:\d\d)\] \((\d+)/(\d+)\) (OK|FAIL) (\S+)(?: \([^)]*\))? \| (.*)$"
)


def progress_time(text):
    """The time of a progress-log stamp (month, day and time): this year, or
    last year for a stamp that would lie more than a day ahead."""
    now = dt.datetime.now()
    time = dt.datetime.strptime(f"{now.year}-{text}", "%Y-%m-%d %H:%M:%S")
    return (
        time.replace(year=now.year - 1) if time > now + dt.timedelta(days=1) else time
    )


def coverage(archives):
    """Distinct place ids and feed ids across the archived builds' indexes,
    plus the per-build sums (a place held by two builds counts once in the
    distinct total)."""
    import pyarrow.parquet as pq

    place_ids, feed_ids = set(), set()
    place_rows = feed_rows = 0
    for archive in archives:
        for path in (archive / "index").rglob("places.parquet"):
            ids = (
                pq.read_table(path, columns=["place_id"]).column("place_id").to_pylist()
            )
            place_rows += len(ids)
            place_ids.update(ids)
        for path in (archive / "index").rglob("feeds.parquet"):
            ids = pq.read_table(path, columns=["feed_id"]).column("feed_id").to_pylist()
            feed_rows += len(ids)
            feed_ids.update(ids)
    return (len(place_ids), place_rows), (len(feed_ids), feed_rows)


def archive_dirs(root):
    """The archives under ``root``: real directories <label>-<16 hex> holding
    a regular index/snapshot.json, nothing on the way a symlink."""
    if not root.is_dir() or root.is_symlink():
        return []
    found = []
    for path in root.iterdir():
        index, snapshot = path / "index", path / "index/snapshot.json"
        if (
            re.fullmatch(r"[a-z0-9-]+-[0-9a-f]{16}", path.name)
            and path.is_dir()
            and not path.is_symlink()
            and index.is_dir()
            and not index.is_symlink()
            and snapshot.is_file()
            and not snapshot.is_symlink()
        ):
            found.append(path)
    return found


def run_archives():
    """The archives of the rebuild: under ARCHIVE and its feeds-only root."""
    return archive_dirs(ARCHIVE) + archive_dirs(
        ARCHIVE.with_name(ARCHIVE.name + "-feeds-only")
    )


def archive_places(archives, label):
    """Place rows in the newest archive of ``label`` among ``archives``, or
    None."""
    import pyarrow.parquet as pq

    own = [d for d in archives if d.name.rsplit("-", 1)[0] == label]
    if not own:
        return None
    newest = max(own, key=lambda d: (d / "index" / "snapshot.json").stat().st_mtime)
    return sum(
        pq.read_metadata(p).num_rows for p in (newest / "index").rglob("places.parquet")
    )


def model_estimate(minutes, todo, run_labels, succeeded, spent, archives):
    """Minutes of builds left, from a least-squares fit of each finished
    build's minutes on its catalogue entries and place count (``minutes``:
    label -> minutes), less what the builds under way have spent (``spent``:
    label -> minutes). Remaining builds take their place count from the
    previous rebuild's archive of the same label (``PREVIOUS``), else the
    median. Returns (minutes left, entries done, entries total, R²) or None
    without a cut or enough builds."""
    import json

    import numpy as np

    cut = cut_dir()
    if cut is None or len(minutes) < 5:
        return None
    if not regular_file(cut / "partition.json"):
        return None
    labels = json.loads((cut / "partition.json").read_text())["labels"]
    entries = {k: v["mdb_rows"] + v["atlas_feeds"] for k, v in labels.items()}
    rows = [
        (entries[k], archive_places(archives, k), m)
        for k, m in minutes.items()
        if k in entries
    ]
    rows = [r for r in rows if r[1] is not None]
    if len(rows) < 5:
        return None
    previous = []
    if PREVIOUS:
        feeds_only = PREVIOUS.with_name(PREVIOUS.name + "-feeds-only")
        previous = archive_dirs(PREVIOUS) + archive_dirs(feeds_only)
    x = np.array([[1, e, p] for e, p, _ in rows], float)
    y = np.array([m for _, _, m in rows])
    coef, *_ = np.linalg.lstsq(x, y, rcond=None)
    fitted = x @ coef
    r2 = 1 - ((y - fitted) ** 2).sum() / max(((y - y.mean()) ** 2).sum(), 1e-9)
    median = float(np.median(x[:, 2]))
    left = 0.0
    for label in todo:
        places = archive_places(previous, label)
        predicted = max(
            float(coef @ [1, entries[label], median if places is None else places]), 2.0
        )
        if label in spent:
            predicted = max(predicted - spent[label], 1.0)
        left += predicted
    done = sum(entries[k] for k in run_labels if k in succeeded and k in entries)
    return left, done, sum(entries[k] for k in run_labels if k in entries), r2


def run_window(lines, run):
    """The newest run of the rebuild ``run`` in the progress log: the index of
    its line, the index of the line the time model fits from (the rebuild's
    newest start), its map and its mode; None when it has no run yet."""
    pattern = re.compile(rf"{STAMP}REBUILD {re.escape(run)} (\w+): map (\S+),")
    runs = [
        (i, match) for i, line in enumerate(lines) if (match := pattern.match(line))
    ]
    if not runs:
        return None
    first, match = runs[-1]
    starts = [i for i, found in runs if found.group(1) == "first"]
    map_file = Path(match.group(2))
    # A run's map is a regular file in the run folder.
    if not (
        map_file.parent.resolve() == RUN_DIR.resolve()
        and re.fullmatch(r"map-[A-Za-z0-9-]+\.txt", map_file.name)
        and map_file.is_file()
        and not map_file.is_symlink()
    ):
        raise SystemExit(
            f"the progress log names an unexpected map for {run}: {map_file}"
        )
    return first, (starts[-1] if starts else first), map_file, match.group(1)


# A line that starts a run of any rebuild, which ends the run before it, and
# a run's stop line; both only at the start of a progress-log line.
RUN_START = re.compile(rf"{STAMP}REBUILD (?!START:)\S+ (first|rest|resume|fix)\b")
RUN_STOPPED = re.compile(rf"{STAMP}REBUILD (STOPPED|ENDED):")
CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
# A label, as the build scripts accept one.
LABEL = re.compile(r"[a-z0-9-]+")
# A line of a label's build log, which starts with its time and level.
LOG_LINE = re.compile(r"^\d\d:\d\d:\d\d (INFO|WARNING|ERROR) ")


def table_text(text):
    """``text`` safe inside a Markdown table cell and a terminal."""
    return CONTROL.sub("?", text).replace("|", "\\|").replace("`", "'")


def free_space():
    """Free GB on the cache volume and the archive root's volume (its nearest
    existing parent), one figure when they share a volume."""
    targets = {}
    for name, path in (("cache", REPO / "cache"), ("archive", ARCHIVE)):
        while not path.exists() and path != path.parent:
            path = path.parent
        targets.setdefault(path.stat().st_dev, (name, path))
    figures = [
        (name, shutil.disk_usage(path).free / 2**30) for name, path in targets.values()
    ]
    if len(figures) == 1:
        return f"{figures[0][1]:.0f} GB"
    return ", ".join(f"{free:.0f} GB {name}" for name, free in figures)


def regular_file(path):
    """Whether ``path`` is a regular file, not a link."""
    return path.is_file() and not path.is_symlink()


def cut_dir():
    """The run's cut in the checkout, as its ``partition`` file names it
    (``cache/sample/partition-<id>``), or None."""
    state = RUN_DIR / "partition"
    if not regular_file(state):
        return None
    name = state.read_text().strip()
    if not re.fullmatch(r"cache/sample/partition-[A-Za-z0-9_]+", name):
        return None
    cut = REPO / name
    return cut if cut.is_dir() and not cut.is_symlink() else None


def label_log(label):
    """The build log of ``label`` in the run's logs, None unless it is a
    regular file there."""
    log = LOGS / f"{label}.log"
    return log if regular_file(log) else None


def log_time(label):
    """When the build log of ``label`` was last written, or None without
    one; a log the scheduler replaces while it is read counts as none."""
    log = label_log(label)
    try:
        return None if log is None else dt.datetime.fromtimestamp(log.stat().st_mtime)
    except OSError:
        return None


def cut_labels():
    """The labels of the recorded cut, or None."""
    cut = cut_dir()
    if cut is None or not regular_file(cut / "rebuild_map.txt"):
        return None
    try:
        text = (cut / "rebuild_map.txt").read_text()
        return {line.split()[0] for line in text.splitlines() if line.strip()}
    except OSError:
        return None


def archived_labels(labels, archives):
    """The labels of ``labels`` with an archive among ``archives``."""
    return {path.name.rsplit("-", 1)[0] for path in archives} & labels


def main():
    """Print the status tables of the run --run names."""
    global RUN_DIR, LOGS, ARCHIVE, PREVIOUS
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--run", required=True, help="the rebuild to report, such as 2026-11"
    )
    parser.add_argument("--run-dir", type=Path, help="its run folder")
    parser.add_argument(
        "--previous-archive", type=Path, help="an earlier rebuild's archive root"
    )
    arguments = parser.parse_args()
    rebuild = arguments.run
    if not re.fullmatch(r"[0-9a-z-]+", rebuild):
        raise SystemExit(f"bad --run {rebuild!r}")
    RUN_DIR = Path(
        arguments.run_dir
        or os.environ.get("RUN_DIR")
        or Path.home() / f".cache/transitio-index/runs/{rebuild}"
    )
    LOGS = RUN_DIR / "logs"
    ARCHIVE = Path.home() / f".cache/transitio-index/builds-{rebuild}"
    PREVIOUS = arguments.previous_archive
    # A run's first start records its archive root on the record's second line.
    record = RUN_DIR / "started"
    if record.is_file() and not record.is_symlink():
        fields = record.read_bytes().split(b"\n")
        # Four lines (five with the Overture release) and a final line feed;
        # the root is plain ASCII, as the start script requires.
        if len(fields) in (5, 6) and re.fullmatch(rb"/[ -~]+", fields[1]):
            ARCHIVE = Path(fields[1].decode("ascii"))
    progress = LOGS / "progress.log"
    # Records end at a line feed only: other separators are text in a record.
    lines = (
        progress.read_text(errors="replace").split("\n")
        if regular_file(progress)
        else []
    )
    window = run_window(lines, rebuild)
    if window is None:
        print(f"No {rebuild} run in the progress log yet.")
        return
    first, started, map_file, mode = window
    # A run ends where the next run of any rebuild starts.
    end = next(
        (j for j in range(first + 1, len(lines)) if RUN_START.match(lines[j])),
        len(lines),
    )
    run = lines[first:end]
    begun = progress_time(run[0][1:15])
    labels = [
        line.split()[0] for line in map_file.read_text().splitlines() if line.strip()
    ]
    labels = list(dict.fromkeys(label for label in labels if LABEL.fullmatch(label)))
    wanted = set(labels)
    rows = [
        row
        for row in (ROW.match(line) for line in run)
        if row and row.group(5) in wanted
    ]
    rows = [row.groups() for row in rows]
    fit_rows = [ROW.match(line).groups() for line in lines[started:] if ROW.match(line)]
    # A label built more than once has one log for several results: left out.
    attempts = collections.Counter(row[4] for row in fit_rows)
    fit_rows = [
        row for row in fit_rows if LABEL.fullmatch(row[4]) and attempts[row[4]] == 1
    ]
    latest = {}
    for row in rows:
        latest[row[4]] = row
    # Each label's latest outcome, in the order they finished.
    done = sorted(latest.values(), key=lambda row: row[0])
    total = len(labels)
    ok = sum(1 for row in done if row[3] == "OK")
    fail = [row[4] for row in done if row[3] == "FAIL"]
    finished = {row[4] for row in done}
    complete = finished == wanted and all(row[3] == "OK" for row in done)
    # A later run's start ends this one, finished or not.
    # A run that has not finished every label ends as stopped at its stop line
    # or at a later run's start.
    stopped = finished != wanted and (
        end < len(lines) or any(RUN_STOPPED.match(line) for line in run)
    )
    over = finished == wanted or stopped
    now = dt.datetime.now()
    succeeded = {row[4] for row in done if row[3] == "OK"}
    # The labels being built: unfinished ones whose log this run wrote (builds
    # run side by side with SLOTS), else the next of the map.
    running = [
        label
        for label in labels
        if not over
        and label not in finished
        and (written := log_time(label)) is not None
        and written >= begun
    ]
    if not over and not running:
        running = [label for label in labels if label not in finished][:1]

    building = {}
    for current in running:
        stage, since, last_line = "-", None, ""
        log = label_log(current)
        try:
            text = log.read_text(errors="replace") if log else ""
        except OSError:  # replaced while read
            text = ""
        log_lines = text.replace("\r", "\n").splitlines()
        info = [line for line in log_lines if LOG_LINE.match(line)]
        if info:
            last_line = table_text(info[-1][:110])
            parts = info[-1].split()
            stage = table_text(parts[2].rstrip(":")) if len(parts) > 2 else "-"
            hh, mm, ss = map(int, info[0][:8].split(":"))
            since = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
            if since > now:
                since -= dt.timedelta(days=1)
        building[current] = (stage, since, last_line)

    # A run that is over ran until its last result or rebuild line; a merge or
    # a release logged after it is not part of it.
    own = [line for line in run if re.match(STAMP + r"(\(\d+/\d+\) |REBUILD )", line)]
    elapsed = (progress_time(own[-1][1:15]) if over and own else now) - begun
    per = None
    spans = []
    minutes = {}
    current_rows = {row for row in rows}
    for row in fit_rows:
        log = label_log(row[4])
        if log is None:
            continue
        lines_of_log = log.read_text(errors="replace").splitlines()
        first = next((line for line in lines_of_log if LOG_LINE.match(line)), None)
        if first:
            end = progress_time(row[0])
            hh, mm, ss = map(int, first[:8].split(":"))
            start = end.replace(hour=hh, minute=mm, second=ss)
            if start > end:
                start -= dt.timedelta(days=1)
            if row in current_rows:
                spans.append(end - start)
            if row[3] == "OK":
                minutes[row[4]] = (end - start).total_seconds() / 60
    if spans:
        per = sum(spans, dt.timedelta()) / len(spans)
    # Builds side by side share the time left.
    parallel = max(1, len(running))
    eta = (per * (total - len(done)) / parallel) if per and not over else None
    archives = run_archives()
    places, feeds = coverage(archives)

    def hm(delta):
        if delta is None:
            return "-"
        minutes = int(delta.total_seconds() // 60)
        return f"{minutes // 60} h {minutes % 60:02d} min"

    spent = {
        label: (now - since).total_seconds() / 60
        for label, (_, since, _) in building.items()
        if since
    }
    todo = [label for label in labels if label not in finished]
    model = (
        None
        if over
        else model_estimate(minutes, todo, labels, succeeded, spent, archives)
    )
    if model:
        left, entries_done, entries_total, r2 = model
        eta = dt.timedelta(minutes=left / parallel)
        share = 100 * entries_done / entries_total
        entries = f"{entries_done:,} of {entries_total:,} ({share:.0f}%)"
    else:
        entries = "-"
    cut = cut_labels()
    in_cut = f"{len(archived_labels(cut, archives))} of {len(cut)}" if cut else "-"
    free = free_space()
    print(f"Rebuild {rebuild} ({mode}) at {now:%H:%M}:\n")
    failed = f"{len(fail)} ({', '.join(fail)})" if fail else "0"
    columns = {
        "Builds done": f"{len(done)} of {total}",
        "OK": str(ok),
        "Failed": failed,
        "Archived of the cut": in_cut,
        "Places (distinct)": f"{places[0]:,}",
        "Feeds (distinct)": f"{feeds[0]:,}",
        "Catalogue entries done": entries,
        "Free disk": free,
        "Running for": hm(elapsed),
        "Estimate left": hm(eta),
    }
    print("| " + " | ".join(columns) + " |")
    print("|" + "---|" * len(columns))
    print("| " + " | ".join(columns.values()) + " |")
    if over:
        word = (
            "stopped"
            if stopped
            else "complete" if complete else "finished with failures"
        )
        print(f"\nThis run ({mode}) is {word}.")
        return
    print()
    print("| Now building | Stage | Started | Last log line |")
    print("|---|---|---|---|")
    for current, (stage, since, last_line) in building.items():
        print(
            f"| {current} | {stage} | {since:%H:%M} | `{last_line}` |"
            if since
            else f"| {current} | - | - | - |"
        )
    if done:
        print(
            "\nLast finished: "
            + ", ".join(f"{row[4]} {row[3]} ({row[0][6:11]})" for row in done[-5:])
        )


if __name__ == "__main__":
    main()
