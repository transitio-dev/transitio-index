"""The rebuild scripts in scripts/rebuild: their refusals and the archive of
one published index."""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell scripts")

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts" / "rebuild"
_REPO = _SCRIPTS.parent.parent
_CUT = [
    "cache/sample/run-a/atlas_sample.tar.gz",
    "cache/sample/run-a/mdb_sample.csv",
    "cache/sample/run-a/gbfs_sample.csv",
]


def _script_environ(checkout, **env):
    """The environment a script runs in over the build checkout ``checkout``,
    with ``env`` added."""
    return {
        **os.environ,
        "BUILD_CHECKOUT": str(checkout),
        "PYTHON": sys.executable,
        "PYTHONPATH": str(_REPO),
        "LOGDIR": str(checkout / "logs"),
        # A locale in which a range such as a-z also matches capitals.
        "LC_ALL": "en_US.UTF-8",
        **env,
    }


def _run_script(name, *args, checkout, **env):
    """Run ``scripts/rebuild/<name>`` with ``args`` over the build checkout
    ``checkout``, with ``env`` added to the environment."""
    return subprocess.run(
        [str(_SCRIPTS / name), *args],
        cwd=checkout,
        env=_script_environ(checkout, **env),
        capture_output=True,
        text=True,
        check=False,
    )


def _slot_checkout(tmp_path):
    """A checkout on main with a map of three labels, aa, bb and cc, and a
    stub label build (noted at its start and end in ``RECORD``, with a crawl
    to park; bb fails, and with ``STOP=crawl`` leaves its crawl unparkable);
    returns the environment the scheduler runs with."""
    stub = tmp_path / "stub.sh"
    stub.write_text(
        "#!/bin/sh\n"
        'echo "start $1 $CACHE $WORKERS $WAIT_FOR_REGISTRY" >> "$RECORD"\n'
        'mkdir -p "$CACHE/crawl"\n'
        "sleep 1\n"
        'echo "end $1" >> "$RECORD"\n'
        'echo "built $1" > "$LOGDIR/$1.log"\n'
        '[ "$1:${STOP:-}" != bb:crawl ] || : > cache/crawl.bb.bak\n'
        '[ "$1" != bb ]\n'
    )
    stub.chmod(0o755)
    git = ["git", "-C", str(tmp_path), "-c", "user.name=t", "-c", "user.email=t@t"]
    subprocess.run(["git", "init", "-q", "-b", "main", str(tmp_path)], check=True)
    subprocess.run([*git, "commit", "-q", "--allow-empty", "-m", "t"], check=True)
    (tmp_path / "cache").mkdir()
    (tmp_path / "map.txt").write_text(
        "".join(f"{label} cache/sample/run-{label}_a\n" for label in ("aa", "bb", "cc"))
    )
    return {"BUILD_LABEL": str(stub), "RECORD": str(tmp_path / "record.txt")}


@pytest.mark.parametrize(
    "script, args, made, env, message",
    [
        ("build_label.sh", ["us2"], [], {}, "usage"),
        ("build_label.sh", ["US2", "cache/sample/run-a"], [], {}, "bad label"),
        ("build_label.sh", ["us2", "elsewhere/run-a"], [], {}, "bad run dir"),
        ("build_label.sh", ["us2", "cache/sample/run-a"], [], {}, "no such run dir"),
        (
            "build_label.sh",
            ["us2", "cache/sample/run-a"],
            ["cache/sample/run-a/"],
            {},
            "atlas_sample.tar.gz missing",
        ),
        (
            "build_label.sh",
            ["us2", "cache/sample/run-a"],
            _CUT,
            {"OVERTURE_RELEASE": "latest"},
            "bad OVERTURE_RELEASE",
        ),
        (
            "build_label.sh",
            ["us2", "cache/sample/run-a"],
            [],
            {"CACHE": "c"},
            "bad CACHE",
        ),
        ("archive_index.sh", ["us2"], [], {"ARCHIVE_ROOT": "builds"}, "absolute"),
        (
            "archive_index.sh",
            ["us2"],
            [],
            {"ARCHIVE_ROOT": "/x/builds", "CACHE": "cache/slot-0"},
            "bad CACHE",
        ),
        ("archive_index.sh", ["us2"], [], {"ARCHIVE_ROOT": "/x/builds/."}, "plain"),
        ("build_labels.sh", ["missing.txt"], [], {}, "no map file"),
        # The last line counts without a newline after it.
        (
            "build_labels.sh",
            ["map.txt"],
            [("map.txt", "us2 cache/sample/run-a\n../x cache/sample/run-b")],
            {},
            "bad map line",
        ),
        ("build_labels.sh", ["map.txt"], [("map.txt", "\n  \n")], {}, "no labels"),
        (
            "build_labels.sh",
            ["map.txt"],
            [("map.txt", "us2 cache/sample/run-a\nus2 cache/sample/run-b\n")],
            {},
            "us2 is twice",
        ),
        (
            "build_labels.sh",
            ["map.txt"],
            [("map.txt", "")],
            {"SLOTS": "10"},
            "bad SLOTS",
        ),
        (
            "build_labels.sh",
            ["map.txt"],
            [("map.txt", "")],
            {"SLOTS": "2", "STOP_IF_FIRST_FAILS": "1"},
            "needs SLOTS=1",
        ),
        ("rebuild.sh", ["2026-11", "check"], [], {"SLOTS": "0"}, "bad SLOTS"),
        ("rebuild.sh", ["2026-11"], [], {}, "usage"),
        ("rebuild.sh", ["2026-11", "go"], [], {}, "usage"),
        ("rebuild.sh", ["Run 1", "check"], [], {}, "bad run name"),
        ("rebuild.sh", ["2026-11", "check"], [], {"RUN_DIR": "runs/a"}, "plain"),
        ("release.sh", ["2026-11"], [], {}, "usage"),
        ("release.sh", ["2026-11", "go"], [], {}, "usage"),
        (
            "release.sh",
            ["2026-11", "merge"],
            [],
            {"RUN_DIR": "/no/run"},
            "no run folder",
        ),
        (
            "rebuild.sh",
            ["2026-11", "check"],
            [],
            {"RUN_DIR": "/nonexistent/run"},
            "no run folder",
        ),
    ],
)
def test_the_rebuild_scripts_refuse_bad_input(
    tmp_path, script, args, made, env, message
):
    for path in made:
        path, text = path if isinstance(path, tuple) else (path, "")
        target = tmp_path / path
        if path.endswith("/"):
            target.mkdir(parents=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
    result = _run_script(script, *args, checkout=tmp_path, **env)
    assert result.returncode == 2 and message in result.stderr, result.stderr


def test_a_published_index_is_archived_once(tmp_path):
    # An index with feeds and no places goes to the feeds-only root, outside
    # the merge root, through a staging folder that is emptied after.
    index = tmp_path / "cache" / "index"
    index.mkdir(parents=True)
    # The manifest's counts decide: an empty places table is no places.
    counts = {"feeds": 1, "places": 0}
    snapshot = {"snapshot_id": "ab12" * 4, "counts": counts}
    (index / "snapshot.json").write_text(json.dumps(snapshot))
    (index / "places.parquet").write_bytes(b"")
    env = {"ARCHIVE_ROOT": str(tmp_path / "builds")}
    (index / "snapshot.json").write_text(json.dumps({**snapshot, "snapshot_id": 12}))
    numeric = _run_script("archive_index.sh", "us2", checkout=tmp_path, **env)
    assert numeric.returncode == 2 and "bad snapshot id" in numeric.stderr
    (index / "snapshot.json").write_text(json.dumps(snapshot))
    wrong = _run_script("archive_index.sh", "us2", "cd34", checkout=tmp_path, **env)
    assert wrong.returncode == 2 and "expected cd34" in wrong.stderr
    (index / "linked.parquet").symlink_to(tmp_path / "elsewhere.parquet")
    linked = _run_script("archive_index.sh", "us2", checkout=tmp_path, **env)
    assert linked.returncode == 2 and "symlink inside" in linked.stderr
    (index / "linked.parquet").unlink()
    first = _run_script("archive_index.sh", "us2", "ab12" * 4, checkout=tmp_path, **env)
    assert first.returncode == 0, first.stderr
    archived = tmp_path / "builds-feeds-only" / f"us2-{'ab12' * 4}" / "index"
    assert (
        json.loads((archived / "snapshot.json").read_text())["snapshot_id"]
        == "ab12" * 4
    )
    assert list((tmp_path / "builds.staging").iterdir()) == []
    again = _run_script("archive_index.sh", "us2", checkout=tmp_path, **env)
    assert again.returncode == 2 and "already exists" in again.stderr


@pytest.mark.skipif(not shutil.which("lsof"), reason="the scripts need lsof")
@pytest.mark.parametrize("slots", [1, 2])
def test_slots_build_labels_side_by_side_in_caches_of_their_own(tmp_path, slots):
    env = _slot_checkout(tmp_path)
    (tmp_path / "cache" / "crawl.cc.bak").mkdir()
    result = _run_script(
        "build_labels.sh", "map.txt", checkout=tmp_path, SLOTS=str(slots), **env
    )
    assert result.returncode == 0, result.stderr
    events = (tmp_path / "record.txt").read_text().splitlines()
    if slots == 1:
        # One after another in cache, on four crawl threads.
        assert events[:2] == ["start aa cache 4 ", "end aa"]
    else:
        # aa and bb start together, each in its slot's cache, taking registry
        # turns on two crawl threads; cc waits for a slot.
        assert sorted(events[:2]) == [
            "start aa cache/slot-1 2 1",
            "start bb cache/slot-2 2 1",
        ]
    assert [e.split()[1] for e in events if e.startswith("start")][2] == "cc"
    progress = (tmp_path / "logs" / "progress.log").read_text()
    for line in (
        "(1/3) OK aa (fresh crawl)",
        "(2/3) FAIL bb (fresh crawl)",
        "(3/3) OK cc (parked crawl)",
        "REBUILD COMPLETE: ok=2 fail=1 of 3",
    ):
        assert line in progress, progress
    # Every crawl is parked in cache, none left in a slot.
    for label in ("aa", "bb", "cc"):
        assert (tmp_path / "cache" / f"crawl.{label}.bak").is_dir()
    assert not list((tmp_path / "cache").glob("slot-*/crawl"))


@pytest.mark.skipif(not shutil.which("lsof"), reason="the scripts need lsof")
@pytest.mark.parametrize("stop", ["crawl", "term"])
def test_slots_stop_together_and_log_it_once(tmp_path, stop):
    # bb's crawl cannot be parked, or the scheduler is asked to stop while
    # aa and bb build: cc never starts, and the stop is logged once, last.
    env = _slot_checkout(tmp_path)
    record = Path(env["RECORD"])
    run = subprocess.Popen(
        [str(_SCRIPTS / "build_labels.sh"), "map.txt"],
        cwd=tmp_path,
        env=_script_environ(tmp_path, SLOTS="2", STOP=stop, **env),
    )
    if stop == "term":
        for _ in range(100):
            if record.exists() and record.read_text().count("start") == 2:
                break
            time.sleep(0.05)
        run.send_signal(signal.SIGTERM)
    assert run.wait(timeout=30) == 1
    assert "cc" not in record.read_text()
    progress = (tmp_path / "logs" / "progress.log").read_text().splitlines()
    stops = [line for line in progress if "REBUILD STOPPED" in line]
    reason = "cannot park the crawl of bb" if stop == "crawl" else "asked to stop"
    assert len(stops) == 1 and reason in stops[0] and progress[-1] == stops[0]
    # A label whose crawl cannot be parked logs the stop instead of a result.
    outcomes = [line for line in progress if " aa (" in line or " bb (" in line]
    assert len(outcomes) == (1 if stop == "crawl" else 2)


def test_the_supervisor_records_how_the_run_ended(tmp_path):
    progress = tmp_path / "progress.log"
    supervise = str(_SCRIPTS / "supervise.sh")
    done = subprocess.run([supervise, str(progress), "sh", "-c", "exit 3"], check=False)
    assert done.returncode == 3
    assert progress.read_text().endswith("REBUILD ENDED: sh exited with code 3\n")
    # A TERM is passed on, and the command's own status is the one recorded.
    ready = tmp_path / "ready"
    child = f'trap "exit 7" TERM; : > "{ready}"; sleep 30 & wait'
    lock = tmp_path / ".lock"
    environ = {**os.environ, "RUN_LOCK": str(lock)}
    run = subprocess.Popen([supervise, str(progress), "sh", "-c", child], env=environ)
    lock.mkdir()
    (lock / "pid").write_text(f"{run.pid}\n")
    for _ in range(100):
        if ready.exists():
            break
        time.sleep(0.05)
    run.send_signal(signal.SIGTERM)
    assert run.wait(timeout=10) == 7
    assert progress.read_text().endswith("REBUILD ENDED: sh exited with code 7\n")
    # The run lock it was handed, still naming it, is released.
    assert not lock.exists()


def test_the_status_tables_follow_a_run(tmp_path):
    pytest.importorskip("pyarrow")
    run_dir, archive = tmp_path / "runs" / "2026-11", tmp_path / "builds"
    (run_dir / "logs").mkdir(parents=True)
    progress = run_dir / "logs" / "progress.log"
    progress.write_text("")
    (run_dir / "started").write_text(f"abc\n{archive}\nmdb\ninputs\n2026-10-21.0\n")
    (run_dir / "map-check.txt").write_text(
        "us2 cache/sample/run-us2_a\nfr1 cache/sample/run-fr1_b\n"
        "de cache/sample/run-de_c\n"
    )

    def status():
        result = subprocess.run(
            [sys.executable, str(_SCRIPTS / "status.py"), "--run", "2026-11"]
            + ["--run-dir", str(run_dir)],
            env={**os.environ, "BUILD_CHECKOUT": str(tmp_path)},
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    assert status().startswith("No 2026-11 run")
    with progress.open("a") as log:
        log.write(
            f"[11-07 10:00:00] REBUILD 2026-11 first: map {run_dir}/map-check.txt, 3 labels\n"
        )
        log.write(
            "[11-07 11:00:00] (1/3) OK us2 (fresh crawl) | [us2 done: runner exit 0]\n"
        )
    running = status()
    assert "| 1 of 3 | 1 | 0 |" in running and "| fr1 |" in running
    assert "| de |" not in running
    # Builds side by side: every label whose log this run wrote.
    for label in ("fr1", "de"):
        (run_dir / "logs" / f"{label}.log").write_text("10:01:00 INFO crawl: 1/2\n")
    running = status()
    assert "| fr1 | crawl |" in running and "| de | crawl |" in running
    with progress.open("a") as log:
        log.write(
            "[11-07 12:00:00] (2/3) FAIL fr1 (fresh crawl) | [fr1 done: runner exit 1]\n"
        )
        log.write(
            "[11-07 12:10:00] (3/3) OK de (fresh crawl) | [de done: runner exit 0]\n"
        )
    assert status().rstrip().endswith("This run (first) is finished with failures.")
