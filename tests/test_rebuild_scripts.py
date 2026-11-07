"""The rebuild scripts in scripts/rebuild: their refusals and the archive of
one published index."""

import json
import os
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


def _run_script(name, *args, checkout, **env):
    """Run ``scripts/rebuild/<name>`` with ``args`` over the build checkout
    ``checkout``, with ``env`` added to the environment."""
    environ = {
        **os.environ,
        "BUILD_CHECKOUT": str(checkout),
        "PYTHON": sys.executable,
        "PYTHONPATH": str(_REPO),
        "LOGDIR": str(checkout / "logs"),
        # A locale in which a range such as a-z also matches capitals.
        "LC_ALL": "en_US.UTF-8",
        **env,
    }
    return subprocess.run(
        [str(_SCRIPTS / name), *args],
        cwd=checkout,
        env=environ,
        capture_output=True,
        text=True,
        check=False,
    )


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
        ("archive_index.sh", ["us2"], [], {"ARCHIVE_ROOT": "builds"}, "absolute"),
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
            f"[11-07 10:00:00] REBUILD 2026-11 first: map {run_dir}/map-check.txt, 2 labels\n"
        )
        log.write(
            "[11-07 11:00:00] (1/2) OK us2 (fresh crawl) | [us2 done: runner exit 0]\n"
        )
    running = status()
    assert "| 1 of 2 | 1 | 0 |" in running and "| fr1 |" in running
    with progress.open("a") as log:
        log.write(
            "[11-07 12:00:00] (2/2) FAIL fr1 (fresh crawl) | [fr1 done: runner exit 1]\n"
        )
    assert status().rstrip().endswith("This run (first) is finished with failures.")
