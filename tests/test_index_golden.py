import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import golden, store  # noqa: E402


def _write_cache(tmp_path, edges):
    cache = tmp_path / "cache"
    directory = store.open_subdir(cache, "coverage")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "coverage",
                "coverage.json",
                {
                    "edges_candidate.jsonl": store.jsonl_chunks(edges),
                    "feeds_covered.jsonl": store.jsonl_chunks([]),
                },
                {"source": "coverage"},
                held=directory,
            )
    finally:
        directory.close()
    return cache


def _entry(feed_id, membership, **extra):
    return {
        "feed_id": feed_id,
        "name": feed_id,
        "why": "fixture",
        "membership": membership,
        "tiers": ["local"],
        "review_state": "confident",
        **extra,
    }


def _golden_file(tmp_path, entries):
    path = tmp_path / "golden.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))
    return path


def _edge(feed_id, place_id):
    return {"feed_id": feed_id, "place_id": place_id}


def test_membership_contracts_are_checked(tmp_path):
    cache = _write_cache(
        tmp_path,
        [
            _edge("f-ok", "Q1"),
            _edge("f-ok", "Q2"),
            _edge("f-short", "Q1"),
            _edge("f-banned", "Q1"),
            _edge("f-banned", "Q9"),
            _edge("f-loose", "Q1"),
            _edge("f-loose", "Q9"),
        ],
    )
    path = _golden_file(
        tmp_path,
        [
            _entry("f-ok", ["Q1", "Q2"]),
            _entry("f-short", ["Q1", "Q2"]),
            _entry("f-banned", ["Q1"], membership_excludes=["Q9"]),
            _entry("f-loose", ["Q1"], membership_exact=True),
            _entry("f-gone", ["Q1"]),
        ],
    )
    report = golden.check(cache, path)
    assert report["entries"] == 5
    assert report["passed"] is False
    problems = {(v["feed_id"], v["problem"]) for v in report["violations"]}
    assert problems == {
        ("f-short", "membership missing places"),
        ("f-banned", "membership includes excluded places"),
        ("f-loose", "membership differs"),
        ("f-gone", "feed has no edges"),
    }


def test_a_clean_build_passes(tmp_path):
    cache = _write_cache(tmp_path, [_edge("f-ok", "Q1"), _edge("f-ok", "Q2")])
    path = _golden_file(tmp_path, [_entry("f-ok", ["Q1"])])
    report = golden.check(cache, path)
    assert report["passed"] is True
    assert golden.main(["--cache-dir", str(cache), "--golden", str(path)]) == 0


@pytest.mark.parametrize(
    "entry",
    [
        {"feed_id": "f-a"},  # missing keys
        _entry("f-a", []),  # empty membership
        _entry("f-a", ["not-a-qid"]),
        _entry("f-a", ["Q1"], review_state="maybe"),
        _entry("f-a", ["Q1"], tiers=[]),
        _entry("f-a", ["Q1"], tiers="local"),
        _entry("f-a", ["Q1"], tiers=["municipal"]),
        _entry("f-a", ["Q1"], membership_exact=0),
        _entry("f-a", ["Q1"], membership_exact=True, membership_excludes=["Q2"]),
        [1, 2],  # not an object
    ],
)
def test_a_malformed_golden_file_is_refused(tmp_path, entry):
    path = _golden_file(tmp_path, [_entry("f-b", ["Q1"]), entry])
    with pytest.raises(golden.GoldenError):
        golden.load_golden(path)


def test_a_duplicate_golden_feed_is_refused(tmp_path):
    path = _golden_file(tmp_path, [_entry("f-a", ["Q1"]), _entry("f-a", ["Q2"])])
    with pytest.raises(golden.GoldenError, match="duplicate"):
        golden.load_golden(path)


def test_the_committed_golden_file_matches_its_catalogue_evidence():
    # The evidence file is captured from a real ingest+crosswalk of the
    # pinned catalogues; a typo'd or renamed golden feed id fails here, not
    # at the first real build.
    report = golden.check_catalogue_evidence(
        REPO / "golden" / "feeds.jsonl",
        REPO / "golden" / "catalogue_evidence.jsonl",
    )
    assert report == {"unevidenced": [], "orphaned": []}


def test_duplicate_evidence_rows_are_refused(tmp_path):
    golden_path = _golden_file(tmp_path, [_entry("f-a", ["Q1"])])
    evidence = tmp_path / "evidence.jsonl"
    evidence.write_text(
        json.dumps({"feed_id": "f-a"}) + "\n" + json.dumps({"feed_id": "f-a"}) + "\n"
    )
    with pytest.raises(golden.GoldenError, match="duplicate evidence"):
        golden.check_catalogue_evidence(golden_path, evidence)


def test_the_committed_golden_file_is_valid():
    entries = golden.load_golden(REPO / "golden" / "feeds.jsonl")
    assert len(entries) == 20
    by_id = {e["feed_id"]: e for e in entries}
    assert by_id["f-dr5r-nyctsubway"]["membership"] == ["Q60", "Q683705"]
    # The seed spans the plan's required shapes.
    whys = " ".join(e["why"] for e in entries)
    for marker in (
        "MTA-style split",
        "national aggregate",
        "auth-gated",
        "long-distance",
    ):
        assert marker in whys
