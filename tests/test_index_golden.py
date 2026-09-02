import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import golden, publish, store  # noqa: E402


def _generation(cache, subdir, pointer, artifacts, manifest):
    directory = store.open_subdir(cache, subdir)
    try:
        with store.exclusive_writer(directory):
            return store.publish(
                cache / subdir,
                pointer,
                {name: store.jsonl_chunks(rows) for name, rows in artifacts.items()},
                manifest,
                held=directory,
            )
    finally:
        directory.close()


def _write_cache(
    tmp_path, edges, *, stage="coverage", unknown_share=None, previous_share=None
):
    """A cache whose latest edge stage holds ``edges``; a classify
    generation is stamped with the coverage and expanded generations it
    descends from, as the real stage does."""
    cache = tmp_path / "cache"
    coverage_edges = [] if stage == "classify" else edges
    expanded_manifest = None
    if stage == "classify":
        expanded_manifest = _generation(
            cache,
            "gazetteer",
            "expanded.json",
            {"places_expanded.jsonl": []},
            {"source": "expand"},
        )
    coverage_manifest = _generation(
        cache,
        "coverage",
        "coverage.json",
        {"edges_candidate.jsonl": coverage_edges, "feeds_covered.jsonl": []},
        {
            "source": "coverage",
            "expanded_generation": (
                expanded_manifest["generation"] if expanded_manifest else None
            ),
        },
    )
    if stage == "classify":
        _generation(
            cache,
            "classify",
            "edges.json",
            {"edges.jsonl": edges, "feeds_classified.jsonl": []},
            {
                "source": "classify",
                "coverage_generation": coverage_manifest["generation"],
                "expanded_generation": expanded_manifest["generation"],
                "unknown_share": unknown_share,
            },
        )
    if previous_share is not None:
        (cache / "index").mkdir()
        (cache / "index" / "snapshot.json").write_text(
            json.dumps({"unknown_share": previous_share})
        )
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


def _edge(feed_id, place_id, tier="unknown", needs_review=True):
    return {
        "feed_id": feed_id,
        "place_id": place_id,
        "tier": tier,
        "needs_review": needs_review,
        "service": None,
    }


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


def test_tier_and_review_state_contracts_are_checked(tmp_path):
    # Classified edges: a matching feed, one whose tier set grew, one whose
    # review state crossed the cutoff, and edges outside the membership list
    # that must not count.
    cache = _write_cache(
        tmp_path,
        [
            _edge("f-ok", "Q1", "local", False),
            _edge("f-ok", "Q9", "national", True),  # every edge counts
            _edge("f-grew", "Q1", "local", False),
            _edge("f-grew", "Q1", "regional", False),
            _edge("f-crossed", "Q1", "local", True),
        ],
        stage="classify",
    )
    path = _golden_file(
        tmp_path,
        [
            _entry(
                "f-ok", ["Q1"], tiers=["local", "national"], review_state="needs_review"
            ),
            _entry("f-grew", ["Q1"]),
            _entry("f-crossed", ["Q1"]),
        ],
    )
    assert golden.check(cache, path, assert_tiers=False)["passed"] is True
    # Classified edges switch the tier assertions on by themselves.
    report = golden.check(cache, path)
    problems = {(v["feed_id"], v["problem"]) for v in report["violations"]}
    assert problems == {
        ("f-grew", "tier set differs"),
        ("f-crossed", "review state crossed the cutoff"),
    }


@pytest.mark.parametrize(
    ("previous", "passes"), [(0.30, False), (0.38, True), (None, True)]
)
def test_the_unknown_share_may_not_drift_past_five_points(tmp_path, previous, passes):
    cache = _write_cache(
        tmp_path,
        [_edge("f-ok", "Q1", "local", False)],
        stage="classify",
        unknown_share=0.40,
        previous_share=previous,
    )
    path = _golden_file(tmp_path, [_entry("f-ok", ["Q1"])])
    report = golden.check(cache, path)
    assert report["passed"] is passes
    if not passes:
        assert report["violations"][0]["problem"] == "unknown share drifted"


def test_publish_runs_the_golden_gate_first(tmp_path):
    cache = _write_cache(
        tmp_path, [_edge("f-ok", "Q1", "local", False)], stage="classify"
    )
    edges = [_edge("f-ok", "Q1", "local", False)]
    manifest = {"source": "classify"}
    path = _golden_file(tmp_path, [_entry("f-ok", ["Q2"])])
    with pytest.raises(publish.PublishError, match="golden diff failed"):
        publish._golden_gate(cache, path, edges, manifest)
    path = _golden_file(tmp_path, [_entry("f-ok", ["Q1"])])
    assert publish._golden_gate(cache, path, edges, manifest)["entries"] == 1


def test_an_unreadable_previous_snapshot_fails_the_gate(tmp_path):
    cache = _write_cache(
        tmp_path,
        [_edge("f-ok", "Q1", "local", False)],
        stage="classify",
        unknown_share=0.4,
    )
    (cache / "index").mkdir()
    path = _golden_file(tmp_path, [_entry("f-ok", ["Q1"])])
    (cache / "index" / "snapshot.json").write_text("not json")
    with pytest.raises(golden.GoldenError, match="previous snapshot"):
        golden.check(cache, path)
    (cache / "index" / "snapshot.json").write_text('{"unknown_share": NaN}')
    with pytest.raises(golden.GoldenError, match="finite"):
        golden.check(cache, path)


def test_the_gate_checks_the_edges_it_is_handed(tmp_path):
    # No edge generation on disk at all: the caller's edges are the truth.
    path = _golden_file(tmp_path, [_entry("f-ok", ["Q1"])])
    edges = [_edge("f-ok", "Q1", "local", False)]
    report = golden.check(
        tmp_path / "empty", path, edges=edges, manifest={"source": "classify"}
    )
    assert report["passed"] is True
    report = golden.check(
        tmp_path / "empty", path, edges=[], manifest={"source": "classify"}
    )
    assert report["violations"][0]["problem"] == "feed has no edges"


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
        _entry("f-a", ["Q1"], membership_excludes=0),
        _entry("f-a", ["Q1"], membership_excludes=""),
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
