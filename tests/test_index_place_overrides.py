"""The places.yaml loader and the shared override staleness helpers."""

import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import overrides  # noqa: E402


def write_overrides(tmp_path, *, places=None, feeds=None, name="overrides"):
    """An overrides directory holding the given places.yaml / feeds.yaml."""
    directory = tmp_path / name
    directory.mkdir(parents=True, exist_ok=True)
    if places is not None:
        (directory / "places.yaml").write_text(yaml.safe_dump(places, sort_keys=False))
    if feeds is not None:
        (directory / "feeds.yaml").write_text(yaml.safe_dump(feeds, sort_keys=False))
    return directory


def test_every_operation_loads_with_its_operation_key(tmp_path):
    entries, digest = overrides.load_place_overrides(
        write_overrides(
            tmp_path,
            places=[
                {
                    "place": "Q1",
                    "add_place": {"kind": "metro", "name": "M", "member_ids": ["Q2"]},
                },
                {
                    "place": "Q3",
                    "add_place": {
                        "kind": "city",
                        "name": "C",
                        "parent_id": "Q4",
                        "boundary": "POLYGON((0 0,1 0,1 1,0 0))",
                    },
                },
                {"place": "Q1", "set_place_members": ["Q2", "Q5"]},
                {"place": "Q3", "set_boundary": "POLYGON((0 0,1 0,1 1,0 0))"},
                {"place": "Q3", "set_aliases": ["Old name"]},
                {"place": "Q6", "source_ref": "ov-6", "resolve_place": True},
            ],
        )
    )
    assert [e["operation"] for e in entries] == [
        "add_place",
        "add_place",
        "set_place_members",
        "set_boundary",
        "set_aliases",
        "resolve_place",
    ]
    assert len(digest) == 64
    assert overrides.load_place_overrides(None) == ([], None)
    assert overrides.load_place_overrides(tmp_path / "nothing") == ([], None)


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"place": "Q1"}, "exactly one operation"),
        ({"place": "", "set_aliases": ["x"]}, "needs a 'place' id"),
        (
            {
                "place": "not-a-qid",
                "add_place": {"kind": "country", "name": "X", "boundary": "P"},
            },
            "needs a real QID",
        ),
        ({"place": "Q1", "set_aliases": ["x"], "bogus": 1}, "unknown keys"),
        (
            {"place": "Q1", "set_aliases": ["x"], "source_ref": "ov"},
            "only resolve_place",
        ),
        ({"place": "Q1", "resolve_place": True}, "needs a source_ref"),
        (
            {
                "place": "Q1",
                "add_place": {"kind": "town", "name": "T", "boundary": "POINT(0 0)"},
            },
            "kind and a name",
        ),
        (
            {
                "place": "Q1",
                "add_place": {"kind": "city", "name": "C", "boundary": "x"},
            },
            "needs a parent_id",
        ),
        (
            {
                "place": "Q1",
                "add_place": {
                    "kind": "metro",
                    "name": "M",
                    "boundary": "P",
                    "member_ids": ["Q2"],
                },
            },
            "not both",
        ),
        (
            {
                "place": "Q1",
                "add_place": {
                    "kind": "country",
                    "name": "N",
                    "boundary": "P",
                    "country_code": False,  # YAML reads an unquoted NO as false
                },
            },
            "two-letter",
        ),
        (
            {
                "place": "Q1",
                "add_place": {"kind": "country", "name": " ", "boundary": "P"},
            },
            "kind and a name",
        ),
        (
            {
                "place": "Q1",
                "add_place": {
                    "kind": "city",
                    "name": "C",
                    "parent_id": "Q2",
                    "member_ids": ["Q3"],
                },
            },
            "belong to a metro",
        ),
        ({"place": "Q1", "set_place_members": []}, "non-empty QID list"),
        ({"place": "Q1", "set_boundary": ""}, "must be WKT"),
        ({"place": "Q1", "set_aliases": ["", "x"]}, "non-empty list of strings"),
        ({"place": "Q1", "set_aliases": ["x"], "evidence_hash": 5}, "must be a string"),
    ],
)
def test_malformed_place_overrides_are_refused(tmp_path, entry, message):
    with pytest.raises(overrides.OverrideError, match=message):
        overrides.load_place_overrides(write_overrides(tmp_path, places=[entry]))


def test_duplicate_place_overrides_are_refused(tmp_path):
    entry = {"place": "Q1", "set_aliases": ["x"]}
    with pytest.raises(overrides.OverrideError, match="duplicate"):
        overrides.load_place_overrides(
            write_overrides(tmp_path, places=[entry, dict(entry)])
        )
    # Two resolutions of one candidate would race for its QID.
    twice = [
        {"place": "Q1", "source_ref": "ov", "resolve_place": True},
        {"place": "Q2", "source_ref": "ov", "resolve_place": True},
    ]
    with pytest.raises(overrides.OverrideError, match="duplicate resolve_place"):
        overrides.load_place_overrides(
            write_overrides(tmp_path / "twice", places=twice)
        )


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({"level": "municipality"}, "level and place_id"),
        ({"level": "town", "place_id": "Q1"}, "level must be one of"),
        ({"level": "municipality", "place_id": ""}, "place_id must be a place id"),
    ],
)
def test_set_coverage_is_validated(tmp_path, spec, message):
    with pytest.raises(overrides.OverrideError, match=message):
        overrides.load_feed_overrides(
            write_overrides(tmp_path, feeds=[{"feed": "f", "set_coverage": spec}])
        )


def test_staleness_is_judged_against_the_current_evidence():
    report = []
    entry = {"place": "Q1", "operation": "set_aliases", "evidence_hash": "0" * 64}
    assert overrides.judge(entry, ["a"], report, "names") is True
    assert (
        overrides.judge({**entry, "evidence_hash": None}, ["a"], report, "names")
        is False
    )
    (row,) = report
    assert row["current_evidence_hash"] == overrides.canonical_digest(["a"])
    assert (
        overrides.judge(
            {**entry, "evidence_hash": row["current_evidence_hash"]},
            ["a"],
            report,
            "names",
        )
        is False
    )
    assert len(report) == 1
