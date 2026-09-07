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
                {
                    "place": "Q7",
                    "evidence_hash": "0" * 64,
                    "set_statistical_area": {
                        "scheme": "eurostat_metro",
                        "code": "FI001",
                    },
                },
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
        "set_statistical_area",
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
            "needs a place reference",
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
        ({"place": "Q1", "set_place_members": []}, "non-empty list of place"),
        ({"place": "Q1", "set_boundary": ""}, "must be WKT"),
        ({"place": "Q1", "set_aliases": ["", "x"]}, "non-empty list of strings"),
        ({"place": "Q1", "set_aliases": ["x"], "evidence_hash": 5}, "must be a string"),
        (
            {"place": "Q1", "set_statistical_area": {"scheme": "eurostat_metro"}},
            "scheme and a code",
        ),
        (
            {"place": "Q1", "set_statistical_area": {"scheme": "x", "code": "1"}},
            "scheme must be one of",
        ),
        (
            {
                "place": "Q1",
                "set_statistical_area": {"scheme": "eurostat_metro", "code": " "},
            },
            "non-empty string",
        ),
        (
            {
                "place": "ov-1",
                "set_statistical_area": {"scheme": "eurostat_metro", "code": "1"},
            },
            "place reference",
        ),
        (
            {
                "place": "Q1",
                "set_statistical_area": {"scheme": "eurostat_metro", "code": "1"},
            },
            "evidence_hash",
        ),
        (
            {
                "place": "Q1",
                "set_statistical_area": {"scheme": "eurostat_metro", "code": "1"},
                "evidence_hash": "abc",
            },
            "SHA-256",
        ),
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


def registry_with_two_places(tmp_path):
    from index_build import registry

    path = tmp_path / "places_registry.jsonl"
    path.write_text('{"next_id": 1, "registry": 1}\n')
    with registry.session(path) as reg:
        for concordances in (
            {"wikidata": ["Q1"], "overture": ["ov1"]},
            {"wikidata": ["Q2"]},
        ):
            reg.identify(concordances, kind="city", minted_from="t", minted_in="t 1")
        reg.save()
    return registry.load(path)


def test_loaders_resolve_references_through_the_registry(tmp_path):
    reg = registry_with_two_places(tmp_path)
    directory = write_overrides(
        tmp_path,
        places=[
            {"place": "tp_1", "set_aliases": ["x"]},
            {
                "place": "Q7",
                "add_place": {
                    "kind": "metro",
                    "name": "M",
                    "member_ids": ["overture:ov1", "tp_2"],
                },
            },
            {"place": "Q1", "set_place_members": ["tp_2"]},
        ],
        feeds=[
            {"feed": "f", "set_coverage": {"level": "municipality", "place_id": "tp_1"}}
        ],
    )
    entries, _ = overrides.load_place_overrides(directory, registry=reg)
    assert [e["place"] for e in entries] == ["Q1", "Q7", "Q1"]
    assert entries[1]["add_place"]["member_ids"] == ["Q1", "Q2"]
    assert entries[2]["set_place_members"] == ["Q2"]
    feeds, _ = overrides.load_feed_overrides(directory, registry=reg)
    assert feeds["f"]["set_coverage"]["place_id"] == "Q1"
    (directory / "edges.yaml").write_text(
        yaml.safe_dump(
            [
                {"feed": "f", "place": "tp_1", "set_tiers": ["local"], **_STAMP},
                {"feed": "f", "place": "*", "set_tiers": ["local"], **_STAMP},
            ]
        )
    )
    edges, _ = overrides.load_edge_overrides(directory, registry=reg)
    assert [e["place"] for e in edges] == ["Q1", "*"]
    # Two references to one place are one duplicate; an unknown non-QID
    # reference names nothing.
    for places, message in [
        (
            [
                {"place": "tp_1", "set_aliases": ["x"]},
                {"place": "Q1", "set_aliases": ["y"]},
            ],
            "duplicate set_aliases",
        ),
        ([{"place": "tp_9", "set_aliases": ["x"]}], "no such place"),
    ]:
        with pytest.raises(overrides.OverrideError, match=message):
            overrides.load_place_overrides(
                write_overrides(tmp_path, places=places, name="bad"), registry=reg
            )


_STAMP = {"reason": "curated", "author": "HT", "date": "2026-09-02"}
