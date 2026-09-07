import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import registry  # noqa: E402

HEADER = {"registry": 1, "next_id": 5}
HELSINKI = {
    "place_id": "tp_1",
    "kind": "city",
    "concordances": {"wikidata": ["Q1757"], "overture": ["ov-hel"]},
    "name": "Helsinki",
    "country_code": "FI",
    "minted_from": "overture:ov-hel",
    "minted_in": "overture 2026-08-19.0",
}
ESPOO = {
    "place_id": "tp_2",
    "kind": "city",
    "concordances": {"wikidata": ["Q5342", "Q999"], "overture": ["ov-esp"]},
    "name": "Espoo",
    "country_code": "FI",
    "minted_from": "overture:ov-esp",
    "minted_in": "overture 2026-08-19.0",
}
MERGED = {
    "place_id": "tp_3",
    "status": "merged",
    "into": "tp_1",
    "concordances": {"wikidata": ["Q777"]},
    "at": "2026-09-08",
    "reason": "duplicate item",
}
RETIRED = {
    "place_id": "tp_4",
    "status": "retired",
    "at": "2026-09-08",
    "reason": "gone",
}
ROWS = [HELSINKI, ESPOO, MERGED, RETIRED]


def _write(path, header=HEADER, rows=ROWS):
    lines = [json.dumps(header, sort_keys=True)]
    lines.extend(json.dumps(row, sort_keys=True) for row in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("header", "rows", "message"),
    [
        (None, ROWS, "no header"),
        ({"registry": 2, "next_id": 4}, ROWS, "header must be"),
        ({"registry": True, "next_id": 5}, ROWS, "header must be"),
        ({"registry": 1.0, "next_id": 5}, ROWS, "header must be"),
        (HEADER, [dict(HELSINKI, name="\ud800")], "not encodable"),
        (HEADER, [dict(HELSINKI, place_id="tp_" + "9" * 19)], "place_id"),
        ({"registry": 1, "next_id": 2}, ROWS, "at or above next_id"),
        (HEADER, [dict(HELSINKI, concordances={})], "needs a concordance"),
        (
            HEADER,
            [HELSINKI, {**RETIRED, "place_id": "tp_2"}, {**MERGED, "into": "tp_2"}],
            "not live",
        ),
        (HEADER, [ESPOO, HELSINKI, MERGED, RETIRED], "out of order"),
        (HEADER, [HELSINKI, HELSINKI], "out of order"),
        (HEADER, [dict(HELSINKI, concordances={"geo": ["1"]})], "unknown namespace"),
        (HEADER, [dict(HELSINKI, concordances={"wikidata": ["1757"]})], "not a QID"),
        (HEADER, [dict(HELSINKI, kind="planet")], "kind"),
        (HEADER, [dict(HELSINKI, extra=1)], "unknown fields"),
        (
            HEADER,
            [HELSINKI, {**MERGED, "place_id": "tp_2", "into": "tp_9"}],
            "not live",
        ),
        (
            HEADER,
            [HELSINKI, {**MERGED, "place_id": "tp_2"}, {**MERGED, "into": "tp_2"}],
            "not live",
        ),
        (HEADER, [{**MERGED, "place_id": "tp_1", "into": "tp_1"}], "different 'into'"),
        (
            HEADER,
            [
                {
                    "place_id": "tp_1",
                    "status": "retired",
                    "into": "tp_2",
                    "at": "a",
                    "reason": "r",
                }
            ],
            "names no successor",
        ),
        (
            HEADER,
            [HELSINKI, dict(ESPOO, concordances={"wikidata": ["Q1757"]})],
            "on both",
        ),
    ],
)
def test_loading_validates_the_file(tmp_path, header, rows, message):
    path = tmp_path / "places_registry.jsonl"
    if header is None:
        path.write_text("")
    else:
        _write(path, header, rows)
    with pytest.raises(registry.RegistryError, match=message):
        registry.load(path)
    path.write_bytes(b"\xff\xfe")
    with pytest.raises(registry.RegistryError, match="not UTF-8"):
        registry.load(path)


ROW = (
    '{"place_id": "tp_1", "kind": "city", "concordances": {"wikidata": ["Q1757"]}, '
    '"name": null, "country_code": null, "minted_from": "x", "minted_in": "y"}'
)


@pytest.mark.parametrize(
    "lines",
    [
        ['{"registry": 1, "next_id": 5, "next_id": 6}'],
        [json.dumps(HEADER), ROW.replace('"kind"', '"place_id": "tp_1", "kind"', 1)],
        [
            json.dumps(HEADER),
            ROW.replace('["Q1757"]}', '["Q1757"], "wikidata": ["Q2"]}', 1),
        ],
    ],
)
def test_duplicate_json_keys_are_refused_at_every_level(tmp_path, lines):
    path = tmp_path / "r.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(registry.RegistryError, match="duplicate key"):
        registry.load(path)


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("tp_1", "tp_1"),
        ("tp_3", "tp_1"),  # merged: its successor
        ("tp_4", None),  # retired: refused
        ("Q777", "tp_1"),  # a merged row's value lands on the survivor
        ("Q5342", "tp_2"),
        ("overture:ov-esp", "tp_2"),
        ("Q999", "tp_2"),  # a later value of the same namespace
        ("tp_9", None),
        ("geonames:1", None),
        ("foo:1", None),
        ("nope", None),
    ],
)
def test_references_resolve_through_concordances_and_aliases(
    tmp_path, reference, expected
):
    reg = registry.load(_write(tmp_path / "r.jsonl"))
    if expected is None:
        with pytest.raises(registry.RegistryError):
            reg.resolve(reference)
    else:
        assert reg.resolve(reference) == expected
    # The first QID is canonical, a merged id answers with its survivor's,
    # and a retired id is refused.
    assert reg.canonical_qid("tp_2") == "Q5342" and reg.effective("tp_2") == {
        "wikidata": ["Q5342", "Q999"],
        "overture": ["ov-esp"],
    }
    assert reg.canonical_qid("tp_3") == "Q1757"
    with pytest.raises(registry.RegistryError, match="retired"):
        reg.canonical_qid("tp_4")
