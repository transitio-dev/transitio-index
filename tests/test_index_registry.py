import hashlib
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
    "concordances": {"wikidata": ["Q999", "Q5342"], "overture": ["ov-esp"]},
    "name": "Espoo",
    "country_code": "FI",
    "minted_from": "overture:ov-esp",
    "minted_in": "overture 2026-08-19.0",
    "detached": {
        "wikidata": [{"value": "Q999", "at": "2026-09-08", "reason": "wrong"}]
    },
}
MERGED = {
    "place_id": "tp_3",
    "status": "merged",
    "into": "tp_1",
    "concordances": {"wikidata": ["Q777", "Q778"]},
    "detached": {
        "wikidata": [{"value": "Q778", "at": "2026-09-08", "reason": "wrong"}]
    },
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
UNDETACHED = {k: v for k, v in ESPOO.items() if k != "detached"}
UNMERGED_DETACH = {k: v for k, v in MERGED.items() if k != "detached"}


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
            [
                dict(
                    HELSINKI,
                    detached={"wikidata": [{"value": "Q9", "at": "a", "reason": "r"}]},
                )
            ],
            "is not stored",
        ),
        (
            HEADER,
            [
                dict(
                    HELSINKI,
                    detached={
                        "wikidata": [{"value": "Q1757", "at": "a", "reason": "r"}] * 2
                    },
                )
            ],
            "detached twice",
        ),
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
            [HELSINKI, {**UNDETACHED, "concordances": {"wikidata": ["Q1757"]}}],
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
        ("Q778", None),  # detached on the merged alias
        ("Q5342", "tp_2"),
        ("overture:ov-esp", "tp_2"),
        ("Q999", None),  # detached: nobody carries it
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
    # The first effective QID is canonical: Espoo's stored-first Q999 is
    # detached, so Q5342 is promoted; a merged id answers with its survivor's,
    # and a retired id is refused.
    assert reg.canonical_qid("tp_2") == "Q5342" and reg.effective("tp_2") == {
        "wikidata": ["Q5342"],
        "overture": ["ov-esp"],
    }
    assert reg.canonical_qid("tp_3") == "Q1757"
    with pytest.raises(registry.RegistryError, match="retired"):
        reg.canonical_qid("tp_4")


def test_the_lock_refuses_a_second_writer_and_a_changed_file_is_not_overwritten(
    tmp_path, monkeypatch
):
    path = _write(tmp_path / "r.jsonl")
    with registry.session(path) as reg:
        with pytest.raises(registry.RegistryError, match="another build"):
            with registry.session(path):
                pass
        # The lock is held through the save's own window too: a writer that
        # arrives between the re-read and the replacement is refused.
        real = registry.store.write_bytes

        def paused(directory, name, data):
            with pytest.raises(registry.RegistryError, match="another build"):
                with registry.session(path):
                    pass
            return real(directory, name, data)

        monkeypatch.setattr(registry.store, "write_bytes", paused)
        reg.minted += 1
        reg.save()
        monkeypatch.setattr(registry.store, "write_bytes", real)
        reg.minted = 0
        # A non-cooperating edit between load and save: the save aborts,
        # whether or not the session has anything to write.
        path.write_bytes(path.read_bytes() + b"\n")
        with pytest.raises(registry.RegistryError, match="changed since"):
            reg.save()
        reg.minted += 1  # a change to save, as identification will make
        with pytest.raises(registry.RegistryError, match="changed since"):
            reg.save()
    with registry.session(path, read_only=True):
        pass
    # Outside a session the registry is read-only: it cannot save at all;
    # a registry kept past its session lost the lock and cannot either.
    outside = registry.load(path)
    outside.minted += 1
    with pytest.raises(registry.RegistryError, match="read-only"):
        outside.save()
    with registry.session(path) as kept:
        pass
    kept.minted += 1
    with pytest.raises(registry.RegistryError, match="holding the lock"):
        kept.save()


def test_a_changed_registry_is_saved_canonically_and_an_unchanged_one_not_at_all(
    tmp_path, monkeypatch
):
    path = _write(tmp_path / "r.jsonl", rows=[HELSINKI, ESPOO])
    before = path.read_bytes()
    with registry.session(path) as reg:
        # Unchanged: no write at all, not even of identical bytes.
        monkeypatch.setattr(registry.store, "write_bytes", None)
        assert reg.save() == reg.base and path.read_bytes() == before
        monkeypatch.undo()
        reg.rows["tp_1"]["concordances"]["cbsa"] = ["12345"]
        reg.enriched += 1
        digest = reg.save()
    saved = path.read_bytes()
    assert hashlib.sha256(saved).hexdigest() == digest
    # Provenance keeps the digest loaded beside the one written.
    assert reg.manifest()["registry_base"] == hashlib.sha256(before).hexdigest()
    assert (
        reg.manifest()["registry_digest"] == digest and reg.manifest()["enriched"] == 1
    )
    assert saved.decode().split("\n")[1] == json.dumps(
        dict(HELSINKI, concordances={**HELSINKI["concordances"], "cbsa": ["12345"]}),
        sort_keys=True,
        ensure_ascii=False,
    )
    with registry.session(path, read_only=True) as reg:
        reg.rows["tp_1"]["name"] = "x"
        reg.enriched += 1
        with pytest.raises(registry.RegistryError, match="read-only"):
            reg.save()
    assert path.read_bytes() == saved


def _identify(reg, concordances, **fields):
    return reg.identify(
        concordances,
        kind=fields.pop("kind", "city"),
        minted_from=fields.pop("minted_from", "test:x"),
        minted_in=fields.pop("minted_in", "test 1"),
        **fields,
    )


def test_identification_mints_enriches_refuses_and_saves_once(tmp_path):
    path = _write(tmp_path / "r.jsonl")
    with registry.session(path) as reg:
        # Known by any concordance, gaining the ones it lacked.
        assert _identify(reg, {"overture": ["ov-hel"], "cbsa": ["12345"]}) == "tp_1"
        assert reg.effective("tp_1")["cbsa"] == ["12345"] and reg.enriched == 1
        # A merged row's value finds the survivor and is not re-added to it.
        assert _identify(reg, {"wikidata": ["Q777"]}) == "tp_1"
        assert reg.effective("tp_1")["wikidata"] == ["Q1757"]
        # Values of two places, a detached value on the matched place, no
        # concordance at all, an unknown kind: refused.
        with pytest.raises(registry.RegistryError, match="several places"):
            _identify(reg, {"wikidata": ["Q1757", "Q5342"]})
        with pytest.raises(registry.RegistryError, match="detached from tp_2"):
            _identify(reg, {"overture": ["ov-esp"], "wikidata": ["Q999"]})
        with pytest.raises(registry.RegistryError, match="detached from tp_3"):
            _identify(reg, {"wikidata": ["Q777", "Q778"]})
        with pytest.raises(registry.RegistryError, match="needs a concordance"):
            _identify(reg, {})
        with pytest.raises(registry.RegistryError, match="kind"):
            _identify(reg, {"geonames": ["1"]}, kind="planet")
        with pytest.raises(registry.RegistryError, match="is a city, not a metro"):
            _identify(reg, {"overture": ["ov-hel"]}, kind="metro")
        # Unknown everywhere: minted from the counter; a detached value may
        # be re-attached to the new place, which then owns it alone.
        assert _identify(reg, {"wikidata": ["Q999"]}, name="Elsewhere") == "tp_5"
        assert reg.resolve("Q999") == "tp_5" and reg.next_id == 6
        assert (
            reg.canonical_qid("tp_2") == "Q5342" and reg.canonical_qid("tp_5") == "Q999"
        )
        assert (reg.minted, reg.enriched) == (1, 1)
        digest = reg.save()
    saved = path.read_bytes()
    assert hashlib.sha256(saved).hexdigest() == digest
    rows = [json.loads(line) for line in saved.decode().split("\n") if line]
    assert rows[0] == {"registry": 1, "next_id": 6}
    assert [row["place_id"] for row in rows[1:]] == [
        "tp_1",
        "tp_2",
        "tp_3",
        "tp_4",
        "tp_5",
    ]
    assert rows[5]["name"] == "Elsewhere" and rows[5]["minted_from"] == "test:x"
    # The same identification against the saved file changes nothing and
    # writes nothing; two fresh copies identified in the same order end
    # byte-identical.
    with registry.session(path) as again:
        assert _identify(again, {"overture": ["ov-hel"], "cbsa": ["12345"]}) == "tp_1"
        assert not again.changed and again.save() == digest
    assert path.read_bytes() == saved
    other = _write(tmp_path / "other.jsonl")
    with registry.session(other) as fresh:
        _identify(fresh, {"overture": ["ov-hel"], "cbsa": ["12345"]})
        _identify(fresh, {"wikidata": ["Q777"]})
        _identify(fresh, {"wikidata": ["Q999"]}, name="Elsewhere")
        fresh.save()
    assert other.read_bytes() == saved


def test_a_refused_mint_consumes_no_id_and_the_id_space_has_an_end(tmp_path):
    path = _write(tmp_path / "r.jsonl")
    with registry.session(path) as reg:
        with pytest.raises(registry.RegistryError, match="minted_in"):
            _identify(reg, {"wikidata": ["Q42"]}, minted_in="")
        assert reg.next_id == 5 and not reg.changed and reg.manifest()["minted"] == 0
        assert _identify(reg, {"wikidata": ["Q42"]}) == "tp_5"
    full = _write(
        tmp_path / "full.jsonl", {"registry": 1, "next_id": 10**18}, [HELSINKI]
    )
    with registry.session(full) as reg:
        with pytest.raises(registry.RegistryError, match="exhausted"):
            _identify(reg, {"wikidata": ["Q42"]})
        assert not reg.changed
    _write(
        tmp_path / "beyond.jsonl", {"registry": 1, "next_id": 10**18 + 1}, [HELSINKI]
    )
    with pytest.raises(registry.RegistryError, match="header must be"):
        registry.load(tmp_path / "beyond.jsonl")


def test_a_truncated_tail_never_reuses_an_id(tmp_path):
    path = _write(tmp_path / "r.jsonl", {"registry": 1, "next_id": 9}, [HELSINKI])
    with registry.session(path) as reg:
        assert _identify(reg, {"wikidata": ["Q42"]}) == "tp_9" and reg.next_id == 10
        reg.save()
    saved = path.read_bytes()
    with registry.session(path) as again:
        assert again.resolve("Q42") == "tp_9" and again.save() == again.base
    assert path.read_bytes() == saved


def test_read_only_refuses_every_change_at_the_point_of_discovery(tmp_path):
    path = _write(tmp_path / "r.jsonl")
    before = path.read_bytes()
    with registry.session(path, read_only=True) as reg:
        with pytest.raises(registry.RegistryError, match="read-only"):
            _identify(reg, {"overture": ["ov-hel"], "cbsa": ["12345"]})
        with pytest.raises(registry.RegistryError, match="read-only"):
            _identify(reg, {"wikidata": ["Q999"]})
        assert _identify(reg, {"overture": ["ov-hel"]}) == "tp_1"
        assert reg.save() == reg.base
    assert path.read_bytes() == before


def test_the_build_defaults_the_registry_to_the_overrides_directory():
    import pathlib

    import build_index

    arguments = build_index.parse_args(
        ["--stage", "gazetteer", "--overrides-dir", "ov"]
    )
    assert build_index.registry_path(arguments) == pathlib.Path("ov") / registry.FILE
    assert not arguments.registry_read_only
    arguments = build_index.parse_args(
        ["--stage", "gazetteer", "--registry", "r.jsonl", "--registry-read-only"]
    )
    assert build_index.registry_path(arguments) == pathlib.Path("r.jsonl")
    assert arguments.registry_read_only


@pytest.mark.parametrize(
    ("head_rows", "next_id", "message"),
    [
        (
            ROWS
            + [dict(HELSINKI, place_id="tp_5", concordances={"wikidata": ["Q42"]})],
            6,
            None,
        ),
        ([HELSINKI, ESPOO, RETIRED], 5, "tp_3: row removed"),
        ([HELSINKI, ESPOO, MERGED], 4, "next_id decreased"),
        (
            [
                dict(HELSINKI, concordances={"wikidata": ["Q1757"]}),
                ESPOO,
                MERGED,
                RETIRED,
            ],
            5,
            "tp_1: overture concordances lost",
        ),
        (
            [HELSINKI, ESPOO, MERGED, dict(RETIRED, reason="other")],
            5,
            "tp_4: retired row changed",
        ),
        (
            [
                HELSINKI,
                ESPOO,
                MERGED,
                dict(HELSINKI, place_id="tp_4", concordances={"wikidata": ["Q4"]}),
            ],
            5,
            "tp_4: retired row changed",
        ),
        (
            [
                HELSINKI,
                ESPOO,
                dict(HELSINKI, place_id="tp_3", concordances={"wikidata": ["Q777"]}),
                RETIRED,
            ],
            5,
            "tp_3: merged row changed status",
        ),
        (
            [dict(HELSINKI, name="Helsingfors"), ESPOO, MERGED, RETIRED],
            5,
            "tp_1: name changed",
        ),
        (
            [HELSINKI, ESPOO, dict(MERGED, reason="other"), RETIRED],
            5,
            "tp_3: reason changed",
        ),
        (
            [HELSINKI, ESPOO, dict(MERGED, into="tp_2"), RETIRED],
            5,
            "without a flattening",
        ),
        (
            [
                {
                    **UNMERGED_DETACH,
                    "place_id": "tp_1",
                    "into": "tp_2",
                    "concordances": HELSINKI["concordances"],
                },
                ESPOO,
                dict(MERGED, into="tp_2"),
                RETIRED,
            ],
            5,
            None,
        ),
    ],
)
def test_the_history_check_enforces_the_contract(tmp_path, head_rows, next_id, message):
    import check_registry_history

    base = _write(tmp_path / "base.jsonl")
    head = _write(
        tmp_path / "head.jsonl", {"registry": 1, "next_id": next_id}, head_rows
    )
    found = check_registry_history.violations(
        check_registry_history._load(base), check_registry_history._load(head)
    )
    if message is None:
        assert found == []
    else:
        assert any(message in line for line in found), found
    # The CLI reports violations with a non-zero status, and an absent base
    # stands for the header-only registry the series started from.
    assert check_registry_history.main([str(base), str(head)]) == (
        0 if message is None else 1
    )
    assert check_registry_history.main([str(tmp_path / "absent.jsonl"), str(base)]) == 0
    # The head must be a regular file: absent, a directory or a symlink fails.
    assert check_registry_history.main([str(base), str(tmp_path / "absent.jsonl")]) == 1
    assert check_registry_history.main([str(base), str(tmp_path)]) == 1
    try:
        (tmp_path / "link.jsonl").symlink_to(base)
    except OSError:  # no symlink privilege (Windows runners)
        return
    assert check_registry_history.main([str(base), str(tmp_path / "link.jsonl")]) == 1
    # An added id below the base counter would reuse one the base had issued.
    gap = _write(tmp_path / "gap.jsonl", {"registry": 1, "next_id": 5}, [HELSINKI])
    added = _write(
        tmp_path / "added.jsonl", {"registry": 1, "next_id": 5}, [HELSINKI, ESPOO]
    )
    found = check_registry_history.violations(
        check_registry_history._load(gap), check_registry_history._load(added)
    )
    assert found == ["tp_2: added below the base counter"]


def test_key_for_resolves_every_reference_form(tmp_path):
    path = tmp_path / "places_registry.jsonl"
    _write(path)
    reg = registry.load(path)
    helsinki = reg.canonical_qid("tp_1")
    for reference, key, internal in [
        ("tp_2", "tp_2", "Q5342"),
        ("overture:ov-esp", "tp_2", "Q5342"),
        ("Q5342", "tp_2", "Q5342"),
        ("Q777", "tp_1", helsinki),  # a merged alias's QID names the survivor
        ("tp_3", "tp_1", helsinki),
        ("Q424242", "Q424242", "Q424242"),  # no row yet: still to be minted
    ]:
        assert reg.key_for(reference) == key, reference
        assert reg.key_for(reference, internal=True) == internal, reference
    # A concordance no row carries mints only where a place may be created.
    assert reg.key_for("overture:new", mint=True) == "overture:new"
    for reference, message in [
        ("tp_9", "no such place"),
        ("tp_4", "retired"),
        ("overture:nope", "no place carries it"),
        ("bogus", "not a place reference"),
        ("", "not a place reference"),
    ]:
        with pytest.raises(registry.RegistryError, match=message):
            reg.key_for(reference)
        with pytest.raises(registry.RegistryError, match=message):
            reg.key_for(reference, mint=reference in ("bogus", ""))
