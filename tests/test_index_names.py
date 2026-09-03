import io
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

pytest.importorskip("pyarrow")
import overture_fixture as fx  # noqa: E402

from index_build import names, overture, store  # noqa: E402


def _place(place_id, *, name, names=None, aliases=None):
    return {
        "place_id": place_id,
        "kind": "city",
        "name": name,
        "names": names or {},
        "aliases": [] if aliases is None else aliases,
    }


PLACES = [
    # Overture gives en + fi; Wikidata should add sv but not override en/fi.
    _place("Q1757", name="Helsinki", names={"en": "Helsinki", "fi": "Helsinki"}),
    # No Wikidata entry: left untouched.
    _place("Q2000", name="Nowhere", names={"en": "Nowhere"}),
    # Existing alias must survive the merge.
    _place("Q3000", name="X", names={"en": "X"}, aliases=["OldAlias"]),
]

LABELS = {
    "Q1757": {
        "labels": {"en": "Helsinki (wd)", "sv": "Helsingfors", "fi": "Helsinki (wd)"},
        "aliases": ["Stadi", "Hesa", "Helsinki"],
    },
    "Q3000": {"labels": {"en": "X"}, "aliases": ["New"]},
}


def _run(tmp_path, overrides_dir=None, strict=False):
    from index_build import overrides

    cache = tmp_path / "cache"
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "gazetteer",
                "geometry.json",
                {"places_seed.jsonl": store.jsonl_chunks(PLACES)},
                {
                    "source": "geometry",
                    "places_overrides_sha256": overrides.places_digest(overrides_dir),
                },
                held=directory,
            )
    finally:
        directory.close()
    manifest = names.merge_names(
        cache,
        wikidata=fx.StubWikidata(labels=LABELS),
        overrides_dir=overrides_dir,
        strict=strict,
    )
    places, _ = store.read_jsonl(cache / "gazetteer", "names.json", "places_seed.jsonl")
    return manifest, {p["place_id"]: p for p in places}


def test_wikidata_fills_missing_languages_and_overture_wins(tmp_path):
    _, places = _run(tmp_path)
    hel = places["Q1757"]
    assert hel["names"]["sv"] == "Helsingfors"  # filled from Wikidata
    assert hel["names"]["en"] == "Helsinki"  # Overture keeps the shared language
    assert hel["names"]["fi"] == "Helsinki"


def test_name_becomes_the_english_label(tmp_path):
    _, places = _run(tmp_path)
    assert places["Q1757"]["name"] == "Helsinki"


def test_aliases_are_the_union_minus_the_name(tmp_path):
    _, places = _run(tmp_path)
    # "Helsinki" is dropped (it equals the name); Stadi/Hesa remain, sorted.
    assert places["Q1757"]["aliases"] == ["Hesa", "Stadi"]


def test_existing_aliases_are_preserved(tmp_path):
    _, places = _run(tmp_path)
    assert set(places["Q3000"]["aliases"]) == {"OldAlias", "New"}


def test_a_place_without_wikidata_is_left_unchanged(tmp_path):
    manifest, places = _run(tmp_path)
    assert places["Q2000"]["names"] == {"en": "Nowhere"}
    assert places["Q2000"]["aliases"] == []
    assert manifest["enriched"] == 2  # Q1757 and Q3000, not Q2000


def test_labels_and_aliases_parses_wbgetentities(monkeypatch):
    payload = {
        "entities": {
            "Q60": {
                "labels": {
                    "en": {"language": "en", "value": "New York City"},
                    "sv": {"language": "sv", "value": "New York"},
                },
                "aliases": {
                    "en": [
                        {"language": "en", "value": "City of New York"},
                        {"language": "en", "value": "NYC"},
                    ]
                },
            },
            "Q999999999": {"missing": ""},  # a missing entity is skipped
        }
    }

    def fake_urlopen(request, timeout=None):
        return io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    result = overture.WikidataClient().labels_and_aliases(["Q60", "Q999999999"])
    assert result == {
        "Q60": {
            "labels": {"en": "New York City", "sv": "New York"},
            "aliases": ["City of New York", "NYC"],
        }
    }


def test_set_aliases_adds_the_curators_names(tmp_path):
    from test_index_place_overrides import write_overrides

    entries = [{"place": "Q2000", "set_aliases": ["Nowheresville", "Nowhere"]}]
    manifest, places = _run(
        tmp_path, overrides_dir=write_overrides(tmp_path, places=entries)
    )
    # The place's own name never doubles as an alias.
    assert places["Q2000"]["aliases"] == ["Nowheresville"]
    assert manifest["overrides_applied"] == 1


def test_a_stale_alias_override_fails_strict_after_publishing_its_report(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    entries = [
        {"place": "Q2000", "set_aliases": ["Nowheresville"], "evidence_hash": "0" * 64}
    ]
    manifest, places = _run(
        tmp_path, overrides_dir=write_overrides(tmp_path, places=entries)
    )
    assert manifest["stale_overrides"] == 1
    assert places["Q2000"]["aliases"] == ["Nowheresville"]
    with pytest.raises(overrides.OverrideError, match="stale override"):
        _run(
            tmp_path / "strict",
            overrides_dir=write_overrides(tmp_path / "strict", places=entries),
            strict=True,
        )
