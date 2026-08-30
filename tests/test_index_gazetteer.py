import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

pytest.importorskip("pyarrow")
import overture_fixture as fx  # noqa: E402

from index_build import overture, store  # noqa: E402

US = ("ov-us", "country", "United States")
NY = ("ov-ny", "region", "New York")

# The skeleton subtypes plus a locality and a rejected finer subtype.
ROWS = [
    fx.division(
        "ov-us",
        "US",
        "country",
        wikidata="Q30",
        name="United States",
        common={"en": "United States", "es": "EE. UU."},
        sources=[fx.osm("relation/148838")],
        hierarchies=fx.chain(US),
    ),
    fx.division(
        "ov-ny",
        "US",
        "region",
        wikidata="Q1384",
        name="New York",
        admin_level=1,
        sources=[fx.osm("relation/61320")],
        hierarchies=fx.chain(US, NY),
    ),
    fx.division(
        "ov-nyc-county",
        "US",
        "county",
        name="New York County",
        admin_level=2,
        sources=[fx.osm("relation/2552450")],
        hierarchies=fx.chain(US, NY, ("ov-nyc-county", "county", "New York County")),
    ),
    fx.division(
        "ov-noqid",
        "US",
        "localadmin",
        name="Nowhere",
        admin_level=3,
        sources=[
            {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "X"}
        ],
        hierarchies=fx.chain(US, ("ov-noqid", "localadmin", "Nowhere")),
    ),
    fx.division(
        "ov-loc",
        "US",
        "locality",
        wikidata="Q1",
        name="Sometown",
        admin_level=4,
        sources=[fx.osm("relation/9")],
        hierarchies=fx.chain(US, ("ov-loc", "locality", "Sometown")),
    ),
    fx.division(
        "ov-fi",
        "FI",
        "country",
        wikidata="Q33",
        name="Finland",
        sources=[fx.osm("relation/54224")],
        hierarchies=fx.chain(("ov-fi", "country", "Finland")),
    ),
]


def _read(cache, artifact):
    records, manifest = store.read_jsonl(cache / "gazetteer", "overture.json", artifact)
    return records, manifest


def _resolve(tmp_path, *, mapping=None):
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS)
    wikidata = fx.StubWikidata({"2552450": "Q11299"} if mapping is None else mapping)
    manifest = overture.resolve(cache, dataset=dataset, wikidata=wikidata)
    return cache, manifest, wikidata


def test_resolve_publishes_resolved_and_report(tmp_path):
    cache, manifest, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    report, _ = _read(cache, "place_resolution_report.jsonl")

    by_id = {r["overture_id"]: r for r in resolved}
    assert by_id["ov-us"]["qid"] == "Q30"
    assert by_id["ov-us"]["resolution_method"] == "overture_wikidata"
    assert by_id["ov-ny"]["qid"] == "Q1384"
    # No wikidata property, resolved through the OSM relation's P402 reverse.
    assert by_id["ov-nyc-county"]["qid"] == "Q11299"
    assert by_id["ov-nyc-county"]["resolution_method"] == "osm_p402"
    # The OSM relation ids are kept on the resolved record as a crosswalk.
    assert by_id["ov-nyc-county"]["osm_relation_ids"] == ["2552450"]
    # The whole admin skeleton is read, so FI resolves like US.
    assert by_id["ov-fi"]["qid"] == "Q33"
    # localities and finer subtypes are not part of the skeleton read.
    assert "ov-loc" not in by_id
    # The bare name/level/country candidate is reported, never minted.
    assert [r["overture_id"] for r in report] == ["ov-noqid"]
    assert manifest["resolved"] == 4
    assert manifest["reported"] == 1
    assert manifest["resolved_by_method"] == {"overture_wikidata": 3, "osm_p402": 1}
    assert manifest["countries"] == ["FI", "US"]
    assert manifest["overture_release"] == overture.OVERTURE_RELEASE


def test_subtype_maps_to_kind_and_keeps_source_subtype(tmp_path):
    cache, _, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    kinds = {r["overture_id"]: (r["kind"], r["source_subtype"]) for r in resolved}
    assert kinds["ov-us"] == ("country", "country")
    assert kinds["ov-ny"] == ("region", "region")
    assert kinds["ov-nyc-county"] == ("region", "county")


def test_ancestors_come_from_the_hierarchy_chain(tmp_path):
    cache, _, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    county = next(r for r in resolved if r["overture_id"] == "ov-nyc-county")
    assert [(a["overture_id"], a["subtype"]) for a in county["ancestors"]] == [
        ("ov-us", "country"),
        ("ov-ny", "region"),
    ]


def test_names_carry_the_primary_and_multilingual_labels(tmp_path):
    cache, _, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    us = next(r for r in resolved if r["overture_id"] == "ov-us")
    assert us["name"] == "United States"
    assert us["names"]["es"] == "EE. UU."


def test_p402_is_queried_only_for_divisions_without_a_qid(tmp_path):
    _, _, wikidata = _resolve(tmp_path)
    # Among the skeleton only ov-nyc-county lacks a wikidata property and has an
    # OSM relation; ov-noqid's source is OSM-less, so 2552450 is all that's asked.
    assert wikidata.queried == [["2552450"]]


def test_a_conflicting_p402_relation_is_reported_not_minted(tmp_path):
    # The county's relation resolves ambiguously (None), so it is not minted and
    # its report entry says so rather than claiming no evidence at all.
    cache, manifest, _ = _resolve(tmp_path, mapping={"2552450": None})
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    report, _ = _read(cache, "place_resolution_report.jsonl")
    assert "ov-nyc-county" not in {r["overture_id"] for r in resolved}
    entry = next(r for r in report if r["overture_id"] == "ov-nyc-county")
    assert entry["reason"] == "conflicting P402 identities"


def test_a_malformed_direct_wikidata_value_is_not_minted(tmp_path):
    row = fx.division(
        "ov-bad",
        "US",
        "region",
        wikidata="not-a-qid",
        name="Bad",
        admin_level=1,
        hierarchies=fx.chain(US, ("ov-bad", "region", "Bad")),
    )
    record = overture.normalize_division(row)
    assert record["wikidata"] is None
    assert overture.resolve_qid(record, {}) == (
        None,
        "name_country",
        "no wikidata property and no P402 match",
    )


def test_a_clean_qid_beside_an_ambiguous_relation_is_still_a_conflict():
    # One relation resolves cleanly, another is ambiguous: any conflict signal
    # leaves the division unresolved, never minted on the clean one.
    record = {"wikidata": None, "osm_relation_ids": ["100", "200"]}
    assert overture.resolve_qid(record, {"100": "Q5", "200": None}) == (
        None,
        "name_country",
        "conflicting P402 identities",
    )


def test_wikidata_client_parses_and_rejects_ambiguous(monkeypatch):
    import io as _io

    payload = {
        "results": {
            "bindings": [
                {
                    "osm": {"value": "10"},
                    "item": {"value": "http://www.wikidata.org/entity/Q10"},
                },
                # 20 maps to two items: ambiguous, minted for neither.
                {
                    "osm": {"value": "20"},
                    "item": {"value": "http://www.wikidata.org/entity/Q20"},
                },
                {
                    "osm": {"value": "20"},
                    "item": {"value": "http://www.wikidata.org/entity/Q21"},
                },
            ]
        }
    }

    def fake_urlopen(request, timeout=None):
        return _io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    client = overture.WikidataClient()
    # 10 is unique; 20 is ambiguous (None, present as a conflict); 30 is absent.
    assert client.p402(["10", "20", "30"]) == {"10": "Q10", "20": None}
