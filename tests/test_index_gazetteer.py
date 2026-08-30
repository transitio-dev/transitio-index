import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402
import pyarrow.dataset as pa_ds  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from index_build import overture, store  # noqa: E402

_NAMES = pa.struct(
    [("primary", pa.string()), ("common", pa.map_(pa.string(), pa.string()))]
)
_SOURCE = pa.struct(
    [
        ("property", pa.string()),
        ("dataset", pa.string()),
        ("license", pa.string()),
        ("record_id", pa.string()),
    ]
)
_STEP = pa.struct(
    [("division_id", pa.string()), ("subtype", pa.string()), ("name", pa.string())]
)
_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("country", pa.string()),
        ("subtype", pa.string()),
        ("admin_level", pa.int32()),
        ("class", pa.string()),
        ("names", _NAMES),
        ("wikidata", pa.string()),
        ("sources", pa.list_(_SOURCE)),
        ("hierarchies", pa.list_(pa.list_(_STEP))),
    ]
)


def _names(primary, common):
    return {"primary": primary, "common": common}


def _osm(record_id):
    return {"dataset": "OpenStreetMap", "license": "ODbL", "record_id": record_id}


def _chain(*steps):
    return [[{"division_id": i, "subtype": s, "name": n} for i, s, n in steps]]


US = ("ov-us", "country", "United States")
NY = ("ov-ny", "region", "New York")


def _fixture(tmp_path):
    """A small division table across the skeleton subtypes plus rejected ones."""
    rows = [
        {
            "id": "ov-us",
            "country": "US",
            "subtype": "country",
            "admin_level": 0,
            "class": None,
            "names": _names("United States", {"en": "United States", "es": "EE. UU."}),
            "wikidata": "Q30",
            "sources": [_osm("relation/148838")],
            "hierarchies": _chain(US),
        },
        {
            "id": "ov-ny",
            "country": "US",
            "subtype": "region",
            "admin_level": 1,
            "class": None,
            "names": _names("New York", {"en": "New York"}),
            "wikidata": "Q1384",
            "sources": [_osm("relation/61320")],
            "hierarchies": _chain(US, NY),
        },
        {
            "id": "ov-nyc-county",
            "country": "US",
            "subtype": "county",
            "admin_level": 2,
            "class": None,
            "names": _names("New York County", {"en": "New York County"}),
            "wikidata": None,
            "sources": [_osm("relation/2552450")],
            "hierarchies": _chain(
                US, NY, ("ov-nyc-county", "county", "New York County")
            ),
        },
        {
            "id": "ov-noqid",
            "country": "US",
            "subtype": "localadmin",
            "admin_level": 3,
            "class": None,
            "names": _names("Nowhere", {"en": "Nowhere"}),
            "wikidata": None,
            "sources": [
                {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "X"}
            ],
            "hierarchies": _chain(US, ("ov-noqid", "localadmin", "Nowhere")),
        },
        {
            "id": "ov-loc",
            "country": "US",
            "subtype": "locality",
            "admin_level": 4,
            "class": None,
            "names": _names("Sometown", {"en": "Sometown"}),
            "wikidata": "Q1",
            "sources": [_osm("relation/9")],
            "hierarchies": _chain(US, ("ov-loc", "locality", "Sometown")),
        },
        {
            "id": "ov-fi",
            "country": "FI",
            "subtype": "country",
            "admin_level": 0,
            "class": None,
            "names": _names("Finland", {"en": "Finland"}),
            "wikidata": "Q33",
            "sources": [_osm("relation/54224")],
            "hierarchies": _chain(("ov-fi", "country", "Finland")),
        },
    ]
    path = tmp_path / "divisions.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), path)
    return pa_ds.dataset(path)


class StubWikidata:
    endpoint = "stub://wikidata"

    def __init__(self, mapping):
        self.mapping = mapping
        self.queried = []

    def p402(self, osm_relation_ids):
        ids = sorted({str(i) for i in osm_relation_ids})
        self.queried.append(ids)
        return {i: self.mapping[i] for i in ids if i in self.mapping}


def _read(cache, artifact):
    records, manifest = store.read_jsonl(cache / "gazetteer", "overture.json", artifact)
    return records, manifest


def _resolve(tmp_path, *, mapping=None):
    cache = tmp_path / "cache"
    dataset = _fixture(tmp_path)
    wikidata = StubWikidata({"2552450": "Q11299"} if mapping is None else mapping)
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
    row = {
        "id": "ov-bad",
        "country": "US",
        "subtype": "region",
        "admin_level": 1,
        "class": None,
        "names": _names("Bad", {"en": "Bad"}),
        "wikidata": "not-a-qid",
        "sources": [],
        "hierarchies": _chain(US, ("ov-bad", "region", "Bad")),
    }
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
