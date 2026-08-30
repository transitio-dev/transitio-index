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

from index_build import metros, overture, seed, store  # noqa: E402

ROWS = [
    fx.division("us", "US", "country", wikidata="Q30", name="United States"),
    fx.division(
        "us-il",
        "US",
        "region",
        wikidata="Q1204",
        name="Illinois",
        admin_level=1,
        hierarchies=fx.chain(("us", "country", "US"), ("us-il", "region", "Illinois")),
    ),
    fx.division(
        "us-chi",
        "US",
        "locality",
        wikidata="Q1297",
        name="Chicago",
        admin_level=2,
        hierarchies=fx.chain(
            ("us", "country", "US"),
            ("us-il", "region", "Illinois"),
            ("us-chi", "locality", "Chicago"),
        ),
    ),
    fx.division(
        "us-spring",
        "US",
        "locality",
        wikidata="Q28515",
        name="Springfield",
        admin_level=2,
        hierarchies=fx.chain(
            ("us", "country", "US"),
            ("us-il", "region", "Illinois"),
            ("us-spring", "locality", "Springfield"),
        ),
    ),
    fx.division("fi", "FI", "country", wikidata="Q33", name="Finland"),
    fx.division(
        "fi-hel",
        "FI",
        "locality",
        wikidata="Q1757",
        name="Helsinki",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-hel", "locality", "Helsinki")
        ),
    ),
]

FEEDS = [
    {
        "feed_id": "f-chi",
        "mdb": {
            "location": {
                "country_code": "US",
                "subdivision_name": "Illinois",
                "municipality": "Chicago",
            }
        },
    },
    {
        "feed_id": "f-spring",
        "mdb": {
            "location": {
                "country_code": "US",
                "subdivision_name": "Illinois",
                "municipality": "Springfield",
            }
        },
    },
    {
        "feed_id": "f-hel",
        "mdb": {
            "location": {
                "country_code": "FI",
                "subdivision_name": None,
                "municipality": "Helsinki",
            }
        },
    },
]

CHICAGO_METRO = {
    "qid": "Q1754965",
    "name": "Chicago metropolitan area",
    "cbsa": "16980",
}


def _publish(cache, subdir, pointer, artifact, records):
    directory = store.open_subdir(cache, subdir)
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / subdir,
                pointer,
                {artifact: store.jsonl_chunks(records)},
                {"source": subdir},
                held=directory,
            )
    finally:
        directory.close()


def _run(tmp_path, metro_map):
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS)
    _publish(cache, "crosswalk", "feeds.json", "feeds.jsonl", FEEDS)
    overture.resolve(cache, dataset=dataset, wikidata=fx.StubWikidata())
    seed.resolve_seed(cache, dataset=dataset, wikidata=fx.StubWikidata())
    manifest = metros.attach_metros(cache, wikidata=fx.StubWikidata(metros=metro_map))
    places, _ = store.read_jsonl(
        cache / "gazetteer", "metros.json", "places_seed.jsonl"
    )
    return manifest, {p["place_id"]: p for p in places}


def test_a_metro_place_and_its_memberships_are_attached(tmp_path):
    manifest, places = _run(
        tmp_path, {"Q1297": [CHICAGO_METRO], "Q28515": [CHICAGO_METRO]}
    )
    metro = places["Q1754965"]
    assert metro["kind"] == "metro"
    assert metro["statistical_area_id"] == "16980"
    assert metro["name"] == "Chicago metropolitan area"
    assert metro["member_ids"] == ["Q1297", "Q28515"]
    assert places["Q1297"]["metro_ids"] == ["Q1754965"]
    assert places["Q28515"]["metro_ids"] == ["Q1754965"]
    assert manifest["metros"] == 1
    assert manifest["cities_with_metro"] == 2


def test_a_city_with_no_metro_keeps_an_empty_list(tmp_path):
    _, places = _run(tmp_path, {"Q1297": [CHICAGO_METRO]})
    assert places["Q28515"]["metro_ids"] == []
    # And the seed cities/ancestors all carry the new uniform fields.
    assert places["Q30"]["statistical_area_id"] is None
    assert places["Q30"]["metro_ids"] == []


def test_a_metro_without_a_cbsa_is_reported_not_published(tmp_path):
    codeless = {"qid": "Q999", "name": "Codeless metro", "cbsa": None}
    manifest, places = _run(tmp_path, {"Q1297": [codeless]})
    assert "Q999" not in places
    assert places["Q1297"]["metro_ids"] == []
    assert manifest["metros"] == 0
    assert manifest["reported"] == 1


def test_get_raises_on_a_response_without_bindings(monkeypatch):
    def fake_urlopen(request, timeout=None):
        return io.BytesIO(json.dumps({"error": "query timeout"}).encode("utf-8"))

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(overture.GazetteerError):
        overture.WikidataClient().statistical_metros(["Q60"])


def test_only_us_cities_are_looked_up_for_metros(tmp_path):
    # Helsinki is offered a metro by the stub, but it is not a US city, so the
    # stage never asks about it and it stays metro-less.
    _, places = _run(tmp_path, {"Q1757": [CHICAGO_METRO], "Q1297": [CHICAGO_METRO]})
    assert places["Q1757"]["metro_ids"] == []
    assert places["Q1297"]["metro_ids"] == ["Q1754965"]


def test_statistical_metros_parses_and_skips_malformed(monkeypatch):
    payload = {
        "results": {
            "bindings": [
                {
                    "city": {"value": "http://www.wikidata.org/entity/Q60"},
                    "metro": {"value": "http://www.wikidata.org/entity/Q683705"},
                    "metroLabel": {"value": "New York metropolitan area"},
                    "cbsa": {"value": "35620"},
                },
                {
                    "city": {"value": "http://www.wikidata.org/entity/Q65"},
                    "metro": {"value": "http://www.wikidata.org/entity/Q1755545"},
                    "metroLabel": {"value": "Los Angeles metropolitan area"},
                },
                {  # a non-QID city value is skipped
                    "city": {"value": "http://www.wikidata.org/entity/not-a-qid"},
                    "metro": {"value": "http://www.wikidata.org/entity/Q1"},
                },
            ]
        }
    }

    def fake_urlopen(request, timeout=None):
        return io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    client = overture.WikidataClient()
    assert client.statistical_metros(["Q60", "Q65"]) == {
        "Q60": [
            {"qid": "Q683705", "name": "New York metropolitan area", "cbsa": "35620"}
        ],
        "Q65": [
            {"qid": "Q1755545", "name": "Los Angeles metropolitan area", "cbsa": None}
        ],
    }
