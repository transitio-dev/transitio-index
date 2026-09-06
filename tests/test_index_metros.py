import hashlib
import io
import json
import sys
import urllib.request
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

pytest.importorskip("pyarrow")
import eurostat_fixture as efx  # noqa: E402
import overture_fixture as fx  # noqa: E402
import shapely  # noqa: E402

from index_build import (  # noqa: E402
    eurostat,
    expand,
    geometry,
    metros,
    overrides,
    overture,
    seed,
    store,
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """The stage can download its pinned inputs; a test never may."""

    def refuse(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)


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
    fx.division(
        "fi-esp",
        "FI",
        "locality",
        wikidata="Q7",
        name="Espoo",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-esp", "locality", "Espoo")
        ),
    ),
    fx.division(
        "fi-oul",
        "FI",
        "locality",
        wikidata="Q8",
        name="Oulu",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-oul", "locality", "Oulu")
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
    {
        "feed_id": "f-esp",
        "mdb": {
            "location": {
                "country_code": "FI",
                "subdivision_name": None,
                "municipality": "Espoo",
            }
        },
    },
    {
        "feed_id": "f-oul",
        "mdb": {
            "location": {
                "country_code": "FI",
                "subdivision_name": None,
                "municipality": "Oulu",
            }
        },
    },
]

CHICAGO_METRO = {
    "qid": "Q1754965",
    "name": "Chicago metropolitan area",
    "cbsa": "16980",
}

WEST = shapely.box(24.0, 60.0, 25.0, 61.0)  # FI1B1, the Helsinki metro
EAST = shapely.box(25.0, 60.0, 26.0, 61.0)  # FI1C1, the Helsinki metro too
SOUTH = shapely.box(24.0, 59.0, 26.0, 60.0)  # FI1E1, the Tampere metro, no city
COMPOSITION = [
    ("FI1B1", "Y", "FI001MC", "Helsinki"),
    ("FI1C1", "Y", "FI001MC", "Helsinki"),
    ("FI1E1", "Y", "FI002M", "Tampere"),
]
BOUNDARIES = [("FI1B1", WEST), ("FI1C1", EAST), ("FI1E1", SOUTH)]
OSM = {"dataset": "OpenStreetMap", "license": "ODbL-1.0", "record_id": "relation/1"}
# Helsinki and Espoo sit in the metro's two regions; Oulu has no land area.
AREAS = [
    fx.area("fi-hel", shapely.to_wkb(shapely.box(24.4, 60.1, 24.6, 60.3)), [OSM]),
    fx.area("fi-esp", shapely.to_wkb(shapely.box(25.1, 60.1, 25.3, 60.3)), [OSM]),
]
MEMBERS = ["Q1757", "Q7"]


def _crosswalk(digest=None, place="Q673425", code="FI001MC"):
    """A crosswalk entry, confirmed against the Helsinki metro's derived member
    list unless another ``digest`` is given."""
    return {
        "place": place,
        "set_statistical_area": {"scheme": "eurostat_metro", "code": code},
        "evidence_hash": digest or overrides.canonical_digest(MEMBERS),
    }


def _assignments(tmp_path):
    return [
        (
            row["city_id"],
            row["status"],
            row["nuts_id"],
            row["metro_code"],
            row["published"],
        )
        for row in _artefact(tmp_path, "metro_assignments.jsonl")
    ]


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


def _inputs(tmp_path, cache):
    """Publish the Eurostat fixture inputs for ``cache``; return their pins."""
    files = {
        eurostat.COMPOSITION_FILE: tmp_path / "composition.xlsx",
        eurostat.BOUNDARIES_FILE: tmp_path / "boundaries.parquet",
    }
    files[eurostat.COMPOSITION_FILE].write_bytes(efx.workbook(COMPOSITION))
    files[eurostat.BOUNDARIES_FILE].write_bytes(efx.parquet(BOUNDARIES))
    pins = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in files.items()
    }
    eurostat.prepare_inputs(cache, files=files, expected=pins)
    return pins


def _run(tmp_path, metro_map, overrides_dir=None, candidates=None):
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS)
    areas = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    _publish(cache, "crosswalk", "feeds.json", "feeds.jsonl", FEEDS)
    overture.resolve(cache, dataset=dataset, wikidata=fx.StubWikidata())
    seed.resolve_seed(
        cache, dataset=dataset, wikidata=fx.StubWikidata(), overrides_dir=overrides_dir
    )
    pins = _inputs(tmp_path, cache)
    manifest = metros.attach_metros(
        cache,
        wikidata=fx.StubWikidata(metros=metro_map, candidates=candidates),
        overrides_dir=overrides_dir,
        dataset=areas,
        pins=pins,
    )
    places, _ = store.read_jsonl(
        cache / "gazetteer", "metros.json", "places_seed.jsonl"
    )
    return manifest, {p["place_id"]: p for p in places}


def _artefact(tmp_path, name):
    rows, _ = store.read_jsonl(tmp_path / "cache" / "gazetteer", "metros.json", name)
    return rows


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
    # Two cities linked to one codeless MSA: one reported metro, two links.
    manifest, places = _run(tmp_path, {"Q1297": [codeless], "Q28515": [codeless]})
    assert "Q999" not in places
    assert places["Q1297"]["metro_ids"] == []
    assert manifest["metros"] == 0
    assert manifest["us"] == {"metros_published": 0, "metros_reported": 1}
    report = _artefact(tmp_path, "metro_report.jsonl")
    assert [row["reason"] for row in report if row["branch"] == "us"] == [
        "US MSA without a CBSA code"
    ] * 2


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
                    "code": {"value": "35620"},
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
    # The same links, read as code-less candidates for a curated crosswalk.
    assert client.metro_candidates(["Q60", "Q65"]) == {
        "Q60": [{"qid": "Q683705", "name": "New York metropolitan area"}],
        "Q65": [{"qid": "Q1755545", "name": "Los Angeles metropolitan area"}],
    }
    # An id that is not a QID never reaches the query text.
    with pytest.raises(overture.GazetteerError, match="not Wikidata QIDs"):
        client.metro_candidates(["Q60", "Q1 } UNION { ?x ?y ?z"])


def test_set_place_members_replaces_a_metros_members_reciprocally(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    both = {"Q1297": [CHICAGO_METRO], "Q28515": [CHICAGO_METRO]}
    entries = [
        {"place": "Q1754965", "set_place_members": ["Q1297"], "evidence_hash": "0" * 64}
    ]
    manifest, places = _run(
        tmp_path, both, overrides_dir=write_overrides(tmp_path, places=entries)
    )
    assert manifest["places_overrides_sha256"] == overrides.places_digest(
        tmp_path / "overrides"
    )
    assert places["Q1754965"]["member_ids"] == ["Q1297"]
    assert "Q1754965" in places["Q1297"]["metro_ids"]
    assert "Q1754965" not in places["Q28515"]["metro_ids"]
    # Recorded against evidence that moved: applied, reported, counted.
    assert manifest["overrides_applied"] == 1 and manifest["stale_overrides"] == 1
    report, _ = store.read_jsonl(
        tmp_path / "cache" / "gazetteer", "metros.json", "override_report.jsonl"
    )
    assert report[0]["current_evidence_hash"] == overrides.canonical_digest(
        ["Q1297", "Q28515"]
    )
    with pytest.raises(overrides.OverrideError, match="needs a seeded metro"):
        _run(
            tmp_path / "bad",
            both,
            overrides_dir=write_overrides(
                tmp_path / "bad",
                places=[{"place": "Q1297", "set_place_members": ["Q28515"]}],
            ),
        )


def test_a_curated_metro_and_its_statistical_twin_are_one_row(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    entries = [
        {
            "place": "Q1754965",
            "add_place": {
                "kind": "metro",
                "name": "Chicagoland",
                "member_ids": ["Q1297"],
            },
        }
    ]
    directory = write_overrides(tmp_path, places=entries)
    manifest, places = _run(
        tmp_path, {"Q28515": [CHICAGO_METRO]}, overrides_dir=directory
    )
    metro = places["Q1754965"]
    assert metro["curated"] is True and metro["name"] == "Chicagoland"
    # The curator's member list is authoritative: the statistical member
    # joins neither here nor when the expand stage discovers it.
    assert metro["member_ids"] == ["Q1297"]
    assert "Q1754965" not in places["Q28515"]["metro_ids"]
    wikidata = fx.StubWikidata(metros={"Q28515": [CHICAGO_METRO]})
    assert expand._attach_metros(places, ["Q28515"], wikidata, []) == []
    assert metro["member_ids"] == ["Q1297"]
    assert manifest["places"] == len(places)
    # places.yaml edited between the seed and metros stages: refused.
    (directory / "places.yaml").write_text("[]\n")
    with pytest.raises(overrides.OverrideError, match="re-run the gazetteer"):
        metros.attach_metros(
            tmp_path / "cache",
            wikidata=fx.StubWikidata(metros={}),
            overrides_dir=directory,
            pins=_inputs(tmp_path, tmp_path / "cache"),
        )


def test_a_metro_qid_already_seeded_as_a_city_fails_the_build(tmp_path):
    collided = {"Q28515": [{**CHICAGO_METRO, "qid": "Q1297"}]}
    with pytest.raises(overture.GazetteerError, match="already seeded"):
        _run(tmp_path, collided)
    places = {
        qid: {"place_id": qid, "kind": "city", "country_code": "US", "metro_ids": []}
        for qid in ("Q28515", "Q1297")
    }
    with pytest.raises(overture.GazetteerError, match="already seeded"):
        expand._attach_metros(places, ["Q28515"], fx.StubWikidata(metros=collided), [])


@pytest.mark.parametrize("stale", [False, True])
def test_a_eurostat_metro_publishes_through_the_crosswalk(tmp_path, stale):
    from test_index_place_overrides import write_overrides

    entry = _crosswalk("0" * 64) if stale else _crosswalk()
    manifest, places = _run(
        tmp_path, {}, overrides_dir=write_overrides(tmp_path, places=[entry])
    )
    metro = places["Q673425"]
    assert metro["kind"] == "metro" and metro["statistical_area_id"] == "FI001MC"
    assert metro["source_subtype"] == "metropolitan region"
    assert metro["country_code"] == "FI" and metro["name"] == "Helsinki"
    # Cities from the metro's two NUTS-3 regions, wired reciprocally.
    assert metro["member_ids"] == MEMBERS
    assert all(places[city]["metro_ids"] == ["Q673425"] for city in MEMBERS)
    assert _assignments(tmp_path) == [
        ("Q1757", "assigned", "FI1B1", "FI001MC", True),
        ("Q7", "assigned", "FI1C1", "FI001MC", True),
        ("Q8", "unplaceable", None, None, False),
    ]
    report = _artefact(tmp_path, "metro_report.jsonl")
    reported = [row["metro_code"] for row in report if row["branch"] == "eurostat"]
    assert reported == ["FI002M"]  # no city falls in it: reported, never minted
    summary = manifest["eurostat"]
    assert summary["metros_published"] == 1 and summary["metros_reported"] == 1
    assert summary["covered_countries"] == ["FI"]
    assert summary["assignments"] == {"assigned": 2, "unplaceable": 1}
    assert manifest["overrides_applied"] == 1
    assert manifest["us"] == {"metros_published": 0, "metros_reported": 0}
    assert [row["allowed"] for row in manifest["derived_inventory"]] == [True] * 3
    # A confirmation against evidence that moved: applied, reported, counted.
    assert manifest["stale_overrides"] == (1 if stale else 0)


def test_a_curated_twin_keeps_its_name_and_gains_the_eurostat_identity(tmp_path):
    from test_index_place_overrides import write_overrides

    curated = {"kind": "metro", "name": "Greater Helsinki", "member_ids": ["Q1757"]}
    entries = [{"place": "Q673425", "add_place": curated}, _crosswalk()]
    _, places = _run(
        tmp_path, {}, overrides_dir=write_overrides(tmp_path, places=entries)
    )
    metro = places["Q673425"]
    assert metro["curated"] is True and metro["name"] == "Greater Helsinki"
    assert metro["statistical_area_id"] == "FI001MC" and metro["country_code"] == "FI"
    assert metro["source_subtype"] == "metropolitan region"
    assert metro["resolution_method"] == "statistical_code"
    # The curator's member list is authoritative over the derived one.
    assert metro["member_ids"] == ["Q1757"] and places["Q7"]["metro_ids"] == []


def test_a_eurostat_metro_without_a_crosswalk_is_reported_with_candidates(tmp_path):
    from test_index_place_overrides import write_overrides

    # Tampere has a crosswalk but no city; Helsinki has cities but no crosswalk.
    tampere = _crosswalk("0" * 64, place="Q999", code="FI002M")
    linked = {
        "Q1757": [
            {"qid": "Q673425", "name": "Helsinki metropolitan area"},
            {"qid": "Q940914", "name": "Helsinki sub-region"},
        ],
        "Q7": [{"qid": "Q673425", "name": "Helsinki metropolitan area"}],
    }
    manifest, places = _run(
        tmp_path,
        {},
        overrides_dir=write_overrides(tmp_path, places=[tampere]),
        candidates=linked,
    )
    assert "Q673425" not in places
    empty = places["Q999"]  # crosswalked without a city: published, memberless
    assert empty["statistical_area_id"] == "FI002M" and empty["member_ids"] == []
    report = _artefact(tmp_path, "metro_report.jsonl")
    assert [row for row in report if row["branch"] == "eurostat"] == [
        {
            "branch": "eurostat",
            "metro_code": "FI001MC",
            "name": "Helsinki",
            "country": "FI",
            "member_ids": MEMBERS,
            "reason": "no crosswalk entry",
            "candidates": [
                {"qid": "Q673425", "name": "Helsinki metropolitan area", "cities": 2},
                {"qid": "Q940914", "name": "Helsinki sub-region", "cities": 1},
            ],
        },
    ]
    assert [row[1:] for row in _assignments(tmp_path)] == [
        ("assigned", "FI1B1", "FI001MC", False),
        ("assigned", "FI1C1", "FI001MC", False),
        ("unplaceable", None, None, False),
    ]
    summary = manifest["eurostat"]
    assert summary["metros_published"] == 1 and summary["metros_reported"] == 1
    # The empty-membership crosswalk was still judged against its evidence.
    assert manifest["stale_overrides"] == 1 and manifest["overrides_applied"] == 1


@pytest.mark.parametrize("missing", metros.EUROSTAT_DERIVED)
def test_the_derived_gate_closes_the_eurostat_branch(tmp_path, monkeypatch, missing):
    from test_index_place_overrides import write_overrides

    allowlist = geometry.DERIVED_SOURCE_ALLOWLIST - {missing}
    monkeypatch.setattr(geometry, "DERIVED_SOURCE_ALLOWLIST", allowlist)
    manifest, places = _run(
        tmp_path,
        {},
        overrides_dir=write_overrides(tmp_path, places=[_crosswalk("0" * 64)]),
    )
    assert "Q673425" not in places
    report = _artefact(tmp_path, "metro_report.jsonl")
    reasons = {
        r["metro_code"]: r["reason"] for r in report if r["branch"] == "eurostat"
    }
    assert reasons == {
        "FI001MC": "derived inputs not allowlisted",
        "FI002M": "no member cities",
    }
    allowed = {
        (row["dataset"], row["license"]): row["allowed"]
        for row in manifest["derived_inventory"]
    }
    assert allowed[missing] is False and sum(allowed.values()) == 2
    assert not any(row[-1] for row in _assignments(tmp_path))
    # Judged even while the gate is closed.
    assert manifest["stale_overrides"] == 1


@pytest.mark.parametrize(
    ("entries", "metro_map", "error", "message"),
    [
        ([_crosswalk(code="FI999M")], {}, "override", "not in the pinned composition"),
        ([_crosswalk(), _crosswalk(place="Q999")], {}, "override", "crosswalked twice"),
        ([_crosswalk(), _crosswalk(code="FI002M")], {}, "override", "duplicate"),
        ([_crosswalk(place="Q1757")], {}, "gazetteer", "already seeded as the city"),
        # Checked even for a metro no city falls in.
        ([_crosswalk(place="Q1757", code="FI002M")], {}, "gazetteer", "already seeded"),
        (
            [
                {
                    "place": "Q673425",
                    "add_place": {
                        "kind": "metro",
                        "name": "Elsewhere",
                        "country_code": "US",
                        "member_ids": ["Q1297"],
                    },
                },
                _crosswalk(),
            ],
            {},
            "override",
            "conflicts with the metro's identity",
        ),
        # The Chicago MSA the US branch minted this run is not a Eurostat metro.
        (
            [_crosswalk(place="Q1754965")],
            {"Q1297": [CHICAGO_METRO]},
            "override",
            "conflicts with the metro's identity",
        ),
    ],
)
def test_wrong_crosswalk_entries_are_refused(
    tmp_path, entries, metro_map, error, message
):
    from test_index_place_overrides import write_overrides

    kind = overrides.OverrideError if error == "override" else overture.GazetteerError
    with pytest.raises(kind, match=message):
        _run(
            tmp_path, metro_map, overrides_dir=write_overrides(tmp_path, places=entries)
        )


def test_crosswalk_targets_are_checked_before_the_gates(tmp_path, monkeypatch):
    from test_index_place_overrides import write_overrides

    # With the derived gate closed nothing publishes, yet a crosswalk naming a
    # seeded city is still refused rather than silently reported.
    allowlist = geometry.DERIVED_SOURCE_ALLOWLIST - {metros.EUROSTAT_DERIVED[0]}
    monkeypatch.setattr(geometry, "DERIVED_SOURCE_ALLOWLIST", allowlist)
    entries = [_crosswalk(place="Q1757")]
    with pytest.raises(overture.GazetteerError, match="already seeded as the city"):
        _run(tmp_path, {}, overrides_dir=write_overrides(tmp_path, places=entries))


def test_the_derived_credits_reach_the_inventory_and_notice(tmp_path):
    from test_index_place_overrides import write_overrides

    overrides_dir = write_overrides(tmp_path, places=[_crosswalk()])
    _run(tmp_path, {}, overrides_dir=overrides_dir)
    cache = tmp_path / "cache"
    areas = fx.write_area_dataset(tmp_path / "areas-geometry.parquet", AREAS)
    geometry.attach_geometry(cache, dataset=areas, overrides_dir=overrides_dir)
    rows, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "licence_inventory.jsonl"
    )
    derived = [row for row in rows if row["use"] == "derived"]
    assert [row["dataset"] for row in derived] == [
        d for d, _ in metros.EUROSTAT_DERIVED
    ]
    assert all(row["credit"] and row["url"] and row["allowed"] for row in derived)
    generation, _ = store.resolve(cache / "gazetteer", "geometry.json")
    with generation:
        notice = generation.read_bytes("NOTICE").decode("utf-8")
    assert "Source: Eurostat, metropolitan regions (NUTS 2021)" in notice
    assert "© EuroGeographics for the administrative boundaries" in notice
