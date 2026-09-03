import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

pytest.importorskip("pyarrow")
import overture_fixture as fx  # noqa: E402

from index_build import overture, seed, store  # noqa: E402

# Skeleton (resolved by the 5a stage) + localities (resolved by the seed stage).
ROWS = [
    fx.division("fi", "FI", "country", wikidata="Q33", name="Finland"),
    fx.division(
        "fi-uusimaa",
        "FI",
        "region",
        wikidata="Q1508",
        name="Uusimaa",
        admin_level=1,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-uusimaa", "region", "Uusimaa")
        ),
    ),
    fx.division("us", "US", "country", wikidata="Q30", name="United States"),
    fx.division("se", "SE", "country", wikidata="Q34", name="Sweden"),
    fx.division(
        "us-il", "US", "region", wikidata="Q1204", name="Illinois", admin_level=1
    ),
    fx.division(
        "us-mo", "US", "region", wikidata="Q1581", name="Missouri", admin_level=1
    ),
    fx.division(
        "fi-helsinki",
        "FI",
        "locality",
        wikidata="Q1757",
        name="Helsinki",
        common={"en": "Helsinki", "sv": "Helsingfors"},
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-helsinki", "locality", "Helsinki"),
        ),
    ),
    # A localadmin sharing Helsinki's QID: the locality must win.
    fx.division(
        "fi-helsinki-la",
        "FI",
        "localadmin",
        wikidata="Q1757",
        name="Helsinki",
        admin_level=3,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-helsinki-la", "localadmin", "Helsinki"),
        ),
    ),
    fx.division(
        "fi-espoo",
        "FI",
        "locality",
        wikidata="Q13291",
        name="Espoo",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-espoo", "locality", "Espoo"),
        ),
    ),
    fx.division(
        "us-spring-il",
        "US",
        "locality",
        wikidata="Q28515",
        name="Springfield",
        admin_level=2,
        hierarchies=fx.chain(
            ("us", "country", "United States"),
            ("us-il", "region", "Illinois"),
            ("us-spring-il", "locality", "Springfield"),
        ),
    ),
    fx.division(
        "us-spring-mo",
        "US",
        "locality",
        wikidata="Q54089",
        name="Springfield",
        admin_level=2,
        hierarchies=fx.chain(
            ("us", "country", "United States"),
            ("us-mo", "region", "Missouri"),
            ("us-spring-mo", "locality", "Springfield"),
        ),
    ),
    fx.division(
        "us-noqid",
        "US",
        "locality",
        wikidata=None,
        name="Nowheresville",
        admin_level=2,
        sources=[
            {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "X"}
        ],
        hierarchies=fx.chain(
            ("us", "country", "United States"),
            ("us-il", "region", "Illinois"),
            ("us-noqid", "locality", "Nowheresville"),
        ),
    ),
    # Vantaa (QID) with a same-name, QID-less sibling in the same region.
    fx.division(
        "fi-vantaa",
        "FI",
        "locality",
        wikidata="Q13360",
        name="Vantaa",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-vantaa", "locality", "Vantaa"),
        ),
    ),
    fx.division(
        "fi-vantaa-x",
        "FI",
        "locality",
        wikidata=None,
        name="Vantaa",
        admin_level=2,
        sources=[
            {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "Y"}
        ],
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-vantaa-x", "locality", "Vantaa"),
        ),
    ),
    # Turku, reachable only through its Swedish name Åbo.
    fx.division(
        "fi-turku",
        "FI",
        "locality",
        wikidata="Q38511",
        name="Turku",
        common={"en": "Turku", "sv": "Åbo"},
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-turku", "locality", "Turku"),
        ),
    ),
]


def _mdb(country, subdivision, municipality):
    return {
        "location": {
            "country_code": country,
            "subdivision_name": subdivision,
            "municipality": municipality,
        }
    }


FEEDS = [
    {"feed_id": "f-hel", "mdb": _mdb("FI", "Uusimaa", "Helsinki")},
    {
        "feed_id": "f-esp",
        "mdb": None,
        "gbfs": {"country_code": "FI", "location": "Espoo"},
    },
    {"feed_id": "f-spring-il", "mdb": _mdb("US", "Illinois", "Springfield")},
    {"feed_id": "f-spring-amb", "mdb": _mdb("US", None, "Springfield")},
    {"feed_id": "f-nowhere", "mdb": _mdb("US", "Illinois", "Nowheresville")},
    {"feed_id": "f-nolocation", "mdb": _mdb(None, None, None)},
    {"feed_id": "f-subdiv", "mdb": _mdb("FI", "Uusimaa", None)},
    {"feed_id": "f-vantaa", "mdb": _mdb("FI", "Uusimaa", "Vantaa")},
    {"feed_id": "f-abo", "mdb": _mdb("FI", "Uusimaa", "Åbo")},
    {"feed_id": "f-wrongsub", "mdb": _mdb("FI", "Lapland", "Espoo")},
    {"feed_id": "f-country", "mdb": _mdb("SE", None, None)},
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


def _seed(tmp_path, feeds=FEEDS, overrides_dir=None):
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS)
    _publish(cache, "crosswalk", "feeds.json", "feeds.jsonl", feeds)
    overture.resolve(cache, dataset=dataset, wikidata=fx.StubWikidata())
    manifest = seed.resolve_seed(
        cache, dataset=dataset, wikidata=fx.StubWikidata(), overrides_dir=overrides_dir
    )
    places, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "places_seed.jsonl")
    report, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "seed_report.jsonl")
    return manifest, {p["place_id"]: p for p in places}, report


def test_seed_places_a_city_with_its_ancestors(tmp_path):
    _, places, _ = _seed(tmp_path)
    # Helsinki resolves to its QID and its admin parent is the region.
    helsinki = places["Q1757"]
    assert helsinki["kind"] == "city"
    assert helsinki["parent_id"] == "Q1508"
    assert helsinki["country_code"] == "FI"
    # Its ancestors are emitted as places too.
    assert places["Q1508"]["kind"] == "region"
    assert places["Q33"]["kind"] == "country"
    assert places["Q1508"]["parent_id"] == "Q33"


def test_a_gbfs_location_places_a_city(tmp_path):
    _, places, _ = _seed(tmp_path)
    assert places["Q13291"]["name"] == "Espoo"


def test_the_feed_to_place_link_is_persisted_with_its_level(tmp_path):
    manifest, _, _ = _seed(tmp_path)
    placements, _ = store.read_jsonl(
        tmp_path / "cache" / "gazetteer", "seed.json", "feed_places.jsonl"
    )
    by_feed = {p["feed_id"]: p for p in placements}
    assert by_feed["f-hel"] == {
        "feed_id": "f-hel",
        "place_id": "Q1757",
        "level": "municipality",
    }
    assert by_feed["f-subdiv"]["place_id"] == "Q1508"
    assert by_feed["f-subdiv"]["level"] == "subdivision"
    assert by_feed["f-country"]["level"] == "country"
    # Reported (unplaced) feeds have no link.
    assert "f-nowhere" not in by_feed
    assert "f-spring-amb" not in by_feed
    assert manifest["feeds_placed"] == len(placements)


def test_a_subdivision_disambiguates_a_shared_city_name(tmp_path):
    _, places, _ = _seed(tmp_path)
    # "Springfield" in Illinois must resolve to the Illinois one, not Missouri.
    assert "Q28515" in places
    assert "Q54089" not in places


def test_an_ambiguous_city_name_is_reported_not_minted(tmp_path):
    _, places, report = _seed(tmp_path)
    entry = next(r for r in report if r["feed_id"] == "f-spring-amb")
    assert "conflicting QIDs" in entry["reason"]


def test_a_city_without_a_qid_is_reported(tmp_path):
    _, _, report = _seed(tmp_path)
    entry = next(r for r in report if r["feed_id"] == "f-nowhere")
    assert entry["reason"] == "the matched division has no QID"


def test_a_qidless_same_name_sibling_makes_the_match_ambiguous(tmp_path):
    # "Vantaa" matches a QID-bearing division and a QID-less one, so even though
    # a single QID is present the identity is unprovable and it is reported.
    _, places, report = _seed(tmp_path)
    assert "Q13360" not in places
    entry = next(r for r in report if r["feed_id"] == "f-vantaa")
    assert entry["reason"] == "the name also matches a division without a QID"


def test_a_lone_city_in_the_wrong_subdivision_is_reported(tmp_path):
    # Espoo is unique, but the feed declares it in Lapland, not Uusimaa; the
    # subdivision must corroborate even a unique match, so it is reported.
    _, _, report = _seed(tmp_path)
    entry = next(r for r in report if r["feed_id"] == "f-wrongsub")
    assert entry["reason"] == "the declared subdivision matches no same-name division"


def test_a_subdivision_only_feed_places_its_region(tmp_path):
    _, places, report = _seed(tmp_path)
    assert "f-subdiv" not in {r["feed_id"] for r in report}
    assert places["Q1508"]["kind"] == "region"
    assert places["Q1508"]["parent_id"] == "Q33"


def test_a_country_only_feed_places_its_country(tmp_path):
    # Sweden is named by no city feed; the country-only feed still seeds it.
    _, places, report = _seed(tmp_path)
    assert "f-country" not in {r["feed_id"] for r in report}
    assert places["Q34"]["kind"] == "country"
    assert places["Q34"]["parent_id"] is None


def test_a_local_language_name_resolves(tmp_path):
    # "Åbo" is Turku's Swedish label; matching must find it by that name.
    _, places, report = _seed(tmp_path)
    assert "f-abo" not in {r["feed_id"] for r in report}
    assert places["Q38511"]["name"] == "Turku"


def test_a_feed_with_no_declared_place_is_skipped(tmp_path):
    manifest, _, report = _seed(tmp_path)
    assert "f-nolocation" not in {r["feed_id"] for r in report}
    # Every feed but f-nolocation carries a declared location.
    assert manifest["feeds_with_location"] == len(FEEDS) - 1


def test_a_locality_is_preferred_over_a_localadmin_of_the_same_qid(tmp_path):
    _, places, _ = _seed(tmp_path)
    # Both fi-helsinki (locality) and fi-helsinki-la (localadmin) carry Q1757;
    # the locality's overture id must be the one recorded.
    assert places["Q1757"]["overture_id"] == "fi-helsinki"


def test_declared_locations_prefers_mdb_and_skips_the_placeless():
    feeds = [
        {
            "feed_id": "a",
            "mdb": _mdb("FI", "Uusimaa", "Helsinki"),
            "gbfs": {"country_code": "SE", "location": "Stockholm"},
        },
        {
            "feed_id": "b",
            "mdb": None,
            "gbfs": {"country_code": "fi", "location": "Espoo"},
        },
        {"feed_id": "c", "mdb": _mdb(None, None, None)},  # no country at all
    ]
    located = list(seed.declared_locations(feeds))
    assert [d["feed_id"] for d in located] == ["a", "b"]
    assert located[0]["municipality"] == "Helsinki"  # MDB won over the GBFS block
    assert located[1]["country"] == "FI"  # country upper-cased


def test_norm_folds_accents_and_case():
    assert seed._norm("Málaga") == seed._norm("malaga") == "malaga"


def test_a_locality_wins_over_a_localadmin_of_one_qid_in_either_order():
    locality = {"place_id": "Q1", "source_subtype": "locality"}
    localadmin = {"place_id": "Q1", "source_subtype": "localadmin"}
    for first, second in ((localadmin, locality), (locality, localadmin)):
        places = {}
        seed._merge_place(places, first)
        seed._merge_place(places, second)
        assert places["Q1"]["source_subtype"] == "locality"


def test_add_place_upserts_curated_places(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    entries = [
        {
            "place": "Q9000",
            "add_place": {
                "kind": "metro",
                "name": "Greater Helsinki",
                "member_ids": ["Q1757"],
            },
            "reason": "curated",
        },
        {
            "place": "Q9001",
            "add_place": {
                "kind": "city",
                "name": "Sipoo",
                "parent_id": "Q1508",
                "country_code": "FI",
                "boundary": "POLYGON((25.2 60.3, 25.5 60.3, 25.5 60.5, 25.2 60.3))",
            },
        },
    ]
    manifest, places, _ = _seed(
        tmp_path, overrides_dir=write_overrides(tmp_path, places=entries)
    )
    metro, city = places["Q9000"], places["Q9001"]
    assert metro["curated"] is True and metro["member_ids"] == ["Q1757"]
    assert "Q9000" in places["Q1757"]["metro_ids"]
    assert city["parent_id"] == "Q1508" and city["boundary_wkt"].startswith("POLYGON")
    assert city["resolution_method"] == "curated" and city["names"]["en"] == "Sipoo"
    assert manifest["overrides_applied"] == 2 and manifest["stale_overrides"] == 0
    # A parent the seed never had is a build error, never a dangling id.
    orphan = [
        {
            "place": "Q9002",
            "add_place": {
                "kind": "city",
                "name": "X",
                "parent_id": "Q404",
                "boundary": "POINT(0 0)",
            },
        }
    ]
    with pytest.raises(overrides.OverrideError, match="not a seeded place"):
        _seed(
            tmp_path / "orphan",
            overrides_dir=write_overrides(tmp_path / "orphan", places=orphan),
        )


def test_resolve_place_assigns_a_qid_to_an_unresolved_candidate(tmp_path):
    from test_index_place_overrides import write_overrides

    # "Nowheresville" is an Overture locality without a QID, so f-nowhere is
    # reported unplaced; the curator names its QID by the candidate's
    # Overture id, and the feed lands there as a curated resolution.
    entries = [{"place": "Q99999", "source_ref": "us-noqid", "resolve_place": True}]
    manifest, places, report = _seed(
        tmp_path, overrides_dir=write_overrides(tmp_path, places=entries)
    )
    assert places["Q99999"]["resolution_method"] == "curated"
    assert places["Q99999"]["curated"] is True
    assert places["Q99999"]["parent_id"] == "Q1204"
    assert "f-nowhere" not in {r["feed_id"] for r in report}
    placements, _ = store.read_jsonl(
        tmp_path / "cache" / "gazetteer", "seed.json", "feed_places.jsonl"
    )
    assert {p["feed_id"]: p["place_id"] for p in placements}["f-nowhere"] == "Q99999"
    assert manifest["overrides_applied"] == 1


def test_add_place_on_an_existing_place_rewrites_its_provenance(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    entries = [
        {
            "place": "Q1757",
            "add_place": {
                "kind": "city",
                "name": "Helsinki (curated)",
                "parent_id": "Q1508",
            },
        },
        {"place": "Q77777", "source_ref": "no-such-candidate", "resolve_place": True},
    ]
    with pytest.raises(overrides.OverrideError, match="no candidate"):
        _seed(tmp_path, overrides_dir=write_overrides(tmp_path, places=entries))
    # A seeded place keeps its kind, boundary and members; a new one needs
    # a boundary or a member list.
    wkt = "POLYGON((24.9 60.1, 25.1 60.1, 25.1 60.3, 24.9 60.1))"
    refused = [
        ("Q1757", {**entries[0]["add_place"], "boundary": wkt}, "use set_boundary"),
        ("Q1757", {**entries[0]["add_place"], "kind": "metro"}, "cannot change"),
        ("Q900002", {"kind": "country", "name": "Nowhere"}, "needs a boundary"),
    ]
    for place, spec, message in refused:
        base = tmp_path / message.split()[-1]
        with pytest.raises(overrides.OverrideError, match=message):
            _seed(
                base,
                overrides_dir=write_overrides(
                    base, places=[{"place": place, "add_place": spec}]
                ),
            )
    loop = [
        {**entries[0], "add_place": {**entries[0]["add_place"], "parent_id": "Q1757"}}
    ]
    with pytest.raises(overrides.OverrideError, match="loops"):
        _seed(
            tmp_path / "loop",
            overrides_dir=write_overrides(tmp_path / "loop", places=loop),
        )
    manifest, places, _ = _seed(
        tmp_path / "ok",
        overrides_dir=write_overrides(tmp_path / "ok", places=entries[:1]),
    )
    helsinki = places["Q1757"]
    assert helsinki["curated"] is True and helsinki["resolution_method"] == "curated"
    assert helsinki["name"] == "Helsinki (curated)"
    assert helsinki["overture_id"] == "fi-helsinki"
    assert manifest["places_overrides_sha256"] == overrides.places_digest(
        tmp_path / "ok" / "overrides"
    )

    # A metro is a member relation, never an administrative parent.
    metro_parent = [
        {
            "place": "Q900003",
            "add_place": {"kind": "metro", "name": "M", "member_ids": ["Q1757"]},
        },
        {
            "place": "Q1757",
            "add_place": {**entries[0]["add_place"], "parent_id": "Q900003"},
        },
    ]
    with pytest.raises(overrides.OverrideError, match="is a metro"):
        _seed(
            tmp_path / "metro",
            overrides_dir=write_overrides(tmp_path / "metro", places=metro_parent),
        )


def test_resolve_place_to_a_qid_of_another_kind_is_a_collision(tmp_path):
    from test_index_place_overrides import write_overrides

    # Q1204 is Illinois, a region: the locality cannot become it.
    entries = [{"place": "Q1204", "source_ref": "us-noqid", "resolve_place": True}]
    with pytest.raises(overture.GazetteerError, match="is both the region"):
        _seed(tmp_path, overrides_dir=write_overrides(tmp_path, places=entries))
