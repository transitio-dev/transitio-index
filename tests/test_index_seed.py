import pytest

pytest.importorskip("pyarrow")
import overture_fixture as fx  # noqa: E402

from transitio_index import coverage, overture, registry, seed, store  # noqa: E402

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
    # Helsinki as Overture lists a city that is its own district: the locality's
    # hierarchy passes through a same-QID county twin, which is the same place.
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
            ("fi-helsinki-county", "county", "Helsinki"),
            ("fi-helsinki", "locality", "Helsinki"),
        ),
    ),
    fx.division(
        "fi-helsinki-county",
        "FI",
        "county",
        wikidata="Q1757",
        name="Helsinki",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-helsinki-county", "county", "Helsinki"),
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
    # A district the skeleton resolves (a county, kind region); a feed's
    # municipality field may name it, in any of its labels.
    fx.division(
        "fi-keski",
        "FI",
        "county",
        wikidata="Q999001",
        name="Keski-Uusimaa",
        common={"en": "Central Uusimaa (district)"},
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-keski", "county", "Keski-Uusimaa"),
        ),
    ),
    # Lahti: a city that is its own district — a locality and a county under
    # one QID — beside a same-name locality with another QID and a QID-less
    # one. The shared QID marks the city.
    fx.division(
        "fi-lahti",
        "FI",
        "locality",
        wikidata="Q2143",
        name="Lahti",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-lahti-county", "county", "Lahti"),
            ("fi-lahti", "locality", "Lahti"),
        ),
    ),
    fx.division(
        "fi-lahti-county",
        "FI",
        "county",
        wikidata="Q2143",
        name="Lahti",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-lahti-county", "county", "Lahti"),
        ),
    ),
    fx.division(
        "fi-lahti-other",
        "FI",
        "locality",
        wikidata="Q999002",
        name="Lahti",
        admin_level=2,
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-lahti-other", "locality", "Lahti"),
        ),
    ),
    fx.division(
        "fi-lahti-x",
        "FI",
        "locality",
        wikidata=None,
        name="Lahti",
        admin_level=2,
        sources=[
            {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "Z"}
        ],
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-lahti-x", "locality", "Lahti"),
        ),
    ),
    # A QID-less hamlet named like the region: a municipality field naming
    # "Uusimaa" must not be placed here.
    fx.division(
        "fi-uusimaa-x",
        "FI",
        "locality",
        wikidata=None,
        name="Uusimaa",
        admin_level=2,
        sources=[
            {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "W"}
        ],
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-uusimaa-x", "locality", "Uusimaa"),
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
    {
        "feed_id": "f-district",
        "mdb": _mdb("FI", "Uusimaa", "Central Uusimaa (district)"),
    },
    {"feed_id": "f-district-wrongsub", "mdb": _mdb("FI", "Lapland", "Keski-Uusimaa")},
    {"feed_id": "f-atlantis", "mdb": _mdb("FI", None, "Atlantis")},
    {"feed_id": "f-lahti", "mdb": _mdb("FI", None, "Lahti")},
    {"feed_id": "f-uusimaa-muni", "mdb": _mdb("FI", "Uusimaa", "Uusimaa")},
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


def _seed(tmp_path, feeds=FEEDS, overrides_dir=None, registry=None):
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS)
    _publish(cache, "crosswalk", "feeds.json", "feeds.jsonl", feeds)
    overture.resolve(cache, dataset=dataset, wikidata=fx.StubWikidata())
    manifest = seed.resolve_seed(
        cache,
        dataset=dataset,
        wikidata=fx.StubWikidata(),
        overrides_dir=overrides_dir,
        registry=registry,
    )
    places, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "places_seed.jsonl")
    report, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "seed_report.jsonl")
    # Keyed by QID where the row has one: the key the tests know the
    # fixtures by, whether or not a registry re-keyed the rows by own id.
    return manifest, {p.get("wikidata_id") or p["place_id"]: p for p in places}, report


def test_the_seed_identifies_every_place_in_the_registry(tmp_path):
    path = tmp_path / "places_registry.jsonl"
    path.write_text('{"next_id": 1, "registry": 1}\n')
    with registry.session(path) as reg:
        manifest, places, _ = _seed(tmp_path, registry=reg)
        assert reg.minted == len(places) and manifest["registry_base"] == reg.base
        reg.save()
    ids = {qid: place["place_id"] for qid, place in places.items()}
    assert all(registry.ID_PATTERN.match(tp) for tp in ids.values())
    assert len(set(ids.values())) == len(ids)
    assert all(
        place["wikidata_id"] == (qid if overture.QID_PATTERN.match(qid) else None)
        for qid, place in places.items()
    )
    assert manifest["identified"] == len(places)
    saved = registry.load(path)
    assert saved.resolve("Q1757") == ids["Q1757"]
    saved_nowhere = saved.resolve("overture:us-noqid")
    nowhere = next(p for p in places.values() if p["name"] == "Nowheresville")
    assert nowhere["place_id"] == saved_nowhere and nowhere["wikidata_id"] is None
    concordances = saved.effective(ids["Q1757"])
    assert concordances["wikidata"] == ["Q1757"]
    assert concordances["overture"] == [places["Q1757"]["overture_id"]]
    assert saved.rows[ids["Q1757"]]["minted_in"].startswith("overture ")
    # A rebuild finds every place again and mints nothing; the ids hold and
    # the registry file is byte-identical.
    before = path.read_bytes()
    with registry.session(path) as again:
        _, places_again, _ = _seed(tmp_path, registry=again)
        assert (again.minted, again.enriched) == (0, 0)
        again.save()
    assert path.read_bytes() == before
    assert {qid: p["place_id"] for qid, p in places_again.items()} == ids
    # Links between places are own ids too, the placements included.
    assert places["Q1757"]["parent_id"] == ids["Q1508"]
    placements, _ = store.read_jsonl(
        tmp_path / "cache" / "gazetteer", "seed.json", "feed_places.jsonl"
    )
    assert {p["feed_id"]: p["place_id"] for p in placements}["f-hel"] == ids["Q1757"]
    # Read-only against a header-only registry: refused at the first place.
    empty = tmp_path / "empty_registry.jsonl"
    empty.write_text('{"next_id": 1, "registry": 1}\n')
    with registry.session(empty, read_only=True) as read_only:
        with pytest.raises(registry.RegistryError, match="read-only"):
            _seed(tmp_path, registry=read_only)
    # Two seeded QIDs the registry has merged — Espoo's into Helsinki's,
    # the alias sorting first — become one row under the survivor's id,
    # the row keyed by the canonical QID kept.
    aliased = tmp_path / "aliased_registry.jsonl"
    aliased.write_text(
        '{"next_id": 3, "registry": 1}\n'
        '{"place_id": "tp_1", "kind": "city", "concordances": {"wikidata": ["Q1757"]}, '
        '"name": "Helsinki", "country_code": "FI", "minted_from": "x", "minted_in": "y"}\n'
        '{"place_id": "tp_2", "status": "merged", "into": "tp_1", '
        '"concordances": {"wikidata": ["Q13291"]}, "at": "2026-09-08", "reason": "dup"}\n'
    )
    with registry.session(aliased) as merged:
        _, places_merged, _ = _seed(tmp_path, registry=merged)
        assert merged.minted == len(places) - 2
    assert "Q13291" not in places_merged
    helsinki = places_merged["Q1757"]
    assert helsinki["place_id"] == "tp_1" and helsinki["name"] == "Helsinki"
    assert helsinki["parent_id"] == places_merged["Q1508"]["place_id"]


def test_seed_places_a_city_with_its_ancestors(tmp_path):
    _, places, _ = _seed(tmp_path)
    # Helsinki resolves to its QID and its admin parent is the region: the
    # same-QID county rung in its hierarchy is Helsinki itself, never its
    # parent, and the one place emitted for the QID is the locality.
    helsinki = places["Q1757"]
    assert helsinki["kind"] == "city"
    assert helsinki["parent_id"] == "Q1508"
    assert helsinki["overture_id"] == "fi-helsinki"
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
    # The QID-less city is placed by its Overture key.
    assert by_feed["f-nowhere"]["place_id"] == "overture:us-noqid"
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


def test_a_city_without_a_qid_is_seeded_by_its_division(tmp_path):
    _, places, report = _seed(tmp_path)
    nowhere = places["overture:us-noqid"]
    assert nowhere["kind"] == "city" and nowhere["name"] == "Nowheresville"
    assert nowhere["resolution_method"] == "overture_id"
    assert places[nowhere["parent_id"]]["name"] == "Illinois"
    assert not any(r["feed_id"] == "f-nowhere" for r in report)
    placements, _ = store.read_jsonl(
        tmp_path / "cache" / "gazetteer", "seed.json", "feed_places.jsonl"
    )
    assert {p["feed_id"]: p["place_id"] for p in placements}["f-nowhere"] == (
        "overture:us-noqid"
    )


def test_a_qidless_same_name_sibling_makes_the_match_ambiguous(tmp_path):
    # "Vantaa" matches a QID-bearing division and a QID-less one, so even though
    # a single QID is present the identity is unprovable and it is reported.
    _, places, report = _seed(tmp_path)
    assert "Q13360" not in places
    entry = next(r for r in report if r["feed_id"] == "f-vantaa")
    assert entry["reason"] == "the name also matches a division without a QID"


def test_a_municipality_naming_a_district_places_the_feed_there(tmp_path):
    # "Central Uusimaa (district)" names no locality; it is the English label
    # of a county the skeleton resolved, so the feed is placed at the district.
    _, places, report = _seed(tmp_path)
    district = places["Q999001"]
    assert district["kind"] == "region" and district["parent_id"] == "Q1508"
    placements, _ = store.read_jsonl(
        tmp_path / "cache" / "gazetteer", "seed.json", "feed_places.jsonl"
    )
    by_feed = {p["feed_id"]: p for p in placements}
    assert by_feed["f-district"] == {
        "feed_id": "f-district",
        "place_id": "Q999001",
        "level": "district",
    }
    assert "district" in coverage.DECLARED_LEVELS  # the coverage stage accepts it
    # The declared subdivision must corroborate a district too, and a name
    # found at neither level says so.
    reasons = {r["feed_id"]: r["reason"] for r in report}
    assert (
        reasons["f-district-wrongsub"]
        == "the declared subdivision matches no same-name division"
    )
    assert (
        reasons["f-atlantis"]
        == "no locality or district of that name in the declared country"
    )


def test_a_name_shared_by_a_city_and_its_own_district_is_the_city(tmp_path):
    # Lahti's locality and county carry one QID; the same-name locality with
    # another QID and the QID-less one are hamlets, so the city places — while
    # Vantaa (a lone QID beside a QID-less sibling, no district twin) and the
    # two Springfields (no shared QID) are still reported.
    _, places, report = _seed(tmp_path)
    assert places["Q2143"]["overture_id"] == "fi-lahti"
    assert places["Q2143"]["kind"] == "city"
    assert places["Q2143"]["parent_id"] == "Q1508"  # the region, not the county twin
    assert "Q999002" not in places
    reasons = {r["feed_id"]: r["reason"] for r in report}
    assert "f-lahti" not in reasons
    # A municipality field naming the region, beside a QID-less hamlet of that
    # name, is reported rather than placed at the hamlet; the declared
    # subdivision corroborates the region by its own name.
    assert reasons["f-uusimaa-muni"] == "the name also matches a division without a QID"
    assert "overture:fi-uusimaa-x" not in places


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

    from transitio_index import overrides

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

    from transitio_index import overrides

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


def test_curated_references_resolve_before_the_seed_applies_them(tmp_path):
    from test_index_place_overrides import write_overrides

    path = tmp_path / "places_registry.jsonl"
    path.write_text('{"next_id": 1, "registry": 1}\n')
    with registry.session(path) as reg:
        _seed(tmp_path, registry=reg)
        reg.save()
    helsinki = registry.load(path).resolve("Q1757")
    directory = write_overrides(
        tmp_path,
        places=[
            {
                "place": "Q77",
                "add_place": {"kind": "metro", "name": "M", "member_ids": [helsinki]},
            }
        ],
    )
    with registry.session(path) as reg:
        _, places, _ = _seed(tmp_path, overrides_dir=directory, registry=reg)
    assert places["Q77"]["member_ids"] == [places["Q1757"]["place_id"]]
    assert places["Q77"]["place_id"] in places["Q1757"]["metro_ids"]


def test_a_curated_place_without_a_qid_is_minted_and_keyed_by_its_own_id(tmp_path):
    from test_index_place_overrides import write_overrides

    directory = write_overrides(
        tmp_path,
        places=[
            {
                "place": "fao_city_region:R1",
                "add_place": {
                    "kind": "metro",
                    "name": "Region",
                    "member_ids": ["Q1757"],
                },
            }
        ],
    )
    path = tmp_path / "places_registry.jsonl"
    path.write_text('{"next_id": 1, "registry": 1}\n')
    with registry.session(path) as reg:
        _, places, _ = _seed(tmp_path, overrides_dir=directory, registry=reg)
        reg.save()
    saved = registry.load(path)
    region_id = saved.resolve("fao_city_region:R1")
    region = places[region_id]
    assert region["wikidata_id"] is None and region["kind"] == "metro"
    assert region["member_ids"] == [places["Q1757"]["place_id"]]
    # A rebuild resolves the reference to the own id and finds the row.
    with registry.session(path) as again:
        _, places_again, _ = _seed(tmp_path, overrides_dir=directory, registry=again)
        assert again.minted == 0
    assert places_again[region_id]["place_id"] == region_id


def test_rekey_folds_the_links_of_rows_the_registry_merged():
    rows = {
        "Q1": {
            "tp_id": "tp_1",
            "metro_ids": ["Q9"],
            "member_ids": [],
            "wikidata_id": "Q1",
        },
        "Q2": {
            "tp_id": "tp_1",
            "metro_ids": ["Q8"],
            "member_ids": [],
            "wikidata_id": "Q1",
        },
        "Q8": {
            "tp_id": "tp_8",
            "metro_ids": [],
            "member_ids": ["Q2"],
            "wikidata_id": "Q8",
        },
        "Q9": {
            "tp_id": "tp_9",
            "metro_ids": [],
            "member_ids": ["Q1"],
            "wikidata_id": "Q9",
        },
    }
    out = seed.rekey(rows, by="tp_id", canonical={"tp_1": "Q1"})
    assert set(out) == {"tp_1", "tp_8", "tp_9"}
    # The survivor keeps both metros; each metro's member list names it.
    assert out["tp_1"]["metro_ids"] == ["tp_8", "tp_9"]
    assert out["tp_8"]["member_ids"] == out["tp_9"]["member_ids"] == ["tp_1"]


def test_only_a_plainly_unresolved_division_is_a_place_without_a_qid():
    plain = {
        "qid": None,
        "overture_id": "x",
        "subtype": "locality",
        "name": "X",
        "resolution_reason": overture.UNRESOLVED,
    }
    conflicting = {**plain, "resolution_reason": "conflicting P402 identities"}
    assert seed._unique_identity([plain]) == (plain, None)
    division, reason = seed._unique_identity([conflicting])
    assert division is None and "conflict" in reason
