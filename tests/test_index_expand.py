import hashlib
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
from index_build import boundaries, crawl, expand, overture, store  # noqa: E402

# A source the geometry allowlist accepts, and one it does not.
GOOD = [{"dataset": "OpenStreetMap", "license": "ODbL-1.0", "property": ""}]
BAD = [{"dataset": "Mystery Maps", "license": "proprietary", "property": ""}]


def _wkb(minx, miny, maxx, maxy):
    return shapely.to_wkb(shapely.box(minx, miny, maxx, maxy))


SEED_PLACES = [
    {
        "place_id": "Q33",
        "kind": "country",
        "name": "Finland",
        "country_code": "FI",
        "overture_id": "fi",
        "metro_ids": [],
        "member_ids": [],
    },
]

DIVISIONS = [
    fx.division(
        "fi",
        "FI",
        "country",
        wikidata="Q33",
        name="Finland",
        hierarchies=fx.chain(("fi", "country", "Finland")),
    ),
    fx.division(
        "fi-tre",
        "FI",
        "locality",
        wikidata="Q40840",
        name="Tampere",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-pirk", "region", "Pirkanmaa"),
            ("fi-tre", "locality", "Tampere"),
        ),
    ),
    fx.division(
        "fi-pirk",
        "FI",
        "region",
        wikidata="Q5697",
        name="Pirkanmaa",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-pirk", "region", "Pirkanmaa"),
        ),
    ),
    fx.division(
        "fi-noqid",
        "FI",
        "locality",
        name="Nowhere",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-noqid", "locality", "Nowhere")
        ),
    ),
    fx.division(
        "us-spring",
        "US",
        "locality",
        wikidata="Q28515",
        name="Springfield",
        hierarchies=fx.chain(
            ("us", "country", "USA"), ("us-spring", "locality", "Springfield")
        ),
    ),
    fx.division(
        "fi-badgeo",
        "FI",
        "locality",
        wikidata="Q999",
        name="Badgeo",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-badgeo", "locality", "Badgeo")
        ),
    ),
]

AREAS = [
    fx.area("fi", _wkb(19.0, 59.0, 32.0, 71.0), GOOD, country="FI"),
    fx.area("fi-tre", _wkb(23.6, 61.4, 24.0, 61.6), GOOD, country="FI"),
    # A second, disjoint component of the same city, far from any stop.
    fx.area("fi-tre", _wkb(21.9, 60.9, 22.1, 61.1), GOOD, country="FI"),
    fx.area("fi-pirk", _wkb(22.5, 61.0, 24.5, 62.0), GOOD, country="FI"),
    fx.area("fi-noqid", _wkb(26.0, 62.0, 26.4, 62.2), GOOD, country="FI"),
    fx.area("us-spring", _wkb(-89.8, 39.7, -89.5, 39.9), GOOD, country="US"),
    fx.area("fi-badgeo", _wkb(28.0, 62.0, 28.4, 62.2), BAD, country="FI"),
]


def _publish_names(cache, places):
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "gazetteer",
                "names.json",
                {"places_seed.jsonl": store.jsonl_chunks(places)},
                {"source": "names", "overture_release": "2026-08-19.0"},
                held=directory,
            )
    finally:
        directory.close()


def _write_crawl(cache, feed_id, stops_rows):
    feed_dir = cache / "crawl" / crawl._dir_name(feed_id)
    feed_dir.mkdir(parents=True, exist_ok=True)
    stops_text = "stop_id,stop_lat,stop_lon\n" + "".join(stops_rows)
    # Bytes, not text: the state digest is over exact bytes, and Windows
    # text mode would rewrite the newlines.
    (feed_dir / "stops.txt").write_bytes(stops_text.encode())
    (feed_dir / "state.json").write_text(
        json.dumps(
            {
                "feed_id": feed_id,
                "members": ["stops.txt"],
                "member_sha256": {
                    "stops.txt": hashlib.sha256(stops_text.encode()).hexdigest()
                },
            }
        )
    )
    log = {
        "feed_id": feed_id,
        "directory": crawl._dir_name(feed_id),
        "members": ["stops.txt"],
        "method": "download",
    }
    log_path = cache / "crawl" / "crawl_log.jsonl"
    existing = log_path.read_text() if log_path.is_file() else ""
    log_path.write_text(existing + json.dumps(log) + "\n")


def _expand(tmp_path, cache, registry=None, labels=None):
    divisions = fx.write_dataset(tmp_path / "divisions.parquet", DIVISIONS)
    areas = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    lookup = boundaries.BoundaryLookup(
        cache, release="2026-08-19.0", area_dataset=areas, division_dataset=divisions
    )
    wikidata = fx.StubWikidata(
        {},
        {"Q28515": [{"qid": "Q912579", "name": "Springfield MSA", "cbsa": "44100"}]},
        {
            "Q40840": {
                "labels": {"en": "Tampere", "fi": "Tampere"},
                "aliases": ["Manse"],
            },
            "Q912579": {
                "labels": {"fi": "Springfieldin metropolialue"},
                "aliases": ["Greater Springfield"],
            },
            **(labels or {}),
        },
    )
    try:
        manifest = expand.expand(
            cache,
            lookup=lookup,
            wikidata=wikidata,
            area_dataset=areas,
            registry=registry,
        )
    finally:
        lookup.close()
    places, _ = store.read_jsonl(
        cache / "gazetteer", "expanded.json", "places_expanded.jsonl"
    )
    report, _ = store.read_jsonl(
        cache / "gazetteer", "expanded.json", "expansion_report.jsonl"
    )
    return manifest, {p.get("wikidata_id") or p["place_id"]: p for p in places}, report


def test_no_crawl_artifacts_pass_the_seed_through(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    manifest, places, report = _expand(tmp_path, cache)
    assert manifest["mode"] == "declared"
    assert manifest["places_added"] == 0
    assert set(places) == {"Q33"}
    assert report == []


def test_a_crawled_stop_discovers_an_unseeded_city(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["mode"] == "expanded"
    tampere = places["Q40840"]
    assert tampere["kind"] == "city"
    # The unseeded intermediate region was added too, chained to the seed.
    region = places["Q5697"]
    assert region["kind"] == "region"
    assert region["parent_id"] == "Q33"
    assert tampere["parent_id"] == "Q5697"
    assert tampere["resolution_method"] == "overture_wikidata"
    # The boundary passed the licence audit and was simplified in — and it is
    # the COMPLETE area read, so the disjoint far component shipped too.
    assert tampere["geometry"]
    assert tampere["geometry_source"] == "overture"
    boundary = shapely.from_wkb(bytes.fromhex(tampere["geometry"]))
    assert boundary.covers(shapely.Point(22.0, 61.0))
    # Wikidata names merged.
    assert tampere["names"]["fi"] == "Tampere"
    assert tampere["aliases"] == ["Manse"]
    # The country was already seeded and is not duplicated or replaced.
    assert places["Q33"] is not tampere
    assert manifest["places_added"] == 2  # the city and its region


def test_an_already_seeded_place_is_not_re_added(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-country", ["s1,68.0,27.0\n"])  # only Finland covers it
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["places_added"] == 0
    assert set(places) == {"Q33"}


def test_a_qidless_division_is_reported_not_minted(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-noqid", ["s1,62.1,26.2\n"])
    manifest, places, report = _expand(tmp_path, cache)
    assert "Q" not in "".join(p for p in places if p != "Q33")
    assert any(r["overture_id"] == "fi-noqid" for r in report)
    assert manifest["reported"] >= 1


def test_a_discovered_us_city_gains_its_metro(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-spring", ["s1,39.8,-89.65\n"])
    manifest, places, _ = _expand(tmp_path, cache)
    city = places["Q28515"]
    metro = places["Q912579"]
    assert metro["kind"] == "metro"
    assert metro["statistical_area_id"] == "44100"
    assert city["metro_ids"] == ["Q912579"]
    assert metro["member_ids"] == ["Q28515"]
    assert manifest["metros_added"] == 1
    # The minted metro is enriched like every other new place: Wikidata labels
    # join for languages the row lacks (its own English label keeps precedence)
    # and aliases merge in.
    assert metro["names"]["fi"] == "Springfieldin metropolialue"
    assert "Greater Springfield" in metro["aliases"]


def test_an_unauditable_boundary_ships_without_geometry(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-badgeo", ["s1,62.1,28.2\n"])
    _, places, _ = _expand(tmp_path, cache)
    badgeo = places["Q999"]
    assert badgeo["geometry"] is None
    assert badgeo["geometry_source"] is None


def test_a_stops_file_that_fails_its_state_digest_is_skipped(tmp_path):
    # A crash can leave a member newer or older than state.json; mismatched
    # bytes must not become gazetteer evidence.
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    stops = cache / "crawl" / crawl._dir_name("f-tre") / "stops.txt"
    stops.write_text(stops.read_text() + "s2,61.5,23.81\n")
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["state_mismatches"] == 1
    assert manifest["places_added"] == 0
    assert set(places) == {"Q33"}


def test_a_corrupt_state_file_skips_only_that_feed(tmp_path):
    # A syntactically valid but non-object state is one feed's corruption;
    # it must be skipped, never abort the expansion.
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    _write_crawl(cache, "f-corrupt", ["s1,61.5,23.8\n"])
    state = cache / "crawl" / crawl._dir_name("f-corrupt") / "state.json"
    state.write_text("[1, 2]")
    log_path = cache / "crawl" / "crawl_log.jsonl"
    log_path.write_text(log_path.read_text() + '[]\n{"directory": 3}\n')
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["feeds_scanned"] == 1
    assert "Q40840" in places


def test_a_stops_member_the_parser_refuses_skips_only_that_feed(tmp_path):
    # A field over csv's parser limit raises mid-parse; that is one feed's
    # corruption, never the run's.
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    _write_crawl(cache, "f-huge", ['"' + "x" * 200000 + '",61.5,23.8\n'])
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["state_mismatches"] == 1
    assert "Q40840" in places


def test_unparsable_stop_rows_are_skipped(tmp_path):
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(
        cache,
        "f-tre",
        ["s1,not-a-lat,23.8\n", "s2,61.5,23.8\n", "s3,1e308,23.8\n", "s4,61.5,200\n"],
    )
    manifest, places, _ = _expand(tmp_path, cache)
    assert manifest["stops_read"] == 1
    assert "Q40840" in places


def test_places_yaml_edited_after_the_gazetteer_refuses_to_expand(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    cache = tmp_path / "cache"
    _publish_names(cache, [])
    directory = write_overrides(
        tmp_path, places=[{"place": "Q1", "set_aliases": ["x"]}]
    )
    with pytest.raises(overrides.OverrideError, match="re-run the gazetteer"):
        expand.expand(cache, overrides_dir=directory)


def test_a_crawled_city_whose_qid_is_a_curated_metro_is_a_collision(tmp_path):
    cache = tmp_path / "cache"
    taken = {**SEED_PLACES[0], "place_id": "Q40840", "kind": "metro", "curated": True}
    _publish_names(cache, SEED_PLACES + [taken])
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    with pytest.raises(overture.GazetteerError, match="is both the seeded metro"):
        _expand(tmp_path, cache)


HEADER = '{"next_id": 1, "registry": 1}\n'


def _crawled_cache(tmp_path):
    """Seed places plus crawled stops in Tampere and Springfield."""
    cache = tmp_path / "cache"
    _publish_names(cache, SEED_PLACES)
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    _write_crawl(cache, "f-us", ["s2,39.8,-89.65\n"])
    return cache


def _publish_run(cache, registry_digest):
    """A gazetteer run manifest naming no stage, for the registry chain."""
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "gazetteer",
                store.RUN_POINTER,
                {"generations.jsonl": store.jsonl_chunks([])},
                {"generations": {}, "registry_digest": registry_digest},
                held=directory,
            )
    finally:
        directory.close()


def _seeded_registry(path, *seeded):
    """The registry the seed left: a row per seeded place, saved."""
    from index_build import registry

    path.write_text(HEADER)
    rows = [("country", {"wikidata": ["Q33"], "overture": ["fi"]})]
    rows.extend(("city", concordances) for concordances in seeded)
    with registry.session(path) as reg:
        for kind, concordances in rows:
            reg.identify(concordances, kind=kind, minted_from="t", minted_in="t 1")
        reg.save()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_discoveries_are_identified_through_the_registry(tmp_path):
    from index_build import registry

    cache = _crawled_cache(tmp_path)
    path = tmp_path / "places_registry.jsonl"
    run_digest = _seeded_registry(path)
    _publish_run(cache, run_digest)
    with registry.session(path) as reg:
        manifest, places, _ = _expand(tmp_path, cache, registry=reg)
        # Tampere, Pirkanmaa, Springfield and its MSA minted; Finland, the
        # seeded place the stops also reach, found.
        assert reg.minted == manifest["minted"] == 4
        assert manifest["identified"] == 5
    assert manifest["registry_base"] == run_digest
    assert manifest["registry_digest"] == hashlib.sha256(path.read_bytes()).hexdigest()
    saved = registry.load(path)
    assert places["Q40840"]["place_id"] == saved.resolve("overture:fi-tre")
    assert places["Q912579"]["place_id"] == saved.resolve("cbsa:44100")
    # A rerun opens on the registry the first expand saved — the run's
    # successor in the chain — finds every place again and mints nothing,
    # and still anchors on the run's digest, so consumers and a further
    # expansion accept the chain.
    for _ in range(2):
        with registry.session(path) as again:
            manifest, _, _ = _expand(tmp_path, cache, registry=again)
            assert (again.minted, manifest["minted"]) == (0, 0)
        assert manifest["registry_base"] == run_digest
        assert (
            expand.expected_registry_digest(cache / "gazetteer", path)
            == hashlib.sha256(path.read_bytes()).hexdigest()
        )
    # A new gazetteer run on the registry expand saved: the stale expansion
    # blocks consumers until expand replaces it, anchored on the new run.
    new_run = hashlib.sha256(path.read_bytes()).hexdigest()
    _publish_run(cache, new_run)
    with pytest.raises(registry.RegistryError, match="rerun expand"):
        expand.expected_registry_digest(cache / "gazetteer", path)
    with registry.session(path) as after:
        manifest, _, _ = _expand(tmp_path, cache, registry=after)
    assert manifest["registry_base"] == new_run
    assert expand.expected_registry_digest(cache / "gazetteer", path) == new_run


def test_expand_refuses_a_read_only_mint_and_a_stale_registry(tmp_path):
    from index_build import registry

    cache = _crawled_cache(tmp_path)
    path = tmp_path / "places_registry.jsonl"
    path.write_text(HEADER)
    # Ids are minted only on a committed gazetteer run.
    with registry.session(path) as reg:
        with pytest.raises(overture.GazetteerError, match="no gazetteer run"):
            _expand(tmp_path, cache, registry=reg)
    _publish_run(cache, hashlib.sha256(path.read_bytes()).hexdigest())
    with registry.session(path, read_only=True) as reg:
        with pytest.raises(registry.RegistryError, match="read-only"):
            _expand(tmp_path, cache, registry=reg)
    assert store.current_generation(cache / "gazetteer", "expanded.json") is None
    # The gazetteer ran on another registry: expand must not build on it.
    _publish_run(cache, "0" * 64)
    with registry.session(path) as reg:
        with pytest.raises(overture.GazetteerError, match="rerun the gazetteer"):
            _expand(tmp_path, cache, registry=reg)


def test_a_seeded_place_reached_through_a_new_division_is_enriched(tmp_path):
    from index_build import registry

    cache = tmp_path / "cache"
    tampere = {
        "place_id": "Q40840",
        "kind": "city",
        "name": "Tampere",
        "country_code": "FI",
        "overture_id": "fi-tre-old",
        "metro_ids": [],
        "member_ids": [],
    }
    _publish_names(cache, SEED_PLACES + [tampere])
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    path = tmp_path / "places_registry.jsonl"
    _publish_run(
        cache,
        _seeded_registry(path, {"wikidata": ["Q40840"], "overture": ["fi-tre-old"]}),
    )
    # Read-only, the new concordance is refused; writable, it enriches the
    # seeded place's row while the seeded row itself stays.
    with registry.session(path, read_only=True) as reg:
        with pytest.raises(registry.RegistryError, match="read-only"):
            _expand(tmp_path, cache, registry=reg)
    with registry.session(path) as reg:
        manifest, places, _ = _expand(tmp_path, cache, registry=reg)
        assert (reg.minted, reg.enriched, manifest["enriched"]) == (1, 1, 1)
    saved = registry.load(path)
    assert saved.effective(saved.resolve("Q40840"))["overture"] == [
        "fi-tre-old",
        "fi-tre",
    ]
    assert places["Q40840"]["overture_id"] == "fi-tre-old"


def test_a_crash_after_the_registry_save_is_recovered_by_a_gazetteer_rerun(
    tmp_path, monkeypatch
):
    from index_build import registry

    cache = _crawled_cache(tmp_path)
    path = tmp_path / "places_registry.jsonl"
    path.write_text(HEADER)
    _publish_run(cache, hashlib.sha256(path.read_bytes()).hexdigest())
    real = store.publish

    def crash(directory, pointer, *args, **kwargs):
        if pointer == expand.EXPANDED_POINTER:
            raise RuntimeError("crash")
        return real(directory, pointer, *args, **kwargs)

    monkeypatch.setattr(store, "publish", crash)
    with registry.session(path) as reg:
        with pytest.raises(RuntimeError, match="crash"):
            _expand(tmp_path, cache, registry=reg)
    monkeypatch.undo()
    # The registry holds the mints, valid rows; expand cannot tell the file
    # from an edit until the gazetteer re-anchors on it, then reproduces.
    assert registry.load(path).resolve("overture:fi-tre")
    with registry.session(path) as reg:
        with pytest.raises(overture.GazetteerError, match="rerun the gazetteer"):
            _expand(tmp_path, cache, registry=reg)
    _publish_run(cache, hashlib.sha256(path.read_bytes()).hexdigest())
    with registry.session(path) as reg:
        manifest, _, _ = _expand(tmp_path, cache, registry=reg)
        assert manifest["minted"] == 0


def test_a_curated_metro_keyed_by_its_code_takes_the_discovered_qid(tmp_path):
    from index_build import registry

    cache = tmp_path / "cache"
    path = tmp_path / "places_registry.jsonl"
    _seeded_registry(path)
    with registry.session(path) as reg:
        curated = reg.identify(
            {"cbsa": ["44100"]}, kind="metro", minted_from="t", minted_in="t 1"
        )
        reg.save()
    _publish_names(
        cache,
        SEED_PLACES
        + [
            {
                "place_id": curated,
                "kind": "metro",
                "source_subtype": "metropolitan statistical area",
                "name": "Curated MSA",
                "country_code": "US",
                "statistical_area_id": "44100",
                "members_curated": True,
                "metro_ids": [],
                "member_ids": [],
            }
        ],
    )
    _write_crawl(cache, "f-us", ["s2,39.8,-89.65\n"])
    _publish_run(cache, hashlib.sha256(path.read_bytes()).hexdigest())
    with registry.session(path) as reg:
        manifest, places, _ = _expand(tmp_path, cache, registry=reg)
        assert (manifest["metros_added"], reg.enriched) == (0, 1)
    metro = places["Q912579"]
    assert metro["place_id"] == curated and metro["name"] == "Curated MSA"
    assert sum(1 for p in places.values() if p["kind"] == "metro") == 1


@pytest.mark.parametrize("seeded", [False, True], ids=["new", "survivor seeded"])
def test_a_discovered_alias_qid_is_its_survivor(tmp_path, seeded):
    from index_build import registry

    cache = tmp_path / "cache"
    survivor = {
        "place_id": "tp_2",
        "tp_id": "tp_2",
        "wikidata_id": "Q77777",
        "kind": "city",
        "name": "Tampere",
        "country_code": "FI",
        "overture_id": "fi-tre-old",
        "metro_ids": [],
        "member_ids": [],
    }
    _publish_names(cache, SEED_PLACES + ([survivor] if seeded else []))
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    _write_crawl(cache, "f-us", ["s2,39.8,-89.65\n"])
    path = tmp_path / "places_registry.jsonl"
    # Tampere's QID is a merged alias: the registry keys the place by Q77777.
    path.write_text(
        '{"next_id": 4, "registry": 1}\n'
        '{"place_id": "tp_1", "kind": "country", "concordances": {"wikidata": ["Q33"], '
        '"overture": ["fi"]}, "name": "Finland", "country_code": "FI", '
        '"minted_from": "t", "minted_in": "t 1"}\n'
        '{"place_id": "tp_2", "kind": "city", "concordances": {"wikidata": ["Q77777"]}, '
        '"name": "Tampere", "country_code": "FI", "minted_from": "t", "minted_in": "t 1"}\n'
        '{"place_id": "tp_3", "status": "merged", "into": "tp_2", '
        '"concordances": {"wikidata": ["Q40840"]}, "at": "2026-09-08", "reason": "dup"}\n'
    )
    _publish_run(cache, hashlib.sha256(path.read_bytes()).hexdigest())
    canonical = {"Q77777": {"labels": {"sv": "Tammerfors"}, "aliases": []}}
    with registry.session(path) as reg:
        manifest, places, _ = _expand(tmp_path, cache, registry=reg, labels=canonical)
        # The survivor's registry row takes the crawled division's Overture
        # id as a concordance; seeded, the row is not added again.
        assert reg.enriched == 1
    tampere = places["Q77777"]
    assert tampere["place_id"] == "tp_2"
    assert sum(1 for p in places.values() if p["place_id"] == "tp_2") == 1
    # Tampere (unless seeded), Pirkanmaa, Springfield and its MSA.
    assert manifest["places_added"] == (3 if seeded else 4)
    if seeded:
        assert tampere["overture_id"] == "fi-tre-old"
    else:
        assert tampere["names"]["sv"] == "Tammerfors"
        assert "Manse" not in tampere["aliases"]


def test_a_crawled_qid_for_a_place_known_by_its_division_joins_it(tmp_path):
    from index_build import registry

    # Tampere was seeded without a QID, identified by its Overture division;
    # the crawl resolves that division with a QID: the seeded row gains the
    # QID as a concordance and is not added again.
    cache = tmp_path / "cache"
    path = tmp_path / "places_registry.jsonl"
    _seeded_registry(path, {"overture": ["fi-tre"]})
    tampere = {
        "place_id": "tp_2",
        "tp_id": "tp_2",
        "wikidata_id": None,
        "kind": "city",
        "name": "Tampere",
        "country_code": "FI",
        "overture_id": "fi-tre",
        "metro_ids": [],
        "member_ids": [],
    }
    _publish_names(cache, SEED_PLACES + [tampere])
    _write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    _publish_run(cache, hashlib.sha256(path.read_bytes()).hexdigest())
    with registry.session(path) as reg:
        manifest, places, _ = _expand(tmp_path, cache, registry=reg)
        assert (manifest["places_added"], reg.minted, reg.enriched) == (1, 1, 1)
    assert sum(1 for p in places.values() if p.get("overture_id") == "fi-tre") == 1
    assert registry.load(path).resolve("Q40840") == "tp_2"
    # The published row carries the QID it gained, and its labels.
    assert (
        places["Q40840"]["place_id"] == "tp_2"
        and "Manse" in places["Q40840"]["aliases"]
    )
