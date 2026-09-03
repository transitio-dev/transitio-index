import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402

from index_build import geometry, overture, store  # noqa: E402


def _place(place_id, kind, *, overture_id=None, country="FI"):
    return {
        "place_id": place_id,
        "kind": kind,
        "source_subtype": kind,
        "name": place_id,
        "names": {},
        "resolution_method": "overture_wikidata",
        "parent_id": None,
        "country_code": country,
        "overture_id": overture_id,
        "osm_relation_id": None,
        "statistical_area_id": None,
        "metro_ids": [],
        "member_ids": [],
    }


PLACES = [
    _place("Q1757", "city", overture_id="fi-helsinki"),
    _place("Q1508", "region", overture_id="fi-uusimaa"),
    _place("Q_METRO", "metro"),  # no overture_id, no geometry here
    _place("Q_NOGEO", "city", overture_id="no-area"),  # no division_area row
    _place("Q_DENIED", "city", overture_id="denied"),  # non-allowlisted licence
    _place("Q_MIXED", "city", overture_id="mixed"),  # one allowed + one unlicensed
    _place("Q_TWOLAND", "city", overture_id="twoland"),  # two allowed land areas
    _place("Q_MULTI", "city", overture_id="multi"),  # one allowed + one unlicensed area
    _place("Q_EMPTY", "city", overture_id="empty"),  # allowed source, empty geometry
    _place("Q_BADWKB", "city", overture_id="badwkb"),  # malformed WKB bytes
    _place("Q_NOSRC", "city", overture_id="nosrc"),  # area with no sources at all
]

BOX = shapely.to_wkb(shapely.box(24.9, 60.1, 25.1, 60.3))
OTHER = shapely.to_wkb(shapely.box(23.0, 60.0, 24.0, 61.0))


def _osm(license="ODbL-1.0"):
    return {"dataset": "OpenStreetMap", "license": license, "record_id": "relation/1"}


AREAS = [
    fx.area("fi-helsinki", BOX, [_osm()]),
    # A second, maritime area for the same division must be ignored.
    fx.area("fi-helsinki", OTHER, [_osm()], is_land=False),
    fx.area(
        "fi-uusimaa",
        BOX,
        [{"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "X"}],
    ),
    fx.area(
        "denied",
        BOX,
        [
            {
                "dataset": "Proprietary",
                "license": "All-Rights-Reserved",
                "record_id": "Z",
            }
        ],
    ),
    fx.area(
        "mixed",
        BOX,
        [_osm(), {"dataset": "Unknown", "license": None, "record_id": "U"}],
    ),
    # Two allowlisted land areas of one division are unioned.
    fx.area("twoland", BOX, [_osm()]),
    fx.area("twoland", OTHER, [_osm()]),
    # Two land areas, one allowlisted and one unlicensed: the whole is omitted.
    fx.area("multi", BOX, [_osm()]),
    fx.area(
        "multi", OTHER, [{"dataset": "Unknown", "license": None, "record_id": "U2"}]
    ),
    # An allowlisted source but an empty polygon: rejected as invalid geometry.
    fx.area("empty", shapely.to_wkb(shapely.Polygon()), [_osm()]),
    # Malformed WKB must be skipped, not abort the stage.
    fx.area("badwkb", b"\x00 not real wkb", [_osm()]),
    # A land area with no sources at all: omitted, but recorded in the inventory.
    fx.area("nosrc", BOX, []),
]


def _publish(cache, records, overrides_dir=None):
    from index_build import overrides

    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "gazetteer",
                "metros.json",
                {"places_seed.jsonl": store.jsonl_chunks(records)},
                {
                    "source": "metros",
                    "places_overrides_sha256": overrides.places_digest(overrides_dir),
                },
                held=directory,
            )
    finally:
        directory.close()


def _run(tmp_path, overrides_dir=None):
    cache = tmp_path / "cache"
    _publish(cache, PLACES, overrides_dir)
    dataset = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    manifest = geometry.attach_geometry(
        cache, dataset=dataset, overrides_dir=overrides_dir
    )
    places, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "places_seed.jsonl"
    )
    return manifest, {p["place_id"]: p for p in places}, cache


def _read_text(cache, artifact):
    generation, _ = store.resolve(cache / "gazetteer", "geometry.json")
    with generation:
        return generation.read_bytes(artifact).decode("utf-8")


def test_geometry_is_attached_where_the_licence_allows(tmp_path):
    manifest, places, _ = _run(tmp_path)
    helsinki = places["Q1757"]
    assert helsinki["geometry_source"] == "overture"
    # Round-trips to a valid polygon.
    assert shapely.from_wkb(bytes.fromhex(helsinki["geometry"])).area > 0
    assert places["Q1508"]["geometry"] is not None
    assert manifest["with_geometry"] == 3


def test_geometry_from_a_disallowed_licence_is_omitted(tmp_path):
    manifest, places, _ = _run(tmp_path)
    assert places["Q_DENIED"]["geometry"] is None
    assert places["Q_DENIED"]["geometry_source"] is None
    # Q_DENIED, Q_MIXED, Q_MULTI and Q_NOSRC (no sources) are all licence-omitted.
    assert manifest["omitted_by_licence"] == 4


def test_geometry_with_any_unlicensed_source_is_omitted(tmp_path):
    # One allowlisted source is not enough: a same-geometry source with no
    # licence leaves the boundary unshippable.
    _, places, _ = _run(tmp_path)
    assert places["Q_MIXED"]["geometry"] is None


def test_an_unlicensed_land_area_omits_the_whole_division(tmp_path):
    # Sources are audited per land-area row, not flattened: one unlicensed area
    # makes the division unshippable even though another area is allowlisted.
    _, places, _ = _run(tmp_path)
    assert places["Q_MULTI"]["geometry"] is None


def test_allowlisted_land_areas_are_unioned(tmp_path):
    _, places, _ = _run(tmp_path)
    shipped = shapely.from_wkb(bytes.fromhex(places["Q_TWOLAND"]["geometry"]))
    # The two disjoint 0.04 and 1.0 boxes union to 1.04.
    assert shipped.area == pytest.approx(1.04, abs=1e-6)


def test_an_empty_or_invalid_geometry_is_rejected(tmp_path):
    manifest, places, _ = _run(tmp_path)
    assert places["Q_EMPTY"]["geometry"] is None
    assert places["Q_EMPTY"]["geometry_source"] is None
    # Empty polygon and malformed WKB are both rejected without aborting.
    assert places["Q_BADWKB"]["geometry"] is None
    assert manifest["invalid_geometry"] == 2


def test_a_null_source_is_denied_not_crashing():
    assert geometry._source_key(None) == (None, None)
    assert geometry._is_shippable([None]) is False
    osm = {"dataset": "OpenStreetMap", "license": "ODbL-1.0"}
    assert geometry._is_shippable([osm, None]) is False


def test_a_metro_and_an_arealess_place_have_no_geometry(tmp_path):
    _, places, _ = _run(tmp_path)
    assert places["Q_METRO"]["geometry"] is None
    assert places["Q_NOGEO"]["geometry"] is None


def test_the_notice_credits_sources_and_records_share_alike(tmp_path):
    _, _, cache = _run(tmp_path)
    notice = _read_text(cache, "NOTICE")
    assert "OpenStreetMap contributors" in notice
    assert "ODbL 1.0" in notice
    assert "geoBoundaries" in notice
    assert "CC BY 4.0" in notice
    assert overture.OVERTURE_RELEASE in notice  # the pinned release
    # ODbL's share-alike obligation is stated, not just the attribution.
    assert "share-alike" in notice
    assert "Derived Database" in notice
    # The disallowed source is never credited.
    assert "Proprietary" not in notice


def test_the_licence_inventory_records_sources_and_the_aggregator(tmp_path):
    _, _, cache = _run(tmp_path)
    inventory, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "licence_inventory.jsonl"
    )
    by_dataset = {row["dataset"]: row for row in inventory}
    # Overture itself is recorded as the aggregator, with a URL and version.
    aggregator = by_dataset["Overture Maps divisions"]
    assert aggregator["role"] == "aggregator"
    assert aggregator["version"] == overture.OVERTURE_RELEASE
    assert aggregator["url"]
    # Component sources carry allow status, a URL for allowed ones, and a version.
    osm = by_dataset["OpenStreetMap"]
    assert osm["allowed"] is True and osm["url"] and osm["version"]
    assert by_dataset["Proprietary"]["allowed"] is False
    # The sourceless area is recorded (null dataset) rather than dropped silently.
    assert any(row["dataset"] is None and row["allowed"] is False for row in inventory)


def test_only_the_land_area_is_used(tmp_path):
    # fi-helsinki has a land box and a larger maritime box; the shipped geometry
    # must come from the land area, not their union.
    _, places, _ = _run(tmp_path)
    shipped = shapely.from_wkb(bytes.fromhex(places["Q1757"]["geometry"]))
    land = shapely.box(24.9, 60.1, 25.1, 60.3)
    assert shipped.difference(land).area == pytest.approx(0.0, abs=1e-9)


def test_set_boundary_attaches_a_curated_polygon(tmp_path):
    from test_index_place_overrides import write_overrides

    from index_build import overrides

    wkt = "POLYGON((24.5 60.0, 25.5 60.0, 25.5 60.6, 24.5 60.6, 24.5 60.0))"
    manifest, places, _ = _run(
        tmp_path,
        overrides_dir=write_overrides(
            tmp_path, places=[{"place": "Q_METRO", "set_boundary": wkt}]
        ),
    )
    metro = places["Q_METRO"]
    assert metro["geometry_source"] == "curated"
    assert shapely.from_wkb(bytes.fromhex(metro["geometry"])).area > 0
    assert manifest["curated_geometry"] == 1 and manifest["stale_overrides"] == 0
    with pytest.raises(overrides.OverrideError, match="valid"):
        _run(
            tmp_path / "bad",
            overrides_dir=write_overrides(
                tmp_path / "bad",
                places=[
                    {"place": "Q_METRO", "set_boundary": "POLYGON((0 0, 1 1, 0 0))"}
                ],
            ),
        )
    projected = (
        "POLYGON((2700000 8400000, 2800000 8400000, 2800000 8500000, 2700000 8400000))"
    )
    with pytest.raises(overrides.OverrideError, match="WGS84"):
        _run(
            tmp_path / "far",
            overrides_dir=write_overrides(
                tmp_path / "far",
                places=[{"place": "Q_METRO", "set_boundary": projected}],
            ),
        )


def test_a_curated_places_boundary_is_attached_without_a_second_judgement(tmp_path):
    from test_index_place_overrides import write_overrides

    wkt = "POLYGON((24.5 60.0, 25.5 60.0, 25.5 60.6, 24.5 60.6, 24.5 60.0))"
    fixed = "POLYGON((24.6 60.1, 25.4 60.1, 25.4 60.5, 24.6 60.5, 24.6 60.1))"
    entries = [
        {
            "place": "Q900001",
            "add_place": {"kind": "country", "name": "New", "boundary": wkt},
            "evidence_hash": "0" * 64,
        },
        {"place": "Q900001", "set_boundary": fixed},
    ]
    directory = write_overrides(tmp_path, places=entries)
    cache = tmp_path / "cache"
    seeded = PLACES + [
        {**_place("Q900001", "country"), "curated": True, "boundary_wkt": wkt}
    ]
    _publish(cache, seeded, directory)
    dataset = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    manifest = geometry.attach_geometry(cache, dataset=dataset, overrides_dir=directory)
    places, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "places_seed.jsonl"
    )
    new = next(p for p in places if p["place_id"] == "Q900001")
    assert new["geometry_source"] == "curated" and "boundary_wkt" not in new
    # The explicit set_boundary correction wins over the add_place boundary.
    assert new["geometry"] == shapely.to_wkb(shapely.from_wkt(fixed)).hex()
    assert manifest["curated_geometry"] == 2 and manifest["stale_overrides"] == 0
