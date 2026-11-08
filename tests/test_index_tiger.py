import pytest

pytest.importorskip("pyarrow")
shapely = pytest.importorskip("shapely")

import fao_fixture as ffx  # noqa: E402
from transitio_index import geometry, tiger  # noqa: E402

PLACES = [
    ("1230000", "Hialeah", "FL", shapely.box(-80.35, 25.83, -80.25, 25.90)),
    ("4835000", "Houston", "TX", shapely.box(-95.8, 29.5, -95.0, 30.1)),
    ("2965000", "St. Louis", "MO", shapely.box(-90.35, 38.53, -90.17, 38.77)),
    ("1770850", "Springfield", "IL", shapely.box(-89.8, 39.7, -89.5, 39.9)),
    ("1770851", "Springfield", "IL", shapely.box(-89.7, 39.75, -89.4, 39.95)),
]


def _prepared(tmp_path, rows):
    files, expected = ffx.pinned_files(
        tmp_path / "in", {tiger.PLACES_FILE: ffx.places_zip(rows)}
    )
    cache = tmp_path / "cache"
    tiger.prepare_inputs(cache, files=files, expected=expected)
    return cache, expected


def test_a_city_takes_the_one_census_place_of_its_name_holding_its_point(tmp_path):
    cache, expected = _prepared(tmp_path, PLACES)
    places, manifest = tiger.load_places(cache, expected=expected)
    assert manifest["source"] == "tiger" and len(places) == 5
    assert places[0]["state"] == "FL" and places[0]["geometry"].is_valid
    cities = [
        ("hialeah", "Hialeah", -80.30, 25.86),
        # A neighbourhood's point inside the city it belongs to.
        ("atascocita", "Atascocita", -95.2, 29.99),
        # Two places of the name hold the point.
        ("springfield", "Springfield", -89.6, 39.8),
        ("saint-louis", "Saint Louis", -90.2, 38.6),
        ("on-the-edge", "Hialeah", -80.35, 25.86),
        ("nowhere", "Hialeah", -70.0, 40.0),
    ]
    found = tiger.match_outlines(places, cities)
    assert {city: place["geoid"] for city, place in found.items()} == {
        "hialeah": "1230000",
        "saint-louis": "2965000",
        "on-the-edge": "1230000",
    }
    assert tiger.DERIVED in geometry.DERIVED_SOURCE_ALLOWLIST
    assert "Census" in geometry.DERIVED_SOURCES[tiger.DERIVED]["credit"]


def test_a_place_repaired_into_a_collection_keeps_its_polygon(tmp_path):
    # The spike to (1, 3) makes the polygon invalid; its repair holds the
    # square and a line, and the line is dropped.
    spiked = shapely.Polygon([(0, 0), (2, 0), (2, 2), (1, 2), (1, 3), (1, 2), (0, 2)])
    cache, expected = _prepared(tmp_path, [("0100001", "Spike", "AL", spiked)])
    (place,), _ = tiger.load_places(cache, expected=expected)
    assert place["geometry"].geom_type == "Polygon" and place["geometry"].is_valid
    assert place["geometry"].area == pytest.approx(4.0)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([PLACES[0], ("1230000", "Hialeah", "FL", shapely.box(0, 0, 1, 1))], "GEOIDs"),
        ([("1230000", "Hialeah", "FL", shapely.Point(0, 0))], "not a polygon"),
    ],
)
def test_the_places_file_contract_is_enforced(tmp_path, rows, message):
    cache, expected = _prepared(tmp_path, rows)
    with pytest.raises(tiger.TigerError, match=message):
        tiger.load_places(cache, expected=expected)


def test_names_compare_without_case_accents_or_saint_spelling():
    keys = {tiger.name_key(n) for n in ("St. Louis", "Saint Louis", "st louis")}
    assert keys == {"saintlouis"}
    assert tiger.name_key("Town 'n' Country") == tiger.name_key("Town n Country")
    assert tiger.name_key("Cañon City") == tiger.name_key("Canon City")
