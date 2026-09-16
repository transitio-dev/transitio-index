"""Tests for the Urban Audit input: pinned, read under its contract into the
shapes the Eurostat assignment consumes."""

import urllib.request

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("geopandas")
import shapely  # noqa: E402

import fao_fixture as ffx  # noqa: E402
from transitio_index import eurostat, geometry, urau  # noqa: E402


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """The stage can download its pinned inputs; a test never may."""

    def refuse(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)


HELSINKI = shapely.box(24.0, 60.0, 25.5, 60.9)
TAMPERE = shapely.box(23.0, 61.2, 24.5, 61.8)
COMO = shapely.box(8.9, 45.7, 9.2, 46.0)
ATHINA = shapely.box(23.5, 37.8, 24.0, 38.2)
AREAS = [
    ("FI001F", "F", "FI", "Helsinki", HELSINKI),
    ("FI002F", "F", "FI", "Tampere", TAMPERE),
    ("CB003F", "F", "CB", "Como/Chiasso", COMO, "CH070"),  # a cross-border part
    ("EL001F", "F", "EL", "Athina", ATHINA),  # Eurostat's prefix for Greece
]


def _inputs(tmp_path, areas=None, **options):
    return ffx.pinned_files(
        tmp_path, {urau.AREAS_FILE: areas or ffx.fua_zip(AREAS, **options)}
    )


def test_the_verified_generation_is_parsed(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path / "inputs")
    manifest = urau.prepare_inputs(cache, files=files, expected=expected)
    assert manifest["edition"] == urau.EDITION and manifest["digests"] == expected
    composition, boundaries, loaded = urau.load_inputs(cache, expected=expected)
    assert loaded["generation"] == manifest["generation"]
    # One region per area under its own code, the country as the gazetteer
    # knows it, the cross-border part's from its NUTS-3 region; the polygons
    # as drawn.
    assert composition == {
        "FI001F": {"name": "Helsinki", "country": "FI", "nuts3": ["FI001F"]},
        "FI002F": {"name": "Tampere", "country": "FI", "nuts3": ["FI002F"]},
        "CB003F": {"name": "Como/Chiasso", "country": "CH", "nuts3": ["CB003F"]},
        "EL001F": {"name": "Athina", "country": "GR", "nuts3": ["EL001F"]},
    }
    assert set(boundaries) == set(composition)
    assert boundaries["FI001F"].equals(HELSINKI)
    assert eurostat.countries(composition) == {"CH", "FI", "GR"}
    # Other pins than the generation carries are refused.
    with pytest.raises(urau.UrauError, match="other inputs"):
        urau.load_inputs(cache)


@pytest.mark.parametrize(
    ("areas", "options", "message"),
    [
        ([("FI001", "F", "FI", "Helsinki", HELSINKI)], {}, "area code 'FI001'"),
        (AREAS + [AREAS[0]], {}, "FI001F twice"),
        ([("FI001C", "C", "FI", "Helsinki", HELSINKI)], {}, "area code"),
        ([("FI001F", "C", "FI", "Helsinki", HELSINKI)], {}, "not a functional"),
        ([("FI001F", "F", "FI", "", HELSINKI)], {}, "has no name"),
        ([("FI001F", "F", "", "Helsinki", HELSINKI)], {}, "has no country"),
        ([("FI001F", "F", "SE", "Helsinki", HELSINKI)], {}, "filed under country 'SE'"),
        ([("CB003F", "F", "CB", "Como/Chiasso", COMO)], {}, "has no NUTS-3 region"),
        (
            [("CB003F", "F", "CB", "Como/Chiasso", COMO, "Ticino")],
            {},
            "in NUTS-3 region 'Ticino'",
        ),
        (
            [("FI001F", "F", "FI", "Helsinki", shapely.LineString([(0, 0), (1, 1)]))],
            {},
            "not a valid polygon",
        ),
        (AREAS, {"crs": "EPSG:3035"}, "CRS"),
        (AREAS, {"crs": None}, "no CRS"),
    ],
    ids=[
        "code-shape",
        "duplicate-code",
        "city-code",
        "city-category",
        "no-name",
        "no-country",
        "other-country",
        "no-nuts3",
        "bad-nuts3",
        "not-a-polygon",
        "other-crs",
        "no-crs",
    ],
)
def test_the_area_contract_is_enforced(areas, options, message):
    with pytest.raises(urau.UrauError, match=message):
        urau.read_areas(ffx.fua_zip(areas, **options))


def test_a_self_intersecting_area_is_repaired():
    bowtie = shapely.Polygon([(24.0, 60.0), (25.0, 61.0), (25.0, 60.0), (24.0, 61.0)])
    _, boundaries = urau.read_areas(ffx.fua_zip([("FI001F", "F", "FI", "X", bowtie)]))
    assert boundaries["FI001F"].is_valid and boundaries["FI001F"].area > 0


def test_the_urau_source_is_registered_and_approved():
    assert urau.DERIVED in geometry.DERIVED_SOURCES
    assert urau.DERIVED in geometry.DERIVED_SOURCE_ALLOWLIST
    assert "EuroGeographics" in geometry.DERIVED_SOURCES[urau.DERIVED]["credit"]
