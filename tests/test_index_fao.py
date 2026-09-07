import hashlib
import io
import sys
import urllib.request
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import fao_fixture as ffx  # noqa: E402
import overture_fixture as fx  # noqa: E402

from index_build import fao, overrides, store  # noqa: E402


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """The stage can download its pinned inputs; a test never may."""

    def refuse(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)


# Three patches: two under tier-2 centre 20 (each also its own tier-1 centre),
# one under tier-1 centre 30 alone.
PATCHES = [
    (1, (10, 20, 0, 0), shapely.box(0.0, 0.0, 1.0, 1.0)),
    (2, (11, 20, 0, 0), shapely.box(1.0, 0.0, 2.0, 1.0)),
    (3, (30, 0, 0, 0), shapely.box(5.0, 5.0, 6.0, 6.0)),
    (
        4,
        (40, 0, 0, 0),
        shapely.box(2.0, 0.0, 3.0, 1.0),
    ),  # east of patch 2, another region
]
REGIONS = [
    ("10", "FIN", 1, ["1"], "S"),
    ("11", "FIN", 1, ["2"], "S"),
    ("20", "FIN", 2, ["1", "2"], "P"),
    ("30", "SWE", 1, ["3"], "S"),
    ("40", "FIN", 1, ["4"], "S"),
]


def _city(place_id, country, overture_id, metro_ids=()):
    return {
        "place_id": place_id,
        "kind": "city",
        "country_code": country,
        "overture_id": overture_id,
        "metro_ids": list(metro_ids),
    }


PLACES = [
    _city("Q_A", "FI", "a"),  # eligible, patch 1
    _city("Q_B", "FI", "b"),  # eligible, patch 2: same tier-2 region as A
    _city("Q_C", "FI", "c", metro_ids=["Q_M"]),  # already in a metro: context
    _city("Q_D", "SE", "d"),  # an official assignment exists: context
    _city("Q_E", "SE", "e"),  # linked to a US MSA in the report: context
    _city("Q_F", "FI", "f"),  # outside every patch
    _city("Q_G", "FI", None),  # no land area: unplaceable
    _city("Q_H", "SE", "h"),  # eligible, patch 3
    _city("Q_I", "SE", "i"),  # an ambiguous official assignment: context
    _city("Q_J", "SE", "j"),  # explicitly unassigned officially: eligible
    _city(
        "Q_K", "FI", None, metro_ids=["Q_M"]
    ),  # no land area, not eligible: still listed
    _city("Q_L", "FI", "l"),  # astride patches 1 and 2, one region: placed
    _city(
        "Q_N", "FI", "n"
    ),  # astride patches 2 and 4, two regions, equal shares: ambiguous
    _city(
        "Q_O", None, "o"
    ),  # patch 3, country unknown: no pasteable country code there
    {"place_id": "Q_FI", "kind": "country", "country_code": "FI", "overture_id": "fi"},
]
AREAS = [
    fx.area(name, shapely.to_wkb(shapely.box(x, y, x + 0.2, y + 0.2)), [])
    for name, x, y in [
        ("a", 0.2, 0.2),
        ("b", 1.2, 0.2),
        ("c", 0.6, 0.6),
        ("d", 5.2, 5.2),
        ("e", 5.6, 5.2),
        ("f", 9.0, 9.0),
        ("h", 5.2, 5.6),
        ("i", 5.6, 5.6),
        ("j", 5.4, 5.4),
        ("l", 0.9, 0.4),
        ("n", 1.9, 0.4),
        ("o", 5.7, 5.7),
        ("fi", 0.0, 0.0),
    ]
]
ASSIGNMENTS = [
    {
        "city_id": "Q_D",
        "status": "assigned",
        "metro_code": "SE001M",
        "published": False,
    },
    {"city_id": "Q_I", "status": "ambiguous", "metro_code": None, "published": False},
    {"city_id": "Q_J", "status": "unassigned", "metro_code": None, "published": False},
]
METRO_REPORT = [
    {
        "branch": "us",
        "city_id": "Q_E",
        "metro_id": "Q_MSA",
        "reason": "US MSA without a CBSA code",
    }
]


def _inputs(tmp_path, patches=None, regions=None):
    files = {
        fao.PATCHES_FILE: tmp_path / "patches.zip",
        fao.REGIONS_FILE: tmp_path / "regions.csv",
    }
    files[fao.PATCHES_FILE].write_bytes(patches or ffx.patches_zip(PATCHES))
    files[fao.REGIONS_FILE].write_bytes(regions or ffx.regions_csv(REGIONS))
    expected = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in files.items()
    }
    return files, expected


def _metros_generation(cache, places, assignments=(), report=()):
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "gazetteer",
                "metros.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(places),
                    "metro_assignments.jsonl": store.jsonl_chunks(list(assignments)),
                    "metro_report.jsonl": store.jsonl_chunks(list(report)),
                },
                {"source": "metros"},
                held=directory,
            )
    finally:
        directory.close()


def test_inputs_are_converted_once_and_loaded_under_the_contract(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    manifest = fao.prepare_inputs(cache, files=files, expected=expected)
    assert manifest["digests"] == expected and manifest["cutoff_hours"] == 1
    converted = fao.convert_patches(cache, expected=expected)
    assert converted["patches"] == 4
    assert converted["sources"] == {fao.PATCHES_FILE: expected[fao.PATCHES_FILE]}
    # Converted once: the same source digest is reused, not re-read.
    assert (
        fao.convert_patches(cache, expected=expected)["generation"]
        == converted["generation"]
    )
    regions, patches, loaded = fao.load_inputs(cache, expected=expected)
    assert regions["20"] == {
        "tier": 2,
        "country": "FIN",
        "category": "P",
        "patches": ["1", "2"],
    }
    assert patches["1"]["centres"] == (10, 20, 0, 0) and patches["1"]["geom"].equals(
        PATCHES[0][2]
    )
    assert [fao.region_of(patches[p]) for p in ("1", "2", "3", "4")] == [
        "20",
        "20",
        "30",
        "40",
    ]
    # The cache is GeoParquet a standard reader recognises, in EPSG:4326.
    import geopandas

    converted_generation, _ = store.resolve(cache / "raw", fao.CONVERTED_POINTER)
    with converted_generation:
        frame = geopandas.read_parquet(
            io.BytesIO(converted_generation.read_bytes(fao.PATCHES_PARQUET))
        )
    assert frame.geometry.name == "geometry" and len(frame) == 4
    assert frame.crs is not None and frame.crs.to_epsg() == 4326
    # The cached conversion is hashed on every resolve: altered bytes are refused.
    generation_dir = converted_generation.path
    parquet_path = generation_dir / fao.PATCHES_PARQUET
    parquet_path.write_bytes(parquet_path.read_bytes() + b"tamper")
    with pytest.raises(store.StoreError, match="digest mismatch"):
        fao.load_inputs(cache, expected=expected)
    # A patches file in another CRS is reprojected to EPSG:4326.
    other = ffx.patches_zip(PATCHES, crs="EPSG:3857")
    table = fao._patches_table(other)
    assert shapely.from_wkb(table["geometry"][0].as_py()).bounds[2] < 0.01


@pytest.mark.parametrize(
    ("patches", "regions", "message"),
    [
        (
            None,
            [("10", "FIN", 1, ["1"], "S"), ("20", "FIN", 1, ["1", "2"], "P")],
            "tier 2 or above",
        ),
        (None, [("10", "FIN", 1, ["1"], "S")], "tier 2 or above"),
        (None, [("10", "FIN", 5, ["1"], "S")], "tier"),
        (None, [("10", "FIN", 1, ["1"], "S"), ("10", "FIN", 1, ["1"], "S")], "twice"),
        (None, [("10", "FIN", 1, ["x"], "S")], "patch ids"),
        (None, REGIONS + [("50", "FIN", 1, ["9"], "S")], "not in the patches file"),
        (
            None,
            [
                ("10", "FIN", 1, ["2"], "S"),
                ("11", "FIN", 1, ["1"], "S"),
                ("20", "FIN", 2, ["1", "2"], "P"),
                ("30", "SWE", 1, ["3"], "S"),
                ("40", "FIN", 1, ["4"], "S"),
            ],
            "does not list it",
        ),
        (
            None,
            [
                ("10", "FIN", 1, ["1"], "P"),
                ("11", "FIN", 1, ["2"], "S"),
                ("20", "FIN", 2, ["1", "2"], "S"),
                ("30", "SWE", 1, ["3"], "S"),
                ("40", "FIN", 1, ["4"], "S"),
            ],
            "primary region",
        ),
        (
            [
                (1, (10, 20, 0, 0), shapely.box(0, 0, 1, 1)),
                (1, (11, 20, 0, 0), shapely.box(1, 0, 2, 1)),
            ],
            None,
            "not unique",
        ),
        ([(1.5, (10, 20, 0, 0), shapely.box(0, 0, 1, 1))], None, "integer ids"),
        ([(1, (10, 20, 0, 0), shapely.Point(0.5, 0.5))], None, "not a polygon"),
        (None, [("10", "FIN", 1, ["1"], "X")], "category"),
    ],
)
def test_the_input_contract_is_enforced(tmp_path, patches, regions, message):
    cache = tmp_path / "cache"
    files, expected = _inputs(
        tmp_path,
        patches=ffx.patches_zip(patches) if patches else None,
        regions=ffx.regions_csv(regions) if regions else None,
    )
    # Preparation converts the patches, so a patches-file refusal fires there.
    with pytest.raises(fao.FaoError, match=message):
        fao.prepare_inputs(cache, files=files, expected=expected)
        fao.load_inputs(cache, expected=expected)


@pytest.mark.parametrize("dtype", ["float64", "float32", "longdouble"])
def test_float_ids_are_exact_only_within_their_width_and_int64(dtype):
    import numpy
    import pandas

    # The last exactly representable integer of the width, capped at int64.
    bound = min(2 ** (numpy.finfo(dtype).nmant + 1), 2**63)
    frame = pandas.DataFrame({"id": numpy.array([bound - 1], dtype=dtype)})
    assert fao.integer_ids(frame, "id", "ids").tolist() == [bound - 1]
    frame = pandas.DataFrame({"id": numpy.array([bound], dtype=dtype)})
    with pytest.raises(fao.FaoError, match="integer ids"):
        fao.integer_ids(frame, "id", "ids")


def test_a_patches_file_without_a_crs_is_refused():
    with pytest.raises(fao.FaoError, match="no CRS"):
        fao._patches_table(ffx.patches_zip(PATCHES, crs=None))


def test_the_stage_reports_eligible_cities_by_highest_tier_region(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    fao.prepare_inputs(cache, files=files, expected=expected)
    _metros_generation(cache, PLACES, ASSIGNMENTS, METRO_REPORT)
    areas = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    manifest = fao.suggest_metros(cache, dataset=areas, pins=expected)
    entries, _ = store.read_jsonl(
        cache / "gazetteer", "fao.json", "suggested_metros_report.jsonl"
    )
    digest = overrides.canonical_digest
    provenance = {
        "doi": fao.DOI,
        "cutoff_hours": 1,
        "license": fao.LICENCE,
        "credit": fao.CREDIT,
        "digests": expected,
    }
    assert entries == [
        {
            **provenance,
            "region_id": "20",
            "tier": 2,
            "category": "P",
            "country": "FIN",
            "cities": ["Q_A", "Q_B", "Q_L"],
            "context": ["Q_C"],
            "evidence_hash": digest(["Q_A", "Q_B", "Q_L"]),
            "override": [
                {
                    "place": "<QID>",
                    "add_place": {
                        "kind": "metro",
                        "name": "<name>",
                        "country_code": "FI",
                    },
                },
                {
                    "place": "<QID>",
                    "set_statistical_area": {"scheme": "fao_city_region", "code": "20"},
                    "evidence_hash": digest(["Q_A", "Q_B", "Q_L"]),
                },
            ],
        },
        {
            **provenance,
            "region_id": "30",
            "tier": 1,
            "category": "S",
            "country": "SWE",
            "cities": ["Q_H", "Q_J", "Q_O"],
            "context": ["Q_D", "Q_E", "Q_I"],
            "evidence_hash": digest(["Q_H", "Q_J", "Q_O"]),
            "override": [
                {"place": "<QID>", "add_place": {"kind": "metro", "name": "<name>"}},
                {
                    "place": "<QID>",
                    "set_statistical_area": {"scheme": "fao_city_region", "code": "30"},
                    "evidence_hash": digest(["Q_H", "Q_J", "Q_O"]),
                },
            ],
        },
    ]
    unplaced, _ = store.read_jsonl(cache / "gazetteer", "fao.json", "unplaced.jsonl")
    assert unplaced == [
        {"city_id": "Q_G", "eligible": True, "reason": "no usable land area"},
        {"city_id": "Q_K", "eligible": False, "reason": "no usable land area"},
        {"city_id": "Q_N", "eligible": True, "reason": "on a boundary between regions"},
    ]
    assert manifest["entries"] == 2 and manifest["eligible_cities"] == 6
    assert manifest["tiers"] == {1: 1, 2: 1}
    assert manifest["unplaced"] == 3 and manifest["doi"] == fao.DOI
    # The places are untouched: nothing is minted.
    places, _ = store.read_jsonl(
        cache / "gazetteer", "metros.json", "places_seed.jsonl"
    )
    assert [p["place_id"] for p in places] == [p["place_id"] for p in PLACES]
