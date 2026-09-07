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

from index_build import fao, store  # noqa: E402


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


def test_inputs_are_converted_once_and_loaded_under_the_contract(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    manifest = fao.prepare_inputs(cache, files=files, expected=expected)
    assert manifest["digests"] == expected and manifest["cutoff_hours"] == 1
    converted = fao.convert_patches(cache, expected=expected)
    assert converted["patches"] == 4
    assert converted["source_sha256"] == expected[fao.PATCHES_FILE]
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


def test_a_patches_file_without_a_crs_is_refused():
    with pytest.raises(fao.FaoError, match="no CRS"):
        fao._patches_table(ffx.patches_zip(PATCHES, crs=None))
