import hashlib
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

from index_build import geometry, store, ucdb  # noqa: E402


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """The stage can download its pinned inputs; a test never may."""

    def refuse(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)


BOWTIE = shapely.Polygon([(20, 0), (21, 1), (21, 0), (20, 1)])  # self-crossing
CENTRES = [
    (1, 4, shapely.box(0, 0, 1, 1)),  # inside Helsinki
    (2, 2, shapely.box(2, 0, 3, 1)),  # 0.6 in Espoo, 0.4 in Vantaa: ambiguous
    (3, 1, shapely.box(5, 5, 6, 6)),  # overlaps nothing
    (4, 2, shapely.box(8, 0, 9, 1)),  # a 0.05 sliver of Lahti: below MIN_SHARE
    (5, 3, shapely.box(12, 0, 13, 1)),  # inside a nameless UCDB centre
    (6, 2, shapely.box(20.05, 0.45, 20.15, 0.55)),  # inside the repaired bow-tie
]
UCDB = [
    (
        572,
        "Helsinki",
        "Helsinki;Helsingfors",
        "Finland",
        shapely.box(-0.5, -0.5, 1.5, 1.5),
    ),
    (573, "Espoo", "Espoo", "Finland", shapely.box(2, 0, 2.6, 1)),
    (574, "Vantaa", "Vantaa", "Finland", shapely.box(2.6, 0, 3, 1)),
    (575, "Lahti", "Lahti", "Finland", shapely.box(8.95, 0, 10, 1)),
    (576, "", "", "Finland", shapely.box(12, 0, 13, 1)),
    (577, "Bowtie", "Bowtie", None, BOWTIE),
]


def _inputs(tmp_path, centres=None, ucdb_rows=None):
    files = {
        ucdb.CENTRES_FILE: tmp_path / "centres.zip",
        ucdb.UCDB_FILE: tmp_path / "ucdb.zip",
    }
    files[ucdb.CENTRES_FILE].write_bytes(centres or ffx.centres_zip(CENTRES))
    files[ucdb.UCDB_FILE].write_bytes(
        ucdb_rows or ffx.ucdb_zip(UCDB, ucdb.UCDB_MEMBER, ucdb.UCDB_LAYER)
    )
    expected = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in files.items()
    }
    return files, expected


def test_centres_are_named_by_overlap_once_and_loaded(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    manifest = ucdb.prepare_inputs(cache, files=files, expected=expected)
    assert manifest["digests"] == expected and manifest["release"] == ucdb.RELEASE
    names, matched = ucdb.load_names(cache, expected=expected)
    assert names["1"] == {
        "centre_id": "1",
        "type": 4,
        "ucdb_id": 572,
        "name": "Helsinki",
        "names": ["Helsinki", "Helsingfors"],
        "country": "Finland",
        "share": 1.0,
        "ambiguous": False,
        "candidates": [{"ucdb_id": 572, "name": "Helsinki", "share": 1.0}],
    }
    # Two candidates with the runner-up within half of the best: named after
    # the best, flagged, both listed.
    assert names["2"]["name"] == "Espoo" and names["2"]["ambiguous"]
    assert [(c["ucdb_id"], c["share"]) for c in names["2"]["candidates"]] == [
        (573, 0.6),
        (574, 0.4),
    ]
    # No overlap, a sliver below MIN_SHARE, or only a nameless centre: unnamed.
    assert set(names) == {"1", "2", "6"}
    # The source's invalid polygon is repaired, not refused.
    assert names["6"]["name"] == "Bowtie" and names["6"]["country"] is None
    assert matched["sources"] == expected and matched["centres"] == 6
    assert (matched["named"], matched["ambiguous"], matched["nameless_ucdb"]) == (
        3,
        1,
        1,
    )
    assert matched["named_by_type"] == {"1": 0, "2": 2, "3": 0, "4": 1}
    # Matched once: the same sources are reused, not recomputed.
    assert (
        ucdb.match_centres(cache, expected=expected)["generation"]
        == matched["generation"]
    )
    # Names derived from other inputs are refused.
    directory = store.open_subdir(cache, "raw")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "raw",
                ucdb.NAMES_POINTER,
                {ucdb.NAMES_FILE: store.jsonl_chunks([])},
                {"sources": {}},
                held=directory,
            )
    finally:
        directory.close()
    with pytest.raises(ucdb.UcdbError, match="other inputs"):
        ucdb.load_names(cache, expected=expected)


HELSINKI = (572, "Helsinki", "Helsinki", "Finland", shapely.box(0, 0, 1, 1))


@pytest.mark.parametrize(
    ("centres", "ucdb_rows", "message"),
    [
        (
            [(1, 4, shapely.box(0, 0, 1, 1)), (1, 2, shapely.box(2, 0, 3, 1))],
            None,
            "not unique",
        ),
        ([(1, 7, shapely.box(0, 0, 1, 1))], None, "not a tier"),
        ([(1.5, 4, shapely.box(0, 0, 1, 1))], None, "integer ids"),
        ([(1, 4, shapely.Point(0, 0))], None, "not a polygon"),
        ("no crs", None, "no CRS"),
        (
            None,
            [HELSINKI, (572, "Espoo", "Espoo", "Finland", shapely.box(2, 0, 3, 1))],
            "not unique",
        ),
        (None, "no country column", "missing columns"),
        (
            None,
            [(572, "Helsinki", "Helsinki", "Finland", shapely.Polygon())],
            "missing or empty",
        ),
    ],
)
def test_the_input_contract_is_enforced(tmp_path, centres, ucdb_rows, message):
    if centres == "no crs":
        centres = ffx.centres_zip(CENTRES, crs=None)
    elif centres:
        centres = ffx.centres_zip(centres)
    if ucdb_rows == "no country column":
        columns = (ucdb.UCDB_ID, ucdb.UCDB_NAME, ucdb.UCDB_NAMES, "elsewhere")
        ucdb_rows = ffx.ucdb_zip(
            UCDB, ucdb.UCDB_MEMBER, ucdb.UCDB_LAYER, columns=columns
        )
    elif ucdb_rows:
        ucdb_rows = ffx.ucdb_zip(ucdb_rows, ucdb.UCDB_MEMBER, ucdb.UCDB_LAYER)
    files, expected = _inputs(tmp_path, centres, ucdb_rows)
    with pytest.raises(ucdb.UcdbError, match=message):
        ucdb.prepare_inputs(tmp_path / "cache", files=files, expected=expected)


def test_the_ucdb_source_is_registered_and_approved():
    assert ucdb.DERIVED in geometry.DERIVED_SOURCE_ALLOWLIST
    assert ucdb.DOI in geometry.DERIVED_SOURCES[ucdb.DERIVED]["credit"]
