import hashlib
import io
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import eurostat  # noqa: E402

PAYLOADS = {
    eurostat.COMPOSITION_FILE: b"PK\x03\x04 a workbook",
    eurostat.BOUNDARIES_FILE: b"PAR1 a parquet file",
}


def _inputs(tmp_path, payloads=PAYLOADS):
    tmp_path.mkdir(exist_ok=True)
    files = {}
    for name, data in payloads.items():
        files[name] = tmp_path / name
        files[name].write_bytes(data)
    expected = {
        name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()
    }
    return files, expected


def test_inputs_are_pinned_published_and_reused(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    manifest = eurostat.prepare_inputs(cache, files=files, expected=expected)
    assert manifest["digests"] == expected
    assert manifest["urls"] == {name: None for name in expected}
    assert manifest["nuts_version"] == eurostat.NUTS_VERSION
    # Published under the same digests: reused, not republished.
    again = eurostat.prepare_inputs(cache, files=files, expected=expected)
    assert again["generation"] == manifest["generation"]
    generation, resolved = eurostat.resolve_inputs(cache, expected=expected)
    with generation:
        assert resolved["generation"] == manifest["generation"]
        assert (
            generation.read_bytes(eurostat.COMPOSITION_FILE)
            == PAYLOADS[eurostat.COMPOSITION_FILE]
        )
    # Other pins than the generation carries: refused, whether they are this
    # module's real pins or a mismatching local file.
    with pytest.raises(eurostat.EurostatError, match="other inputs"):
        eurostat.resolve_inputs(cache)
    wrong = {**expected, eurostat.COMPOSITION_FILE: "0" * 64}
    with pytest.raises(eurostat.EurostatError, match="does not match the pinned"):
        eurostat.prepare_inputs(cache, files=files, expected=wrong)
    with pytest.raises(eurostat.EurostatError, match="no pinned URL"):
        eurostat.prepare_inputs(cache, files=files, expected={"other.bin": "0" * 64})


def test_a_generation_with_other_digests_is_republished(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    first = eurostat.prepare_inputs(cache, files=files, expected=expected)
    newer = {**PAYLOADS, eurostat.COMPOSITION_FILE: b"PK\x03\x04 a reissued workbook"}
    files, expected = _inputs(tmp_path / "newer", newer)
    second = eurostat.prepare_inputs(cache, files=files, expected=expected)
    assert second["generation"] != first["generation"]
    assert second["digests"] == expected


class _Response:
    """A minimal ``urlopen`` result: a body with a Content-Length header."""

    def __init__(self, body):
        self._body = io.BytesIO(body)
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self, size=-1):
        return self._body.read(size)


def test_downloaded_inputs_are_verified_against_the_pins(tmp_path, monkeypatch):
    monkeypatch.setattr(
        eurostat.csv_source.urllib.request,
        "urlopen",
        lambda url, timeout=None: _Response(PAYLOADS[url.rsplit("/", 1)[-1]]),
    )
    cache = tmp_path / "cache"
    expected = {
        name: hashlib.sha256(data).hexdigest() for name, data in PAYLOADS.items()
    }
    # A download that does not match its pin is refused and leaves nothing
    # cached: no input file, no pointer.
    wrong = {**expected, eurostat.BOUNDARIES_FILE: "0" * 64}
    with pytest.raises(eurostat.EurostatError, match="does not match the pinned"):
        eurostat.prepare_inputs(cache, expected=wrong)
    assert [
        p.name for p in (cache / "raw").iterdir() if not p.name.startswith(".")
    ] == []
    manifest = eurostat.prepare_inputs(cache, expected=expected)
    assert manifest["urls"] == eurostat.URLS
    generation, _ = eurostat.resolve_inputs(cache, expected=expected)
    with generation:
        assert (
            generation.read_bytes(eurostat.BOUNDARIES_FILE)
            == PAYLOADS[eurostat.BOUNDARIES_FILE]
        )


# ---- readers and derivation ----

import shapely  # noqa: E402

from eurostat_fixture import parquet, workbook  # noqa: E402

WEST = shapely.box(24.0, 60.0, 25.0, 61.0)  # FI1B1, in the Helsinki metro
EAST = shapely.box(25.0, 60.0, 26.0, 61.0)  # FI1C1, in the Helsinki metro
NORTH = shapely.box(24.0, 64.0, 26.0, 66.0)  # FI1D1, in no metro
SOUTH = shapely.box(24.0, 59.0, 26.0, 60.0)  # FI1E1, the Tampere metro
FAR = shapely.box(10.0, 50.0, 11.0, 51.0)  # DE300, an uncovered country
ATTICA = shapely.box(23.0, 37.5, 24.0, 38.5)  # EL301, prefix EL = GR
LONDON = shapely.box(-1.0, 51.0, 1.0, 52.0)  # UKI31, prefix UK = GB

COMPOSITION = [
    ("FI1B1", "Y", "FI001MC", "Helsinki"),
    ("FI1C1", "Y", "FI001MC", "Helsinki"),
    ("FI1D1", "N", None, None),
    ("FI1E1", "Y", "FI002M", "Tampere"),
    ("DE300", "N", None, None),
    ("EL301", "Y", "EL001MC", "Athina"),
    ("UKI31", "Y", "UK001MC", "London"),
]
BOUNDARIES = [
    ("FI1B1", WEST),
    ("FI1C1", EAST),
    ("FI1D1", NORTH),
    ("FI1E1", SOUTH),
    ("DE300", FAR),
    ("EL301", ATTICA),
    ("UKI31", LONDON),
]
METROS = {
    "FI001MC": {"name": "Helsinki", "country": "FI", "nuts3": ["FI1B1", "FI1C1"]},
    "FI002M": {"name": "Tampere", "country": "FI", "nuts3": ["FI1E1"]},
    "EL001MC": {"name": "Athina", "country": "GR", "nuts3": ["EL301"]},
    "UK001MC": {"name": "London", "country": "GB", "nuts3": ["UKI31"]},
}


def _fixtures(tmp_path, composition=None, boundaries=None):
    return _inputs(
        tmp_path,
        {
            eurostat.COMPOSITION_FILE: composition or workbook(COMPOSITION),
            eurostat.BOUNDARIES_FILE: boundaries or parquet(BOUNDARIES),
        },
    )


def test_the_verified_generation_is_parsed(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _fixtures(tmp_path)
    eurostat.prepare_inputs(cache, files=files, expected=expected)
    metros, boundaries, manifest = eurostat.load_inputs(cache, expected=expected)
    assert metros == METROS
    assert set(boundaries) == {nuts_id for nuts_id, _ in BOUNDARIES}
    assert boundaries["FI1B1"].equals(WEST)
    assert manifest["nuts_version"] == eurostat.NUTS_VERSION
    assert eurostat.countries(metros) == {"FI", "GR", "GB"}


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"rows": COMPOSITION, "sheet": "Other"}, "sheet"),
        ({"rows": COMPOSITION, "header": ("NUTS", "Flag", "Code", "Label")}, "header"),
        (
            {"rows": [("FI1B1", "Y", "FI001MC", "H"), ("FI1B1", "Y", "FI001MC", "H")]},
            "twice",
        ),
        ({"rows": [("FI1B1", "Y", None, "H")]}, "flag"),
        ({"rows": [("FI1B1", "Y", "FI001MC", None)]}, "no label"),
        ({"rows": [("FI1B1", "N", "FI001MC", "H")]}, "flag"),
        ({"rows": [("FI1B1", "Y", "FI1M", "H")]}, "metro code"),
        (
            {"rows": [("FI1B1", "Y", "FI001M", "H"), ("FI1C1", "Y", "FI001M", "X")]},
            "labelled",
        ),
        ({"rows": [("FI1B1", "N", None, None)]}, "no metropolitan"),
    ],
)
def test_the_composition_contract_is_enforced(kwargs, message):
    with pytest.raises(eurostat.EurostatError, match=message):
        eurostat.read_composition(workbook(**kwargs))


@pytest.mark.parametrize(
    ("regions", "level", "message"),
    [
        (BOUNDARIES, 2, "not a NUTS-3"),
        ([("FI1B1", WEST), ("FI1B1", EAST)], 3, "twice"),
        ([("FI1B1", b"\x01\x02")], 3, "geometry"),
        ([("FI1B1", shapely.Point(24.5, 60.5))], 3, "valid polygon"),
        ([("FI1B1", shapely.Polygon())], 3, "valid polygon"),
    ],
)
def test_the_boundary_contract_is_enforced(regions, level, message):
    with pytest.raises(eurostat.EurostatError, match=message):
        eurostat.read_boundaries(parquet(regions, level))


@pytest.mark.parametrize(
    ("regions", "message"),
    [
        (
            BOUNDARIES[:1],
            r"6 sheet codes without a boundary \['DE300', 'EL301', 'FI1C1', 'FI1D1'",
        ),
        (BOUNDARIES + [("SE110", FAR)], r"1 boundary ids not in the sheet \['SE110'\]"),
    ],
)
def test_sheet_and_boundary_ids_must_match_both_ways(tmp_path, regions, message):
    cache = tmp_path / "cache"
    files, expected = _fixtures(tmp_path, boundaries=parquet(regions))
    eurostat.prepare_inputs(cache, files=files, expected=expected)
    with pytest.raises(eurostat.EurostatError, match=message):
        eurostat.load_inputs(cache, expected=expected)


def _city(place_id, country, overture_id):
    return {
        "place_id": place_id,
        "kind": "city",
        "country_code": country,
        "overture_id": overture_id,
    }


def test_every_assignment_status():
    metros = eurostat.read_composition(workbook(COMPOSITION)).metros
    boundaries = eurostat.read_boundaries(parquet(BOUNDARIES))
    places = [
        _city("Q_HEL", "FI", "hel"),  # in a metro region
        _city("Q_ESP", "FI", "esp"),  # two land areas, union inside the metro
        _city("Q_OUL", "FI", "oul"),  # a covered country, a region in no metro
        _city("Q_SEA", "FI", "sea"),  # outside every region
        _city("Q_BRD", "FI", "brd"),  # astride a border inside one metro
        _city("Q_TIE", "FI", "tie"),  # astride a border between two metros
        _city("Q_NOID", "FI", None),  # no overture id
        _city("Q_NOGEO", "FI", "nogeo"),  # only unreadable areas
        _city("Q_ATH", "GR", "ath"),  # gazetteer GR, Eurostat prefix EL
        _city("Q_LON", "GB", "lon"),  # gazetteer GB, Eurostat prefix UK
        _city("Q_BER", "DE", "ber"),  # an uncovered country: no row
        {
            "place_id": "Q_FI",
            "kind": "country",
            "country_code": "FI",
            "overture_id": "fi",
        },
    ]
    areas = {
        "hel": [{"geom": shapely.box(24.4, 60.4, 24.6, 60.6), "sources": []}],
        "esp": [
            {"geom": shapely.box(25.1, 60.1, 25.2, 60.2), "sources": []},
            {"geom": shapely.box(25.7, 60.7, 25.9, 60.9), "sources": []},
        ],
        "oul": [{"geom": shapely.box(25.0, 65.0, 25.1, 65.1), "sources": []}],
        "sea": [{"geom": shapely.box(0.0, 0.0, 1.0, 1.0), "sources": []}],
        "brd": [{"geom": shapely.box(24.9, 60.4, 25.1, 60.6), "sources": []}],
        "tie": [{"geom": shapely.box(24.2, 59.9, 24.4, 60.1), "sources": []}],
        "nogeo": [{"geom": None, "sources": []}],
        "ath": [{"geom": shapely.box(23.6, 37.9, 23.8, 38.1), "sources": []}],
        "lon": [{"geom": shapely.box(-0.2, 51.4, 0.1, 51.6), "sources": []}],
        "ber": [{"geom": shapely.box(10.4, 50.4, 10.6, 50.6), "sources": []}],
        "fi": [{"geom": WEST, "sources": []}],
    }
    rows = eurostat.assign(places, areas, metros, boundaries)
    assert rows == [
        {
            "city_id": "Q_ATH",
            "status": "assigned",
            "nuts_id": "EL301",
            "metro_code": "EL001MC",
        },
        {
            "city_id": "Q_BRD",
            "status": "assigned",
            "nuts_id": "FI1B1",
            "metro_code": "FI001MC",
        },
        {
            "city_id": "Q_ESP",
            "status": "assigned",
            "nuts_id": "FI1C1",
            "metro_code": "FI001MC",
        },
        {
            "city_id": "Q_HEL",
            "status": "assigned",
            "nuts_id": "FI1B1",
            "metro_code": "FI001MC",
        },
        {
            "city_id": "Q_LON",
            "status": "assigned",
            "nuts_id": "UKI31",
            "metro_code": "UK001MC",
        },
        {
            "city_id": "Q_NOGEO",
            "status": "unplaceable",
            "nuts_id": None,
            "metro_code": None,
        },
        {
            "city_id": "Q_NOID",
            "status": "unplaceable",
            "nuts_id": None,
            "metro_code": None,
        },
        {
            "city_id": "Q_OUL",
            "status": "unassigned",
            "nuts_id": "FI1D1",
            "metro_code": None,
        },
        {
            "city_id": "Q_SEA",
            "status": "unassigned",
            "nuts_id": None,
            "metro_code": None,
        },
        {
            "city_id": "Q_TIE",
            "status": "ambiguous",
            "nuts_id": None,
            "metro_code": None,
        },
    ]


def test_a_boundary_point_is_settled_by_footprint_share():
    boundaries = eurostat.read_boundaries(parquet(BOUNDARIES))
    by_nuts3 = {"FI1B1": "FI001MC", "FI1C1": "FI001MC", "FI1E1": "FI002M"}

    def pick(candidates, footprint):
        return eurostat._pick(candidates, footprint, boundaries, by_nuts3)

    # Regions implying one metro need no share; between metros the larger
    # share wins; equal shares, or a footprint without area, stay ambiguous.
    assert pick(["FI1B1", "FI1C1"], shapely.box(24.9, 60.4, 25.1, 60.6)) == (
        "FI1B1",
        False,
    )
    assert pick(["FI1B1", "FI1E1"], shapely.box(24.2, 59.95, 24.4, 60.15)) == (
        "FI1B1",
        False,
    )
    assert pick(["FI1B1", "FI1E1"], shapely.box(24.2, 59.9, 24.4, 60.1)) == (None, True)
    assert pick(["FI1B1", "FI1E1"], shapely.Point(24.3, 60.0)) == (None, True)
    assert pick([], shapely.Point(0.0, 0.0)) == (None, False)
