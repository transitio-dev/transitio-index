"""The place-geometry check: the Wikidata area lookup, the judging of one city
and a run over a published index, with Wikidata stubbed."""

import csv
import importlib.util
import io
import json
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("pyproj")
import builds_fixture as bfx  # noqa: E402
import shapely  # noqa: E402

from transitio_index import overture  # noqa: E402

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_place_geometry.py"
_spec = importlib.util.spec_from_file_location("check_place_geometry", _SCRIPT)
gpc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gpc)

ENTITY = "http://www.wikidata.org/entity/"
# Geodesic areas of the boxes the tests draw on the equator, in km².
TENTH = 123.09
HUNDREDTH = 1.2309


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)


def _box(size):
    return shapely.box(0, 0, size, size)


def test_areas_reads_best_ranked_values_in_square_kilometres(monkeypatch):
    payload = {
        "results": {
            "bindings": [
                {"item": {"value": ENTITY + "Q3826"}, "m2": {"value": "3500000000"}},
                {"item": {"value": ENTITY + "Q3826"}, "m2": {"value": "3.9e9"}},
                {"item": {"value": ENTITY + "not-a-qid"}, "m2": {"value": "1000000"}},
            ]
        }
    }
    queries = []

    def fake_urlopen(request, timeout=None):
        query = urllib.parse.urlsplit(request.full_url).query
        queries.append(urllib.parse.parse_qs(query)["query"][0])
        return io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    client = overture.WikidataClient()
    assert client.areas(["Q3826", "Q11568", None]) == {"Q3826": [3500.0, 3900.0]}
    (query,) = queries
    assert "VALUES ?item { wd:Q11568 wd:Q3826 }" in query
    assert "?st a wikibase:BestRank" in query
    assert "psn:P2046/wikibase:quantityAmount" in query
    with pytest.raises(overture.GazetteerError, match="not Wikidata QIDs"):
        client.areas(["Q1 } UNION { ?x ?y ?z"])
    assert len(queries) == 1


def test_area_is_the_same_whatever_the_rings_winding():
    hole = shapely.box(0.02, 0.02, 0.04, 0.04)
    holed = shapely.Polygon(_box(0.1).exterior, [hole.exterior])  # hole drawn ccw
    clockwise = shapely.box(1, 0, 1.1, 0.1, ccw=False)
    # Counter-clockwise across the antimeridian, clockwise in the plane.
    across = shapely.Polygon([(179.95, 0), (-179.95, 0), (-179.95, 0.1), (179.95, 0.1)])
    multi = shapely.MultiPolygon([clockwise, holed])
    hole_area = gpc.area_km2(shapely.to_wkb(hole))
    assert gpc.area_km2(shapely.to_wkb(multi)) == pytest.approx(
        2 * TENTH - hole_area, rel=1e-4
    )
    assert gpc.area_km2(shapely.to_wkb(across)) == pytest.approx(TENTH, rel=1e-4)


@pytest.mark.parametrize(
    "qid, areas, expected",
    [
        ("Q1", {"Q1": [10.0]}, ("larger", 10.0)),
        ("Q1", {"Q1": [2000.0]}, ("smaller", 2000.0)),
        ("Q1", {"Q1": [100.0]}, None),
        # The value nearest by ratio decides, not the nearest by difference.
        ("Q1", {"Q1": [5.0, 200.0]}, None),
        ("Q1", {"Q1": [1.0, 5000.0]}, ("smaller", 5000.0)),
        (None, {}, ("no_qid", None)),
        ("Q1", {}, None),
    ],
    ids=[
        "larger",
        "smaller",
        "within",
        "nearest-within",
        "nearest-off",
        "no-qid",
        "no-area",
    ],
)
def test_judge(qid, areas, expected):
    place = {
        "place_id": "p1",
        "name": "Muscat",
        "country_code": "OM",
        "wikidata_id": qid,
        "geometry_source": "overture",
        "geometry": shapely.to_wkb(_box(0.1)),
    }
    row = gpc.judge(place, areas, 10.0)
    if expected is None:
        assert row is None
        return
    flag, km2 = expected
    assert (row["flag"], row["wikidata_km2"]) == (flag, km2)
    assert row["area_km2"] == pytest.approx(TENTH, rel=1e-4)
    assert row["ratio"] == (None if km2 is None else pytest.approx(TENTH / km2, 1e-3))
    assert list(row) == list(gpc.COLUMNS)


class _Wikidata:
    def __init__(self, areas):
        self._areas = areas
        self.asked = None

    def areas(self, qids):
        self.asked = sorted({qid for qid in qids if qid})
        return {qid: self._areas[qid] for qid in self.asked if qid in self._areas}


# A name a spreadsheet would run as a formula.
NAMES = {"hamlet": "=1+1"}


def test_main_writes_the_flagged_cities_in_order(tmp_path, capsys):
    def city(place_id, country, size, qid=None, source="overture", kind="city"):
        return bfx._place(
            place_id,
            kind,
            NAMES.get(place_id, place_id.title()),
            None,
            _box(size) if size else None,
            country=country,
            wikidata_id=qid,
            geometry_source=source,
        )

    places = [
        city("fi", "FI", 1.0, "Q33", kind="country"),
        city("big", "FI", 0.1, "Q1"),
        city("tiny", "FI", 0.01, "Q2"),
        city("fit", "FI", 0.1, "Q3"),
        city("bare", "FI", 0.1, "Q4"),  # no Wikidata area: not judged
        city("void", "FI", None, "Q1"),  # no polygon: not judged
        city("hamlet", "NZ", 0.01),
        # Larger than hamlet by less than the written area's rounding.
        city("wick", "NZ", 0.010001),
        city("auckland", "NZ", 0.2, source="derived_from_feeds"),
        city("metro", "NZ", 0.3, kind="metro"),  # only cities are checked
        city("sto", "SE", 0.3),  # outside --countries
    ]
    index = tmp_path / "index"
    bfx.write_partitioned_build(index, places=places)
    wikidata = _Wikidata({"Q1": [10.0], "Q2": [3500.0], "Q3": [120.0], "Q33": [1.0]})
    out = tmp_path / "check.csv"

    argv = ["--index", str(index), "--countries", "fi", "NZ", "--out", str(out)]
    assert gpc.main(argv, wikidata=wikidata) == 0

    assert wikidata.asked == ["Q1", "Q2", "Q3", "Q4"]
    with out.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [(r["place_id"], r["flag"], r["wikidata_km2"]) for r in rows] == [
        ("tiny", "smaller", "3500.0"),
        ("big", "larger", "10.0"),
        ("auckland", "no_qid", ""),
        ("wick", "no_qid", ""),
        ("hamlet", "no_qid", ""),
    ]
    assert rows[2]["geometry_source"] == "derived_from_feeds"
    assert rows[4]["name"] == "'=1+1"
    assert rows[3]["area_km2"] == rows[4]["area_km2"] == "1.231"
    assert float(rows[0]["ratio"]) == pytest.approx(HUNDREDTH / 3500, rel=1e-3)
    assert capsys.readouterr().out.splitlines() == [
        "cities: 8 (4 with a Wikidata area)",
        "larger: 1 (FI 1)",
        "smaller: 1 (FI 1)",
        "no_qid: 3 (NZ 3)",
        f"wrote: {out}",
    ]
