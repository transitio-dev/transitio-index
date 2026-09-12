import threading

import pytest

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
from transitio_index import boundaries, store  # noqa: E402

CC0 = [{"dataset": "OpenStreetMap", "license": "ODbL", "property": ""}]


def _wkb(minx, miny, maxx, maxy):
    return shapely.to_wkb(shapely.box(minx, miny, maxx, maxy))


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
        "fi-uusimaa",
        "FI",
        "region",
        wikidata="Q1508",
        name="Uusimaa",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"), ("fi-uusimaa", "region", "Uusimaa")
        ),
    ),
    fx.division(
        "fi-hel",
        "FI",
        "locality",
        wikidata="Q1757",
        name="Helsinki",
        hierarchies=fx.chain(
            ("fi", "country", "Finland"),
            ("fi-uusimaa", "region", "Uusimaa"),
            ("fi-hel", "locality", "Helsinki"),
        ),
    ),
]

AREAS = [
    fx.area("fi", _wkb(19.0, 59.0, 32.0, 71.0), CC0, country="FI"),
    fx.area("fi-uusimaa", _wkb(23.0, 59.8, 26.5, 60.9), CC0, country="FI"),
    fx.area("fi-hel", _wkb(24.8, 60.1, 25.3, 60.35), CC0, country="FI"),
    # A maritime polygon COVERING the query point and a malformed row: both
    # must be ignored despite matching the bbox pushdown.
    fx.area("fi-sea", _wkb(24.0, 59.0, 25.5, 60.3), CC0, is_land=False, country="FI"),
    fx.area("fi-bad", b"not wkb", CC0, country="FI"),
    # A usable polygon naming no division is not evidence for anything.
    fx.area("", _wkb(24.8, 60.1, 25.3, 60.35), CC0, country="FI"),
    # Parseable but not usable as containment evidence: a line and a bowtie.
    fx.area(
        "fi-line",
        shapely.to_wkb(shapely.LineString([(24.9, 60.12), (25.0, 60.18)])),
        CC0,
        country="FI",
    ),
    fx.area(
        "fi-bowtie",
        shapely.to_wkb(
            shapely.Polygon([(24.9, 60.15), (25.0, 60.2), (25.0, 60.15), (24.9, 60.2)])
        ),
        CC0,
        country="FI",
    ),
]

HEL_BOX = (24.9, 60.15, 25.0, 60.2)


def _datasets(tmp_path):
    divisions = fx.write_dataset(tmp_path / "divisions.parquet", DIVISIONS)
    areas = fx.write_area_dataset(tmp_path / "areas.parquet", AREAS)
    return divisions, areas


def _lookup(tmp_path, cache):
    divisions, areas = _datasets(tmp_path)
    return boundaries.BoundaryLookup(
        cache, release="test-release", area_dataset=areas, division_dataset=divisions
    )


def test_a_point_resolves_most_specific_first(tmp_path):
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        added = lookup.ensure([HEL_BOX])
        assert added == 3  # locality, region, country; sea and junk ignored
        found = lookup.divisions_at(24.94, 60.17)
        assert [r["division_id"] for r in found] == ["fi-hel", "fi-uusimaa", "fi"]
        assert found[0]["kind"] == "city"
        assert found[0]["wikidata"] == "Q1757"
        assert [a["overture_id"] for a in found[0]["ancestors"]][:2] == [
            "fi",
            "fi-uusimaa",
        ]
        # Outside the city but inside the region.
        rural = lookup.divisions_at(23.5, 60.5)
        assert [r["division_id"] for r in rural] == ["fi-uusimaa", "fi"]


def test_the_memo_answers_without_datasets(tmp_path):
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([HEL_BOX])
    reopened = boundaries.BoundaryLookup(cache, release="test-release")
    try:
        assert reopened.ensure([HEL_BOX]) == 0  # covered: no datasets needed
        found = reopened.divisions_at(24.94, 60.17)
        assert [r["division_id"] for r in found] == ["fi-hel", "fi-uusimaa", "fi"]
    finally:
        reopened.close()


def test_an_uncovered_box_without_datasets_is_refused(tmp_path):
    cache = tmp_path / "cache"
    lookup = boundaries.BoundaryLookup(cache, release="test-release")
    try:
        with pytest.raises(store.StoreError, match="datasets"):
            lookup.ensure([HEL_BOX])
    finally:
        lookup.close()


def test_covered_boxes_are_not_rescanned(tmp_path):
    cache = tmp_path / "cache"

    class Counting:  # every read starts by listing the dataset's fragments
        def __init__(self, dataset):
            self._dataset = dataset
            self.scans = 0

        def get_fragments(self):
            self.scans += 1
            return self._dataset.get_fragments()

    divisions, areas = _datasets(tmp_path)
    counting = Counting(areas)
    lookup = boundaries.BoundaryLookup(
        cache,
        release="test-release",
        area_dataset=counting,
        division_dataset=divisions,
    )
    try:
        lookup.ensure([HEL_BOX])
        first = counting.scans
        lookup.ensure([HEL_BOX])
        lookup.ensure([(24.92, 60.16, 24.96, 60.18)])  # inside the covered box
        assert counting.scans == first
    finally:
        lookup.close()


def test_a_stalled_box_scan_is_retried_on_a_fresh_connection(tmp_path, monkeypatch):
    """A tile scan that yields nothing for the deadline is abandoned and retried
    on the dataset ``reopen_area`` returns, which then serves the boxes that
    follow without another reopen."""
    from transitio_index import geometry

    monkeypatch.setattr(geometry, "AREA_READ_DEADLINE", 0.2)
    divisions, areas = _datasets(tmp_path)
    opened = []

    class Hanging:  # a connection the proxy dropped: nothing ever arrives
        def get_fragments(self):
            threading.Event().wait(3)
            return iter(())

    def reopen():
        opened.append(1)
        return areas

    lookup = boundaries.BoundaryLookup(
        tmp_path / "cache",
        release="test-release",
        area_dataset=Hanging(),
        division_dataset=divisions,
        reopen_area=reopen,
    )
    try:
        assert lookup.ensure([HEL_BOX]) > 0  # served by the reopened dataset
        lookup.ensure([(23.0, 60.0, 24.0, 61.0)])  # a further box: no second reopen
        assert opened == [1]
    finally:
        lookup.close()


def test_a_rediscovered_division_gains_its_new_component(tmp_path):
    # One division with two disjoint polygons: a later box must merge the
    # second component into the cached record, not discard it.
    cache = tmp_path / "cache"
    divisions = fx.write_dataset(tmp_path / "d.parquet", DIVISIONS)
    areas = fx.write_area_dataset(
        tmp_path / "a.parquet",
        [
            fx.area("fi-hel", _wkb(24.8, 60.1, 25.3, 60.35), CC0, country="FI"),
            fx.area("fi-hel", _wkb(30.0, 65.0, 31.0, 66.0), CC0, country="FI"),
        ],
    )
    with boundaries.BoundaryLookup(
        cache, release="test-release", area_dataset=areas, division_dataset=divisions
    ) as lookup:
        lookup.ensure([HEL_BOX])
        assert lookup.divisions_at(30.5, 65.5) == []  # second component unknown
        lookup.ensure([(30.4, 65.4, 30.6, 65.6)])
        found = lookup.divisions_at(30.5, 65.5)
        assert [r["division_id"] for r in found] == ["fi-hel"]


def test_geometries_never_duplicate_across_boxes(tmp_path):
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        # Two disjoint boxes, both inside the big country polygon.
        lookup.ensure([HEL_BOX])
        lookup.ensure([(28.0, 68.0, 28.1, 68.1)])
        record = lookup._records["fi"]
        assert len(record["geoms"]) == 1


def test_a_point_outside_everything_finds_nothing(tmp_path):
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([(0.0, 0.0, 1.0, 1.0)])
        assert lookup.divisions_at(0.5, 0.5) == []


def test_a_stale_memo_entry_is_not_trusted(tmp_path):
    # A memo written before geometry validation existed (or corrupted on
    # disk) must be filtered on load, not fed to the containment index.
    import geopandas as gpd

    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([HEL_BOX])
    memo = (
        cache
        / "boundary_lookup"
        / boundaries.memo_name("test-release")
        / "divisions-0001.parquet"
    )
    frame = gpd.read_parquet(memo)
    line = shapely.LineString([(24.9, 60.12), (25.0, 60.18)])
    geoms = [
        line if division_id == "fi-hel" else geom
        for division_id, geom in zip(frame["division_id"], frame.geometry)
    ]
    frame = frame.set_geometry(gpd.GeoSeries(geoms, crs=frame.crs))
    frame.to_parquet(memo)
    reopened = boundaries.BoundaryLookup(cache, release="test-release")
    try:
        found = reopened.divisions_at(24.94, 60.17)
        assert [r["division_id"] for r in found] == ["fi-uusimaa", "fi"]
    finally:
        reopened.close()
    # The corruption cleared coverage, so a lookup WITH datasets refetches
    # and the record recovers its geometry.
    with _lookup(tmp_path, cache) as recovered:
        recovered.ensure([HEL_BOX])
        found = recovered.divisions_at(24.94, 60.17)
        assert [r["division_id"] for r in found] == ["fi-hel", "fi-uusimaa", "fi"]


def test_the_memo_is_keyed_by_release(tmp_path):
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([HEL_BOX])
    memo = cache / "boundary_lookup" / boundaries.memo_name("test-release")
    assert (memo / "divisions-0001.parquet").is_file()
    assert (memo / "covered.jsonl").is_file()


def test_memoized_geometry_is_simplified(tmp_path):
    # A boundary carrying a vertex that deviates less than the shipping
    # tolerance is memoized simplified: the sub-tolerance vertex is dropped,
    # so the lookup does not keep full-resolution coastline.
    dense = shapely.Polygon(
        [
            (24.90, 60.15),
            (24.95, 60.1503),  # ~0.0003 deg off the edge (< 0.001 tolerance)
            (25.00, 60.15),
            (25.00, 60.20),
            (24.90, 60.20),
        ]
    )
    divisions = fx.write_dataset(tmp_path / "d.parquet", [DIVISIONS[2]])  # fi-hel
    areas = fx.write_area_dataset(
        tmp_path / "a.parquet",
        [fx.area("fi-hel", shapely.to_wkb(dense), CC0, country="FI")],
    )
    with boundaries.BoundaryLookup(
        tmp_path / "cache",
        release="test-release",
        area_dataset=areas,
        division_dataset=divisions,
    ) as lookup:
        lookup.ensure([HEL_BOX])
        stored = lookup._records["fi-hel"]["geoms"][0]
        assert len(stored.exterior.coords) < len(dense.exterior.coords)
        assert lookup.divisions_at(24.94, 60.17)[0]["division_id"] == "fi-hel"


def test_the_memo_is_keyed_by_the_simplification_tolerance(tmp_path, monkeypatch):
    from transitio_index import geometry

    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([HEL_BOX])
    current = cache / "boundary_lookup" / boundaries.memo_name("test-release")
    assert (current / "covered.jsonl").is_file()
    # Polygons simplified at one tolerance are another tolerance's stale read:
    # a changed constant opens a fresh memo and leaves the old one alone.
    monkeypatch.setattr(geometry, "SIMPLIFY_TOLERANCE_DEG", 0.002)
    other = cache / "boundary_lookup" / boundaries.memo_name("test-release")
    assert other != current and other.name == "test-release-t0.002"
    with _lookup(tmp_path, cache) as lookup:
        assert lookup.divisions_at(24.95, 60.17) == []  # nothing memoized here yet
        lookup.ensure([HEL_BOX])
        assert lookup.divisions_at(24.95, 60.17)
    assert (other / "covered.jsonl").is_file()
