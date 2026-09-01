import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
from index_build import boundaries, store  # noqa: E402

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

    class Counting:
        def __init__(self, dataset):
            self._dataset = dataset
            self.scans = 0

        def to_batches(self, **kw):
            self.scans += 1
            return self._dataset.to_batches(**kw)

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
        assert len(record["geometries"]) == 1
        assert len(record["geoms"]) == 1


def test_overlapping_boxes_merge_into_one_scan():
    merged = boundaries._merge_boxes([(0, 0, 2, 2), (1, 1, 3, 3), (10, 10, 11, 11)])
    assert sorted(merged) == [(0, 0, 3, 3), (10, 10, 11, 11)]


def test_a_point_outside_everything_finds_nothing(tmp_path):
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([(0.0, 0.0, 1.0, 1.0)])
        assert lookup.divisions_at(0.5, 0.5) == []


def test_a_stale_memo_entry_is_not_trusted(tmp_path):
    # A memo written before geometry validation existed (or corrupted on
    # disk) must be filtered on load, not fed to the containment index.
    cache = tmp_path / "cache"
    with _lookup(tmp_path, cache) as lookup:
        lookup.ensure([HEL_BOX])
    memo = cache / "boundary_lookup" / "test-release" / "divisions.jsonl"
    rows = [json.loads(line) for line in memo.read_text().splitlines()]
    line_hex = shapely.to_wkb(shapely.LineString([(24.9, 60.12), (25.0, 60.18)])).hex()
    for row in rows:
        if row["division_id"] == "fi-hel":
            row["geometries"] = [line_hex, "not hex"]
    memo.write_text("".join(json.dumps(r) + "\n" for r in rows))
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
    assert (cache / "boundary_lookup" / "test-release" / "divisions.jsonl").is_file()
    assert (cache / "boundary_lookup" / "test-release" / "covered.jsonl").is_file()
