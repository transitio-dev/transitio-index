"""Regression tests: one per fixed defect, guarding against reappearance.

Fixtures are imported from the stage test modules rather than duplicated.
"""

import pytest

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
from transitio_index import boundaries, overture  # noqa: E402

GOOD = [{"dataset": "OpenStreetMap", "license": "ODbL-1.0", "property": ""}]


def _wkb(minx, miny, maxx, maxy):
    return shapely.to_wkb(shapely.box(minx, miny, maxx, maxy))


def test_a_qidless_division_reloaded_from_the_memo_keeps_a_none_wikidata(tmp_path):
    """The boundary memo's null scalar columns read back as None, not a NaN
    float: a NaN wikidata is truthy and resolve_qid would mint it as a QID.

    The memo must mix QID-bearing and QID-less divisions — the null-to-NaN
    round-trip only happens in a string column that also holds real values.
    """
    cache = tmp_path / "cache"
    divisions = fx.write_dataset(
        tmp_path / "d.parquet",
        [
            fx.division(
                "x-hel",
                "FI",
                "locality",
                wikidata="Q1757",
                name="Helsinki",
                hierarchies=fx.chain(("x-hel", "locality", "Helsinki")),
            ),
            fx.division(
                "x-noqid",
                "FI",
                "locality",
                name="Nowhere",
                hierarchies=fx.chain(("x-noqid", "locality", "Nowhere")),
            ),
        ],
    )
    areas = fx.write_area_dataset(
        tmp_path / "a.parquet",
        [
            fx.area("x-hel", _wkb(24.8, 60.1, 25.3, 60.35), GOOD, country="FI"),
            fx.area("x-noqid", _wkb(26.0, 62.0, 26.4, 62.2), GOOD, country="FI"),
        ],
    )
    with boundaries.BoundaryLookup(
        cache, release="test-release", area_dataset=areas, division_dataset=divisions
    ) as lookup:
        lookup.ensure([(24.0, 60.0, 27.0, 63.0)])
    # Reopen from the memo alone (the path a later build takes): the round-trip
    # must not turn the null wikidata into a truthy NaN.
    reopened = boundaries.BoundaryLookup(cache, release="test-release")
    try:
        (noqid,) = reopened.divisions_at(26.1, 62.1)
        assert noqid["wikidata"] is None
        assert overture.resolve_qid(noqid, {})[0] is None
        # The QID-bearing neighbour still round-trips its QID.
        (hel,) = reopened.divisions_at(24.94, 60.17)
        assert hel["wikidata"] == "Q1757"
    finally:
        reopened.close()
