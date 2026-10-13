"""The route-span reference table: its generator and the shipped table."""

import importlib.util
from pathlib import Path

from transitio_index import classify

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "route_span_reference.py"
_spec = importlib.util.spec_from_file_location("route_span_reference", _SCRIPT)
reference = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reference)


def test_a_route_counts_once_per_bucket_and_small_countries_fall_to_star():
    routes = [
        ("FI", "bus", ("1", b"a"), 1.0),
        ("FI", "bus", ("2", b"b"), 2.0),
        ("FI", "bus", ("3", b"c"), 3.0),
        ("FI", "bus", ("4", b"d"), 4.0),
        # Another feed carrying FI's route 1 over the same stops.
        ("FI", "bus", ("1", b"a"), 1.0),
        ("SE", "bus", ("5", b"e"), 10.0),
        ("SE", "bus", ("6", b"f"), 20.0),
        # A feed without a home country reaches the * row only.
        (None, "rail", ("7", b"g"), 7.0),
        # An aerial lift is no basic mode.
        ("FI", "1300", ("8", b"h"), 0.5),
    ]
    rows = reference.reference_rows(routes, minimum=3)
    assert rows == [
        {"country": "*", "mode": "bus", "routes": 6, "p90_km": 15.0},
        {"country": "*", "mode": "rail", "routes": 1, "p90_km": 7.0},
        {"country": "FI", "mode": "bus", "routes": 4, "p90_km": 3.7},
    ]
    # The script refuses a table without a * row for every basic mode.
    assert reference.missing_modes(rows) == ["ferry", "subway", "tram"]


def test_a_copy_that_fails_to_read_gives_way_to_the_next(monkeypatch):
    copies = [(name, {"feed_id": "f"}, "same digests") for name in "abc"]
    monkeypatch.setattr(reference, "_crawls", lambda roots: iter(copies))
    read = {"a": None, "b": [("bus", ("1", b"x"), 1.0)], "c": []}
    monkeypatch.setattr(reference, "feed_routes", lambda d, state: read[d])
    measured = [routes for _, routes in reference.measured_feeds([])]
    assert measured == [read["b"]]


def test_the_shipped_table_has_a_value_for_every_basic_mode():
    table = classify.route_span_reference()
    for mode in classify.BASIC_MODES:
        assert table[("*", mode)] > 0
