import hashlib
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import transitio.index as transitio_index  # noqa: E402
from index_build import classify, licensing, publish, store  # noqa: E402
from test_index_publish import (  # noqa: E402
    PLACES,
    _build_index,
    _covered_feed,
    _edge,
    _publish_audit,
    _publish_coverage,
    _publish_gen,
)

LICENSED = {
    "spdx_identifier": "CC-BY-4.0",
    "url": "https://example.org/l",
    "redistribution_allowed": "yes",
}


def _cache(tmp_path, feeds=None, edges=None):
    """A crosswalk, an expanded generation with a geometry audit behind it,
    classified coverage edges: what the license stage reads."""
    pytest.importorskip("geopandas")
    cache, _ = _build_index(tmp_path)
    audit = _publish_audit(cache)
    expanded = _publish_gen(
        cache,
        "expanded.json",
        "places_expanded.jsonl",
        PLACES,
        {
            "source": "expand",
            "overture_release": "2026-08-19.0",
            "places_overrides_sha256": None,
            "geometry_generation": audit["generation"],
        },
    )
    _publish_coverage(
        cache,
        {
            "overture_release": "2026-08-19.0",
            "expanded_generation": expanded["generation"],
        },
        feeds,
        edges,
    )
    classify.classify(cache)
    return cache


def test_the_stage_licenses_what_publication_reads_and_publication_ships_it(tmp_path):
    cache = _cache(tmp_path)
    before = publish.read_inputs(cache, None)
    manifest = licensing.license_index(cache)
    assert manifest["licensed"] is True
    assert manifest["generations"] == before["generations"]
    assert manifest["inputs"]["edges"]["generation"] == before["coverage"]["generation"]
    feeds, _ = store.read_jsonl(
        cache / "license", "licensed.json", "feeds_licensed.jsonl"
    )
    edges, _ = store.read_jsonl(
        cache / "license", "licensed.json", "edges_licensed.jsonl"
    )
    places, _ = store.read_jsonl(
        cache / "license", "licensed.json", "places_licensed.jsonl"
    )
    # The tables pass through, the feeds stamped with the licence judgement.
    assert feeds == [
        {**r, "redistribution_allowed": licensing.redistribution_allowed(r)}
        for r in before["records"]
    ]
    assert (edges, places) == (before["edges"], before["places"])
    inventory, _ = store.read_jsonl(
        cache / "license", "licensed.json", "licence_inventory.jsonl"
    )
    assert inventory[0]["role"] == "aggregator"
    catalogues = {r["dataset"]: r for r in inventory if r["role"] == "catalogue"}
    assert (
        catalogues["Transitland Atlas"]["version"]
        == before["sources"]["atlas"]["commit"]
    )
    assert (
        catalogues["Mobility Database catalog"]["version"]
        == before["sources"]["mdb"]["csv_sha256"]
    )
    assert all(
        r["license"] is None and r["allowed"] is None for r in catalogues.values()
    )
    assert inventory[-1] == {
        "role": "feed_licence",
        "dataset": None,
        "license": None,
        "url": None,
        "version": None,
        "allowed": None,
        "feeds": len(feeds),
        "redistribution_allowed": {"unknown": len(feeds)},
        "judgement": {"unknown": len(feeds)},
        "attribution_text": None,
        "attribution_instructions": None,
    }
    generation, _ = store.resolve(cache / "license", "licensed.json")
    with generation:
        notice = generation.read_bytes("NOTICE").decode()
    assert notice.startswith("Boundary geometry from Overture.")
    assert "Transitland Atlas, commit " in notice and "none declared" in notice
    # Publication reads the licensed artifacts and ships the NOTICE.
    snapshot = publish.publish(cache)
    assert snapshot["licensed"] is True
    assert snapshot["generations"]["license/licensed.json"] == manifest["generation"]
    assert set(snapshot["leaves"].values()) == {"license/licensed.json"}
    assert (cache / "index" / "NOTICE").read_text() == notice
    assert snapshot["notice_sha256"] == hashlib.sha256(notice.encode()).hexdigest()
    index = transitio_index.read_index(cache / "index")
    assert len(index.feeds) == len(feeds) and len(index.edges) == len(edges)


def test_feed_licences_are_inventoried_per_declared_licence():
    rows = licensing._feed_rows(
        [
            {**_covered_feed("f-a"), "atlas": {"license": LICENSED}},
            {
                **_covered_feed("f-b"),
                "atlas": {"license": {**LICENSED, "redistribution_allowed": False}},
            },
            _covered_feed("f-c"),
            {**_covered_feed("f-d"), "mdb": {"license_url": "https://example.org/m"}},
            {
                **_covered_feed("f-e"),
                "atlas": {"license": {**LICENSED, "attribution_text": "Data by E"}},
            },
            # An Atlas declaration is never combined with the MDB URL.
            {
                **_covered_feed("f-f"),
                "atlas": {"license": {"spdx_identifier": "ODbL-1.0"}},
                "mdb": {"license_url": "https://example.org/m"},
            },
        ]
    )
    assert [
        (r["license"], r["url"], r["feeds"], r["redistribution_allowed"]) for r in rows
    ] == [
        ("CC-BY-4.0", "https://example.org/l", 1, {"yes": 1}),
        ("CC-BY-4.0", "https://example.org/l", 2, {"false": 1, "yes": 1}),
        (None, None, 1, {"unknown": 1}),
        (None, "https://example.org/m", 1, {"unknown": 1}),
        ("ODbL-1.0", None, 1, {"unknown": 1}),
    ]
    notice = licensing._notice(None, {}, rows)
    assert "  - CC-BY-4.0: 2\n      url: https://example.org/l" in notice
    assert "  - no identifier: 1\n      url: https://example.org/m" in notice
    assert "  - none declared: 1" in notice
    # A required attribution ships verbatim, grouped apart from feeds without it.
    assert (
        "  - CC-BY-4.0: 1\n      url: https://example.org/l\n"
        "      attribution: Data by E"
    ) in notice


def test_a_stale_license_generation_is_refused_and_none_means_unlicensed(tmp_path):
    cache = _cache(tmp_path)
    licensing.license_index(cache)
    publish.publish(cache)
    assert (cache / "index" / "NOTICE").is_file()
    # The inputs moved on: the licensed artifacts no longer describe them.
    classify.classify(cache)
    with pytest.raises(publish.PublishError, match="re-run the license stage"):
        publish.publish(cache)
    # Without a license generation publication ships the inputs and says so,
    # and the stale NOTICE does not linger.
    (cache / "license" / "licensed.json").unlink()
    snapshot = publish.publish(cache)
    assert snapshot["licensed"] is False
    assert not (cache / "index" / "NOTICE").exists()
    assert "license/licensed.json" not in snapshot["generations"]


def test_the_cli_runs_the_stage_between_prune_and_publish():
    import build_index

    order = list(build_index.STAGES)
    assert order.index("prune") < order.index("license") < order.index("publish")
    assert build_index.stages_from("license", True) == ["license", "publish"]


def test_only_a_licensed_snapshot_is_released(tmp_path):
    from index_build import publisher

    cache = _cache(tmp_path)
    publish.publish(cache)
    snapshot = json.loads((cache / "index" / "snapshot.json").read_text())
    assert snapshot["licensed"] is False
    with pytest.raises(publisher.PublishIndexError, match="not licensed|NOTICE"):
        publisher.pack(cache / "index", cache_dir=cache)


def test_a_places_build_without_a_geometry_audit_cannot_be_licensed(tmp_path):
    cache = _cache(tmp_path)
    (cache / "gazetteer" / "geometry.json").unlink()
    with pytest.raises(licensing.LicenseError, match="geometry audit"):
        licensing.license_index(cache)


def test_a_moved_geometry_audit_requires_the_gazetteer_to_rerun(tmp_path):
    cache = _cache(tmp_path)
    licensing.license_index(cache)
    publish.publish(cache)
    _publish_audit(cache, notice="Boundary geometry from Overture, corrected.\n")
    # The places still descend from the earlier audit: a regenerated one
    # cannot relabel them, and the whole gazetteer chain must run again.
    with pytest.raises(licensing.LicenseError, match="re-run the gazetteer stage"):
        licensing.license_index(cache)
    # Publication refuses the same way rather than shipping the old licence.
    with pytest.raises(publish.PublishError, match="re-run the gazetteer stage"):
        publish.publish(cache)


HULL = "0101000000" + "00" * 16  # a WKB point


@pytest.mark.parametrize(
    ("block", "expected"),
    [
        ({"redistribution_allowed": "yes"}, True),
        ({"redistribution_allowed": "no", "spdx_identifier": "CC0-1.0"}, False),
        ({"redistribution_allowed": False}, False),
        ({"spdx_identifier": "ODbL-1.0"}, True),
        ({"spdx_identifier": "ODC-By-1.0"}, True),
        ({"spdx_identifier": "pddl-1.0"}, True),  # SPDX ids compare case-insensitively
        ({"spdx_identifier": "Proprietary-X"}, None),
        (None, None),
    ],
)
def test_redistribution_is_judged_from_the_declared_licence(block, expected):
    record = {"atlas": {"license": block}} if block is not None else {}
    assert licensing.redistribution_allowed(record) is expected


def test_prohibited_hulls_are_nulled_and_the_judgement_ships(tmp_path):
    feeds = [
        {
            **_covered_feed("f-a", coverage_source="crawl"),
            "coverage": HULL,
            "atlas": {"license": {"redistribution_allowed": "no"}},
        },
        {**_covered_feed("f-b", coverage_source="crawl"), "coverage": HULL},
        {
            **_covered_feed("f-c", coverage_source="crawl"),
            "coverage": HULL,
            "atlas": {"license": {"spdx_identifier": "CC-BY-4.0"}},
        },
    ]
    cache = _cache(tmp_path, feeds)
    manifest = licensing.license_index(cache)
    assert manifest["hulls_nulled"] == 1
    assert manifest["redistribution_allowed"] == {"false": 1, "none": 1, "true": 1}
    licensed, _ = store.read_jsonl(
        cache / "license", "licensed.json", "feeds_licensed.jsonl"
    )
    inventory, _ = store.read_jsonl(
        cache / "license", "licensed.json", "licence_inventory.jsonl"
    )
    judgements = {
        (r["license"], tuple(sorted(r["redistribution_allowed"]))): r["judgement"]
        for r in inventory
        if r["role"] == "feed_licence"
    }
    # Grouped by licence requirement, so the two undeclared feeds share a
    # row: its declarations and its judgements are counted side by side.
    assert judgements == {
        (None, ("no", "unknown")): {"false": 1, "unknown": 1},
        ("CC-BY-4.0", ("unknown",)): {"true": 1},
    }
    by_id = {f["feed_id"]: f for f in licensed}
    assert (
        by_id["f-a"]["coverage"] is None and by_id["f-a"]["coverage_source"] == "crawl"
    )
    assert (
        by_id["f-b"]["coverage"] == HULL
        and by_id["f-b"]["redistribution_allowed"] is None
    )
    assert (
        by_id["f-c"]["coverage"] == HULL
        and by_id["f-c"]["redistribution_allowed"] is True
    )
    publish.publish(cache)
    index = transitio_index.read_index(cache / "index")
    rows = index.feeds.set_index("feed_id")
    assert (
        rows.loc["f-a", "coverage"] is None
        and rows.loc["f-a", "redistribution_allowed"] is False
    )
    assert rows.loc["f-c", "coverage"] == bytes.fromhex(HULL)
    feed = next(
        f
        for f in transitio_index.place("Q1757", index=index).feeds()
        if f.feed_id == "f-a"
    )
    assert feed.redistribution_allowed is False and feed.coverage is None


@pytest.mark.parametrize(
    "block, derived",
    [
        ({"spdx_identifier": "CC-BY-4.0"}, True),  # redistributable: contributes
        ({}, False),  # unknown: ships, but builds no boundary
    ],
)
def test_a_place_without_a_boundary_gets_one_from_redistributable_hulls(
    tmp_path, block, derived
):
    shapely = pytest.importorskip("shapely")
    near = shapely.to_wkb(shapely.box(24.9, 60.1, 25.1, 60.3)).hex()
    far = shapely.to_wkb(shapely.box(27.6, 62.8, 27.8, 63.0)).hex()
    feeds = [
        {**_covered_feed("f-a"), "coverage": near, "atlas": {"license": block}},
        {
            **_covered_feed("f-p"),
            "coverage": far,
            "atlas": {"license": {"redistribution_allowed": "no"}},
        },
    ]
    edges = [_edge("Q1757", "f-a"), _edge("Q-metro", "f-a"), _edge("Q-metro", "f-p")]
    cache = _cache(tmp_path, feeds, edges)
    manifest = licensing.license_index(cache)
    places, _ = store.read_jsonl(
        cache / "license", "licensed.json", "places_licensed.jsonl"
    )
    by_id = {p["place_id"]: p for p in places}
    # A place with a boundary of its own keeps it.
    assert by_id["Q1757"]["geometry_source"] == "overture"
    metro = by_id["Q-metro"]
    if not derived:
        assert metro["geometry"] is None and metro["geometry_source"] is None
        assert manifest["geometry_derived"] == 0
        assert manifest["places_without_geometry"] == 1
        return
    assert manifest["geometry_derived"] == 1
    assert manifest["places_without_geometry"] == 0
    assert metro["geometry_source"] == "derived_from_feeds"
    boundary = shapely.from_wkb(bytes.fromhex(metro["geometry"]))
    # The union of the redistributable hull, widened; the withheld hull is
    # not in it.
    assert boundary.covers(shapely.from_wkb(bytes.fromhex(near)))
    assert boundary.disjoint(shapely.from_wkb(bytes.fromhex(far)))
    publish.publish(cache)
    index = transitio_index.read_index(cache / "index")
    rows = index.places.set_index("place_id")
    assert rows.loc["Q-metro", "geometry_source"] == "derived_from_feeds"
    assert rows.loc["Q-metro", "geometry"].equals(boundary)


def test_a_derived_boundary_stays_inside_the_wgs84_domain():
    shapely = pytest.importorskip("shapely")
    hull = shapely.to_wkb(shapely.box(179.99, 60.0, 180.0, 60.1)).hex()
    places = [{"place_id": "P", "geometry": None, "geometry_source": None}]
    edges = [{"place_id": "P", "feed_id": "f"}]
    feeds = [{"feed_id": "f", "coverage": hull, "redistribution_allowed": True}]
    assert licensing._derive_geometry(places, edges, feeds) == (1, 0)
    bounds = shapely.from_wkb(bytes.fromhex(places[0]["geometry"])).bounds
    assert bounds[2] == 180.0 and bounds[0] > 179.9


def test_the_post_condition_catches_a_surviving_prohibited_hull():
    with pytest.raises(licensing.LicenseError, match="prohibited coverage hulls"):
        licensing._assert_sanitised(
            [{"feed_id": "f-x", "redistribution_allowed": False, "coverage": HULL}]
        )


def test_a_license_generation_from_an_older_policy_is_refused(tmp_path, monkeypatch):
    cache = _cache(tmp_path)
    monkeypatch.setattr(licensing, "POLICY_VERSION", 0)
    licensing.license_index(cache)
    monkeypatch.undo()
    with pytest.raises(publish.PublishError, match="older licensing policy"):
        publish.publish(cache)


def test_a_changed_judgement_is_a_new_snapshot_even_for_feeds_only(
    tmp_path, monkeypatch
):
    cache, _ = _build_index(tmp_path)
    licensing.license_index(cache)
    before = publish.publish(cache)
    assert before["licensed"] is True
    monkeypatch.setattr(licensing, "redistribution_allowed", lambda record: True)
    licensing.license_index(cache)
    after = publish.publish(cache)
    assert after["snapshot_id"] != before["snapshot_id"]
