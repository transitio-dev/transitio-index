"""Regression tests: one per fixed defect, guarding against reappearance.

Fixtures are imported from the stage test modules rather than duplicated.
"""

import http.client
import logging
import os

import pytest

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
import test_index_boundaries as bt  # noqa: E402
from transitio_index import (  # noqa: E402
    boundaries,
    coverage,
    fetch,
    geometry,
    overture,
    registry,
    seed,
)

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


def test_a_crawled_qidless_division_is_minted_through_the_registry(tmp_path):
    """A crawl-discovered division no QID names, keyed by an ``overture:``
    concordance the registry does not yet carry, is minted rather than
    aborting expand with "no place carries it"."""
    import test_index_expand as ex

    cache = tmp_path / "cache"
    ex._publish_names(cache, ex.SEED_PLACES)
    ex._write_crawl(cache, "f-noqid", ["s1,62.1,26.2\n"])
    path = tmp_path / "places_registry.jsonl"
    ex._publish_run(cache, ex._seeded_registry(path))
    with registry.session(path) as reg:
        manifest, places, report = ex._expand(tmp_path, cache, registry=reg)
        assert (reg.minted, manifest["minted"], manifest["places_added"]) == (1, 1, 1)
    saved = registry.load(path)
    minted_id = saved.resolve("overture:fi-noqid")
    nowhere = places[minted_id]
    assert nowhere["kind"] == "city" and nowhere["name"] == "Nowhere"
    assert nowhere["resolution_method"] == "overture_id"
    assert not any(r.get("overture_id") == "fi-noqid" for r in report)


@pytest.mark.parametrize(
    "boom",
    [
        lambda: http.client.IncompleteRead(b"partial"),
        lambda: http.client.RemoteDisconnected("dropped"),
    ],
    ids=["truncated", "disconnected"],
)
def test_wikidata_labels_bisect_a_failing_batch_and_skip_a_bad_entity(boom):
    """A wbgetentities reply the transport truncates OR drops is not fatal: the
    batch is halved until each reply lands, and a single id that always fails is
    skipped rather than aborting the build."""

    class Flaky(overture.WikidataClient):
        def _entities_batch(self, batch, out):
            # An oversized batch or any batch holding the always-bad id fails;
            # a small good batch succeeds.
            if len(batch) > 3 or "Q666" in batch:
                raise boom()
            for qid in batch:
                out[qid] = {"labels": {"en": qid}, "aliases": []}

    client = Flaky()
    qids = [f"Q{n}" for n in range(1, 11)] + ["Q666"]
    result = client.labels_and_aliases(qids)
    assert "Q666" not in result
    assert set(result) == {f"Q{n}" for n in range(1, 11)}


@pytest.mark.parametrize("code, fatal", [(404, True), (502, False)], ids=["4xx", "5xx"])
def test_wikidata_labels_treat_a_4xx_as_fatal_and_a_5xx_as_transient(code, fatal):
    """A 4xx during a label batch is our request's fault and stays fatal — not
    bisected and skipped — even though HTTPError is a urllib.error.URLError
    subclass; a 5xx (a proxy's 502 on a dropped tunnel included) is the
    transport's and degrades like one: bisected, then the ids skipped."""

    class Failing(overture.WikidataClient):
        def _entities_batch(self, batch, out):
            raise overture.urllib.error.HTTPError(
                overture.WIKIDATA_API, code, "boom", None, None
            )

    client = Failing()
    qids = [f"Q{n}" for n in range(1, 6)]
    if fatal:
        with pytest.raises(overture.urllib.error.HTTPError):
            client.labels_and_aliases(qids)
    else:
        assert client.labels_and_aliases(qids) == {}


@pytest.mark.parametrize(
    "boom, retried",
    [
        (lambda: http.client.RemoteDisconnected("boom"), True),
        (
            lambda: overture.urllib.error.HTTPError("u", 502, "gateway", None, None),
            True,
        ),
        (
            lambda: overture.urllib.error.HTTPError("u", 404, "missing", None, None),
            False,
        ),
    ],
    ids=["disconnect", "5xx", "4xx"],
)
def test_wikidata_get_json_retries_transport_failures_not_bad_requests(
    monkeypatch, boom, retried
):
    """A dropped Wikidata connection, or a 5xx from the server or a gateway, is
    retried rather than fatal; a 4xx is our request's fault and is raised at
    once."""
    calls = {"n": 0}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"ok": 1}'

    def fake_urlopen(request, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise boom()
        return FakeResponse()

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(overture.time, "sleep", lambda seconds: None)
    client = overture.WikidataClient()
    if retried:
        assert client._get_json("https://example.invalid") == {"ok": 1}
        assert calls["n"] == 2
    else:
        with pytest.raises(overture.urllib.error.HTTPError):
            client._get_json("https://example.invalid")
        assert calls["n"] == 1


def test_a_stop_on_another_division_of_a_known_qid_is_not_stale():
    """A QID names several Overture divisions but a place records only one
    overture_id; a stop on another of its divisions is matched by QID, not
    flagged stale — including a registry build keyed by tp_ ids."""

    class FakeLookup:
        def divisions_at(self, x, y):
            return [
                {
                    "overture_id": "ov-B",  # not the id the place recorded
                    "wikidata": "Q100",
                    "kind": "city",
                    "country": "FI",
                }
            ]

    places = {
        "tp_5": {
            "place_id": "tp_5",
            "wikidata_id": "Q100",
            "overture_id": "ov-A",
            "kind": "city",
        }
    }
    by_overture = coverage.place_index(places)
    by_qid = coverage.place_qids(places)
    hit, _, stale = coverage.stop_places(FakeLookup(), 0.0, 0.0, by_overture, by_qid)
    assert hit == {"tp_5"}
    assert stale == set()


def test_a_crawled_division_conflicting_in_kind_with_a_seed_is_reported(
    tmp_path, monkeypatch
):
    """A QID that names Overture divisions of different kinds (a seeded city and
    a crawled region) is reported and the seeded place kept, not an abort."""
    import test_index_expand as ex

    divisions = [
        fx.division(
            "at",
            "AT",
            "country",
            wikidata="Q40",
            name="Austria",
            hierarchies=fx.chain(("at", "country", "Austria")),
        ),
        fx.division(
            "at-graz-region",
            "AT",
            "region",
            wikidata="Q13298",
            name="Graz",
            hierarchies=fx.chain(
                ("at", "country", "Austria"),
                ("at-graz-region", "region", "Graz"),
            ),
        ),
    ]
    areas = [
        fx.area("at", _wkb(9.0, 46.0, 17.0, 49.0), GOOD, country="AT"),
        fx.area("at-graz-region", _wkb(15.3, 47.0, 15.5, 47.15), GOOD, country="AT"),
    ]
    monkeypatch.setattr(ex, "DIVISIONS", divisions)
    monkeypatch.setattr(ex, "AREAS", areas)
    cache = tmp_path / "cache"
    ex._publish_names(
        cache,
        [
            {
                "place_id": "Q40",
                "kind": "country",
                "name": "Austria",
                "country_code": "AT",
                "overture_id": "at",
                "metro_ids": [],
                "member_ids": [],
            },
            {
                "place_id": "Q13298",
                "kind": "city",
                "name": "Graz",
                "country_code": "AT",
                "overture_id": "at-graz-city",
                "metro_ids": [],
                "member_ids": [],
            },
        ],
    )
    ex._write_crawl(cache, "f-graz", ["s1,47.07,15.44\n"])
    manifest, places, report = ex._expand(tmp_path, cache)
    assert places["Q13298"]["kind"] == "city"  # the seeded city stands
    assert manifest["places_added"] == 0
    assert any(
        r.get("kind") == "conflict" and r.get("place_id") == "Q13298" for r in report
    )


def test_overture_s3_filesystem_passes_the_env_proxy(monkeypatch):
    """s3_filesystem routes S3 through HTTPS_PROXY/HTTP_PROXY when one is set:
    the AWS SDK behind S3FileSystem ignores those variables on its own, so a
    proxy-only environment could not read Overture without this.
    """
    import pyarrow.fs

    captured = []
    monkeypatch.setattr(
        pyarrow.fs, "S3FileSystem", lambda **kw: captured.append(kw) or "fs"
    )
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(var, raising=False)

    overture.s3_filesystem()
    assert "proxy_options" not in captured[-1]

    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    # Behind a proxy pyarrow's IO pool is capped once, on the first open; a
    # reopen after a stall (whose retry enlarged the pool) must not shrink it.
    import pyarrow

    capped = []
    monkeypatch.setattr(pyarrow, "set_io_thread_count", capped.append)
    monkeypatch.setattr(overture, "_io_threads_capped", False)
    overture.s3_filesystem()
    assert captured[-1]["proxy_options"] == "http://proxy.example:8080"
    overture.s3_filesystem()
    assert capped == [min(pyarrow.io_thread_count(), overture.PROXY_IO_THREADS)]


def test_overture_s3_filesystem_widens_the_timeout_and_retries(monkeypatch):
    """S3 reads get a generous timeout and retry budget, so a transient slow
    response over a proxy does not abort the gazetteer at the AWS SDK's ~3s
    default request timeout.
    """
    import pyarrow.fs

    captured = []
    monkeypatch.setattr(
        pyarrow.fs, "S3FileSystem", lambda **kw: captured.append(kw) or "fs"
    )
    overture.s3_filesystem()
    kw = captured[-1]
    assert kw["connect_timeout"] >= 10
    assert kw["request_timeout"] >= 30
    assert kw["retry_strategy"] is not None


def test_a_seed_place_conflicting_with_a_registry_place_is_skipped_not_fatal(
    tmp_path, caplog
):
    """A seeded place that shares its QID with a registry place of another kind
    (CA's Saskatoon: a region in the registry, a city from a feed) cannot be
    identified; it is logged and dropped — a child of a dropped place moves up
    to its nearest surviving ancestor — so one cross-level clash no longer
    aborts the gazetteer."""
    path = tmp_path / "places_registry.jsonl"
    path.write_text('{"next_id": 1, "registry": 1}\n')
    with registry.session(path) as reg:
        for qid, kind in (("Q10566", "region"), ("Q77", "city"), ("Q88", "city")):
            reg.identify(
                {"wikidata": [qid]},
                kind=kind,
                country_code="CA",
                minted_from=f"overture:{qid}",
                minted_in="overture test",
            )
        ca = {"country_code": "CA"}
        places = {
            "Q1": {"kind": "country", **ca},
            "Q77": {"kind": "region", "parent_id": "Q1", **ca},
            "Q5": {"kind": "city", "parent_id": "Q77", "metro_ids": ["Q77"], **ca},
            # A dropped region that names itself as its parent: its child has
            # no surviving ancestor, and the walk must not spin.
            "Q88": {"kind": "region", "parent_id": "Q88", **ca},
            "Q6": {"kind": "city", "parent_id": "Q88", **ca},
            "Q10566": {"kind": "city", "parent_id": "Q1", **ca},
            "Q2": {"kind": "city", "parent_id": "Q1", **ca},
        }
        with caplog.at_level(logging.WARNING):
            identified = seed._identify_places(places, reg, "digest")
    assert identified == 4 and set(places) == {"Q1", "Q5", "Q6", "Q2"}
    assert all(registry.ID_PATTERN.match(places[k]["tp_id"]) for k in places)
    # The city under the dropped region is re-parented past it, and no longer
    # names it as a metro; the one under the self-parenting region is orphaned.
    assert places["Q5"]["parent_id"] == "Q1" and places["Q5"]["metro_ids"] == []
    assert places["Q6"]["parent_id"] is None
    assert sum("not identified" in m and "skipped" in m for m in caplog.messages) == 3


def test_the_boundary_memo_grows_by_appended_parts(tmp_path, monkeypatch):
    """The memo used to be one file rewritten whole on every ensure(), so a
    memo accumulated over enough countries crossed the store's artifact
    ceiling and aborted the build. It now appends parts split under
    PART_BYTES; a damaged part clears coverage rather than answering with
    false negatives, is salvaged without duplicating a healthy part's
    polygons, and is replaced under a fresh name, never its own."""
    import geopandas as gpd
    import pandas as pd

    monkeypatch.setattr(boundaries, "PART_BYTES", 1)  # every row its own part
    cache = tmp_path / "cache"
    memo = cache / "boundary_lookup" / "test-release"
    divisions = fx.write_dataset(tmp_path / "d.parquet", bt.DIVISIONS)
    areas = fx.write_area_dataset(
        tmp_path / "a.parquet",
        bt.AREAS[:3]
        + [fx.area("fi-hel", _wkb(30.0, 65.0, 31.0, 66.0), GOOD, country="FI")],
    )
    far = (30.4, 65.4, 30.6, 65.6)  # Helsinki's second, disjoint component

    def parts():
        return sorted(p.name for p in memo.glob("divisions-*.parquet"))

    def lookup():
        return boundaries.BoundaryLookup(
            cache,
            release="test-release",
            area_dataset=areas,
            division_dataset=divisions,
        )

    with lookup() as fresh:
        fresh.ensure([bt.HEL_BOX])
        first = parts()
        before = [(memo / name).read_bytes() for name in first]
        fresh.ensure([far])
    # Three divisions, three parts; the new component appended a fourth and
    # left the first three untouched.
    assert len(first) == 3 and len(parts()) == 4
    assert [(memo / name).read_bytes() for name in first] == before
    reopened = boundaries.BoundaryLookup(cache, release="test-release")
    try:
        assert reopened.ensure([bt.HEL_BOX, far]) == 0
        found = reopened.divisions_at(30.5, 65.5)
        assert [r["division_id"] for r in found] == ["fi-hel", "fi"]
    finally:
        reopened.close()

    def repair():
        # Coverage was cleared: the lost component is refetched.
        with lookup() as repaired:
            assert [r["division_id"] for r in repaired.divisions_at(30.5, 65.5)] == [
                "fi"
            ]
            repaired.ensure([far])
            found = repaired.divisions_at(30.5, 65.5)
            assert [r["division_id"] for r in found] == ["fi-hel", "fi"]
        return repaired

    # A listed part that is gone; its replacement takes a fresh name.
    (memo / parts()[-1]).unlink()
    repair()
    assert parts() == first + ["divisions-0005.parquet"]
    # A damaged part — a copy of a healthy part's row beside a row whose
    # geometry is not a polygon — is salvaged without duplicating the healthy
    # polygon (one new row, so one new part) and its name is not reused.
    healthy = gpd.read_parquet(memo / first[0])
    junk = healthy.set_geometry(
        gpd.GeoSeries([shapely.LineString([(0, 0), (1, 1)])], crs=healthy.crs)
    )
    pd.concat([healthy, junk]).to_parquet(memo / parts()[-1])
    repaired = repair()
    (duplicated,) = healthy["division_id"]
    assert len(repaired._records[duplicated]["geoms"]) == 1
    assert parts() == first + ["divisions-0006.parquet"]
    # A listed part replaced by a symlink is refused like any unreadable part;
    # one replaced by a directory is too, and stays in place with its name.
    (memo / parts()[-1]).unlink()
    os.symlink(memo / first[0], memo / "divisions-0006.parquet")
    repair()
    assert parts() == first + ["divisions-0007.parquet"]
    (memo / parts()[-1]).unlink()
    (memo / "divisions-0007.parquet").mkdir()
    repair()
    assert parts() == first + ["divisions-0007.parquet", "divisions-0008.parquet"]


def test_a_division_without_area_rows_is_not_rescanned(tmp_path):
    """The area cache kept no negative entries, so a division the theme has no
    rows for was rescanned by every stage asking for it — and an id filter
    cannot prune row groups, so each rescan read a whole country's geometry.
    Absence is now recorded with the scan's country scope and answers a later
    read whose scope it covers."""
    dataset = fx.write_area_dataset(
        tmp_path / "a.parquet", [fx.area("A", _wkb(0, 0, 1, 1), GOOD, country="FI")]
    )
    scans = {"n": 0}

    class Counting:
        def to_batches(self, **kwargs):
            scans["n"] += 1
            return dataset.to_batches(**kwargs)

    cache = (tmp_path / "cache", "2026-08-19.0")

    def read(ids, countries):
        return geometry.read_areas(Counting(), ids, cache=cache, countries=countries)

    assert set(read({"A", "ghost"}, {"FI"})) == {"A"}
    assert read({"ghost"}, {"FI"}) == {} and scans["n"] == 1  # same scope: answered
    assert read({"ghost"}, {"FI", "SE"}) == {} and scans["n"] == 2  # wider: scanned
    assert read({"ghost"}, {"SE"}) == {} and scans["n"] == 2  # covered by the wider
    assert read({"ghost"}, None) == {} and scans["n"] == 3  # unnarrowed: scanned
    assert read({"ghost"}, {"DE"}) == {} and scans["n"] == 3  # `*` covers any scope
    # The absence table is held to the cache root like the rows are.
    absent = cache[0] / "overture_areas" / cache[1] / "absent"
    outside = tmp_path / "outside"
    outside.mkdir()
    for entry in absent.iterdir():
        entry.rename(outside / entry.name)
    absent.rmdir()
    absent.symlink_to(outside)
    with pytest.raises(ValueError, match="escapes the cache root"):
        read({"ghost"}, {"FI"})


def test_the_metros_read_warms_the_area_cache_for_every_seeded_division(tmp_path):
    """A scan reads every row group of the places' countries whatever ids it
    asks for, yet the metros/fao read fetched only the cities' areas and the
    geometry stage then scanned the same country again for the regions. The
    shared read now fetches every seeded division, so the later read is local."""
    dataset = fx.write_area_dataset(
        tmp_path / "a.parquet",
        [
            fx.area("city", _wkb(0, 0, 1, 1), GOOD, country="FI"),
            fx.area("region", _wkb(0, 0, 5, 5), GOOD, country="FI"),
        ],
    )
    scans = {"n": 0}

    class Counting:
        def to_batches(self, **kwargs):
            scans["n"] += 1
            return dataset.to_batches(**kwargs)

    places = [
        {"overture_id": "city", "kind": "city", "country_code": "FI"},
        {"overture_id": "region", "kind": "region", "country_code": "FI"},
    ]
    cache_dir = tmp_path / "cache"
    areas = geometry.place_areas(cache_dir, Counting(), places, {"city"})
    assert set(areas) == {"city"} and scans["n"] == 1
    later = geometry.read_areas(
        Counting(),
        {"region"},
        cache=(cache_dir, overture.OVERTURE_RELEASE),
        countries={"FI"},
    )
    assert set(later) == {"region"} and scans["n"] == 1  # served from the warmed cache


@pytest.mark.parametrize("proxied", [True, False], ids=["proxy", "direct"])
def test_the_conservative_scan_settings_apply_only_behind_a_proxy(monkeypatch, proxied):
    """The single-connection scan settings kept a proxied read alive but were
    applied to every S3 scan, throttling a direct connection to a few rows a
    second; they now follow the proxy the environment names."""
    for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(name, raising=False)
    if proxied:
        monkeypatch.setenv("https_proxy", "http://127.0.0.1:3128")
    expected = geometry.PROXY_SCAN_OPTIONS if proxied else {}
    assert geometry.scan_options() == expected
    assert (overture.proxy_url() is not None) is proxied


def test_the_boundary_lookup_reads_only_the_row_groups_its_cells_touch(tmp_path):
    """Query rectangles used to be merged into their bounding box and scanned
    as one: a chain of stop cells across a continent read every row group
    between them, and the memo's box-level coverage never matched the next
    build's box. Row groups are now chosen by their footer footprints, each
    read once, and coverage is recorded per rectangle."""
    areas = fx.write_area_dataset(
        tmp_path / "a.parquet",
        [
            fx.area("west", _wkb(0, 0, 1, 1), GOOD, country="FI"),
            fx.area("middle", _wkb(5, 5, 6, 6), GOOD, country="FI"),
            fx.area("east", _wkb(10, 10, 11, 11), GOOD, country="FI"),
        ],
        row_group_size=1,
    )
    divisions = fx.write_dataset(
        tmp_path / "d.parquet",
        [
            fx.division(
                name,
                "FI",
                "locality",
                wikidata=f"Q{i}",
                name=name.title(),
                hierarchies=fx.chain((name, "locality", name.title())),
            )
            for i, name in enumerate(("west", "middle", "east"), start=1)
        ],
    )
    reads = []

    class Counting:  # the dataset, its fragments, and their row-group subsets
        def __init__(self, target):
            self._target = target

        def __getattr__(self, name):
            return getattr(self._target, name)

        def get_fragments(self):
            return [Counting(f) for f in self._target.get_fragments()]

        def subset(self, **kw):
            reads.extend(kw["row_group_ids"])
            return self._target.subset(**kw)

    cells = [(0.2, 0.2, 0.8, 0.8), (10.2, 10.2, 10.8, 10.8)]
    with boundaries.BoundaryLookup(
        tmp_path / "cache",
        release="test-release",
        area_dataset=Counting(areas),
        division_dataset=divisions,
    ) as lookup:
        assert lookup.ensure(cells) == 2
        assert sorted(reads) == [0, 2]  # the row group between them: untouched
        assert lookup.ensure(cells) == 0 and len(reads) == 2  # covered per cell
        assert [r["division_id"] for r in lookup.divisions_at(10.5, 10.5)] == ["east"]
        assert lookup.divisions_at(5.5, 5.5) == []


def test_a_crawled_division_the_registry_refuses_is_reported_by_expand(tmp_path):
    """A discovered division whose QID the registry holds under another kind
    (a city that is its own district, registered as a region from its county)
    was dropped with only a log line; the coverage stage then read a stop in it
    as proof of a stale expansion and aborted. The drop is now a ``conflict``
    row of the expansion report."""
    import test_index_expand as ex

    cache = tmp_path / "cache"
    ex._publish_names(cache, ex.SEED_PLACES)
    ex._write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    path = tmp_path / "places_registry.jsonl"
    ex._publish_run(cache, ex._seeded_registry(path))
    with registry.session(path) as reg:
        # Tampere's QID, held as a region before the crawl discovers the city.
        reg.identify(
            {"wikidata": ["Q40840"]},
            kind="region",
            country_code="FI",
            minted_from="t",
            minted_in="t 1",
        )
        manifest, places, report = ex._expand(tmp_path, cache, registry=reg)
    assert manifest["mode"] == "expanded"
    # Dropped: no returned row is the discovered division, under any key.
    assert all(p.get("overture_id") != "fi-tre" for p in places.values())
    (row,) = [r for r in report if r.get("kind") == "conflict"]
    assert row["place_id"] == "Q40840" and row["overture_id"] == "fi-tre"
    assert "not a city" in row["reason"]


def test_a_discovered_district_that_is_also_a_city_is_the_city(tmp_path):
    """Only the district of a city that is its own district carries an area, so
    a stop discovers the district and expand minted it as a region — while the
    seed places the same QID as a city from its name, and the two then refused
    each other's place. A region-kind discovery whose QID a locality also
    carries is now the city; a district no locality shares stays a region."""
    import test_index_expand as ex

    cache = tmp_path / "cache"
    ex._publish_names(cache, ex.SEED_PLACES)
    ex._write_crawl(cache, "f-tku", ["s1,60.45,22.2\n", "s2,60.45,23.2\n"])
    _, places, _ = ex._expand(tmp_path, cache)
    turku = places["Q38511"]
    assert turku["kind"] == "city" and turku["source_subtype"] == "county"
    # The district's area and ancestry are kept: the city sits under its region.
    assert turku["overture_id"] == "fi-tku-county" and turku["parent_id"] == "Q999004"
    assert places["Q999004"]["kind"] == "region"
    assert places["Q999003"]["kind"] == "region"
    assert places["Q999003"]["parent_id"] == "Q999004"
    # Nothing to look up means no scan: a memo-only lookup opens no dataset.
    assert seed.city_qids(None, set()) == set()


def test_the_fetcher_gives_up_on_a_connect_sooner_than_on_a_read():
    """One 60 s timeout covered the handshake too, so a host that never answers
    cost a minute per resolved address per request — six silent minutes for a
    deprecated feed whose host had a private address among its records. The
    connect timeout is now bounded separately; reads keep the full budget."""
    with fetch.Fetcher() as fetcher:
        timeout = fetcher._client.timeout
    assert timeout.connect == fetch.CONNECT_TIMEOUT < fetch.TIMEOUT
    assert timeout.read == timeout.write == timeout.pool == fetch.TIMEOUT
