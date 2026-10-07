"""Regression tests: one per fixed defect, guarding against reappearance.

Fixtures are imported from the stage test modules rather than duplicated.
"""

import hashlib
import http.client
import json
import logging
import math
import os
from pathlib import Path

import httpx
import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
import test_index_boundaries as bt  # noqa: E402
import test_index_coverage as ct  # noqa: E402
import test_index_crawl as crt  # noqa: E402
import test_index_fetch as ft  # noqa: E402
import test_index_publish as pt  # noqa: E402
import test_index_ziprange as zt  # noqa: E402
from transitio_index import (  # noqa: E402
    atlas,
    boundaries,
    builds,
    classify,
    coverage,
    crawl,
    crosswalk,
    csv_source,
    eurostat,
    fetch,
    geometry,
    mdb,
    metros,
    names,
    overture,
    publish,
    publisher,
    registry,
    resolve,
    seed,
    store,
)
from transitio.index import release as contract  # noqa: E402

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
                out[qid] = {"labels": {"en": qid}, "aliases": {}}

    client = Flaky()
    qids = [f"Q{n}" for n in range(1, 11)] + ["Q666"]
    result = client.labels_and_aliases(qids)
    assert "Q666" not in result
    assert set(result) == {f"Q{n}" for n in range(1, 11)}


@pytest.mark.parametrize(
    "code, fatal", [(404, True), (429, True), (502, False)], ids=["4xx", "429", "5xx"]
)
def test_wikidata_labels_treat_a_4xx_as_fatal_and_a_5xx_as_transient(code, fatal):
    """A 4xx during a label batch is our request's fault and stays fatal — not
    bisected and skipped — even though HTTPError is a urllib.error.URLError
    subclass, and so is a 429 that outlasted the retries; a 5xx (a proxy's 502
    on a dropped tunnel included) is the transport's and degrades like one:
    bisected, then the ids skipped."""

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


def _too_many(retry_after):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return overture.urllib.error.HTTPError("u", 429, "too many", headers, None)


@pytest.mark.parametrize(
    "boom, retried, waited",
    [
        (lambda: http.client.RemoteDisconnected("boom"), True, 1),
        (
            lambda: overture.urllib.error.HTTPError("u", 502, "gateway", None, None),
            True,
            1,
        ),
        # A 429 waits its Retry-After, capped; without one, a longer back-off.
        (lambda: _too_many("7"), True, 7.0),
        (lambda: _too_many("3600"), True, overture.RETRY_AFTER_MAX),
        (lambda: _too_many(None), True, 4),
        (lambda: _too_many("NaN"), True, 4),
        (
            lambda: overture.urllib.error.HTTPError("u", 404, "missing", None, None),
            False,
            None,
        ),
    ],
    ids=["disconnect", "5xx", "429", "429-capped", "429-no-header", "429-nan", "4xx"],
)
def test_wikidata_get_json_retries_transport_failures_not_bad_requests(
    monkeypatch, boom, retried, waited
):
    """A dropped Wikidata connection, a 5xx from the server or a gateway, or a
    429 is retried rather than fatal; any other 4xx is our request's fault and
    is raised at once."""
    calls = {"n": 0}
    sleeps = []

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
    monkeypatch.setattr(overture.time, "sleep", sleeps.append)
    client = overture.WikidataClient()
    if retried:
        assert client._get_json("https://example.invalid") == {"ok": 1}
        assert calls["n"] == 2
        assert sleeps == [waited]
    else:
        with pytest.raises(overture.urllib.error.HTTPError):
            client._get_json("https://example.invalid")
        assert calls["n"] == 1
        assert sleeps == []


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
    memo = cache / "boundary_lookup" / boundaries.memo_name("test-release")
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


def test_an_area_read_for_a_recorded_release_uses_that_releases_cache(tmp_path):
    """Expansion redraws metros from the seed's recorded Overture release; the
    shared area read keyed its cache by the pinned release instead, so a cache
    the pinned release had warmed answered for the recorded one."""
    places = [{"overture_id": "city", "kind": "city", "country_code": "FI"}]
    cache_dir = tmp_path / "cache"
    pinned = fx.write_area_dataset(
        tmp_path / "pinned.parquet",
        [fx.area("city", _wkb(0, 0, 1, 1), GOOD, country="FI")],
    )
    recorded = fx.write_area_dataset(
        tmp_path / "recorded.parquet",
        [fx.area("city", _wkb(0, 0, 2, 2), GOOD, country="FI")],
    )
    geometry.place_areas(cache_dir, pinned, places, {"city"})
    areas = geometry.place_areas(
        cache_dir, recorded, places, {"city"}, release="2020-01-01.0"
    )
    assert shapely.area(areas["city"][0]["geom"]) == 4.0


def test_expansion_places_seeded_cities_from_the_recorded_release(monkeypatch):
    """The FAO placement of seeded cities during expansion read their areas
    through the pinned release's cache, not the seed's recorded release."""
    from transitio_index import expand, fao

    calls = []

    def read(*args, **kwargs):
        calls.append(kwargs)
        return {}

    def reopen():
        return None

    monkeypatch.setattr(geometry, "place_areas", read)
    monkeypatch.setattr(fao, "place_cities", lambda *args: ({}, None, None, None))
    places = {"c": {"kind": "city", "overture_id": "a"}}
    seeded = expand._SeededPlacement(
        "cache", None, places, [], None, None, release="2020-01-01.0", reopen=reopen
    )
    assert seeded.cities("region") == []
    assert calls == [{"release": "2020-01-01.0", "reopen": reopen}]


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


def test_sampler_keeps_matching_atlas_feeds_not_files_or_platform_hosts(tmp_path):
    """The sampler kept a whole DMFR file whenever one feed in it matched a kept
    MDB feed by host, and host-matched every feed on a shared platform host:
    the FI+EE sample's Atlas archive held 80 feeds from Remix and gtfs.de for
    a handful of Finnish matches. Only exact matches stay, plus the other feeds
    of a host where at least half of its Atlas feeds match by exact URL.
    """
    import test_sample_catalogues as sct

    sc = sct.sc
    hsl = "https://hsl.fi/gtfs.zip"
    remix_fi = "https://eu-gtfs.remix.com/fi-tku.zip"
    local_a, local_b = "https://local.fi/a.zip", "https://local.fi/b.zip"
    mdb_rows = [
        {"id": "1", sc.MDB_DOWNLOAD: hsl},
        {"id": "2", sc.MDB_DOWNLOAD: remix_fi},
        {"id": "3", sc.MDB_DOWNLOAD: local_a},
        {"id": "4", sc.MDB_DOWNLOAD: local_b},
        {"id": "5", sc.MDB_DOWNLOAD: "https://rt.example/mdb-only.zip"},
    ]
    urls, hosts = sc._mdb_targets(mdb_rows)
    assert {"hsl.fi", "eu-gtfs.remix.com", "local.fi", "rt.example"} <= hosts

    def feed(feed_id, url, **more):
        return {"id": feed_id, "spec": "gtfs", "urls": {"static_current": url, **more}}

    remix = {
        "feeds": [feed("f-fi-tku", remix_fi)]
        + [
            feed(f"f-remix-{i}", f"https://eu-gtfs.remix.com/x{i}.zip")
            for i in range(4)
        ]
    }
    local = {
        "feeds": [feed("f-a", local_a), feed("f-b", local_b)]
        + [feed("f-c", "https://local.fi/c.zip")]
    }
    # Only the static URL carries identity: the HSL feed's exact match must not
    # credit the host of its realtime URL, and a realtime URL on a host that
    # passes the threshold (local.fi) must not pull its feed in.
    hsl_file = {
        "feeds": [
            feed("f-hsl", hsl, realtime_vehicle_positions="https://rt.example/vp"),
            feed("f-other", "https://other.fi/x"),
            feed("f-rt", "https://rt.example/other.zip"),
            feed(
                "f-rt-local", "https://elsewhere.fi/x", realtime="https://local.fi/rt"
            ),
        ]
    }
    archive = tmp_path / "atlas.tar.gz"
    sct._archive(
        archive,
        [
            ("r/feeds/remix.dmfr.json", remix),
            ("r/feeds/local.dmfr.json", local),
            ("r/feeds/hsl.dmfr.json", hsl_file),
        ],
    )
    kept = {
        source: [f["id"] for f in payload["feeds"]]
        for source, payload in sc._select_atlas(archive, urls, hosts)
    }
    # Remix: 1 of 5 feeds matches exactly -> only that feed; local.fi: 2 of 3 ->
    # the third comes along; the HSL file loses its unrelated feeds, and
    # rt.example (0 of 2 exact on that host) is not host-matched.
    assert kept == {
        "remix.dmfr.json": ["f-fi-tku"],
        "local.dmfr.json": ["f-a", "f-b", "f-c"],
        "hsl.dmfr.json": ["f-hsl"],
    }


def test_an_http_atlas_url_is_sampled_and_paired_with_its_https_mdb_twin(tmp_path):
    """STM's Atlas feed lists its download as ``http://`` and its MDB row as
    ``https://``. The sampler kept an Atlas feed only on a byte-identical URL,
    so the twins were cut into different samples (ca-full and atlas3), and the
    crosswalk's url-exact match compared full strings and would not pair them.
    """
    import test_index_crosswalk as cwt
    import test_sample_catalogues as sct

    sc = sct.sc
    path = "stm.example/gtfs/gtfs_stm.zip"
    src = tmp_path / "feeds_v2.csv"
    src.write_text(
        "id,data_type,location.country_code,urls.direct_download\n"
        f"mdb-1,gtfs,CA,https://{path}\n",
        encoding="utf-8",
    )
    archive = tmp_path / "atlas.tar.gz"
    sct._archive(
        archive, [("r/feeds/stm.dmfr.json", sct._dmfr("f-stm", f"http://{path}"))]
    )

    _, mdb_rows = sc._select_csv(
        src, sc.MDB_COUNTRY, (sc.MDB_COUNTRY, sc.MDB_DOWNLOAD), {"CA"}
    )
    kept = sc._select_atlas(archive, *sc._mdb_targets(mdb_rows))
    # The CA cut keeps the Atlas feed.
    assert [feed["id"] for _, payload in kept for feed in payload["feeds"]] == ["f-stm"]

    sample = tmp_path / "atlas_sample.tar.gz"
    sc._write_atlas(sample, kept)
    records, _ = crosswalk.build_records(
        atlas.parse(sample)["feeds"],
        [cwt.mdb_feed("mdb-1", url=mdb_rows[0][sc.MDB_DOWNLOAD])],
    )
    assert [(r["source"], r["feed_id"], r["mdb_id"]) for r in records] == [
        ("both", "f-stm", "mdb-1")
    ]


def test_a_partition_cut_places_every_static_feed_in_exactly_one_label(
    tmp_path, monkeypatch
):
    """The rebuild's label map was assembled from country cuts: Japan's was
    capped at 80 rows, some countries, rows without a country and misfiled
    rows had no label, and the Atlas-only cut assumed every Atlas feed sharing
    a URL with an MDB row was reached. 574 MDB and 385 Atlas GTFS feeds of
    the 2026-10-02 catalogues were in no cut. The partition cuts every row and
    feed into exactly one label, keeping each fold in one label.
    """
    import test_sample_catalogues as sct

    sc = sct.sc
    shared = "https://shared.example/g.zip"
    rows = [
        sct._mdb_gtfs("j2", "JP", "https://t.jp/2.zip", "Tokyo"),
        sct._mdb_gtfs("j4", "JP", shared, "Tokyo"),
        sct._mdb_gtfs("j1", "JP", "https://a.jp/1.zip", "Aichi"),
        sct._mdb_gtfs("j3", "JP", "https://t.jp/3.zip", "Tokyo", redirect="j4"),
        sct._mdb_gtfs("f1", "FI", "https://f.fi/1.zip", redirect="s1"),
        sct._mdb_gtfs("f2", "FI", "https://f.fi/2.zip", redirect="s1"),
        sct._mdb_gtfs("s1", "SE", shared),
        sct._mdb_gtfs("b1", "", "https://a.jp/2.zip"),
        sct._mdb_gtfs("x1", "SA", "https://x.tr/1.zip"),
        sct._mdb_gtfs("n1", "ß", "https://n.example/1.zip"),  # not "SS"
    ]
    files = [
        (
            "r/feeds/a.jp.dmfr.json",
            sct._payload(
                sct._feed("f-j1", "https://a.jp/1.zip"),
                sct._feed("f-a2", "https://a.jp/2.zip"),
            ),
        ),
        ("r/feeds/shared.dmfr.json", sct._payload(sct._feed("f-shared", shared))),
        (
            "r/feeds/x.dmfr.json",
            sct._payload(
                sct._feed("f-lonely", "https://lonely.example/g.zip"),
                sct._feed("f-nourl", None),
                sct._feed("f-rt", "https://t.jp/2.zip", spec="gtfs-rt"),
            ),
        ),
    ]
    sct._partition_inputs(tmp_path, monkeypatch, rows, files)
    out = tmp_path / "out"
    argv = ["--partition", "--out-dir", str(out), "--batch-size", "2"]
    sc.main(argv + ["--exclude", "x1", "--country", "KE", "--country", "JP"])

    (published,) = out.glob("partition-*")
    labels = ["atlas1", "cities", "jp1", "jp2", "other", "se"]
    lines = (published / "rebuild_map.txt").read_text().splitlines()
    cuts = {label: Path(cut) for label, cut in (line.split(" ", 1) for line in lines)}
    assert list(cuts) == labels
    assert all(cuts[label].parent == out for label in labels)
    assert all(cuts[label].name.startswith(f"run-{label}_") for label in labels)
    manifest = json.loads((published / "partition.json").read_text())
    # JP splits by subdivision (Aichi first) with the fold j3 -> j4 whole. The
    # FI rows fold into their SE successor: one unit of three rows, so one
    # label whatever --batch-size. The excluded and blank rows go to other.
    want = {"jp1": "j1 j2", "jp2": "j4 j3", "se": "f1 f2 s1", "other": "b1 x1 n1"}
    assert manifest["mdb"] == {i: lb for lb, ids in want.items() for i in ids.split()}
    # A feed two labels carry exactly goes to the first; an exact match outranks
    # an earlier host match (f-a2); a realtime feed gets no label. Each country
    # is owned by one label (countries.txt): its own, part 1 of a split, or
    # cities, which holds no feeds.
    held = {}
    for label, cut in cuts.items():
        feeds = atlas.parse(cut / "atlas_sample.tar.gz")["feeds"]
        owned = (cut / "countries.txt").read_text().split()
        held[label] = (owned, sorted(feed["onestop_id"] for feed in feeds))
    assert held == {
        "atlas1": ([], ["f-lonely", "f-nourl"]),
        "cities": (["KE"], []),
        "jp1": (["JP"], ["f-j1"]),
        "jp2": ([], ["f-shared"]),
        "other": ([], ["f-a2"]),
        "se": (["SE"], []),
    }
    assert manifest["atlas"] == {f: lb for lb, (_, ids) in held.items() for f in ids}
    assert len((cuts["cities"] / "mdb_sample.csv").read_text().splitlines()) == 1
    digest = hashlib.sha256((cuts["jp1"] / "mdb_sample.csv").read_bytes())
    assert manifest["labels"]["jp1"]["mdb_sha256"] == digest.hexdigest()


def test_stats_keys_gbfs_systems_sharing_id_and_country_by_ordinal():
    """The ES build's stats stage refused two GBFS systems both listed as
    ``seville`` in ES (Cooltra and Sevici): the crosswalk skips both as
    ambiguous, and the catalogue rows must still carry distinct keys.
    """
    from transitio_index import stats

    def system(name, url):
        return {
            "source": "gbfs",
            "system_id": "seville",
            "spec": "gbfs",
            "country_code": "ES",
            "location": "Seville",
            "name": name,
            "requires_auth": False,
            "auto_discovery_url": url,
        }

    rows = stats.catalogue_rows(
        {
            "gbfs": [
                system("Cooltra", "https://a/gbfs.json"),
                system("Sevici", "https://b/gbfs.json"),
            ]
        },
        [],
    )
    assert [r["source_id"] for r in rows] == ["seville/ES#1", "seville/ES#2"]
    assert {r["drop_reason"] for r in rows} == {"ambiguous_id"}
    assert stats.identity(rows, [])["gbfs_duplicate_system_ids"] == 1


# The GTFS route types: the basic set and the extended set (families by
# hundred, sub-types as enumerated by the extended route types reference).
GTFS_BASIC_ROUTE_TYPES = (0, 1, 2, 3, 4, 5, 6, 7, 11, 12)
GTFS_EXTENDED_ROUTE_TYPES = (
    *range(100, 118),
    *range(200, 210),
    300,
    *range(400, 406),
    500,
    600,
    *range(700, 717),
    800,
    *range(900, 907),
    *range(1000, 1022),
    *range(1100, 1115),
    1200,
    *range(1300, 1308),
    1400,
    *range(1500, 1508),
    *range(1600, 1605),
    *range(1700, 1703),
)


def test_every_gtfs_route_type_decides_a_tier_or_is_out_of_scope_by_design():
    """A route type the decision table did not list fell through to
    ``unknown`` silently: funiculars, trolleybuses, ferries and monorails
    did, in 271 edges over ten archived builds. Every enumerated type now
    decides a tier given full signals, or is unclassifiable by design (rule
    10) and lies in the table's out-of-scope ranges."""
    for route_type in (*GTFS_BASIC_ROUTE_TYPES, *GTFS_EXTENDED_ROUTE_TYPES):
        decision = classify.classify_route(
            route_type, {"AA": frozenset({"s"})}, 10.0, 1.0
        )
        out_of_scope = any(
            low <= route_type <= high for low, high in classify.UNCLASSIFIABLE_RANGES
        )
        if out_of_scope:
            assert (decision["tier"], decision["rule"]) == ("unknown", 10), route_type
        else:
            assert decision["tier"] != "unknown", route_type
            assert decision["rule"] != 10, route_type


@pytest.mark.parametrize(
    ("routes", "status"),
    [
        (b"route_id,route_type\nbus,3\n", "no_service"),
        (b"route_id,route_type\n", "no_routes"),
    ],
    ids=["routes-without-trips", "no-routes"],
)
def test_a_complete_crawl_without_a_schedule_keeps_its_edges_and_home(
    tmp_path, routes, status
):
    """Five Finnish ELY-centre feeds ship stops and routes with header-only
    trips and stop_times; complete mode dropped every candidate as unserved
    and left no country evidence, so Finnish feeds landed in international/.
    Such a feed keeps explicit unknown edges and counts its located stops."""
    from test_index_classify import LOOKUP, _candidate, _coverage, _write_crawl

    cache = tmp_path / "cache"
    feeds = [
        {
            "feed_id": "f-shell",
            "spec": "gtfs",
            "coverage_source": "crawl",
            "aliases": [],
        }
    ]
    _write_crawl(
        cache,
        "f-shell",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n",
            "routes.txt": routes,
            "trips.txt": b"trip_id,route_id\n",
            "stop_times.txt": b"trip_id,stop_id,stop_sequence\n",
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-shell", 2)])
    manifest = classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(
        cache / "classify", "edges.json", classify.EDGES_ARTIFACT
    )
    (edge,) = edges
    assert edge["tier"] == "unknown" and edge["needs_review"] is True
    assert edge["evidence"]["unknown_reason"] == status
    records, _ = store.read_jsonl(
        cache / "classify", "edges.json", classify.FEEDS_ARTIFACT
    )
    (feed,) = records
    assert feed["country_stops"] == {"AA": 2} and feed["country_basis"] == "located"
    assert feed["scope"] == "domestic" and feed["home_country"] == "AA"
    assert manifest["feeds_by_status"] == {status: 1}
    assert manifest["edges_dropped_no_serving_route"] == 0


@pytest.mark.parametrize(
    ("abroad", "tier"), [(1, "local"), (3, "international")], ids=["one-stop", "three"]
)
def test_one_stop_across_a_boundary_does_not_make_a_route_international(
    tmp_path, abroad, tier
):
    """Rule 1 fired on any second country among a route's stops, so a stop on
    the wrong side of a simplified boundary, or on an overlap sliver resolving
    to two countries, made a city route international at 0.95 and hid its
    scale. The gate needs two minority stops and a tenth of the route, the
    scale is recorded either way, and the artefacts are counted per feed."""
    from test_index_classify import LOOKUP, _candidate, _coverage, _write_crawl

    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-b", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    stops = [(f"h{i}", 1.0 + i / 1000, 10.0) for i in range(17)]  # AA, in Q-city
    stops += [(f"b{i}", 1.0, 60.0) for i in range(abroad)]  # BB only
    stops += [("sliver", 1.0, 65.0), ("nowhere", 1.0, 30.0)]  # AA+BB; no country
    stops_txt = "stop_id,stop_lat,stop_lon\n" + "".join(
        f"{s},{lat},{lon}\n" for s, lat, lon in stops
    )
    times = "trip_id,stop_id,stop_sequence\n" + "".join(
        f"t,{s},{i}\n" for i, (s, _, _) in enumerate(stops, 1)
    )
    _write_crawl(
        cache,
        "f-b",
        {
            "stops.txt": stops_txt.encode(),
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": b"trip_id,route_id\nt,tram\n",
            "stop_times.txt": times.encode(),
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-b", 17)])
    manifest = classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(
        cache / "classify", "edges.json", classify.EDGES_ARTIFACT
    )
    (edge,) = edges
    assert edge["tier"] == tier and edge["evidence"]["scale_tiers"] == ["local"]
    assert edge["evidence"].get("border_stops") == ({"BB": 1} if abroad == 1 else None)
    records, _ = store.read_jsonl(
        cache / "classify", "edges.json", classify.FEEDS_ARTIFACT
    )
    (feed,) = records
    assert feed["stops_in_several_countries"] == 1
    assert feed["stops_without_country"] == 1
    assert feed["border_stops"] == abroad and feed["home_country"] == "AA"
    assert manifest["stop_artefacts"] == {
        "stops_in_several_countries": 1,
        "stops_without_country": 1,
        "border_stops": abroad,
    }


def test_a_feeds_own_crawl_outranks_one_filed_under_its_alias(tmp_path):
    """A renamed feed's old crawl maps to the feed through its alias; it must
    not stand in for the feed's own crawl however the log orders them."""
    cache = tmp_path / "cache"
    ct._write_crawl(cache, "f-new", ct._rows(2, 10.0))
    ct._write_crawl(cache, "f-old", ct._rows(3, 20.0))
    states, unmatched = coverage.crawled_states(
        cache, [ct._feed("f-new", aliases=["f-old"])]
    )
    assert states["f-new"][1]["feed_id"] == "f-new" and not unmatched


@pytest.mark.parametrize("version", [10, 11])
def test_a_flat_build_claiming_schema_10_needs_its_columns(tmp_path, version):
    """The schema's required columns were checked on partitioned builds only,
    so a flat manifest claiming schema 10 loaded without contained_in."""
    import json

    from builds_fixture import write_build

    from transitio_index import builds

    path = tmp_path / "index"
    write_build(path)
    assert builds.load_tables(path) is not None
    snapshot = json.loads((path / "snapshot.json").read_text())
    snapshot["schema_version"] = version
    (path / "snapshot.json").write_text(json.dumps(snapshot))
    assert builds.load_tables(path) is None
    # Whatever its columns: a schema-11 build ships its providers at the root.
    assert (builds.snapshot_files(snapshot) is None) == (version == 11)


def test_a_dropped_download_resumes_from_the_bytes_that_arrived(tmp_path):
    """A connection cut mid-body keeps what arrived and resumes from it, and a
    cut that added bytes is not a failed attempt: a server that drops every
    connection after a slice still delivers the whole file, each byte once."""
    body, cut = ft.BODY, 4096
    ranges = []

    def handler(request):
        ranges.append(request.headers.get("Range"))
        start = int(ranges[-1][6:-1]) if ranges[-1] else 0
        rest = body[start:]
        headers = {"Content-Length": str(len(rest)), "ETag": '"v1"'}
        if start:
            headers["Content-Range"] = f"bytes {start}-{len(body) - 1}/{len(body)}"
        stream = (
            ft._FailingStream(rest, cut) if len(rest) > cut else ft._RawStream(rest)
        )
        return httpx.Response(206 if start else 200, headers=headers, stream=stream)

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with ft._fetcher(httpx.MockTransport(handler)) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
            assert fetcher.bytes_fetched == len(body)
    finally:
        directory.close()
    assert ranges == [None] + [f"bytes={n}-" for n in range(cut, len(body), cut)]
    assert len(ranges) > fetch.DOWNLOAD_ATTEMPTS
    assert result["sha256"] == hashlib.sha256(body).hexdigest()
    assert (tmp_path / "crawl" / "feed.zip").read_bytes() == body


def test_a_failed_refetch_keeping_its_crawl_gets_that_crawl_identity(tmp_path):
    """A feed whose re-fetch fails keeps its previous crawl for coverage; a
    crawl from before identities were recorded must get one then too, or the
    feed can never fold with its copies."""
    cache = tmp_path / "cache"
    crt._publish_resolved(cache, [crt._feed("f-a", "https://feeds.example/a.zip")])
    crt._crawl(cache, crt._server({"/a.zip": (crt._zip_bytes(), '"v1"')}))
    path = crt._feed_dir(cache, "f-a") / "state.json"
    state = json.loads(path.read_text())
    identity = state.pop("identity")
    del state["identity_version"]
    path.write_text(json.dumps(state))
    _, log = crt._crawl(cache, crt._server({}))
    assert log["f-a"]["method"] == "failed"
    assert json.loads(path.read_text())["identity"] == identity


def test_the_merge_rescores_relevance_over_the_merged_edges(tmp_path):
    """The merge kept each edge's relevance from the build that won its
    feed, so a feed alone at Paris in one build kept 0.7 for its place share
    and outranked Paris's main feed from another build."""
    import json

    from builds_fixture import BOX, _feed7, _place, _run

    from transitio_index import builds, merge, rank

    def edge(place, feed, tier, stops, departures, evidence, relevance, category=None):
        service = {"stops": stops}
        if departures is not None:
            service["departures_per_day"] = departures
        return {
            "place_id": place,
            "feed_id": feed,
            "tier": tier,
            "service": json.dumps(service),
            "evidence": json.dumps(evidence),
            "needs_review": False,
            "relevance_category": category or rank.CATEGORY_BY_TIER[tier],
            "relevance": relevance,
            "cross_border": False,
        }

    places = [
        _place(*row, country="FR")
        for row in (
            ("fr", "country", "France", None, BOX(-5, 42, 8, 51)),
            ("ara", "region", "Rhône", "fr", BOX(4, 44, 7, 46.5)),
            ("par", "city", "Paris", "fr", BOX(2.2, 48.8, 2.5, 48.9)),
            ("mrs", "city", "Marseille", "fr", BOX(5.3, 43.2, 5.5, 43.4)),
            ("lyo", "city", "Lyon", "ara", BOX(4.8, 45.7, 4.9, 45.8)),
        )
    ]
    older = [
        ("par", "x", "local", 4, 4.5, {"share_of_feed": 0.0002}, 0.7 + 0.3 * 0.0002),
        # A place filed under another kind in this build.
        ("mrs", "x", "local", 5, None, {"breadth": 1.0}, 1.0),
        ("fr", "x", "national", 9, 4.5, {"breadth": 1.0}, 1.0),
    ]
    stale = {"share_of_feed": 0.9, "stale_when_indexed": "2026-01-31"}
    newer = [
        ("par", "y", "local", 400, 15614, {"share_of_feed": 0.5}, 0.85),
        ("mrs", "y", "local", 20, 100, {"share_of_feed": 0.05}, 0.715),
        ("lyo", "z", "local", 30, 50, {"share_of_feed": 0.4}, 0.82),
        ("ara", "s", "regional", 3, 2, stale, 0.0),
        ("ara", "z", "unknown", 1, 1, {"share_of_feed": 0.1}, 0.3, "primary"),
    ]
    sources = []
    for label, digit, feeds, rows, day in (
        ("atlas1", 1, ["x"], older, 10),
        ("mdb", 2, ["y", "z", "s"], newer, 15),
    ):
        run = _run(
            tmp_path,
            label,
            digit,
            places=places,
            feeds=[_feed7(f, f, "FR", "domestic") for f in feeds],
            edges={"FR": [edge(*row) for row in rows]},
            built_at=f"2026-09-{day}T00:00:00+00:00",
        )
        snapshot, _, tables = builds.load_tables(tmp_path / run / "index")
        sources.append((run, snapshot, tables))
    snapshot, tables = merge.merge_tables(sources)
    edges = {
        (e["place_id"], e["feed_id"]): {**e, "evidence": json.loads(e["evidence"])}
        for e in tables["edges.parquet"].to_pylist()
    }
    paris = 4.5 + 15614
    assert edges["par", "x"]["relevance"] == pytest.approx(
        0.7 * 4.5 / paris + 0.3 * 0.0002
    )
    assert edges["par", "y"]["relevance"] == pytest.approx(
        0.7 * 15614 / paris + 0.3 * 0.5
    )
    # One pair without departures puts the whole place on stops.
    for feed, share in (("x", 5 / 25), ("y", 20 / 25)):
        assert edges["mrs", feed]["evidence"]["share_basis"] == "stops"
        assert edges["mrs", feed]["evidence"]["share_of_place"] == pytest.approx(share)
    assert edges["mrs", "x"]["relevance"] == pytest.approx(0.7 * 5 / 25)
    assert edges["mrs", "x"]["evidence"]["relevance_note"] == "no_share_of_feed"
    assert edges["lyo", "z"]["relevance"] == pytest.approx(0.82)
    assert edges["fr", "x"]["evidence"]["breadth"] == pytest.approx(2 / 3)
    assert edges["fr", "x"]["relevance"] == pytest.approx(0.7 + 0.3 * 2 / 3)
    assert edges["ara", "s"]["relevance"] == 0.0
    assert edges["ara", "s"]["evidence"]["stale_when_indexed"] == "2026-01-31"
    unknown = edges["ara", "z"]
    assert (unknown["relevance_category"], unknown["relevance"]) == ("unknown", 0.0)
    assert unknown["needs_review"] is True
    assert snapshot["relevance"] == {
        "edges": 8,
        "changed": 6,
        "share_basis_by_place": {"departures": 4, "stops": 1},
        "no_share_of_feed": 1,
    }


def test_departures_per_day_average_over_the_days_the_timetable_covers(tmp_path):
    """PID's calendar runs a year while its full timetable covers two weeks,
    so dividing by the whole calendar put Prague at 28,270 departures a day
    against about 690,000 on a weekday."""
    from test_index_classify import LOOKUP, _candidate, _coverage, _write_crawl

    cache = tmp_path / "cache"
    feeds = [
        {"feed_id": "f-pid", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    trips = {"t1": "two", "t2": "two", "t3": "two", "t4": "two", "ty": "year"}
    _write_crawl(
        cache,
        "f-pid",
        {
            "stops.txt": b"stop_id,stop_lat,stop_lon\ns1,1.0,10.0\ns2,1.0,10.01\n",
            "routes.txt": b"route_id,route_type\ntram,0\n",
            "trips.txt": (
                "trip_id,route_id,service_id\n"
                + "".join(f"{t},tram,{s}\n" for t, s in trips.items())
            ).encode(),
            "calendar.txt": (
                b"service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
                b"sunday,start_date,end_date\n"
                b"two,1,1,1,1,1,1,1,20261003,20261016\n"
                b"year,1,1,1,1,1,1,1,20251213,20261212\n"
            ),
            "stop_times.txt": (
                "trip_id,stop_id,stop_sequence\n"
                + "".join(f"{t},s1,1\n{t},s2,2\n" for t in trips)
            ).encode(),
        },
        "complete",
    )
    _coverage(cache, feeds, [_candidate("Q-city", "f-pid", 2)])
    classify.classify(cache, lookup=LOOKUP)
    edges, _ = store.read_jsonl(
        cache / "classify", "edges.json", classify.EDGES_ARTIFACT
    )
    (edge,) = edges
    # Five trips a day over the fourteen days, two stop-events each.
    assert edge["service"]["departures_per_day"] == pytest.approx(10.0)


def test_an_expired_feed_leaves_the_place_shares_and_service():
    """DPP's Prague feed ended in 2023 yet held 0.915 of Prague's departures
    against PID's current timetable, and was summed into Prague's service."""
    from transitio_index import rank

    def edge(place, feed, departures, **evidence):
        return {
            "place_id": place,
            "feed_id": feed,
            "tier": "local",
            "service": {"stops": 10, "routes": 1, "departures_per_day": departures},
            "evidence": {"share_of_feed": 0.5, **evidence},
        }

    ended = {"stale_when_indexed": "2023-11-06"}
    edges = [
        edge("prg", "pid", 600_000.0),
        edge("prg", "dpp", 500_000.0, **ended),
        edge("brn", "dpp", None, **ended),
    ]
    places = {p: {"kind": "city", "country_code": "CZ"} for p in ("prg", "brn")}
    scored, basis, _ = rank.score_edges(edges, places)
    evidence = {(e["place_id"], e["feed_id"]): e["evidence"] for e in scored}
    assert evidence["prg", "pid"]["share_of_place"] == 1.0
    assert evidence["prg", "dpp"]["share_of_place"] == 0.0
    # A place only stale feeds serve keeps their basis and has no service.
    assert basis == {"prg": "departures", "brn": "stops"}
    assert publish._service_by_place(edges) == {
        "prg": {
            "feeds": 1,
            "stops": 10,
            "routes": 1,
            "departures_per_day": 600_000.0,
        }
    }


def test_a_merged_places_service_and_validity_sum_every_builds_feeds(tmp_path):
    """The merge kept a place's service and validity from the build serving
    it most, so Prague stored 16 feeds and 822 departures a day against 42
    feeds over the merged edges."""
    from builds_fixture import BOX, _feed7, _place, _run

    from transitio_index import merge

    def edge(feed, stops, departures, **evidence):
        service = {"stops": stops, "routes": 1, "departures_per_day": departures}
        return {
            "place_id": "prg",
            "feed_id": feed,
            "tier": "local",
            "service": json.dumps(service),
            "evidence": json.dumps({"share_of_feed": 1.0, **evidence}),
            "needs_review": False,
            "relevance_category": "primary",
            "relevance": 1.0,
            "cross_border": False,
        }

    places = [
        _place(p, "city", p, None, BOX(14, 49, 17, 50), "CZ", validity="{}")
        for p in ("prg", "brn")
    ]
    ended = {"stale_when_indexed": "2023-11-06"}
    sources = []
    for label, digit, feeds, edges in (
        ("cz", 1, [("pid", "2026-10-01", "2026-12-12")], [edge("pid", 8000, 6e5)]),
        (
            "de",
            2,
            [
                ("a", "2026-09-01", "2026-12-31"),
                ("b", "2026-10-01", "2026-11-30"),
                ("dpp", "2023-01-01", "2023-11-06"),
            ],
            [edge("a", 50, 800.0), edge("b", 23, 22.0), edge("dpp", 400, 5e5, **ended)],
        ),
    ):
        run = _run(
            tmp_path,
            label,
            digit,
            places=places,
            feeds=[_feed7(f, f, "CZ", "domestic", start=s, end=e) for f, s, e in feeds],
            edges={"CZ": edges},
            built_at=f"2026-09-1{digit}T00:00:00+00:00",
        )
        snapshot, _, tables = builds.load_tables(tmp_path / run / "index")
        sources.append((run, snapshot, tables))
    _, tables = merge.merge_tables(sources)
    rows = {p["place_id"]: p for p in tables["places.parquet"].to_pylist()}
    assert rows["prg"]["build_id"] == sources[1][0]
    # The stale feed stays out of the service and in the validity.
    assert json.loads(rows["prg"]["service"]) == {
        "feeds": 3,
        "stops": 8073,
        "routes": 3,
        "departures_per_day": 600822.0,
    }
    validity = json.loads(rows["prg"]["validity"])
    assert (validity["feeds_dated"], validity["start"], validity["end"]) == (
        4,
        "2023-01-01",
        "2026-12-31",
    )
    assert rows["brn"]["service"] is None and rows["brn"]["validity"] is None


def _padded(text, width=150):
    """Every line of ``text`` padded with trailing spaces, as Renfe writes."""
    return "".join(line + " " * width + "\n" for line in text.splitlines()).encode()


_CALENDAR = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\ns1,1,1,1,1,1,1,1,20260929,20261011\n"
)
_CALENDAR_DATES = "service_id,date,exception_type\ns1,20261001,2\n"
_ROUTES = "route_id,agency_id,route_type\nr1,a,3\nr2,a,0\n"
_STOPS = "stop_id,stop_lat,stop_lon\ns1,40.4,-3.7\ns2,40.5,-3.6\n"


@pytest.mark.parametrize(
    "read, members",
    [
        pytest.param(
            lambda calendar, dates: classify._read_calendar(calendar, dates),
            (_CALENDAR, _CALENDAR_DATES),
            id="calendar",
        ),
        pytest.param(classify._read_routes, (_ROUTES,), id="routes"),
        pytest.param(crawl.stop_rows, (_STOPS,), id="stops"),
    ],
)
def test_a_padded_member_reads_as_the_clean_one(read, members):
    """Renfe pads every line, headers included, so the last column's
    name carried the padding and every calendar row lost its end_date."""
    import io

    clean = read(*(io.BytesIO(m.encode()) for m in members))
    assert read(*(io.BytesIO(_padded(m)) for m in members)) == clean
    if members[0] == _CALENDAR:
        weights = classify._calendar_weights(clean, {"t1": "s1"})
        assert weights == {"s1": pytest.approx(12 / 13)}
        assert [d.isoformat() for d in clean.span] == ["2026-09-29", "2026-10-11"]


def test_a_feed_with_spaced_header_names_keeps_its_fingerprint():
    """Metra writes a space after each comma of its header rows, so no
    stop coordinate parsed and the feed had no edge."""
    import io

    from transitio.index import fingerprint

    trips = "route_id,service_id,trip_id\nr1,s1,t1\nr2,s1,t2\n"
    stop_times = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "t1,08:00:00,08:00:00,s1,1\nt1,08:10:00,08:10:00,s2,2\n"
        "t2,09:00:00,09:00:00,s2,1\nt2,09:05:00,09:05:00,s1,2\n"
    )

    def digest(spaced):
        def member(text):
            header, _, rest = text.partition("\n")
            return io.BytesIO(
                (
                    (header.replace(",", ", ") if spaced else header) + "\n" + rest
                ).encode()
            )

        routes, _ = classify._read_routes(member(_ROUTES))
        rows, _ = crawl.stop_rows(member(_STOPS))
        coords = {stop_id: (x, y) for stop_id, x, y in rows}
        trip_routes, trip_services, _ = classify._read_trips(member(trips), routes)
        stops = classify._read_stop_times(
            member(stop_times), trip_routes, trip_services
        )[0]
        return fingerprint.compute("route_stops", routes, coords, stops)

    assert digest(spaced=True) == digest(spaced=False)


def test_a_streamed_archive_is_read_through_ranges(tmp_path):
    """A zip written to a stream flags every member with a data descriptor and
    leaves each local CRC and size zero, as a Mobility Database hosted copy
    does; the range reader refused such an archive, so the crawl downloaded
    it whole."""
    cache = tmp_path / "cache"
    data = zt._zip_bytes(crt.FULL_MEMBERS, streamed=True)
    crt._publish_resolved(cache, [crt._feed("f-a", "https://feeds.example/a.zip")])
    _, log = crt._crawl(
        cache, crt._server({"/a.zip": (data, '"v1"')}), range_threshold=1
    )
    assert log["f-a"]["method"] == "range"
    assert log["f-a"]["fallback_reason"] is None
    stop_times = crt._feed_dir(cache, "f-a") / "stop_times.txt"
    assert stop_times.read_bytes() == crt.STOP_TIMES


LINZ_CREDIT = (
    "  - Land Information New Zealand (LINZ) — CC BY 4.0 "
    "(https://creativecommons.org/licenses/by/4.0/)"
)


def test_a_linz_area_ships_and_is_credited(tmp_path):
    """Overture's New Zealand localities carry the LINZ source under CC BY
    4.0, which was not allowlisted: their boundaries were omitted and the
    licence stage drew each from its feeds' hulls."""
    import test_index_geometry as gt

    cache = tmp_path / "cache"
    gt._publish(
        cache, [gt._place("Q37100", "city", overture_id="nz-akl", country="NZ")]
    )
    linz = [{"dataset": "Linz", "license": "CC-BY-4.0", "record_id": "L"}]
    dataset = fx.write_area_dataset(
        tmp_path / "areas.parquet", [fx.area("nz-akl", gt.BOX, linz)]
    )
    geometry.attach_geometry(cache, dataset=dataset)
    (place,), _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "places_seed.jsonl"
    )
    assert place["geometry_source"] == "overture"
    assert LINZ_CREDIT + "\n" in gt._read_text(cache, "NOTICE")
    inventory, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "licence_inventory.jsonl"
    )
    assert [
        (row["dataset"], row["license"], row["allowed"])
        for row in inventory
        if row["role"] == "component"
    ] == [("Linz", "CC-BY-4.0", True)]


def test_a_source_only_expand_shipped_is_credited(tmp_path):
    """The expand stage shipped boundaries without recording their sources,
    and the NOTICE was the geometry stage's, written over the seeded places
    only: a source that shipped only through expand went uncredited."""
    import test_index_license as lt

    from transitio_index import licensing, merge

    component = {
        "role": "component",
        "use": "geometry",
        "dataset": "Linz",
        "license": "CC-BY-4.0",
        "url": "https://creativecommons.org/licenses/by/4.0/",
        "version": "2026-08-19.0",
        "allowed": True,
        "geometries": 1,
    }
    cache = lt._cache(
        tmp_path,
        expanded={
            "licence_sources": ["Linz|CC-BY-4.0"],
            "licence_inventory": [component],
        },
    )
    licensing.license_index(cache)
    generation, _ = store.resolve(cache / "license", "licensed.json")
    with generation:
        notice = generation.read_bytes("NOTICE")
    assert LINZ_CREDIT in merge._notice_sections(notice, "b")["geometry"]
    inventory, _ = store.read_jsonl(
        cache / "license", "licensed.json", "licence_inventory.jsonl"
    )
    assert {**component, "stage": "expand"} in inventory


@pytest.mark.parametrize(
    ("expanded", "error"),
    [
        # Expanded before the stage recorded what its boundaries shipped.
        ({"mode": "expanded"}, "rerun the expand stage"),
        # A shipped source the allowlist has since dropped.
        (
            {
                "mode": "expanded",
                "licence_sources": ["Retired|X-1.0"],
                "licence_inventory": [],
            },
            "no longer allowlisted",
        ),
    ],
    ids=["unaudited", "dropped"],
)
def test_an_expand_audit_the_licence_stage_cannot_credit_is_refused(expanded, error):
    """Expanded places whose shipped sources cannot be credited are refused,
    not licensed with the geometry stage's credit alone."""
    from transitio_index import licensing

    with pytest.raises(licensing.LicenseError, match=error):
        licensing._geometry_notice("", [], {}, expanded, "2026-08-19.0")


@pytest.mark.parametrize(
    ("stale", "joined"),
    [(False, False), (True, False), (False, True)],
    ids=["discovered", "stale", "joined"],
)
def test_set_boundary_applies_to_a_place_expand_discovers(tmp_path, stale, joined):
    """The geometry stage applies ``set_boundary`` to the seeded places it
    can key, and expand applied none: a place first found by expand — every
    place of a build that seeds none — or a seeded row that gained the
    entry's QID only in expand shipped its Overture boundary instead of the
    curated one. Expand now applies it, judged like the geometry stage's."""
    import test_index_expand as ex
    from test_index_place_overrides import write_overrides

    from transitio_index import overrides

    curated = shapely.box(23.7, 61.45, 23.9, 61.55)
    entry = {"place": "Q40840", "set_boundary": shapely.to_wkt(curated)}
    if stale:
        entry["evidence_hash"] = "0" * 64
    directory = write_overrides(tmp_path, places=[entry])
    cache = tmp_path / "cache"
    seeded, known = list(ex.SEED_PLACES), []
    if joined:
        # Tampere seeded without a QID, known by its Overture division.
        known.append({"overture": ["fi-tre"]})
        seeded.append(
            {
                "place_id": "tp_2",
                "tp_id": "tp_2",
                "kind": "city",
                "name": "Tampere",
                "country_code": "FI",
                "overture_id": "fi-tre",
                "metro_ids": [],
                "member_ids": [],
            }
        )
    ex._publish_names(
        cache, seeded, places_overrides_sha256=overrides.places_digest(directory)
    )
    ex._write_crawl(cache, "f-tre", ["s1,61.5,23.8\n"])
    path = tmp_path / "places_registry.jsonl"
    ex._publish_run(cache, ex._seeded_registry(path, *known))
    with registry.session(path) as reg:
        manifest, places, report = ex._expand(
            tmp_path, cache, registry=reg, overrides_dir=directory
        )
    # Joined, Tampere is the seeded row and only Pirkanmaa is added.
    assert manifest["places_added"] == (1 if joined else 2)
    tampere = places["Q40840"]
    assert tampere["geometry_source"] == geometry.CURATED
    assert shapely.from_wkb(bytes.fromhex(tampere["geometry"])).equals(curated)
    rows = [row for row in report if row.get("kind") == "stale_override"]
    assert [(row["place"], row["operation"]) for row in rows] == [
        ("Q40840", "set_boundary")
    ] * stale
    assert manifest["stale_place_overrides"] == stale


@pytest.mark.parametrize("seeded", [False, True], ids=["discovered", "seeded"])
def test_a_discovered_city_takes_its_councils_curated_boundary(tmp_path, seeded):
    """Expand lent a discovered city with no area its council's Overture
    area and never lent again, so the city kept that area when the council's
    boundary was curated — in expand, or by the geometry stage for a seeded
    council. It now takes the council's curated boundary."""
    import test_index_expand as ex
    from test_index_place_overrides import write_overrides

    from transitio_index import overrides

    curated = shapely.box(17.9, 59.25, 18.1, 59.4)
    entry = {"place": "Q506250", "set_boundary": shapely.to_wkt(curated)}
    directory = write_overrides(tmp_path, places=[entry])
    rows = list(ex.SEED_PLACES)
    if seeded:
        # The municipality as the geometry stage left it.
        rows.append(
            {
                "place_id": "Q506250",
                "kind": "region",
                "source_subtype": "county",
                "resolution_method": "overture_wikidata",
                "name": "Stockholms kommun",
                "country_code": "SE",
                "overture_id": "se-sto-county",
                "geometry": shapely.to_wkb(curated).hex(),
                "geometry_source": geometry.CURATED,
                "metro_ids": [],
                "member_ids": [],
            }
        )
    cache = tmp_path / "cache"
    ex._publish_names(
        cache, rows, places_overrides_sha256=overrides.places_digest(directory)
    )
    ex._write_crawl(cache, "f-sto", ["s1,59.33,18.07\n"])
    _, places, _ = ex._expand(tmp_path, cache, overrides_dir=directory)
    stockholm = places["Q1754"]
    assert stockholm["parent_id"] == "Q506250"
    assert stockholm["geometry_source"] == geometry.COUNCIL_AREA
    assert shapely.from_wkb(bytes.fromhex(stockholm["geometry"])).equals(curated)


def _council(country, subtype, name, qid=None, **names):
    """A division record as ``seed.council_area`` reads it."""
    return {
        "country": country,
        "source_subtype": subtype,
        "qid": qid,
        "resolution_method": "overture_wikidata" if qid else "overture_id",
        "name": name,
        "names": names,
    }


@pytest.mark.parametrize(
    ("city", "area", "expected"),
    [
        (
            "Stockholm",
            _council(
                "SE",
                "county",
                "Stockholms kommun",
                "Q506250",
                en="Stockholm Municipality",
            ),
            True,
        ),
        ("Göteborg", _council("SE", "county", "Göteborgs Stad", "Q52502"), True),
        ("København", _council("DK", "county", "Københavns Kommune", "Q999101"), True),
        ("Bergen", _council("NO", "county", "Bergen", "Q10428388"), True),
        # A place row keys its country as ``country_code``.
        (
            "Ljubljana",
            {
                "country_code": "SI",
                **_council(None, "region", "Ljubljana", "Q3434113"),
            },
            True,
        ),
        (
            "Stockholm",
            _council("SE", "region", "Stockholms län", "Q999102", sv="Stockholm"),
            False,
        ),
        ("Oslo", _council("NO", "county", "Bergen", "Q10428388"), False),
        ("Turku", _council("FI", "county", "Turku", "Q999103"), False),
        ("Leeds", _council("GB", "county", "Leeds", "Q999104"), False),
        ("Edinburgh", _council("GB", "county", "City of Edinburgh"), True),
    ],
    ids=[
        "municipality",
        "stad",
        "kommune",
        "bare",
        "place-row",
        "not-the-subtype",
        "other-town",
        "other-country",
        "gb-with-qid",
        "gb-council-area",
    ],
)
def test_a_municipality_named_after_its_town_is_its_council_area(city, area, expected):
    """Stockholm, Göteborg, Bergen and Ljubljana were found only as their
    municipalities: a stop reaches the municipality's area alone, and the
    council-area rule knew only the QID-less counties of GB, CA and CO, with
    only a "City" affix stripped. A municipality in DK, NO, SE or SI, with
    its own QID and its town's name, a suffix or a genitive "s" aside, is
    now its town's council area."""
    assert seed.council_area({"name": city, "names": {}}, area) is expected


@pytest.mark.parametrize(
    "place, entry, expected",
    [
        (
            {"country_code": "DE", "names": {"en": "Munich"}},
            {
                "labels": {},
                "aliases": {
                    "it": ["Monaco"],
                    "de": ["MUC"],
                    "de-at": ["LHM"],
                    "mul": ["München"],
                    "en": ["Muenchen"],
                },
            },
            ["LHM", "MUC", "Muenchen", "München"],
        ),
        (
            {"country_code": "US", "names": {"en": "New York"}},
            {
                "labels": {"en": "New York City", "fr": "New York"},
                "aliases": {"mul": ["NYC"], "eo": ["Gotham"]},
            },
            ["NYC", "New York City"],
        ),
        (
            {"names": {"en": "Z"}},
            {"labels": {}, "aliases": {"en": ["Y"], "fr": ["X"]}},
            ["Y"],
        ),
    ],
    ids=["foreign-alias", "replaced-label", "no-country"],
)
def test_wikidata_aliases_are_kept_in_the_places_own_languages(place, entry, expected):
    """A Wikidata alias in a language not the place's own ("Monaco", Italian for
    Munich) does not join its aliases; English, ``mul`` and the country's
    languages do, and so does an own-language label Overture replaced."""
    names._merge(place, entry)
    assert place["aliases"] == expected


NOT_MAPPED = (None, "the centre's country is not mapped")


@pytest.mark.parametrize(
    "region_id, ucdb_country, iso3, countries, expected",
    [
        ("1", "Switzerland", "DEU", ["FR", "FR", "CH"], ("CH", None)),
        ("1", None, "DEU", ["PL", "PL", "DE"], ("DE", None)),
        ("293", "China", "CHN", ["HK"], ("HK", None)),
        ("293", "China", "CHN", ["CN", "CN", "HK"], ("HK", None)),
        ("9114", "China", "CHN", ["MO"], ("MO", None)),
        ("165", "Palestine", "PSE", ["XW"], ("XW", None)),
        ("151", "Syria", "SYR", ["XH"], (None, "no city in the centre's country")),
        ("1", "Palestine", "PSE", ["XW"], NOT_MAPPED),
    ],
    ids=[
        "ucdb over fao",
        "fao without a name",
        "hong kong",
        "hong kong among china",
        "macao",
        "west bank",
        "no city there",
        "unmapped",
    ],
)
def test_a_fao_metros_country_is_its_centres_whatever_cities_a_build_holds(
    region_id, ucdb_country, iso3, countries, expected
):
    """A FAO metro took the country most of a build's cities in its region
    were in, so builds holding different towns filed one region under
    different countries (Ljubljana's under AT, Copenhagen's under SE)."""
    by_id = {
        f"c{i}": {"place_id": f"c{i}", "kind": "city", "country_code": country}
        for i, country in enumerate(countries)
    }
    names = {region_id: {"country": ucdb_country}} if ucdb_country else {}
    regions = {region_id: {"country": iso3}}
    country = metros.fao_country(region_id, sorted(by_id), by_id, names, regions)
    assert country == expected


def test_a_city_with_a_sliver_in_a_functional_urban_area_stays_out():
    """A city whose representative point lay outside every functional urban
    area joined any area its land overlapped: Weilheim, 0.001 of its area
    inside München's, was a member, and the metro outgrew the official FUA."""
    rows = eurostat.assign(
        [{"place_id": "Q_W", "kind": "city", "country_code": "DE", "overture_id": "w"}],
        {"w": [{"geom": shapely.box(10.51, 47.9, 11.01, 48.0), "sources": []}]},
        {"DE003F": {"name": "München", "country": "DE", "nuts3": ["DE003F"]}},
        {"DE003F": shapely.box(11.0, 47.8, 11.5, 48.2)},
        min_share=metros.FUNCTIONAL_URBAN_AREA.min_share,
        land={"DE21N": shapely.box(10.0, 47.0, 12.0, 49.0)},
    )
    assert [row["status"] for row in rows] == ["unassigned"]


@pytest.mark.parametrize("place_id, refused", [("Q404", False), ("Q-nowhere", True)])
def test_a_set_coverage_for_another_builds_feed_is_skipped(tmp_path, place_id, refused):
    """A set_coverage naming a feed the build does not hold raised "names no
    feed", so a partitioned rebuild failed in every label but the feed's."""
    from test_index_place_overrides import write_overrides

    from transitio_index import overrides

    entries = [
        {
            "feed": "f-elsewhere",
            "set_coverage": {"level": "country", "place_id": place_id},
        }
    ]
    directory = write_overrides(tmp_path, feeds=entries)
    if refused:
        with pytest.raises(overrides.OverrideError, match="names no feed"):
            ct._cover(tmp_path, overrides_dir=directory)
        return
    manifest, _, edges = ct._cover(tmp_path, overrides_dir=directory)
    assert manifest["overrides_applied"] == 0 and "f-elsewhere" not in edges


def test_a_fao_metro_carries_its_fao_centre_not_its_members_centroid(tmp_path):
    """CT-48: a place without a centre was routed from a point of its
    polygon; a FAO metro from its member union, off its urban centre."""
    import test_index_metros as mt

    centre = shapely.box(-87.9, 41.9, -87.8, 42.0)
    mt._run(
        tmp_path,
        {},
        fao=mt._fao_inputs(tmp_path),
        ucdb=mt._ucdb_inputs(tmp_path, centre=centre),
    )
    cache = tmp_path / "cache"
    areas = fx.write_area_dataset(tmp_path / "areas-again.parquet", mt.AREAS)
    manifest = geometry.attach_geometry(cache, dataset=areas)
    places, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "places_seed.jsonl"
    )
    (metro,) = [p for p in places if p["place_id"] == "fao_city_region:50"]
    point = shapely.from_wkb(metro["centre"])
    union = shapely.from_wkb(metro["geometry"])
    assert point.equals(shapely.point_on_surface(centre))
    assert union.covers(point) and not point.equals(union.centroid)
    assert manifest["centres"] == sum(bool(p.get("centre")) for p in places)


def test_a_seeded_citys_population_survives_the_merge(tmp_path):
    """Only the build seeding a city records its urban centre's population,
    and the merge took the place row of the build serving it most, so a city
    another build fed lost its population."""
    fx = pytest.importorskip("index_fixture")
    from test_index_merge import BUILT, _archive, _merged

    # The seeding build serves only the country; the newer one feeds Lima.
    for label, digit, lima, served in (
        ("seed", 1, {"population": 10**7}, "pe"),
        ("fed", 2, {"name": "Lima (fed)"}, "lima"),
    ):
        _archive(
            fx,
            tmp_path,
            label,
            digit,
            built_at=BUILT(13 + digit),
            feeds=[{**fx.covered_feed(f"{label}-bus"), "home_country": "PE"}],
            places=[
                fx.place("pe", "country", country_code="PE"),
                fx.place("lima", "city", country_code="PE", **lima),
            ],
            edges=[fx.edge(served, f"{label}-bus", tier="local")],
        )
    _, tables = _merged(tmp_path)
    places = tables["places.parquet"].select(["place_id", "name", "population"])
    assert {row["place_id"]: row for row in places.to_pylist()}["lima"] == {
        "place_id": "lima",
        "name": "Lima (fed)",
        "population": 10**7,
    }


def test_a_feed_needing_a_key_is_crawled_from_its_hosted_copy(tmp_path):
    """resolve refused every GTFS feed the catalogues flag as needing a key,
    so the crawl never read one, though the Mobility Database hosts keyless
    copies of many (all of Trafiklab's). Its URL is now read without the key,
    then the hosted copy; the access details stay the catalogue's."""
    cache = tmp_path / "cache"
    page = "https://register.example/"
    mdb = {
        "requires_auth": True,
        "authentication_type": "1",
        "api_key_parameter_name": "key",
        "authentication_info": page,
        "urls": {"direct_download": crt.PRODUCER_URL, "latest": crt.HOSTED},
    }
    ct._publish(
        cache,
        "crosswalk",
        "feeds.json",
        "feeds.jsonl",
        [
            {
                "feed_id": "f-key",
                "spec": "gtfs",
                "source": "mdb",
                "aliases": [],
                "mdb": mdb,
            }
        ],
    )
    resolve.resolve(cache, overrides_dir=None)
    served = {"/a.zip": (401, None), "/mdb-1/latest.zip": (crt._zip_bytes(), None)}
    _, log = crt._crawl(cache, crt._server(served))
    record = log["f-key"]
    assert (record["method"], record["fetched_from"]) == ("download", "mdb_latest")
    assert "HTTP 401" in record["producer_failure"]
    feeds, _ = store.read_jsonl(
        cache / "resolve", "feeds_resolved.json", "feeds_resolved.jsonl"
    )
    fields = ("crawlable", "access", "auth_method", "registration_url")
    assert [feeds[0][field] for field in fields] == [True, "key", "query_param", page]


@pytest.mark.parametrize(
    "records",
    [[], [{"feed_id": "f-a", "home_country": None}]],
    ids=["no-feeds", "feeds-placed-nowhere"],
)
def test_a_build_with_no_edges_still_loads_as_a_build_with_places(records):
    """A build whose feeds reach no place, like one with no feeds at all,
    gives each partition of places empty feeds and edges tables: the loader
    refuses a partitioned build that lacks any of the three tables."""
    places = [pt._place("riy", "city", country_code="SA")]
    parts = publish.partition(records, places, [])
    assert parts["SA"]["feeds"] == [] and parts["SA"]["edges"] == []
    tables = {
        f"{name}/{table}.parquet": pa.table(
            {"id": pa.array([None] * len(rows), pa.string())}
        )
        for name, by_table in parts.items()
        for table, rows in by_table.items()
    }
    assert builds._join_partitions(tables) is not None


def test_a_release_asset_uploads_in_pieces_of_a_declared_length(tmp_path, monkeypatch):
    """An asset went up as a single write, which the client's timeout bounded
    as a whole, so an archive too large to send within it never uploaded."""
    fx = pytest.importorskip("index_fixture")
    from test_index_publisher import _index

    monkeypatch.setattr(publisher, "UPLOAD_CHUNK", 1024)
    inner = fx.FakeGitHub().transport()
    uploads = {}

    class Recording(httpx.BaseTransport):
        def handle_request(self, request):
            if request.method == "POST" and "name" in request.url.params:
                pieces = list(request.stream)
                uploads[request.url.params["name"]] = (request.headers, pieces)
                request = httpx.Request(
                    request.method,
                    request.url,
                    headers=request.headers,
                    content=b"".join(pieces),
                )
            return inner.handle_request(request)

    summary = publisher.publish_index(
        _index(tmp_path),
        cache_dir=tmp_path / "cache",
        repository="o/r",
        token="secret",
        api_url=fx.API,
        out_dir=tmp_path / "out",
        transport=Recording(),
    )
    headers, pieces = uploads[contract.archive_name(summary["snapshot_id"])]
    assert int(headers["Content-Length"]) == sum(map(len, pieces))
    assert "Transfer-Encoding" not in headers
    assert len(pieces) > 1 and max(map(len, pieces)) <= 1024


@pytest.mark.parametrize("stale", [2, 99], ids=["catches-up", "stays-stale"])
def test_the_round_trip_waits_out_a_cached_release_listing(
    tmp_path, monkeypatch, stale
):
    """GitHub serves the anonymous release listing cached for up to 60 s, so
    the round trip, asked once right after the flip, resolved the release
    before the one just published and reported a good release as failed."""
    fx = pytest.importorskip("index_fixture")
    from test_index_publisher import _index

    monkeypatch.setattr(publisher, "ROUND_TRIP_INTERVAL", 0)
    fake = fx.FakeGitHub()
    manifest = fx.manifest_bytes(snapshot_id="0000000000000001")
    older = fake.seed("index-0000000000000001", {"manifest.json": manifest})
    inner = fake.transport()
    listings = []

    class Cached(httpx.BaseTransport):
        def handle_request(self, request):
            response = inner.handle_request(request)
            if (
                request.url.path == "/repos/o/r/releases"
                and request.method == "GET"
                and "Authorization" not in request.headers
            ):
                listings.append(request)
                if len(listings) <= stale:
                    kept = [r for r in response.json() if r["id"] == older["id"]]
                    return httpx.Response(200, json=kept)
            return response

    def publish():
        return publisher.publish_index(
            _index(tmp_path),
            cache_dir=tmp_path / "cache",
            repository="o/r",
            token="secret",
            api_url=fx.API,
            out_dir=tmp_path / "out",
            transport=Cached(),
        )

    if stale < publisher.ROUND_TRIP_ATTEMPTS:
        assert publish()["snapshot_id"] != "0000000000000001"
        assert len(listings) == stale + 1
    else:
        with pytest.raises(publisher.PublishIndexError, match="'0000000000000001'"):
            publish()
        assert len(listings) == publisher.ROUND_TRIP_ATTEMPTS


def test_a_metro_leaves_no_holes_along_its_members_seams(tmp_path):
    """Members were simplified one by one and the metro was the union of
    those: along a winding border the two sides' simplified edges parted and
    left holes in the metro, about 42,000 m² of them here."""
    import test_index_geometry as gt

    seam = [
        (24.0 + 0.05 * i / 80, 60.0 + 0.002 * math.sin(i * math.pi / 5))
        for i in range(81)
    ]
    areas = {
        "a": shapely.Polygon([(24.0, 59.9), (24.05, 59.9), *seam[::-1]]),
        "b": shapely.Polygon([*seam[:41], (24.025, 60.1), (24.0, 60.1)]),
        "c": shapely.Polygon([*seam[40:], (24.05, 60.1), (24.025, 60.1)]),
    }
    old = shapely.union_all([geometry._simplify(area) for area in areas.values()])
    assert len(old.interiors) > 0
    cache = tmp_path / "cache"
    members = [gt._place(f"Q_{key}", "city", overture_id=key) for key in areas]
    metro = gt._place("Q_METRO", "metro", members=[m["place_id"] for m in members])
    gt._publish(cache, [*members, metro])
    dataset = fx.write_area_dataset(
        tmp_path / "areas.parquet",
        [fx.area(key, shapely.to_wkb(area), GOOD) for key, area in areas.items()],
    )
    geometry.attach_geometry(cache, dataset=dataset)
    places, _ = store.read_jsonl(
        cache / "gazetteer", "geometry.json", "places_seed.jsonl"
    )
    (row,) = [place for place in places if place["place_id"] == "Q_METRO"]
    drawn = shapely.from_wkb(row["geometry"])
    assert drawn.geom_type == "Polygon" and len(drawn.interiors) == 0
    assert drawn.covers(shapely.Polygon(old.interiors[0]).point_on_surface())


@pytest.mark.parametrize(
    "inline, listing, name",
    [
        (["A", " B ", "A", " "], [], "A, B"),
        ([], ["C"], "C"),
        (["A"], ["C"], "A"),
    ],
    ids=["inline", "listing", "inline-first"],
)
def test_an_atlas_only_feed_takes_its_operators_name(inline, listing, name):
    """1,731 Atlas-only feeds had no name: the crosswalk took only the DMFR
    feed's own ``name``, which most Atlas feeds leave to their operators."""
    import test_index_crosswalk as cwt

    feed = cwt.atlas_feed("f-a", operators=[{"name": n} for n in inline])
    records, _ = crosswalk.build_records(
        [feed], [], [cwt.operator(n, "f-a") for n in listing]
    )
    assert [record["name"] for record in records] == [name]


def test_an_mdb_row_on_an_atlas_feeds_past_url_is_that_feed(tmp_path, monkeypatch):
    """mdb-779's download URL is a past URL of the Atlas MVV feed, but the
    crosswalk matched current URLs only and the partition cut placed Atlas
    feeds by current URL: the index carried MVV twice, its Atlas copy
    nameless."""
    import test_sample_catalogues as sct

    sc = sct.sc
    mvv = "Münchner Verkehrs- und Tarifverbund GmbH (MVV)"
    rows = [
        {**sct._mdb_gtfs("d1", "DE", "https://mvv.example/old"), "provider": mvv},
        sct._mdb_gtfs("d2", "DE", "https://x.example/now"),
        sct._mdb_gtfs("a1", "AT", "https://x.example/then"),
        sct._mdb_gtfs("a2", "AT", "https://mvv.example/h"),
        sct._mdb_gtfs("d3", "DE", "https://two.example/1"),
        sct._mdb_gtfs("a3", "AT", "https://two.example/2"),
        sct._mdb_gtfs("a4", "AT", "https://s.example/a"),
        sct._mdb_gtfs("d4", "DE", "https://s.example/p"),
    ]

    def feed(feed_id, current, *past):
        urls = {
            "static_current": f"https://{current}",
            "static_historic": [f"https://{url}" for url in past],
        }
        return {"id": feed_id, "spec": "gtfs", "urls": urls}

    files = [
        (
            "r/feeds/de.dmfr.json",
            sct._payload(
                feed("f-mvv", "mvv.example/new", "mvv.example/old"),
                feed("f-x", "x.example/now", "x.example/then"),
                feed("f-h", "mvv.example/h", "mvv.example/g"),
                feed("f-two", "two.example/0", "two.example/1", "two.example/2"),
                feed("f-s1", "s.example/a", "s.example/p"),
                feed("f-s2", "t.example/b", "s.example/p"),
            ),
        )
    ]
    sct._partition_inputs(tmp_path, monkeypatch, rows, files)
    sc.main(["--partition", "--out-dir", str(tmp_path / "out")])

    (published,) = (tmp_path / "out").glob("partition-*")
    manifest = json.loads((published / "partition.json").read_text())
    # AT picks f-mvv by host, DE by a past URL; f-x's current URL is in DE,
    # its past one in AT. Rows in two labels reach f-two, which pairs neither,
    # so neither cut holds it; nor does DE hold f-s2 by the past URL it shares
    # with f-s1, which is no identity.
    assert manifest["atlas"] == {
        "f-mvv": "de",
        "f-x": "de",
        "f-h": "at",
        "f-two": "atlas1",
        "f-s1": "at",
        "f-s2": "atlas1",
    }
    cut = Path(manifest["labels"]["de"]["cut"])
    text = (cut / "mdb_sample.csv").read_text(encoding="utf-8")
    mdb_feeds, _ = mdb.parse_rows(
        csv_source.read_rows(text, mdb.REQUIRED_HEADERS), "mdb_sample.csv"
    )
    records, _ = crosswalk.build_records(
        atlas.parse(cut / "atlas_sample.tar.gz")["feeds"], mdb_feeds
    )
    assert [
        (r["feed_id"], r["mdb_id"], r["crosswalk_method"], r["name"]) for r in records
    ] == [
        ("f-x", "d2", "url_exact", None),
        ("f-mvv", "d1", "url_historic", mvv),
        ("f-mdb-d3", "d3", "none", None),
        ("f-mdb-d4", "d4", "none", None),
    ]


def test_a_catalogue_note_is_not_a_feed_name(tmp_path):
    """DELFI's feed was listed as "User registration required to download",
    the Mobility Database name column holding a note about the feed, not a
    name."""
    note = ct.DELFI_NOTE
    feed = {
        **ct._feed("f-city"),
        "name": note,
        "mdb": {"name": note, "provider": "DELFI"},
    }
    manifest, covered, _ = ct._cover(tmp_path, feeds=[feed])
    assert covered["f-city"]["name"] == "DELFI"
    assert covered["f-city"]["mdb"]["name"] == note
    assert manifest["named_after_provider"] == ["f-city"]


def test_feeds_running_the_same_lines_name_each_other(tmp_path):
    """MVV, DELFI and gtfs.de urban each carried about 95 % of Munich's
    departures and the index said nothing; MVV types the S-Bahn as tram."""
    import test_index_classify as clt

    cache = tmp_path / "cache"
    built = [
        clt._lines(cache, feed_id, [("S1", kind), ("210", 3)])
        for feed_id, kind in (("f-mvv", 0), ("f-delfi", 109), ("f-urban", 2))
    ]
    feeds, candidates = zip(*built)
    clt._coverage(cache, list(feeds), list(candidates))
    classify.classify(cache, lookup=clt.LOOKUP)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    for edge in edges:
        others = edge["evidence"]["overlap"]["with"]
        assert set(others) == {"f-mvv", "f-delfi", "f-urban"} - {edge["feed_id"]}
        assert {s for shares in others.values() for s in shares.values()} == {1.0}
