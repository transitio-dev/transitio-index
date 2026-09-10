"""Regression tests: one per fixed defect, guarding against reappearance.

Fixtures are imported from the stage test modules rather than duplicated.
"""

import http.client
import logging

import pytest

pytest.importorskip("pyarrow")
import shapely  # noqa: E402

import overture_fixture as fx  # noqa: E402
from transitio_index import boundaries, coverage, overture, registry, seed  # noqa: E402

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
