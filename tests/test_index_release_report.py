import pandas
import pytest

from transitio_index import release_report

CRAWLED = "2026-10-03T12:00:00+00:00"


def _feed(feed_id, **kw):
    return {
        "feed_id": feed_id,
        "crawl_status": kw.get("crawl_status", "ok"),
        "uncrawlable_reason": kw.get("uncrawlable_reason"),
        "stop_count": kw.get("stop_count", 20.0),
        "home_country": kw.get("home_country", "FI"),
        "last_crawled": kw.get("last_crawled", CRAWLED),
        "service_start": kw.get("service_start", "2026-01-01"),
        "service_end": kw.get("service_end", "2026-12-31"),
        "contained_in": kw.get("contained_in", []),
    }


def _edge(place_id, feed_id, tier="local", cross_border=False):
    return {
        "place_id": place_id,
        "feed_id": feed_id,
        "tier": tier,
        "cross_border": cross_border,
    }


def _city(place_id, name, population):
    return {
        "place_id": place_id,
        "kind": "city",
        "name": name,
        "country_code": "FI",
        "population": population,
    }


def _tables(snapshot, feeds, places, edges):
    return (
        snapshot,
        pandas.DataFrame(feeds),
        pandas.DataFrame(places),
        pandas.DataFrame(edges),
    )


def test_the_report_counts_flags_and_compares_the_index():
    # One large city served once, one served only by stale feeds, a feed
    # without a home, a large unknown-tier feed, a long window, a keyed feed
    # never crawled and one contained in another.
    feeds = [
        _feed("f-city"),
        *(_feed(f"f-old{n}", service_end="2026-06-30") for n in range(3)),
        _feed("f-homeless", stop_count=10.0, home_country=None),
        _feed("f-unknown", stop_count=1500.0),
        _feed("f-long", service_start="1990-01-01", service_end="2030-12-31"),
        _feed(
            "f-keyed",
            crawl_status=None,
            stop_count=None,
            uncrawlable_reason="requires authentication",
            last_crawled=None,
        ),
        _feed("f-inner", contained_in=["f-city"]),
    ]
    places = [
        _city("tp_1", "Big", 2_000_000.0),
        _city("tp_2", "Stale", 1_500_000.0),
        _city("tp_3", "Small|town\x1b[31m", 5_000.0),
        _city("tp_4", "Faded", 1_200_000.0),
    ]
    edges = [
        _edge("tp_1", "f-city"),
        *(_edge("tp_2", f"f-old{n}", "regional") for n in range(3)),
        _edge("tp_3", "f-unknown", "unknown"),
        _edge("tp_3", "f-homeless", cross_border=True),
        _edge("tp_4", "f-old0"),
        _edge("tp_4", "f-old1"),
    ]
    new = _tables("0000000000000002", feeds, places, edges)
    _, feeds_, places_, edges_ = new
    totals = release_report.totals(feeds_, places_, edges_)
    assert {k: totals[k] for k in ("feeds", "stale", "contained", "no_home")} == {
        "feeds": 9,
        "stale": 3,
        "contained": 1,
        "no_home": 1,
    }
    assert totals["not_crawled"] == {"requires authentication": 1}
    assert (totals["unknown_tier_edges"], totals["cross_border_edges"]) == (1, 1)

    found = release_report.anomalies(feeds_, places_, edges_)
    assert found == {
        "few_feeds": ["Big (tp_1, FI): 1", "Faded (tp_4, FI): 2"],
        "stale_only": ["Stale (tp_2, FI): 3", "Faded (tp_4, FI): 2"],
        "no_home": ["f-homeless: 10 stops"],
        "unknown_tier": ["f-unknown: 1500 stops"],
        "long_window": ["f-long: 1990-01-01 to 2030-12-31"],
    }
    none = dict.fromkeys(release_report.TIERS, 0)
    assert release_report.place_tiers(places_, edges_, ["tp_2", "tp_1", "tp_9"]) == {
        "Stale (tp_2, FI)": {**none, "regional": 3},
        "Big (tp_1, FI)": {**none, "local": 1},
        "tp_9 (not in the index)": none,
    }

    # The release before: a feed since removed, Big's feed then regional,
    # Small then served by none.
    previous = _tables(
        "0000000000000001",
        [*feeds[:4], _feed("f-gone")],
        places,
        [
            _edge("tp_1", "f-city", "regional"),
            _edge("tp_1", "f-gone"),
            *edges[1:4],
            *edges[-2:],
        ],
    )
    changed = release_report.changes(new, previous)
    assert changed["feeds_removed"] == ["f-gone"]
    assert (
        "f-keyed" in changed["feeds_added"] and "f-city" not in changed["feeds_added"]
    )
    assert changed["places_changed"] == [
        "Big (tp_1): added f-city local; removed f-city regional, f-gone local",
        "Small|town\x1b[31m (tp_3): added f-homeless local, f-unknown unknown; "
        "removed none",
    ]
    page = release_report.render(
        {
            "snapshot": new[0],
            "previous": previous[0],
            "totals": totals,
            "places": {},
            "anomalies": found,
            "changes": changed,
            "reference": None,
        }
    )
    assert "Compared with snapshot 0000000000000001." in page
    assert "- Big (tp_1, FI): 1" in page and "- f-gone" in page
    assert "- Small&#124;town [31m (tp_3): added" in page and "\x1b" not in page
    assert "| Not crawled: requires authentication | 1 |" in page


def test_reference_checks_are_compared_between_releases(tmp_path, monkeypatch):
    # The publishing token and an inherited stop-at-first-failure never
    # reach the checks.
    monkeypatch.setenv("GITHUB_TOKEN", "secret")
    monkeypatch.setenv("RELEASE_PAT", "secret")
    monkeypatch.setenv("PYTEST_ADDOPTS", "-x")
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / release_report.REFERENCE_TEST).write_text(
        "import os\n"
        "import pytest\n\n"
        "def test_kept():\n"
        "    assert os.environ['TRANSITIO_REFERENCE_RAW'] == '1'\n"
        "    assert 'GITHUB_TOKEN' not in os.environ\n"
        "    assert 'RELEASE_PAT' not in os.environ\n\n"
        "def test_broken():\n"
        "    assert os.environ['TRANSITIO_REFERENCE_INDEX'].endswith('new')\n\n"
        "def test_gap():\n"
        "    pytest.skip('not in the index')\n"
    )
    passing = release_report.reference_outcomes(
        tmp_path / "new", tests, withhold=("RELEASE_PAT",)
    )
    assert passing == {
        "test_kept": "passed",
        "test_broken": "passed",
        "test_gap": "skipped",
    }
    failing = release_report.reference_outcomes(
        tmp_path / "old", tests, withhold=("RELEASE_PAT",)
    )
    assert failing["test_broken"] == "failed"
    # Failing now, passing on the release before: a regression.
    compared = release_report.compare(failing, passing)
    assert compared == {
        "passed": 1,
        "failed": 1,
        "skipped": 1,
        "regressions": ["test_broken"],
        "repaired": [],
    }
    assert release_report.regressions({"reference": compared}) == ["test_broken"]
    assert release_report.compare(passing, failing)["repaired"] == ["test_broken"]
    # Two checks of one name would hide each other's outcome.
    twice = tmp_path / "twice.xml"
    twice.write_text(
        '<testsuite><testcase classname="a" name="t"/>'
        '<testcase classname="b" name="t"/></testsuite>'
    )
    with pytest.raises(release_report.ReleaseReportError, match="two checks"):
        release_report._outcomes(twice)
    # A run that cannot collect its checks reports nothing to compare, and
    # the changed file changes the checks' digest.
    digest = release_report._reference_digest(tests)
    (tests / release_report.REFERENCE_TEST).write_text("def test_kept(:\n")
    assert release_report._reference_digest(tests) != digest
    with pytest.raises(release_report.ReleaseReportError, match="pytest exit 2"):
        release_report.reference_outcomes(tmp_path / "new", tests)
