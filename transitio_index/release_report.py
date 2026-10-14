"""The release report: what a built index holds, where it looks wrong, and
what changed since the previous release, read before it is published.

:func:`build_report` returns the report as a dict and :func:`render` as a
page of Markdown, which the publish step prints and attaches to the release:

- **Totals**: feeds, places and edges; the feeds not crawled, by the reason
  recorded; the feeds stale when indexed, contained in another feed, crawled
  without stops, and with stops but no home country; the edges of unknown
  tier and the cross-border edges.
- **Places**: each reference place's and each large city's feeds by tier.
- **Anomalies**: a city of at least ``LARGE_CITY`` people with ``FEW_FEEDS``
  feeds or fewer, or with stale feeds only; a feed with stops but no home
  country; a feed whose every edge is of unknown tier, with more than
  ``UNKNOWN_STOPS`` stops; a service window longer than ``LONG_WINDOW_DAYS``
  days or starting before ``EARLY_START``.
- **Changes** since the previous release: the feeds added and removed, the
  places whose feeds or tiers changed, and the reference checks that turned
  green or red. The reference checks are transitio's
  ``tests/test_reference_places.py``, run on each index with no defect named
  (:func:`reference_outcomes`); a check that passes on the previous release
  and fails on this one is a regression (:func:`regressions`).
"""

import hashlib
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from xml.etree import ElementTree

import pandas

from transitio_index import crawl
from transitio_index.rank import STALE_DAYS

LARGE_CITY = 1_000_000
FEW_FEEDS = 2
UNKNOWN_STOPS = 1_000
LONG_WINDOW_DAYS = 5 * 365
EARLY_START = "2000-01-01"
REFERENCE_TEST = "test_reference_places.py"
REFERENCE_DATA = Path("data") / "reference_places.json"
TIERS = ("local", "regional", "national", "international", "unknown")
# Entries a rendered list shows; the report keeps them all.
SHOWN = 25
# The pytest exit codes of a run that ran every check: all passed, or some
# failed.
_COMPLETE_RUNS = (0, 1)
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]+")
# Environment variables the reference checks never see: credentials, and
# pytest options that could stop or narrow a run.
_WITHHELD = re.compile(r"TOKEN|SECRET|PASSW|CREDENTIAL|KEY|^PYTEST_ADDOPTS$", re.I)
# The files the reference checks run from, digested around the two runs.
_REFERENCE_FILES = (REFERENCE_TEST, str(REFERENCE_DATA), "conftest.py")


class ReleaseReportError(RuntimeError):
    """The report could not be made."""


def _tables(path):
    from transitio.index import read_index

    index = read_index(path)
    return index.snapshot_id, index.feeds, index.places, index.edges


def _day(series):
    return pandas.to_datetime(series.astype("string").str[:10], errors="coerce")


def _stale(feeds):
    ended = _day(feeds["last_crawled"]) - _day(feeds["service_end"])
    return ended.dt.days > STALE_DAYS


def _contained(feeds):
    return feeds["contained_in"].map(lambda value: value is not None and len(value) > 0)


def _label(row):
    return f"{row.name} ({row.place_id}, {row.country_code})"


def totals(feeds, places, edges):
    causes = pandas.Series(
        [
            crawl.unread_cause(status, reason)
            for status, reason in zip(
                feeds["crawl_status"], feeds["uncrawlable_reason"]
            )
        ],
        index=feeds.index,
        dtype=object,
    )
    crawled = causes.isna()
    stops = feeds["stop_count"].fillna(0)
    reasons = causes[~crawled]
    cross = edges["cross_border"].astype("boolean").fillna(False)
    return {
        "feeds": len(feeds),
        "places": len(places),
        "edges": len(edges),
        "not_crawled": {k: int(n) for k, n in reasons.value_counts().items()},
        "stale": int(_stale(feeds).sum()),
        "contained": int(_contained(feeds).sum()),
        "without_stops": int((crawled & (stops == 0)).sum()),
        "no_home": int(((stops > 0) & feeds["home_country"].isna()).sum()),
        "unknown_tier_edges": int((edges["tier"] == "unknown").sum()),
        "cross_border_edges": int(cross.sum()),
    }


def _large_cities(places):
    """The cities of at least ``LARGE_CITY`` people, the most populous first."""
    large = (places["kind"] == "city") & (places["population"] >= LARGE_CITY)
    return places[large].sort_values(["population", "place_id"], ascending=False)


def place_tiers(places, edges, place_ids):
    """``{place label: {tier: feeds}}`` for the places named, in that order."""
    named = places.set_index("place_id")
    rows = edges[edges["place_id"].isin(place_ids)]
    counts = rows.groupby(["place_id", "tier"])["feed_id"].nunique()
    found = {}
    for place_id in place_ids:
        if place_id in named.index:
            row = named.loc[place_id]
            label = f"{row['name']} ({place_id}, {row['country_code']})"
        else:
            label = f"{place_id} (not in the index)"
        found[label] = {tier: int(counts.get((place_id, tier), 0)) for tier in TIERS}
    return found


def anomalies(feeds, places, edges):
    stale = set(feeds.loc[_stale(feeds), "feed_id"])
    cities = _large_cities(places)
    served = edges.groupby("place_id")["feed_id"].agg(set)
    few, stale_only = [], []
    for row in cities.itertuples():
        feeds_here = served.get(row.place_id, set())
        if len(feeds_here) <= FEW_FEEDS:
            few.append(f"{_label(row)}: {len(feeds_here)}")
        if feeds_here and feeds_here <= stale:
            stale_only.append(f"{_label(row)}: {len(feeds_here)}")
    stops = feeds["stop_count"].fillna(0)
    no_home = feeds[(stops > 0) & feeds["home_country"].isna()]
    tiers = edges.groupby("feed_id")["tier"].agg(set)
    unknown = {feed_id for feed_id, found in tiers.items() if found == {"unknown"}}
    unknown = feeds[feeds["feed_id"].isin(unknown) & (stops > UNKNOWN_STOPS)]
    start, end = _day(feeds["service_start"]), _day(feeds["service_end"])
    long = ((end - start).dt.days > LONG_WINDOW_DAYS) | (
        start < pandas.Timestamp(EARLY_START)
    )
    windows = feeds[long.fillna(False)]
    return {
        "few_feeds": few,
        "stale_only": stale_only,
        "no_home": [
            f"{r.feed_id}: {int(r.stop_count)} stops" for r in no_home.itertuples()
        ],
        "unknown_tier": [
            f"{r.feed_id}: {int(r.stop_count)} stops" for r in unknown.itertuples()
        ],
        "long_window": [
            f"{r.feed_id}: {r.service_start} to {r.service_end}"
            for r in windows.itertuples()
        ],
    }


def _pairs(edges):
    pairs = edges["feed_id"] + " " + edges["tier"]
    return pairs.groupby(edges["place_id"]).agg(frozenset)


def changes(new, previous):
    """The feeds added and removed, and the places present in both indexes
    whose feeds or tiers changed, each with what changed."""
    _, feeds, places, edges = new
    _, old_feeds, old_places, old_edges = previous
    now, before = set(feeds["feed_id"]), set(old_feeds["feed_id"])
    pairs, old_pairs = _pairs(edges), _pairs(old_edges)
    names = places.set_index("place_id")["name"]
    changed = []
    none = frozenset()
    for place_id in sorted(set(places["place_id"]) & set(old_places["place_id"])):
        added = sorted(pairs.get(place_id, none) - old_pairs.get(place_id, none))
        removed = sorted(old_pairs.get(place_id, none) - pairs.get(place_id, none))
        if added or removed:
            changed.append(
                (
                    len(added) + len(removed),
                    place_id,
                    names.get(place_id),
                    added,
                    removed,
                )
            )
    changed.sort(key=lambda item: (-item[0], item[1]))
    return {
        "feeds_added": sorted(now - before),
        "feeds_removed": sorted(before - now),
        "places_changed": [
            f"{name} ({place_id}): added {', '.join(added) or 'none'}; "
            f"removed {', '.join(removed) or 'none'}"
            for _, place_id, name, added, removed in changed
        ],
    }


def _outcomes(junit):
    found = {}
    for case in ElementTree.parse(junit).iter("testcase"):
        if case.get("name") in found:
            raise ReleaseReportError(f"two checks are named {case.get('name')}")
        if case.find("failure") is not None or case.find("error") is not None:
            found[case.get("name")] = "failed"
        elif case.find("skipped") is not None:
            found[case.get("name")] = "skipped"
        else:
            found[case.get("name")] = "passed"
    return found


def reference_outcomes(index_dir, tests_dir, withhold=()):
    """``{check: "passed" | "failed" | "skipped"}``: transitio's reference
    checks (``tests_dir``, a transitio checkout's ``tests``) run on the
    index at ``index_dir`` with no defect named, in an environment without
    credentials, the variables named in ``withhold`` or inherited pytest
    options."""
    test = Path(tests_dir) / REFERENCE_TEST
    if not test.is_file():
        raise ReleaseReportError(f"{test}: no such file")
    environment = {
        name: value
        for name, value in os.environ.items()
        if not _WITHHELD.search(name)
        and name not in withhold
        and name != "TRANSITIO_REFERENCE_FETCH"
    }
    environment["TRANSITIO_REFERENCE_INDEX"] = str(Path(index_dir).resolve())
    environment["TRANSITIO_REFERENCE_RAW"] = "1"
    with tempfile.TemporaryDirectory() as directory:
        junit = Path(directory) / "reference.xml"
        run = subprocess.run(
            [sys.executable, "-m", "pytest", str(test), "-q", "-p", "no:cacheprovider"]
            + ["-o", "addopts=", f"--junitxml={junit}"],
            env=environment,
            capture_output=True,
            text=True,
        )
        if run.returncode not in _COMPLETE_RUNS or not junit.is_file():
            raise ReleaseReportError(
                f"the reference checks did not run to the end (pytest exit "
                f"{run.returncode}): {(run.stdout + run.stderr)[-2000:]}"
            )
        return _outcomes(junit)


def compare(now, before):
    """The reference outcomes now, and the checks that turned red
    (``regressions``) or green (``repaired``) since ``before``."""
    counts = {
        kind: sum(v == kind for v in now.values()) for kind in ("passed", "failed")
    }
    return {
        **counts,
        "skipped": sum(v == "skipped" for v in now.values()),
        "regressions": sorted(
            c for c, v in now.items() if v == "failed" and before.get(c) == "passed"
        ),
        "repaired": sorted(
            c for c, v in now.items() if v == "passed" and before.get(c) == "failed"
        ),
    }


def _reference_places(tests_dir):
    import json

    path = Path(tests_dir) / REFERENCE_DATA
    data = json.loads(path.read_text(encoding="utf-8"))
    return [place["place"] for place in data["places"]]


def build_report(index_dir, *, previous_dir=None, tests_dir=None, withhold=()):
    """The release report of the index at ``index_dir``, against the release
    at ``previous_dir`` when given; ``tests_dir`` (a transitio checkout's
    ``tests``) adds the reference places and checks, run without the
    environment variables named in ``withhold``."""
    digest = None if tests_dir is None else _reference_digest(tests_dir)
    new = _tables(index_dir)
    snapshot, feeds, places, edges = new
    named = list(_large_cities(places)["place_id"][:SHOWN])
    if tests_dir is not None:
        named = _reference_places(tests_dir) + named
    report = {
        "snapshot": snapshot,
        "previous": None,
        "totals": totals(feeds, places, edges),
        "places": place_tiers(places, edges, named),
        "anomalies": anomalies(feeds, places, edges),
        "changes": None,
        "reference": None,
    }
    if previous_dir is not None:
        previous = _tables(previous_dir)
        report["previous"] = previous[0]
        report["changes"] = changes(new, previous)
    if tests_dir is not None:
        now = reference_outcomes(index_dir, tests_dir, withhold)
        before = {}
        if previous_dir is not None:
            before = reference_outcomes(previous_dir, tests_dir, withhold)
            if set(before) != set(now):
                raise ReleaseReportError(
                    "the reference checks run on the two indexes differ"
                )
        if _reference_digest(tests_dir) != digest:
            raise ReleaseReportError("the reference checks changed while they ran")
        report["reference"] = compare(now, before)
    # The checks read the indexes again: they must still be the snapshots the
    # tables came from.
    for path, expected in ((index_dir, snapshot), (previous_dir, report["previous"])):
        if path is not None and _snapshot_id(path) != expected:
            raise ReleaseReportError(f"{path} changed while the report was made")
    return report


def _snapshot_id(path):
    import json

    try:
        snapshot = json.loads((Path(path) / "snapshot.json").read_text("utf-8"))
    except (OSError, ValueError):
        return None
    return snapshot.get("snapshot_id") if isinstance(snapshot, dict) else None


def _reference_digest(tests_dir):
    digest = hashlib.sha256()
    for name in _REFERENCE_FILES:
        path = Path(tests_dir) / name
        digest.update(name.encode() + b"\0")
        digest.update(path.read_bytes() if path.is_file() else b"")
    return digest.hexdigest()


def regressions(report):
    """The reference checks that passed on the previous release and fail on
    this one."""
    return (report.get("reference") or {}).get("regressions", [])


def _text(value):
    """``value`` safe in a Markdown table cell or list item and in a
    terminal: control characters as spaces, ``|``, ``<`` and ``>`` as
    character references."""
    text = _CONTROL.sub(" ", str(value))
    return text.replace("|", "&#124;").replace("<", "&lt;").replace(">", "&gt;")


def _listed(title, entries):
    lines = [f"### {title} ({len(entries)})", ""]
    lines += [f"- {_text(entry)}" for entry in entries[:SHOWN]]
    if len(entries) > SHOWN:
        lines.append(f"- and {len(entries) - SHOWN} more")
    return lines + [""]


def render(report):
    """The report as a page of Markdown."""
    totals_ = report["totals"]
    lines = [f"# Release report: index snapshot {report['snapshot']}", ""]
    lines.append(
        f"Compared with snapshot {report['previous']}."
        if report["previous"]
        else "No previous release to compare with."
    )
    lines += ["", "## Totals", "", "| | |", "|---|---:|"]
    for key, label in (
        ("feeds", "Feeds"),
        ("places", "Places"),
        ("edges", "Edges"),
        ("stale", "Feeds stale when indexed"),
        ("contained", "Feeds contained in another feed"),
        ("without_stops", "Feeds crawled without stops"),
        ("no_home", "Feeds with stops but no home country"),
        ("unknown_tier_edges", "Edges of unknown tier"),
        ("cross_border_edges", "Cross-border edges"),
    ):
        lines.append(f"| {label} | {totals_[key]:,} |")
    for reason, count in totals_["not_crawled"].items():
        lines.append(f"| Not crawled: {_text(reason)} | {count:,} |")
    reference = report["reference"]
    lines += ["", "## Reference checks", ""]
    if reference is None:
        lines += ["Not run.", ""]
    else:
        lines.append(
            f"{reference['passed']} passed, {reference['failed']} failed, "
            f"{reference['skipped']} skipped."
        )
        lines.append("")
        lines += _listed("Passed before, fail now", reference["regressions"])
        lines += _listed("Failed before, pass now", reference["repaired"])
    lines += ["## Places", "", "| Place | " + " | ".join(TIERS) + " |"]
    lines.append("|---|" + "---:|" * len(TIERS))
    for label, counts in report["places"].items():
        cells = " | ".join(str(counts[t]) for t in TIERS)
        lines.append(f"| {_text(label)} | {cells} |")
    found = report["anomalies"]
    lines += ["", "## Anomalies", ""]
    lines += _listed(
        f"Cities of {LARGE_CITY:,} people or more with at most " f"{FEW_FEEDS} feeds",
        found["few_feeds"],
    )
    lines += _listed("Large cities with stale feeds only", found["stale_only"])
    lines += _listed("Feeds with stops but no home country", found["no_home"])
    lines += _listed(
        f"Feeds of unknown tier only with over {UNKNOWN_STOPS:,} stops",
        found["unknown_tier"],
    )
    lines += _listed(
        f"Service windows over {LONG_WINDOW_DAYS:,} days or starting "
        f"before {EARLY_START}",
        found["long_window"],
    )
    if report["changes"] is not None:
        changed = report["changes"]
        lines += ["## Changes", ""]
        lines += _listed("Feeds added", changed["feeds_added"])
        lines += _listed("Feeds removed", changed["feeds_removed"])
        lines += _listed(
            "Places whose feeds or tiers changed", changed["places_changed"]
        )
    return "\n".join(lines).rstrip() + "\n"
