"""The golden-set diff: hand-verified feeds checked against a built cache.

The golden file (``golden/feeds.jsonl``, committed) is ground truth written by
a person, never captured from a build: each entry names a real feed and what
the index must say about it — the places its membership must include (and may
be required to exactly equal or to exclude), and, recorded for the classifier
to be asserted once it exists, its expected tier set and review state. The
check compares a build's coverage artifacts against those expectations and
reports every violation; regeneration must fail loudly on any.

Membership uses include/exclude contracts rather than only exact sets because
a national feed's full membership is open-ended while its anchor places are
not; a small city feed can pin ``exact`` instead. Tier and review-state
assertions stay off until the classifier lands — the entries already record
them so the switch is a flag, not a schema change.
"""

import json
import math
import os

from index_build import overture, store

REQUIRED_KEYS = ("feed_id", "name", "why", "membership", "tiers", "review_state")
REVIEW_STATES = ("confident", "needs_review")
# The index-wide unknown-edge share may not move more than this, in
# percentage points, between one snapshot and the next.
UNKNOWN_DRIFT = 0.05
TIERS = ("local", "regional", "national", "international", "unknown")


class GoldenError(RuntimeError):
    """The golden file itself is malformed."""


def load_golden(path):
    """The validated golden entries, or :class:`GoldenError`.

    A malformed golden file must fail the gate itself, never read as an
    empty set of expectations.
    """
    entries = []
    seen = set()
    for record in store.parse_jsonl(path.read_bytes()):
        if not isinstance(record, dict):
            raise GoldenError("golden entry is not an object")
        missing = [key for key in REQUIRED_KEYS if key not in record]
        if missing:
            raise GoldenError(f"{record.get('feed_id')}: missing {missing}")
        if record["feed_id"] in seen:
            raise GoldenError(f"{record['feed_id']}: duplicate entry")
        seen.add(record["feed_id"])
        for key in ("membership", "membership_excludes"):
            # Present means a QID list — a falsy non-list (0, false, "") is
            # malformed, never an empty contract.
            places = record.get(key, [])
            if not isinstance(places, list) or any(
                not (isinstance(q, str) and overture.QID_PATTERN.match(q))
                for q in places
            ):
                raise GoldenError(f"{record['feed_id']}: {key} must be a QID list")
        if "membership_exact" in record and not isinstance(
            record["membership_exact"], bool
        ):
            raise GoldenError(f"{record['feed_id']}: membership_exact must be a bool")
        if record.get("membership_exact") and record.get("membership_excludes"):
            raise GoldenError(f"{record['feed_id']}: exact membership already excludes")
        if not record["membership"]:
            raise GoldenError(f"{record['feed_id']}: empty membership")
        if record["review_state"] not in REVIEW_STATES:
            raise GoldenError(f"{record['feed_id']}: bad review_state")
        tiers = record["tiers"]
        if (
            not isinstance(tiers, list)
            or not tiers
            or any(tier not in TIERS for tier in tiers)
        ):
            raise GoldenError(f"{record['feed_id']}: bad tiers")
        entries.append(record)
    if not entries:
        raise GoldenError("golden file has no entries")
    return entries


def check_catalogue_evidence(golden_path, evidence_path):
    """Every golden feed must match the committed catalogue evidence, 1:1.

    ``golden/catalogue_evidence.jsonl`` is captured from a real
    ingest+crosswalk of the pinned catalogues when the golden set changes;
    diffing the two committed files keeps a typo'd or renamed feed id from
    sitting unnoticed until a real build runs. Returns the mismatch lists.
    """
    golden_ids = {entry["feed_id"] for entry in load_golden(golden_path)}
    evidence_ids = set()
    for row in store.parse_jsonl(evidence_path.read_bytes()):
        if not isinstance(row, dict) or not row.get("feed_id"):
            raise GoldenError("catalogue evidence row is not a feed record")
        if row["feed_id"] in evidence_ids:
            raise GoldenError(f"{row['feed_id']}: duplicate evidence row")
        evidence_ids.add(row["feed_id"])
    return {
        "unevidenced": sorted(golden_ids - evidence_ids),
        "orphaned": sorted(evidence_ids - golden_ids),
    }


def _actual(cache_dir, overrides_dir=None):
    """``{feed_id: [edges]}`` from the latest edge stage — curated edges
    when a fresh curate generation exists, classified edges when a fresh
    classify generation does, candidate edges otherwise; a stale generation
    fails the gate rather than being checked, and so do curated edges whose
    ``edges.yaml`` has moved on (or an ``edges.yaml`` nobody applied)."""
    from index_build import classify, overrides

    try:
        _, edges, manifest = classify.read_edges(cache_dir)
        overrides.applied_digest(manifest, overrides_dir)
    except (classify.ClassifyError, overrides.OverrideError) as error:
        raise GoldenError(str(error)) from error
    if edges is None:
        raise GoldenError("no edge generation to check against")
    actual = {}
    for edge in edges:
        actual.setdefault(edge["feed_id"], []).append(edge)
    return actual, manifest


def _unknown_drift(cache_dir, manifest):
    """The change in unknown-edge share against the previous snapshot, or
    None when either side has no measurement."""
    current = manifest.get("unknown_share")
    path = cache_dir / "index" / "snapshot.json"
    if current is None or not (path.is_symlink() or path.exists()):
        return None
    try:
        with os.fdopen(store.open_nofollow(path), "rb") as opened:
            snapshot = json.loads(opened.read())
    except (OSError, ValueError) as error:
        # A snapshot that is there but unreadable must not silently
        # disable the drift check.
        raise GoldenError(f"the previous snapshot is unreadable: {error}") from error
    if not isinstance(snapshot, dict):
        raise GoldenError("the previous snapshot is not an object")
    previous = snapshot.get("unknown_share")
    if previous is None:
        return None
    if not (_is_share(previous) and _is_share(current)):
        # NaN parses as JSON and compares false to everything.
        raise GoldenError("unknown_share is not a finite number")
    return current - previous


def _is_share(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def check(
    cache_dir,
    golden_path,
    *,
    assert_tiers=None,
    edges=None,
    manifest=None,
    overrides_dir=None,
):
    """Diff the build against the golden entries; returns the report.

    The report's ``violations`` list one dict per broken expectation; an
    empty list is a pass. Tier and review-state assertions are ON whenever
    the edges come from a classify or curate generation (``assert_tiers`` overrides
    either way): the feed's tier set over EVERY edge it has must equal the
    recorded one and the review state must match — ``needs_review`` when
    any of its edges needs review — so a rule change that moves a golden
    feed across the cutoff fails the diff without anyone remembering a
    flag. ``edges``/``manifest`` let a caller check the edges it already
    read instead of resolving the latest generation again.
    """
    entries = load_golden(golden_path)
    if edges is None:
        actual, manifest = _actual(cache_dir, overrides_dir)
    else:
        # The caller's own read: publish passes the very edges it ships, so
        # a concurrent republish cannot make the gate judge another set.
        actual = {}
        for edge in edges:
            actual.setdefault(edge["feed_id"], []).append(edge)
        manifest = manifest or {}
    if assert_tiers is None:
        assert_tiers = manifest.get("source") in ("classify", "curate")
    violations = []
    for entry in entries:
        feed_id = entry["feed_id"]
        edges = actual.get(feed_id) or []
        places = {edge["place_id"] for edge in edges}
        if not places:
            violations.append({"feed_id": feed_id, "problem": "feed has no edges"})
            continue
        expected = set(entry["membership"])
        if assert_tiers:
            relevant = edges
            tiers = {edge["tier"] for edge in relevant}
            if tiers != set(entry["tiers"]):
                violations.append(
                    {
                        "feed_id": feed_id,
                        "problem": "tier set differs",
                        "expected": sorted(entry["tiers"]),
                        "actual": sorted(tiers),
                    }
                )
            state = (
                "needs_review"
                if any(edge.get("needs_review") for edge in relevant)
                else "confident"
            )
            if state != entry["review_state"]:
                violations.append(
                    {
                        "feed_id": feed_id,
                        "problem": "review state crossed the cutoff",
                        "expected": entry["review_state"],
                        "actual": state,
                    }
                )
        if entry.get("membership_exact"):
            if places != expected:
                violations.append(
                    {
                        "feed_id": feed_id,
                        "problem": "membership differs",
                        "missing": sorted(expected - places),
                        "unexpected": sorted(places - expected),
                    }
                )
            continue
        missing = expected - places
        if missing:
            violations.append(
                {
                    "feed_id": feed_id,
                    "problem": "membership missing places",
                    "missing": sorted(missing),
                }
            )
        banned = set(entry.get("membership_excludes") or []) & places
        if banned:
            violations.append(
                {
                    "feed_id": feed_id,
                    "problem": "membership includes excluded places",
                    "unexpected": sorted(banned),
                }
            )
    if assert_tiers:
        drift = _unknown_drift(cache_dir, manifest)
        # Exactly five points is permitted; the tolerance keeps binary
        # rounding (0.40 - 0.35) from failing it.
        if (
            drift is not None
            and abs(drift) > UNKNOWN_DRIFT
            and not math.isclose(abs(drift), UNKNOWN_DRIFT)
        ):
            violations.append(
                {
                    "feed_id": None,
                    "problem": "unknown share drifted",
                    "delta": drift,
                    "limit": UNKNOWN_DRIFT,
                }
            )
    return {
        "entries": len(entries),
        "violations": violations,
        "passed": not violations,
    }


def main(argv=None):
    import argparse
    import pathlib

    parser = argparse.ArgumentParser(description="Run the golden-set diff")
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--golden", required=True, type=pathlib.Path)
    parser.add_argument(
        "--membership-only",
        action="store_true",
        help="check membership only, even against classified edges",
    )
    parser.add_argument(
        "--overrides-dir",
        type=pathlib.Path,
        default=pathlib.Path("overrides"),
        help="directory of override YAML files (default: overrides)",
    )
    args = parser.parse_args(argv)
    report = check(
        args.cache_dir,
        args.golden,
        assert_tiers=False if args.membership_only else None,
        overrides_dir=args.overrides_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
