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

from index_build import overture, store

REQUIRED_KEYS = ("feed_id", "name", "why", "membership", "tiers", "review_state")
REVIEW_STATES = ("confident", "needs_review")
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
            places = record.get(key) or []
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


def _actual(cache_dir):
    """``{feed_id: set of place ids}`` from the build's candidate edges."""
    edges, _ = store.read_jsonl(
        cache_dir / "coverage", "coverage.json", "edges_candidate.jsonl"
    )
    actual = {}
    for edge in edges:
        actual.setdefault(edge["feed_id"], set()).add(edge["place_id"])
    return actual


def check(cache_dir, golden_path, *, assert_tiers=False):
    """Diff the build against the golden entries; returns the report.

    The report's ``violations`` list one dict per broken expectation; an
    empty list is a pass. ``assert_tiers`` stays False until the classifier
    exists — entries record tiers and review state for that switch.
    """
    entries = load_golden(golden_path)
    actual = _actual(cache_dir)
    violations = []
    for entry in entries:
        feed_id = entry["feed_id"]
        places = actual.get(feed_id)
        if not places:
            violations.append({"feed_id": feed_id, "problem": "feed has no edges"})
            continue
        expected = set(entry["membership"])
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
        raise NotImplementedError("tier assertions land with the classifier")
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
    args = parser.parse_args(argv)
    report = check(args.cache_dir, args.golden)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
