"""Coverage stage, declared mode: membership edges from declared feed locations.

Which places does a feed serve? With no crawl artifact the answer comes from
what the catalogues declare: the seed stage resolved each feed's declared
municipality, subdivision or country to a gazetteer place, and this stage turns
those placements into candidate edges — one ``(place, feed)`` row each, at the
plan's flat declared confidences (always below the review cutoff), with
``tier = "unknown"`` and no selector, since nothing measured which routes or
stops are involved.

Declared edges propagate explicitly. A crawled feed would reach its city's
ancestors and metros for free, since its stops fall inside every enclosing
polygon; a declared feed has no geometry to test, so an edge to a city also
yields edges to the city's administrative ancestors and to every metro in its
``metro_ids``, at the same confidence. Without that a declared-only feed would be
invisible to the bare-name query, which promotes to the default metro.

A GTFS-RT feed linked to a static feed gets no edges here: it inherits the static
feed's membership in the edge-override stage, after curation, so a curated change
to the static feed reaches its companion. An unlinked GTFS-RT feed falls back to
declared coverage like any uncrawlable feed. Crawled evidence supersedes all of
this feed by feed once the crawl exists.
"""

import collections
import datetime

from index_build import store

COVERAGE_POINTER = "coverage.json"
FEEDS_ARTIFACT = "feeds_covered.jsonl"
EDGES_ARTIFACT = "edges_candidate.jsonl"


class CoverageError(RuntimeError):
    """The coverage inputs do not describe one consistent build."""


def _check_lineage(resolve_manifest, seed_manifest, expanded_manifest):
    """Refuse a mixed snapshot: the three inputs move independently.

    The resolved feeds and the seed placements must descend from the same
    crosswalk (same catalogue ``sources``), and the placements and the expanded
    places from the same Overture release — otherwise a partial rerun or a
    concurrent republish would silently mix builds.
    """
    if resolve_manifest.get("sources") != seed_manifest.get("sources"):
        raise CoverageError(
            "resolved feeds and seed placements come from different "
            "catalogue versions; re-run the pipeline in stage order"
        )
    if seed_manifest.get("overture_release") != expanded_manifest.get(
        "overture_release"
    ):
        raise CoverageError(
            "seed placements and expanded places come from different "
            "Overture releases; re-run the pipeline in stage order"
        )


# Flat declared membership confidences: an exact municipality match, or a
# coarser subdivision/country match. Both sit below the 0.70 review cutoff.
DECLARED_CONFIDENCE = {"municipality": 0.50, "subdivision": 0.35, "country": 0.35}
REVIEW_CUTOFF = 0.70


def _edge(place_id, feed_id, confidence, placement):
    return {
        "place_id": place_id,
        "feed_id": feed_id,
        "tier": "unknown",
        "confidence": confidence,
        "tier_confidence": 0.0,
        "method": "inferred",
        "rehomed_from": [],
        "evidence": {
            "declared_level": placement["level"],
            "declared_place_id": placement["place_id"],
        },
        "curation": None,
        "merged_evidence": [],
        "curation_history": [],
        "classification_fingerprint": None,
        "fingerprint_kind": "none",
        "selector_state": "unavailable",
        "selector": None,
        "needs_review": confidence < REVIEW_CUTOFF,
    }


def _reach(place_id, places):
    """The place, its administrative ancestors, and its metros — by id."""
    reached = []
    seen = set()
    current = place_id
    while current and current not in seen and current in places:
        seen.add(current)
        reached.append(current)
        current = places[current].get("parent_id")
    for metro_id in places[place_id].get("metro_ids") or []:
        if metro_id in places and metro_id not in seen:
            seen.add(metro_id)
            reached.append(metro_id)
    return reached


def _canonical_ids(feeds):
    """Every resolved feed id and alias, mapped to the feed's canonical id.

    Placements were recorded against the crosswalk ids; an identity override may
    have renamed a feed since, keeping the old id in its aliases. The resolve
    stage guarantees this namespace is unique.
    """
    lookup = {}
    for feed in feeds:
        for key in [feed["feed_id"], *(feed.get("aliases") or [])]:
            lookup[key] = feed["feed_id"]
    return lookup


def link_static_feeds(feeds, canonical):
    """Canonicalise each GTFS-RT feed's ``static_feed_id``; return the linked ids.

    The link was recorded against crosswalk ids, so it is rewritten to the
    static feed's canonical id (a rename keeps the old one as an alias). A link
    naming no resolved feed — or a target that is not a static GTFS feed (a
    self, GTFS-RT or GBFS reference) — is dangling: the RT feed is left
    unlinked, so it falls back to declared coverage rather than waiting on an
    inheritance that could never happen, and the bad id is recorded on the feed.
    """
    by_id = {feed["feed_id"]: feed for feed in feeds}
    linked = set()
    for feed in feeds:
        if feed.get("spec") != "gtfs-rt" or not feed.get("static_feed_id"):
            continue
        static_id = canonical.get(feed["static_feed_id"])
        target = by_id.get(static_id) if static_id else None
        if (
            target is None
            or target.get("spec") != "gtfs"
            or static_id == feed["feed_id"]
        ):
            feed["dangling_static_feed_id"] = feed["static_feed_id"]
            feed["static_feed_id"] = None
            # The row is now explicitly unlinked; the method must say so too.
            feed["static_link_method"] = "none"
            continue
        feed["static_feed_id"] = static_id
        linked.add(feed["feed_id"])
    return linked


def declared_edges(feeds, places, placements):
    """Candidate edges for the placements, and what could not be placed.

    Returns ``(edges, unknown_place_ids, unmatched_feed_ids)``: one edge per
    reached ``(place, feed)`` pair, keeping the higher confidence when a feed
    reaches a place twice; the placement place ids absent from the expanded
    places; and the placement feed ids that match no resolved feed. A linked
    GTFS-RT feed's placements are skipped — it inherits later.
    """
    canonical = _canonical_ids(feeds)
    linked = link_static_feeds(feeds, canonical)
    by_key = {}
    unknown_places = set()
    unmatched_feeds = set()
    for placement in placements:
        feed_id = canonical.get(placement["feed_id"])
        if feed_id is None:
            unmatched_feeds.add(placement["feed_id"])
            continue
        if feed_id in linked:
            continue
        if placement["place_id"] not in places:
            unknown_places.add(placement["place_id"])
            continue
        confidence = DECLARED_CONFIDENCE[placement["level"]]
        for place_id in _reach(placement["place_id"], places):
            key = (place_id, feed_id)
            if key not in by_key or by_key[key]["confidence"] < confidence:
                by_key[key] = _edge(place_id, feed_id, confidence, placement)
    edges = [by_key[key] for key in sorted(by_key)]
    return edges, sorted(unknown_places), sorted(unmatched_feeds)


def cover(cache_dir):
    """Derive declared membership edges; publish the ``coverage`` generation.

    Reads the resolved feeds, the expanded places and the seed placements, and
    writes ``feeds_covered.jsonl`` (every feed stamped with its
    ``coverage_source``) and ``edges_candidate.jsonl``. Returns the manifest.
    """
    directory = store.open_subdir(cache_dir, "coverage")
    try:
        with store.exclusive_writer(directory):
            feeds, resolve_manifest = store.read_jsonl(
                cache_dir / "resolve", "feeds_resolved.json", "feeds_resolved.jsonl"
            )
            place_rows, expanded_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "expanded.json", "places_expanded.jsonl"
            )
            placements, seed_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "seed.json", "feed_places.jsonl"
            )
            _check_lineage(resolve_manifest, seed_manifest, expanded_manifest)
            places = {place["place_id"]: place for place in place_rows}
            edges, unknown_places, unmatched_feeds = declared_edges(
                feeds, places, placements
            )

            covered = {edge["feed_id"] for edge in edges}
            for feed in feeds:
                feed["coverage_source"] = (
                    "declared" if feed["feed_id"] in covered else None
                )

            manifest = {
                "source": "coverage",
                "mode": "declared",
                "sources": resolve_manifest.get("sources"),
                "overture_release": expanded_manifest.get("overture_release"),
                "feeds": len(feeds),
                "feeds_covered": len(covered),
                "linked_rt_feeds": sum(
                    1
                    for feed in feeds
                    if feed.get("spec") == "gtfs-rt" and feed.get("static_feed_id")
                ),
                "dangling_static_links": sorted(
                    feed["feed_id"]
                    for feed in feeds
                    if feed.get("dangling_static_feed_id")
                ),
                "edges": len(edges),
                "edges_by_place_kind": dict(
                    collections.Counter(places[e["place_id"]]["kind"] for e in edges)
                ),
                "unknown_place_ids": unknown_places,
                "unmatched_feed_ids": unmatched_feeds,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "coverage",
                COVERAGE_POINTER,
                {
                    FEEDS_ARTIFACT: store.jsonl_chunks(feeds),
                    EDGES_ARTIFACT: store.jsonl_chunks(edges),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
