"""Stage: relevance of every final edge, between curate and prune.

Tier says what kind of service a feed runs in a place; relevance says how much
that place should care. Every edge gets a ``relevance_category`` from its
tier (local → primary, regional → secondary, national → tertiary,
international → international, unknown stays unknown at relevance 0), a
``relevance`` score in [0, 1] within the category and ``cross_border``, true
when the place's country is not the feed's home country (or the feed has
none).

The scoring unit is the ``(feed, place)`` pair: every tier edge of a pair
carries the same ``service`` struct (asserted here), so the pair is scored
once and each of its edges gets that score. For city, metro and region places
``relevance = W_PLACE × share_of_place + W_FEED × share_of_feed``:
``share_of_place`` is the pair's departures per day (or stops, when any pair
of the place lacks a crawled calendar — one basis per place, recorded as
``share_basis``) over the place total; ``share_of_feed`` is the pair's stops
over the feed's stops in its home country (all its countries when it has no
home). For country places ``share_of_feed`` is replaced by breadth: the
cities the feed serves in the country over all served cities there. The
weights live here and nothing else in the pipeline reads them.
"""

import collections
import contextlib
import datetime
import statistics

from transitio_index import classify, store

RANK_POINTER = classify.RANK_POINTER
EDGES_ARTIFACT = classify.RANKED_EDGES_ARTIFACT
FEEDS_ARTIFACT = classify.RANKED_FEEDS_ARTIFACT

W_PLACE = 0.7
W_FEED = 0.3
CATEGORY_BY_TIER = {
    "local": "primary",
    "regional": "secondary",
    "national": "tertiary",
    "international": "international",
    "unknown": "unknown",
}
# Place kinds scored by the feed's stop share; a country is scored by breadth.
FEED_SHARE_KINDS = ("city", "metro", "region")


class RankError(RuntimeError):
    """The ranking inputs do not describe one consistent build."""


def _pair_value(service, basis):
    value = service.get("departures_per_day" if basis == "departures" else "stops")
    return float(value or 0.0)


def rank_edges(edges, feeds, places):
    """The edges with their relevance fields, and the stage report.

    ``places`` maps place ids to expanded place rows (``kind``,
    ``country_code``); ``feeds`` are the curated feed rows with classify's
    ``home_country`` and ``country_stops``.
    """
    feed_by_id = {}
    for feed in feeds:
        if feed["feed_id"] in feed_by_id:
            raise RankError(f"duplicate feed {feed['feed_id']!r}")
        feed_by_id[feed["feed_id"]] = feed
    pairs = {}
    for edge in edges:
        key = (edge["feed_id"], edge["place_id"])
        service = edge.get("service") or {}
        if pairs.setdefault(key, service) != service:
            raise RankError(
                f"{edge['feed_id']} at {edge['place_id']}: the pair's tier edges "
                "carry different service structs"
            )
        if edge["place_id"] not in places:
            raise RankError(f"edge to an unknown place {edge['place_id']!r}")
        if edge["feed_id"] not in feed_by_id:
            raise RankError(f"edge of an unknown feed {edge['feed_id']!r}")
        if edge["tier"] not in CATEGORY_BY_TIER:
            raise RankError(f"edge with an unknown tier {edge['tier']!r}")
    by_place = collections.defaultdict(list)
    for key in pairs:
        by_place[key[1]].append(key)
    basis, totals = {}, {}
    for place_id, keys in by_place.items():
        crawled = all(pairs[k].get("departures_per_day") is not None for k in keys)
        basis[place_id] = "departures" if crawled else "stops"
        totals[place_id] = sum(_pair_value(pairs[k], basis[place_id]) for k in keys)
    served_cities = collections.defaultdict(set)
    feed_cities = collections.defaultdict(set)
    for feed_id, place_id in pairs:
        place = places[place_id]
        if place.get("kind") == "city":
            served_cities[place.get("country_code")].add(place_id)
            feed_cities[(feed_id, place.get("country_code"))].add(place_id)

    ranked = []
    categories = collections.Counter()
    by_kind = collections.defaultdict(list)
    cross_border = 0
    for edge in edges:
        feed = feed_by_id[edge["feed_id"]]
        place = places[edge["place_id"]]
        service = pairs[(edge["feed_id"], edge["place_id"])]
        home = feed.get("home_country")
        country = place.get("country_code")
        category = CATEGORY_BY_TIER[edge["tier"]]
        evidence = dict(edge.get("evidence") or {})
        evidence["share_basis"] = basis[edge["place_id"]]
        total = totals[edge["place_id"]]
        share_of_place = (
            _pair_value(service, basis[edge["place_id"]]) / total if total else 0.0
        )
        evidence["share_of_place"] = share_of_place
        if place.get("kind") in FEED_SHARE_KINDS:
            country_stops = feed.get("country_stops") or {}
            denominator = (
                country_stops.get(home, 0) if home else sum(country_stops.values())
            )
            if denominator:
                second = min(1.0, float(service.get("stops") or 0) / denominator)
            else:
                second = 0.0
                evidence["relevance_note"] = "no_country_stops"
            evidence["share_of_feed"] = second
        else:
            cities = served_cities.get(country, ())
            second = (
                len(feed_cities.get((edge["feed_id"], country), ())) / len(cities)
                if cities
                else 0.0
            )
            evidence["breadth"] = second
        relevance = (
            0.0 if category == "unknown" else W_PLACE * share_of_place + W_FEED * second
        )
        crossing = home is None or country != home
        cross_border += crossing
        categories[category] += 1
        by_kind[place.get("kind")].append(relevance)
        ranked.append(
            {
                **edge,
                "relevance_category": category,
                "relevance": relevance,
                "cross_border": crossing,
                "needs_review": bool(edge.get("needs_review")) or category == "unknown",
                "evidence": evidence,
            }
        )
    report = {
        "edges_by_category": dict(categories),
        "cross_border_edges": cross_border,
        "share_basis_by_place": dict(collections.Counter(basis.values())),
        "relevance_by_kind": {kind: _quantiles(v) for kind, v in by_kind.items()},
    }
    return ranked, report


def _quantiles(values):
    summary = {"edges": len(values), "min": min(values), "max": max(values)}
    if len(values) >= 2:
        q1, median, q3 = statistics.quantiles(values, n=4, method="inclusive")
        summary.update({"p25": q1, "median": median, "p75": q3})
    return summary


def rank(cache_dir):
    """Rank the curated edges; publish the ``rank`` generation. Returns the
    manifest."""
    with contextlib.ExitStack() as stack:
        # The global lock order — this stage's own directory is its last
        # entry — then the crawl's.
        for subdir in classify.EDGE_STAGES:
            directory = store.open_subdir(cache_dir, subdir)
            stack.callback(directory.close)
            stack.enter_context(store.exclusive_writer(directory))
        from transitio_index import crawl

        stack.enter_context(crawl.reading(cache_dir))
        try:
            feeds, edges, curated = classify.read_edges(
                cache_dir, locked=True, ranked=False
            )
        except classify.ClassifyError as error:
            raise RankError(str(error)) from error
        if curated is None or curated.get("source") != "curate":
            raise RankError("no curate generation to rank; run curate")
        place_rows, expanded = store.read_jsonl(
            cache_dir / "gazetteer", "expanded.json", "places_expanded.jsonl"
        )
        if curated.get("expanded_generation") != expanded.get("generation"):
            raise RankError(
                "the curated edges were not derived from the current expanded "
                "places; re-run the pipeline in stage order"
            )
        places = {place["place_id"]: place for place in place_rows}
        ranked, report = rank_edges(edges, feeds, places)
        lineage = (
            "mode",
            "sources",
            "overture_release",
            "classify_generation",
            "coverage_generation",
            "feeds_overrides_sha256",
            "stale_feed_overrides",
            "expanded_generation",
            "overrides_sha256",
            "stale_overrides",
            "classifier",
        )
        by_tier = collections.Counter(e["tier"] for e in ranked)
        manifest = {
            "source": "rank",
            **{key: curated.get(key) for key in lineage},
            "curate_generation": curated.get("generation"),
            "weights": {"place": W_PLACE, "feed": W_FEED},
            "feeds": len(feeds),
            "edges": len(ranked),
            # The golden drift gate reads these from the latest edge stage.
            "edges_by_tier": dict(by_tier),
            "unknown_share": (by_tier["unknown"] / len(ranked)) if ranked else 0.0,
            "needs_review": sum(1 for e in ranked if e["needs_review"]),
            "edges_near_threshold": classify.near_threshold_count(ranked),
            **report,
            "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        return store.publish(
            cache_dir / "rank",
            RANK_POINTER,
            {
                FEEDS_ARTIFACT: store.jsonl_chunks(feeds),
                EDGES_ARTIFACT: store.jsonl_chunks(ranked),
            },
            manifest,
            held=directory,
        )
