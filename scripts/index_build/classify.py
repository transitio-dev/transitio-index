"""Stage 5: tier classification of the candidate edges.

Tier is a property of routes. For every crawled feed with complete
``stop_times.txt`` the stage rebuilds each route's geography from the crawl —
its stops, span (greatest distance between any two stops), median inter-stop
distance and the countries its stops fall in — and runs the plan's decision
table over it, first match wins, with the margin penalty for a route decided
within 20 % of a threshold. A route serves a place when enough of its stops lie
inside it; a feed then gets one edge per ``(place, tier)`` for which a serving
route classifies to that tier, its ``tier_confidence`` the stop-share-weighted
mean of those routes. A feed that skipped ``stop_times.txt`` under the crawl
predicate is single-place and fixed-tier by construction, so every route serves
every candidate place at its geography-free tier.

``unknown`` is an explicit tier, never a null: a route missing a signal a rule
needs, a feed without route evidence, and every declared-only feed yield
``tier = "unknown"`` with ``tier_confidence = 0.0`` and ``needs_review``, while
the edge keeps its membership and the coverage stage's service level — not
knowing a route's tier says nothing about whether the feed serves the place.
Every tier edge of a ``(place, feed)`` pair carries the same ``service``
struct: the pair's scheduled stops and serving routes in the place, and its
stop-events per average calendar day when the calendar was crawled.
Selectors and fingerprints are the next stage's concern; every edge leaves
here ``selector_state = "unavailable"``.
"""

import collections
import contextlib
import csv
import datetime
import heapq
import io
import math
import statistics
import types

from index_build import coverage, crawl, store

CLASSIFY_POINTER = "edges.json"
EDGES_ARTIFACT = "edges.jsonl"
FEEDS_ARTIFACT = "feeds_classified.jsonl"

REVIEW_CUTOFF = coverage.REVIEW_CUTOFF
# A route serves a place with this many SCHEDULED stops inside it. One stop is
# service: an intercity train has one station per municipality and serves
# every one of them; a neighbourhood route's single stop serves the people
# around it.
ROUTE_MIN_STOPS = 1
# A route decided within this fraction of a threshold has its confidence
# multiplied by the penalty.
MARGIN = 0.20
MARGIN_PENALTY = 0.7

RAIL_SPAN_KM = 150.0
WATER_SPAN_KM = 50.0
BUS_LOCAL_MEDIAN_KM = 1.5
BUS_LOCAL_SPAN_KM = 40.0
BUS_REGIONAL_MEDIAN_KM = 10.0
BUS_REGIONAL_SPAN_KM = 200.0
# A route's span is exact over every pair of its distinct stops up to this
# many; past it the span is unmeasurable within bounds and stays a missing
# signal — no geometric shortcut is used.
SPAN_MAX_STOPS = 2048
# The median inter-stop distance is taken over the legs of this many
# DISTINCT longest stop patterns per route, found among a bounded,
# deterministic sample of trips (PATTERN_SAMPLE per distinct length, for
# the PATTERN_SAMPLE longest lengths).
PATTERN_SAMPLE = 8

EARTH_RADIUS_KM = 6371.0088


class ClassifyError(RuntimeError):
    """The classification inputs do not describe one consistent build."""


def haversine_km(a, b):
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))


def _near(value, threshold):
    return abs(value - threshold) <= MARGIN * threshold


def _decide(tier, confidence, rule, pairs=()):
    margin = any(_near(value, threshold) for value, threshold in pairs)
    return {
        "tier": tier,
        "tier_confidence": confidence * MARGIN_PENALTY if margin else confidence,
        "rule": rule,
        "margin": margin,
    }


_UNKNOWN = {"tier": "unknown", "tier_confidence": 0.0, "rule": 9, "margin": False}


def classify_route(route_type, countries, span_km, median_km):
    """The decision table, first match wins; ``{tier, tier_confidence, rule,
    margin}``.

    A rule whose signal is missing (``None``) is skipped rather than decided;
    rules 6–8 are guarded on both bus signals being known, so an unmeasured
    bus route can never be swallowed as ``national``.
    """
    if countries is not None and len(countries) >= 2:
        return _decide("international", 0.95, 1)
    if route_type is not None and route_type >= 100:
        if 100 <= route_type <= 117:
            if span_km is None:
                return dict(_UNKNOWN)
            tier = "regional" if span_km <= RAIL_SPAN_KM else "national"
            return _decide(tier, 0.95, 2, [(span_km, RAIL_SPAN_KM)])
        if 200 <= route_type <= 209:
            return _decide("national", 0.95, 2)
        if (
            400 <= route_type <= 405
            or 700 <= route_type <= 716
            or 800 <= route_type <= 999
        ):
            return _decide("local", 0.95, 2)
        if route_type == 1000:
            if span_km is None:
                return dict(_UNKNOWN)
            tier = "local" if span_km <= WATER_SPAN_KM else "regional"
            return _decide(tier, 0.95, 2, [(span_km, WATER_SPAN_KM)])
        return dict(_UNKNOWN)
    if route_type in (0, 1):
        return _decide("local", 0.90, 3)
    if route_type == 2 and span_km is not None:
        tier = "regional" if span_km <= RAIL_SPAN_KM else "national"
        return _decide(tier, 0.75, 4, [(span_km, RAIL_SPAN_KM)])
    if route_type == 4 and span_km is not None:
        tier = "local" if span_km <= WATER_SPAN_KM else "regional"
        return _decide(tier, 0.75, 5, [(span_km, WATER_SPAN_KM)])
    if route_type == 3 and span_km is not None and median_km is not None:
        # "Any threshold it was decided by": rules 6–8 are decided against
        # every bus threshold, passed or failed, so the borderline coach that
        # looks like a long city bus is penalised whichever side it lands.
        pairs = [
            (median_km, BUS_LOCAL_MEDIAN_KM),
            (span_km, BUS_LOCAL_SPAN_KM),
            (median_km, BUS_REGIONAL_MEDIAN_KM),
            (span_km, BUS_REGIONAL_SPAN_KM),
        ]
        if median_km <= BUS_LOCAL_MEDIAN_KM and span_km <= BUS_LOCAL_SPAN_KM:
            return _decide("local", 0.85, 6, pairs)
        if median_km <= BUS_REGIONAL_MEDIAN_KM and span_km <= BUS_REGIONAL_SPAN_KM:
            return _decide("regional", 0.65, 7, pairs)
        return _decide("national", 0.60, 8, pairs)
    return dict(_UNKNOWN)


def route_serves(stops_in_place, route_stops, *, route_min_stops=ROUTE_MIN_STOPS):
    """Whether a route with ``route_stops`` stops serves a place holding
    ``stops_in_place`` of its scheduled stops."""
    return route_stops > 0 and stops_in_place >= route_min_stops


class _LaterFirst:
    """A heap key that ranks the lexicographically LATER trip id as smaller,
    so the bounded heap evicts it first and keeps the earlier id on ties."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return self.value > other.value

    def __eq__(self, other):
        return self.value == other.value


def _reader(opened):
    """csv rows over a binary file, leaving the file open afterwards.

    A ``TextIOWrapper`` closes its underlying file when it is collected;
    detaching keeps ``opened`` usable for a second pass.
    """
    text = io.TextIOWrapper(opened, encoding="utf-8-sig", errors="strict")
    try:
        yield from csv.DictReader(text)
    finally:
        text.detach()


def _read_routes(opened):
    """``{route_id: route_type or None}`` — an unparsable type is a missing
    signal, never a guessed one."""
    routes = {}
    for row in _reader(opened):
        route_id = row.get("route_id") or ""
        if not route_id:
            continue
        value = (row.get("route_type") or "").strip()
        routes[route_id] = int(value) if value.isdigit() else None
    return routes


def _read_trips(opened, routes):
    """``({trip_id: route_id}, {trip_id: service_id}, orphans)`` for trips of
    known routes, ids verbatim; ``orphans`` counts trips naming a route the
    feed lacks."""
    trips = {}
    services = {}
    orphans = 0
    for row in _reader(opened):
        trip_id = row.get("trip_id") or ""
        route_id = row.get("route_id") or ""
        if not trip_id:
            continue
        if route_id in routes:
            trips[trip_id] = route_id
            services[trip_id] = row.get("service_id") or ""
        else:
            orphans += 1
    return trips, services, orphans


def _read_calendar(calendar, calendar_dates):
    """``(active_days, span_days)``: per service id, the number of dates it
    runs over the feed's calendar span, and that span in days.

    Weekday flags apply over each service's own date range; ``calendar_dates``
    adds (type 1) or removes (type 2) single dates. The span is the whole
    calendar's extent, so a service running only on Sundays counts one day
    in seven. ``calendar`` may be None (a feed with only exceptions). Days
    are counted arithmetically — a legal row may span year 1 to 9999, and
    walking it date by date would be millions of steps per service.
    """
    windows = {}
    if calendar is not None:
        for row in _reader(calendar):
            service_id = row.get("service_id") or ""
            start = _date(row.get("start_date"))
            end = _date(row.get("end_date"))
            if not service_id or start is None or end is None or end < start:
                continue
            flags = [
                (row.get(day) or "").strip() == "1"
                for day in (
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                )
            ]
            windows[service_id] = (start, end, flags)
    added = collections.defaultdict(set)
    removed = collections.defaultdict(set)
    if calendar_dates is not None:
        for row in _reader(calendar_dates):
            service_id = row.get("service_id") or ""
            date = _date(row.get("date"))
            kind = (row.get("exception_type") or "").strip()
            if not service_id or date is None:
                continue
            if kind == "1":
                added[service_id].add(date)
            elif kind == "2":
                removed[service_id].add(date)
    dates = [w[0] for w in windows.values()] + [w[1] for w in windows.values()]
    for exceptions in added.values():
        dates.extend(exceptions)
    if not dates:
        return {}, 0
    first, last = min(dates), max(dates)
    span_days = (last - first).days + 1
    active_days = {}
    for service_id in set(windows) | set(added):
        window = windows.get(service_id)
        extra = added.get(service_id, set())
        count = _window_days(window) if window else 0
        # An exception counts only where it changes the answer: adding a
        # date the window already runs, or removing one it never ran, is
        # a no-op — and a removal wins over an addition of the same date.
        count += sum(1 for date in extra if not _in_window(window, date))
        count -= sum(
            1
            for date in removed.get(service_id, set())
            if date in extra or _in_window(window, date)
        )
        active_days[service_id] = count
    return active_days, span_days


def _window_days(window):
    """How many dates in ``(start, end, flags)`` fall on a flagged weekday."""
    start, end, flags = window
    days = (end - start).days + 1
    weeks, rest = divmod(days, 7)
    count = weeks * sum(flags)
    for offset in range(rest):
        count += flags[(start.weekday() + offset) % 7]
    return count


def _in_window(window, date):
    if window is None:
        return False
    start, end, flags = window
    return start <= date <= end and flags[date.weekday()]


def _date(value):
    value = (value or "").strip()
    if len(value) != 8 or not value.isdigit():
        return None
    try:
        return datetime.date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError:
        return None


def _read_stop_times(opened, trip_routes, trip_services, weights=None):
    """Per route, its distinct stop ids and its longest DISTINCT stop
    patterns, in two streamed passes — the file is never held whole.

    Row order in GTFS is arbitrary and routes carry several patterns
    (branches, express runs, both directions), so the inter-stop legs come
    from up to ``PATTERN_SAMPLE`` distinct patterns per route. The first
    pass counts rows per trip and keeps, per route and per distinct trip
    length, the ``PATTERN_SAMPLE`` smallest trip ids, then the
    ``PATTERN_SAMPLE`` largest lengths — deterministic, and a shorter
    pattern can never be crowded out by duplicates of a longer one; the
    second pass keeps only those trips' rows, sorted by ``stop_sequence``;
    identical patterns then collapse BEFORE the sample is cut. Two
    different patterns of the very same length can still compete for that
    length's slots — a bounded approximation, never unbounded memory.

    ``weights`` maps a service id to its share of calendar days; with it,
    every scheduled stop-event adds that share to its stop's departures per
    day as the row streams by, one float per stop — the fourth result, or
    None without a calendar.
    """
    stops = collections.defaultdict(set)
    trip_rows = collections.Counter()
    departures = collections.Counter()
    dangling = 0
    for row in _reader(opened):
        trip_id = row.get("trip_id") or ""
        route_id = trip_routes.get(trip_id)
        stop_id = row.get("stop_id") or ""
        if route_id is None:
            # A row whose trip the feed does not join: counted, so a feed
            # classified from a partial join says so in its evidence.
            dangling += bool(trip_id)
            continue
        if not stop_id:
            continue
        trip_rows[trip_id] += 1
        if (row.get("pickup_type") or "").strip() == "1" and (
            row.get("drop_off_type") or ""
        ).strip() == "1":
            # Neither boarding nor alighting: traversal, not service. The
            # row still shapes the trip's pattern (legs), never its stops.
            continue
        stops[route_id].add(stop_id)
        if weights is not None:
            departures[stop_id] += weights.get(trip_services.get(trip_id, ""), 0.0)
    # Candidates are bounded PER DISTINCT TRIP LENGTH: for each route and
    # each stop count, a small heap keeps the PATTERN_SAMPLE smallest trip
    # ids, and the PATTERN_SAMPLE largest lengths are then taken — so
    # duplicates of one long pattern can never crowd out a shorter branch
    # (only two different patterns of the very same length can still
    # compete for the slots). Memory stays O(routes × sample²); the
    # per-trip counter itself is the size of the trip->route map the join
    # already needs.
    by_length = collections.defaultdict(lambda: collections.defaultdict(list))
    for trip_id, count in trip_rows.items():
        heap = by_length[trip_routes[trip_id]][count]
        item = _LaterFirst(trip_id)
        if len(heap) < PATTERN_SAMPLE:
            heapq.heappush(heap, item)
        else:
            heapq.heappushpop(heap, item)
    wanted = {}
    for route_id, lengths in by_length.items():
        for count in sorted(lengths, reverse=True)[:PATTERN_SAMPLE]:
            for later in lengths[count]:
                wanted[later.value] = route_id
    opened.seek(0)
    rows = collections.defaultdict(list)
    unordered = set()
    for row in _reader(opened):
        trip_id = row.get("trip_id") or ""
        stop_id = row.get("stop_id") or ""
        if trip_id not in wanted:
            continue
        value = (row.get("stop_sequence") or "").strip()
        if not stop_id or not value.isdigit():
            # A row with no stop, or no sequence, leaves a hole in the trip
            # that would be bridged by a fabricated leg: no legs from it.
            unordered.add(trip_id)
            continue
        rows[trip_id].append((int(value), stop_id))
    patterns = collections.defaultdict(set)
    for trip_id, pairs in rows.items():
        if trip_id in unordered:
            continue
        patterns[wanted[trip_id]].add(tuple(stop_id for _, stop_id in sorted(pairs)))
    sequences = {
        route_id: sorted(found, key=lambda s: (-len(s), s))[:PATTERN_SAMPLE]
        for route_id, found in patterns.items()
    }
    return (
        stops,
        sequences,
        dangling,
        (dict(departures) if weights is not None else None),
    )


def _span_km(points):
    """The greatest haversine distance between any two points, or None
    when there are more than ``SPAN_MAX_STOPS`` distinct points.

    Exact, always: no hull, projection or sampling stands between the
    stops and the answer, so no metric mismatch can hide the true farthest
    pair. The scan is bounded by the cap squared; a route past the cap (an
    absurd or hostile feed) gets a MISSING span and classifies as unknown
    rather than to a guessed tier.
    """
    points = list(set(points))
    if len(points) > SPAN_MAX_STOPS:
        return None
    best = 0.0
    for i, a in enumerate(points):
        for b in points[i + 1 :]:
            best = max(best, haversine_km(a, b))
    return best


def _route_geography(stop_ids, sequences, coords):
    """``(span_km, median_km, stop_count)`` for a route's stops.

    ``stop_count`` is every distinct stop the route lists — the service
    denominator — while the geometry is a missing signal (``None``) unless
    EVERY stop has usable coordinates: a span or median over a subset would
    be presented as complete when it is not.
    """
    points = [coords[s] for s in stop_ids if s in coords]
    if not points or len(points) < len(stop_ids):
        return None, None, len(stop_ids)
    span = _span_km(points)
    if span is None:
        # Unmeasurable geometry: every geography-dependent rule must skip.
        return None, None, len(points)
    if any(s not in coords for sequence in sequences for s in sequence):
        # A pattern stop without coordinates — a traversal-only stop sits in
        # the patterns but not the scheduled set — would leave the legs a
        # subset presented as complete: no median.
        return span, None, len(points)
    legs = [
        haversine_km(coords[a], coords[b])
        for sequence in sequences
        for a, b in zip(sequence, sequence[1:])
    ]
    median = statistics.median(legs) if legs else None
    return span, median, len(points)


def _calendar_weights(feed_dir, state):
    """``{service_id: share of calendar days it runs}`` from the crawled
    calendar members, streamed under their digests; None when the feed has
    neither file, so no departure count can be measured.

    A calendar member the state records but that fails verification is a
    state mismatch, raised like any other unverifiable member.
    """
    recorded = state.get("member_sha256") or {}
    names = [n for n in ("calendar.txt", "calendar_dates.txt") if n in recorded]
    if not names:
        return None
    with contextlib.ExitStack() as stack:
        opened = {}
        for name in names:
            member = stack.enter_context(crawl.verified_member(feed_dir, state, name))
            if member is None:
                raise ValueError(f"{name}: not the member the state recorded")
            opened[name] = member
        active_days, span_days = _read_calendar(
            opened.get("calendar.txt"), opened.get("calendar_dates.txt")
        )
    if not span_days:
        return None
    return {service: days / span_days for service, days in active_days.items()}


def _members(feed_dir, state, names):
    """The digest-verified members parsed, or None on an unverifiable or
    unparsable member — one feed's problem, never the run's. Only data
    errors are caught; a programming defect surfaces."""
    if state.get("members_requested") != sorted(crawl.MEMBERS):
        # A cache the crawler asked fewer members of (before the calendar
        # files joined the set) cannot say whether the feed has them:
        # classifying it would pass off "never fetched" as "not there".
        raise ClassifyError(
            f"{state.get('feed_id')}: the crawl state predates the current "
            "member set; re-run the crawl stage"
        )
    parsed = {}
    try:
        with crawl.verified_member(feed_dir, state, "stops.txt") as opened:
            if opened is None:
                return None
            rows, _ = crawl.stop_rows(opened)
        parsed["coords"] = {stop_id: (x, y) for stop_id, x, y in rows if stop_id}
        with crawl.verified_member(feed_dir, state, "routes.txt") as opened:
            if opened is None:
                return None
            parsed["routes"] = _read_routes(opened)
        if "trips.txt" in names:
            with crawl.verified_member(feed_dir, state, "trips.txt") as opened:
                if opened is None:
                    return None
                trip_routes, trip_services, orphans = _read_trips(
                    opened, parsed["routes"]
                )
            # The calendar comes first so each stop-event is weighted as it
            # streams by: one float per stop, never an event table.
            weights = _calendar_weights(feed_dir, state)
            with crawl.verified_member(feed_dir, state, "stop_times.txt") as opened:
                if opened is None:
                    return None
                (
                    parsed["stops"],
                    parsed["sequences"],
                    dangling,
                    parsed["stop_departures"],
                ) = _read_stop_times(opened, trip_routes, trip_services, weights)
            parsed["join_gaps"] = {
                "orphan_trips": orphans,
                "dangling_stop_times": dangling,
            }
    except crawl.MEMBER_ERRORS:
        return None
    return parsed


def _edge(candidate, tier, tier_confidence, evidence, needs_review):
    edge = dict(candidate)
    edge["tier"] = tier
    edge["tier_confidence"] = tier_confidence
    edge["evidence"] = evidence
    edge["needs_review"] = needs_review
    return edge


def _unknown_edge(candidate, reason, route_min_stops):
    evidence = dict(candidate.get("evidence") or {})
    evidence.update(
        {
            "route_min_stops": route_min_stops,
            "review_cutoff": REVIEW_CUTOFF,
            "unknown_reason": reason,
        }
    )
    return _edge(candidate, "unknown", 0.0, evidence, True)


def _tier_edges(candidate, contributing, route_min_stops, extra=None, service=None):
    """One edge per tier over the contributing ``(route, decision, weight,
    signals)`` tuples, ``tier_confidence`` the weight-averaged decision."""
    by_tier = collections.defaultdict(list)
    for item in contributing:
        by_tier[item["decision"]["tier"]].append(item)
    edges = []
    for tier in sorted(by_tier):
        items = by_tier[tier]
        weights = sum(item["weight"] for item in items)
        tier_confidence = (
            sum(item["weight"] * item["decision"]["tier_confidence"] for item in items)
            / weights
            if weights
            else 0.0
        )
        if tier == "unknown":
            tier_confidence = 0.0
        medians = [i["median_km"] for i in items if i["median_km"] is not None]
        spans = [i["span_km"] for i in items if i["span_km"] is not None]
        types = {i["route_type"] for i in items if i["route_type"] is not None}
        evidence = dict(candidate.get("evidence") or {})
        evidence.update(
            {
                "matched_route_types": sorted(t for t in types if t < 100),
                "extended_route_types": sorted(t for t in types if t >= 100),
                "median_interstop_km": statistics.median(medians) if medians else None,
                "spread_km": max(spans) if spans else None,
                "serving_routes": len(items),
                "route_min_stops": route_min_stops,
                "review_cutoff": REVIEW_CUTOFF,
                **(extra or {}),
            }
        )
        needs_review = (
            tier == "unknown"
            or tier_confidence < REVIEW_CUTOFF
            or any(item["decision"]["margin"] for item in items)
        )
        edge = _edge(candidate, tier, tier_confidence, evidence, needs_review)
        if service is not None:
            edge["service"] = service
        edges.append(edge)
    return edges


def _place_stop_ids(route, place_id, place):
    """The route's scheduled stops inside the place. A metro adds the union
    over its member cities to whatever its own polygon (if it has one)
    placed — a minted member-union metro has no geometry of its own."""
    inside = set(route["place_stops"].get(place_id, ()))
    if place.get("kind") == "metro":
        for member in place.get("member_ids") or []:
            inside.update(route["place_stops"].get(member, ()))
    return inside


def _service_level(contributing, place_id, place, stop_departures):
    """The feed's service level in the place over its serving routes."""
    stops = set()
    for route in contributing:
        stops |= _place_stop_ids(route, place_id, place)
    departures = None
    if stop_departures is not None:
        departures = sum(stop_departures.get(stop_id, 0.0) for stop_id in stops)
    return {
        "stops": len(stops),
        "routes": len(contributing),
        "departures_per_day": departures,
    }


def _stops_inside(route, place_id, place):
    """How many DISTINCT stops of the route lie inside the place.

    Minted metros have no geometry, so a route's stops inside a metro are
    its stops inside ANY member city — the union, so a stop in two
    overlapping members counts once — taken BEFORE the service rule, so a
    route split across two members still serves the metro.
    """
    return len(_place_stop_ids(route, place_id, place))


def _classify_feed(
    candidates, feed_dir, state, lookup, places, by_overture, route_min_stops
):
    """The classified edges for one crawled feed; ``(edges, status, routes,
    dropped, join_gaps)`` — ``dropped`` counting candidate places no route
    serves, ``join_gaps`` the trips and stop_times rows the feed failed to
    join (None without route evidence).

    ``candidates`` is ``{place_id: candidate edge}``. The stop_times state
    decides the mode: ``complete`` measures each route; ``skipped`` is the
    whole-feed case the crawl predicate proved single-place and fixed-tier;
    anything else has no route evidence and yields ``unknown`` edges.
    """
    mode = (state.get("stop_times") or {}).get("state")
    names = ("trips.txt",) if mode == "complete" else ()
    parsed = _members(feed_dir, state, names)
    if parsed is None:
        return (
            [
                _unknown_edge(c, "state_mismatch", route_min_stops)
                for c in candidates.values()
            ],
            "state_mismatch",
            0,
            0,
            None,
        )
    coords = parsed["coords"]
    routes = parsed["routes"]
    lookup.ensure(crawl.cluster_boxes(list(coords.values())))
    stop_places = {}
    stop_countries = {}
    stale = set()
    for stop_id, (x, y) in coords.items():
        hit, countries, stale_here = coverage.stop_places(
            lookup, x, y, places, by_overture
        )
        stop_places[stop_id] = hit
        stop_countries[stop_id] = countries
        stale.update(stale_here)
    if stale:
        raise ClassifyError(
            "crawled stops hit QID-bearing divisions the gazetteer does not "
            f"know ({', '.join(sorted(stale)[:5])}); places_expanded predates "
            "the crawl — re-run the expand stage"
        )

    if mode == "skipped":
        # Single place, single country, geography-free tiers: every route
        # serves every candidate place, weighted equally. No parseable route
        # at all is explicit unknown, never a vanished edge.
        if not routes:
            return (
                [
                    _unknown_edge(c, "no_routes", route_min_stops)
                    for c in candidates.values()
                ],
                "no_routes",
                0,
                0,
                None,
            )
        # The crawl's skip rested on single-country, single-city conditions
        # judged against the boundary memo of ITS time; judge them again
        # against the current lookup before trusting a whole-feed claim.
        still_skipped, _ = crawl._skip_stop_times(
            types.SimpleNamespace(path=feed_dir),
            state.get("member_sha256") or {},
            lookup,
            False,
        )
        if not still_skipped:
            return (
                [
                    _unknown_edge(c, "skip_stale", route_min_stops)
                    for c in candidates.values()
                ],
                "skip_stale",
                0,
                0,
                None,
            )
        feed_countries = (
            set().union(*stop_countries.values()) if stop_countries else set()
        )
        contributing = [
            {
                "decision": classify_route(route_type, feed_countries, None, None),
                "weight": 1.0,
                "route_type": route_type,
                "span_km": None,
                "median_km": None,
            }
            for route_type in routes.values()
        ]
        edges = []
        service = {
            "stops": len(coords),
            "routes": len(routes),
            "departures_per_day": None,
        }
        for candidate in candidates.values():
            edges.extend(
                _tier_edges(candidate, contributing, route_min_stops, None, service)
            )
        return edges, "whole_feed", len(routes), 0, None

    if mode != "complete":
        return (
            [
                _unknown_edge(c, "no_route_evidence", route_min_stops)
                for c in candidates.values()
            ],
            "no_route_evidence",
            0,
            0,
            None,
        )

    # Per-route measurement, then service to each candidate place.
    measured = {}
    for route_id, route_type in routes.items():
        stop_ids = parsed["stops"].get(route_id, set())
        span, median, count = _route_geography(
            stop_ids, parsed["sequences"].get(route_id, []), coords
        )
        countries = set()
        place_stops = collections.defaultdict(set)
        for stop_id in stop_ids:
            countries.update(stop_countries.get(stop_id, ()))
            for hit in stop_places.get(stop_id, ()):
                place_stops[hit].add(stop_id)
        measured[route_id] = {
            "decision": classify_route(
                route_type, countries if count else None, span, median
            ),
            "route_type": route_type,
            "span_km": span,
            "median_km": median,
            "stop_count": count,
            "place_stops": place_stops,
        }

    edges = []
    dropped = 0
    for place_id, candidate in candidates.items():
        place = places.get(place_id) or {}
        contributing = []
        for route in measured.values():
            inside = _stops_inside(route, place_id, place)
            if route_serves(
                inside, route["stop_count"], route_min_stops=route_min_stops
            ):
                # Stop-SHARE weighted: the route's stops inside the place
                # over its stops, so a long route with a few stops in town
                # does not outweigh a short one entirely inside it.
                contributing.append(
                    {**route, "weight": inside / max(route["stop_count"], 1)}
                )
        if not contributing:
            # Coverage admitted the feed on a stop no route schedules (an
            # unused stop, a parent station): no edge — counted, so the
            # omission is loud in the manifest.
            dropped += 1
            continue
        edges.extend(
            _tier_edges(
                candidate,
                contributing,
                route_min_stops,
                {"join_gaps": parsed.get("join_gaps")},
                _service_level(
                    contributing, place_id, place, parsed.get("stop_departures")
                ),
            )
        )
    return edges, "route_stops", len(routes), dropped, parsed.get("join_gaps")


def _pointer_present(path):
    """Whether a pointer is there to be resolved — a dangling symlink is
    present (and will fail to resolve), never absent."""
    return path.is_symlink() or path.exists()


def _current_generation(cache_dir, subdir, pointer):
    """The generation id the pointer names now, None with no pointer; a
    pointer that exists but will not resolve is corruption, never absence."""
    if not _pointer_present(cache_dir / subdir / pointer):
        return None
    try:
        generation, manifest = store.resolve(cache_dir / subdir, pointer)
    except (store.StoreError, ValueError) as error:
        raise ClassifyError(
            f"the {subdir} generation is unreadable: {error}"
        ) from error
    with generation:
        pass
    return manifest.get("generation")


def _check_descends(manifest, recorded_key, current, what, rerun):
    """A recorded input generation must be the current one.

    With a current input present, a manifest that recorded no generation
    for it cannot demonstrate descent and is refused too. An input that
    never existed (neither recorded nor present) is exempt; one that was
    recorded and has since vanished is not — the edges then rest on an
    input nothing can verify.
    """
    recorded = manifest.get(recorded_key)
    if current is None and recorded is None:
        return
    if recorded != current:
        raise ClassifyError(
            f"{what} predates the current inputs; re-run the {rerun} stage"
        )


def _require_service(edges, stage):
    """Refuse edges from before the service level: a record without the
    ``service`` key, or still carrying the retired ``confidence``, was
    written by an older stage and would publish as no service at all."""
    for edge in edges:
        if "service" not in edge or "confidence" in edge:
            raise ClassifyError(
                f"the {stage} edges predate the service level; "
                "re-run the coverage and classify stages"
            )


def read_edges(cache_dir, *, locked=False):
    """``(feeds, edges, manifest)`` of the latest edge stage, or ``(None,
    None, None)`` with no edge stage at all.

    The classify generation is preferred over the coverage one, and either
    is refused (:class:`ClassifyError`) unless it descends from the CURRENT
    resolved feeds, expanded places, crawl (by digest) and — for classify —
    coverage generation: a rerun of any input that was not followed
    downstream makes the edges stale, and stale edges are never published
    or checked as fresh. One pointer resolution serves both artifacts of a generation, so
    a republish racing this read cannot pair feeds from one generation with
    edges from another; atomicity against upstream REPUBLISHES between the
    checks and a commit is the caller's — publish holds every upstream
    stage's writer lock for its whole duration.
    """
    if not _pointer_present(cache_dir / "coverage" / coverage.COVERAGE_POINTER):
        if _pointer_present(cache_dir / "classify" / CLASSIFY_POINTER):
            # Classified edges cannot exist without the coverage they
            # descend from: this is corruption, never "no edge stage".
            raise ClassifyError(
                "a classify generation exists without its coverage generation"
            )
        return None, None, None
    current_resolve = _current_generation(cache_dir, "resolve", "feeds_resolved.json")
    current_expanded = _current_generation(cache_dir, "gazetteer", "expanded.json")
    try:
        coverage_generation, coverage_manifest = store.resolve(
            cache_dir / "coverage", coverage.COVERAGE_POINTER
        )
    except (store.StoreError, ValueError) as error:
        raise ClassifyError(
            f"the coverage generation is unreadable: {error}"
        ) from error
    with coverage_generation:
        if locked:
            # The caller holds the crawl lock for its whole operation.
            current_crawl = crawl.states_digest(cache_dir)
        else:
            with crawl.reading(cache_dir):
                current_crawl = crawl.states_digest(cache_dir)
        if coverage_manifest.get("crawl_digest") != current_crawl:
            # Membership was measured against a crawl that has since moved;
            # every edge downstream of it is stale.
            raise ClassifyError(
                "the crawl changed since the coverage stage read it; "
                "re-run the coverage stage"
            )
        _check_descends(
            coverage_manifest,
            "resolve_generation",
            current_resolve,
            "coverage",
            "coverage",
        )
        _check_descends(
            coverage_manifest,
            "expanded_generation",
            current_expanded,
            "coverage",
            "coverage",
        )
        classified = None
        if _pointer_present(cache_dir / "classify" / CLASSIFY_POINTER):
            try:
                classified = store.resolve(cache_dir / "classify", CLASSIFY_POINTER)
            except (store.StoreError, ValueError) as error:
                raise ClassifyError(
                    f"the classify generation is unreadable: {error}"
                ) from error
        if classified is None:
            feeds = store.parse_jsonl(
                coverage_generation.read_bytes(coverage.FEEDS_ARTIFACT)
            )
            edges = store.parse_jsonl(
                coverage_generation.read_bytes(coverage.EDGES_ARTIFACT)
            )
            _require_service(edges, "coverage")
            return feeds, edges, coverage_manifest
    generation, manifest = classified
    with generation:
        _check_descends(
            manifest,
            "coverage_generation",
            coverage_manifest.get("generation"),
            "the classified edges",
            "classify",
        )
        _check_descends(
            manifest,
            "expanded_generation",
            current_expanded,
            "the classified edges",
            "classify",
        )
        feeds = store.parse_jsonl(generation.read_bytes(FEEDS_ARTIFACT))
        edges = store.parse_jsonl(generation.read_bytes(EDGES_ARTIFACT))
    _require_service(edges, "classified")
    return feeds, edges, manifest


def classify(cache_dir, *, lookup=None, route_min_stops=ROUTE_MIN_STOPS):
    """Classify the candidate edges; publish the ``classify`` generation.

    Reads the coverage generation (feeds and candidate edges from one pointer
    resolution), the expanded places and the crawl artifacts, and writes
    ``edges.jsonl`` — every candidate edge carried forward with its tier —
    alongside the feeds unchanged. Returns the manifest.
    """
    directory = store.open_subdir(cache_dir, "classify")
    opened_lookup = None
    crawl_lock = None
    try:
        with store.exclusive_writer(directory):
            generation, coverage_manifest = store.resolve(
                cache_dir / "coverage", coverage.COVERAGE_POINTER
            )
            with generation:
                feeds = store.parse_jsonl(
                    generation.read_bytes(coverage.FEEDS_ARTIFACT)
                )
                candidates = store.parse_jsonl(
                    generation.read_bytes(coverage.EDGES_ARTIFACT)
                )
                _require_service(candidates, "coverage")
            place_rows, expanded_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "expanded.json", "places_expanded.jsonl"
            )
            if coverage_manifest.get("expanded_generation") != expanded_manifest.get(
                "generation"
            ):
                raise ClassifyError(
                    "coverage was not derived from the current expanded places; "
                    "re-run the pipeline in stage order"
                )
            places = {place["place_id"]: place for place in place_rows}
            by_overture = coverage.place_index(places)
            canonical = coverage._canonical_ids(feeds)
            crawl_lock = crawl.reading(cache_dir)
            crawl_lock.__enter__()
            if coverage_manifest.get("crawl_digest") != crawl.states_digest(cache_dir):
                # Membership was measured against one crawl; tiers must be
                # measured against the same one, never a mix of two.
                raise ClassifyError(
                    "the crawl changed since the coverage stage read it; "
                    "re-run the coverage stage"
                )
            crawled = {}
            for feed_dir, state in crawl.crawled_feeds(cache_dir):
                state_id = state.get("feed_id")
                feed_id = canonical.get(state_id) if isinstance(state_id, str) else None
                if feed_id is not None:
                    crawled[feed_id] = (feed_dir, state)
            by_feed = collections.defaultdict(dict)
            for candidate in candidates:
                by_feed[candidate["feed_id"]][candidate["place_id"]] = candidate
            sources = {f["feed_id"]: f.get("coverage_source") for f in feeds}

            if crawled and lookup is None:
                from index_build import boundaries

                opened_lookup = boundaries.BoundaryLookup(
                    cache_dir, release=expanded_manifest.get("overture_release")
                )
                lookup = opened_lookup

            edges = []
            statuses = collections.Counter()
            routes_classified = 0
            edges_dropped = 0
            join_gaps = collections.Counter()
            for feed_id in sorted(by_feed):
                feed_candidates = by_feed[feed_id]
                if sources.get(feed_id) == "crawl" and feed_id in crawled:
                    feed_dir, state = crawled[feed_id]
                    classified, status, routes, dropped, gaps = _classify_feed(
                        feed_candidates,
                        feed_dir,
                        state,
                        lookup,
                        places,
                        by_overture,
                        route_min_stops,
                    )
                    routes_classified += routes
                    edges_dropped += dropped
                    for key, value in (gaps or {}).items():
                        join_gaps[key] += value
                else:
                    classified = [
                        _unknown_edge(c, "declared", route_min_stops)
                        for c in feed_candidates.values()
                    ]
                    status = "declared"
                statuses[status] += 1
                edges.extend(classified)
            edges.sort(key=lambda e: (e["place_id"], e["feed_id"], e["tier"]))

            by_tier = collections.Counter(e["tier"] for e in edges)
            manifest = {
                "source": "classify",
                "mode": coverage_manifest.get("mode"),
                "sources": coverage_manifest.get("sources"),
                "overture_release": coverage_manifest.get("overture_release"),
                # The exact generations classified from, so a later consumer
                # can refuse these edges once either input has moved on.
                "coverage_generation": coverage_manifest.get("generation"),
                "expanded_generation": expanded_manifest.get("generation"),
                "feeds": len(feeds),
                "feeds_by_status": dict(statuses),
                "routes_classified": routes_classified,
                "route_min_stops": route_min_stops,
                "review_cutoff": REVIEW_CUTOFF,
                "edges": len(edges),
                "edges_dropped_no_serving_route": edges_dropped,
                "join_gaps": dict(join_gaps),
                "edges_by_tier": dict(by_tier),
                "unknown_share": (by_tier["unknown"] / len(edges)) if edges else 0.0,
                "needs_review": sum(1 for e in edges if e["needs_review"]),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "classify",
                CLASSIFY_POINTER,
                {
                    FEEDS_ARTIFACT: store.jsonl_chunks(feeds),
                    EDGES_ARTIFACT: store.jsonl_chunks(edges),
                },
                manifest,
                held=directory,
            )
    finally:
        if crawl_lock is not None:
            crawl_lock.__exit__(None, None, None)
        if opened_lookup is not None:
            opened_lookup.close()
        directory.close()
