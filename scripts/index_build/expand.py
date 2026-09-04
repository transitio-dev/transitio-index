"""Stage 2b: gazetteer expansion from crawled stops.

The declared seed only knows places feeds *declare*; the crawl shows where
they actually stop. This stage reads each crawled feed's ``stops.txt``,
resolves the stop clusters through the boundary lookup, and adds every
QID-bearing admin unit the seed missed — running the same ancestor-and-metro
expansion the seed uses, so a crawl-discovered city arrives with its region,
country and any US metro that contains it, its boundary licence-audited and
simplified like every other place, and its names enriched from Wikidata.
Divisions that resolve to no QID are reported, never minted.

With no crawl artifacts the stage is a pass-through: the seed places republish
unchanged as ``places_expanded.jsonl``, so the declared path keeps running end
to end.
"""

import datetime

import shapely

from index_build import (
    boundaries,
    crawl,
    geometry,
    metros,
    overrides,
    overture,
    seed,
    store,
)
from index_build import names as names_stage

EXPANDED_POINTER = "expanded.json"
PLACES_ARTIFACT = "places_expanded.jsonl"
REPORT_ARTIFACT = "expansion_report.jsonl"


def _stop_points(feed_dir, state):
    """``(points, dropped)`` for one crawled feed's stops, or None.

    Read through the digest-verified member; a data failure — a csv field
    over the parser limit, an undecodable byte, memory — answers None: one
    feed's corrupt member must never abort the run. A programming defect is
    not caught, so it cannot masquerade as bad feed data.
    """
    try:
        with crawl.verified_member(feed_dir, state, "stops.txt") as opened:
            if opened is None:
                return None
            return crawl.stop_coordinates(opened)
    except crawl.MEMBER_ERRORS:
        return None


def _attach_boundary(place, rows):
    """The division's licence-audited, simplified boundary onto the place.

    ``rows`` are the division's COMPLETE land areas from an id-filtered
    :func:`geometry.read_areas` read — never the lookup's box-clipped subset,
    which would ship a multi-part city truncated to wherever its stops were.
    The contract mirrors the geometry stage's exactly: every area's every
    source allowlisted and every polygon valid, else the place ships without
    geometry rather than with unaudited or partial geometry.
    """
    place.setdefault("geometry", None)
    place.setdefault("geometry_source", None)
    if not rows:
        return
    if not all(geometry._is_shippable(row["sources"]) for row in rows):
        return
    geoms = [row["geom"] for row in rows]
    if not all(geometry._valid_polygon(geom) for geom in geoms):
        return
    merged = geoms[0] if len(geoms) == 1 else shapely.unary_union(geoms)
    simplified = geometry._simplify(merged)
    if not geometry._valid_polygon(simplified):
        return
    place["geometry"] = shapely.to_wkb(simplified).hex()
    place["geometry_source"] = "overture"


def _attach_metros(places_by_id, new_cities, wikidata, report):
    """US metro membership for the discovered cities, like the metros stage."""
    us_cities = [
        qid for qid in new_cities if places_by_id[qid].get("country_code") == "US"
    ]
    added = []
    membership = wikidata.statistical_metros(us_cities) if us_cities else {}
    for city_qid, found in membership.items():
        city = places_by_id.get(city_qid)
        if city is None:
            continue
        for record in found:
            if not record["cbsa"]:
                report.append(
                    {
                        "kind": "metro",
                        "city_id": city_qid,
                        "metro_id": record["qid"],
                        "reason": "US MSA without a CBSA code",
                    }
                )
                continue
            metro = places_by_id.get(record["qid"])
            if metro is None:
                metro = metros._metro_place(record)
                places_by_id[metro["place_id"]] = metro
                added.append(metro["place_id"])
            elif metro.get("kind") != "metro":
                raise overture.GazetteerError(
                    f"metro {record['qid']!r} is already seeded as the "
                    f"{metro['kind']} {metro.get('name')!r}"
                )
            if metro.get("members_curated"):
                # A curator's member list is authoritative: never added to.
                continue
            metro.setdefault("member_ids", [])
            if city_qid not in metro["member_ids"]:
                metro["member_ids"].append(city_qid)
                metro["member_ids"].sort()
            city.setdefault("metro_ids", [])
            if metro["place_id"] not in city["metro_ids"]:
                city["metro_ids"].append(metro["place_id"])
    return added


def _discover(cache_dir, places_by_id, lookup, wikidata, area_dataset, report):
    """Resolve crawled stops and fold the missing places in; returns counts."""
    # Two passes, one feed's stops in memory at a time — never every crawled
    # feed's stops at once: first the lookup boxes, then, with the boxes
    # ensured, the per-point division resolution.
    crawled = crawl.crawled_feeds(cache_dir)
    stops_read = 0
    state_mismatches = 0
    boxes = []
    usable = []
    for feed_dir, state in crawled:
        read = _stop_points(feed_dir, state)
        if read is None:
            state_mismatches += 1
            continue
        points, _ = read
        stops_read += len(points)
        boxes.extend(crawl.cluster_boxes(points))
        usable.append((feed_dir, state))
    lookup.ensure(boxes)

    divisions = {}
    for feed_dir, state in usable:
        read = _stop_points(feed_dir, state)
        if read is None:
            # Changed between the passes: no longer trustworthy evidence.
            state_mismatches += 1
            continue
        points, _ = read
        for x, y in points:
            for record in lookup.divisions_at(x, y):
                divisions[record["division_id"]] = record

    candidates = [dict(record) for record in divisions.values() if record.get("kind")]
    seed._resolve_candidates(candidates, wikidata)
    skeleton = {}
    for record in candidates:
        if record["qid"]:
            skeleton[record["overture_id"]] = record
        else:
            report.append(
                {
                    "kind": "division",
                    "overture_id": record["overture_id"],
                    "name": record.get("name"),
                    "reason": record.get("resolution_method") or "no QID",
                }
            )

    discovered = {}
    for record in skeleton.values():
        seed._add_place(discovered, skeleton, record)
    for qid, place in discovered.items():
        existing = places_by_id.get(qid)
        if existing is not None and existing.get("kind") != place.get("kind"):
            raise overture.GazetteerError(
                f"{qid!r} is both the seeded {existing['kind']} "
                f"{existing.get('name')!r} and the crawled {place['kind']} "
                f"{place.get('name')!r}"
            )
    new_ids = [qid for qid in discovered if qid not in places_by_id]
    # Boundaries come from a complete, id-filtered area read, so a multi-part
    # place ships whole even when its stops touched only one component.
    areas = geometry.read_areas(
        area_dataset, {discovered[qid]["overture_id"] for qid in new_ids}
    )
    for qid in new_ids:
        place = discovered[qid]
        place.setdefault("aliases", [])
        place.setdefault("statistical_area_id", None)
        _attach_boundary(place, areas.get(place["overture_id"]))
        places_by_id[qid] = place

    new_cities = [qid for qid in new_ids if places_by_id[qid].get("kind") == "city"]
    new_metros = _attach_metros(places_by_id, new_cities, wikidata, report)

    # Enrichment covers the minted metros too, so every new place carries the
    # same multilingual labels the names stage gives seeded ones.
    enrich = new_ids + new_metros
    labels = wikidata.labels_and_aliases(enrich) if enrich else {}
    for qid in enrich:
        entry = labels.get(qid)
        if entry is not None:
            names_stage._merge(places_by_id[qid], entry)

    return {
        "feeds_scanned": len(crawled),
        "stops_read": stops_read,
        "state_mismatches": state_mismatches,
        "divisions_hit": len(divisions),
        "places_added": len(new_ids) + len(new_metros),
        "metros_added": len(new_metros),
    }


def _expanded(
    cache_dir, places_by_id, report, counts, lookup, wikidata, area_dataset, release
):
    """Discovery under the crawl lock; ``(crawl_digest, counts, mode)``.

    The digest is taken while the lock is still held, so it describes
    exactly the states discovery read.
    """
    mode = "declared"
    opened_lookup = None
    try:
        if crawl.crawled_feeds(cache_dir):
            mode = "expanded"
            if wikidata is None:
                wikidata = overture.WikidataClient()
            if area_dataset is None:
                area_dataset = geometry.division_area_dataset(release)
            if lookup is None:
                opened_lookup = boundaries.BoundaryLookup(
                    cache_dir,
                    release=release,
                    area_dataset=area_dataset,
                    division_dataset=overture.overture_dataset(release),
                )
                lookup = opened_lookup
            counts = _discover(
                cache_dir, places_by_id, lookup, wikidata, area_dataset, report
            )
    finally:
        if opened_lookup is not None:
            opened_lookup.close()
    return crawl.states_digest(cache_dir), counts, mode


def expand(
    cache_dir, *, lookup=None, wikidata=None, area_dataset=None, overrides_dir=None
):
    """Publish ``places_expanded``: the seed plus crawl-discovered places.

    Reads the enriched seed places, discovers what the crawled stops reach
    that the seed missed, and republishes the union, carrying the Overture
    release forward. Returns the generation manifest.
    """
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, names_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "names.json", "places_seed.jsonl"
            )
            release = names_manifest.get("overture_release")
            overrides.expect_digest(
                names_manifest.get("places_overrides_sha256"),
                overrides.places_digest(overrides_dir),
                "places.yaml",
                "gazetteer",
            )
            places_by_id = {place["place_id"]: place for place in places}
            report = []
            counts = {
                "feeds_scanned": 0,
                "stops_read": 0,
                "divisions_hit": 0,
                "places_added": 0,
                "metros_added": 0,
            }
            with crawl.reading(cache_dir):
                crawl_digest, counts, mode = _expanded(
                    cache_dir,
                    places_by_id,
                    report,
                    counts,
                    lookup,
                    wikidata,
                    area_dataset,
                    release,
                )
            manifest = {
                "source": "expand",
                "sources": names_manifest.get("sources"),
                "seed_generation": names_manifest.get("seed_generation"),
                "names_generation": names_manifest.get("generation"),
                "geometry_generation": names_manifest.get("geometry_generation"),
                "places_overrides_sha256": names_manifest.get(
                    "places_overrides_sha256"
                ),
                "stale_place_overrides": names_manifest.get("stale_place_overrides"),
                "crawl_digest": crawl_digest,
                "mode": mode,
                "places": len(places_by_id),
                "overture_release": release,
                "reported": len(report),
                **counts,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "gazetteer",
                EXPANDED_POINTER,
                {
                    PLACES_ARTIFACT: store.jsonl_chunks(list(places_by_id.values())),
                    REPORT_ARTIFACT: store.jsonl_chunks(report),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
