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
import hashlib
import json
import os
import re

import shapely

from index_build import boundaries, crawl, geometry, metros, overture, seed, store
from index_build import names as names_stage

EXPANDED_POINTER = "expanded.json"
PLACES_ARTIFACT = "places_expanded.jsonl"
REPORT_ARTIFACT = "expansion_report.jsonl"

# The only directory shape the crawl stage produces; anything else in the log
# is refused rather than joined into a path.
_CRAWL_DIR = re.compile(r"\Aid-[0-9a-f]{64}\Z")


def _stop_points(feed_dir, state):
    """``(points, dropped)`` for one crawled feed's stops, or None.

    The bytes are verified against the digest ``state.json`` — the crawl's
    per-feed commit point — recorded for them: a crash mid-crawl can leave a
    member newer or older than the state, and a mismatched file must be
    skipped, never resolved as evidence. The file is streamed twice (digest,
    then parse) so no whole member is buffered, and ANY failure — a symlink,
    a csv field over the parser limit, memory — answers None: one feed's
    corrupt member must never abort the run.
    """
    path = feed_dir / "stops.txt"
    digests = state.get("member_sha256")
    expected = digests.get("stops.txt") if isinstance(digests, dict) else None
    if not expected:
        return None
    try:
        handle = store.open_nofollow(path)
    except OSError:
        return None
    try:
        with os.fdopen(handle, "rb") as opened:
            digest = hashlib.sha256()
            for chunk in iter(lambda: opened.read(1024 * 1024), b""):
                digest.update(chunk)
            if digest.hexdigest() != expected:
                return None
            opened.seek(0)
            return crawl.stop_coordinates(opened)
    except Exception:
        return None


def _crawled_feeds(cache_dir):
    """``(feed_dir, state)`` for crawled feeds whose state records stops.

    The per-feed ``state.json`` is the crawl's commit point, so it — not the
    run-level log alone — decides what may be read as evidence.
    """
    crawl_root = cache_dir / "crawl"
    if crawl_root.is_symlink() or not crawl_root.is_dir():
        return []
    log_path = crawl_root / "crawl_log.jsonl"
    try:
        handle = store.open_nofollow(log_path)
    except OSError:
        return []
    with os.fdopen(handle, "rb") as opened:
        raw = opened.read()
    found = []
    for record in store.parse_jsonl(raw):
        if not isinstance(record, dict):
            continue
        name = record.get("directory")
        if not isinstance(name, str) or not _CRAWL_DIR.fullmatch(name):
            # A corrupted or foreign log line must not traverse the cache.
            continue
        feed_dir = crawl_root / name
        # The whole chain must be real: a symlinked feed directory would let
        # the final-component checks below inspect files outside the cache.
        if feed_dir.is_symlink() or not feed_dir.is_dir():
            continue
        state_path = feed_dir / "state.json"
        try:
            handle = store.open_nofollow(state_path)
            with os.fdopen(handle, "rb") as opened:
                state = json.loads(opened.read())
        except (OSError, ValueError):
            continue
        # One corrupt state skips one feed; it must never abort expansion.
        if not isinstance(state, dict) or not isinstance(state.get("members"), list):
            continue
        if "stops.txt" in state["members"]:
            found.append((feed_dir, state))
    return found


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
    crawled = _crawled_feeds(cache_dir)
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
        if record["qid"] not in places_by_id:
            seed._add_place(discovered, skeleton, record)
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


def expand(cache_dir, *, lookup=None, wikidata=None, area_dataset=None):
    """Publish ``places_expanded``: the seed plus crawl-discovered places.

    Reads the enriched seed places, discovers what the crawled stops reach
    that the seed missed, and republishes the union, carrying the Overture
    release forward. Returns the generation manifest.
    """
    directory = store.open_subdir(cache_dir, "gazetteer")
    opened_lookup = None
    try:
        with store.exclusive_writer(directory):
            places, names_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "names.json", "places_seed.jsonl"
            )
            release = names_manifest.get("overture_release")
            places_by_id = {place["place_id"]: place for place in places}
            report = []
            counts = {
                "feeds_scanned": 0,
                "stops_read": 0,
                "divisions_hit": 0,
                "places_added": 0,
                "metros_added": 0,
            }
            mode = "declared"
            if _crawled_feeds(cache_dir):
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

            manifest = {
                "source": "expand",
                "sources": names_manifest.get("sources"),
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
        if opened_lookup is not None:
            opened_lookup.close()
        directory.close()
