"""Stage 2b: gazetteer expansion from crawled stops.

The declared seed only knows places feeds *declare*; the crawl shows where
they actually stop. This stage reads each crawled feed's ``stops.txt``,
resolves the stop clusters through the boundary lookup, and adds every
QID-bearing admin unit the seed missed — running the same ancestor-and-metro
expansion the seed uses, so a crawl-discovered city arrives with its region,
country and any metro — US, Eurostat or FAO — that contains it, its boundary
licence-audited and simplified like every other place, and its names
enriched from Wikidata.
A named division that resolves to no QID is a place of its own, keyed by
its Overture id; a nameless one, or one whose signals conflict, is reported.

With no crawl artifacts the stage is a pass-through: the seed places republish
unchanged as ``places_expanded.jsonl``, so the declared path keeps running end
to end.
"""

import datetime
import functools

import shapely

from transitio_index import registry as _registry
from transitio_index import (
    boundaries,
    crawl,
    geometry,
    metros,
    overrides,
    overture,
    seed,
    store,
)
from transitio_index import names as names_stage
from transitio_index.progress import progress

EXPANDED_POINTER = "expanded.json"
PLACES_ARTIFACT = "places_expanded.jsonl"
REPORT_ARTIFACT = "expansion_report.jsonl"


def expected_registry_digest(gazetteer, path):
    """The digest the registry must have for the gazetteer's outputs to be
    consumed: the run's saved digest, or the expand stage's when it ran on
    that registry afterwards — the newest registry-writing stage — or None
    when no run manifest exists. A manifest that records no valid digest is
    refused; it never means no check."""
    return _registry_chain(gazetteer, path)[1]


def _registry_chain(gazetteer, path, *, replacing=False):
    """``(run_digest, head, run_generation)``: the digest the gazetteer run
    saved, which every expansion anchors on, the chain's newest digest, and
    the run's generation. An expansion built on another run — by registry
    or by generation — is refused to consumers; the expand stage itself
    (``replacing``) passes over it, since its own output replaces it."""
    run = store.run_manifest(gazetteer)
    if run is None:
        return None, None, None
    run_digest = expected = run.get("registry_digest")
    if not _digest(expected):
        raise _registry.RegistryError(
            f"{path}: the gazetteer run records no registry digest; "
            "rerun the gazetteer"
        )
    generation = run.get("generation")
    expanded = store.pointer_manifest(gazetteer, EXPANDED_POINTER)
    if expanded is not None and (
        expanded.get("registry_base") != expected
        or expanded.get("run_generation") != generation
    ):
        # Expanded places prove they were built on this run by its digest
        # and its generation; ones from before, or without the proof, are
        # rerun.
        if replacing:
            return run_digest, expected, generation
        raise _registry.RegistryError(
            f"{path}: the expanded places predate the gazetteer run; rerun expand"
        )
    if expanded is not None:
        expected = expanded.get("registry_digest")
        if not _digest(expected):
            raise _registry.RegistryError(
                f"{path}: the expanded places record no registry digest; rerun expand"
            )
    return run_digest, expected, generation


def _digest(value):
    return isinstance(value, str) and bool(store.DIGEST_PATTERN.match(value))


def _canonical_place_key(registry, qid, row):
    """The key a discovered division stands for inside the stage: the key
    the registry gives its QID, or — when the QID names no row — the one it
    gives the division's Overture or OSM id, else its own key. A first-seen
    QID-less division, keyed by an ``overture:`` concordance the registry does
    not yet carry, keeps that key and is minted downstream rather than aborting
    the run."""
    if registry is None:
        return qid
    if overture.QID_PATTERN.match(qid):
        key = metros._canonical_key(registry, qid)
        if key != qid:
            return key
    for namespace, value in (
        ("overture", row.get("overture_id")),
        ("osm_relation", row.get("osm_relation_id")),
    ):
        if value:
            try:
                return registry.key_for(f"{namespace}:{value}", internal=True)
            except _registry.RegistryError:
                continue
    return qid


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


def _attach_metros(places_by_id, codes, new_cities, wikidata, report, registry=None):
    """US metro membership for the discovered cities, like the metros stage,
    ``codes`` indexing the metros by statistical code; ``(added, touched)`` —
    the metros minted, and every metro a discovered CBSA pair names, a seeded
    one included."""
    us_cities = [
        qid
        for qid in new_cities
        if places_by_id[qid].get("country_code") == "US"
        and overture.QID_PATTERN.match(qid)
    ]
    added = []
    touched = []
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
            key = metros._canonical_key(registry, record["qid"])
            code = ("metropolitan statistical area", record["cbsa"])
            metro = places_by_id.get(key)
            if metro is not None and metro.get("kind") != "metro":
                raise overture.GazetteerError(
                    f"metro {record['qid']!r} is already seeded as the "
                    f"{metro['kind']} {metro.get('name')!r}"
                )
            # The row under the QID and the one indexed under the CBSA must be
            # one row: the code names one place.
            indexed = codes.get(code)
            if indexed is not None and metro is not None and indexed is not metro:
                raise overture.GazetteerError(
                    f"metros {key!r} and {indexed['place_id']!r} both carry the "
                    f"CBSA {record['cbsa']!r}"
                )
            if metro is None and indexed is not None:
                # A curated metro keyed by the code takes the discovered QID.
                metro = indexed
                metro.setdefault("discovered_qids", []).append(record["qid"])
            if metro is None:
                # Keyed by the lookup key, so a later record for the same
                # place finds it; the QID the source named stays a concordance.
                metro = metros._metro_place(record)
                metro["place_id"] = key
                if key != record["qid"]:
                    metro.setdefault("discovered_qids", []).append(record["qid"])
                places_by_id[key] = metro
                added.append(key)
            metros._take_msa(metro, record)
            codes[code] = metro
            if metro["place_id"] not in touched:
                touched.append(metro["place_id"])
            metros.join(metro, city)
    return added, touched


def dropped_qids(generation):
    """The QIDs of the discoveries the expansion report in ``generation`` — a
    resolved ``expanded.json`` — lists as conflicts: placed nowhere, so a stop
    inside one is a known miss for the stages measuring stops, not a sign of
    a stale expansion. An expansion published without a report dropped
    nothing."""
    if not generation.has(REPORT_ARTIFACT):
        return set()
    return {
        row["place_id"]
        for row in store.parse_jsonl(generation.read_bytes(REPORT_ARTIFACT))
        if row.get("kind") == "conflict" and row.get("place_id")
    }


def _shipped_footprint(place):
    """A place's shipped boundary as a geometry, or None when it ships none or
    the boundary does not parse."""
    if not place.get("geometry"):
        return None
    try:
        return shapely.from_wkb(bytes.fromhex(place["geometry"]))
    except (ValueError, shapely.errors.ShapelyError):
        return None


def _draw_member_geometry(places_by_id, keys):
    """Draw each metro in ``keys`` from its members' shipped polygons, like
    the geometry stage does for seeded metros — over the full membership, so
    a seeded metro's boundary grows with the discovered cities it gained; one
    whose members ship no polygon keeps the boundary it has. Without one a
    metro is dropped by the licence stage's rehoming."""
    for key in keys:
        metro = places_by_id[key]
        members = (places_by_id.get(m) for m in metro.get("member_ids") or [])
        geoms = [
            geom
            for geom in (_shipped_footprint(m) for m in members if m is not None)
            if geom is not None
        ]
        if not geoms:
            continue
        merged = geoms[0] if len(geoms) == 1 else shapely.unary_union(geoms)
        simplified = geometry._simplify(merged)
        if geometry._valid_polygon(simplified):
            metro["geometry"] = shapely.to_wkb(simplified).hex()
            metro["geometry_source"] = "member_union"


def _derived_inputs(cache_dir, recorded):
    """The Eurostat and FAO inputs, and the UCDB names, the metros stage
    published metros from: ``(eurostat, fao, names)``, loaded under the
    digests the run's ``derived_inputs`` recorded — so expansion derives
    metros from the run's snapshot, never from the module's current pins —
    each None (the names empty) where the run records none, that branch
    having published none. A recorded input the raw store no longer holds, or
    one no longer allowlisted, refuses the expansion: the seeded metros were
    derived from it, so only a gazetteer rerun keeps the output consistent."""
    from transitio_index import eurostat, fao, ucdb

    def load(branch, keys, read):
        pins = recorded.get(branch)
        if pins is None:
            return None
        if not all(k in geometry.DERIVED_SOURCE_ALLOWLIST for k in keys):
            raise overture.GazetteerError(
                f"the {branch} inputs the gazetteer run derived metros from "
                "are no longer allowlisted; rerun the gazetteer"
            )
        try:
            return read(pins)
        except metros.INPUT_UNAVAILABLE as error:
            raise overture.GazetteerError(
                f"the {branch} inputs the gazetteer run derived metros from "
                f"cannot be read ({error}); rerun the gazetteer"
            ) from None

    euro = load(
        "eurostat",
        metros.EUROSTAT_DERIVED,
        lambda pins: eurostat.load_inputs(cache_dir, expected=pins),
    )
    fao_inputs = load(
        "fao",
        metros.FAO_DERIVED,
        lambda pins: fao.load_inputs(cache_dir, expected=pins),
    )
    names = None
    if fao_inputs is not None:
        names = load(
            "ucdb",
            (ucdb.DERIVED,),
            lambda pins: ucdb.load_names(cache_dir, expected=pins),
        )
    return euro, fao_inputs, names[0] if names is not None else {}


class _SeededPlacement:
    """The seeded cities no metro covers, placed in their FAO regions from
    their Overture land areas as the metros stage placed them — once, and
    only when a region the seed left unpublished is minted, since the seed's
    own FAO metros already hold their cities."""

    def __init__(self, cache_dir, dataset, places_by_id, city_rows, regions, patches):
        discovered = {row["place_id"] for row in city_rows}
        self._rows = [
            row
            for key, row in places_by_id.items()
            if row.get("kind") == "city" and key not in discovered
        ]
        self._read = functools.partial(geometry.place_areas, cache_dir, dataset)
        self._regions = regions
        self._patches = patches
        self._grouped = None

    def cities(self, region_id):
        """The eligible seeded cities in ``region_id``."""
        if self._grouped is None:
            from transitio_index import fao

            wanted = {r["overture_id"] for r in self._rows if r.get("overture_id")}
            self._grouped, _, _, _, _ = fao.place_cities(
                self._rows,
                self._read(self._rows, wanted),
                self._regions,
                self._patches,
                [],
            )
        return self._grouped.get(region_id, [])


def _mint(places_by_id, codes, added, key, metro):
    """Add the metro row ``metro`` under ``key``, indexed by its code and
    listed in ``added``. A row already under the key — one seeded without
    the statistical code that would have indexed it — is never silently
    overwritten."""
    taken = places_by_id.get(key)
    if taken is not None:
        raise overture.GazetteerError(
            f"metro {key!r} is already seeded as the {taken.get('kind')} "
            f"{taken.get('name')!r} without its statistical code"
        )
    places_by_id[key] = metro
    codes[(metro["source_subtype"], metro["statistical_area_id"])] = metro
    added.append(key)
    return metro


def _attach_eurostat_metros(places_by_id, codes, city_rows, areas, euro):
    """Eurostat metro membership for the discovered cities, mirroring the
    metros stage over the inputs it read (``euro``, None for none): each
    city is assigned to its metropolitan region, whose metro is found among
    the published places by its code (``codes``) or minted, and joined.
    Returns ``(added, touched, assignments)`` — the metro keys minted, every
    metro a city joined, and the assignment rows the FAO branch reads."""
    from transitio_index import eurostat

    added = []
    touched = set()
    if euro is None:
        return added, touched, []
    composition, nuts_boundaries, _ = euro
    assignments = eurostat.assign(city_rows, areas, composition, nuts_boundaries)
    for row in assignments:
        # Every assigned city's metro publishes here, found or minted.
        row["published"] = row["status"] == "assigned"
        if not row["published"]:
            continue
        code = row["metro_code"]
        metro = codes.get(("metropolitan region", code))
        if metro is None:
            key = f"eurostat_metro:{code}"
            metro = _mint(
                places_by_id,
                codes,
                added,
                key,
                metros._eurostat_place(key, code, composition[code]),
            )
        touched.add(metro["place_id"])
        metros.join(metro, places_by_id[row["city_id"]])
    return added, touched, assignments


def _attach_fao_metros(
    cache_dir,
    dataset,
    places_by_id,
    codes,
    city_rows,
    areas,
    report,
    fao_inputs,
    names,
    assignments,
):
    """FAO city-region membership for the discovered cities no official
    metro covers, mirroring the metros stage over the inputs it read
    (``fao_inputs``, None for none, and the UCDB ``names``): each such city
    joins its region's metro, found by its code (``codes``) or minted — over
    every eligible city in the region, the seeded ones included, when the
    seed left it unpublished. A region already published is never
    deduplicated against itself; one the official metros cover now, a
    discovered city having joined them, is dropped as the metros stage would
    have found it. Returns ``(added, touched)``."""
    from transitio_index import fao

    added = []
    touched = set()
    if fao_inputs is None:
        return added, touched
    regions, patches, _ = fao_inputs
    grouped, _, _, _, _ = fao.place_cities(
        city_rows, areas, regions, patches, assignments
    )
    published = shapely.STRtree(
        metros.official_footprints(places_by_id, _shipped_footprint)
    )
    for (subtype, region_id), metro in list(codes.items()):
        if subtype != metros.FAO_SUBTYPE or region_id not in regions:
            continue
        footprint = metros._region_footprint(regions, patches, region_id)
        if metros._over_published_metro(footprint, published):
            metros._unjoin(metro, places_by_id)
            del places_by_id[metro["place_id"]]
            del codes[(subtype, region_id)]
            metros._report_fao(report, region_id, "duplicate of a published metro")
    seeded = _SeededPlacement(
        cache_dir, dataset, places_by_id, city_rows, regions, patches
    )
    for region_id in sorted(grouped):
        metro = codes.get((metros.FAO_SUBTYPE, region_id))
        members = grouped[region_id]
        if metro is None:
            footprint = metros._region_footprint(regions, patches, region_id)
            if metros._over_published_metro(footprint, published):
                continue
            # A region the seed left unpublished — for want of a majority
            # country, say — is minted over every eligible city in it, the
            # seeded ones included, so its partition, name and members do
            # not depend on which cities the crawl found first.
            members = sorted({*members, *seeded.cities(region_id)})
            country = metros._majority_country(members, places_by_id)
            if country is None:
                metros._report_fao(report, region_id, "no majority country")
                continue
            key = f"fao_city_region:{region_id}"
            name = metros._fao_name(
                names, region_id, members, places_by_id, _shipped_footprint
            )
            metro = _mint(
                places_by_id,
                codes,
                added,
                key,
                metros._fao_place(key, region_id, name, country),
            )
        touched.add(metro["place_id"])
        for city_id in sorted(members):
            metros.join(metro, places_by_id[city_id])
    return added, touched


def _discover(
    cache_dir,
    places_by_id,
    lookup,
    wikidata,
    area_dataset,
    report,
    registry,
    digest,
    release,
    derived,
    reopen=None,
):
    """Resolve crawled stops and fold the missing places in; returns counts.
    With ``registry``, every discovered place is identified through it;
    ``derived`` is the run's ``derived_inputs`` — the digests the metros stage
    derived metros from — and ``reopen`` reopens ``area_dataset`` when its S3
    scan stalls."""
    # Two passes, one feed's stops in memory at a time — never every crawled
    # feed's stops at once: first the lookup boxes, then, with the boxes
    # ensured, the per-point division resolution.
    crawled = crawl.crawled_feeds(cache_dir)
    stops_read = 0
    state_mismatches = 0
    boxes = []
    usable = []
    for feed_dir, state in progress(crawled, "expand scan"):
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
    for feed_dir, state in progress(usable, "expand resolve"):
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
    # A district or region whose QID a city-level division also carries is a
    # city that is its own district (Augsburg, Ulm, Wien): only the district
    # has an area, so the stop reached it, but the place is the city — the
    # kind the seed gives the same QID from its declared name.
    shared = {c["qid"] for c in candidates if c["kind"] == "region" and c["qid"]}
    cities = set()
    if shared:
        # A memo-only lookup answered the stops from its cache; the theme is
        # opened here for the one scan.
        dataset = lookup.division_dataset() or overture.overture_dataset(release)
        cities = seed.city_qids(dataset, shared)
    for record in candidates:
        if record["kind"] == "region" and record["qid"] in cities:
            record["kind"] = "city"
    skeleton = {}
    for record in candidates:
        if record["qid"] or overture.qidless_place(record):
            # A named division no QID names is a place of its own, keyed
            # by its Overture id.
            skeleton[record["overture_id"]] = record
        else:
            report.append(
                {
                    "kind": "division",
                    "overture_id": record["overture_id"],
                    "name": record.get("name"),
                    "reason": record.get("resolution_reason") or "no name",
                }
            )

    discovered = {}
    for record in skeleton.values():
        seed._add_place(discovered, skeleton, record)
    # A discovered place the registry already knows — by a merged alias of
    # its QID, or by its division when the QID is new — is the seeded row,
    # and two aliases of one place are one row; every discovered row is
    # still identified, enriching the survivor.
    canonical = {
        qid: _canonical_place_key(registry, qid, row) for qid, row in discovered.items()
    }
    conflicts = []
    for qid, place in discovered.items():
        existing = places_by_id.get(canonical[qid])
        if existing is None or existing.get("kind") == place.get("kind"):
            continue
        if existing.get("curated"):
            # A curator defined this place; a crawled row of another kind under
            # its key is a curation mistake, not messy source data — fail loudly.
            raise overture.GazetteerError(
                f"{qid!r} is both the seeded {existing['kind']} "
                f"{existing.get('name')!r} and the crawled {place['kind']} "
                f"{place.get('name')!r}"
            )
        # The same QID names Overture divisions of different kinds (a locality
        # and a region both tagged it): the seeded place stands and the crawled
        # one is reported, never an abort. A child that named the dropped place
        # as its parent still resolves to the survivor, which shares its key.
        report.append(
            {
                "kind": "conflict",
                "place_id": qid,
                "overture_id": place.get("overture_id"),
                "name": place.get("name"),
                "reason": (
                    f"crawled {place.get('kind')} conflicts with the "
                    f"{existing.get('kind')} {existing.get('name')!r} already "
                    f"keyed by {canonical[qid]}"
                ),
            }
        )
        conflicts.append(qid)
    for qid in conflicts:
        del discovered[qid]
        del canonical[qid]
    identified = 0
    if registry is not None:
        # Every resolved signal, a seeded place reached through a new
        # division included: the concordance enriches the row, or conflicts,
        # and read-only refuses. A discovery the registry refuses is dropped
        # here, before it can stand in for its aliases, read a boundary, or
        # seed a metro's membership — and reported, so the coverage stage
        # knows a stop landing on it is not proof of a stale expansion.
        identified = seed._identify_places(discovered, registry, digest, report=report)
        for qid in set(canonical) - set(discovered):
            del canonical[qid]
    new_ids = []
    taken = set()
    for qid in discovered:
        if canonical[qid] in places_by_id or canonical[qid] in taken:
            continue
        taken.add(canonical[qid])
        new_ids.append(qid)
    # Boundaries come from a complete, id-filtered area read, so a multi-part
    # place ships whole even when its stops touched only one component.
    areas = geometry.read_areas(
        area_dataset,
        {discovered[qid]["overture_id"] for qid in new_ids},
        simplify=geometry.SIMPLIFY_TOLERANCE_DEG,
        cache=(cache_dir, release),
        reopen=reopen,
        countries={discovered[qid].get("country_code") for qid in new_ids} - {None},
    )
    for qid in new_ids:
        place = discovered[qid]
        place.setdefault("aliases", [])
        place.setdefault("statistical_area_id", None)
        place.setdefault("metro_ids", [])
        _attach_boundary(place, areas.get(place["overture_id"]))
        places_by_id[qid] = place

    new_cities = [qid for qid in new_ids if places_by_id[qid].get("kind") == "city"]
    city_rows = [places_by_id[qid] for qid in new_cities]
    codes = metros._code_index(places_by_id.values())
    new_metros, metro_pairs = _attach_metros(
        places_by_id, codes, new_cities, wikidata, report, registry
    )
    euro = fao_inputs = None
    names = {}
    if city_rows:
        euro, fao_inputs, names = _derived_inputs(cache_dir, derived)
    eurostat_metros, eurostat_touched, assignments = _attach_eurostat_metros(
        places_by_id, codes, city_rows, areas, euro
    )
    # The official metros — US and Eurostat, minted or joined — are
    # partitioned before the FAO branch, as in the metros stage, so a FAO
    # region is deduplicated only against metros that publish and a city
    # whose official metro cannot is eligible for a city-region instead.
    official = set(new_metros) | set(metro_pairs) | eurostat_touched
    dropped = metros.partition(
        {key: places_by_id[key] for key in official}, places_by_id, codes, report
    )
    metros.reconcile(assignments, codes)
    fao_metros, fao_touched = _attach_fao_metros(
        cache_dir,
        area_dataset,
        places_by_id,
        codes,
        city_rows,
        areas,
        report,
        fao_inputs,
        names,
        assignments,
    )
    dropped |= metros.partition(
        {key: places_by_id[key] for key in fao_touched}, places_by_id, codes, report
    )
    minted = [key for key in eurostat_metros + fao_metros if key not in dropped]
    new_metros = [key for key in new_metros if key not in dropped] + minted
    metro_pairs = [key for key in metro_pairs if key not in dropped]
    # Every metro this expansion minted or added a member to is redrawn from
    # its full membership, so one without a boundary is not dropped by the
    # licence stage's rehoming.
    touched = (official | fao_touched) - dropped
    for key in touched:
        metro = places_by_id[key]
        metro["member_ids"].sort()
        for member in metro["member_ids"]:
            places_by_id[member]["metro_ids"].sort()
    _draw_member_geometry(places_by_id, touched)
    if registry is not None:
        # A seeded metro a discovered CBSA pair names is enriched too; the
        # Eurostat and FAO metros minted here are identified by their code.
        identified += metros._identify_metros(
            {qid: places_by_id[qid] for qid in metro_pairs + minted},
            registry,
            derived,
        )
        # A seeded row a discovery joined through its division publishes
        # the QID the registry now keys it by, and is enriched under it.
        joined = []
        for qid, key in canonical.items():
            row = places_by_id.get(key)
            if key != qid and row is not None and row.get("tp_id"):
                row["wikidata_id"] = registry.canonical_qid(row["tp_id"])
                joined.append(key)

    # Enrichment covers the minted metros too, so every new place carries the
    # same multilingual labels the names stage gives seeded ones — by the
    # QID each row carries after identification: a discovered alias takes
    # its survivor's labels.
    enrich = {
        key: places_by_id[key].get("wikidata_id") or key
        for key in new_ids + new_metros + (joined if registry is not None else [])
    }
    qids = sorted({q for q in enrich.values() if overture.QID_PATTERN.match(q)})
    labels = wikidata.labels_and_aliases(qids) if qids else {}
    for key, qid in enrich.items():
        entry = labels.get(qid)
        if entry is not None:
            names_stage._merge(places_by_id[key], entry)

    return {
        "feeds_scanned": len(crawled),
        "stops_read": stops_read,
        "state_mismatches": state_mismatches,
        "divisions_hit": len(divisions),
        "places_added": len(new_ids) + len(new_metros),
        "metros_added": len(new_metros),
        "identified": identified,
    }


def _expanded(
    cache_dir,
    places_by_id,
    report,
    counts,
    lookup,
    wikidata,
    area_dataset,
    release,
    registry,
    digest,
    derived,
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
            reopen = None
            if area_dataset is None:
                area_dataset = geometry.division_area_dataset(release)
                # A fresh connection when the S3 area scan stalls.
                reopen = functools.partial(geometry.division_area_dataset, release)
            if lookup is None:
                opened_lookup = boundaries.BoundaryLookup(
                    cache_dir,
                    release=release,
                    area_dataset=area_dataset,
                    division_dataset=overture.overture_dataset(release),
                    reopen_area=reopen,
                    reopen_division=functools.partial(
                        overture.overture_dataset, release
                    ),
                )
                lookup = opened_lookup
            counts = _discover(
                cache_dir,
                places_by_id,
                lookup,
                wikidata,
                area_dataset,
                report,
                registry,
                digest,
                release,
                derived,
                reopen,
            )
    finally:
        if opened_lookup is not None:
            opened_lookup.close()
    return crawl.states_digest(cache_dir), counts, mode


def expand(
    cache_dir,
    *,
    lookup=None,
    wikidata=None,
    area_dataset=None,
    overrides_dir=None,
    registry=None,
):
    """Publish ``places_expanded``: the seed plus crawl-discovered places.

    Reads the enriched seed places, discovers what the crawled stops reach
    that the seed missed, and republishes the union, carrying the Overture
    release forward. With ``registry`` — a session over the registry the
    gazetteer ran with — every discovered place is identified through it,
    the registry is saved before the generation is published, and the
    manifest records the digests loaded and saved. Returns the generation
    manifest.
    """
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, names_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "names.json", "places_seed.jsonl"
            )
            release = names_manifest.get("overture_release")
            # The digests the metros stage derived metros from, by branch;
            # a names generation recording none derives none here either.
            derived = names_manifest.get("derived_inputs") or {}
            recorded = names_manifest.get("simplify_tolerance_deg")
            if recorded != geometry.SIMPLIFY_TOLERANCE_DEG:
                # The seeded boundaries were simplified at another tolerance,
                # or at one nobody recorded: discovered places must not ship
                # beside them at this one.
                raise overture.GazetteerError(
                    f"the seed geometry was simplified at {recorded}°, not "
                    f"{geometry.SIMPLIFY_TOLERANCE_DEG}°; re-run the geometry "
                    "stage before expanding"
                )
            overrides.expect_digest(
                names_manifest.get("places_overrides_sha256"),
                overrides.places_digest(overrides_dir),
                "places.yaml",
                "gazetteer",
            )
            digest = names_manifest.get("places_overrides_sha256")
            anchor = run_generation = None
            if registry is not None:
                # Ids are minted only on a committed gazetteer run, against
                # the registry it left or the one a previous expand saved on
                # it: any other file is a change to rerun from — a crash
                # between the save below and the publish included, which the
                # gazetteer rerun re-anchors on. The manifest anchors on the
                # run's digest and generation, so the chain stays
                # run -> expand across reruns.
                anchor, expected, run_generation = _registry_chain(
                    cache_dir / "gazetteer", registry.path, replacing=True
                )
                if anchor is None:
                    raise overture.GazetteerError(
                        "no gazetteer run to expand; rerun the gazetteer"
                    )
                if expected != registry.base:
                    raise overture.GazetteerError(
                        "the registry changed since the gazetteer ran; "
                        "rerun the gazetteer"
                    )
            places_by_id = {place["place_id"]: place for place in places}
            if registry is not None:
                # Discovery joins by QID; the rows are keyed by their ids
                # again before they are published.
                places_by_id = seed.rekey_by_qid(places_by_id)
            report = []
            counts = {
                "feeds_scanned": 0,
                "stops_read": 0,
                "divisions_hit": 0,
                "places_added": 0,
                "metros_added": 0,
                "identified": 0,
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
                    registry,
                    digest,
                    derived,
                )
            # Saved before the generation is visible, as the gazetteer run
            # does: a crash between the two leaves rows a rerun reproduces.
            registry_digest = None
            if registry is not None:
                places_by_id = seed.rekey_by_own_id(places_by_id, registry)
                registry_digest = registry.save()
            manifest = {
                "source": "expand",
                "registry_base": anchor,
                "registry_digest": registry_digest,
                "run_generation": run_generation,
                "minted": registry.minted if registry is not None else 0,
                "enriched": registry.enriched if registry is not None else 0,
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
                "simplify_tolerance_deg": geometry.SIMPLIFY_TOLERANCE_DEG,
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
