"""Attach metropolitan-area membership to the seeded places.

Two branches, each gated on a city's country. US cities: Wikidata links a
city to the metropolitan statistical area it belongs to (P8138, class
``US_MSA_CLASS``), keyed by the metro's CBSA code (P882), and each such metro
is emitted as a ``metro`` place. Cities of the countries Eurostat's pinned
composition covers: the NUTS-3 region containing the city's Overture land
areas gives its metropolitan region; that membership is derived offline and
recorded for every such city in ``metro_assignments.jsonl``, but a Eurostat
metro is published only through a curated ``set_statistical_area`` crosswalk
naming its QID, and only while every derived input is allowlisted — otherwise
it is reported with the Wikidata candidates a curator can pick from. Every
member city carries its metros in ``metro_ids`` — a city can belong to more
than one. Metro geometry is later work; this stage adds membership only.
"""

import collections
import datetime
import functools
import json
import hashlib

from index_build import eurostat, geometry, overrides, overture, seed, store

# The derived inputs the Eurostat branch needs approved, as allowlist keys;
# any one missing runs the branch report-only.
EUROSTAT_DERIVED = (
    ("Overture Maps divisions", "CDLA-Permissive-2.0"),
    ("Eurostat metropolitan regions", "Eurostat-2011/833/EU"),
    ("GISCO NUTS 2021", "EuroGeographics-NC"),
)


def _metro_place(metro):
    """A ``metro`` place row from a resolved MSA (geometry added later)."""
    name = metro.get("name")
    return {
        "place_id": metro["qid"],
        "kind": "metro",
        "source_subtype": "metropolitan statistical area",
        "name": name,
        "names": {"en": name} if name else {},
        "resolution_method": "statistical_code",
        "parent_id": None,
        "country_code": "US",
        "overture_id": None,
        "osm_relation_id": None,
        "statistical_area_id": metro.get("cbsa"),
        "metro_ids": [],
        "member_ids": [],
    }


def _eurostat_place(qid, code, metro):
    """A ``metro`` place row for a crosswalked Eurostat metropolitan region."""
    return {
        "place_id": qid,
        "kind": "metro",
        "source_subtype": "metropolitan region",
        "name": metro["name"],
        "names": {},
        "resolution_method": "statistical_code",
        "parent_id": None,
        "country_code": metro["country"],
        "overture_id": None,
        "osm_relation_id": None,
        "statistical_area_id": code,
        "metro_ids": [],
        "member_ids": [],
    }


def _join_member(by_id, metros, qid, make_row, city):
    """The metro row for ``qid`` with ``city`` joined as a member, reciprocally.

    A curated metro of the same QID (``add_place``) is the row and the
    statistical membership joins it, never a second row; a QID seeded as
    anything but a metro fails the build; a curator's member list wins.
    """
    existing = by_id.get(qid)
    if existing is not None and existing.get("kind") != "metro":
        raise overture.GazetteerError(
            f"metro {qid!r} is already seeded as the "
            f"{existing['kind']} {existing.get('name')!r}"
        )
    metro = metros.get(qid) or existing or make_row()
    metros[qid] = metro
    if metro.get("members_curated"):
        return metro
    if city["place_id"] not in metro["member_ids"]:
        metro["member_ids"].append(city["place_id"])
    if qid not in city["metro_ids"]:
        city["metro_ids"].append(qid)
    return metro


def _set_members(by_id, entries, report):
    """A curator's member list for a metro, reciprocal on the cities: the
    old members lose the metro, the new ones gain it. Judged against the
    metro's current member list."""
    applied = 0
    for entry in entries:
        metro = by_id.get(entry["place"])
        if metro is None or metro.get("kind") != "metro":
            raise overrides.OverrideError(
                f"place {entry['place']!r}: set_place_members needs a seeded metro"
            )
        for member in entry["set_place_members"]:
            city = by_id.get(member)
            if city is None or city.get("kind") != "city":
                raise overrides.OverrideError(
                    f"place {entry['place']!r}: member {member!r} is not a seeded city"
                )
        overrides.judge(entry, sorted(metro.get("member_ids") or []), report, "metros")
        for old in metro.get("member_ids") or []:
            city = by_id.get(old)
            if city is not None and metro["place_id"] in (city.get("metro_ids") or []):
                city["metro_ids"].remove(metro["place_id"])
        metro["member_ids"] = sorted(set(entry["set_place_members"]))
        metro["members_curated"] = True
        for member in metro["member_ids"]:
            by_id[member].setdefault("metro_ids", [])
            if metro["place_id"] not in by_id[member]["metro_ids"]:
                by_id[member]["metro_ids"].append(metro["place_id"])
        applied += 1
    return applied


def _attach_us(places, by_id, metros, report, wikidata):
    """The US branch: each US city's MSA from Wikidata; an MSA without a CBSA
    code is reported for a later pass rather than published. Returns the
    branch summary."""
    us_cities = [
        p["place_id"]
        for p in places
        if p["kind"] == "city" and p["country_code"] == "US"
    ]
    membership = wikidata.statistical_metros(us_cities) if us_cities else {}
    published = set()
    reported_metros = set()
    for city_qid, found in membership.items():
        city = by_id.get(city_qid)
        if city is None:
            continue
        for record in found:
            if not record["cbsa"]:
                report.append(
                    {
                        "branch": "us",
                        "city_id": city_qid,
                        "metro_id": record["qid"],
                        "name": record["name"],
                        "reason": "US MSA without a CBSA code",
                    }
                )
                reported_metros.add(record["qid"])
                continue
            metro = _join_member(
                by_id,
                metros,
                record["qid"],
                functools.partial(_metro_place, record),
                city,
            )
            # A curated metro of this QID takes the discovered identity it
            # lacks; a different code already on it is a conflict.
            code_now = metro.get("statistical_area_id")
            if code_now not in (None, record["cbsa"]):
                raise overture.GazetteerError(
                    f"metro {record['qid']!r} carries statistical code "
                    f"{code_now!r}, not CBSA {record['cbsa']!r}"
                )
            metro["statistical_area_id"] = record["cbsa"]
            metro["country_code"] = metro.get("country_code") or "US"
            metro["source_subtype"] = (
                metro.get("source_subtype") or "metropolitan statistical area"
            )
            published.add(metro["place_id"])
    return {"metros_published": len(published), "metros_reported": len(reported_metros)}


def _crosswalk(place_overrides, composition, by_id, metros):
    """``{metro_code: entry}`` from the ``eurostat_metro`` crosswalk entries,
    every target checked up front, whether or not its metro publishes this
    build. Codes and QIDs pair one to one: a code the pinned composition
    lacks, two entries naming one code, one QID naming two codes, a QID
    seeded as anything but a metro, or a metro already carrying another
    statistical code or country is refused — the crosswalk names official
    regions, not free text."""
    crosswalk = {}
    qids = set()
    for entry in overrides.by_operation(place_overrides, "set_statistical_area"):
        spec = entry["set_statistical_area"]
        if spec["scheme"] != "eurostat_metro":
            continue
        code, qid = spec["code"], entry["place"]
        if code not in composition:
            raise overrides.OverrideError(
                f"place {qid!r}: set_statistical_area code {code!r} is not in the "
                "pinned composition"
            )
        if code in crosswalk or qid in qids:
            raise overrides.OverrideError(
                f"place {qid!r}: set_statistical_area {code} is crosswalked twice"
            )
        # A metro the US branch created this run is in ``metros``, not ``by_id``.
        existing = metros.get(qid) or by_id.get(qid)
        if existing is not None:
            if existing.get("kind") != "metro":
                raise overture.GazetteerError(
                    f"metro {qid!r} is already seeded as the "
                    f"{existing['kind']} {existing.get('name')!r}"
                )
            code_now = existing.get("statistical_area_id")
            country_now = existing.get("country_code")
            country = composition[code]["country"]
            if code_now not in (None, code) or country_now not in (None, country):
                raise overrides.OverrideError(
                    f"place {qid!r}: set_statistical_area {code} conflicts with "
                    f"the metro's identity ({code_now!r}, {country_now!r})"
                )
        crosswalk[code] = entry
        qids.add(qid)
    return crosswalk


def _publish_eurostat(by_id, metros, qid, code, metro, city_ids):
    """Publish a crosswalked Eurostat metro under ``qid``: a new row, or the
    curated metro of that QID (checked compatible by :func:`_crosswalk`)
    joined by the derived members (possibly none) and given the Eurostat
    identity fields, the curator keeping name and members."""
    make_row = functools.partial(_eurostat_place, qid, code, metro)
    row = metros.get(qid) or by_id.get(qid) or make_row()
    metros[qid] = row
    for city_id in city_ids:
        _join_member(by_id, metros, qid, make_row, by_id[city_id])
    row.update(
        source_subtype="metropolitan region",
        resolution_method="statistical_code",
        country_code=metro["country"],
        statistical_area_id=code,
    )
    return row


def _candidates(wikidata, members):
    """Per unpublished metro code, the Wikidata metro-like entities its member
    cities link to and how many members link to each — the curator's
    shortlist, never an identity."""
    cities = sorted({city for ids in members.values() for city in ids})
    linked = wikidata.metro_candidates(cities) if cities else {}
    result = {}
    for code, ids in members.items():
        counts = collections.Counter()
        names = {}
        for city in ids:
            for candidate in linked.get(city, []):
                counts[candidate["qid"]] += 1
                names[candidate["qid"]] = candidate["name"]
        result[code] = [
            {"qid": qid, "name": names[qid], "cities": count}
            for qid, count in sorted(
                counts.items(), key=lambda item: (-item[1], item[0])
            )
        ]
    return result


def _derived_inventory(inputs_manifest, memberships):
    """One licence-inventory row per derived input, at its pinned version and
    with the credit and terms its source requires, allowed or not."""
    versions = {
        "Overture Maps divisions": overture.OVERTURE_RELEASE,
        "Eurostat metropolitan regions": inputs_manifest["digests"][
            eurostat.COMPOSITION_FILE
        ],
        "GISCO NUTS 2021": inputs_manifest["digests"][eurostat.BOUNDARIES_FILE],
    }
    rows = []
    for dataset, licence in EUROSTAT_DERIVED:
        meta = geometry.DERIVED_SOURCES[(dataset, licence)]
        rows.append(
            {
                "role": "derived_input",
                "use": "derived",
                "dataset": dataset,
                "license": licence,
                "credit": meta["credit"],
                "terms": meta["licence"],
                "url": meta["url"],
                "version": versions[dataset],
                "allowed": (dataset, licence) in geometry.DERIVED_SOURCE_ALLOWLIST,
                "memberships": memberships,
            }
        )
    return rows


def _attach_eurostat(
    places,
    by_id,
    metros,
    report,
    override_report,
    *,
    cache_dir,
    dataset,
    pins,
    wikidata,
    place_overrides,
):
    """The Eurostat branch; returns ``(assignments, derived_inventory, summary,
    applied)``, the last being the crosswalk entries that published.

    Every city of a covered country gets an assignment row. Every crosswalk
    entry is judged against the sorted derived member list it named, whether
    or not its metro publishes; a metro publishes only with a crosswalk entry
    and every derived input allowlisted (its derived members may be none), and
    every other
    composition metro is reported with the reason and its candidates.
    """
    composition, boundaries, inputs_manifest = eurostat.load_inputs(
        cache_dir, expected=pins
    )
    covered = eurostat.countries(composition)
    wanted = {
        p["overture_id"]
        for p in places
        if p["kind"] == "city" and p["country_code"] in covered and p.get("overture_id")
    }
    if wanted and dataset is None:
        dataset = geometry.division_area_dataset()
    areas = geometry.read_areas(dataset, wanted) if wanted else {}
    assignments = eurostat.assign(places, areas, composition, boundaries)
    members = {}
    for row in assignments:
        if row["status"] == "assigned":
            members.setdefault(row["metro_code"], set()).add(row["city_id"])
    # Canonical member lists: what the evidence hash is taken over.
    members = {code: sorted(ids) for code, ids in members.items()}

    crosswalk = _crosswalk(place_overrides, composition, by_id, metros)
    allowed = all(key in geometry.DERIVED_SOURCE_ALLOWLIST for key in EUROSTAT_DERIVED)
    published = set()
    unpublished = {}
    # Report-first: every composition metro is reported unless it publishes.
    for code in sorted(composition):
        city_ids = members.get(code, [])
        entry = crosswalk.get(code)
        if entry is not None:
            overrides.judge(entry, city_ids, override_report, "metros")
        if entry is None:
            reason = "no member cities" if not city_ids else "no crosswalk entry"
        elif not allowed:
            reason = "derived inputs not allowlisted"
        else:
            _publish_eurostat(
                by_id, metros, entry["place"], code, composition[code], city_ids
            )
            published.add(code)
            continue
        if city_ids:
            unpublished[code] = city_ids
        report.append(
            {
                "branch": "eurostat",
                "metro_code": code,
                "name": composition[code]["name"],
                "country": composition[code]["country"],
                "member_ids": city_ids,
                "reason": reason,
            }
        )
    candidates = _candidates(wikidata, unpublished)
    for row in report:
        if row.get("branch") == "eurostat":
            row["candidates"] = candidates.get(row["metro_code"], [])
    for row in assignments:
        row["published"] = (
            row["status"] == "assigned" and row["metro_code"] in published
        )
    summary = {
        "nuts_version": inputs_manifest.get("nuts_version"),
        "digests": inputs_manifest.get("digests"),
        "covered_countries": sorted(covered),
        "metros_published": len(published),
        "metros_reported": len(composition) - len(published),
        "assignments": dict(
            sorted(collections.Counter(row["status"] for row in assignments).items())
        ),
    }
    assigned = sum(1 for row in assignments if row["status"] == "assigned")
    inventory = _derived_inventory(inputs_manifest, assigned)
    return assignments, inventory, summary, len(published)


def _identify_metros(metros, registry, pins):
    """Give every metro row its registry id: found by its QID (a curated
    metro the seed identified) or minted, and enriched with its statistical
    code as a concordance."""
    if registry is None:
        return 0
    for qid in sorted(metros):
        row = metros[qid]
        code = row.get("statistical_area_id")
        if row.get("source_subtype") == "metropolitan region":
            namespace = "eurostat_metro"
            minted_in = f"eurostat {pins[eurostat.COMPOSITION_FILE]}"
        else:
            # Wikidata is live, so the provenance is a digest of the exact
            # identity this row was minted from, not a version.
            namespace = "cbsa"
            identity = {"qid": qid, "cbsa": code, "name": row.get("name")}
            digest = hashlib.sha256(
                json.dumps(identity, sort_keys=True).encode("utf-8")
            ).hexdigest()
            minted_in = f"wikidata P8138 {digest[:16]}"
        concordances = {"wikidata": [qid]}
        if code:
            concordances[namespace] = [str(code)]
        row["tp_id"] = registry.identify(
            concordances,
            kind="metro",
            name=row.get("name"),
            country_code=row.get("country_code"),
            minted_from=f"{namespace}:{code}" if code else f"wikidata:{qid}",
            minted_in=minted_in,
        )
        seed._shadow_gate(registry, row["tp_id"], qid)
        row["wikidata_id"] = qid
    return len(metros)


def attach_metros(
    cache_dir,
    *,
    wikidata=None,
    overrides_dir=None,
    strict=False,
    dataset=None,
    pins=None,
    registry=None,
    run=None,
):
    """Add metro places and memberships to the seed places.

    Runs the US and the Eurostat branches and republishes the places with
    metro rows appended and ``metro_ids`` / ``member_ids`` filled, beside the
    report (``metro_report.jsonl``) and the Eurostat assignments
    (``metro_assignments.jsonl``). ``dataset`` is the Overture
    ``division_area`` dataset the Eurostat branch reads city land areas from
    (the pinned release by default) and ``pins`` the Eurostat inputs' digests
    (the module's pins by default). One writer lock spans the seed read, the
    live queries and the publish, so a concurrent gazetteer run cannot shift
    the seed under it. With a ``registry`` session every metro row gets its
    registry id and ``wikidata_id``. Returns the generation manifest.
    """
    if wikidata is None:
        wikidata = overture.WikidataClient()
    pins = dict(pins or eurostat.PINS)
    # Prepared under the raw store's own lock, before the gazetteer lock below,
    # so the two are never held together.
    eurostat.prepare_inputs(cache_dir, expected=pins)

    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, seed_manifest = store.read_jsonl(
                cache_dir / "gazetteer",
                "seed.json",
                "places_seed.jsonl",
                generations=run,
            )
            by_id = {}
            for place in places:
                # Every row carries the metro and statistical-code fields, so a
                # metro row and a city row share one schema for the writer.
                place.setdefault("metro_ids", [])
                place.setdefault("statistical_area_id", None)
                by_id[place["place_id"]] = place
            place_overrides, places_digest = overrides.load_place_overrides(
                overrides_dir
            )
            overrides.expect_digest(
                seed_manifest.get("places_overrides_sha256"),
                places_digest,
                "places.yaml",
                "gazetteer",
            )

            metros = {}
            report = []
            override_report = []
            us_summary = _attach_us(places, by_id, metros, report, wikidata)
            assignments, derived_inventory, eurostat_summary, crosswalked = (
                _attach_eurostat(
                    places,
                    by_id,
                    metros,
                    report,
                    override_report,
                    cache_dir=cache_dir,
                    dataset=dataset,
                    pins=pins,
                    wikidata=wikidata,
                    place_overrides=place_overrides,
                )
            )

            for metro in metros.values():
                metro["member_ids"].sort()
            output = places + [
                metros[qid] for qid in sorted(metros) if qid not in by_id
            ]
            by_id.update({m["place_id"]: m for m in metros.values()})
            applied = crosswalked + _set_members(
                by_id,
                overrides.by_operation(place_overrides, "set_place_members"),
                override_report,
            )
            for place in output:
                place["metro_ids"] = sorted(place["metro_ids"])
            identified = _identify_metros(metros, registry, pins)

            manifest = {
                "source": "metros",
                "identified": identified,
                "registry_base": registry.base if registry is not None else None,
                # Carried forward so downstream lineage checks can prove the
                # expanded places descend from this catalogue snapshot.
                "sources": seed_manifest.get("sources"),
                "seed_generation": seed_manifest.get("generation"),
                "seed_places": seed_manifest.get("places"),
                "metros": len(metros),
                "cities_with_metro": sum(
                    1 for p in places if p["kind"] == "city" and p["metro_ids"]
                ),
                "reported": len(report),
                "us": us_summary,
                "eurostat": eurostat_summary,
                "derived_inventory": derived_inventory,
                "places_overrides_sha256": places_digest,
                "overrides_applied": applied,
                "stale_overrides": len(override_report),
                "stale_place_overrides": (
                    seed_manifest.get("stale_place_overrides") or 0
                )
                + len(override_report),
                "places": len(output),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            published = store.publish(
                cache_dir / "gazetteer",
                "metros.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(output),
                    "metro_report.jsonl": store.jsonl_chunks(report),
                    "metro_assignments.jsonl": store.jsonl_chunks(assignments),
                    "override_report.jsonl": store.jsonl_chunks(override_report),
                },
                manifest,
                held=directory,
                staged=run is not None,
            )
            if run is not None:
                run["metros.json"] = published["generation"]
            overrides.strict_check(strict, override_report, "metros")
            return published
    finally:
        directory.close()
