"""Attach metropolitan-area membership to the seeded places.

Three branches. US cities: Wikidata links a city to the metropolitan
statistical area it belongs to (P8138, class ``US_MSA_CLASS``), keyed by the
metro's CBSA code (P882), and each such metro is emitted as a ``metro`` place.
Cities of the countries Eurostat's pinned composition covers: the NUTS-3
region containing the city's Overture land areas gives its metropolitan
region, and every region with member cities is published as a ``metro`` keyed
by its Eurostat metro code; a curator ``set_statistical_area`` crosswalk may
instead merge it onto a chosen QID. Cities elsewhere: the FAO city-region a
city's Overture land area falls in is published as a ``metro`` keyed by its
region id and named from its GHS-UCDB centre. The Eurostat and FAO branches
publish only while their derived inputs are allowlisted, and never cover a
city another branch already placed. Every member city carries its metros in
``metro_ids`` — a city can belong to more than one. This stage adds
membership only; the geometry stage draws a metro from its members' shipped
polygons.
"""

import collections
import datetime
import functools
import json
import hashlib

import shapely

from transitio_index import (
    csv_source,
    eurostat,
    geometry,
    overrides,
    overture,
    pinned,
    seed,
    store,
)
from transitio_index.progress import progress

# The Overture land areas every derived membership is placed by.
OVERTURE_DERIVED = ("Overture Maps divisions", "CDLA-Permissive-2.0")
# The derived inputs the Eurostat branch needs approved, as allowlist keys;
# any one missing runs the branch report-only.
EUROSTAT_DERIVED = (
    OVERTURE_DERIVED,
    ("Eurostat metropolitan regions", "Eurostat-2011/833/EU"),
    ("GISCO NUTS 2021", "EuroGeographics-NC"),
)
# A FAO city-region's members were placed by Overture land areas in FAO
# regions, so both are derived inputs of the published membership.
FAO_DERIVED = (OVERTURE_DERIVED, geometry.FAO_DERIVED)
FAO_SUBTYPE = "city-region (FAO)"
# A definition of a metropolitan area the Eurostat branch derives: the
# manifest key its inputs are recorded under, the subtype its rows carry, the
# registry namespace (and crosswalk scheme) of its codes, the derived-source
# keys a membership consumes, and the pinned files those are versioned by —
# one per key after Overture's.
Definition = collections.namedtuple(
    "Definition", ["branch", "subtype", "namespace", "derived", "files"]
)
METROPOLITAN_REGION = Definition(
    "eurostat",
    "metropolitan region",
    "eurostat_metro",
    EUROSTAT_DERIVED,
    (eurostat.COMPOSITION_FILE, eurostat.BOUNDARIES_FILE),
)
DEFINITIONS = (METROPOLITAN_REGION,)
# What a derived input raises when it is missing, altered, unreadable or
# cannot be downloaded: the branch reading it is disabled, never the stage.
INPUT_UNAVAILABLE = (
    pinned.PinnedInputError,
    csv_source.IngestError,
    store.StoreError,
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


def _eurostat_place(definition, qid, code, metro):
    """A ``metro`` place row for a Eurostat metro of ``definition``."""
    return {
        "place_id": qid,
        "kind": "metro",
        "source_subtype": definition.subtype,
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


def _fao_place(key, region_id, name, country):
    """A ``metro`` place row for an auto-published FAO city-region."""
    return {
        "place_id": key,
        "kind": "metro",
        "source_subtype": FAO_SUBTYPE,
        "name": name,
        "names": {"en": name} if name else {},
        "resolution_method": "derived_from_fao",
        "parent_id": None,
        "country_code": country,
        "overture_id": None,
        "osm_relation_id": None,
        "statistical_area_id": region_id,
        "metro_ids": [],
        "member_ids": [],
    }


def _code_index(rows):
    """``{(subtype, code): metro}`` over the metro rows carrying a statistical
    code, so a metro keyed by that concordance rather than a QID is found by
    its code in one lookup. Codes are namespaced by the scheme's subtype: a
    CBSA never matches a Eurostat metro's code. Two rows carrying one code
    fail the build: the concordance names one place."""
    index = {}
    for row in rows:
        if row.get("kind") != "metro" or not row.get("statistical_area_id"):
            continue
        key = (row.get("source_subtype"), row["statistical_area_id"])
        if key in index:
            raise overture.GazetteerError(
                f"metros {index[key]['place_id']!r} and {row['place_id']!r} both "
                f"carry the {key[0]} code {key[1]!r}"
            )
        index[key] = row
    return index


def _join_member(by_id, metros, codes, qid, make_row, city, code):
    """The metro row for ``qid`` with ``city`` joined as a member, reciprocally.

    A curated metro of the same QID (``add_place``), or the one ``codes``
    indexes under the ``(subtype, code)`` pair ``code``, is the row and the
    statistical membership joins it, never a second row — the discovered QID
    becoming its QID; a QID seeded as anything but a metro fails the build; a
    curator's member list wins.
    """
    existing = by_id.get(qid)
    if existing is not None and existing.get("kind") != "metro":
        raise overture.GazetteerError(
            f"metro {qid!r} is already seeded as the "
            f"{existing['kind']} {existing.get('name')!r}"
        )
    indexed = codes.get(code)
    metro = metros.get(qid) or existing or indexed
    if indexed is not None and indexed is not metro:
        raise overture.GazetteerError(
            f"metros {qid!r} and {indexed['place_id']!r} both carry the "
            f"{code[0]} code {code[1]!r}"
        )
    if metro is None:
        # A new row is keyed by the key the lookup used — the survivor's
        # canonical key when the source named a merged alias — so a later
        # record for the same place finds it whatever the source order; the
        # QID the source named stays a concordance.
        metro = make_row()
        source_qid = metro["place_id"]
        metro["place_id"] = qid
        if source_qid != qid:
            metro.setdefault("discovered_qids", []).append(source_qid)
    elif overture.QID_PATTERN.match(qid):
        # Every QID a source names for the row: enriched onto it, or a
        # conflict, at identification.
        metro.setdefault("discovered_qids", []).append(qid)
    # Indexed under its code as soon as it is known, so a later record naming
    # the same code under another QID finds this row, never a second one.
    metros[metro["place_id"]] = codes[code] = metro
    join(metro, city)
    return metro


def join(metro, city):
    """Join ``city`` to ``metro`` as a member, reciprocally — unless the
    metro's members are curated: a curator's list is never added to."""
    if metro.get("members_curated"):
        return
    members = metro.setdefault("member_ids", [])
    if city["place_id"] not in members:
        members.append(city["place_id"])
    metro_ids = city.setdefault("metro_ids", [])
    if metro["place_id"] not in metro_ids:
        metro_ids.append(metro["place_id"])


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
        _unjoin(metro, by_id)
        metro["member_ids"] = sorted(set(entry["set_place_members"]))
        metro["members_curated"] = True
        for member in metro["member_ids"]:
            by_id[member].setdefault("metro_ids", [])
            if metro["place_id"] not in by_id[member]["metro_ids"]:
                by_id[member]["metro_ids"].append(metro["place_id"])
        applied += 1
    return applied


def _take_code(metro, code, scheme):
    """A metro takes the statistical ``code`` (of ``scheme``) it lacks; a
    different code already on it is a conflict."""
    code_now = metro.get("statistical_area_id")
    if code_now not in (None, code):
        raise overture.GazetteerError(
            f"metro {metro['place_id']!r} carries statistical code {code_now!r}, "
            f"not the {scheme} {code!r}"
        )
    metro["statistical_area_id"] = code


def _take_subtype(metro, subtype):
    """A metro takes the source ``subtype`` it lacks; a different one already
    on it — a row curated as one scheme's metro joined as another's — is a
    conflict."""
    now = metro.get("source_subtype")
    if now not in (None, subtype):
        raise overture.GazetteerError(
            f"metro {metro['place_id']!r} is a {now}, not a {subtype}"
        )
    metro["source_subtype"] = subtype


def _take_fao(metro, region_id):
    """The FAO identity a row joined by its region takes: the region id as
    its statistical code, and the subtype."""
    _take_code(metro, region_id, "FAO region")
    _take_subtype(metro, FAO_SUBTYPE)


def _canonical_key(registry, qid):
    """The key a QID a source names stands for inside the stage: the QID
    the registry keys that place by — a merged alias resolving to its
    survivor — or, without a registry or a row, the QID itself."""
    return qid if registry is None else registry.key_for(qid, internal=True)


def _take_msa(metro, record):
    """The discovered MSA identity: its CBSA code, and the country and
    subtype every US metro carries."""
    _take_code(metro, record["cbsa"], "CBSA")
    _take_subtype(metro, "metropolitan statistical area")
    metro["country_code"] = metro.get("country_code") or "US"


def _available(load):
    """``load()``'s result, or None when a derived input it reads is
    unavailable — so the branch fed by it degrades to nothing while the US
    branch still publishes."""
    try:
        return load()
    except INPUT_UNAVAILABLE:
        return None


def _unjoin(metro, by_id):
    """Take ``metro`` off every member city's ``metro_ids``."""
    for member in metro.get("member_ids") or []:
        city = by_id.get(member)
        if city is not None and metro["place_id"] in (city.get("metro_ids") or []):
            city["metro_ids"].remove(metro["place_id"])


def _partition(metro, by_id):
    """Give ``metro`` the country partition most of its members are in,
    recomputed over its full membership once every join and override is in,
    so a cross-border member list repartitions it; with members but no
    majority — none carries a country, or the leading countries tie — it
    cannot publish, whichever stage finds it so. A metro with no member yet
    (a crosswalked placeholder awaiting the cities the expand stage finds)
    has only its source's country to be partitioned by, and keeps it.
    Returns whether it can publish."""
    members = metro.get("member_ids") or []
    if not members:
        return bool(metro.get("country_code"))
    country = _majority_country(members, by_id)
    if country is None:
        return False
    metro["country_code"] = country
    return True


def partition(rows, by_id, codes, report):
    """Partition every metro in ``rows`` (``{key: row}``) by country. One
    that cannot publish — its members give no majority country, or it has
    neither members nor a country — is unjoined from its members, reported
    and removed from ``rows``, ``by_id`` and the code index ``codes``, so no
    later join finds it; every place belongs to one country partition.
    Returns the keys removed."""
    dropped = set()
    for key, metro in list(rows.items()):
        if _partition(metro, by_id):
            continue
        _unjoin(metro, by_id)
        report.append(
            {
                "kind": "metro",
                "metro_id": metro["place_id"],
                "reason": "no majority country",
            }
        )
        del rows[key]
        by_id.pop(key, None)
        code = (metro.get("source_subtype"), metro.get("statistical_area_id"))
        if codes.get(code) is metro:
            del codes[code]
        dropped.add(key)
    return dropped


def reconcile(assignments, codes):
    """Clear the ``published`` flag of every Eurostat-branch assignment (of
    any definition, named by its ``subtype``) whose metro the partition pass
    dropped from the code index ``codes``, so the FAO branch counts its city
    as covered by nothing."""
    for row in assignments:
        if row.get("published") and (row["subtype"], row["metro_code"]) not in codes:
            row["published"] = False


def _attach_us(places, by_id, metros, codes, report, wikidata, registry=None):
    """The US branch: each US city's MSA from Wikidata; an MSA without a CBSA
    code is reported for a later pass rather than published. Returns the
    branch summary."""
    # Wikidata answers for QIDs only; a city keyed by its own id has none.
    us_cities = [
        p["place_id"]
        for p in places
        if p["kind"] == "city"
        and p["country_code"] == "US"
        and overture.QID_PATTERN.match(p["place_id"])
    ]
    membership = wikidata.statistical_metros(us_cities) if us_cities else {}
    published = set()
    reported_metros = set()
    for city_qid, found in progress(membership.items(), "metros"):
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
                codes,
                _canonical_key(registry, record["qid"]),
                functools.partial(_metro_place, record),
                city,
                ("metropolitan statistical area", record["cbsa"]),
            )
            _take_msa(metro, record)
            published.add(metro["place_id"])
    return {"metros_published": len(published), "metros_reported": len(reported_metros)}


def _crosswalk(place_overrides, composition, by_id, metros, scheme):
    """``{metro_code: entry}`` from the crosswalk entries of ``scheme``,
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
        if spec["scheme"] != scheme:
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


def _publish_eurostat(definition, by_id, metros, codes, qid, code, metro, city_ids):
    """Publish a Eurostat metro of ``definition`` under ``qid`` — its own code
    key, or the QID a crosswalk merges it onto: a new row, or the curated
    metro of that QID (checked compatible by :func:`_crosswalk`) joined by
    the derived members (possibly none) and given the identity fields, the
    curator keeping name and members."""
    make_row = functools.partial(_eurostat_place, definition, qid, code, metro)
    row = metros.get(qid) or by_id.get(qid) or make_row()
    # Indexed under its code here too, members or none: a row already there
    # under another key would be a second metro with the same concordance.
    indexed = codes.get((definition.subtype, code))
    if indexed is not None and indexed is not row:
        raise overture.GazetteerError(
            f"metros {qid!r} and {indexed['place_id']!r} both carry the "
            f"{definition.subtype} code {code!r}"
        )
    metros[qid] = codes[(definition.subtype, code)] = row
    for city_id in city_ids:
        _join_member(
            by_id,
            metros,
            codes,
            qid,
            make_row,
            by_id[city_id],
            (definition.subtype, code),
        )
    row.update(
        source_subtype=definition.subtype,
        resolution_method="statistical_code",
        country_code=metro["country"],
        statistical_area_id=code,
    )
    return row


def _candidates(wikidata, members):
    """Per unpublished metro code, the Wikidata metro-like entities its member
    cities link to and how many members link to each — the curator's
    shortlist, never an identity."""
    cities = sorted(
        {
            city
            for ids in members.values()
            for city in ids
            if overture.QID_PATTERN.match(city)
        }
    )
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


def _derived_inventory(consumed):
    """The licence inventory: one row per derived input a branch read, at its
    pinned version and with the credit and terms its source requires, allowed
    or not, and the memberships derived with it. ``consumed`` are the
    branches' ``{(dataset, licence): (version, memberships)}`` maps; an input
    two branches share (Overture, for Eurostat and FAO alike) is one row with
    their memberships summed."""
    merged = {}
    for branch in consumed:
        for key, (version, memberships) in branch.items():
            _, counted = merged.get(key, (version, 0))
            merged[key] = (version, counted + memberships)
    rows = []
    for (dataset, licence), (version, memberships) in sorted(merged.items()):
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
                "version": version,
                "allowed": (dataset, licence) in geometry.DERIVED_SOURCE_ALLOWLIST,
                "memberships": memberships,
            }
        )
    return rows


def _consumed(definition, inputs_manifest, memberships):
    """A definition's derived inputs at their pinned versions."""
    digests = inputs_manifest["digests"]
    versions = (
        overture.OVERTURE_RELEASE,
        *(digests[name] for name in definition.files),
    )
    return {
        key: (version, memberships)
        for key, version in zip(definition.derived, versions)
    }


def _fao_version(inputs_manifest):
    """The version the FAO derived row records: both pinned inputs and the
    cutoff, since a membership depends on all three."""
    from transitio_index import fao

    digests = inputs_manifest["digests"]
    return overrides.canonical_digest(
        {
            "patches": digests[fao.PATCHES_FILE],
            "regions": digests[fao.REGIONS_FILE],
            "cutoff_hours": fao.CUTOFF_HOURS,
        }
    )


def _majority_country(city_ids, by_id):
    """The country more than half of ``city_ids`` — those carrying a
    country — are in, the metro's one country partition; None when none
    carries one or no country has a majority: member order must never
    choose a partition."""
    counts = collections.Counter(
        by_id[c].get("country_code")
        for c in city_ids
        if by_id.get(c) and by_id[c].get("country_code")
    )
    if not counts:
        return None
    country, count = counts.most_common(1)[0]
    return country if count * 2 > sum(counts.values()) else None


def official_footprints(by_id, footprint):
    """The footprints — ``footprint(place)``, None for none — of every city in
    a US or Eurostat metro, seeded or curated rows included, for the FAO
    duplicate check: a FAO city-region whose core falls inside one duplicates
    it. FAO members are excluded: a FAO region is deduplicated only against the
    official (US/Eurostat) metros, never against another FAO metro — else a
    city discovered in a region already published would reject the region as
    its own duplicate."""
    official = {
        p["place_id"]
        for p in by_id.values()
        if p.get("kind") == "metro" and p.get("source_subtype") != FAO_SUBTYPE
    }
    footprints = []
    for place in by_id.values():
        if place.get("kind") != "city":
            continue
        if not any(mid in official for mid in place.get("metro_ids") or []):
            continue
        geom = footprint(place)
        if geom is not None:
            footprints.append(geom)
    return footprints


def _region_footprint(regions, patches, region_id):
    """The land footprint of a FAO city-region: its patches' union."""
    return eurostat._footprint(
        [
            {"geom": patches[p]["geom"]}
            for p in regions[region_id]["patches"]
            if p in patches
        ]
    )


def _over_published_metro(footprint, published):
    """Whether a FAO city-region's ``footprint`` sits over a metro already
    published: its representative point falls inside a published metro member's
    footprint held in the ``published`` STRtree."""
    if footprint is None:
        return False
    point = footprint.representative_point()
    return bool(len(published.query(point, predicate="covered_by")))


def _report_fao(report, region_id, reason):
    """One report row for a FAO city-region left unpublished."""
    report.append(
        {
            "branch": "fao",
            "metro_id": f"fao_city_region:{region_id}",
            "code": region_id,
            "reason": reason,
        }
    )


def _fao_name(names, region_id, members, by_id, footprint):
    """A FAO city-region's name: its GHS-UCDB centre's, else — no centre
    matched, or the UCDB not read — the name of the member with the largest
    footprint (``footprint(place)``, None for none), the core city as a rule,
    so every published metro is searchable by name."""
    named = names.get(region_id)
    if named:
        return named["name"]

    def extent(city_id):
        geom = footprint(by_id[city_id])
        return geom.area if geom is not None else 0.0

    return by_id[max(members, key=lambda c: (extent(c), c))].get("name")


def _snapshot(inputs, keys):
    """The digests a branch published metros from — its loaded ``inputs``' —
    or None when it read none, or published none for want of its ``keys``
    all allowlisted, so a later stage derives nothing either."""
    if inputs is None or not all(k in geometry.DERIVED_SOURCE_ALLOWLIST for k in keys):
        return None
    return inputs[2]["digests"]


def _apply_fao(
    places,
    by_id,
    metros,
    codes,
    report,
    *,
    inputs,
    names,
    cache_dir,
    dataset,
    assignments,
):
    """Publish an FAO city-region metro for every region of the loaded
    ``inputs`` holding eligible cities — cities no US or Eurostat metro
    already covers. Each metro is keyed by its FAO region id, named from the
    region's GHS-UCDB centre match in ``names`` (empty unless the UCDB derived
    input was read), its country the one most of its cities are in, and the
    eligible cities joined as members. It publishes only while every FAO
    derived input is allowlisted; otherwise the regions are reported, not
    published. Returns ``(versions, touched)`` — the derived inputs read, at
    their pinned versions, as ``{(dataset, licence): version}``, and every
    region's metro joined as ``{key: members joined}``."""
    from transitio_index import fao

    regions, patches, inputs_manifest = inputs
    wanted = {
        p["overture_id"] for p in places if p["kind"] == "city" and p.get("overture_id")
    }
    areas = geometry.place_areas(cache_dir, dataset, places, wanted)
    grouped, _, _, _, _ = fao.place_cities(places, areas, regions, patches, assignments)
    allowed = all(key in geometry.DERIVED_SOURCE_ALLOWLIST for key in FAO_DERIVED)
    # A FAO city-region whose core sits inside a metro already published this
    # run (Eurostat or US) duplicates it; those are dropped, not minted again.
    duplicates = shapely.STRtree(
        official_footprints(by_id, functools.partial(eurostat.place_footprint, areas))
    )
    touched = {}
    for region_id in sorted(grouped):
        derived = sorted(grouped[region_id])
        # A metro belongs to one country partition, so a cross-border region
        # takes the country most of its cities are in; one with no majority is
        # reported rather than published countryless.
        country = _majority_country(derived, by_id)
        if country is None:
            _report_fao(report, region_id, "no majority country")
            continue
        if not allowed:
            _report_fao(report, region_id, "a derived input is not allowlisted")
            continue
        footprint = _region_footprint(regions, patches, region_id)
        if _over_published_metro(footprint, duplicates):
            _report_fao(report, region_id, "duplicate of a published metro")
            continue
        key = f"fao_city_region:{region_id}"
        name = _fao_name(
            names,
            region_id,
            derived,
            by_id,
            functools.partial(eurostat.place_footprint, areas),
        )
        make_row = functools.partial(_fao_place, key, region_id, name, country)
        for city_id in derived:
            row = _join_member(
                by_id,
                metros,
                codes,
                key,
                make_row,
                by_id[city_id],
                (FAO_SUBTYPE, region_id),
            )
        # A seeded row joined by its key takes the region's identity, as a
        # US row takes its MSA's; a different code already on it conflicts.
        _take_fao(row, region_id)
        touched[row["place_id"]] = len(derived)
    versions = {
        OVERTURE_DERIVED: overture.OVERTURE_RELEASE,
        geometry.FAO_DERIVED: _fao_version(inputs_manifest),
    }
    return versions, touched


def _attach_definition(
    definition,
    places,
    by_id,
    metros,
    codes,
    report,
    override_report,
    *,
    inputs,
    areas,
    wikidata,
    place_overrides,
):
    """The Eurostat branch for one ``definition`` over its loaded ``inputs``
    and the cities' land ``areas``; returns ``(assignments, consumed,
    summary, applied)`` — the assignment rows (each naming the definition's
    subtype), the derived inputs read as ``{(dataset, licence): (version,
    memberships)}``, the branch summary, and the crosswalk entries that
    published.

    Every city of a covered country gets an assignment row. Every crosswalk
    entry of the definition's scheme is judged against the sorted derived
    member list it named, whether or not its metro publishes. A metro with
    member cities publishes under its own code, or under the QID a crosswalk
    entry merges it onto — even before any member is derived, so the expand
    stage joins the cities it discovers there to the curator's QID rather
    than minting a code-keyed twin — while every derived input is
    allowlisted; every other composition metro is reported with the reason
    and its candidates.
    """
    composition, boundaries, inputs_manifest = inputs
    covered = eurostat.countries(composition)
    assignments = eurostat.assign(places, areas, composition, boundaries)
    for row in assignments:
        row["subtype"] = definition.subtype
    members = {}
    for row in assignments:
        if row["status"] == "assigned":
            members.setdefault(row["metro_code"], set()).add(row["city_id"])
    # Canonical member lists: what the evidence hash is taken over.
    members = {code: sorted(ids) for code, ids in members.items()}

    crosswalk = _crosswalk(
        place_overrides, composition, by_id, metros, definition.namespace
    )
    allowed = all(
        key in geometry.DERIVED_SOURCE_ALLOWLIST for key in definition.derived
    )
    published = set()
    crosswalked = 0
    unpublished = {}
    # A metro with member cities publishes on its own Eurostat metro code; a
    # curator crosswalk may instead name a QID to merge it onto. A metro with
    # no derived members, or an unallowlisted input, is reported not published.
    for code in sorted(composition):
        city_ids = members.get(code, [])
        entry = crosswalk.get(code)
        if entry is not None:
            overrides.judge(entry, city_ids, override_report, "metros")
        if entry is None and not city_ids:
            reason = "no member cities"
        elif not allowed:
            reason = "derived inputs not allowlisted"
        else:
            key = f"{definition.namespace}:{code}" if entry is None else entry["place"]
            _publish_eurostat(
                definition, by_id, metros, codes, key, code, composition[code], city_ids
            )
            published.add(code)
            if entry is not None:
                crosswalked += 1
            continue
        if city_ids:
            unpublished[code] = city_ids
        report.append(
            {
                "branch": definition.branch,
                "metro_code": code,
                "name": composition[code]["name"],
                "country": composition[code]["country"],
                "member_ids": city_ids,
                "reason": reason,
            }
        )
    candidates = _candidates(wikidata, unpublished)
    for row in report:
        if row.get("branch") == definition.branch:
            row["candidates"] = candidates.get(row["metro_code"], [])
    for row in assignments:
        row["published"] = (
            row["status"] == "assigned" and row["metro_code"] in published
        )
    summary = {
        **{
            key: inputs_manifest[key]
            for key in ("nuts_version", "edition")
            if key in inputs_manifest
        },
        "digests": inputs_manifest.get("digests"),
        "covered_countries": sorted(covered),
        "metros_published": len(published),
        "metros_reported": len(composition) - len(published),
        "assignments": dict(
            sorted(collections.Counter(row["status"] for row in assignments).items())
        ),
    }
    assigned = sum(1 for row in assignments if row["status"] == "assigned")
    return (
        assignments,
        _consumed(definition, inputs_manifest, assigned),
        summary,
        crosswalked,
    )


def _identify_metros(metros, registry, derived):
    """Give every metro row its registry id: found by its QID (a curated
    metro the seed identified) or minted, and enriched with its statistical
    code as a concordance of its scheme. ``derived`` — the digests the
    Eurostat and FAO branches read, by branch — dates the provenance of the
    metros they minted."""
    if registry is None:
        return 0
    for qid in sorted(metros):
        row = metros[qid]
        code = row.get("statistical_area_id")
        definition = next(
            (d for d in DEFINITIONS if row.get("source_subtype") == d.subtype), None
        )
        if definition is not None:
            namespace = definition.namespace
            version = derived[definition.branch][definition.files[0]]
            minted_in = f"{definition.branch} {version}"
        elif row.get("source_subtype") == FAO_SUBTYPE:
            from transitio_index import fao

            namespace = "fao_city_region"
            minted_in = f"fao {derived['fao'][fao.REGIONS_FILE]}"
        else:
            # Wikidata is live, so the provenance is a digest of the exact
            # identity this row was minted from, not a version.
            namespace = "cbsa"
            identity = {"qid": qid, "cbsa": code, "name": row.get("name")}
            digest = hashlib.sha256(
                json.dumps(identity, sort_keys=True).encode("utf-8")
            ).hexdigest()
            minted_in = f"wikidata P8138 {digest[:16]}"
        concordances = seed._key_concordance(qid, registry)
        for discovered in row.pop("discovered_qids", []):
            if discovered not in concordances.get("wikidata", []):
                concordances.setdefault("wikidata", []).append(discovered)
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
        row["wikidata_id"] = registry.canonical_qid(row["tp_id"])
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
    fao_files=None,
    fao_pins=None,
    ucdb_pins=None,
    derive_fao=True,
):
    """Add metro places and memberships to the seed places.

    Runs the US, Eurostat and FAO branches and republishes the places with
    metro rows appended and ``metro_ids`` / ``member_ids`` filled, beside the
    report (``metro_report.jsonl``) and the Eurostat assignments
    (``metro_assignments.jsonl``). ``dataset`` is the Overture
    ``division_area`` dataset the branches read city land areas from
    (the pinned release by default) and ``pins`` the Eurostat inputs' digests
    (the module's pins by default). One writer lock spans the seed read, the
    live queries and the publish, so a concurrent gazetteer run cannot shift
    the seed under it. With a ``registry`` session every metro row gets its
    registry id and ``wikidata_id``. ``fao_files``, ``fao_pins`` and
    ``ucdb_pins`` name the FAO and UCDB inputs the FAO branch derives and names
    city-regions from (the modules' pins by default). ``derive_fao`` runs the
    FAO branch (on by default). A derived input that is unavailable disables
    the branch reading it — the manifest's ``derived_inputs`` records, per
    branch, the digests read or ``None`` — so the stage still publishes the US
    metros. Returns the generation manifest.
    """
    if wikidata is None:
        wikidata = overture.WikidataClient()
    pins = dict(pins or eurostat.PINS)
    from transitio_index import fao, ucdb

    fao_pins = dict(fao_pins or fao.PINS)
    ucdb_pins = dict(ucdb_pins or ucdb.PINS)
    # Prepared under the raw store's own lock, before the gazetteer lock below,
    # so the two are never held together. An input that cannot be fetched or
    # verified is left unprepared: the load under the lock then disables its
    # branch rather than aborting the stage.
    _available(lambda: eurostat.prepare_inputs(cache_dir, expected=pins))
    if derive_fao:
        _available(
            lambda: (
                fao.prepare_inputs(cache_dir, files=fao_files, expected=fao_pins),
                fao.convert_patches(cache_dir, expected=fao_pins),
            )
        )
        _available(lambda: ucdb.prepare_inputs(cache_dir, expected=ucdb_pins))

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
            if registry is not None:
                # The stage joins Wikidata and Eurostat by QID: the seed's
                # rows are keyed by it again here, and by their ids below.
                by_id = seed.rekey_by_qid(by_id)
            place_overrides, places_digest = overrides.load_place_overrides(
                overrides_dir, registry=registry, internal=True
            )
            overrides.expect_digest(
                seed_manifest.get("places_overrides_sha256"),
                places_digest,
                "places.yaml",
                "gazetteer",
            )

            # The branches' inputs, read before anything is joined: one that
            # is missing, altered or unreadable disables its branch (None)
            # rather than aborting the stage. Names are a derived use of the
            # UCDB, read only while its allowlist entry stands.
            euro = _available(lambda: eurostat.load_inputs(cache_dir, expected=pins))
            fao_inputs = None
            names, names_manifest = {}, None
            if derive_fao:
                fao_inputs = _available(
                    lambda: fao.load_inputs(cache_dir, expected=fao_pins)
                )
            if (
                fao_inputs is not None
                and ucdb.DERIVED in geometry.DERIVED_SOURCE_ALLOWLIST
            ):
                names, names_manifest = _available(
                    lambda: ucdb.load_names(cache_dir, expected=ucdb_pins)
                ) or ({}, None)

            metros = {}
            codes = _code_index(by_id.values())
            report = []
            override_report = []
            consumed = []
            us_summary = _attach_us(
                places, by_id, metros, codes, report, wikidata, registry
            )
            # The cities' land areas, read once for every definition: those
            # of every country any definition covers.
            branches = ((METROPOLITAN_REGION, euro),)
            covered = set()
            for _, inputs in branches:
                if inputs is not None:
                    covered |= eurostat.countries(inputs[0])
            wanted = {
                p["overture_id"]
                for p in places
                if p["kind"] == "city"
                and p["country_code"] in covered
                and p.get("overture_id")
            }
            areas = geometry.place_areas(cache_dir, dataset, places, wanted)
            assignments, crosswalked = [], 0
            summaries = {}
            for definition, inputs in branches:
                if inputs is None:
                    summaries[definition.subtype] = {
                        "status": "inputs unavailable",
                        "metros_published": 0,
                    }
                    continue
                rows, consumed_by, summary, applied = _attach_definition(
                    definition,
                    places,
                    by_id,
                    metros,
                    codes,
                    report,
                    override_report,
                    inputs=inputs,
                    areas=areas,
                    wikidata=wikidata,
                    place_overrides=place_overrides,
                )
                assignments.extend(rows)
                consumed.append(consumed_by)
                summaries[definition.subtype] = summary
                crosswalked += applied

            # The seed places' keys before the metros join them, so the metro
            # rows minted in this stage (US, Eurostat and FAO) are told apart.
            city_keys = set(by_id)
            by_id.update({m["place_id"]: m for m in metros.values()})
            applied = crosswalked + _set_members(
                by_id,
                overrides.by_operation(place_overrides, "set_place_members"),
                override_report,
            )
            # The official metros are partitioned before the FAO branch, so a
            # city whose metro cannot publish is eligible for a city-region
            # instead of covered by nothing; FAO metros are minted partitioned.
            official = {
                p["place_id"]: p
                for p in [*places, *metros.values()]
                if p["kind"] == "metro"
            }
            dropped = partition(official, by_id, codes, report)
            for key in dropped:
                row = metros.pop(key, None)
                if row is None:
                    continue  # a seeded metro this run did not publish
                # The branch summaries describe what survives to publish.
                summary = summaries.get(row.get("source_subtype"))
                if summary is not None:
                    summary["metros_published"] -= 1
                    summary["metros_reported"] += 1
                elif row.get("source_subtype") == "metropolitan statistical area":
                    us_summary["metros_published"] -= 1
            # A Eurostat metro the pass dropped covers nothing any more.
            reconcile(assignments, codes)
            fao_summary = {"published": 0, "memberships": 0}
            if fao_inputs is not None:
                fao_versions, fao_touched = _apply_fao(
                    places,
                    by_id,
                    metros,
                    codes,
                    report,
                    inputs=fao_inputs,
                    names=names,
                    cache_dir=cache_dir,
                    dataset=dataset,
                    assignments=assignments,
                )
                # A seeded FAO metro joined here is repartitioned over its
                # grown membership; one left without a majority is dropped.
                fao_dropped = partition(
                    {key: metros[key] for key in fao_touched}, by_id, codes, report
                )
                for key in fao_dropped:
                    metros.pop(key, None)
                dropped |= fao_dropped
                fao_published = {
                    key: joined
                    for key, joined in fao_touched.items()
                    if key not in fao_dropped
                }
                fao_memberships = sum(fao_published.values())
                if names_manifest is not None:
                    # Versioned by both pinned UCDB inputs, like the FAO row.
                    fao_versions[ucdb.DERIVED] = overrides.canonical_digest(
                        names_manifest["sources"]
                    )
                consumed.append(
                    {
                        key: (version, fao_memberships)
                        for key, version in fao_versions.items()
                    }
                )
                fao_summary = {
                    "published": len(fao_published),
                    "memberships": fao_memberships,
                }
            derived_inputs = {
                "eurostat": _snapshot(euro, EUROSTAT_DERIVED),
                "fao": _snapshot(fao_inputs, FAO_DERIVED),
                "ucdb": (
                    names_manifest["sources"] if names_manifest is not None else None
                ),
            }
            for metro in metros.values():
                metro["member_ids"].sort()
            output = places + [
                metros[key] for key in sorted(metros) if key not in city_keys
            ]
            output = [p for p in output if p["place_id"] not in dropped]
            for place in output:
                place["metro_ids"] = sorted(place["metro_ids"])
            identified = _identify_metros(metros, registry, derived_inputs)
            if registry is not None:
                output = list(
                    seed.rekey_by_own_id(
                        {p["place_id"]: p for p in output},
                        registry,
                        records=[*assignments, *report],
                    ).values()
                )

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
                "eurostat": summaries[METROPOLITAN_REGION.subtype],
                "fao": fao_summary,
                "derived_inputs": derived_inputs,
                "derived_inventory": _derived_inventory(consumed),
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
