"""Declared-seed resolution: feed locations to gazetteer places.

Matches each feed's declared municipality (MDB) or ``Location`` (``systems.csv``)
to an Overture locality/localadmin by name within its country — disambiguated by
the declared subdivision — resolves that division to a QID with the same rules as
the skeleton stage, and emits the city place plus its administrative ancestors as
``places_seed.jsonl``. A municipality no locality is named by is matched against
the skeleton's regions and counties instead — catalogues name districts there —
and placed at that division. A feed that declares only a subdivision resolves to
that region. A feed whose location does not resolve to a single place — a
QID-bearing division, or a named one no QID names, identified by its Overture
id — is reported, never minted. The capitals and the cities of at least
200,000 people (GHS-UCDB urban centres) of the countries a build lists are
seeded too, with or without feeds.

Matching folds accents and case and considers every language label a division
carries, so a feed naming a place in a local language still resolves. Only feeds
with a declared place name are placed here; geometry-based placement (MDB
bounding-box centroids, Atlas geohashes) and the boundary geometry are a later
stage. The locality read is streamed and kept to the names feeds actually
declare, so the 3.5M-row locality universe is never materialised.
"""

import datetime
import logging
import re
import unicodedata

import pyarrow.dataset as ds
import shapely

from transitio_index import country_codes, crosswalk, overrides, overture, pinned, store
from transitio_index import registry as _registry
from transitio_index.progress import progress

log = logging.getLogger(__name__)

# The Overture subtypes that stand in for a city, most specific first: a name
# resolving to both prefers the locality (decision in the plan's subtype table).
CITY_SUBTYPES = ("locality", "localadmin")
# A city candidate's columns: the gazetteer's, its label point and population.
CANDIDATE_COLUMNS = [*overture.PROJECT, "geometry", "population"]

# The GHS-UCDB centres seeded as places, matched to Overture once per release
# and rules version (``prepare_centres``).
CENTRES_POINTER = "ucdb-centres.json"
CENTRES_FILE = "centres.jsonl"
CENTRES_VERSION = 1
# How near (degrees) a label point outside a centre's polygon may lie when
# none lies inside.
NEAR_DEG = 0.05
# Overture's spelling of a name the UCDB spells otherwise.
UCDB_SPELLINGS = {"Sri Jayawardenepura Kotte": "Sri Jayewardenepura Kotte"}
_BRACKET = re.compile(r"\A(.*?)\s*\[(.*)\]\s*\Z")


def _norm(name):
    """A name folded for matching: accents stripped, case-folded, spaced flat.

    ``casefold`` rather than ``lower`` so case-equivalent labels that differ
    under simple lowercasing — German ``Straße`` versus ``STRASSE`` — still match.
    """
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.casefold().split())


def _name_variants(record):
    """Every folded label a division carries: its primary name and each common."""
    variants = {_norm(record.get("name"))}
    for label in (record.get("names") or {}).values():
        variants.add(_norm(label))
    variants.discard("")
    return variants


# The countries whose same-named county is a city's own council area; in
# others it is a district larger than the town (a Swiss Bezirk, a Kenyan
# sub-county).
COUNCIL_AREA_COUNTRIES = frozenset({"CA", "CO", "GB"})

# The countries whose municipality carries its own QID and is named after its
# town, which has no area of its own; the value is the municipality's subtype.
MUNICIPALITY_SUBTYPES = {"DK": "county", "NO": "county", "SE": "county", "SI": "region"}

# The suffixes a municipality's name adds to its town's, which may take a
# genitive "s": "Stockholms kommun", "Göteborgs Stad", "Københavns Kommune".
_MUNICIPALITY_SUFFIXES = (" municipality", " kommun", " kommune", " stad")


def _area_names(record):
    """The folded names a division carries as a city's council area: each
    label, and each without a ``City`` affix — "City of Edinburgh" and
    "Glasgow City" carry Edinburgh's and Glasgow's names — or a municipality
    suffix, its genitive "s" taken off too: "Stockholms kommun" carries
    Stockholm's."""
    names = set()
    for name in _name_variants(record):
        names.add(name)
        if name.startswith("city of "):
            names.add(name[len("city of ") :])
        if name.endswith(" city"):
            names.add(name[: -len(" city")])
        for suffix in _MUNICIPALITY_SUFFIXES:
            if name.endswith(suffix):
                town = name[: -len(suffix)]
                names.add(town)
                if town.endswith("s"):
                    names.add(town[:-1])
    names.discard("")
    return names


def council_unit(area):
    """Whether ``area`` may be a city's own council area: a county no QID
    names in ``COUNCIL_AREA_COUNTRIES``, or a municipality of the subtype
    ``MUNICIPALITY_SUBTYPES`` gives its country, with or without a QID. A
    candidate record and a place row answer alike."""
    country = area.get("country") or area.get("country_code")
    subtype = area.get("source_subtype")
    if country in COUNCIL_AREA_COUNTRIES:
        return subtype == "county" and area.get("resolution_method") == "overture_id"
    return (
        country in MUNICIPALITY_SUBTYPES and subtype == MUNICIPALITY_SUBTYPES[country]
    )


def council_area(city, area):
    """Whether ``area`` is ``city``'s own council area: a council unit
    (``council_unit``) that carries the city's name — Manchester's
    Manchester, Edinburgh's City of Edinburgh, Stockholm's Stockholms kommun.
    A candidate record and a place row answer alike."""
    return council_unit(area) and bool(_name_variants(city) & _area_names(area))


def declared_locations(feeds):
    """One declared location per placeable feed, finest declared level first.

    A feed is placed at the finest level its catalogues name: an MDB (or a
    curated feed's) municipality or subdivision, else a GBFS ``Location``, else
    a bare country code — the plan treats each as valid declared coverage. A
    feed that names no
    country at all is skipped; it is placed geometrically in a later stage. The
    ``municipality`` and ``subdivision`` may both be ``None`` for a country-only
    feed.
    """
    for feed in feeds:
        location = crosswalk.declared_location(feed)
        gbfs = feed.get("gbfs") or {}
        mdb_country = location.get("country_code")
        municipality = location.get("municipality")
        subdivision = location.get("subdivision_name")
        if mdb_country and (municipality or subdivision):
            country = mdb_country
        elif gbfs.get("country_code") and gbfs.get("location"):
            country, subdivision, municipality = (
                gbfs["country_code"],
                None,
                gbfs["location"],
            )
        elif mdb_country or gbfs.get("country_code"):
            country = mdb_country or gbfs["country_code"]
            subdivision, municipality = None, None
        else:
            continue
        yield {
            "feed_id": feed["feed_id"],
            "country": country.upper(),
            "subdivision": subdivision,
            "municipality": municipality,
        }


def _subdivision_names(candidate, skeleton):
    """The folded names of a candidate's region and county ancestors.

    Both admin levels are considered — a declared ``subdivision_name`` may name
    either — and each ancestor's full set of language labels is taken from the
    skeleton the ancestor was resolved into, so a subdivision named in a local
    language still corroborates. Falls back to the hierarchy label when the
    ancestor was not resolved.
    """
    names = set()
    for ancestor in candidate.get("ancestors", []):
        if ancestor.get("subtype") not in ("region", "county"):
            continue
        resolved = skeleton.get(ancestor.get("overture_id"))
        if resolved is not None:
            names |= _name_variants(resolved)
        else:
            names.add(_norm(ancestor.get("name")))
    return names


def read_city_candidates(dataset, countries, wanted):
    """Normalised locality/localadmin records feeds actually name, each with
    its label point (``point``, WKB) and Overture ``population``.

    Streamed with a country + subtype predicate so only the relevant partitions
    are scanned, and kept only where one of a division's ``(country, folded
    name)`` labels is one a feed declared, so the working set is bounded by the
    feeds, not the theme. ``countries=None`` scans every country for
    ``(None, folded name)`` labels.
    """
    if (countries is not None and not countries) or not wanted:
        return []
    predicate = ds.field("subtype").isin(list(CITY_SUBTYPES))
    if countries is not None:
        predicate &= ds.field("country").isin(sorted(countries))
    kept = []
    for batch in progress(
        dataset.to_batches(columns=CANDIDATE_COLUMNS, filter=predicate),
        "seed localities",
    ):
        for row in batch.to_pylist():
            primary, labels = overture._names(row["names"])
            country = None if countries is None else row["country"]
            names = _name_variants({"name": primary, "names": labels})
            if any((country, name) in wanted for name in names):
                record = overture.normalize_division(row)
                record["point"] = row["geometry"]
                record["population"] = row["population"]
                kept.append(record)
    return kept


def city_qids(dataset, qids):
    """The QIDs among ``qids`` that a locality or localadmin carries in the
    theme — one scan by QID. A district or region sharing its QID with a
    city-level division is a city that is its own district; the place is the
    city, as ``match`` decides for a declared name."""
    wanted = {qid for qid in qids if qid}
    if not wanted:
        return set()
    predicate = ds.field("subtype").isin(list(CITY_SUBTYPES)) & ds.field(
        "wikidata"
    ).isin(sorted(wanted))
    found = set()
    for batch in dataset.to_batches(columns=["wikidata"], filter=predicate):
        found.update(batch.column("wikidata").to_pylist())
    return found & wanted


def council_area_cities(dataset, areas):
    """``{area overture_id: locality record}``: the one QID-bearing locality
    each of ``areas`` is the council area of (see ``council_area``), its
    ancestry cut at the area so the city sits in it. One locality scan over
    the areas' countries."""
    by_id = {area["overture_id"]: area for area in areas}
    countries = {area["country"] for area in areas}
    wanted = {(area["country"], name) for area in areas for name in _area_names(area)}
    found = {}
    for record in read_city_candidates(dataset, countries, wanted):
        if record["subtype"] != "locality" or not record["wikidata"]:
            continue
        for depth, ancestor in enumerate(record["ancestors"]):
            area = by_id.get(ancestor.get("overture_id"))
            if area is not None and council_area(record, area):
                record["ancestors"] = record["ancestors"][: depth + 1]
                found.setdefault(area["overture_id"], []).append(record)
                break
    return {key: cities[0] for key, cities in found.items() if len(cities) == 1}


def _resolve_candidates(candidates, wikidata):
    """Attach a resolved ``qid``/``resolution_method`` to each candidate."""
    pending = {
        relation
        for record in candidates
        if not record["wikidata"]
        for relation in record["osm_relation_ids"]
    }
    p402_map = wikidata.p402(pending) if pending else {}
    for record in candidates:
        qid, method, reason = overture.resolve_qid(record, p402_map)
        record["qid"] = qid
        record["resolution_reason"] = reason
        # Resolved like the skeleton stage resolves it: a named division no
        # QID names is identified by its Overture id.
        if qid is None and overture.qidless_place(record):
            method = "overture_id"
        record["resolution_method"] = method


def _index(records, by_country=True):
    """``{(country, folded label): [record, ...]}`` over every name variant;
    the country is None unless ``by_country``."""
    index = {}
    for record in records:
        country = record["country"] if by_country else None
        for name in _name_variants(record):
            index.setdefault((country, name), []).append(record)
    return index


def _principal_qid(candidates):
    """The one QID of a city that is its own district, while the other
    same-name divisions are hamlets — or ``None`` when no QID is, or more than
    one. Such a city appears at both levels under its QID (Augsburg, Karlsruhe,
    Ulm), or its district is its council area (``council_area``: Manchester,
    Cardiff, Bergen)."""
    levels = {}
    for candidate in candidates:
        if candidate["qid"]:
            city_level = candidate["subtype"] in CITY_SUBTYPES
            levels.setdefault(candidate["qid"], set()).add(city_level)
    shared = {qid for qid, seen in levels.items() if seen == {True, False}}
    by_id = {c["overture_id"]: c for c in candidates}
    for candidate in candidates:
        if candidate["qid"] and candidate["subtype"] in CITY_SUBTYPES:
            ancestors = candidate.get("ancestors", [])
            areas = (by_id.get(a.get("overture_id")) for a in ancestors)
            if any(area and council_area(candidate, area) for area in areas):
                shared.add(candidate["qid"])
    return shared.pop() if len(shared) == 1 else None


def _unique_identity(candidates):
    """The single division the candidates agree on, or ``(None, why)``.

    Candidates that share one QID are the same place (a locality and its
    localadmin, say); the locality is preferred. Two distinct QIDs conflict, and
    a QID-less same-name division leaves the identity unprovable — either way the
    match is reported rather than minted — unless one QID is a city that is
    its own district (``_principal_qid``): that is the principal place, and the
    other same-name divisions are set aside.
    """
    qids = {c["qid"] for c in candidates if c["qid"]}
    if len(qids) > 1 or (qids and any(not c["qid"] for c in candidates)):
        principal = _principal_qid(candidates)
        if principal is not None:
            candidates = [c for c in candidates if c["qid"] == principal]
            qids = {principal}
    if len(qids) > 1:
        return None, "the name matches divisions with conflicting QIDs"
    if not qids and len({c["overture_id"] for c in candidates}) > 1:
        return None, "the name matches several divisions without a QID"
    if not qids and not all(overture.qidless_place(c) for c in candidates):
        return None, "the matched division's identity signals conflict"
    if qids and any(not c["qid"] for c in candidates):
        return None, "the name also matches a division without a QID"
    qid = qids.pop() if qids else None
    best = min(
        (c for c in candidates if c["qid"] == qid),
        key=lambda c: (
            CITY_SUBTYPES.index(c["subtype"])
            if c["subtype"] in CITY_SUBTYPES
            else len(CITY_SUBTYPES)
        ),
    )
    return best, None


def _lookup(index, country, name):
    """The distinct records indexed under ``(country, folded name)``."""
    seen = {}
    for record in index.get((country, _norm(name)), []):
        seen.setdefault(record["overture_id"], record)
    return list(seen.values())


def match(
    index, country, subdivision, municipality, skeleton, what="locality", districts=None
):
    """The single division for a declared location — QID-bearing, or a named
    division no QID names — or ``(None, why)``; ``what`` names the level the
    index holds, for the reason when nothing carries the name.

    With ``districts`` (the skeleton's region/county index) the same-name
    districts and regions join the candidates: a QID a city shares with its own
    district marks the principal place (see ``_unique_identity``), and a
    municipality field naming a region beside a same-name hamlet is reported
    rather than placed at the hamlet.

    A declared subdivision must corroborate the match: it is required to name
    one of the candidate's region/county ancestors — or the candidate itself,
    for a region — so a lone same-name city in a different subdivision is
    reported rather than accepted.
    """
    candidates = _lookup(index, country, municipality)
    if not candidates:
        return None, f"no {what} of that name in the declared country"
    if districts is not None:
        candidates += _lookup(districts, country, municipality)
    if subdivision:
        folded = _norm(subdivision)
        narrowed = [
            c
            for c in candidates
            if folded in _subdivision_names(c, skeleton)
            or (c["subtype"] == "region" and folded in _name_variants(c))
        ]
        if not narrowed:
            return None, "the declared subdivision matches no same-name division"
        candidates = narrowed
    return _unique_identity(candidates)


def place_key(record):
    """The key a resolved division has inside the stages: its QID, or
    ``overture:<id>`` for a division no QID names — the concordance the
    registry identifies it by."""
    return record["qid"] or f"overture:{record['overture_id']}"


def _place(record, *, parent_id):
    """A places_seed row from a resolved division (geometry added later)."""
    relations = record.get("osm_relation_ids") or []
    return {
        "place_id": place_key(record),
        "kind": record["kind"],
        "source_subtype": record["source_subtype"],
        "name": record["name"],
        "names": record["names"],
        "resolution_method": record["resolution_method"],
        "parent_id": parent_id,
        "country_code": record["country"],
        "overture_id": record["overture_id"],
        "osm_relation_id": relations[0] if relations else None,
        "curated": bool(record.get("curated")),
        "metro_ids": [],
        "member_ids": [],
    }


def _ancestor_places(division, skeleton):
    """The division's admin ancestors as places, and its parent key.

    Ancestors come from the division's Overture hierarchy; each is emitted as a
    place under the key the skeleton stage gives it — its QID, or its Overture
    id for a named division no QID names. An ancestor
    the skeleton could not resolve is skipped, and ``parent_id`` links only
    resolved rungs, so the chain never points at an id that was never minted.
    A rung that is the division itself at another level — a city that is its
    own district lists its county twin, under the same QID, as an ancestor —
    or repeats the rung before it is one place, not a parent: it is skipped, so
    no place is ever its own parent.
    """
    places = []
    parent_id = None
    own = place_key(division)
    for ancestor in division.get("ancestors", []):
        resolved = skeleton.get(ancestor.get("overture_id"))
        if resolved is None:
            continue
        key = place_key(resolved)
        if key in (own, parent_id):
            continue
        places.append(_place(resolved, parent_id=parent_id))
        parent_id = key
    return places, parent_id


def _subtype_rank(place):
    """Precedence for two places of one QID: a locality outranks a localadmin."""
    subtype = place["source_subtype"]
    return (
        CITY_SUBTYPES.index(subtype) if subtype in CITY_SUBTYPES else len(CITY_SUBTYPES)
    )


def _merge_place(places, place):
    """Keep, per QID, the higher-precedence record regardless of feed order.
    A curated QID that already names a place of another kind is a collision,
    never a replacement."""
    existing = places.get(place["place_id"])
    if (
        existing is not None
        and (place.get("curated") or existing.get("curated"))
        and existing.get("kind") != place.get("kind")
    ):
        raise overture.GazetteerError(
            f"{place['place_id']!r} is both the {existing['kind']} "
            f"{existing.get('name')!r} and the {place['kind']} {place.get('name')!r}"
        )
    if existing is None or _subtype_rank(place) < _subtype_rank(existing):
        places[place["place_id"]] = place


def _add_twins(places, skeleton):
    """Record on each place the other skeleton divisions its QID names
    (``twin_overture_ids``) — a city that is also its own county or region —
    whose area it ships when its own division has none."""
    by_qid = {}
    for record in skeleton.values():
        if record.get("qid"):
            by_qid.setdefault(record["qid"], set()).add(record["overture_id"])
    for key, place in places.items():
        twins = by_qid.get(key, set()) - {place.get("overture_id")}
        if twins:
            place["twin_overture_ids"] = sorted(twins)


def _add_place(places, skeleton, division):
    """Add a resolved division and its ancestors to ``places`` by QID."""
    ancestors, parent_id = _ancestor_places(division, skeleton)
    for place in ancestors:
        _merge_place(places, place)
    _merge_place(places, _place(division, parent_id=parent_id))


def _resolve_place_overrides(candidates, entries, report):
    """A curator's QID for an Overture candidate the skeleton could not
    resolve (keyed by its Overture id): assigned as ``curated``. Judged
    against the candidate as it stands. ``(applied, unmatched)``: a missing
    candidate is another build's when its place may be (see
    ``overrides.elsewhere``) — the entries left unmatched — else refused."""
    by_ref = {e["source_ref"]: e for e in entries}
    applied = 0
    consumed = set()
    for record in candidates:
        entry = by_ref.get(record.get("overture_id"))
        if entry is None:
            continue
        consumed.add(entry["source_ref"])
        overrides.judge(
            entry,
            {
                key: record.get(key)
                for key in (
                    "overture_id",
                    "qid",
                    "resolution_method",
                    "kind",
                    "source_subtype",
                    "name",
                    "names",
                    "country",
                )
            },
            report,
            "seed",
        )
        record["qid"] = entry["place"]
        record["resolution_method"] = "curated"
        record["curated"] = True
        applied += 1
    unmatched = [by_ref[ref] for ref in sorted(set(by_ref) - consumed)]
    for entry in unmatched:
        if not overrides.elsewhere(entry["place"]):
            _no_candidate(entry)
    return applied, unmatched


def _no_candidate(entry):
    raise overrides.OverrideError(
        f"resolve_place: no candidate with Overture id {entry['source_ref']!r}"
    )


def _held_add_places(entries, places):
    """The add_place entries this build holds: its place, or a parent, or one
    member, among its places or the places of other entries it holds. An
    entry naming neither a parent nor members, or naming a place no other
    build may hold (see ``overrides.elsewhere``), is held by every build."""
    known = set(places)
    held = set()
    grew = True
    while grew:
        grew = False
        for entry in entries:
            spec = entry["add_place"]
            if entry["place"] in held:
                continue
            parent, members = spec.get("parent_id"), spec.get("member_ids") or []
            refs = [entry["place"], *([parent] if parent else []), *members]
            if entry["place"] in places or not all(map(overrides.elsewhere, refs)):
                inside = True
            elif parent is None and not members:
                inside = True
            else:
                inside = parent in known or any(m in known for m in members)
            if inside:
                held.add(entry["place"])
                known.add(entry["place"])
                grew = True
    return [entry for entry in entries if entry["place"] in held]


def _add_place_overrides(places, entries, report, statistical=frozenset()):
    """Curated places upserted into the seed: a place reference — a QID, or
    a concordance the place is minted from — a kind, a name, and either a
    boundary (attached by the geometry stage) or a member list (a metro's
    cities, linked reciprocally). ``curated`` exempts them from pruning.
    Judged against the row that exists, if any."""
    for entry in entries:
        spec = entry["add_place"]
        place_id = entry["place"]
        overrides.judge(entry, places.get(place_id), report, "seed")
        existing = places.get(place_id)
        # A metro whose statistical area the same file names gets its
        # members from that derivation, in the metros stage.
        if existing is None and not (
            "boundary" in spec or "member_ids" in spec or place_id in statistical
        ):
            raise overrides.OverrideError(
                f"place {place_id!r}: add_place needs a boundary or member_ids"
            )
        if existing is not None:
            # The kind shapes every derived field; the boundary and the
            # member list have their own operations.
            if existing.get("kind") != spec["kind"]:
                raise overrides.OverrideError(
                    f"place {place_id!r}: add_place cannot change a seeded place's "
                    "kind"
                )
            for field, operation in (
                ("boundary", "set_boundary"),
                ("member_ids", "set_place_members"),
            ):
                if field in spec:
                    raise overrides.OverrideError(
                        f"place {place_id!r}: add_place cannot replace a seeded "
                        f"place's {field}; use {operation}"
                    )
        row = existing or {
            "place_id": place_id,
            "kind": spec["kind"],
            "source_subtype": None,
            "names": {},
            "resolution_method": "curated",
            "country_code": None,
            "overture_id": None,
            "osm_relation_id": None,
            "metro_ids": [],
            "member_ids": [],
        }
        row.update(
            {
                "kind": spec["kind"],
                "name": spec["name"],
                "parent_id": spec.get("parent_id"),
                "resolution_method": "curated",
                "curated": True,
            }
        )
        row["names"] = {**(row.get("names") or {}), "en": spec["name"]}
        if "country_code" in spec:
            row["country_code"] = spec["country_code"]
        if "boundary" in spec:
            row["boundary_wkt"] = spec["boundary"]
        if "member_ids" in spec:
            row["member_ids"] = sorted(set(spec["member_ids"]))
            row["members_curated"] = True
        places[place_id] = row
    for entry in entries:
        spec = entry["add_place"]
        parent = spec.get("parent_id")
        if parent and parent not in places:
            raise overrides.OverrideError(
                f"place {entry['place']!r}: parent {parent!r} is not a seeded place"
            )
        if parent and places[parent].get("kind") == "metro":
            raise overrides.OverrideError(
                f"place {entry['place']!r}: parent {parent!r} is a metro, not an "
                "administrative place"
            )
        for member in spec.get("member_ids", []):
            city = places.get(member)
            if city is None or city.get("kind") != "city":
                raise overrides.OverrideError(
                    f"place {entry['place']!r}: member {member!r} is not a seeded city"
                )
            if entry["place"] not in city.setdefault("metro_ids", []):
                city["metro_ids"].append(entry["place"])
                city["metro_ids"].sort()
    for entry in entries:
        seen = set()
        place_id = entry["place"]
        while place_id is not None:
            if place_id in seen:
                raise overrides.OverrideError(
                    f"place {entry['place']!r}: its parent chain loops"
                )
            seen.add(place_id)
            place_id = (places.get(place_id) or {}).get("parent_id")


def centre_seeds(centre):
    """``[(name, folded names, primary)]``: a UCDB centre's seed rows. The
    main name, less a bracket, is matched by each part of a "/" pair, each
    also without a ``City`` affix and in Overture's spelling; a capital's
    bracket names its capital, a second row."""
    found = _BRACKET.match(centre["name"])
    main, bracket = found.groups() if found else (centre["name"], None)
    rows = [(main, main.split("/"), True)]
    if bracket and centre["capital"]:
        rows.append((bracket, [bracket], False))
    seeds = []
    for name, parts, primary in rows:
        names = set()
        for part in map(str.strip, parts):
            for spelling in (part, UCDB_SPELLINGS.get(part)):
                if spelling:
                    names |= _area_names({"name": spelling})
        seeds.append((name, names, primary))
    return seeds


def _choose(candidates, polygon):
    """The candidate a centre's ``polygon`` holds the label point of — or,
    when none, the one within ``NEAR_DEG`` — by precedence: a locality over a
    localadmin, a QID over none, the larger population, the point nearest
    the polygon's own, the Overture id. None when no point is near."""
    located = [(c, shapely.from_wkb(c["point"])) for c in candidates if c["point"]]
    held = [item for item in located if shapely.covers(polygon, item[1])] or [
        item for item in located if shapely.dwithin(item[1], polygon, NEAR_DEG)
    ]
    if not held:
        return None
    centre = shapely.point_on_surface(polygon)
    best, _ = min(
        held,
        key=lambda item: (
            CITY_SUBTYPES.index(item[0]["subtype"]),
            not item[0]["wikidata"],
            -(item[0]["population"] or 0),
            shapely.distance(item[1], centre),
            item[0]["overture_id"],
        ),
    )
    return best


def centre_rows(centres, dataset):
    """One row per seed row of the UCDB ``centres`` (polygons in WGS84): the
    centre's ``ucdb_id``, the row's ``name``, ``primary``, ``population``,
    ``capital``, ``gadm_country``, ``boundary`` (the polygon as WKB hex, a
    main name's only) and ``division``, the same-name Overture locality or
    localadmin ``_choose`` takes, or None. One scan of the theme."""
    seeds = [(centre, *seed) for centre in centres for seed in centre_seeds(centre)]
    wanted = {(None, name) for _, _, names, _ in seeds for name in names}
    index = _index(read_city_candidates(dataset, None, wanted), by_country=False)
    rows = []
    for centre, name, names, primary in progress(seeds, "seed centres"):
        found = {c["overture_id"]: c for n in names for c in index.get((None, n), [])}
        division = _choose(found.values(), centre["geom"])
        if division is not None:
            division = {k: v for k, v in division.items() if k != "point"}
        rows.append(
            {
                "ucdb_id": centre["id"],
                "name": name,
                "primary": primary,
                "population": centre["population"],
                "capital": centre["capital"],
                "gadm_country": centre["country"],
                "boundary": (
                    shapely.to_wkb(centre["geom"], hex=True) if primary else None
                ),
                "division": division,
            }
        )
    return rows


def centre_country(row):
    """A centre row's country: its division's, else its GADM name's."""
    if row["division"]:
        return row["division"]["country"]
    return country_codes.by_ucdb(row["gadm_country"])


def prepare_centres(cache_dir, dataset, *, expected=None):
    """``(rows, manifest)``: the ``centre_rows`` of the UCDB centres that
    qualify, derived once into ``raw/ucdb-centres.json`` and reused while the
    UCDB pins, the Overture release and the rules stay the same."""
    from transitio_index import ucdb  # ucdb -> fao -> geometry -> seed

    expected = dict(expected or ucdb.PINS)
    inputs = ucdb.prepare_inputs(cache_dir, expected=expected)
    sources = {
        **inputs["digests"],
        "overture": overture.OVERTURE_RELEASE,
        "rules": CENTRES_VERSION,
        "min_population": ucdb.MIN_POPULATION,
        "near_deg": NEAR_DEG,
    }

    def build():
        generation, _ = pinned.resolve(
            cache_dir, pointer=ucdb.POINTER, expected=expected, error=ucdb.UcdbError
        )
        with generation:
            data = generation.read_bytes(ucdb.UCDB_FILE)
        centres, _ = ucdb.read_ucdb(data, "EPSG:4326")
        rows = centre_rows([c for c in centres if ucdb.qualifies(c)], dataset)
        return {CENTRES_FILE: store.jsonl_chunks(rows)}, {
            "source": "ghs-ucdb-centres",
            "release": ucdb.RELEASE,
            "doi": ucdb.DOI,
            "license": ucdb.LICENCE,
            "rows": len(rows),
            "matched": sum(row["division"] is not None for row in rows),
            # Without a country no build seeds them.
            "unplaced": [
                {"ucdb_id": row["ucdb_id"], "gadm_country": row["gadm_country"]}
                for row in rows
                if centre_country(row) is None
            ],
        }

    pinned.derive(cache_dir, pointer=CENTRES_POINTER, sources=sources, build=build)
    rows, manifest = store.read_jsonl(cache_dir / "raw", CENTRES_POINTER, CENTRES_FILE)
    if manifest.get("sources") != sources:
        raise ucdb.UcdbError(
            f"raw/{CENTRES_POINTER} derives from other inputs than this build's"
        )
    return rows, manifest


def _mark(place, row):
    """Mark a place a centre row lands on: of all its rows, the one with a
    polygon, then the lowest id, gives ``ucdb_id`` and the polygon
    (``ucdb_boundary``); the largest ``population`` is kept."""
    rank = (not row.get("boundary"), row["ucdb_id"])
    if "ucdb_id" not in place or rank < (
        "ucdb_boundary" not in place,
        place["ucdb_id"],
    ):
        place["ucdb_id"] = row["ucdb_id"]
        if row.get("boundary"):
            place["ucdb_boundary"] = row["boundary"]
    if (row.get("population") or 0) > (place.get("population") or 0):
        place["population"] = row["population"]


def _seed_centres(places, skeleton, countries, rows, report):
    """Seed the centre ``rows`` as places, main names first, then by id: a
    row on a division adds it with its ancestors as a feed's city is, so a
    place of its key stays unless the division outranks it; a main name on
    none is a city of its own, ``ghs_ucdb:<id>``, under its country
    (``countries``, by code); a bracket on none is reported.
    ``(counts, marks)``: the ``(key, row)`` pairs to ``_mark``."""
    counts = dict.fromkeys(("matched", "existing", "urban", "reported"), 0)
    marks = []
    for row in sorted(rows, key=lambda row: (not row["primary"], row["ucdb_id"])):
        division = row["division"]
        if division and (division["qid"] or overture.qidless_place(division)):
            key = place_key(division)
            counts["matched"] += 1
            counts["existing"] += key in places
            _add_place(places, skeleton, division)
        elif row["primary"]:
            key = f"ghs_ucdb:{row['ucdb_id']}"
            counts["urban"] += 1
            if key not in places:
                country = countries[centre_country(row)]
                _add_place(places, skeleton, country)
                record = {
                    "qid": None,
                    "overture_id": None,
                    "kind": "city",
                    "source_subtype": "urban centre",
                    "name": row["name"],
                    "names": {},
                    "resolution_method": "ghs_ucdb",
                    "country": country["country"],
                }
                place = _place(record, parent_id=place_key(country))
                places[key] = {**place, "place_id": key}
        else:
            counts["reported"] += 1
            report.append(
                {
                    "ucdb_id": row["ucdb_id"],
                    "name": row["name"],
                    "reason": "no same-name locality in the capital's urban centre",
                }
            )
            continue
        marks.append((key, row))
    return counts, marks


def _key_concordance(key, registry):
    """The concordances a stage's key states: a QID; ``namespace:value``
    for a curated place minted from another concordance; or, for an own id
    a QID-less place resolves to on a rebuild, everything its row carries."""
    if overture.QID_PATTERN.match(key):
        return {"wikidata": [key]}
    if _registry.ID_PATTERN.match(key):
        return {ns: list(values) for ns, values in registry.effective(key).items()}
    namespace, value = key.split(":", 1)
    return {namespace: [value]}


def _identify_places(places, registry, places_digest, report=None):
    """Give every place its registry id — found by its concordances or
    minted — and the QID the registry keys it by; ``rekey_by_own_id`` then
    makes the id the key. A place whose concordances conflict with an
    existing registry place — a name shared across administrative levels,
    e.g. a city that shares its QID with a like-named region — cannot be
    identified; it is logged, appended to ``report`` as a ``conflict`` row
    when one is given, and dropped from ``places`` so one collision no longer
    aborts the gazetteer. Returns the number identified."""
    if registry is None:
        return 0
    from transitio_index import ucdb  # ucdb -> fao -> geometry -> seed

    skipped = []
    for place_id in sorted(places):
        row = places[place_id]
        concordances = _key_concordance(place_id, registry)
        if row.get("overture_id"):
            concordances["overture"] = [row["overture_id"]]
            minted_from = f"overture:{row['overture_id']}"
            minted_in = f"overture {overture.OVERTURE_RELEASE}"
        elif row.get("resolution_method") == "ghs_ucdb":
            minted_from = place_id
            minted_in = f"GHS-UCDB {ucdb.RELEASE}"
        else:
            minted_from = f"places.yaml:{place_id}"
            minted_in = f"places.yaml {places_digest}"
        if row.get("osm_relation_id"):
            concordances["osm_relation"] = [str(row["osm_relation_id"])]
        try:
            row["tp_id"] = registry.identify(
                concordances,
                kind=row["kind"],
                name=row.get("name"),
                country_code=row.get("country_code"),
                minted_from=minted_from,
                minted_in=minted_in,
            )
        except _registry.RegistryError as exc:
            # A read-only session refuses every change loudly; only a writable
            # build treats an identity conflict as a per-item miss.
            if registry.read_only:
                raise
            log.warning("seed: %s not identified (%s); skipped", place_id, exc)
            skipped.append(place_id)
            if report is not None:
                report.append(
                    {
                        "kind": "conflict",
                        "place_id": place_id,
                        "overture_id": row.get("overture_id"),
                        "name": row.get("name"),
                        "reason": str(exc),
                    }
                )
            continue
        row["wikidata_id"] = registry.canonical_qid(row["tp_id"])
        if row["kind"] == "metro" and not row.get("statistical_area_id"):
            _restore_statistical_identity(row, registry.effective(row["tp_id"]))
    if skipped:
        _drop_places(places, skipped)
    return len(places)


def _drop_places(places, keys):
    """Remove ``keys`` from ``places`` and mend the links that named them: a
    child of a dropped place moves up to its nearest surviving ancestor, a
    dangling default metro is cleared, and dropped members leave every metro."""
    gone = {key: places.pop(key) for key in keys}
    for row in places.values():
        parent = row.get("parent_id")
        seen = set()
        while parent in gone and parent not in seen:
            seen.add(parent)
            parent = gone[parent].get("parent_id")
        if parent in gone:
            parent = None  # the dropped rows loop: no surviving ancestor
        if parent != row.get("parent_id"):
            row["parent_id"] = parent
        if row.get("default_metro_id") in gone:
            row["default_metro_id"] = None
        for field in ("metro_ids", "member_ids"):
            if row.get(field):
                row[field] = [v for v in row[field] if v not in gone]


STATISTICAL_SUBTYPES = {
    "cbsa": "metropolitan statistical area",
    "eurostat_metro": "metropolitan region",
    "eurostat_fua": "functional urban area",
}


def _restore_statistical_identity(row, concordances):
    """A metro keyed by its statistical code carries that code and its
    scheme's subtype on every build, so the metro a stage discovers under
    the code joins it."""
    for namespace, subtype in STATISTICAL_SUBTYPES.items():
        if concordances.get(namespace):
            row["statistical_area_id"] = concordances[namespace][0]
            row["source_subtype"] = row.get("source_subtype") or subtype
            return


def rekey(rows, *, by, records=(), canonical=None):
    """``rows`` keyed by the field ``by`` — ``tp_id`` for the own id every
    identified row carries, ``wikidata_id`` for the QID a stage joins on —
    with the links between them (``parent_id``, ``default_metro_id``,
    ``metro_ids``, ``member_ids``) and the place ids in ``records`` mapped
    alike. A row without the field keeps its key. Two rows the registry
    calls one place become one: the row keyed by the place's canonical QID
    (``canonical``, key by target) survives, else the first."""
    mapping = {key: row.get(by) or key for key, row in rows.items()}
    canonical = canonical or {}

    def order(key):
        # The key is the QID the row was seeded by; identification has
        # already given every converging row the survivor's canonical QID.
        return (mapping[key], key != canonical.get(mapping[key]), key)

    out = {}
    for key in sorted(rows, key=order):
        row = rows[key]
        target = mapping[key]
        if target in out:
            # The same place under another key: its links fold into the
            # survivor, the rest of the row is the survivor's.
            survivor = out[target]
            for field in ("metro_ids", "member_ids"):
                survivor[field] = sorted(
                    set(survivor.get(field) or []) | set(row.get(field) or [])
                )
            if "ucdb_id" in row:
                _mark(survivor, {**row, "boundary": row.get("ucdb_boundary")})
            continue
        row["place_id"] = target
        for field in ("parent_id", "default_metro_id"):
            if row.get(field):
                row[field] = mapping.get(row[field], row[field])
        for field in ("metro_ids", "member_ids"):
            if field in row:
                row[field] = sorted({mapping.get(v, v) for v in row[field] or []})
        out[target] = row
    for row in out.values():
        for field in ("metro_ids", "member_ids"):
            if field in row:
                row[field] = sorted({mapping.get(v, v) for v in row[field]})
    for record in records:
        for field in ("place_id", "city_id", "metro_id"):
            if record.get(field) in mapping:
                record[field] = mapping[record[field]]
    return out


def rekey_by_own_id(rows, registry, records=()):
    """The rows of a stage keyed by their own ids, the way it publishes them."""
    canonical = {
        row["tp_id"]: registry.canonical_qid(row["tp_id"])
        for row in rows.values()
        if row.get("tp_id")
    }
    return rekey(rows, by="tp_id", records=records, canonical=canonical)


def rekey_by_qid(rows):
    """Published rows keyed by their QIDs again, for a stage that joins on
    them; a row without a QID keeps its own id."""
    return rekey(rows, by="wikidata_id")


def resolve_seed(
    cache_dir,
    *,
    dataset=None,
    wikidata=None,
    overrides_dir=None,
    strict=False,
    registry=None,
    run=None,
    seed_countries=(),
    ucdb_pins=None,
):
    """Build ``places_seed.jsonl`` from the feeds' declared locations.

    Reads the crosswalk feeds and the skeleton stage's resolved divisions,
    matches each feed's declared municipality to a QID-bearing Overture city — or,
    when no locality carries the name, to a skeleton region or county, placed at
    level ``district`` — (its subdivision to a region, or its country to a
    country, when no finer level is declared), and emits that place with its
    administrative ancestors; unmatched feeds go to ``seed_report.jsonl``. The
    capitals and the cities of at least ``ucdb.MIN_POPULATION`` people in
    ``seed_countries`` (codes the skeleton has a country for) are seeded too,
    from the GHS-UCDB centres ``prepare_centres`` matches (``ucdb_pins`` the
    UCDB inputs', the module's by default; see ``_seed_centres``). With a
    ``registry`` session every place also gets its registry id (``tp_id``)
    and ``wikidata_id``. Returns the generation manifest.
    """
    if wikidata is None:
        wikidata = overture.WikidataClient()

    feeds, crosswalk_manifest = store.read_jsonl(
        cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
    )
    resolved_divisions, _ = store.read_jsonl(
        cache_dir / "gazetteer",
        "overture.json",
        "overture_divisions.jsonl",
        generations=run,
    )
    skeleton = {record["overture_id"]: record for record in resolved_divisions}
    region_index = _index([r for r in skeleton.values() if r["kind"] == "region"])
    country_index = {
        r["country"]: r for r in skeleton.values() if r["kind"] == "country"
    }
    seed_countries = set(seed_countries)
    unknown = sorted(seed_countries - set(country_index))
    if unknown:
        raise overture.GazetteerError(
            f"no Overture country or dependency for seed countries {unknown}"
        )

    locations = list(declared_locations(feeds))
    city_locations = [loc for loc in locations if loc["municipality"]]
    countries = {loc["country"] for loc in city_locations}
    wanted = {(loc["country"], _norm(loc["municipality"])) for loc in city_locations}

    if dataset is None:
        dataset = overture.overture_dataset()
    candidates = read_city_candidates(dataset, countries, wanted)
    centres, centres_manifest = [], {}
    if seed_countries:
        rows, centres_manifest = prepare_centres(cache_dir, dataset, expected=ucdb_pins)
        centres = [row for row in rows if centre_country(row) in seed_countries]
    # One record per division, so an override applies to it once.
    divisions = {c["overture_id"]: c for c in candidates}
    for row in centres:
        if row["division"]:
            key = row["division"]["overture_id"]
            row["division"] = divisions.setdefault(key, row["division"])
    _resolve_candidates(list(divisions.values()), wikidata)
    place_overrides, places_digest = overrides.load_place_overrides(
        overrides_dir, registry=registry, internal=True
    )
    override_report = []
    resolved_by_hand, unmatched = _resolve_place_overrides(
        list(divisions.values()),
        overrides.by_operation(place_overrides, "resolve_place"),
        override_report,
    )
    city_index = _index(candidates)

    places = {}
    report = []
    placements = []
    for location in progress(locations, "seed"):
        if location["municipality"]:
            level = "municipality"
            division, reason = match(
                city_index,
                location["country"],
                location["subdivision"],
                location["municipality"],
                skeleton,
                districts=region_index,
            )
            if division is None and not _lookup(
                city_index, location["country"], location["municipality"]
            ):
                # A municipality no locality is named by may name a district
                # or region the skeleton resolved — "Augsburg (district)",
                # "Kreis Soest" — and places the feed there.
                level = "district"
                division, reason = match(
                    region_index,
                    location["country"],
                    location["subdivision"],
                    location["municipality"],
                    skeleton,
                    what="locality or district",
                )
        elif location["subdivision"]:
            level = "subdivision"
            regions = _lookup(
                region_index, location["country"], location["subdivision"]
            )
            if regions:
                division, reason = _unique_identity(regions)
            else:
                division, reason = (
                    None,
                    "no region of that name in the declared country",
                )
        else:
            level = "country"
            division = country_index.get(location["country"])
            reason = None if division else "no country division for the declared code"
        if division is None:
            report.append(
                {
                    "feed_id": location["feed_id"],
                    "country": location["country"],
                    "subdivision": location["subdivision"],
                    "municipality": location["municipality"],
                    "reason": reason,
                }
            )
            continue
        # The feed -> place link is persisted alongside the places: declared
        # coverage derives its membership edges from it without re-resolving.
        placements.append(
            {
                "feed_id": location["feed_id"],
                "place_id": place_key(division),
                "level": level,
            }
        )
        _add_place(places, skeleton, division)
    fed = set(places)
    counts, marks = _seed_centres(places, skeleton, country_index, centres, report)
    added = _held_add_places(
        overrides.by_operation(place_overrides, "add_place"), places
    )
    statistical = {
        e["place"]
        for e in overrides.by_operation(place_overrides, "set_statistical_area")
    }
    # An entry defining a place only a centre seeded replaces the centre's row.
    for entry in added:
        spec, key = entry["add_place"], entry["place"]
        if key not in fed and (
            "boundary" in spec or "member_ids" in spec or key in statistical
        ):
            places.pop(key, None)
    _add_place_overrides(places, added, override_report, statistical)
    for key, row in marks:
        _mark(places[key], row)
    for entry in unmatched:
        # A place this build holds names a candidate that is gone.
        if entry["place"] in places:
            _no_candidate(entry)
    _add_twins(places, skeleton)
    before = set(places)
    identified = _identify_places(places, registry, places_digest)
    conflicts = before - set(places)
    if registry is not None:
        placements = [p for p in placements if p["place_id"] in places]
        places = rekey_by_own_id(places, registry, records=placements)

    manifest = {
        "source": "seed",
        "identified": identified,
        "seed_conflicts": len(conflicts),
        # The digest loaded, never the one to be saved: the run saves the
        # registry only after its last stage.
        "registry_base": registry.base if registry is not None else None,
        # The exact places.yaml applied: every later gazetteer stage must read
        # the same bytes, and publish checks the file against it.
        "places_overrides_sha256": places_digest,
        "overrides_applied": resolved_by_hand + len(added),
        "stale_overrides": len(override_report),
        "stale_place_overrides": len(override_report),
        "overture_release": overture.OVERTURE_RELEASE,
        # The catalogue versions the placements were derived from, carried
        # forward so coverage can refuse a mixed-lineage input set.
        "sources": crosswalk_manifest.get("sources"),
        # The feed set placed, curated feeds included: coverage pairs these
        # placements only with feeds resolved from the same generation.
        "crosswalk_generation": crosswalk_manifest.get("generation"),
        "feeds_with_location": len(locations),
        "feeds_placed": len(placements),
        "places": len(places),
        "reported": len(report),
        "seed_countries": sorted(seed_countries),
        "centre_sources": centres_manifest.get("sources"),
        "centre_rows": len(centres),
        **{f"centre_rows_{outcome}": count for outcome, count in counts.items()},
        "centre_rows_unplaced": (
            len(centres_manifest["unplaced"]) if centres_manifest else None
        ),
        "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            published = store.publish(
                cache_dir / "gazetteer",
                "seed.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(list(places.values())),
                    "feed_places.jsonl": store.jsonl_chunks(placements),
                    "seed_report.jsonl": store.jsonl_chunks(report),
                    "override_report.jsonl": store.jsonl_chunks(override_report),
                },
                manifest,
                held=directory,
                staged=run is not None,
            )
            if run is not None:
                run["seed.json"] = published["generation"]
            overrides.strict_check(strict, override_report, "seed")
            return published
    finally:
        directory.close()
