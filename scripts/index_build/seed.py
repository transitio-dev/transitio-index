"""Declared-seed resolution: feed locations to gazetteer places.

Matches each feed's declared municipality (MDB) or ``Location`` (``systems.csv``)
to an Overture locality/localadmin by name within its country — disambiguated by
the declared subdivision — resolves that division to a QID with the same rules as
the skeleton stage, and emits the city place plus its administrative ancestors as
``places_seed.jsonl``. A feed that declares only a subdivision resolves to that
region instead. A feed whose location does not resolve to a single QID-bearing
place is reported, never minted.

Matching folds accents and case and considers every language label a division
carries, so a feed naming a place in a local language still resolves. Only feeds
with a declared place name are placed here; geometry-based placement (MDB
bounding-box centroids, Atlas geohashes) and the boundary geometry are a later
stage. The locality read is streamed and kept to the names feeds actually
declare, so the 3.5M-row locality universe is never materialised.
"""

import datetime
import unicodedata

import pyarrow.dataset as ds

from index_build import overrides, overture, store

# The Overture subtypes that stand in for a city, most specific first: a name
# resolving to both prefers the locality (decision in the plan's subtype table).
CITY_SUBTYPES = ("locality", "localadmin")


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


def declared_locations(feeds):
    """One declared location per placeable feed, finest declared level first.

    A feed is placed at the finest level its catalogues name: an MDB
    municipality or subdivision, else a GBFS ``Location``, else a bare country
    code — the plan treats each as valid declared coverage. A feed that names no
    country at all is skipped; it is placed geometrically in a later stage. The
    ``municipality`` and ``subdivision`` may both be ``None`` for a country-only
    feed.
    """
    for feed in feeds:
        location = (feed.get("mdb") or {}).get("location") or {}
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
    """Normalised locality/localadmin records feeds actually name.

    Streamed with a country + subtype predicate so only the relevant partitions
    are scanned, and kept only where one of a division's ``(country, folded
    name)`` labels is one a feed declared, so the working set is bounded by the
    feeds, not the theme.
    """
    if not countries or not wanted:
        return []
    predicate = ds.field("subtype").isin(list(CITY_SUBTYPES)) & ds.field(
        "country"
    ).isin(sorted(countries))
    kept = []
    for batch in dataset.to_batches(columns=overture.PROJECT, filter=predicate):
        for row in batch.to_pylist():
            record = overture.normalize_division(row)
            if any(
                (record["country"], name) in wanted for name in _name_variants(record)
            ):
                kept.append(record)
    return kept


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
        qid, method, _ = overture.resolve_qid(record, p402_map)
        record["qid"] = qid
        record["resolution_method"] = method


def _index(records):
    """``{(country, folded label): [record, ...]}`` over every name variant."""
    index = {}
    for record in records:
        for name in _name_variants(record):
            index.setdefault((record["country"], name), []).append(record)
    return index


def _unique_identity(candidates):
    """The single division the candidates agree on, or ``(None, why)``.

    Candidates that share one QID are the same place (a locality and its
    localadmin, say); the locality is preferred. Two distinct QIDs conflict, and
    a QID-less same-name division leaves the identity unprovable — either way the
    match is reported rather than minted.
    """
    qids = {c["qid"] for c in candidates if c["qid"]}
    if len(qids) > 1:
        return None, "the name matches divisions with conflicting QIDs"
    if not qids:
        return None, "the matched division has no QID"
    if any(not c["qid"] for c in candidates):
        return None, "the name also matches a division without a QID"
    qid = qids.pop()
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


def match(index, country, subdivision, municipality, skeleton):
    """The single QID-bearing city for a declared location, or ``(None, why)``.

    A declared subdivision must corroborate the match: it is required to name
    one of the candidate's region/county ancestors, so a lone same-name city in
    a different subdivision is reported rather than accepted.
    """
    candidates = _lookup(index, country, municipality)
    if not candidates:
        return None, "no locality of that name in the declared country"
    if subdivision:
        folded = _norm(subdivision)
        narrowed = [c for c in candidates if folded in _subdivision_names(c, skeleton)]
        if not narrowed:
            return None, "the declared subdivision matches no same-name division"
        candidates = narrowed
    return _unique_identity(candidates)


def _place(record, *, parent_id):
    """A places_seed row from a resolved division (geometry added later)."""
    relations = record.get("osm_relation_ids") or []
    return {
        "place_id": record["qid"],
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
    """The division's admin ancestors as places, and its parent QID.

    Ancestors come from the division's Overture hierarchy; each is emitted as a
    place using the QID the skeleton stage already resolved for it. An ancestor
    the skeleton could not resolve is skipped, and ``parent_id`` links only
    resolved rungs, so the chain never points at an id that was never minted.
    """
    places = []
    parent_id = None
    for ancestor in division.get("ancestors", []):
        resolved = skeleton.get(ancestor.get("overture_id"))
        if resolved is None:
            continue
        places.append(_place(resolved, parent_id=parent_id))
        parent_id = resolved["qid"]
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


def _add_place(places, skeleton, division):
    """Add a resolved division and its ancestors to ``places`` by QID."""
    ancestors, parent_id = _ancestor_places(division, skeleton)
    for place in ancestors:
        _merge_place(places, place)
    _merge_place(places, _place(division, parent_id=parent_id))


def _resolve_place_overrides(candidates, entries, report):
    """A curator's QID for an Overture candidate the skeleton could not
    resolve (keyed by its Overture id): assigned as ``curated``. Judged
    against the candidate as it stands."""
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
    missing = sorted(set(by_ref) - consumed)
    if missing:
        # A candidate that vanished from the skeleton cannot take the QID:
        # the override no longer names anything, which is a build error.
        raise overrides.OverrideError(
            f"resolve_place: no candidate with Overture id {missing[0]!r}"
        )
    return applied


def _add_place_overrides(places, entries, report):
    """Curated places upserted into the seed: a real QID, a kind, a name,
    and either a boundary (attached by the geometry stage) or a member list
    (a metro's cities, linked reciprocally). ``curated`` exempts them from
    pruning. Judged against the row that exists, if any."""
    for entry in entries:
        spec = entry["add_place"]
        place_id = entry["place"]
        overrides.judge(entry, places.get(place_id), report, "seed")
        existing = places.get(place_id)
        if existing is None and not ("boundary" in spec or "member_ids" in spec):
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


def resolve_seed(
    cache_dir, *, dataset=None, wikidata=None, overrides_dir=None, strict=False
):
    """Build ``places_seed.jsonl`` from the feeds' declared locations.

    Reads the crosswalk feeds and the skeleton stage's resolved divisions,
    matches each feed's declared municipality to a QID-bearing Overture city (its
    subdivision to a region, or its country to a country, when no finer level is
    declared), and emits that place with its administrative ancestors; unmatched
    feeds go to ``seed_report.jsonl``. Returns the generation manifest.
    """
    if wikidata is None:
        wikidata = overture.WikidataClient()

    feeds, crosswalk_manifest = store.read_jsonl(
        cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
    )
    resolved_divisions, _ = store.read_jsonl(
        cache_dir / "gazetteer", "overture.json", "overture_divisions.jsonl"
    )
    skeleton = {record["overture_id"]: record for record in resolved_divisions}
    region_index = _index([r for r in skeleton.values() if r["kind"] == "region"])
    country_index = {
        r["country"]: r for r in skeleton.values() if r["kind"] == "country"
    }

    locations = list(declared_locations(feeds))
    city_locations = [loc for loc in locations if loc["municipality"]]
    countries = {loc["country"] for loc in city_locations}
    wanted = {(loc["country"], _norm(loc["municipality"])) for loc in city_locations}

    if dataset is None:
        dataset = overture.overture_dataset()
    candidates = read_city_candidates(dataset, countries, wanted)
    _resolve_candidates(candidates, wikidata)
    place_overrides, places_digest = overrides.load_place_overrides(overrides_dir)
    override_report = []
    resolved_by_hand = _resolve_place_overrides(
        candidates,
        overrides.by_operation(place_overrides, "resolve_place"),
        override_report,
    )
    city_index = _index(candidates)

    places = {}
    report = []
    placements = []
    for location in locations:
        if location["municipality"]:
            level = "municipality"
            division, reason = match(
                city_index,
                location["country"],
                location["subdivision"],
                location["municipality"],
                skeleton,
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
                "place_id": division["qid"],
                "level": level,
            }
        )
        _add_place(places, skeleton, division)
    added = overrides.by_operation(place_overrides, "add_place")
    _add_place_overrides(places, added, override_report)

    manifest = {
        "source": "seed",
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
        "feeds_with_location": len(locations),
        "feeds_placed": len(placements),
        "places": len(places),
        "reported": len(report),
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
            )
            overrides.strict_check(strict, override_report, "seed")
            return published
    finally:
        directory.close()
