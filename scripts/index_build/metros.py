"""Attach US metropolitan-area membership to the seeded places.

For each seeded US city, Wikidata links it to the metropolitan statistical area
it belongs to (P8138, class ``US_MSA_CLASS``), keyed by the metro's CBSA code
(P882). Each such metro is emitted as a ``metro`` place carrying its member city
QIDs, and every member city carries the metro in ``metro_ids`` — a city can
belong to more than one. Metro geometry, EU metros and the Wikidata
member-union path are later work; this stage adds membership only, from
Wikidata, with no geometry.
"""

import datetime

from index_build import overrides, overture, store


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


def attach_metros(cache_dir, *, wikidata=None, overrides_dir=None, strict=False):
    """Add US metro places and memberships to the seed places.

    Resolves each seeded US city's metropolitan area from Wikidata and
    republishes the places with metro rows appended and ``metro_ids`` /
    ``member_ids`` filled. Only CBSA-keyed metros are published; an MSA missing
    its code is reported (``metro_report.jsonl``) for a later pass. One writer
    lock spans the seed read, the live query and the publish, so a concurrent
    gazetteer run cannot shift the seed under it. Returns the generation manifest.
    """
    if wikidata is None:
        wikidata = overture.WikidataClient()

    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, seed_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "seed.json", "places_seed.jsonl"
            )
            by_id = {}
            for place in places:
                # Every row carries the metro and statistical-code fields, so a
                # metro row and a city row share one schema for the writer.
                place.setdefault("metro_ids", [])
                place.setdefault("statistical_area_id", None)
                by_id[place["place_id"]] = place

            us_cities = [
                p["place_id"]
                for p in places
                if p["kind"] == "city" and p["country_code"] == "US"
            ]
            membership = wikidata.statistical_metros(us_cities) if us_cities else {}

            metros = {}
            report = []
            for city_qid, found in membership.items():
                city = by_id.get(city_qid)
                if city is None:
                    continue
                for record in found:
                    if not record["cbsa"]:
                        report.append(
                            {
                                "city_id": city_qid,
                                "metro_id": record["qid"],
                                "name": record["name"],
                                "reason": "US MSA without a CBSA code",
                            }
                        )
                        continue
                    # A curated metro of the same QID (add_place) is the row;
                    # the statistical membership joins it, never a second row.
                    existing = by_id.get(record["qid"])
                    if existing is not None and existing.get("kind") != "metro":
                        raise overture.GazetteerError(
                            f"metro {record['qid']!r} is already seeded as the "
                            f"{existing['kind']} {existing.get('name')!r}"
                        )
                    metro = metros.setdefault(
                        record["qid"], existing or _metro_place(record)
                    )
                    if metro.get("members_curated"):
                        continue
                    if city_qid not in metro["member_ids"]:
                        metro["member_ids"].append(city_qid)
                    if metro["place_id"] not in city["metro_ids"]:
                        city["metro_ids"].append(metro["place_id"])

            for metro in metros.values():
                metro["member_ids"].sort()
            output = places + [
                metros[qid] for qid in sorted(metros) if qid not in by_id
            ]
            by_id.update({m["place_id"]: m for m in metros.values()})
            place_overrides, places_digest = overrides.load_place_overrides(
                overrides_dir
            )
            overrides.expect_digest(
                seed_manifest.get("places_overrides_sha256"),
                places_digest,
                "places.yaml",
                "gazetteer",
            )
            override_report = []
            applied = _set_members(
                by_id,
                overrides.by_operation(place_overrides, "set_place_members"),
                override_report,
            )
            for place in output:
                place["metro_ids"] = sorted(place["metro_ids"])

            manifest = {
                "source": "metros",
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
                    "override_report.jsonl": store.jsonl_chunks(override_report),
                },
                manifest,
                held=directory,
            )
            overrides.strict_check(strict, override_report, "metros")
            return published
    finally:
        directory.close()
