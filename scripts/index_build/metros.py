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

from index_build import overture, store


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


def attach_metros(cache_dir, *, wikidata=None):
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
                    metro = metros.setdefault(record["qid"], _metro_place(record))
                    if city_qid not in metro["member_ids"]:
                        metro["member_ids"].append(city_qid)
                    if metro["place_id"] not in city["metro_ids"]:
                        city["metro_ids"].append(metro["place_id"])

            for metro in metros.values():
                metro["member_ids"].sort()
            for place in places:
                place["metro_ids"] = sorted(place["metro_ids"])
            output = places + [metros[qid] for qid in sorted(metros)]

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
                "places": len(output),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "gazetteer",
                "metros.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(output),
                    "metro_report.jsonl": store.jsonl_chunks(report),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
