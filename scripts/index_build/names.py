"""Merge Wikidata labels and aliases into the gazetteer places.

Each place keeps its Overture ``names`` — Overture wins for a shared language —
and gains Wikidata labels for the languages Overture lacks; ``name`` is set to the
English label. ``aliases`` is the union of any existing aliases and Wikidata's
"also known as" values (both CC0), with the place's own name removed. Wikidata is
queried once per build via the ``wbgetentities`` API; curated additions apply in a
later override stage.
"""

import datetime

from index_build import overture, store


def _merge(place, entry):
    """Fold one place's Wikidata labels and aliases into its record in place."""
    names = dict(entry["labels"])
    names.update(place.get("names") or {})  # Overture wins for a shared language
    place["names"] = names
    name = names.get("en") or place.get("name")
    place["name"] = name
    existing = set(place.get("aliases") or [])
    place["aliases"] = sorted((existing | set(entry["aliases"])) - {name})


def merge_names(cache_dir, *, wikidata=None):
    """Enrich the places' names and aliases from Wikidata, and republish them.

    Reads the geometry-stage places, fetches labels and aliases for their QIDs and
    merges them in, and publishes the places as the ``names`` generation. Returns
    the generation manifest.
    """
    if wikidata is None:
        wikidata = overture.WikidataClient()

    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, _ = store.read_jsonl(
                cache_dir / "gazetteer", "geometry.json", "places_seed.jsonl"
            )
            qids = [
                p["place_id"]
                for p in places
                if p.get("place_id") and overture.QID_PATTERN.match(p["place_id"])
            ]
            data = wikidata.labels_and_aliases(qids)

            enriched = 0
            for place in places:
                place.setdefault("aliases", [])
                entry = data.get(place.get("place_id"))
                if entry is None:
                    continue
                _merge(place, entry)
                enriched += 1

            manifest = {
                "source": "names",
                "wikidata_api": overture.WIKIDATA_API,
                "places": len(places),
                "enriched": enriched,
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "gazetteer",
                "names.json",
                {"places_seed.jsonl": store.jsonl_chunks(places)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()
