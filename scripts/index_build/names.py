"""Merge Wikidata labels and aliases into the gazetteer places.

Each place keeps its Overture ``names`` — Overture wins for a shared language —
and gains Wikidata labels for the languages Overture lacks; ``name`` is set to the
English label. ``aliases`` is the union of any existing aliases and Wikidata's
"also known as" values (both CC0), with the place's own name removed. Wikidata is
queried once per build via the ``wbgetentities`` API; a curator's
``set_aliases`` additions are applied here, after the Wikidata merge.
"""

import datetime

from index_build import overrides, overture, store


def _merge(place, entry):
    """Fold one place's Wikidata labels and aliases into its record in place."""
    names = dict(entry["labels"])
    names.update(place.get("names") or {})  # Overture wins for a shared language
    place["names"] = names
    name = names.get("en") or place.get("name")
    place["name"] = name
    existing = set(place.get("aliases") or [])
    place["aliases"] = sorted((existing | set(entry["aliases"])) - {name})


def _set_aliases(places, entries, report):
    """A curator's aliases join the place's, minus its name. Judged against
    the name and aliases the place has now."""
    by_id = {p["place_id"]: p for p in places}
    for entry in entries:
        place = by_id.get(entry["place"])
        if place is None:
            raise overrides.OverrideError(
                f"place {entry['place']!r}: set_aliases needs a seeded place"
            )
        overrides.judge(
            entry,
            {"name": place.get("name"), "aliases": sorted(place.get("aliases") or [])},
            report,
            "names",
        )
        merged = set(place.get("aliases") or []) | set(entry["set_aliases"])
        place["aliases"] = sorted(merged - {place.get("name")})
    return len(entries)


def merge_names(cache_dir, *, wikidata=None, overrides_dir=None, strict=False):
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
            places, geometry_manifest = store.read_jsonl(
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
            place_overrides, places_digest = overrides.load_place_overrides(
                overrides_dir
            )
            overrides.expect_digest(
                geometry_manifest.get("places_overrides_sha256"),
                places_digest,
                "places.yaml",
                "gazetteer",
            )
            override_report = []
            applied = _set_aliases(
                places,
                overrides.by_operation(place_overrides, "set_aliases"),
                override_report,
            )

            manifest = {
                "source": "names",
                "sources": geometry_manifest.get("sources"),
                "seed_generation": geometry_manifest.get("seed_generation"),
                "wikidata_api": overture.WIKIDATA_API,
                # Carried forward so the publish stage reads the pinned release
                # from the same generation as the places, not a separate pointer.
                "overture_release": geometry_manifest.get("overture_release"),
                "places": len(places),
                "enriched": enriched,
                "places_overrides_sha256": places_digest,
                "overrides_applied": applied,
                "stale_overrides": len(override_report),
                "stale_place_overrides": (
                    geometry_manifest.get("stale_place_overrides") or 0
                )
                + len(override_report),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            published = store.publish(
                cache_dir / "gazetteer",
                "names.json",
                {
                    "places_seed.jsonl": store.jsonl_chunks(places),
                    "override_report.jsonl": store.jsonl_chunks(override_report),
                },
                manifest,
                held=directory,
            )
            overrides.strict_check(strict, override_report, "names")
            return published
    finally:
        directory.close()
