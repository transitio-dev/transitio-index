"""Stage 2b, declared mode: pass the seed places through as the expanded set.

Crawl-driven expansion — adding places the declared seed missed, from crawled
stop clusters — needs the crawl, so until it exists 2b republishes the gazetteer
places as ``places_expanded.jsonl`` unchanged. The coverage stage always reads the
expanded pointer, so the declared path runs end to end with no "no crawl yet"
special case; the crawl-driven half extends this stage later.
"""

import datetime

from index_build import store

EXPANDED_POINTER = "expanded.json"
PLACES_ARTIFACT = "places_expanded.jsonl"


def expand(cache_dir):
    """Republish the gazetteer places as the expanded set. Returns the manifest.

    Reads the ``names`` places generation and publishes them under the
    ``expanded`` pointer, carrying the Overture release forward so the coverage
    and publish stages keep reading it from the generation they consume.
    """
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            places, names_manifest = store.read_jsonl(
                cache_dir / "gazetteer", "names.json", "places_seed.jsonl"
            )
            manifest = {
                "source": "expand",
                "mode": "declared",
                "places": len(places),
                "overture_release": names_manifest.get("overture_release"),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "gazetteer",
                EXPANDED_POINTER,
                {PLACES_ARTIFACT: store.jsonl_chunks(places)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()
