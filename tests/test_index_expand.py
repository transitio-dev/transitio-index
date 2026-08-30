import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import expand, store  # noqa: E402


def _publish(cache, subdir, pointer, artifact, records, manifest):
    directory = store.open_subdir(cache, subdir)
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / subdir,
                pointer,
                {artifact: store.jsonl_chunks(records)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()


PLACES = [
    {"place_id": "Q1757", "kind": "city", "name": "Helsinki", "metro_ids": []},
    {"place_id": "Q-metro", "kind": "metro", "name": "Metro", "member_ids": ["Q1757"]},
]


def test_expand_passes_the_seed_places_through_unchanged(tmp_path):
    cache = tmp_path / "cache"
    _publish(
        cache,
        "gazetteer",
        "names.json",
        "places_seed.jsonl",
        PLACES,
        {"source": "names", "overture_release": "2026-08-19.0"},
    )
    expand.expand(cache)
    places, manifest = store.read_jsonl(
        cache / "gazetteer", "expanded.json", "places_expanded.jsonl"
    )
    assert places == PLACES
    assert manifest["mode"] == "declared"
    assert manifest["places"] == 2
    assert manifest["overture_release"] == "2026-08-19.0"
