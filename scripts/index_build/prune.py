"""Stage 6b, gazetteer pass B2: prune the expanded places against the final
edges and publish the ``prune`` generation (``places_pruned.jsonl``).

A place stays when an edge names it, when a curator added or confirmed it
(``curated``), or when something kept needs it: the administrative ancestors
of a kept place, and the metros a kept city belongs to — a metro retained
only through membership rather than administrative descent must not be
pruned out from under a promotion. Everything else has no edge and no kept
descendant and goes.

Removals cascade rather than leave dangling ids, the same closure stage 7
re-applies after licensing: a dropped parent re-points its children at the
nearest surviving ancestor, a dropped ``default_metro_id`` is cleared and
marked so (the city simply stops promoting — publish must not re-infer a
default from a lone surviving metro), and metro membership stays reciprocal — a
dropped metro leaves its members' ``metro_ids``, a dropped city its metros'
``member_ids``. Pruning runs after edge overrides, never before: a curator's
``add_edge`` can name a place automated pruning would have removed, and
``remove_edge`` can strip the last edge from one that would then ship with
nothing pointing at it.
"""

import collections
import contextlib
import copy
import datetime

from index_build import classify, store

PRUNE_POINTER = "places_pruned.json"
PLACES_ARTIFACT = "places_pruned.jsonl"
KINDS = ("country", "region", "city", "metro")
# Every metric a manifest carries, zero included: a rerun that pruned nothing
# says so, rather than leaving the reader to guess from a missing key.
METRICS = (
    "kept",
    "kept_curated_without_edges",
    "reparented",
    "default_metro_cleared",
    "metro_ids_trimmed",
    "member_ids_trimmed",
    *(f"dropped_{kind}" for kind in KINDS),
)


class PruneError(RuntimeError):
    """The pruning stage cannot run against the artifacts as they are."""


def keep_set(places, edges):
    """The ids of the places that survive: edge-bearing or curated places,
    their administrative ancestors, and the metros of kept cities."""
    kept = {edge["place_id"] for edge in edges if edge["place_id"] in places}
    kept |= {pid for pid, place in places.items() if place.get("curated")}
    queue = list(kept)
    while queue:
        place = places.get(queue.pop())
        if place is None:
            continue
        wanted = [place.get("parent_id")] + list(place.get("metro_ids") or [])
        for pid in wanted:
            if pid and pid in places and pid not in kept:
                kept.add(pid)
                queue.append(pid)
    return kept


def _surviving_ancestor(place, places, kept):
    parent = place.get("parent_id")
    seen = set()
    while parent and parent not in kept and parent in places and parent not in seen:
        seen.add(parent)
        parent = places[parent].get("parent_id")
    return parent if parent in kept else None


def prune_places(places, edges):
    """``(kept places, report)``: the surviving places with the closure
    applied, and what pruning did."""
    by_id = {place["place_id"]: copy.deepcopy(place) for place in places}
    kept = keep_set(by_id, edges)
    report = collections.Counter({metric: 0 for metric in METRICS})
    survivors = []
    for pid in sorted(by_id):
        place = by_id[pid]
        if pid not in kept:
            report[f"dropped_{place.get('kind')}"] += 1
            continue
        if place.get("parent_id") and place["parent_id"] not in kept:
            place["parent_id"] = _surviving_ancestor(place, by_id, kept)
            report["reparented"] += 1
        metros = list(place.get("metro_ids") or [])
        if not place.get("default_metro_id") and len(metros) == 1:
            # The implicit single-metro default, made explicit before the
            # closure: a default this pass clears must never be re-inferred
            # downstream from whichever metro happens to survive.
            place["default_metro_id"] = metros[0]
        if place.get("default_metro_id") and place["default_metro_id"] not in kept:
            place["default_metro_id"] = None
            place["default_metro_cleared"] = True
            report["default_metro_cleared"] += 1
        for key in ("metro_ids", "member_ids"):
            before = list(place.get(key) or [])
            after = [other for other in before if other in kept]
            if after != before:
                place[key] = after
                report[f"{key}_trimmed"] += 1
        survivors.append(place)
    report["kept"] = len(survivors)
    report["kept_curated_without_edges"] = sum(
        1
        for place in survivors
        if place.get("curated")
        and place["place_id"] not in {e["place_id"] for e in edges}
    )
    return survivors, dict(report)


def prune(cache_dir):
    """Prune the expanded places against the final edges; publish the
    ``prune`` generation. Returns the manifest."""
    with contextlib.ExitStack() as stack:
        # The global lock order, then this stage's own, then the crawl's.
        for subdir in classify.EDGE_STAGES:
            held = store.open_subdir(cache_dir, subdir)
            stack.callback(held.close)
            stack.enter_context(store.exclusive_writer(held))
        directory = store.open_subdir(cache_dir, "prune")
        stack.callback(directory.close)
        stack.enter_context(store.exclusive_writer(directory))
        from index_build import crawl

        stack.enter_context(crawl.reading(cache_dir))
        try:
            _, edges, manifest = classify.read_edges(cache_dir, locked=True)
        except classify.ClassifyError as error:
            raise PruneError(str(error)) from error
        if manifest is None or manifest.get("source") != "curate":
            # Only final edges may prune: an add_edge or remove_edge that has
            # not been applied would leave places and edges disagreeing.
            raise PruneError("no curate generation to prune against; run curate")
        place_rows, expanded = store.read_jsonl(
            cache_dir / "gazetteer", "expanded.json", "places_expanded.jsonl"
        )
        if manifest.get("expanded_generation") != expanded.get("generation"):
            raise PruneError(
                "the curated edges were not derived from the current expanded "
                "places; re-run the pipeline in stage order"
            )
        survivors, report = prune_places(place_rows, edges)
        manifest_out = {
            "source": "prune",
            "sources": manifest.get("sources"),
            "overture_release": expanded.get("overture_release"),
            # The exact generations pruned against: publish refuses these
            # places once either has moved on.
            "curate_generation": manifest.get("generation"),
            "expanded_generation": expanded.get("generation"),
            "places_before": len(place_rows),
            **report,
            "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        return store.publish(
            cache_dir / "prune",
            PRUNE_POINTER,
            {PLACES_ARTIFACT: store.jsonl_chunks(survivors)},
            manifest_out,
            held=directory,
        )
