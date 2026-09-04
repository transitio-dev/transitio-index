"""Stage 7, licensing: the licence inventory and NOTICE for what ships, and
the licensed artifacts publication reads.

Reads exactly what publication would read — the feeds, edges and places of
the current build, through the same lineage-checked readers — records every
contributing source in ``licence_inventory.jsonl`` (the geometry audit's rows
and the feeds' own licence blocks), writes the ``NOTICE`` that ships with the
index, and publishes the three tables as ``feeds_licensed.jsonl``,
``places_licensed.jsonl`` and ``edges_licensed.jsonl``.

The feeds are sanitised on the way: a feed's coverage hull is derived from
its contents, so it ships only where the feed's licence permits
redistribution — declared as such, or a known-permissive licence — and is
nulled where redistribution is explicitly disallowed; a feed whose licence
is unknown keeps its hull, a recorded judgement the ``redistribution_allowed``
column lets a stricter user overrule.

A place without a boundary of its own — its sources outside the geometry
allowlist, or a metro — is given the buffered union of the redistributable
coverage hulls of the feeds with an edge to it, labelled
``geometry_source = "derived_from_feeds"``, so every place that can have an
AOI has one materialised. Places still without a geometry are counted;
dropping them and rehoming their edges follow.
"""

import collections
import contextlib
import datetime

from index_build import crawl, store

POINTER = "licensed.json"
FEEDS_ARTIFACT = "feeds_licensed.jsonl"
PLACES_ARTIFACT = "places_licensed.jsonl"
EDGES_ARTIFACT = "edges_licensed.jsonl"
INVENTORY_ARTIFACT = "licence_inventory.jsonl"
NOTICE_ARTIFACT = "NOTICE"


# SPDX identifiers whose terms permit redistributing derived data (with
# attribution or share-alike conditions the NOTICE carries); a feed under
# one of these ships its hull even when the record says nothing explicit.
# SPDX identifiers compare case-insensitively.
KNOWN_PERMISSIVE = frozenset(
    identifier.lower()
    for identifier in (
        "CC0-1.0",
        "CC-BY-3.0",
        "CC-BY-4.0",
        "CC-BY-SA-3.0",
        "CC-BY-SA-4.0",
        "ODbL-1.0",
        "ODC-By-1.0",
        "PDDL-1.0",
        "OGL-UK-3.0",
        "MIT",
        "Apache-2.0",
    )
)

# The sanitisation rules a licensed generation was built under; publication
# refuses a generation from an older policy rather than shipping it.
POLICY_VERSION = 2

DERIVED_SOURCE = "derived_from_feeds"
# A derived boundary is the hulls' union widened a little, so an AOI cut
# from it reaches past the outermost stops.
DERIVED_BUFFER_DEG = 0.01


class LicenseError(RuntimeError):
    """The licence inventory could not be built, or sanitisation failed."""


def redistribution_allowed(record):
    """Whether the feed's licence permits redistributing data derived from
    it: the record's own declaration when it makes one, else True for a
    known-permissive licence, else None (unknown)."""
    block = (record.get("atlas") or {}).get("license") or {}
    declared = block.get("redistribution_allowed")
    if isinstance(declared, bool):
        return declared
    if isinstance(declared, str) and declared.strip().lower() in ("yes", "no"):
        return declared.strip().lower() == "yes"
    spdx = block.get("spdx_identifier") or block.get("spdx_id")
    if isinstance(spdx, str) and spdx.strip().lower() in KNOWN_PERMISSIVE:
        return True
    return None


def _sanitise_feeds(records):
    """Stamp ``redistribution_allowed`` on every feed and null the coverage
    hull of each feed whose licence disallows redistribution; returns the
    number of hulls nulled."""
    nulled = 0
    for record in records:
        allowed = redistribution_allowed(record)
        record["redistribution_allowed"] = allowed
        if allowed is False and record.get("coverage") is not None:
            record["coverage"] = None
            nulled += 1
    return nulled


def _assert_sanitised(records):
    """The post-condition: nothing prohibited survived into the artifacts."""
    leaked = [
        r["feed_id"]
        for r in records
        if r.get("redistribution_allowed") is False and r.get("coverage") is not None
    ]
    if leaked:
        raise LicenseError(
            f"prohibited coverage hulls survived sanitisation: {sorted(leaked)[:5]}"
        )


def _derive_geometry(places, edges, records):
    """Give each place without a boundary the buffered union of the
    redistributable coverage hulls of the feeds with an edge to it. Only a
    hull judged redistributable contributes: a place's boundary built from
    a withheld hull would ship what the policy withholds. Returns how many
    boundaries were derived and how many places still have none."""
    import shapely

    from index_build import geometry

    hulls = {
        r["feed_id"]: r["coverage"]
        for r in records
        if r.get("coverage") is not None and r.get("redistribution_allowed") is True
    }
    by_place = collections.defaultdict(set)
    for edge in edges or ():
        if edge["feed_id"] in hulls:
            by_place[edge["place_id"]].add(hulls[edge["feed_id"]])
    derived = missing = 0
    for place in places:
        if place.get("geometry") is not None:
            continue
        shapes = [
            shapely.from_wkb(bytes.fromhex(hull))
            for hull in sorted(by_place.get(place["place_id"], ()))
        ]
        union = None
        if shapes:
            # Buffered in degrees, then clipped to the WGS84 domain like the
            # hulls themselves, so a boundary at the dateline stays in range.
            union = geometry._simplify(
                shapely.intersection(
                    shapely.unary_union(shapes).buffer(DERIVED_BUFFER_DEG),
                    shapely.box(-180.0, -90.0, 180.0, 90.0),
                )
            )
        if not geometry._valid_polygon(union):
            missing += 1
            continue
        place["geometry"] = shapely.to_wkb(union).hex()
        place["geometry_source"] = DERIVED_SOURCE
        derived += 1
    return derived, missing


def _geometry_audit(cache_dir):
    """The geometry stage's inventory rows, NOTICE and generation, or none
    without it."""
    pointer = cache_dir / "gazetteer" / "geometry.json"
    if not (pointer.is_symlink() or pointer.exists()):
        return [], None, None
    try:
        rows, manifest = store.read_jsonl(
            cache_dir / "gazetteer", "geometry.json", "licence_inventory.jsonl"
        )
        generation, _ = store.resolve(cache_dir / "gazetteer", "geometry.json")
        with generation:
            notice = generation.read_bytes(NOTICE_ARTIFACT).decode("utf-8")
    except (store.StoreError, ValueError) as error:
        raise LicenseError(f"the geometry audit is unreadable: {error}") from error
    return rows, notice, manifest.get("generation")


def _feed_rows(records):
    """One inventory row per distinct feed licence: the SPDX id and URL the
    feed's catalogue record declares, how many feeds carry it, and what it
    says about redistribution. Feeds with no licence block count as one
    row of their own, so the inventory says how much is unknown."""
    counts = collections.Counter()
    redistribution = collections.defaultdict(collections.Counter)
    judgements = collections.defaultdict(collections.Counter)
    for record in records:
        block = (record.get("atlas") or {}).get("license") or {}
        # The Atlas declaration is taken whole; the Mobility Database's
        # licence URL stands in only for a feed with no Atlas declaration,
        # never combined with one.
        url = block.get("url")
        if not block:
            url = (record.get("mdb") or {}).get("license_url")
        # Feeds group only when their whole attribution requirement matches.
        key = (
            block.get("spdx_identifier") or block.get("spdx_id"),
            url,
            block.get("attribution_text"),
            block.get("attribution_instructions"),
        )
        counts[key] += 1
        value = block.get("redistribution_allowed")
        redistribution[key]["unknown" if value is None else str(value).lower()] += 1
        # The effective judgement the stage applied, beside the raw declaration.
        judged = record.get("redistribution_allowed")
        judgements[key]["unknown" if judged is None else str(judged).lower()] += 1
    rows = []
    for key, count in sorted(
        counts.items(), key=lambda item: tuple(str(part) for part in item[0])
    ):
        spdx_id, url, text, instructions = key
        rows.append(
            {
                "role": "feed_licence",
                "dataset": None,
                "license": spdx_id,
                "url": url,
                "version": None,
                "allowed": None,
                "feeds": count,
                "redistribution_allowed": dict(sorted(redistribution[key].items())),
                "judgement": dict(sorted(judgements[key].items())),
                "attribution_text": text,
                "attribution_instructions": instructions,
            }
        )
    return rows


# The catalogues a build reads, with the manifest keys that pin them.
CATALOGUES = (
    ("Transitland Atlas", "atlas", "commit"),
    ("Mobility Database catalog", "mdb", "csv_sha256"),
    ("GBFS systems.csv", "gbfs", "csv_sha256"),
)


def _catalogue_rows(sources):
    """One inventory row per catalogue the build read, at its pinned
    version. Their licences are not audited yet, so the rows record that
    plainly rather than a guess."""
    rows = []
    for dataset, key, version_key in CATALOGUES:
        pinned = (sources or {}).get(key) or {}
        if pinned.get(version_key):
            rows.append(
                {
                    "role": "catalogue",
                    "dataset": dataset,
                    "license": None,
                    "url": None,
                    "version": pinned[version_key],
                    "allowed": None,
                }
            )
    return rows


def _notice(geometry_notice, sources, feed_rows):
    """The NOTICE that ships: the geometry audit's text, then the feed
    catalogues at their pinned versions and the feed licences seen."""
    lines = []
    if geometry_notice:
        lines.append(geometry_notice.rstrip("\n"))
        lines.append("")
    lines.append("Feed identities and coverage were compiled from:")
    atlas = (sources or {}).get("atlas") or {}
    if atlas.get("commit"):
        lines.append(f"  - Transitland Atlas, commit {atlas['commit']}")
    mdb = (sources or {}).get("mdb") or {}
    if mdb.get("csv_sha256"):
        lines.append(f"  - Mobility Database catalog, sha256 {mdb['csv_sha256']}")
    gbfs = (sources or {}).get("gbfs") or {}
    if gbfs.get("csv_sha256"):
        lines.append(f"  - GBFS systems.csv, sha256 {gbfs['csv_sha256']}")
    lines.append("")
    lines.append("Feed licences declared by the catalogues (feeds per licence):")
    for row in feed_rows:
        name = row["license"] or "no identifier"
        if row["license"] is None and row["url"] is None:
            name = "none declared"
        lines.append(f"  - {name}: {row['feeds']}")
        if row["url"]:
            lines.append(f"      url: {row['url']}")
        # The attribution a licence requires ships verbatim with the count.
        if row.get("attribution_text"):
            lines.append(f"      attribution: {row['attribution_text']}")
        if row.get("attribution_instructions"):
            lines.append(f"      instructions: {row['attribution_instructions']}")
    return "\n".join(lines) + "\n"


def license_index(cache_dir, *, overrides_dir=None):
    """Publish the ``license`` generation for the current build. Returns
    the manifest. Every lock publication holds is held here too, in the
    same order, then this stage's own, then the crawl's."""
    from index_build import publish

    with contextlib.ExitStack() as stack:
        for subdir in publish.STAGE_LOCKS:
            held = store.open_subdir(cache_dir, subdir)
            stack.callback(held.close)
            stack.enter_context(store.exclusive_writer(held))
        directory = store.open_subdir(cache_dir, "license")
        stack.callback(directory.close)
        stack.enter_context(store.exclusive_writer(directory))
        stack.enter_context(crawl.reading(cache_dir))
        try:
            inputs = publish.read_inputs(cache_dir, overrides_dir)
        except publish.PublishError as error:
            raise LicenseError(str(error)) from error
        audit_rows, geometry_notice, _ = _geometry_audit(cache_dir)
        if inputs["places"] is not None and geometry_notice is None:
            raise LicenseError(
                "a places build has no geometry audit to license; run the gazetteer "
                "stage"
            )
        records = inputs["records"]
        hulls_nulled = _sanitise_feeds(records)
        _assert_sanitised(records)
        derived = missing = None
        if inputs["places"] is not None:
            derived, missing = _derive_geometry(
                inputs["places"], inputs["edges"], records
            )
        feed_rows = _feed_rows(records)
        inventory = audit_rows + _catalogue_rows(inputs["sources"]) + feed_rows
        notice = _notice(geometry_notice, inputs["sources"], feed_rows)
        artifacts = {
            FEEDS_ARTIFACT: store.jsonl_chunks(inputs["records"]),
            INVENTORY_ARTIFACT: store.jsonl_chunks(inventory),
            NOTICE_ARTIFACT: lambda: [notice],
        }
        if inputs["places"] is not None:
            artifacts[PLACES_ARTIFACT] = store.jsonl_chunks(inputs["places"])
        if inputs["edges"] is not None:
            artifacts[EDGES_ARTIFACT] = store.jsonl_chunks(inputs["edges"])
        manifest = {
            "source": "license",
            "licensed": True,
            "policy": POLICY_VERSION,
            "sources": inputs["sources"],
            "overture_release": inputs["overture_release"],
            # What was read, so publication can prove the licensed tables
            # descend from the inputs it would otherwise read itself.
            "generations": inputs["generations"],
            "leaves": inputs["leaves"],
            "inputs": {
                "edges": inputs["coverage"],
                "resolve": inputs["resolve_manifest"],
                "places": inputs["places_manifest"],
                "override_digest": inputs["override_digest"],
            },
            "feeds": len(records),
            "hulls_nulled": hulls_nulled,
            "redistribution_allowed": dict(
                collections.Counter(
                    str(r["redistribution_allowed"]).lower() for r in records
                )
            ),
            "places": None if inputs["places"] is None else len(inputs["places"]),
            "geometry_derived": derived,
            "places_without_geometry": missing,
            "edges": None if inputs["edges"] is None else len(inputs["edges"]),
            "inventory": len(inventory),
            "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        return store.publish(
            cache_dir / "license", POINTER, artifacts, manifest, held=directory
        )
