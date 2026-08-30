"""Overture divisions ingest and Wikidata QID resolution for the gazetteer.

Reads the pinned Overture ``divisions`` release as projected GeoParquet, keeps
the whole administrative skeleton (country, region, county, localadmin — geometry
excluded), and resolves each division to a canonical Wikidata QID: the
division's own ``wikidata`` property, else a reverse lookup
from an OSM relation id it was built from (Wikidata ``P402``), else — for a bare
name/level/country candidate — the resolution report only, never a minted
identity. Localities are feed-driven (3.5M rows, ~26% carry a QID) and are
matched from feed municipalities in a later stage rather than bulk-resolved here.

The Overture read uses ``pyarrow.fs.S3FileSystem`` against the anonymous public
bucket; tests pass a local dataset and a stub Wikidata client instead, so the
stage runs offline with no network and no live counts.
"""

import datetime
import json
import re
import urllib.parse
import urllib.request

import pyarrow.dataset as ds

from index_build import store

OVERTURE_RELEASE = "2026-08-19.0"
OVERTURE_BUCKET = "overturemaps-us-west-2"
OVERTURE_REGION = "us-west-2"
DIVISION_PATH = "release/{release}/theme=divisions/type=division"

# Overture ``subtype`` to the gazetteer's ``kind``. Finer subtypes (borough,
# neighborhood, macrohood, microhood) are below the resolution feeds are worth
# indexing at and are rejected, not coerced; the raw value stays in
# ``source_subtype``. ``dependency`` is a country-level territory.
SUBTYPE_TO_KIND = {
    "country": "country",
    "dependency": "country",
    "region": "region",
    "county": "region",
    "localadmin": "city",
    "locality": "city",
}

# The administrative skeleton bulk-resolved here — bounded (~64k rows globally)
# and QID-rich. ``locality`` is deliberately excluded: it is the feed-driven
# city level and is matched from feed municipalities in a later stage.
SKELETON_SUBTYPES = ("country", "dependency", "region", "county", "localadmin")

# The only columns the resolution needs; predicate pushdown means the full
# theme is never materialised.
PROJECT = [
    "id",
    "country",
    "subtype",
    "admin_level",
    "class",
    "names",
    "wikidata",
    "sources",
    "hierarchies",
]

# An OSM relation reference inside a source ``record_id`` (``relation/123`` or a
# bare ``123``); ways and nodes are not admin boundaries and are not matched.
_OSM_RELATION = re.compile(r"\Arelation[/:]?(\d+)\Z|\A(\d+)\Z")

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
QID_PATTERN = re.compile(r"\AQ[1-9][0-9]*\Z")
# Wikimedia asks automated clients to identify themselves; a generic urllib
# agent is liable to be refused, which only shows up on the live build path.
USER_AGENT = "transitio-index-build (+https://github.com/cafein-py/transitio)"


class GazetteerError(RuntimeError):
    """The gazetteer stage could not resolve its inputs."""


def overture_dataset(release=OVERTURE_RELEASE):
    """The pinned Overture ``division`` theme as a pyarrow dataset over S3."""
    from pyarrow.fs import S3FileSystem

    filesystem = S3FileSystem(anonymous=True, region=OVERTURE_REGION)
    path = f"{OVERTURE_BUCKET}/{DIVISION_PATH.format(release=release)}"
    return ds.dataset(path, filesystem=filesystem, format="parquet")


def read_divisions(dataset, *, subtypes=SKELETON_SUBTYPES):
    """The projected admin-skeleton divisions (whole world, geometry excluded).

    Filtered to the skeleton subtypes only, never to a country set: a feed's
    country is not always known from catalogue metadata (Atlas locates by
    geohash, with no ISO code), so country-scoping here would silently drop
    those feeds' administrative context. The skeleton is ~64k rows globally;
    the feed-driven pruning to the shipped place universe happens in later
    stages.
    """
    predicate = ds.field("subtype").isin(list(subtypes))
    return dataset.to_table(columns=PROJECT, filter=predicate).to_pylist()


def _names(names):
    """``(primary, {lang: label})`` from an Overture ``names`` struct."""
    if not names:
        return None, {}
    primary = names.get("primary")
    # pyarrow renders a map as a list of ``(key, value)`` pairs; ``dict`` takes
    # either that or an already-materialised mapping.
    return primary, dict(names.get("common") or [])


def _ancestors(hierarchies):
    """The admin ancestor chain (country → parent), self excluded.

    Overture ``hierarchies`` is a list of chains from country down to the
    division; the first is the primary one. The last element is the division
    itself, so it is dropped.
    """
    if not hierarchies:
        return []
    chain = hierarchies[0] or []
    return [
        {
            "overture_id": step.get("division_id"),
            "subtype": step.get("subtype"),
            "name": step.get("name"),
        }
        for step in chain[:-1]
    ]


def _osm_relation_ids(sources):
    """OSM relation ids among a division's sources, in order, de-duplicated."""
    found = []
    for source in sources or []:
        dataset_name = (source.get("dataset") or "").lower()
        if "openstreetmap" not in dataset_name and dataset_name != "osm":
            continue
        match = _OSM_RELATION.match((source.get("record_id") or "").strip())
        if match:
            relation = match.group(1) or match.group(2)
            if relation not in found:
                found.append(relation)
    return found


def _source_licences(sources):
    """The distinct ``(dataset, license)`` pairs backing a division.

    Kept for the later boundary licence audit; a division's geometry may not
    ship until every source that built it is on the allowlist.
    """
    seen = []
    for source in sources or []:
        pair = {
            "dataset": source.get("dataset"),
            "license": source.get("license"),
        }
        if pair not in seen:
            seen.append(pair)
    return seen


def normalize_division(row):
    """One Overture division row as a gazetteer candidate record."""
    subtype = row.get("subtype")
    primary, labels = _names(row.get("names"))
    # A malformed ``wikidata`` value is not a QID and must not be minted as
    # identity; it is dropped to None so the division falls to P402 or report.
    wikidata = row.get("wikidata")
    if not (wikidata and QID_PATTERN.match(wikidata)):
        wikidata = None
    return {
        "overture_id": row.get("id"),
        "subtype": subtype,
        "kind": SUBTYPE_TO_KIND.get(subtype),
        "source_subtype": subtype,
        "admin_level": row.get("admin_level"),
        "country": row.get("country"),
        "name": primary,
        "names": labels,
        "wikidata": wikidata,
        "osm_relation_ids": _osm_relation_ids(row.get("sources")),
        "sources": _source_licences(row.get("sources")),
        "ancestors": _ancestors(row.get("hierarchies")),
    }


class WikidataClient:
    """Reverse OSM-relation → QID lookups against the Wikidata SPARQL endpoint.

    Queried once per build, in batches keyed by the relation id. Only the
    Overture release is pinned; this endpoint is live, so results track Wikidata
    at build time. Tests substitute a stub with the same ``p402`` method rather
    than reaching the network.
    """

    def __init__(self, endpoint=WIKIDATA_SPARQL, *, timeout=60, batch_size=200):
        self.endpoint = endpoint
        self.timeout = timeout
        self.batch_size = batch_size

    def p402(self, osm_relation_ids):
        """``{osm_relation_id: qid}``; an ambiguous id maps to ``None``.

        A relation present with a ``None`` value has conflicting P402 claims —
        distinct from an id that is simply absent (no claim) — so a caller can
        tell a conflict from missing evidence.
        """
        ids = sorted({str(rid) for rid in osm_relation_ids if rid})
        found = {}
        for start in range(0, len(ids), self.batch_size):
            self._query_batch(ids[start : start + self.batch_size], found)
        return found

    def _query_batch(self, batch, found):
        values = " ".join(f'"{rid}"' for rid in batch)
        query = (
            "SELECT ?osm ?item WHERE { "
            f"VALUES ?osm {{ {values} }} ?item wdt:P402 ?osm . }}"
        )
        url = (
            self.endpoint
            + "?"
            + urllib.parse.urlencode({"query": query, "format": "json"})
        )
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/sparql-results+json",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        for binding in payload.get("results", {}).get("bindings", []):
            osm = binding.get("osm", {}).get("value")
            item = binding.get("item", {}).get("value", "")
            qid = item.rsplit("/", 1)[-1]
            if not osm or not QID_PATTERN.match(qid):
                continue
            # A relation mapped to a second, different item is ambiguous:
            # record None so neither is minted.
            if osm in found and found[osm] != qid:
                found[osm] = None
            elif osm not in found:
                found[osm] = qid


def resolve_qid(record, p402_map):
    """``(qid, method, reason)`` for a candidate.

    A direct ``wikidata`` QID wins. Otherwise every OSM relation the division
    was built from is looked up: one agreed QID resolves it; two different QIDs,
    or a relation Wikidata itself reports ambiguously, resolves to none and is
    reported as conflicting rather than absent. ``reason`` is set only when
    there is no QID.
    """
    if record["wikidata"]:
        return record["wikidata"], "overture_wikidata", None
    qids = set()
    conflicting = False
    for relation in record["osm_relation_ids"]:
        if relation in p402_map:
            value = p402_map[relation]
            if value is None:
                conflicting = True
            else:
                qids.add(value)
    if len(qids) == 1 and not conflicting:
        return qids.pop(), "osm_p402", None
    if qids or conflicting:
        # One clean QID alongside an ambiguous relation is still ambiguous:
        # any conflict signal leaves the division unresolved, never minted.
        return None, "name_country", "conflicting P402 identities"
    return None, "name_country", "no wikidata property and no P402 match"


def resolve(cache_dir, *, dataset=None, wikidata=None):
    """Ingest the Overture admin skeleton and resolve each division to a QID.

    Reads the whole skeleton from the pinned Overture release and resolves each
    division to a QID. Divisions with a QID are published to
    ``overture_divisions.jsonl`` (with the OSM relation ids kept as a
    crosswalk); bare name/level/country candidates go to
    ``place_resolution_report.jsonl`` for curation, never minted. Returns the
    generation manifest.
    """
    if dataset is None:
        dataset = overture_dataset()
    if wikidata is None:
        wikidata = WikidataClient()

    records = [normalize_division(row) for row in read_divisions(dataset)]

    # One batched P402 pass for every relation id on a division without a QID.
    pending = {
        relation
        for record in records
        if not record["wikidata"]
        for relation in record["osm_relation_ids"]
    }
    p402_map = wikidata.p402(pending) if pending else {}

    resolved = []
    report = []
    by_method = {"overture_wikidata": 0, "osm_p402": 0}
    for record in records:
        qid, method, reason = resolve_qid(record, p402_map)
        if qid is None:
            report.append(
                {
                    "overture_id": record["overture_id"],
                    "kind": record["kind"],
                    "subtype": record["subtype"],
                    "country": record["country"],
                    "name": record["name"],
                    "admin_level": record["admin_level"],
                    "osm_relation_ids": record["osm_relation_ids"],
                    "reason": reason,
                }
            )
            continue
        by_method[method] += 1
        published = dict(record)
        published["qid"] = qid
        published["resolution_method"] = method
        resolved.append(published)

    manifest = {
        "source": "overture",
        "overture_release": OVERTURE_RELEASE,
        "wikidata_endpoint": wikidata.endpoint,
        "countries": sorted({r["country"] for r in resolved if r["country"]}),
        "divisions_read": len(records),
        "resolved": len(resolved),
        "reported": len(report),
        "resolved_by_method": by_method,
        "retrieved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    out = cache_dir / "gazetteer"
    # Reach the store through ``cache_dir`` so a symlink at the cache root
    # cannot redirect the publish; one lock covers the whole write.
    directory = store.open_subdir(cache_dir, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            return store.publish(
                out,
                "overture.json",
                {
                    "overture_divisions.jsonl": store.jsonl_chunks(resolved),
                    "place_resolution_report.jsonl": store.jsonl_chunks(report),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
