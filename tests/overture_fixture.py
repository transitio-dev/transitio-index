"""Shared builder for a small Overture ``division`` dataset in tests.

Mirrors the real theme's nested schema (names struct, sources list, nested
hierarchies) so the gazetteer and seed stages read a fixture exactly as they
read the pinned release — no live network, no live counts.
"""

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq

NAMES = pa.struct(
    [("primary", pa.string()), ("common", pa.map_(pa.string(), pa.string()))]
)
SOURCE = pa.struct(
    [
        ("property", pa.string()),
        ("dataset", pa.string()),
        ("license", pa.string()),
        ("record_id", pa.string()),
    ]
)
STEP = pa.struct(
    [("division_id", pa.string()), ("subtype", pa.string()), ("name", pa.string())]
)
SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("country", pa.string()),
        ("subtype", pa.string()),
        ("admin_level", pa.int32()),
        ("class", pa.string()),
        ("names", NAMES),
        ("wikidata", pa.string()),
        ("sources", pa.list_(SOURCE)),
        ("hierarchies", pa.list_(pa.list_(STEP))),
    ]
)


def names(primary, common):
    return {"primary": primary, "common": common}


def osm(record_id, license="ODbL"):
    return {"dataset": "OpenStreetMap", "license": license, "record_id": record_id}


def chain(*steps):
    """One hierarchy chain of ``(id, subtype, name)`` steps, country → leaf."""
    return [[{"division_id": i, "subtype": s, "name": n} for i, s, n in steps]]


def division(
    id,
    country,
    subtype,
    *,
    wikidata=None,
    name=None,
    common=None,
    admin_level=0,
    sources=None,
    hierarchies=None,
):
    """One division row with sensible defaults for the fields a test omits."""
    label = name if name is not None else id
    return {
        "id": id,
        "country": country,
        "subtype": subtype,
        "admin_level": admin_level,
        "class": None,
        "names": names(label, common or {"en": label}),
        "wikidata": wikidata,
        "sources": sources if sources is not None else [],
        "hierarchies": (
            hierarchies if hierarchies is not None else chain((id, subtype, label))
        ),
    }


def write_dataset(path, rows):
    """Write ``rows`` as a parquet file and return it as a pyarrow dataset."""
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
    return pa_ds.dataset(path)


AREA_SCHEMA = pa.schema(
    [
        ("division_id", pa.string()),
        ("geometry", pa.binary()),
        ("sources", pa.list_(SOURCE)),
        ("is_land", pa.bool_()),
        ("country", pa.string()),
        (
            "bbox",
            pa.struct(
                [
                    ("xmin", pa.float64()),
                    ("xmax", pa.float64()),
                    ("ymin", pa.float64()),
                    ("ymax", pa.float64()),
                ]
            ),
        ),
    ]
)


def area(division_id, wkb, sources, *, is_land=True, country=None):
    """One ``division_area`` row: a division's polygon and its licence sources.

    The ``bbox`` column mirrors the real release (it is what spatial pushdown
    filters on); it is derived from the WKB, zeroed when that is malformed.
    """
    try:
        import shapely

        xmin, ymin, xmax, ymax = shapely.from_wkb(wkb).bounds
    except Exception:
        xmin = ymin = xmax = ymax = 0.0
    return {
        "division_id": division_id,
        "geometry": wkb,
        "sources": sources,
        "is_land": is_land,
        "country": country,
        "bbox": {"xmin": xmin, "xmax": xmax, "ymin": ymin, "ymax": ymax},
    }


def write_area_dataset(path, rows):
    """Write ``division_area`` ``rows`` as parquet and return a dataset."""
    pq.write_table(pa.Table.from_pylist(rows, schema=AREA_SCHEMA), path)
    return pa_ds.dataset(path)


class StubWikidata:
    """A Wikidata client stand-in: fixed P402 and metro maps, no network."""

    endpoint = "stub://wikidata"

    def __init__(self, mapping=None, metros=None, labels=None):
        self.mapping = mapping or {}
        self.metros = metros or {}
        self.labels = labels or {}
        self.queried = []

    def p402(self, osm_relation_ids):
        ids = sorted({str(i) for i in osm_relation_ids})
        self.queried.append(ids)
        return {i: self.mapping[i] for i in ids if i in self.mapping}

    def statistical_metros(self, city_qids):
        return {c: self.metros[c] for c in city_qids if c in self.metros}

    def labels_and_aliases(self, qids):
        return {q: self.labels[q] for q in qids if q in self.labels}
