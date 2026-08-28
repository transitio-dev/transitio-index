"""GBFS ingest from the MobilityData ``systems.csv``, into the shared shape.

The systems catalogue is an always-latest export (1,535 systems as observed
on 2026-08-28), so the manifest's content SHA-256 is its identity. Each
system is cheaply placeable from its ``Location`` + ``Country Code`` and is
never crawled; normalization happens here, placement is a later stage.
"""

import collections

from index_build import csv_source

CSV_URL = "https://raw.githubusercontent.com/MobilityData/gbfs/master/systems.csv"
# A human tag for the fetch; the manifest's csv_sha256 is the real identity
# (systems.csv is always-latest, carrying no immutable version of its own).
DEFAULT_LABEL = "2026-08-28"

# Every column ``normalize_row`` reads; a rename upstream would otherwise be
# read as null rather than caught.
REQUIRED_HEADERS = {
    "System ID",
    "Name",
    "Location",
    "Country Code",
    "URL",
    "Auto-Discovery URL",
    "Supported Versions",
    "Authentication Type",
}


def _versions(value):
    """The ``Supported Versions`` cell as a list; e.g. ``"1.1 ; 2.3 ; 3.0"``."""
    raw = csv_source.blank_to_none(value)
    if raw is None:
        return []
    return [part.strip() for part in raw.split(";") if part.strip()]


def normalize_row(row, source_file, position):
    """One ``systems.csv`` row as an ingest record."""
    system_id = csv_source.require_id(
        row.get("System ID"), "system", source_file, position
    )
    auth_type = csv_source.blank_to_none(row.get("Authentication Type"))
    return {
        "source": "gbfs",
        "system_id": system_id,
        "spec": "gbfs",
        "name": csv_source.blank_to_none(row.get("Name")),
        "location": csv_source.blank_to_none(row.get("Location")),
        "country_code": csv_source.blank_to_none(row.get("Country Code")),
        "url": csv_source.blank_to_none(row.get("URL")),
        "auto_discovery_url": csv_source.blank_to_none(row.get("Auto-Discovery URL")),
        "supported_versions": _versions(row.get("Supported Versions")),
        "authentication_type": auth_type,
        "requires_auth": bool(auth_type),
    }


def parse_rows(rows, source_file):
    """Normalize every system row.

    System ids are NOT unique upstream: two distinct systems can share one
    (``seville`` is Cooltra and Sevici; ``citiz_la_rochelle`` is Citiz and
    Yélo). Both rows are kept — dropping or merging would lose a real system
    — and the collision is counted so a later minting stage can disambiguate.
    """
    records = [
        normalize_row(row, source_file, position) for position, row in enumerate(rows)
    ]
    counts = collections.Counter(record["system_id"] for record in records)
    collisions = sorted(name for name, count in counts.items() if count > 1)
    by_country = collections.Counter(
        record["country_code"] for record in records if record["country_code"]
    )
    return records, {
        "system_id_collisions": collisions,
        "countries": len(by_country),
    }


def ingest(cache_dir, *, csv_path=None, label=DEFAULT_LABEL, expected_sha256=None):
    """Run the GBFS ingest, publishing a ``gbfs.json`` generation."""
    return csv_source.ingest_csv(
        cache_dir,
        source="gbfs",
        label=label,
        url=CSV_URL,
        pointer="gbfs.json",
        artifact="gbfs_systems.jsonl",
        parse_rows=parse_rows,
        required_headers=REQUIRED_HEADERS,
        csv_path=csv_path,
        expected_sha256=expected_sha256,
    )
