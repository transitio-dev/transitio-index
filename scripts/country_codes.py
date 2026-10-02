#!/usr/bin/env python3

"""Write ``transitio_index/country_codes.csv``, the index's country code for
each FAO country code and GHS-UCDB country name.

A maintainer tool, not part of the package: the build reads the CSV. It reads
the pinned FAO regions and UCDB from ``--cache-dir`` and the ISO 3166-1 list
of Debian's iso-codes as pycountry 26.2.16 ships it, fetched into the same
raw store and refused unless its SHA-256 is ``ISO_PIN``. A FAO code maps from
alpha-3 to alpha-2; a UCDB name by an iso-codes name, common name or official
name, read with its mis-decoded accents repaired, or by ``OVERRIDES``. It
prints each FAO code whose named centres the UCDB places in other countries.
"""

import argparse
import collections
import csv
import json
import pathlib

from transitio_index import country_codes, fao, pinned, ucdb

ISO_FILE = "iso3166-1.json"
ISO_URL = (
    "https://cdn.jsdelivr.net/gh/pycountry/pycountry@26.2.16/"
    f"src/pycountry/databases/{ISO_FILE}"
)
ISO_PIN = "f01b812b57fba9f31ff621bf33e7c7570a01964dbeb5be2167e94decf538c89f"
ISO_POINTER = "iso-codes.json"
# By the repaired UCDB name.
OVERRIDES = {
    "Brunei": "BN",
    "Democratic Republic of the Congo": "CD",
    "Kosovo": "XK",
    "México": "MX",
    "Russia": "RU",
    "Swaziland": "SZ",
    "São Tomé and Príncipe": "ST",
    "Turkey": "TR",
}
FAO_CODES = {"XKO": "XK"}
# Territories the index partitions apart from the country the inputs name.
LEFT_OUT = {"Palestine", "Western Sahara", "Northern Cyprus"}
LEFT_OUT |= {"PSE", "ESH", "ZNC", "XAD", "Z01", "Z06", "Z07"}


def _repaired(name):
    """``name`` with UTF-8 bytes read as Latin-1 decoded again."""
    try:
        return name.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return name


def _read(cache_dir, pointer, pins, name, error=pinned.PinnedInputError):
    generation, _ = pinned.resolve(
        cache_dir, pointer=pointer, expected=pins, error=error
    )
    with generation:
        return generation.read_bytes(name)


def _mappings(cache):
    """``(regions, by_iso3, by_ucdb)``: the FAO regions, and the index's
    code of each FAO code and UCDB name."""
    regions = fao.read_regions(_read(cache, fao.POINTER, fao.PINS, fao.REGIONS_FILE))
    centres, _ = ucdb.read_ucdb(_read(cache, ucdb.POINTER, ucdb.PINS, ucdb.UCDB_FILE))
    pins = {ISO_FILE: ISO_PIN}
    pinned.prepare(cache, pointer=ISO_POINTER, urls={ISO_FILE: ISO_URL}, expected=pins)
    iso = json.loads(_read(cache, ISO_POINTER, pins, ISO_FILE))["3166-1"]
    alpha2 = {row["alpha_3"]: row["alpha_2"] for row in iso}
    by_name = {
        row[key]: row["alpha_2"]
        for row in iso
        for key in ("name", "common_name", "official_name")
        if row.get(key)
    }
    by_iso3 = {
        code: FAO_CODES.get(code) or alpha2.get(code)
        for code in {region["country"] for region in regions.values()} - LEFT_OUT
    }
    by_ucdb = {}
    for name in {centre["country"] for centre in centres} - {None}:
        repaired = _repaired(name)
        if repaired not in LEFT_OUT:
            by_ucdb[name] = OVERRIDES.get(repaired) or by_name.get(repaired)
    return regions, by_iso3, by_ucdb


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--cache-dir", type=pathlib.Path, default=pathlib.Path("cache"))
    parser.add_argument("--out", type=pathlib.Path, default=country_codes.TABLE)
    args = parser.parse_args()
    regions, by_iso3, by_ucdb = _mappings(args.cache_dir)
    unmapped = sorted(
        k for m in (by_iso3, by_ucdb) for k, code in m.items() if not code
    )
    if unmapped:
        raise SystemExit(f"no country code for {unmapped}")
    rows = {}
    for column, mapping in (("iso3", by_iso3), ("ucdb", by_ucdb)):
        for value, code in sorted(mapping.items()):
            row = rows.setdefault(code, dict.fromkeys(country_codes.COLUMNS, ""))
            if row[column]:
                raise SystemExit(f"{code}: {column} {row[column]!r} and {value!r}")
            row.update({"code": code, column: value})
    with open(args.out, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, country_codes.COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows[code] for code in sorted(rows))
    votes = collections.defaultdict(collections.Counter)
    names, _ = ucdb.load_names(args.cache_dir)
    for region_id, row in names.items():
        code = by_iso3.get(regions[region_id]["country"])
        if code:
            votes[code][by_ucdb.get(row["country"])] += 1
    agree = sum(counts[code] for code, counts in votes.items())
    total = sum(sum(counts.values()) for counts in votes.values())
    print(f"{len(rows)} codes; {agree} of {total} named centres agree")
    for code, counts in sorted(votes.items()):
        if counts[code] != sum(counts.values()):
            print(f"{code}: {dict(counts.most_common())}")


if __name__ == "__main__":
    main()
