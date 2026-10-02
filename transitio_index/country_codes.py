"""The index's country codes for the countries the FAO and GHS-UCDB inputs name.

``country_codes.csv``, written by ``scripts/country_codes.py`` from the
pinned inputs and Debian's iso-codes, maps each country code the index
partitions by (Overture's ISO 3166-1 alpha-2, ``XK`` for Kosovo) to FAO's
ISO3 code and the UCDB country name (``GC_CNT_GAD_2025``, spelt as the
source spells it); blank where an input names none. The UCDB's Palestine,
Western Sahara and Northern Cyprus, and FAO's codes for them, are left out:
the index partitions them otherwise. ``FAO_REGIONS`` gives the regions
centred in Hong Kong, Macao and the West Bank, which the inputs file under
China or Palestine, their own codes.
"""

import functools
import pathlib

from transitio_index import csv_source

TABLE = pathlib.Path(__file__).with_name("country_codes.csv")
COLUMNS = ("code", "iso3", "ucdb")

# Region id -> code: Hong Kong, Macao and the West Bank.
FAO_REGIONS = {
    **dict.fromkeys("289 290 293 9087 25163 25257".split(), "HK"),
    "9114": "MO",
    **dict.fromkeys(
        "165 1601 6239 6279 6300 6387 18785 18808 18838 18930 18931 19004 19010"
        " 19097 19107 19112 19141 19167 19174 19179".split(),
        "XW",
    ),
}


@functools.cache
def _lookups():
    """``(by UCDB name, by ISO3)`` from the table."""
    rows = csv_source.read_rows(TABLE.read_text(encoding="utf-8"), COLUMNS)
    return (
        {row["ucdb"]: row["code"] for row in rows if row["ucdb"]},
        {row["iso3"]: row["code"] for row in rows if row["iso3"]},
    )


def by_ucdb(name):
    """The code of a UCDB country name, None when unmapped."""
    return _lookups()[0].get(name)


def by_iso3(code):
    """The code of a FAO ISO3 code, None when unmapped."""
    return _lookups()[1].get(code)
