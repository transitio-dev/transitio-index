"""Unit tests for the sample-catalogue cutting recipe.

The recipe is a standalone maintainer script under ``scripts/``, so it is
imported by path. Its downloads and the build run are exercised elsewhere;
these cover the pure cutting logic — country filtering, the Atlas URL/host
match, header validation and the tar-member safety guard.
"""

import csv
import importlib.util
import io
import json
import tarfile
from pathlib import Path

import pytest

from transitio_index import atlas, mdb

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "sample_catalogues.py"
_spec = importlib.util.spec_from_file_location("sample_catalogues", _SCRIPT)
sc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sc)


@pytest.mark.parametrize(
    "host,shared",
    [
        ("s3.amazonaws.com", True),
        ("s3.eu-west-1.amazonaws.com", True),
        ("s3-eu-west-1.amazonaws.com", True),
        ("agency-bucket.s3.amazonaws.com", False),
        ("storage.googleapis.com", True),
        ("bucket.storage.googleapis.com", False),
        ("raw.githubusercontent.com", True),
        ("hsl.fi", False),
    ],
)
def test_shared_host_classification(host, shared):
    # A path-style endpoint is shared; a bucket-specific virtual host is not.
    assert sc._is_shared(host) is shared


def _dmfr(feed_id, url):
    return {"feeds": [{"id": feed_id, "spec": "gtfs", "urls": {"static_current": url}}]}


def _archive(path, files):
    with tarfile.open(path, "w:gz") as tar:
        for name, payload in files:
            data = json.dumps(payload).encode("utf-8")
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def test_cut_filters_by_country_and_keeps_matching_atlas(tmp_path):
    mdb = tmp_path / "feeds_v2.csv"
    mdb.write_text(
        "id,location.country_code,urls.direct_download\n"
        "1,FI,https://hsl.fi/gtfs.zip\n"
        "2,ee,https://transport.ee/gtfs.zip\n"  # a lowercase code still matches
        "3,DE,https://bvg.de/gtfs.zip\n"
    )
    _, mdb_rows = sc._select_csv(
        mdb, sc.MDB_COUNTRY, (sc.MDB_COUNTRY, sc.MDB_DOWNLOAD), {"FI", "EE"}
    )
    assert sorted(row["id"] for row in mdb_rows) == ["1", "2"]

    urls, hosts = sc._mdb_targets(mdb_rows)
    archive = tmp_path / "atlas.tar.gz"
    _archive(
        archive,
        [
            ("r/feeds/hsl.fi.dmfr.json", _dmfr("f-a", "https://hsl.fi/gtfs.zip")),
            ("r/feeds/bvg.de.dmfr.json", _dmfr("f-b", "https://bvg.de/gtfs.zip")),
        ],
    )
    kept = sc._select_atlas(archive, urls, hosts)
    out = tmp_path / "atlas_sample.tar.gz"
    sc._write_atlas(out, kept)
    # The DE feed is dropped; the trimmed archive re-parses through the build.
    parsed = atlas.parse(out)
    assert sorted(feed["onestop_id"] for feed in parsed["feeds"]) == ["f-a"]


def test_missing_required_column_fails(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("id,location.country_code\n1,FI\n")  # no download column
    with pytest.raises(SystemExit, match="missing columns"):
        sc._select_csv(bad, sc.MDB_COUNTRY, (sc.MDB_COUNTRY, sc.MDB_DOWNLOAD), {"FI"})


def test_write_atlas_rejects_a_traversing_member_name(tmp_path):
    out = tmp_path / "atlas_sample.tar.gz"
    with pytest.raises(SystemExit, match="unsafe"):
        sc._write_atlas(out, [("..\\..\\evil.dmfr.json", _dmfr("f-x", "https://x/f"))])


def test_missing_country_in_a_multi_country_request_is_reported():
    rows = [{"location.country_code": "FI"}]
    assert sc._missing(rows, sc.MDB_COUNTRY, {"FI", "EE"}) == ["EE"]
    assert sc._missing(rows, sc.MDB_COUNTRY, {"FI"}) == []


def test_build_command_runs_full_pipeline_pinned_to_the_commit():
    commit = "a4d02044f59f954bf3d2fe13b52f7cd1b7e92846"
    atlas_out = Path("out/atlas_sample.tar.gz")
    cmd = sc._build_command(
        atlas_out, Path("out/mdb_sample.csv"), Path("out/gbfs_sample.csv"), commit
    )
    assert "-m transitio_index.build" in cmd
    assert "--stage ingest --downstream" in cmd
    assert f"--commit {commit}" in cmd
    assert str(atlas_out) in cmd  # path separator is platform-dependent


def test_limit_per_country_caps_each_country_in_order():
    rows = [
        {"c": "ES", "id": "a"},
        {"c": "ES", "id": "b"},
        {"c": "ES", "id": "c"},
        {"c": "FR", "id": "d"},
        {"c": "FR", "id": "e"},
    ]
    capped = sc._limit_per_country(rows, "c", 2)
    assert [r["id"] for r in capped] == ["a", "b", "d", "e"]
    # A full country name counts against the same cap as its ISO code.
    mixed = [
        {"c": "ES", "id": "a"},
        {"c": "SPAIN", "id": "b"},
        {"c": "ES", "id": "c"},
    ]
    assert [r["id"] for r in sc._limit_per_country(mixed, "c", 2)] == ["a", "b"]
    # None passes the rows through unchanged.
    assert sc._limit_per_country(rows, "c", None) is rows
    # With a subdivision column the cap is per country *and* subdivision, so a
    # requested spread of states is sampled evenly, not from the first-listed.
    states = [
        {"c": "US", "s": "California", "id": "a"},
        {"c": "US", "s": "California", "id": "b"},
        {"c": "US", "s": "New York", "id": "c"},
        {"c": "US", "s": "Puerto Rico", "id": "d"},
    ]
    capped = sc._limit_per_country(states, "c", 1, within="s")
    assert [r["id"] for r in capped] == ["a", "c", "d"]


@pytest.mark.parametrize(
    "countries, subdivisions, outcome",
    [
        ({"US"}, {"PUERTO RICO", "CALIFORNIA"}, ["a", "c"]),
        ({"US", "CA"}, {"PUERTO RICO"}, r"no MDB feeds for \['CA'\]"),
        ({"US"}, {"PUERTO RICO", "GUAM"}, r"no MDB feeds in subdivisions \['GUAM'\]"),
        ({"US", "CA"}, set(), ["a", "d"]),
    ],
    ids=["spread", "country-outside-subdivisions", "absent-subdivision", "no-filter"],
)
def test_narrow_spreads_the_cap_and_refuses_what_nothing_carries(
    countries, subdivisions, outcome
):
    """With subdivisions the MDB cap is per subdivision (GBFS stays per country);
    a requested country whose feeds all lie outside them, or a subdivision no row
    carries, stops the cut instead of silently thinning the sample. Without
    subdivisions the cap is per country, as before."""
    rows = [
        {sc.MDB_COUNTRY: "US", sc.MDB_SUBDIVISION: "California", "id": "a"},
        {sc.MDB_COUNTRY: "US", sc.MDB_SUBDIVISION: "California", "id": "b"},
        {sc.MDB_COUNTRY: "US", sc.MDB_SUBDIVISION: "Puerto Rico", "id": "c"},
        {sc.MDB_COUNTRY: "CA", sc.MDB_SUBDIVISION: "Ontario", "id": "d"},
    ]
    gbfs = [{sc.GBFS_COUNTRY: "US"}] * 3
    if isinstance(outcome, str):
        with pytest.raises(SystemExit, match=outcome):
            sc._narrow(rows, gbfs, countries, subdivisions, 1)
        return
    mdb, kept_gbfs = sc._narrow(rows, gbfs, countries, subdivisions, 1)
    assert [r["id"] for r in mdb] == outcome and len(kept_gbfs) == 1


def test_a_blank_subdivision_is_refused_before_any_download(monkeypatch):
    monkeypatch.setattr(sc, "_download", lambda *a, **k: pytest.fail("downloaded"))
    with pytest.raises(SystemExit):
        sc.main(["--country", "US", "--subdivision", "  "])


def test_atlas_selection_keeps_only_matching_feeds_of_a_file(tmp_path):
    match_url = "https://match.example/gtfs.zip"
    also_url = "https://also.example/gtfs.zip"
    urls, hosts = {match_url, also_url}, set()
    dense = {
        "feeds": [
            {"id": "f-match", "spec": "gtfs", "urls": {"static_current": match_url}},
            {"id": "f-also", "spec": "gtfs", "urls": {"static_current": also_url}},
            {
                "id": "f-other",
                "spec": "gtfs",
                "urls": {"static_current": "https://x.example/gtfs.zip"},
            },
        ]
    }
    archive = tmp_path / "atlas.tar.gz"
    _archive(archive, [("r/feeds/dense.dmfr.json", dense)])
    # The file keeps its matching feeds only, never the others.
    kept = sc._select_atlas(archive, urls, hosts)
    assert [f["id"] for _, p in kept for f in p["feeds"]] == ["f-match", "f-also"]


def test_exclude_drops_named_rows_and_refuses_an_unknown_id():
    mdb = [{"id": "mdb-1090"}, {"id": "mdb-1"}]
    gbfs = [{"System ID": "seville"}, {"System ID": "tartu"}]
    kept_mdb, kept_gbfs = sc._drop_excluded(mdb, gbfs, {"mdb-1090", "tartu"})
    assert [r["id"] for r in kept_mdb] == ["mdb-1"]
    assert [r["System ID"] for r in kept_gbfs] == ["seville"]
    with pytest.raises(SystemExit, match=r"--exclude \['mdb-9'\] matches no"):
        sc._drop_excluded(mdb, gbfs, {"mdb-9"})
    assert sc._drop_excluded(mdb, gbfs, set()) == (mdb, gbfs)


def _mdb_row(row_id, south, north, west, east):
    return {
        "id": row_id,
        "provider": "P",
        "location.bounding_box.minimum_latitude": str(south),
        "location.bounding_box.maximum_latitude": str(north),
        "location.bounding_box.minimum_longitude": str(west),
        "location.bounding_box.maximum_longitude": str(east),
    }


def test_foreign_bounding_boxes_are_flagged_with_the_exclude_that_drops_them():
    rows = [
        _mdb_row("mdb-1090", 47.3, 55.1, 5.9, 15.0),  # Germany, tagged FI
        _mdb_row("mdb-fi", 60.0, 61.0, 24.0, 25.5),  # Helsinki
        _mdb_row("mdb-wide", 40.0, 70.0, -10.0, 40.0),  # Europe-wide, touches FI
        _mdb_row("mdb-cross", 60.0, 61.0, 170.0, -170.0),  # antimeridian, foreign
        {"id": "mdb-nobox", "provider": "P"},  # no box: not judged
    ]
    err = io.StringIO()
    flagged = sc._warn_foreign_boxes(rows, {"FI", "EE"}, out=err)
    assert flagged == ["mdb-1090", "mdb-cross"]
    assert "--exclude mdb-1090 takes it out of the cut" in err.getvalue()
    # A crossing box is judged on both of its longitude ranges (Aleutians, US).
    aleutians = {"min_lon": 170.0, "max_lon": -160.0, "min_lat": 51.0, "max_lat": 55.0}
    assert sc._touches(aleutians, [sc.COUNTRY_BOXES["US"]])
    # A requested country without a curated box skips the check with a note.
    err = io.StringIO()
    assert sc._warn_foreign_boxes(rows, {"FI", "XK"}, out=err) == []
    assert "no country box for ['XK']" in err.getvalue()


def test_chunks_and_even_split_partition_rows():
    assert sc._chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert sc._chunks([], 3) == [[]]  # an empty country still yields one batch
    # even_split spreads the remainder across the leading groups, in order.
    assert sc._even_split([1, 2, 3, 4, 5], 3) == [[1, 2], [3, 4], [5]]
    assert sc._even_split([1, 2], 3) == [[1], [2], []]  # more parts than rows
    assert sc._even_split([1, 2, 3], 1) == [[1, 2, 3]]


def test_emit_writes_an_mdb_only_sample_when_no_atlas_overlaps(tmp_path, capsys):
    out = tmp_path / "batch-000"
    out.mkdir()
    sample = (
        [{"id": "m1", "location.country_code": "NL"}],
        [{"system_id": "g1", "Country Code": "NL"}],
        [],  # no Atlas DMFR file overlaps the kept feeds
    )
    sc._emit(
        out,
        ["id", "location.country_code"],
        ["system_id", "Country Code"],
        sample,
        {"NL"},
        "c" * 40,
    )
    err = capsys.readouterr().err
    assert "MDB-only" in err  # the fallback note is printed, not an abort
    assert (out / "mdb_sample.csv").exists()
    assert (out / "gbfs_sample.csv").exists()
    # An (empty) archive is still written so the build command's --archive is valid.
    with tarfile.open(out / "atlas_sample.tar.gz") as tar:
        assert tar.getmembers() == []


def test_country_selection_matches_code_and_curated_name_and_rejects_others():
    assert sc._country_code("NETHERLANDS") == "NL"  # a curated name canonicalises
    assert sc._country_code("NL") == "NL"
    # rows carrying the code or the full name both match; a bare code matches
    # only itself.
    assert sc._match_values("NL") == {"NL", "NETHERLANDS"}
    assert sc._match_values("FR") == {"FR"}
    # a full name outside the curated set, or a non-ASCII look-alike, is refused
    # up front rather than passed through as a match value
    assert sc._unrecognized_countries({"NL", "FR", "FRANCE"}) == ["FRANCE"]
    assert sc._unrecognized_countries({"NL", "NETHERLANDS", "MX"}) == []
    assert sc._unrecognized_countries({"ÑL"}) == ["ÑL"]  # non-ASCII, not a code
    # a non-ASCII char must not case-fold into a code ("ß".upper() == "SS")
    assert sc._unrecognized_countries({"ß"}) == ["ß"]


def test_include_pulls_a_named_atlas_feed_and_a_foreign_mdb_row(tmp_path):
    # An Atlas feed carries no country, and an MDB row can be filed under the
    # wrong one: --include reaches both; an id matching nothing is refused.
    archive = tmp_path / "atlas.tar.gz"
    _archive(
        archive,
        [
            ("r/feeds/ovapi.dmfr.json", _dmfr("f-u-nl", "https://gtfs.ovapi.nl/g.zip")),
            ("r/feeds/other.dmfr.json", _dmfr("f-x", "https://x.example/gtfs.zip")),
        ],
    )
    kept = sc._select_atlas(archive, set(), set(), includes={"f-u-nl"})
    assert [f["id"] for _, p in kept for f in p["feeds"]] == ["f-u-nl"]
    mdb = tmp_path / "feeds_v2.csv"
    mdb.write_text(
        "id,location.country_code,urls.direct_download\n"
        "mdb-1,NL,https://a/x.zip\nmdb-1090,FI,https://b/y.zip\nmdb-3,DE,https://c/z.zip\n"
    )
    _, rows = sc._select_csv(
        mdb, sc.MDB_COUNTRY, (sc.MDB_COUNTRY, sc.MDB_DOWNLOAD), {"NL"}, {"mdb-1090"}
    )
    assert sorted(r["id"] for r in rows) == ["mdb-1", "mdb-1090"]
    sc._check_includes({"f-u-nl", "mdb-1090"}, rows, kept)
    with pytest.raises(SystemExit, match=r"--include \['f-nope'\] matches no"):
        sc._check_includes({"f-nope"}, rows, kept)


def test_an_included_row_survives_the_cap_and_the_subdivision_filter():
    pulled = [{"id": "mdb-9", sc.MDB_COUNTRY: "FI"}]
    narrowed = [{"id": "mdb-1", sc.MDB_COUNTRY: "NL"}]
    assert sc._with_included(narrowed, pulled) == narrowed + pulled
    # Already present after narrowing: not duplicated.
    assert sc._with_included(narrowed + pulled, pulled) == narrowed + pulled


def _feed(feed_id, url, spec="gtfs"):
    urls = {"static_current": url} if url is not None else {}
    return {"id": feed_id, "spec": spec, "urls": urls}


def _payload(*feeds):
    return {"feeds": list(feeds)}


def _mdb_gtfs(row_id, country, url, subdivision="", redirect=""):
    return {
        "id": row_id,
        "data_type": "gtfs",
        "status": "deprecated" if redirect else "active",
        "redirect.id": redirect,
        sc.MDB_COUNTRY: country,
        sc.MDB_SUBDIVISION: subdivision,
        sc.MDB_DOWNLOAD: url,
    }


def _partition_inputs(tmp_path, monkeypatch, rows, files):
    """Point ``_download`` at an MDB CSV of ``rows`` with every column the
    ingest reads, an empty GBFS CSV and an Atlas archive of ``files``."""
    mdb_path = tmp_path / "feeds_v2.csv"
    with open(mdb_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, sorted(mdb.REQUIRED_HEADERS), restval="")
        writer.writeheader()
        writer.writerows(rows)
    gbfs_path = tmp_path / "systems.csv"
    gbfs_path.write_text("System ID,Country Code\n", encoding="utf-8")
    atlas_path = tmp_path / "atlas.tar.gz"
    _archive(atlas_path, files)
    monkeypatch.setattr(
        sc, "_download", lambda work, commit: (atlas_path, mdb_path, gbfs_path)
    )


def test_a_partition_stopped_mid_write_publishes_no_map(tmp_path, monkeypatch):
    rows = [
        _mdb_gtfs("m1", "FI", "https://a.fi/1.zip"),
        _mdb_gtfs("m2", "SE", "https://a.se/2.zip"),
    ]
    _partition_inputs(tmp_path, monkeypatch, rows, [])
    calls, write = [], sc._write_atlas

    def write_atlas(out, files):
        calls.append(out)
        if len(calls) == 2:
            raise OSError("disk full")
        write(out, files)

    monkeypatch.setattr(sc, "_write_atlas", write_atlas)
    out = tmp_path / "out"
    with pytest.raises(OSError, match="disk full"):
        sc.main(["--partition", "--out-dir", str(out)])
    assert len(calls) == 2
    assert [p.name for p in out.iterdir() if "partition-" in p.name] == []


@pytest.mark.parametrize(
    "mdb_labels, atlas_labels, message",
    [
        ("ab", "a", r"1 MDB GTFS id\(s\) not in exactly one label: m1 in 2"),
        ("a", "", r"1 Atlas GTFS id\(s\) not in exactly one label: f-1 in 0"),
    ],
    ids=["mdb-in-two-labels", "atlas-in-none"],
)
def test_the_partition_refuses_an_id_not_in_exactly_one_label(
    mdb_labels, atlas_labels, message
):
    # The realtime row r1 shares m1's labels but is not checked.
    rows = [
        _mdb_gtfs("m1", "FI", "https://a.fi/1.zip"),
        {**_mdb_gtfs("r1", "FI", ""), "data_type": "gtfs_rt"},
    ]
    files = [("a.dmfr.json", _payload(_feed("f-1", "https://a.fi/1.zip")))]
    planned = {
        label: (
            [],
            rows if label in mdb_labels else [],
            files if label in atlas_labels else [],
        )
        for label in "ab"
    }
    with pytest.raises(SystemExit, match=message):
        sc._check_partition(planned, rows, files)


@pytest.mark.parametrize(
    "extra, message",
    [
        (["--limit", "5"], "cannot be combined with --limit"),
        (["--subdivision", "Uusimaa"], "cannot be combined with --subdivision"),
        (["--include", "f-x"], "cannot be combined with --include"),
        (["--out-dir", "cache/a\nb"], "--out-dir must not contain a line break"),
    ],
    ids=["limit", "subdivision", "include", "line-break"],
)
def test_the_partition_refuses_bad_flags_before_any_download(
    extra, message, monkeypatch, capsys
):
    monkeypatch.setattr(sc, "_download", lambda *a, **k: pytest.fail("downloaded"))
    with pytest.raises(SystemExit):
        sc.main(["--partition", *extra])
    assert message in capsys.readouterr().err
