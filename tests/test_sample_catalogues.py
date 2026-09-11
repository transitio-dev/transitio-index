"""Unit tests for the sample-catalogue cutting recipe.

The recipe is a standalone maintainer script under ``scripts/``, so it is
imported by path. Its downloads and the build run are exercised elsewhere;
these cover the pure cutting logic — country filtering, the Atlas URL/host
match, header validation and the tar-member safety guard.
"""

import importlib.util
import io
import json
import tarfile
from pathlib import Path

import pytest

from transitio_index import atlas

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


def test_atlas_limit_trims_to_matching_feeds_and_caps_the_total(tmp_path):
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
    # Without a limit the whole feed-dense file is kept.
    whole = sc._select_atlas(archive, urls, hosts)
    assert sum(len(payload["feeds"]) for _, payload in whole) == 3
    # With a limit only matching feeds survive, capped to the limit.
    trimmed = sc._select_atlas(archive, urls, hosts, limit=1)
    assert [f["id"] for _, payload in trimmed for f in payload["feeds"]] == ["f-match"]


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
