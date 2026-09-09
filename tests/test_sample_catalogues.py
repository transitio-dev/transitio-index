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
