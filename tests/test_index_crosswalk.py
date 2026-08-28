import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "build_index.py"

sys.path.insert(0, str(REPO / "scripts"))

from index_build import crosswalk, mdb, store  # noqa: E402


@pytest.fixture(params=["descriptor", "paths"], autouse=True)
def addressing(request, monkeypatch, tmp_path):
    """Run every test in both the descriptor and the path-fallback mode."""
    if request.param == "paths":
        monkeypatch.setattr(store, "HAVE_DIR_FD", False)
        monkeypatch.setattr(store, "O_NOFOLLOW", 0)
    elif os.name == "posix":
        with store.open_directory(tmp_path / ".probe") as directory:
            assert directory.fd is not None


def atlas_feed(onestop_id, *, spec="gtfs", url=None, name=None, urls=None):
    if urls is None:
        urls = {}
        if url is not None:
            key = "static_current" if spec == "gtfs" else "realtime_trip_updates"
            urls[key] = url
    return {
        "source": "atlas",
        "onestop_id": onestop_id,
        "spec": spec,
        "urls": urls,
        "name": name,
    }


def mdb_feed(mdb_id, *, spec="gtfs", url=None, name=None, provider=None):
    urls = {"direct_download": url} if url is not None else {}
    return {
        "source": "mdb",
        "mdb_id": mdb_id,
        "spec": spec,
        "urls": urls,
        "name": name,
        "provider": provider,
    }


def by_feed_id(records):
    return {record["feed_id"]: record for record in records}


# --- url-exact identity ---------------------------------------------------


def test_url_exact_match_makes_one_both_record(tmp_path):
    url = "https://example.org/gtfs.zip"
    records, summary = crosswalk.build_records(
        [atlas_feed("f-a", url=url, name="A Transit")],
        [mdb_feed("mdb-7", url=url, provider="A")],
    )

    assert len(records) == 1
    both = records[0]
    assert both["source"] == "both"
    assert both["feed_id"] == "f-a"
    assert both["onestop_id"] == "f-a"
    assert both["mdb_id"] == "mdb-7"
    assert both["aliases"] == ["f-mdb-7"]
    assert both["id_minted"] is False
    assert both["crosswalk_method"] == "url_exact"
    assert both["crosswalk_confidence"] == 1.0
    assert both["name"] == "A Transit"
    # The contributing rows are kept verbatim for downstream stages.
    assert both["atlas"]["onestop_id"] == "f-a"
    assert both["mdb"]["mdb_id"] == "mdb-7"
    assert summary["crosswalk_by_method"] == {"url_exact": 1, "none": 0}
    assert summary["feeds_by_source"] == {"atlas": 0, "mdb": 0, "both": 1}


def test_unmatched_feeds_are_kept_separate_and_minted(tmp_path):
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://a.example/gtfs.zip")],
        [mdb_feed("mdb-9", url="https://b.example/gtfs.zip", name="B")],
    )
    found = by_feed_id(records)

    assert set(found) == {"f-a", "f-mdb-9"}
    assert found["f-a"]["source"] == "atlas"
    assert found["f-a"]["crosswalk_method"] == "none"
    assert found["f-a"]["id_minted"] is False
    assert found["f-mdb-9"]["source"] == "mdb"
    assert found["f-mdb-9"]["id_minted"] is True
    assert found["f-mdb-9"]["onestop_id"] is None


def test_mint_strips_redundant_mdb_prefix_and_keeps_slugs(tmp_path):
    records, _ = crosswalk.build_records(
        [],
        [mdb_feed("mdb-1"), mdb_feed("jbda-town-bus")],
    )
    minted = {record["mdb_id"]: record["feed_id"] for record in records}
    assert minted["mdb-1"] == "f-mdb-1"
    assert minted["jbda-town-bus"] == "f-mdb-jbda-town-bus"


def test_both_record_alias_uses_the_stripped_mint(tmp_path):
    url = "https://example.org/gtfs.zip"
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url=url)], [mdb_feed("mdb-42", url=url)]
    )
    assert records[0]["aliases"] == ["f-mdb-42"]


# --- ambiguity is left unresolved ----------------------------------------


def test_a_url_shared_by_two_mdb_feeds_is_not_an_identity(tmp_path):
    url = "https://vendor.example/gtfs.zip"
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url=url)],
        [mdb_feed("mdb-1", url=url), mdb_feed("mdb-2", url=url)],
    )
    found = by_feed_id(records)
    # No merge: the Atlas feed stays on its own, both MDB feeds minted.
    assert set(found) == {"f-a", "f-mdb-1", "f-mdb-2"}
    assert found["f-a"]["source"] == "atlas"
    assert found["f-a"]["crosswalk_method"] == "none"


def test_a_url_shared_by_two_atlas_feeds_is_not_an_identity(tmp_path):
    url = "https://vendor.example/gtfs.zip"
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url=url), atlas_feed("f-b", url=url)],
        [mdb_feed("mdb-1", url=url)],
    )
    found = by_feed_id(records)
    assert set(found) == {"f-a", "f-b", "f-mdb-1"}
    assert all(found[fid]["crosswalk_method"] == "none" for fid in found)


def test_rt_feeds_are_never_url_matched(tmp_path):
    url = "https://example.org/rt"
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", spec="gtfs-rt", url=url)],
        [mdb_feed("mdb-1", spec="gtfs-rt", url=url)],
    )
    found = by_feed_id(records)
    # A GTFS-RT URL match is one-to-many upstream, so identity is not asserted.
    assert set(found) == {"f-a", "f-mdb-1"}
    assert found["f-a"]["source"] == "atlas"
    assert found["f-mdb-1"]["source"] == "mdb"


# --- exactness of the URL match ------------------------------------------


def test_match_is_exact_scheme_and_slash_sensitive(tmp_path):
    records, _ = crosswalk.build_records(
        [
            atlas_feed("f-http", url="http://example.org/gtfs.zip"),
            atlas_feed("f-slash", url="https://example.org/gtfs.zip/"),
        ],
        [mdb_feed("mdb-1", url="https://example.org/gtfs.zip")],
    )
    # http != https, and a trailing slash is a different path: neither matches.
    assert all(record["source"] != "both" for record in records)


def test_whitespace_differing_urls_do_not_match(tmp_path):
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="  https://example.org/gtfs.zip  ")],
        [mdb_feed("mdb-1", url="https://example.org/gtfs.zip")],
    )
    # url_exact is byte-identical; a whitespace difference is not a match.
    assert all(record["source"] != "both" for record in records)


def test_a_feed_with_no_or_blank_download_url_is_never_matched(tmp_path):
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", urls={}), atlas_feed("f-b", url="   ")],
        [mdb_feed("mdb-1", url=None), mdb_feed("mdb-2", url="   ")],
    )
    # A blank URL is not an identity, so the two blank feeds are not merged.
    assert {record["source"] for record in records} == {"atlas", "mdb"}


# --- completeness ---------------------------------------------------------


def test_every_feed_appears_exactly_once_with_a_unique_id(tmp_path):
    shared = "https://example.org/shared.zip"
    atlas_feeds = [
        atlas_feed("f-a", url=shared),
        atlas_feed("f-b", url="https://example.org/b.zip"),
        atlas_feed("f-rt", spec="gtfs-rt", url="https://example.org/rt"),
    ]
    mdb_feeds = [
        mdb_feed("mdb-1", url=shared),
        mdb_feed("mdb-2", url="https://example.org/m2.zip"),
    ]
    records, summary = crosswalk.build_records(atlas_feeds, mdb_feeds)

    ids = [record["feed_id"] for record in records]
    assert len(ids) == len(set(ids))
    # 3 atlas + 2 mdb feeds, one pair merged -> 4 records.
    assert summary["feeds"] == 4
    assert summary["feeds_by_source"] == {"atlas": 2, "mdb": 1, "both": 1}


# --- identity is refused when ambiguous ----------------------------------


def test_duplicate_atlas_id_is_refused(tmp_path):
    with pytest.raises(crosswalk.CrosswalkError, match="appears more than once"):
        crosswalk.build_records([atlas_feed("f-a"), atlas_feed("f-a")], [])


def test_duplicate_mdb_id_is_refused(tmp_path):
    with pytest.raises(crosswalk.CrosswalkError, match="appears more than once"):
        crosswalk.build_records([], [mdb_feed("mdb-1"), mdb_feed("mdb-1")])


def test_a_minted_id_colliding_with_an_alias_is_refused(tmp_path):
    # "mdb-1" mints/aliases f-mdb-1; a slug id "1" mints f-mdb-1 too. The
    # namespace collision is refused rather than published.
    url = "https://example.org/gtfs.zip"
    with pytest.raises(crosswalk.CrosswalkError, match="more than one feed"):
        crosswalk.build_records(
            [atlas_feed("f-a", url=url)],
            [mdb_feed("mdb-1", url=url), mdb_feed("1")],
        )


def test_a_canonical_id_colliding_with_a_minted_id_is_refused(tmp_path):
    # An Atlas feed whose Onestop ID is literally "f-mdb-1" collides with the
    # id minted for MDB "mdb-1"; the two are different feeds, so it is refused.
    with pytest.raises(crosswalk.CrosswalkError, match="more than one feed"):
        crosswalk.build_records([atlas_feed("f-mdb-1")], [mdb_feed("mdb-1")])


def test_a_match_whose_onestop_id_is_the_mint_has_no_self_alias(tmp_path):
    # If the matched Atlas Onestop ID already equals the minted id, the alias
    # would be a redundant self-reference; it is dropped, not a collision.
    url = "https://example.org/gtfs.zip"
    records, _ = crosswalk.build_records(
        [atlas_feed("f-mdb-1", url=url)], [mdb_feed("mdb-1", url=url)]
    )
    assert len(records) == 1
    assert records[0]["feed_id"] == "f-mdb-1"
    assert records[0]["aliases"] == []


# --- the stage, end to end ------------------------------------------------

COLUMNS = sorted(mdb.REQUIRED_HEADERS)


def mdb_csv(*rows):
    lines = [",".join(COLUMNS)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in COLUMNS))
    return "\n".join(lines) + "\n"


def atlas_archive(tmp_path, feeds):
    payload = json.dumps({"feeds": feeds}).encode("utf-8")
    path = tmp_path / "atlas.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo("transitland-atlas-x/feeds/x.dmfr.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return path


def test_crosswalk_stage_over_ingested_catalogues(tmp_path):
    from index_build import atlas

    cache = tmp_path / "cache"
    url = "https://example.org/gtfs.zip"
    atlas.ingest(
        cache,
        archive=atlas_archive(
            tmp_path,
            [
                {"id": "f-a", "spec": "gtfs", "urls": {"static_current": url}},
                {"id": "f-b", "spec": "gtfs", "urls": {"static_current": "https://x"}},
            ],
        ),
        commit="a" * 40,
    )
    mdb.ingest(
        cache,
        csv_path=_write(
            tmp_path / "m.csv",
            mdb_csv(
                {"id": "mdb-1", "data_type": "gtfs", "urls.direct_download": url},
                {"id": "mdb-2", "data_type": "gtfs"},
            ),
        ),
    )

    manifest = crosswalk.crosswalk(cache)
    assert manifest["url_exact_pairs"] == 1

    generation, _ = store.resolve(cache / "crosswalk", "feeds.json")
    with generation:
        records = [
            json.loads(line)
            for line in generation.read_bytes("feeds.jsonl").decode().splitlines()
        ]
    found = by_feed_id(records)
    assert found["f-a"]["source"] == "both"
    assert found["f-a"]["mdb_id"] == "mdb-1"
    assert "f-mdb-2" in found
    assert "f-b" in found


def test_a_line_separator_in_a_name_survives_the_read(tmp_path):
    from index_build import atlas

    cache = tmp_path / "cache"
    # U+2028/U+2029 are written raw by ensure_ascii=False; splitting on them
    # would break the record in two.
    name = "Metro Transit Line"
    atlas.ingest(
        cache,
        archive=atlas_archive(
            tmp_path,
            [
                {
                    "id": "f-a",
                    "spec": "gtfs",
                    "name": name,
                    "urls": {"static_current": "u"},
                }
            ],
        ),
        commit="a" * 40,
    )
    mdb.ingest(
        cache,
        csv_path=_write(
            tmp_path / "m.csv", mdb_csv({"id": "mdb-1", "data_type": "gtfs"})
        ),
    )

    crosswalk.crosswalk(cache)
    generation, _ = store.resolve(cache / "crosswalk", "feeds.json")
    with generation:
        records = [
            json.loads(line)
            for line in generation.read_bytes("feeds.jsonl").decode().split("\n")
            if line
        ]
    found = by_feed_id(records)
    # The record is intact, not split into two by the line separators.
    assert found["f-a"]["name"] == name


def test_cli_runs_the_crosswalk_stage(tmp_path):
    from index_build import atlas

    cache = tmp_path / "cache"
    url = "https://example.org/gtfs.zip"
    atlas.ingest(
        cache,
        archive=atlas_archive(
            tmp_path, [{"id": "f-a", "spec": "gtfs", "urls": {"static_current": url}}]
        ),
        commit="a" * 40,
    )
    mdb.ingest(
        cache,
        csv_path=_write(
            tmp_path / "m.csv",
            mdb_csv({"id": "mdb-1", "data_type": "gtfs", "urls.direct_download": url}),
        ),
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--stage",
            "crosswalk",
            "--cache-dir",
            str(cache),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    summary = json.loads(completed.stdout)
    assert summary["source"] == "crosswalk"
    assert summary["url_exact_pairs"] == 1


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return path
