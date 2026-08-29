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


def atlas_feed(
    onestop_id, *, spec="gtfs", url=None, name=None, urls=None, operators=None
):
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
        "operators": operators or [],
    }


def operator(name, *feed_ids):
    return {"name": name, "associated_feed_ids": list(feed_ids)}


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
    assert summary["crosswalk_by_method"] == {"url_exact": 1, "same_host": 0, "none": 0}
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


# --- gated same-host ------------------------------------------------------


def test_same_host_name_agreement_makes_a_both_record(tmp_path):
    records, summary = crosswalk.build_records(
        [
            atlas_feed(
                "f-a", url="https://vendor.example/a.zip", name="Petaluma Transit"
            )
        ],
        [
            mdb_feed(
                "mdb-1", url="https://vendor.example/b.zip", provider="Petaluma Transit"
            )
        ],
    )
    both = by_feed_id(records)["f-a"]
    assert both["source"] == "both"
    assert both["mdb_id"] == "mdb-1"
    assert both["aliases"] == ["f-mdb-1"]
    assert both["crosswalk_method"] == "same_host"
    assert both["crosswalk_confidence"] == crosswalk.SAME_HOST_CONFIDENCE
    assert summary["same_host_pairs"] == 1
    assert summary["provisional_links"] == []


def test_same_host_ignores_case_accents_and_legal_suffixes(tmp_path):
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://v.example/a", name="Société de Transport Inc")],
        [mdb_feed("mdb-1", url="https://v.example/b", provider="societe de transport")],
    )
    assert by_feed_id(records)["f-a"]["source"] == "both"


def test_same_host_uses_inline_operator_names(tmp_path):
    records, _ = crosswalk.build_records(
        [
            atlas_feed(
                "f-a",
                url="https://v.example/a",
                operators=[{"name": "Alameda-Contra Costa Transit"}],
            )
        ],
        [mdb_feed("mdb-1", url="https://v.example/b", provider="Alameda Contra Costa")],
    )
    assert by_feed_id(records)["f-a"]["source"] == "both"


def test_same_host_uses_associated_operator_names(tmp_path):
    # The name lives on a top-level operator record associated to the feed.
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://v.example/a")],
        [mdb_feed("mdb-1", url="https://v.example/b", provider="Star Transit")],
        operators=[operator("Star Transit", "f-a")],
    )
    assert by_feed_id(records)["f-a"]["source"] == "both"


def test_different_hosts_do_not_same_host_match(tmp_path):
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://one.example/a", name="Metro")],
        [mdb_feed("mdb-1", url="https://two.example/b", provider="Metro")],
    )
    assert {record["source"] for record in records} == {"atlas", "mdb"}


def test_unrelated_agencies_on_a_vendor_host_are_not_merged(tmp_path):
    # The plan's negative case: a shared vendor host alone is not an identity.
    records, summary = crosswalk.build_records(
        [atlas_feed("f-a", url="https://vendor.example/a", name="Springfield Transit")],
        [
            mdb_feed(
                "mdb-1", url="https://vendor.example/b", provider="Shelbyville Buses"
            )
        ],
    )
    assert {record["source"] for record in records} == {"atlas", "mdb"}
    assert summary["provisional_links"] == []


def test_an_ambiguous_same_host_name_goes_to_provisional_not_merged(tmp_path):
    # One Atlas name agrees with two MDB feeds on the host: unmergeable, so the
    # candidates are recorded for review and nothing is merged.
    records, summary = crosswalk.build_records(
        [atlas_feed("f-a", url="https://vendor.example/a", name="JR East")],
        [
            mdb_feed("mdb-1", url="https://vendor.example/b", provider="JR East"),
            mdb_feed("mdb-2", url="https://vendor.example/c", provider="JR East"),
        ],
    )
    assert {record["feed_id"] for record in records} == {"f-a", "f-mdb-1", "f-mdb-2"}
    assert summary["same_host_pairs"] == 0
    assert {(p["onestop_id"], p["mdb_id"]) for p in summary["provisional_links"]} == {
        ("f-a", "mdb-1"),
        ("f-a", "mdb-2"),
    }
    assert all(p["host"] == "vendor.example" for p in summary["provisional_links"])


def test_url_exact_takes_precedence_over_same_host(tmp_path):
    url = "https://vendor.example/same.zip"
    records, summary = crosswalk.build_records(
        [atlas_feed("f-a", url=url, name="Metro")],
        [mdb_feed("mdb-1", url=url, provider="Metro")],
    )
    assert by_feed_id(records)["f-a"]["crosswalk_method"] == "url_exact"
    assert summary["same_host_pairs"] == 0


def test_rt_feeds_are_not_same_host_matched(tmp_path):
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", spec="gtfs-rt", url="https://v.example/rt", name="Metro")],
        [
            mdb_feed(
                "mdb-1", spec="gtfs-rt", url="https://v.example/rt2", provider="Metro"
            )
        ],
    )
    assert {record["source"] for record in records} == {"atlas", "mdb"}


def test_same_host_matches_a_non_latin_name(tmp_path):
    # A non-Latin name must keep its script: it tokenizes and matches, rather
    # than collapsing to an empty set.
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://v.example/a", name="青森市")],
        [mdb_feed("mdb-1", url="https://v.example/b", provider="青森市")],
    )
    assert by_feed_id(records)["f-a"]["source"] == "both"


def test_non_latin_names_sharing_only_a_digit_do_not_merge(tmp_path):
    # Two unrelated agencies whose names share only "2024" must not merge; the
    # script tokens keep the overlap below the threshold.
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://v.example/a", name="青森バス 2024")],
        [mdb_feed("mdb-1", url="https://v.example/b", provider="弘前バス 2024")],
    )
    assert {record["source"] for record in records} == {"atlas", "mdb"}


def test_same_host_ignores_userinfo_port_and_trailing_dot(tmp_path):
    # The host alone decides sharing; userinfo, port and a trailing dot do not.
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="https://user@Vendor.Example:8080/a", name="Metro")],
        [mdb_feed("mdb-1", url="https://vendor.example./b", provider="Metro")],
    )
    assert by_feed_id(records)["f-a"]["source"] == "both"


def test_cross_field_name_recombination_does_not_match(tmp_path):
    # Merging a feed's several names would let tokens recombine to a false 1.0;
    # kept apart, the best actual name pair scores only 1/3, so nothing merges.
    records, _ = crosswalk.build_records(
        [
            atlas_feed(
                "f-a",
                url="https://v.example/a",
                operators=[{"name": "Alpha Bus"}, {"name": "Beta Rail"}],
            )
        ],
        [
            mdb_feed(
                "mdb-1",
                url="https://v.example/b",
                provider="Alpha Rail",
                name="Beta Bus",
            )
        ],
    )
    assert {record["source"] for record in records} == {"atlas", "mdb"}


def test_an_extra_operator_name_does_not_block_a_valid_match(tmp_path):
    # One operator name agrees exactly; an extra unrelated one must not dilute it.
    records, _ = crosswalk.build_records(
        [
            atlas_feed(
                "f-a",
                url="https://v.example/a",
                operators=[{"name": "Petaluma Transit"}, {"name": "Unrelated Vendor"}],
            )
        ],
        [mdb_feed("mdb-1", url="https://v.example/b", provider="Petaluma Transit")],
    )
    assert by_feed_id(records)["f-a"]["source"] == "both"


def test_sentinel_download_values_do_not_url_match(tmp_path):
    # "N/A" is not a URL, so two feeds carrying it are not the same feed.
    records, _ = crosswalk.build_records(
        [atlas_feed("f-a", url="N/A")], [mdb_feed("mdb-1", url="N/A")]
    )
    assert {record["source"] for record in records} == {"atlas", "mdb"}


def test_summary_counts_candidates_pairs_and_provisional_separately(tmp_path):
    # One host resolves cleanly; another is ambiguous. The raw candidate pool,
    # the accepted pairs and the provisional links are each reported apart.
    records, summary = crosswalk.build_records(
        [
            atlas_feed("f-clean", url="https://one.example/a", name="Metro North"),
            atlas_feed("f-amb", url="https://two.example/a", name="JR East"),
        ],
        [
            mdb_feed("mdb-1", url="https://one.example/b", provider="Metro North"),
            mdb_feed("mdb-2", url="https://two.example/b", provider="JR East"),
            mdb_feed("mdb-3", url="https://two.example/c", provider="JR East"),
        ],
    )
    assert summary["same_host_candidates"] == 5
    assert summary["same_host_pairs"] == 1
    assert len(summary["provisional_links"]) == 2


# --- the stage, end to end ------------------------------------------------

COLUMNS = sorted(mdb.REQUIRED_HEADERS)


def mdb_csv(*rows):
    lines = [",".join(COLUMNS)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in COLUMNS))
    return "\n".join(lines) + "\n"


def atlas_archive(tmp_path, feeds, operators=None):
    body = {"feeds": feeds}
    if operators is not None:
        body["operators"] = operators
    payload = json.dumps(body).encode("utf-8")
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


def test_crosswalk_stage_publishes_provisional_links(tmp_path):
    from index_build import atlas

    cache = tmp_path / "cache"
    # One Atlas name agrees with two MDB feeds on the same host -> ambiguous.
    atlas.ingest(
        cache,
        archive=atlas_archive(
            tmp_path,
            [
                {
                    "id": "f-a",
                    "spec": "gtfs",
                    "name": "JR East",
                    "urls": {"static_current": "https://vendor.example/a"},
                }
            ],
        ),
        commit="a" * 40,
    )
    mdb.ingest(
        cache,
        csv_path=_write(
            tmp_path / "m.csv",
            mdb_csv(
                {
                    "id": "mdb-1",
                    "data_type": "gtfs",
                    "provider": "JR East",
                    "urls.direct_download": "https://vendor.example/b",
                },
                {
                    "id": "mdb-2",
                    "data_type": "gtfs",
                    "provider": "JR East",
                    "urls.direct_download": "https://vendor.example/c",
                },
            ),
        ),
    )

    manifest = crosswalk.crosswalk(cache)
    assert manifest["provisional_links"] == 2

    generation, _ = store.resolve(cache / "crosswalk", "feeds.json")
    with generation:
        provisional = [
            json.loads(line)
            for line in generation.read_bytes("provisional_links.jsonl")
            .decode()
            .split("\n")
            if line
        ]
    assert {p["mdb_id"] for p in provisional} == {"mdb-1", "mdb-2"}


def test_read_atlas_reads_feeds_and_operators_from_one_generation(tmp_path):
    from index_build import atlas

    cache = tmp_path / "cache"
    atlas.ingest(
        cache,
        archive=atlas_archive(
            tmp_path,
            [{"id": "f-a", "spec": "gtfs", "urls": {"static_current": "u"}}],
            operators=[
                {
                    "onestop_id": "o-x",
                    "name": "X Transit",
                    "associated_feeds": [{"feed_onestop_id": "f-a"}],
                }
            ],
        ),
        commit="a" * 40,
    )
    feeds, operators = crosswalk._read_atlas(cache)
    assert [feed["onestop_id"] for feed in feeds] == ["f-a"]
    # Both artifacts come from the one resolved generation, so the operator's
    # association names a feed that generation actually contains.
    assert operators and operators[0]["name"] == "X Transit"
    assert operators[0]["associated_feed_ids"] == ["f-a"]


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
