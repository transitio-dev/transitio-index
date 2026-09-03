import io
import json
import os
import sys
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import atlas, crosswalk, gbfs, mdb, store  # noqa: E402

# Skip only if pyarrow is genuinely absent; then import publish directly, so an
# internal ImportError in the publish code fails loudly rather than skipping.
pytest.importorskip("pyarrow")
import shapely  # noqa: E402

from index_build import publish  # noqa: E402
from index_build import classify  # noqa: E402
from index_build import overrides  # noqa: E402

# Import the read layer directly too, so an old installed transitio shadowing
# the source (which would lack it) fails loudly rather than skipping the module.
from transitio import index as transitio_index  # noqa: E402
from transitio.exceptions import IncompatibleIndexError  # noqa: E402


@pytest.fixture(params=["descriptor", "paths"], autouse=True)
def addressing(request, monkeypatch, tmp_path):
    if request.param == "paths":
        monkeypatch.setattr(store, "HAVE_DIR_FD", False)
        monkeypatch.setattr(store, "O_NOFOLLOW", 0)
    elif os.name == "posix":
        with store.open_directory(tmp_path / ".probe") as directory:
            assert directory.fd is not None


MDB_COLUMNS = sorted(mdb.REQUIRED_HEADERS)
GBFS_COLUMNS = sorted(gbfs.REQUIRED_HEADERS)


def _csv(columns, rows):
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in columns))
    return "\n".join(lines) + "\n"


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return path


def _atlas_archive(tmp_path, feeds):
    payload = json.dumps({"feeds": feeds}).encode("utf-8")
    path = tmp_path / "atlas.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo("transitland-atlas-x/feeds/x.dmfr.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return path


def _publish_gen(cache, pointer, artifact, records, manifest):
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory):
            return store.publish(
                cache / "gazetteer",
                pointer,
                {artifact: store.jsonl_chunks(records)},
                manifest,
                held=directory,
            )
    finally:
        directory.close()


def _place(place_id, kind, *, geometry=None, **kw):
    return {
        "place_id": place_id,
        "kind": kind,
        "source_subtype": kw.get("source_subtype", kind),
        "name": kw.get("name", place_id),
        "names": kw.get("names", {"en": place_id}),
        "aliases": kw.get("aliases", []),
        "resolution_method": kw.get("resolution_method", "overture_wikidata"),
        "default_metro_id": kw.get("default_metro_id"),
        "parent_id": kw.get("parent_id"),
        "country_code": kw.get("country_code", "FI"),
        "overture_id": kw.get("overture_id"),
        "osm_relation_id": None,
        "statistical_area_id": kw.get("statistical_area_id"),
        "metro_ids": kw.get("metro_ids", []),
        "member_ids": kw.get("member_ids", []),
        "geometry": geometry,
        "geometry_source": "overture" if geometry else None,
    }


GEOM_HEX = shapely.to_wkb(shapely.box(24.9, 60.1, 25.1, 60.3)).hex()
PLACES = [
    _place(
        "Q1757",
        "city",
        geometry=GEOM_HEX,
        names={"en": "Helsinki", "sv": "Helsingfors"},
        aliases=["Stadi"],
        metro_ids=["Q-metro"],
    ),
    _place("Q-metro", "metro", member_ids=["Q1757"]),  # no geometry
]


SOURCES = {
    "atlas": {"commit": "a" * 40, "archive_sha256": "x"},
    "mdb": {"csv_sha256": "y"},
    "gbfs": {"csv_sha256": "z"},
}


def _covered_feed(feed_id, **kw):
    return {
        "feed_id": feed_id,
        "onestop_id": kw.get("onestop_id"),
        "mdb_id": None,
        "id_minted": kw.get("id_minted", False),
        "source": "atlas",
        "spec": kw.get("spec", "gtfs"),
        "name": kw.get("name", feed_id),
        "aliases": [],
        "crosswalk_method": "url_exact",
        "crosswalk_confidence": 1.0,
        "crawlable": kw.get("crawlable", True),
        "uncrawlable_reason": None,
        "coverage_source": kw.get("coverage_source", "declared"),
    }


def _edge(place_id, feed_id, **kw):
    return {
        "place_id": place_id,
        "feed_id": feed_id,
        "tier": kw.get("tier", "unknown"),
        "service": kw.get("service"),
        "tier_confidence": 0.0,
        "method": "inferred",
        "rehomed_from": [],
        "evidence": {"declared_level": "municipality", "declared_place_id": place_id},
        "curation": None,
        "merged_evidence": [],
        "curation_history": [],
        "classification_fingerprint": None,
        "fingerprint_kind": "none",
        "selector_state": "unavailable",
        "selector": None,
        "needs_review": True,
    }


def _edges_index(
    tmp_path, edges, feeds=None, release="2026-08-19.0", places=None, stale=(0, 0)
):
    """A cache with expanded places and a coverage generation, published;
    ``stale`` is the (place, feed) override staleness those record."""
    cache = tmp_path / "cache"
    expanded = _publish_gen(
        cache,
        "expanded.json",
        "places_expanded.jsonl",
        PLACES if places is None else places,
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "overture_release": release,
            "stale_place_overrides": stale[0],
        },
    )
    if feeds is None:
        feeds = [_covered_feed("f-a"), _covered_feed("f-b", coverage_source=None)]
    directory = store.open_subdir(cache, "coverage")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "coverage",
                "coverage.json",
                {
                    "feeds_covered.jsonl": store.jsonl_chunks(feeds),
                    "edges_candidate.jsonl": store.jsonl_chunks(edges),
                },
                {
                    "source": "coverage",
                    "feeds_overrides_sha256": None,
                    "mode": "declared",
                    "sources": SOURCES,
                    "overture_release": "2026-08-19.0",
                    "expanded_generation": expanded["generation"],
                    "stale_feed_overrides": stale[1],
                },
                held=directory,
            )
    finally:
        directory.close()
    classify.classify(cache)
    manifest = publish.publish(cache)
    return cache, manifest


def _build_index(tmp_path, archive=None, places=None, release="2026-08-19.0"):
    """Ingest a small fixture through crosswalk and publish; return the cache."""
    cache = tmp_path / "cache"
    url = "https://example.org/gtfs.zip"
    if archive is None:
        archive = _atlas_archive(
            tmp_path,
            [
                {
                    "id": "f-a",
                    "spec": "gtfs",
                    "name": "A",
                    "urls": {"static_current": url},
                },
                {
                    "id": "f-b",
                    "spec": "gtfs",
                    "urls": {"static_current": "https://b.zip"},
                },
            ],
        )
    atlas.ingest(cache, archive=archive, commit="a" * 40)
    mdb.ingest(
        cache,
        csv_path=_write(
            tmp_path / "m.csv",
            _csv(
                MDB_COLUMNS,
                [
                    {"id": "mdb-1", "data_type": "gtfs", "urls.direct_download": url},
                    {"id": "mdb-2", "data_type": "gtfs"},
                ],
            ),
        ),
    )
    gbfs.ingest(
        cache,
        csv_path=_write(tmp_path / "s.csv", _csv(GBFS_COLUMNS, [{"System ID": "sys"}])),
    )
    crosswalk.crosswalk(cache)
    if places is not None:
        _publish_gen(
            cache,
            "names.json",
            "places_seed.jsonl",
            places,
            {
                "source": "names",
                "places_overrides_sha256": None,
                "overture_release": release,
            },
        )
    manifest = publish.publish(cache)
    return cache, manifest


def test_publish_round_trips_through_the_reader(tmp_path):
    cache, manifest = _build_index(tmp_path)
    index = transitio_index.read_index(cache / "index")

    assert index.snapshot_id == manifest["snapshot_id"]
    assert index.schema_version == publish.SCHEMA_VERSION
    # f-a is url-matched to mdb-1; f-b, f-mdb-2 and the GBFS system stand alone.
    feed_ids = set(index.feeds["feed_id"])
    assert {"f-a", "f-b", "f-mdb-2", "f-gbfs-sys"} <= feed_ids
    both = index.feeds[index.feeds["feed_id"] == "f-a"].iloc[0]
    assert both["source"] == "both"
    assert both["crosswalk_method"] == "url_exact"
    assert both["mdb_id"] == "mdb-1"
    assert list(both["aliases"]) == ["f-mdb-1"]
    assert both["snapshot"] == manifest["snapshot_id"]


def test_snapshot_manifest_records_sources_and_counts(tmp_path):
    _, manifest = _build_index(tmp_path)
    assert manifest["sources"]["atlas"]["commit"] == "a" * 40
    assert manifest["sources"]["mdb"]["csv_sha256"]
    assert manifest["sources"]["gbfs"]["csv_sha256"]
    assert manifest["counts"]["feeds"] == 4
    assert manifest["counts"]["by_source"] == {
        "atlas": 1,
        "both": 1,
        "mdb": 1,
        "systems_csv": 1,
    }
    assert manifest["counts"]["by_spec"] == {"gtfs": 3, "gbfs": 1}


def test_the_snapshot_id_is_deterministic_in_the_sources(tmp_path):
    # The same source bytes must yield the same id; reuse one archive so its
    # gzip-wrapper timestamp is identical across the two builds.
    archive = _atlas_archive(
        tmp_path, [{"id": "f-a", "spec": "gtfs", "urls": {"static_current": "u"}}]
    )
    _, first = _build_index(tmp_path / "one", archive=archive)
    _, second = _build_index(tmp_path / "two", archive=archive)
    assert first["snapshot_id"] == second["snapshot_id"]


def test_the_verbatim_source_blocks_round_trip_as_json(tmp_path):
    cache, _ = _build_index(tmp_path)
    index = transitio_index.read_index(cache / "index")
    both = index.feeds[index.feeds["feed_id"] == "f-a"].iloc[0]
    assert json.loads(both["atlas"])["onestop_id"] == "f-a"
    assert json.loads(both["mdb"])["mdb_id"] == "mdb-1"
    system = index.feeds[index.feeds["feed_id"] == "f-gbfs-sys"].iloc[0]
    assert json.loads(system["gbfs"])["system_id"] == "sys"


def test_a_minted_feed_has_a_null_onestop_id(tmp_path):
    import pandas

    cache, _ = _build_index(tmp_path)
    index = transitio_index.read_index(cache / "index")
    minted = index.feeds[index.feeds["feed_id"] == "f-mdb-2"].iloc[0]
    assert pandas.isna(minted["onestop_id"])
    assert minted["id_minted"]


def test_the_reader_refuses_an_unsupported_schema_version(tmp_path):
    index_dir = tmp_path / "index"
    index_dir.mkdir()
    (index_dir / "snapshot.json").write_text(
        json.dumps({"schema_version": 999, "snapshot_id": "x"}), encoding="utf-8"
    )
    with pytest.raises(IncompatibleIndexError, match="schema_version"):
        transitio_index.read_index(index_dir)


def test_the_reader_refuses_a_non_integer_schema_version(tmp_path):
    # ``1.0`` equals ``1`` in Python but is not a valid schema version.
    index_dir = tmp_path / "index"
    index_dir.mkdir()
    (index_dir / "snapshot.json").write_text(
        json.dumps({"schema_version": 1.0, "snapshot_id": "x"}), encoding="utf-8"
    )
    with pytest.raises(IncompatibleIndexError):
        transitio_index.read_index(index_dir)


def test_the_reader_refuses_a_parquet_that_does_not_match_its_manifest(tmp_path):
    cache, _ = _build_index(tmp_path)
    (cache / "index" / "feeds.parquet").write_bytes(b"not the published parquet")
    with pytest.raises(IncompatibleIndexError, match="feeds_sha256"):
        transitio_index.read_index(cache / "index")


def test_the_reader_requires_a_feeds_sha256(tmp_path):
    # A supported-version manifest with no digest cannot bypass the check.
    cache, _ = _build_index(tmp_path)
    snapshot = json.loads((cache / "index" / "snapshot.json").read_text())
    del snapshot["feeds_sha256"]
    (cache / "index" / "snapshot.json").write_text(json.dumps(snapshot))
    with pytest.raises(IncompatibleIndexError, match="feeds_sha256"):
        transitio_index.read_index(cache / "index")


def test_the_reader_refuses_a_symlinked_index_file(tmp_path):
    if not getattr(os, "O_NOFOLLOW", 0):
        pytest.skip("platform lacks O_NOFOLLOW; a symlink is followed (documented)")
    cache, _ = _build_index(tmp_path)
    parquet = cache / "index" / "feeds.parquet"
    real = parquet.rename(parquet.with_name("real.parquet"))
    try:
        parquet.symlink_to(real)
    except (OSError, NotImplementedError):
        pytest.skip("this platform cannot create symlinks")
    with pytest.raises(IncompatibleIndexError, match="cannot read"):
        transitio_index.read_index(cache / "index")


def test_the_reader_refuses_a_fifo_index_file(tmp_path):
    # A FIFO in place of a file must be refused, not block the open forever.
    cache, _ = _build_index(tmp_path)
    parquet = cache / "index" / "feeds.parquet"
    parquet.unlink()
    try:
        os.mkfifo(parquet)
    except (AttributeError, OSError, NotImplementedError):
        pytest.skip("this platform cannot create FIFOs")
    with pytest.raises(IncompatibleIndexError, match="not a regular file"):
        transitio_index.read_index(cache / "index")


def test_the_reader_requires_a_snapshot_id(tmp_path):
    cache, _ = _build_index(tmp_path)
    snap = cache / "index" / "snapshot.json"
    manifest = json.loads(snap.read_text())
    del manifest["snapshot_id"]
    snap.write_text(json.dumps(manifest))
    with pytest.raises(IncompatibleIndexError, match="snapshot_id"):
        transitio_index.read_index(cache / "index")


def test_the_reader_refuses_a_parquet_with_the_wrong_columns(tmp_path):
    # A structurally wrong Parquet whose digest is made to match is still refused.
    import hashlib

    import pyarrow as pa
    import pyarrow.parquet as pq

    cache, _ = _build_index(tmp_path)
    sink = io.BytesIO()
    pq.write_table(pa.table({"unexpected": [1, 2]}), sink)
    data = sink.getvalue()
    (cache / "index" / "feeds.parquet").write_bytes(data)
    snap = cache / "index" / "snapshot.json"
    manifest = json.loads(snap.read_text())
    manifest["feeds_sha256"] = hashlib.sha256(data).hexdigest()
    snap.write_text(json.dumps(manifest))
    with pytest.raises(IncompatibleIndexError, match="columns"):
        transitio_index.read_index(cache / "index")


def test_places_round_trip_through_the_reader(tmp_path):
    pytest.importorskip("geopandas")
    cache, manifest = _build_index(tmp_path, places=PLACES)
    index = transitio_index.read_index(cache / "index")

    assert index.places is not None
    assert len(index.places) == 2
    by_id = {row["place_id"]: row for _, row in index.places.iterrows()}
    helsinki = by_id["Q1757"]
    assert helsinki["kind"] == "city"
    assert helsinki.geometry.area > 0  # the boundary round-tripped
    assert dict(helsinki["names"])["sv"] == "Helsingfors"  # a map column
    assert list(helsinki["aliases"]) == ["Stadi"]
    assert helsinki["default_metro_id"] == "Q-metro"  # the sole metro
    # The metro has no geometry here.
    assert by_id["Q-metro"].geometry is None
    assert manifest["places_sha256"]
    assert manifest["overture_release"] == "2026-08-19.0"
    assert manifest["counts"]["places"] == 2
    assert manifest["counts"]["places_by_kind"] == {"city": 1, "metro": 1}


def test_a_feeds_only_index_has_no_places(tmp_path, monkeypatch):
    from test_index_place_overrides import write_overrides

    # Tables left by an earlier build must not linger under a new manifest.
    (tmp_path / "cache" / "index").mkdir(parents=True)
    for stale in ("places.parquet", "edges.parquet"):
        (tmp_path / "cache" / "index" / stale).write_bytes(b"stale")
    cache, manifest = _build_index(tmp_path)  # no gazetteer
    assert "places_sha256" not in manifest
    index = transitio_index.read_index(cache / "index")
    assert index.places is None and index.edges is None
    assert not (cache / "index" / "places.parquet").exists()
    assert not (cache / "index" / "edges.parquet").exists()
    # A places.yaml that no gazetteer generation applied is a missing stage.
    directory = write_overrides(
        tmp_path, places=[{"place": "Q1", "set_aliases": ["x"]}]
    )
    with pytest.raises(publish.PublishError, match="run the gazetteer stage"):
        publish.publish(cache, overrides_dir=directory)
    # Created after the feeds-only read: refused at activation.
    late = tmp_path / "late" / "overrides"
    late.mkdir(parents=True)

    def read_then_write(overrides_dir):
        write_overrides(
            tmp_path / "late", places=[{"place": "Q1", "set_aliases": ["x"]}]
        )
        return None, None, None, None

    monkeypatch.setattr(publish, "_no_places", read_then_write)
    with pytest.raises(publish.PublishError, match="changed during publication"):
        publish.publish(cache, overrides_dir=late)
    # The same for a feeds.yaml created after the crosswalk-only read.
    monkeypatch.undo()
    late_feeds = tmp_path / "late_feeds" / "overrides"
    late_feeds.mkdir(parents=True)

    def read_then_write_feeds(cache_dir, overrides_dir, *, check_file=True):
        write_overrides(
            tmp_path / "late_feeds", feeds=[{"feed": "f", "mark_uncrawlable": True}]
        )
        return None, None

    monkeypatch.setattr(publish, "_read_resolved", read_then_write_feeds)
    with pytest.raises(publish.PublishError, match="feeds.yaml changed during"):
        publish.publish(cache, overrides_dir=late_feeds)


def test_the_reader_refuses_a_places_parquet_that_does_not_match(tmp_path):
    pytest.importorskip("geopandas")
    cache, _ = _build_index(tmp_path, places=PLACES)
    (cache / "index" / "places.parquet").write_bytes(b"not the published parquet")
    with pytest.raises(IncompatibleIndexError, match="places_sha256"):
        transitio_index.read_index(cache / "index")


def test_the_snapshot_id_folds_in_the_overture_release(tmp_path):
    # Same feeds (one reused archive), but a places build adds the release to the
    # snapshot id, so it differs from the feeds-only build's id.
    archive = _atlas_archive(
        tmp_path, [{"id": "f-a", "spec": "gtfs", "urls": {"static_current": "u"}}]
    )
    _, feeds_only = _build_index(tmp_path / "a", archive=archive)
    _, with_places = _build_index(tmp_path / "b", archive=archive, places=PLACES)
    assert feeds_only["snapshot_id"] != with_places["snapshot_id"]


def test_the_snapshot_id_reflects_distinct_places_content(tmp_path):
    # Same feeds and same Overture release, but different places (a Wikidata edit
    # the release does not pin) must not collide on one snapshot id.
    pytest.importorskip("geopandas")
    archive = _atlas_archive(
        tmp_path, [{"id": "f-a", "spec": "gtfs", "urls": {"static_current": "u"}}]
    )
    other = [_place("Q1757", "city", geometry=GEOM_HEX, names={"en": "Helsinki"})]
    _, a = _build_index(tmp_path / "a", archive=archive, places=PLACES)
    _, b = _build_index(tmp_path / "b", archive=archive, places=other)
    assert a["snapshot_id"] != b["snapshot_id"]


def test_an_empty_gazetteer_is_a_places_index_not_feeds_only(tmp_path):
    # A gazetteer that ran with zero places is still a places index, distinct
    # from a feeds-only build that never ran the gazetteer.
    pytest.importorskip("geopandas")
    archive = _atlas_archive(
        tmp_path, [{"id": "f-a", "spec": "gtfs", "urls": {"static_current": "u"}}]
    )
    _, feeds_only = _build_index(tmp_path / "a", archive=archive)
    cache, manifest = _build_index(tmp_path / "b", archive=archive, places=[])
    assert manifest["places_sha256"]
    assert manifest["counts"]["places"] == 0
    assert manifest["snapshot_id"] != feeds_only["snapshot_id"]
    index = transitio_index.read_index(cache / "index")
    assert index.places is not None
    assert len(index.places) == 0


def test_an_explicit_default_metro_wins_over_metro_derivation(tmp_path):
    pytest.importorskip("geopandas")
    places = [
        _place("Q-pick", "city", metro_ids=["Q-a", "Q-b"], default_metro_id="Q-b"),
    ]
    cache, _ = _build_index(tmp_path, places=places)
    index = transitio_index.read_index(cache / "index")
    assert index.places.iloc[0]["default_metro_id"] == "Q-b"


def test_places_without_an_overture_release_are_refused(tmp_path):
    with pytest.raises(publish.PublishError, match="overture_release"):
        _build_index(tmp_path, places=PLACES, release="")


def test_edges_round_trip_through_the_reader(tmp_path):
    import pandas

    pytest.importorskip("geopandas")
    edges = [_edge("Q1757", "f-a"), _edge("Q-metro", "f-a")]
    cache, manifest = _edges_index(tmp_path, edges)
    index = transitio_index.read_index(cache / "index")
    assert manifest["stale_overrides"] == 0
    assert manifest["overrides_sha256"] is None

    assert index.edges is not None
    assert len(index.edges) == 2
    row = index.edges.set_index("place_id").loc["Q1757"]
    assert row["feed_id"] == "f-a"
    assert row["tier"] == "unknown"
    assert pandas.isna(row["service"])  # a null column reads None or NaN
    assert row["selector_state"] == "unavailable"
    assert bool(row["needs_review"]) is True
    assert json.loads(row["evidence"])["declared_level"] == "municipality"
    # The feeds come from the coverage generation, stamped with their coverage.

    by_id = {r["feed_id"]: r for _, r in index.feeds.iterrows()}
    assert by_id["f-a"]["coverage_source"] == "declared"
    assert pandas.isna(by_id["f-b"]["coverage_source"])  # null reads back as NaN
    assert manifest["edges_sha256"]
    assert manifest["coverage_mode"] == "declared"
    assert manifest["counts"]["edges"] == 2
    assert manifest["counts"]["edges_by_tier"] == {"unknown": 2}


def test_a_manifest_snapshot_id_that_disagrees_with_the_rows_is_refused(tmp_path):
    cache, _ = _build_index(tmp_path)
    snap_path = cache / "index" / "snapshot.json"
    snapshot = json.loads(snap_path.read_text())
    snapshot["snapshot_id"] = "somethingelse0000"
    snap_path.write_text(json.dumps(snapshot))
    with pytest.raises(IncompatibleIndexError, match="snapshot other than"):
        transitio_index.read_index(cache / "index")


def test_a_feeds_only_index_has_no_edges(tmp_path):
    cache, manifest = _build_index(tmp_path)
    assert "edges_sha256" not in manifest
    index = transitio_index.read_index(cache / "index")
    assert index.edges is None
    assert not (cache / "index" / "edges.parquet").exists()


def test_the_reader_refuses_an_edges_parquet_that_does_not_match(tmp_path):
    pytest.importorskip("geopandas")
    cache, _ = _edges_index(tmp_path, [_edge("Q1757", "f-a")])
    (cache / "index" / "edges.parquet").write_bytes(b"not the published parquet")
    with pytest.raises(IncompatibleIndexError, match="edges_sha256"):
        transitio_index.read_index(cache / "index")


def test_a_duplicate_column_label_is_refused_even_when_correctly_hashed(tmp_path):
    pytest.importorskip("geopandas")
    import hashlib

    import pyarrow as pa
    import pyarrow.parquet as pq

    cache, _ = _edges_index(tmp_path, [_edge("Q1757", "f-a")])
    # Every expected edge column plus a second "tier": a set comparison alone
    # would accept it; the load itself refuses it, as a controlled error.
    from index_build.publish import _EDGES_SCHEMA

    fields = list(_EDGES_SCHEMA) + [pa.field("tier", pa.string())]
    table = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in fields],
        schema=pa.schema(fields),
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    data = sink.getvalue()
    (cache / "index" / "edges.parquet").write_bytes(data)
    snap_path = cache / "index" / "snapshot.json"
    snapshot = json.loads(snap_path.read_text())
    snapshot["edges_sha256"] = hashlib.sha256(data).hexdigest()
    snap_path.write_text(json.dumps(snapshot))
    with pytest.raises(IncompatibleIndexError, match="not a readable edges table"):
        transitio_index.read_index(cache / "index")


def test_an_override_file_edited_during_publication_aborts_it(tmp_path, monkeypatch):
    # The digest is read when the edges are, and again right before the
    # snapshot is written: a file edited in between aborts the activation.
    pytest.importorskip("geopandas")
    cache, _ = _edges_index(tmp_path, [_edge("Q1757", "f-a")])
    # Read when the edges are, then once more at activation: two reads.
    digests = iter([None, "edited"])
    monkeypatch.setattr(overrides, "edges_digest", lambda overrides_dir: next(digests))
    with pytest.raises(publish.PublishError, match="during publication"):
        publish.publish(cache)
    # The abort came before any file was replaced: the previous index reads.
    assert len(transitio_index.read_index(cache / "index").edges) == 1


def test_the_snapshot_id_reflects_distinct_edges_content(tmp_path):
    pytest.importorskip("geopandas")
    _, a = _edges_index(tmp_path / "a", [_edge("Q1757", "f-a")])
    service = {"stops": 1, "routes": 1, "departures_per_day": 2.0}
    _, b = _edges_index(tmp_path / "b", [_edge("Q1757", "f-a", service=service)])
    assert a["snapshot_id"] != b["snapshot_id"]


def test_a_place_sums_the_service_of_the_feeds_serving_it(tmp_path):
    pytest.importorskip("geopandas")
    measured = {"stops": 3, "routes": 2, "departures_per_day": 40.0}
    edges = [
        _edge("Q1757", "f-a", service=measured),
        # The pair's second tier repeats the struct: counted once.
        _edge("Q1757", "f-a", tier="national", service=measured),
        _edge("Q1757", "f-b", service={"stops": 1, "routes": None}),
        # A declared-only place: one feed, nothing measured.
        _edge("Q-metro", "f-b"),
    ]
    cache, _ = _edges_index(tmp_path, edges)
    index = transitio_index.read_index(cache / "index")
    service = transitio_index.place("Q1757", index=index).service
    assert (service.feeds, service.stops, service.routes) == (2, 4, 2)
    assert service.departures_per_day == 40.0
    declared = transitio_index.place("Q-metro", index=index).service
    assert (declared.feeds, declared.stops, declared.departures_per_day) == (
        1,
        None,
        None,
    )


def test_a_feeds_only_snapshot_needs_an_explicit_no_golden(tmp_path):
    cache, _ = _build_index(tmp_path)
    with pytest.raises(publish.PublishError, match="no-golden"):
        publish.publish(cache, golden_path=REPO / "golden" / "feeds.jsonl")


def test_unclassified_edges_are_refused(tmp_path):
    cache = tmp_path / "cache"
    expanded = _publish_gen(
        cache,
        "expanded.json",
        "places_expanded.jsonl",
        PLACES,
        {
            "source": "expand",
            "places_overrides_sha256": None,
            "overture_release": "2026-08-19.0",
        },
    )
    directory = store.open_subdir(cache, "coverage")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "coverage",
                "coverage.json",
                {
                    "feeds_covered.jsonl": store.jsonl_chunks([_covered_feed("f-a")]),
                    "edges_candidate.jsonl": store.jsonl_chunks(
                        [_edge("Q1757", "f-a")]
                    ),
                },
                {
                    "source": "coverage",
                    "feeds_overrides_sha256": None,
                    "mode": "declared",
                    "sources": SOURCES,
                    "overture_release": "2026-08-19.0",
                    "expanded_generation": expanded["generation"],
                },
                held=directory,
            )
    finally:
        directory.close()
    with pytest.raises(publish.PublishError, match="unclassified"):
        publish.publish(cache)


def _publish_coverage(cache, manifest):
    directory = store.open_subdir(cache, "coverage")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "coverage",
                "coverage.json",
                {
                    "feeds_covered.jsonl": store.jsonl_chunks([_covered_feed("f-a")]),
                    "edges_candidate.jsonl": store.jsonl_chunks(
                        [_edge("Q1757", "f-a")]
                    ),
                },
                {
                    "source": "coverage",
                    "mode": "declared",
                    "sources": SOURCES,
                    "feeds_overrides_sha256": None,
                    **manifest,
                },
                held=directory,
            )
    finally:
        directory.close()


def test_coverage_without_a_places_generation_is_refused(tmp_path):
    cache = tmp_path / "cache"
    _publish_coverage(cache, {})
    with pytest.raises(publish.PublishError, match="no places generation"):
        publish.publish(cache)


def test_coverage_not_from_the_current_resolve_generation_is_refused(tmp_path):
    # The resolve generation it recorded is gone: nothing can verify it.
    cache = tmp_path / "cache"
    _publish_coverage(cache, {"resolve_generation": "old"})
    with pytest.raises(publish.PublishError, match="re-run the coverage stage"):
        publish.publish(cache)


def test_mismatched_coverage_and_places_releases_are_refused(tmp_path):
    with pytest.raises(publish.PublishError, match="different Overture releases"):
        _edges_index(tmp_path, [_edge("Q1757", "f-a")], release="2026-07-01.0")


def test_the_snapshot_sums_every_override_files_staleness(tmp_path):
    pytest.importorskip("geopandas")
    _, manifest = _edges_index(tmp_path, [_edge("Q1757", "f-a")], stale=(1, 2))
    assert manifest["stale_overrides"] == 3
    assert (
        manifest["stale_place_overrides"],
        manifest["stale_feed_overrides"],
        manifest["stale_edge_overrides"],
    ) == (1, 2, 0)


def test_a_names_generation_from_an_older_seed_is_refused(tmp_path):
    cache = tmp_path / "cache"
    _publish_gen(cache, "seed.json", "feed_places.jsonl", [], {"source": "seed"})
    _publish_gen(
        cache,
        "names.json",
        "places_seed.jsonl",
        PLACES,
        {
            "source": "names",
            "places_overrides_sha256": None,
            "overture_release": "2026-08-19.0",
            "seed_generation": "old",
        },
    )
    with pytest.raises(publish.PublishError, match="current seed.json"):
        publish._read_places(cache)


def test_crawl_evidence_and_provenance_round_trip(tmp_path):
    import transitio

    pytest.importorskip("geopandas")
    hull = "0101000000" + "00" * 16  # a WKB point
    crawled = {
        **_covered_feed("f-a", coverage_source="crawl"),
        "coverage": hull,
        "stop_count": 250,
        "crawl_status": "ok",
        "last_crawled": "2026-09-01T00:00:00+00:00",
    }
    cache, manifest = _edges_index(tmp_path, [_edge("Q1757", "f-a")], feeds=[crawled])
    assert manifest["schema_version"] == 3
    assert (
        manifest["discovery_semantics_version"]
        == transitio_index.DISCOVERY_SEMANTICS_VERSION
    )
    assert manifest["min_reader_version"] == transitio_index.MIN_READER_VERSIONS[3]
    assert manifest["built_with"] == transitio.__version__
    index = transitio_index.read_index(cache / "index")
    row = index.feeds.set_index("feed_id").loc["f-a"]
    assert row["coverage"] == bytes.fromhex(hull) and row["stop_count"] == 250
    assert row["crawl_status"] == "ok"
    feed = transitio_index.place("Q1757", index=index).feeds()[0]
    assert feed.stop_count == 250 and feed.coverage == bytes.fromhex(hull)
    assert feed.provenance == {
        "snapshot": manifest["snapshot_id"],
        "discovery_semantics_version": transitio_index.DISCOVERY_SEMANTICS_VERSION,
        "transitio_version": transitio.__version__,
    }
    # The tabular export carries the coverage hull as its geometry.
    frame = transitio_index.place("Q1757", index=index).feeds().to_geodataframe()
    assert frame.geometry.iloc[0].wkb == bytes.fromhex(hull)


def _publish_resolved(cache, feeds, manifest):
    directory = store.open_subdir(cache, "resolve")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "resolve",
                "feeds_resolved.json",
                {"feeds_resolved.jsonl": store.jsonl_chunks(feeds)},
                {"source": "resolve", **manifest},
                held=directory,
            )
    finally:
        directory.close()


@pytest.mark.parametrize(
    "lineage", [{"crosswalk_generation": "old"}, {}], ids=["older", "unrecorded"]
)
def test_resolved_feeds_not_from_the_current_crosswalk_are_refused(tmp_path, lineage):
    cache, _ = _build_index(tmp_path)
    _publish_resolved(cache, [], lineage)
    with pytest.raises(publish.PublishError, match="re-run the resolve stage"):
        publish.publish(cache)


def test_a_feeds_only_snapshot_ships_the_resolved_feeds(tmp_path):
    from test_index_place_overrides import write_overrides

    cache, before = _build_index(tmp_path)
    feeds, crosswalk = store.read_jsonl(
        cache / "crosswalk", "feeds.json", "feeds.jsonl"
    )
    feeds[0].update(crawlable=False, uncrawlable_reason="auth")
    lineage = {
        "crosswalk_generation": crosswalk["generation"],
        "sources": crosswalk["sources"],
        "feeds_overrides_sha256": None,
    }
    _publish_resolved(cache, feeds, lineage)
    manifest = publish.publish(cache)
    index = transitio_index.read_index(cache / "index")
    row = index.feeds.set_index("feed_id").loc[feeds[0]["feed_id"]]
    assert not row["crawlable"]
    assert manifest["snapshot_id"] != before["snapshot_id"]
    # A feeds.yaml the resolve generation did not apply: refused, before
    # and at activation; one nobody applied is a stage that has not run.
    directory = write_overrides(
        tmp_path, feeds=[{"feed": "f", "mark_uncrawlable": True}]
    )
    with pytest.raises(publish.PublishError, match="re-run the resolve"):
        publish.publish(cache, overrides_dir=directory)
    _publish_resolved(cache, feeds, {**lineage, "feeds_overrides_sha256": "x"})
    with pytest.raises(publish.PublishError, match="re-run the resolve"):
        publish.publish(cache, overrides_dir=directory)
    # With a coverage generation shipping instead, the file is coverage's to
    # check: a set_coverage edit does not send the resolve stage back.
    _, manifest = publish._read_resolved(cache, directory, check_file=False)
    assert manifest["feeds_overrides_sha256"] == "x"
    bare = tmp_path / "bare"
    bare.mkdir()
    _build_index(bare)
    with pytest.raises(publish.PublishError, match="run the resolve stage"):
        publish.publish(bare / "cache", overrides_dir=directory)


def test_reader_versions_order_pre_releases_below_finals():
    key = transitio_index._version_key
    assert key("1.0.0rc1") < key("1.0.0") < key("1.0.0.post1") < key("1.0.1")
    assert key("1.0.0-dev") < key("1.0") == key("1.0.0")
    assert key("1.0.0.dev1") < key("1.0.0a1") < key("1.0.0b1") < key("1.0.0rc1")
    assert key("1.x") is None and key("") is None
    # Bounded: an untrusted manifest cannot feed int() an unbounded digit run.
    assert key("1" * 400) is None and key("1.0.0rc" + "9" * 40) is None
    # Schema 3 arrived after the 0.10.0 release, which reads schema 2 only.
    assert key(transitio_index.MIN_READER_VERSIONS[3]) > key("0.10.0")


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"min_reader_version": "0.1.0"}, "discovery_semantics"),
        (
            {"discovery_semantics_version": 1, "min_reader_version": ""},
            "min_reader_version",
        ),
        (
            {"discovery_semantics_version": 1, "min_reader_version": "1.x"},
            "min_reader_version",
        ),
        (
            {"discovery_semantics_version": 1, "min_reader_version": "9" * 400},
            "min_reader_version",
        ),
        (
            {"discovery_semantics_version": 1, "min_reader_version": "999.0.0"},
            "needs transitio >= 999.0.0",
        ),
    ],
)
def test_the_reader_requires_the_schema_3_manifest_fields(tmp_path, fields, message):
    manifest = {"schema_version": 3, "snapshot_id": "x", "feeds_sha256": "0" * 64}
    (tmp_path / "snapshot.json").write_text(
        json.dumps({**manifest, **fields}), encoding="utf-8"
    )
    with pytest.raises(IncompatibleIndexError, match=message):
        transitio_index.read_index(tmp_path)


def test_generations_before_override_tracking_are_refused(tmp_path, monkeypatch):
    cache, _ = _build_index(tmp_path)
    _, crosswalk = store.read_jsonl(cache / "crosswalk", "feeds.json", "feeds.jsonl")
    _publish_resolved(cache, [], {"crosswalk_generation": crosswalk["generation"]})
    with pytest.raises(publish.PublishError, match="predates override tracking"):
        publish.publish(cache)
    monkeypatch.setattr(
        classify, "read_edges", lambda *a, **k: ([], [], {"source": "classify"})
    )
    with pytest.raises(publish.PublishError, match="predates override tracking"):
        publish._read_coverage(cache)


def test_an_unplaceable_reader_version_fails_closed(tmp_path, monkeypatch):
    import transitio

    cache, _ = _build_index(tmp_path)
    monkeypatch.setattr(transitio, "__version__", "not-a-version", raising=False)
    with pytest.raises(IncompatibleIndexError, match="cannot be compared"):
        transitio_index.read_index(cache / "index")


@pytest.mark.parametrize("pointer", ["expanded.json", "names.json"])
def test_places_generations_before_override_tracking_are_refused(tmp_path, pointer):
    cache = tmp_path / "cache"
    artifact = (
        "places_expanded.jsonl" if pointer == "expanded.json" else "places_seed.jsonl"
    )
    _publish_gen(cache, pointer, artifact, PLACES, {"source": "gazetteer"})
    with pytest.raises(publish.PublishError, match="predates override tracking"):
        publish._read_places(cache)
