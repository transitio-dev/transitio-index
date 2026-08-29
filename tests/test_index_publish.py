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
from index_build import publish  # noqa: E402

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


def _build_index(tmp_path, archive=None):
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
