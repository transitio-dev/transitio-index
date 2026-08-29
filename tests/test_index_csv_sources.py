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

from index_build import csv_source, gbfs, mdb, store  # noqa: E402


@pytest.fixture(params=["descriptor", "paths"], autouse=True)
def addressing(request, monkeypatch, tmp_path):
    """Run every test in both the descriptor and the path-fallback mode."""
    if request.param == "paths":
        monkeypatch.setattr(store, "HAVE_DIR_FD", False)
        monkeypatch.setattr(store, "O_NOFOLLOW", 0)
    elif os.name == "posix":
        with store.open_directory(tmp_path / ".probe") as directory:
            assert directory.fd is not None


COLUMNS = sorted(mdb.REQUIRED_HEADERS)


def mdb_csv(*rows):
    """A feeds_v2.csv with every required column; rows fill what they name."""
    lines = [",".join(COLUMNS)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in COLUMNS))
    return "\n".join(lines) + "\n"


GBFS_COLUMNS = sorted(gbfs.REQUIRED_HEADERS)


def gbfs_csv(*rows):
    """A systems.csv with every required column; rows fill what they name."""
    lines = [",".join(GBFS_COLUMNS)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in GBFS_COLUMNS))
    return "\n".join(lines) + "\n"


def write(path, text):
    path.write_text(text, encoding="utf-8")
    return path


def records_of(tmp_path, pointer, name):
    generation, _ = store.resolve(tmp_path / "cache" / "raw", pointer)
    with generation:
        return [
            json.loads(line)
            for line in generation.read_bytes(name).decode("utf-8").splitlines()
        ]


GTFS = {"id": "mdb-1", "data_type": "gtfs"}


def test_mdb_normalizes_and_maps_spec(tmp_path):
    csv = write(
        tmp_path / "feeds_v2.csv",
        mdb_csv(
            {
                "id": "mdb-1",
                "data_type": "gtfs",
                "provider": "青森市",
                "name": "Aomori",
                "status": "active",
                "is_official": "True",
                "location.country_code": "JP",
                "location.bounding_box.minimum_latitude": "40.0",
                "location.bounding_box.maximum_latitude": "41.0",
                "location.bounding_box.minimum_longitude": "140.0",
                "location.bounding_box.maximum_longitude": "141.0",
                "urls.authentication_type": "0",
                "features": "Fares V1|Headsigns",
            },
            {
                "id": "mdb-2",
                "data_type": "gtfs_rt",
                "status": "active",
                "urls.authentication_type": "1",
                "static_reference": "mdb-1",
            },
            {
                "id": "mdb-3",
                "data_type": "gtfs",
                "status": "deprecated",
                "is_official": "False",
                "urls.authentication_type": "0",
                "redirect.id": "mdb-1",
            },
        ),
    )

    mdb.ingest(tmp_path / "cache", csv_path=csv)

    generation, manifest = store.resolve(tmp_path / "cache" / "raw", "mdb.json")
    with generation:
        records = [
            json.loads(line)
            for line in generation.read_bytes("mdb_feeds.jsonl").splitlines()
        ]
    assert manifest["records_by_spec"] == {"gtfs": 2, "gtfs-rt": 1}
    assert manifest["records_by_status"]["deprecated"] == 1

    static = records[0]
    assert static["spec"] == "gtfs"
    assert static["mdb_id"] == "mdb-1"
    assert static["official"] is True
    assert static["bounding_box"] == {
        "min_lat": 40.0,
        "max_lat": 41.0,
        "min_lon": 140.0,
        "max_lon": 141.0,
    }
    assert static["features"] == ["Fares V1", "Headsigns"]
    assert static["requires_auth"] is False

    realtime = records[1]
    assert realtime["spec"] == "gtfs-rt"
    assert realtime["static_references"] == ["mdb-1"]
    assert realtime["requires_auth"] is True
    assert realtime["bounding_box"] is None
    assert records[2]["redirect_ids"] == ["mdb-1"]
    assert records[2]["official"] is False


def test_mdb_non_ascii_id_is_accepted(tmp_path):
    csv = write(
        tmp_path / "feeds_v2.csv", mdb_csv({"id": "f-青森", "data_type": "gtfs"})
    )

    mdb.ingest(tmp_path / "cache", csv_path=csv)

    got = records_of(tmp_path, "mdb.json", "mdb_feeds.jsonl")[0]["mdb_id"]
    assert got == "f-青森"


def test_mdb_duplicate_id_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv(GTFS, GTFS))
    with pytest.raises(csv_source.IngestError, match="appears at rows"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


def test_mdb_unsupported_data_type_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv({"id": "x", "data_type": "gbfs"}))
    with pytest.raises(csv_source.IngestError, match="unsupported data_type"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


def test_mdb_blank_id_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv({"data_type": "gtfs"}))
    with pytest.raises(csv_source.IngestError, match="no usable id"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


@pytest.mark.parametrize("field", ["is_official"])
def test_mdb_non_boolean_official_is_an_error(tmp_path, field):
    csv = write(
        tmp_path / "feeds_v2.csv",
        mdb_csv({"id": "x", "data_type": "gtfs", field: "maybe"}),
    )
    with pytest.raises(csv_source.IngestError, match="is not a boolean"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


@pytest.mark.parametrize("field", ["static_reference", "redirect.id"])
def test_mdb_unsafe_reference_id_is_an_error(tmp_path, field):
    csv = write(
        tmp_path / "feeds_v2.csv",
        mdb_csv({"id": "x", "data_type": "gtfs", field: "../escape"}),
    )
    with pytest.raises(csv_source.IngestError, match="path separator"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


def test_mdb_reference_lists_are_split(tmp_path):
    # redirect.id and static_reference are pipe-joined lists upstream (up to
    # 15 parts in the real catalogue), each a valid id.
    csv = write(
        tmp_path / "feeds_v2.csv",
        mdb_csv(
            {
                "id": "x",
                "data_type": "gtfs_rt",
                "static_reference": "mdb-1|mdb-2|mdb-3",
                "redirect.id": "mdb-9|mdb-10",
            }
        ),
    )

    record = (
        mdb.ingest(tmp_path / "cache", csv_path=csv)
        and records_of(tmp_path, "mdb.json", "mdb_feeds.jsonl")[0]
    )
    assert record["static_references"] == ["mdb-1", "mdb-2", "mdb-3"]
    assert record["redirect_ids"] == ["mdb-9", "mdb-10"]


def test_mdb_missing_required_column_is_an_error(tmp_path):
    header = ",".join(c for c in COLUMNS if c != "id")
    csv = write(tmp_path / "feeds_v2.csv", header + "\n")
    with pytest.raises(csv_source.IngestError, match="missing CSV columns: id"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


def test_mdb_duplicate_column_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", ",".join(COLUMNS + ["id"]) + "\n")
    with pytest.raises(csv_source.IngestError, match="duplicate CSV column"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


def test_mdb_ragged_row_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", ",".join(COLUMNS) + "\n" + "extra\n")
    with pytest.raises(csv_source.IngestError, match="fewer cells than columns"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


@pytest.mark.parametrize(
    "box",
    [
        {"min_lat": "100.0", "max_lat": "101.0", "min_lon": "10.0", "max_lon": "11.0"},
        {"min_lat": "41.0", "max_lat": "40.0", "min_lon": "10.0", "max_lon": "11.0"},
        {"min_lat": "40.0", "max_lat": "41.0", "min_lon": "-200.0", "max_lon": "11.0"},
        {"min_lat": "nan", "max_lat": "41.0", "min_lon": "10.0", "max_lon": "11.0"},
        # only one cell present, and it is out of range: present but invalid
        {"min_lat": "", "max_lat": "", "min_lon": "999.0", "max_lon": ""},
    ],
)
def test_mdb_invalid_bounding_box_is_dropped_and_counted(tmp_path, box):
    row = {
        "id": "x",
        "data_type": "gtfs",
        "location.bounding_box.minimum_latitude": box["min_lat"],
        "location.bounding_box.maximum_latitude": box["max_lat"],
        "location.bounding_box.minimum_longitude": box["min_lon"],
        "location.bounding_box.maximum_longitude": box["max_lon"],
    }
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv(row))

    summary = mdb.ingest(tmp_path / "cache", csv_path=csv)

    assert summary["invalid_bounding_boxes"] == 1
    got = records_of(tmp_path, "mdb.json", "mdb_feeds.jsonl")[0]["bounding_box"]
    assert got is None


def test_mdb_antimeridian_box_is_kept(tmp_path):
    row = {
        "id": "x",
        "data_type": "gtfs",
        "location.bounding_box.minimum_latitude": "40.0",
        "location.bounding_box.maximum_latitude": "41.0",
        "location.bounding_box.minimum_longitude": "170.0",
        "location.bounding_box.maximum_longitude": "-170.0",
    }
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv(row))

    summary = mdb.ingest(tmp_path / "cache", csv_path=csv)

    assert summary["invalid_bounding_boxes"] == 0
    box = records_of(tmp_path, "mdb.json", "mdb_feeds.jsonl")[0]["bounding_box"]
    assert box == {
        "min_lat": 40.0,
        "max_lat": 41.0,
        "min_lon": 170.0,
        "max_lon": -170.0,
    }


def test_mdb_pinned_digest_mismatch_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv(GTFS))
    with pytest.raises(csv_source.IngestError, match="does not match the pinned"):
        mdb.ingest(tmp_path / "cache", csv_path=csv, expected_sha256="0" * 64)


def test_mdb_records_are_unpinned_by_default(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", mdb_csv(GTFS))
    summary = mdb.ingest(tmp_path / "cache", csv_path=csv)
    assert summary["pinned"] is False
    assert len(summary["csv_sha256"]) == 64


def test_mdb_blank_status_becomes_a_string_key(tmp_path):
    # A None status key would raise when the manifest is serialized with
    # sort_keys=True alongside string keys.
    csv = write(
        tmp_path / "feeds_v2.csv",
        mdb_csv(
            {"id": "a", "data_type": "gtfs"},
            {"id": "b", "data_type": "gtfs", "status": "active"},
        ),
    )

    summary = mdb.ingest(tmp_path / "cache", csv_path=csv)

    assert summary["records_by_status"] == {"unknown": 1, "active": 1}


def test_gbfs_normalizes_and_keeps_colliding_system_ids(tmp_path):
    csv = write(
        tmp_path / "systems.csv",
        gbfs_csv(
            {
                "System ID": "careem_bike",
                "Name": "Careem BIKE",
                "Location": "Dubai",
                "Country Code": "AE",
                "URL": "https://careem.com",
                "Auto-Discovery URL": "https://careem/gbfs.json",
                "Supported Versions": "1.1 ; 2.3 ; 3.0",
            },
            {
                "System ID": "seville",
                "Name": "Cooltra Seville",
                "Location": "Seville",
                "Country Code": "ES",
                "Supported Versions": "2.3",
            },
            {
                "System ID": "seville",
                "Name": "Sevici",
                "Location": "Seville",
                "Country Code": "ES",
                "Supported Versions": "2.3",
            },
        ),
    )

    summary = gbfs.ingest(tmp_path / "cache", csv_path=csv)
    records = records_of(tmp_path, "gbfs.json", "gbfs_systems.jsonl")

    first = records[0]
    assert first["spec"] == "gbfs"
    assert first["system_id"] == "careem_bike"
    assert first["supported_versions"] == ["1.1", "2.3", "3.0"]
    assert first["requires_auth"] is False
    # Both Seville systems are kept; the collision is reported, not merged.
    assert [r["name"] for r in records if r["system_id"] == "seville"] == [
        "Cooltra Seville",
        "Sevici",
    ]
    assert summary["system_id_collisions"] == ["seville"]
    assert summary["countries"] == 2


def test_gbfs_requires_auth_when_authentication_type_is_set(tmp_path):
    csv = write(
        tmp_path / "systems.csv",
        gbfs_csv(
            {"System ID": "open", "Authentication Type": ""},
            {"System ID": "keyed", "Authentication Type": "2"},
        ),
    )

    records = {
        record["system_id"]: record
        for record in (
            gbfs.ingest(tmp_path / "cache", csv_path=csv)
            and records_of(tmp_path, "gbfs.json", "gbfs_systems.jsonl")
        )
    }
    assert records["open"]["requires_auth"] is False
    assert records["keyed"]["requires_auth"] is True
    assert records["keyed"]["authentication_type"] == "2"


def test_gbfs_blank_system_id_is_an_error(tmp_path):
    csv = write(tmp_path / "systems.csv", gbfs_csv({"Name": "x", "Location": "y"}))
    with pytest.raises(csv_source.IngestError, match="no usable id"):
        gbfs.ingest(tmp_path / "cache", csv_path=csv)


def test_gbfs_missing_required_column_is_an_error(tmp_path):
    header = ",".join(c for c in GBFS_COLUMNS if c != "System ID")
    csv = write(tmp_path / "systems.csv", header + "\n")
    with pytest.raises(csv_source.IngestError, match="missing CSV columns: System ID"):
        gbfs.ingest(tmp_path / "cache", csv_path=csv)


def test_row_count_over_the_cap_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(csv_source, "MAX_ROWS", 1)
    csv = write(
        tmp_path / "feeds_v2.csv",
        mdb_csv({"id": "a", "data_type": "gtfs"}, {"id": "b", "data_type": "gtfs"}),
    )
    with pytest.raises(csv_source.IngestError, match="more than 1 rows"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


def test_empty_csv_is_an_error(tmp_path):
    csv = write(tmp_path / "feeds_v2.csv", ",".join(COLUMNS) + "\n")
    with pytest.raises(csv_source.IngestError, match="no rows"):
        mdb.ingest(tmp_path / "cache", csv_path=csv)


@pytest.mark.parametrize("bad", ["a/b", "a\\b", "..", "x\ty"])
def test_require_id_refuses_unsafe_ids(bad):
    with pytest.raises(csv_source.IngestError):
        csv_source.require_id(bad, "feed", "f.csv", 0)


class _ShortResponse:
    """A response whose body ends before its declared Content-Length."""

    def __init__(self, body, declared):
        self._body = io.BytesIO(body)
        self._declared = declared

    @property
    def headers(self):
        return {"Content-Length": str(self._declared)}

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self, size=-1):
        return self._body.read(size)


def test_a_truncated_download_is_refused_and_retried(tmp_path, monkeypatch):
    calls = []

    def urlopen(url, timeout=None):
        calls.append(url)
        return _ShortResponse(b"id,data_type\n", declared=9999)

    monkeypatch.setattr(csv_source.urllib.request, "urlopen", urlopen)
    directory = store.open_directory(tmp_path)
    try:
        with pytest.raises(csv_source.IngestError, match="expected 9999"):
            csv_source.download_csv(directory, "x.csv", "https://example/x.csv")
    finally:
        directory.close()

    assert len(calls) == csv_source.DOWNLOAD_ATTEMPTS
    assert not (tmp_path / "x.csv").exists()
    assert list(tmp_path.glob(".tmp-*")) == []


def test_a_fifo_csv_path_is_refused_not_blocked_on(tmp_path):
    fifo = tmp_path / "feeds.csv"
    try:
        os.mkfifo(fifo)
    except (AttributeError, OSError):
        pytest.skip("this platform has no FIFOs")
    with pytest.raises(csv_source.IngestError, match="not a regular file"):
        mdb.ingest(tmp_path / "cache", csv_path=fifo)


def _symlink_or_skip(link, target):
    try:
        link.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("this platform cannot create symlinks")


def test_a_symlinked_cache_root_is_refused_on_write(tmp_path):
    # A symlink swapped in at the cache root would redirect the whole publish;
    # opening `raw` through it must refuse the link, not follow it.
    (tmp_path / "real").mkdir()
    link = tmp_path / "cache"
    _symlink_or_skip(link, tmp_path / "real")
    with pytest.raises(store.StoreError, match="symlink"):
        mdb.ingest(link, csv_path=write(tmp_path / "m.csv", mdb_csv(GTFS)))


def test_a_symlinked_cache_root_is_refused_on_read(tmp_path):
    # Publish into a real cache, then resolve through a symlink standing in for
    # its root: the read guards the cache root too, not only the store dir.
    real = tmp_path / "real"
    mdb.ingest(real, csv_path=write(tmp_path / "m.csv", mdb_csv(GTFS)))
    link = tmp_path / "cache"
    _symlink_or_skip(link, real)
    with pytest.raises(store.StoreError, match="symlink"):
        store.resolve(link / "raw", "mdb.json")


def _atlas_archive(tmp_path):
    payload = json.dumps({"feeds": [{"id": "f-x", "spec": "gtfs"}]}).encode("utf-8")
    path = tmp_path / "atlas.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo("transitland-atlas-x/feeds/x.dmfr.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return path


def test_mdb_and_atlas_coexist_in_one_cache(tmp_path):
    # Two catalogue sources, two pointers in one store: re-publishing one
    # must not prune the other's live generation.
    from index_build import atlas

    cache = tmp_path / "cache"
    atlas.ingest(cache, archive=_atlas_archive(tmp_path), commit="a" * 40)
    for _ in range(store.KEEP_GENERATIONS + 2):
        mdb.ingest(cache, csv_path=write(tmp_path / "m.csv", mdb_csv(GTFS)))

    atlas_generation, atlas_manifest = store.resolve(cache / "raw", "atlas.json")
    mdb_generation, mdb_manifest = store.resolve(cache / "raw", "mdb.json")
    assert atlas_manifest["source"] == "atlas"
    assert mdb_manifest["source"] == "mdb"
    atlas_generation.close()
    mdb_generation.close()


def test_gbfs_pointer_survives_mdb_republish_pruning(tmp_path):
    # One store, three catalogue pointers: re-publishing MDB past the keep
    # window must not prune GBFS's live generation.
    cache = tmp_path / "cache"
    gbfs.ingest(cache, csv_path=write(tmp_path / "g.csv", gbfs_csv({"System ID": "s"})))
    for _ in range(store.KEEP_GENERATIONS + 2):
        mdb.ingest(cache, csv_path=write(tmp_path / "m.csv", mdb_csv(GTFS)))

    gbfs_generation, gbfs_manifest = store.resolve(cache / "raw", "gbfs.json")
    with gbfs_generation:
        assert gbfs_manifest["source"] == "gbfs"
        assert gbfs_generation.read_bytes("gbfs_systems.jsonl")


def test_cli_ingests_mdb_offline(tmp_path):
    cache = tmp_path / "cache"
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--stage",
            "ingest",
            "--source",
            "mdb",
            "--cache-dir",
            str(cache),
            "--mdb-csv",
            str(
                write(
                    tmp_path / "m.csv",
                    mdb_csv(GTFS, {"id": "y", "data_type": "gtfs_rt"}),
                )
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    summary = json.loads(completed.stdout)
    assert summary["source"] == "mdb"
    assert summary["records"] == 2


def test_cli_ingests_mdb_and_gbfs_offline(tmp_path):
    cache = tmp_path / "cache"
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--stage",
            "ingest",
            "--source",
            "mdb",
            "--source",
            "gbfs",
            "--cache-dir",
            str(cache),
            "--mdb-csv",
            str(write(tmp_path / "m.csv", mdb_csv(GTFS))),
            "--gbfs-csv",
            str(write(tmp_path / "g.csv", gbfs_csv({"System ID": "s"}))),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    sources = {json.loads(block)["source"] for block in _json_blocks(completed.stdout)}
    assert sources == {"mdb", "gbfs"}


def _json_blocks(text):
    depth = 0
    start = None
    for index, char in enumerate(text):
        if char == "{":
            if depth == 0:
                start = index
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                yield text[start : index + 1]
