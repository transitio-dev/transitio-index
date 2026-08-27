import hashlib
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

from index_build import atlas, store  # noqa: E402

MTA = {
    "$schema": "https://dmfr.transit.land/json-schema/dmfr.schema-v0.6.0.json",
    "feeds": [
        {
            "id": "f-dr5r-nyctsubway",
            "spec": "GTFS",
            "urls": {"static_current": "https://mta.info/gtfs.zip"},
            "name": "MTA Subway",
            "operators": [
                {
                    "onestop_id": "o-dr5r-mta",
                    "associated_feeds": [
                        {"feed_onestop_id": "f-dr5r-nyctsubway"},
                        {"feed_onestop_id": "f-dr5r-nyctsubway~rt"},
                    ],
                },
                {"name": "no id here"},
            ],
            "license": {
                "spdx_identifier": "CC-BY-4.0",
                "redistribution_allowed": "yes",
            },
            "tags": {"mdb_source_id": "1234"},
            "languages": ["en"],
        },
        {
            "id": "f-dr5r-nyctsubway~rt",
            "spec": "gtfs-rt",
            "urls": {"realtime_alerts": "https://mta.info/alerts"},
            "authorization": {"type": "header", "param_name": "x-api-key"},
            "supersedes_ids": ["f-old"],
        },
    ],
    "operators": [
        {
            "onestop_id": "o-dr5r-mta",
            "name": "Metropolitan Transportation Authority",
            "short_name": "MTA",
            "website": "https://mta.info",
            "associated_feeds": [{"feed_onestop_id": "f-dr5r-nyctsubway"}],
        }
    ],
}

BIKES = {
    "feeds": [
        {
            "id": "f-dr5r-citibike",
            "spec": "gbfs",
            "urls": {"gbfs_auto_discovery": "https://citibike/gbfs.json"},
        }
    ]
}


@pytest.fixture(params=["descriptor", "paths"], autouse=True)
def addressing(request, monkeypatch, tmp_path):
    """Run every test both ways round.

    Windows has neither ``O_NOFOLLOW`` nor descriptor-relative operations,
    so the store falls back to full paths there. Forcing that fallback on
    every platform is the only way that branch gets exercised outside a
    Windows CI job — it went out untested once already.

    The descriptor case asserts it actually got a descriptor: a capability
    probe that quietly reports "unsupported" on a platform that supports it
    would make both parameters run the same fallback, which is exactly what
    happened before `os.replace` was swapped for `os.rename` in the probe.
    """
    if request.param == "paths":
        monkeypatch.setattr(store, "HAVE_DIR_FD", False)
        # Windows has no O_NOFOLLOW either, so emulating only the missing
        # descriptors would leave the fallback stronger here than there.
        monkeypatch.setattr(store, "O_NOFOLLOW", 0)
    elif os.name == "posix":
        probe = tmp_path / ".addressing-probe"
        with store.open_directory(probe) as directory:
            assert directory.fd is not None, "descriptor mode has no descriptor"
    return request.param


class FakeResponse:
    """Stands in for urlopen's result; ``body=None`` fails like a reset."""

    def __init__(self, body):
        self._body = io.BytesIO(body) if body is not None else None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self, size=-1):
        if self._body is None:
            raise OSError("connection reset")
        return self._body.read(size)


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as opened_file:
        for chunk in iter(lambda: opened_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_archive(path, files, prefix="transitland-atlas-abc123"):
    """A tarball shaped like GitHub's, plus some members to be ignored."""
    with tarfile.open(path, "w:gz") as tar:
        for name, payload in files.items():
            raw = json.dumps(payload).encode("utf-8")
            info = tarfile.TarInfo(f"{prefix}/feeds/{name}")
            info.size = len(raw)
            tar.addfile(info, io.BytesIO(raw))

        readme = b"# not a feed\n"
        info = tarfile.TarInfo(f"{prefix}/README.md")
        info.size = len(readme)
        tar.addfile(info, io.BytesIO(readme))

        # Right suffix, wrong directory.
        info = tarfile.TarInfo(f"{prefix}/schema/example.dmfr.json")
        info.size = len(readme)
        tar.addfile(info, io.BytesIO(readme))

        info = tarfile.TarInfo(f"{prefix}/feeds/link.dmfr.json")
        info.type = tarfile.SYMTYPE
        info.linkname = "../../../etc/passwd"
        tar.addfile(info)
    return path


def test_parse_normalizes_feeds_and_operators(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {"mta.info.dmfr.json": MTA, "citibikenyc.com.dmfr.json": BIKES},
    )

    parsed = atlas.parse(archive)
    feeds, operators = parsed["feeds"], parsed["operators"]

    assert parsed["dmfr_files"] == 2
    assert [feed["onestop_id"] for feed in feeds] == [
        "f-dr5r-nyctsubway",
        "f-dr5r-nyctsubway~rt",
        "f-dr5r-citibike",
    ]
    assert [operator["onestop_id"] for operator in operators] == ["o-dr5r-mta"]

    subway = feeds[0]
    assert subway["spec"] == "gtfs"
    assert subway["source"] == "atlas"
    assert subway["source_file"] == "mta.info.dmfr.json"
    assert subway["source_domain"] == "mta.info"
    # The id-less inline entry keeps its name, and the declared realtime
    # link survives: both are evidence later stages cannot recover.
    assert [entry["onestop_id"] for entry in subway["operators"]] == [
        "o-dr5r-mta",
        None,
    ]
    assert subway["operators"][1]["name"] == "no id here"
    assert subway["operators"][0]["associated_feed_ids"] == [
        "f-dr5r-nyctsubway",
        "f-dr5r-nyctsubway~rt",
    ]
    assert subway["license"] == {
        "spdx_identifier": "CC-BY-4.0",
        "redistribution_allowed": "yes",
    }
    assert subway["tags"] == {"mdb_source_id": "1234"}
    assert subway["requires_auth"] is False

    alerts = feeds[1]
    assert alerts["requires_auth"] is True
    assert alerts["authorization"]["param_name"] == "x-api-key"
    assert alerts["supersedes_ids"] == ["f-old"]
    assert alerts["name"] is None

    assert operators[0]["name"] == "Metropolitan Transportation Authority"
    assert operators[0]["source_domain"] == "mta.info"
    assert operators[0]["associated_feed_ids"] == ["f-dr5r-nyctsubway"]


def test_parse_ignores_non_dmfr_and_non_regular_members(tmp_path):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})

    names = [name for name, _ in atlas.iter_dmfr(archive)]

    assert names == ["mta.info.dmfr.json"]


def test_oversized_member_is_an_error_not_a_skip(tmp_path, monkeypatch):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})
    monkeypatch.setattr(atlas, "MAX_MEMBER_BYTES", 8)

    with pytest.raises(atlas.IngestError, match="exceeds"):
        list(atlas.iter_dmfr(archive))


def test_feed_without_an_id_is_an_error(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {"broken.com.dmfr.json": {"feeds": [{"spec": "gtfs", "urls": {}}]}},
    )

    with pytest.raises(atlas.IngestError, match="broken.com.dmfr.json"):
        atlas.parse(archive)


def test_operator_without_an_id_is_an_error(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {"broken.com.dmfr.json": {"operators": [{"name": "nameless"}]}},
    )

    with pytest.raises(atlas.IngestError, match="operator 0 has no usable id"):
        atlas.parse(archive)


@pytest.mark.parametrize(
    "bad_id, message",
    [
        (" f-x", "surrounding whitespace"),
        ("f/x", "path separator"),
        ("f\tx", "path separator"),
        ("..", "path separator"),
        ("f" * 201, "over the 200-byte limit"),
    ],
)
def test_unusable_id_is_an_error(tmp_path, bad_id, message):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {"odd.com.dmfr.json": {"feeds": [{"id": bad_id, "spec": "gtfs"}]}},
    )

    with pytest.raises(atlas.IngestError, match=message):
        atlas.parse(archive)


def test_non_ascii_ids_are_accepted(tmp_path):
    # 870 of the 6,638 ids in the pinned Atlas are non-ASCII; rejecting
    # them would drop an eighth of the catalogue.
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {
            "aomori.dmfr.json": {
                "feeds": [{"id": "f-xpk0-青森市営バス0305", "spec": "gtfs"}]
            }
        },
    )

    feeds = atlas.parse(archive)["feeds"]

    assert feeds[0]["onestop_id"] == "f-xpk0-青森市営バス0305"


def test_realtime_linkage_evidence_survives_ingest(tmp_path):
    # The realtime link is declared on the operator, and the file the two
    # feeds share is the fallback evidence when no operator claims them.
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})

    feeds = atlas.parse(archive)["feeds"]

    static, realtime = feeds[0], feeds[1]
    assert realtime["spec"] == "gtfs-rt"
    assert realtime["source_file"] == static["source_file"]
    assert realtime["source_domain"] == static["source_domain"]


def test_unsupported_spec_is_an_error(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {"odd.com.dmfr.json": {"feeds": [{"id": "f-x", "spec": "netex"}]}},
    )

    with pytest.raises(atlas.IngestError, match="unsupported spec"):
        atlas.parse(archive)


@pytest.mark.parametrize(
    "field, value",
    [
        ("authorization", "header"),
        ("languages", "en"),
        ("license", ["CC-BY-4.0"]),
        ("urls", "https://example.com/gtfs.zip"),
        ("operators", ["o-dr5r-mta"]),
        ("name", 42),
    ],
)
def test_malformed_nested_field_is_an_error(tmp_path, field, value):
    record = {"id": "f-x", "spec": "gtfs", field: value}
    archive = build_archive(
        tmp_path / "atlas.tar.gz", {"odd.com.dmfr.json": {"feeds": [record]}}
    )

    with pytest.raises(atlas.IngestError, match=field.split()[0]):
        atlas.parse(archive)


def test_ingest_parses_a_snapshot_not_the_live_archive(tmp_path):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})
    original = sha256_of(archive)
    cache = tmp_path / "cache"

    summary = atlas.ingest(cache, archive=archive, commit="abc123")

    # The digest describes the bytes that were parsed, and no temporary
    # copy is left behind.
    assert summary["archive_sha256"] == original
    assert list((cache / "raw").glob(".tmp-*")) == []


def test_payload_with_a_wrong_shaped_collection_is_an_error(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz", {"odd.com.dmfr.json": {"feeds": {}}}
    )

    with pytest.raises(atlas.IngestError, match="feeds is dict, expected list"):
        atlas.parse(archive)


def test_symlinked_cache_directory_is_refused(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    try:
        (cache / ".probe").symlink_to(cache, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("this platform will not let the test create a symlink")
    (cache / ".probe").unlink()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "precious.txt").write_text("precious\n")
    (cache / "raw").symlink_to(elsewhere, target_is_directory=True)
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})

    # `match="symlink"` alone would pass on any error, because the temp path
    # itself contains "symlinked" — assert the actual message phrase.
    with pytest.raises(store.StoreError, match="is a symlink"):
        atlas.ingest(cache, archive=archive, commit="abc123")

    assert list(elsewhere.iterdir()) == [elsewhere / "precious.txt"]


def test_cached_archive_is_reused_only_when_its_digest_matches(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    raw = cache / "raw"
    raw.mkdir(parents=True)
    commit = "a" * 40
    cached = raw / f"transitland-atlas-{commit}.tar.gz"
    build_archive(cached, {"mta.info.dmfr.json": MTA})
    # No .sha256 sidecar: nothing ties these bytes to the commit.
    downloads = []

    def fake_download(directory, name, commit=commit):
        # Stands in for a real fetch: the bytes are already there, so it
        # returns the digest a download would have computed from the wire.
        downloads.append(name)
        return sha256_of(cached)

    monkeypatch.setattr(atlas, "download_archive", fake_download)

    summary = atlas.ingest(cache, commit=commit)

    assert downloads == [cached.name]
    assert summary["commit_verified"] is True
    assert (raw / f"transitland-atlas-{commit}.tar.gz.sha256").exists()

    # Second run: the sidecar now vouches for the bytes, so no download.
    downloads.clear()
    atlas.ingest(cache, commit=commit)
    assert downloads == []


def test_ingest_writes_jsonl_and_provenance(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {"mta.info.dmfr.json": MTA, "citibikenyc.com.dmfr.json": BIKES},
    )
    cache = tmp_path / "cache"

    summary = atlas.ingest(cache, archive=archive, commit="abc123")

    generation, manifest = store.resolve(cache / "raw", "atlas.json")
    feeds = [
        json.loads(line)
        for line in (generation / "atlas_feeds.jsonl").read_text().splitlines()
    ]
    operators = [
        json.loads(line)
        for line in (generation / "atlas_operators.jsonl").read_text().splitlines()
    ]
    assert len(feeds) == 3
    assert len(operators) == 1
    assert manifest == summary

    assert summary["dmfr_files"] == 2
    assert summary["feeds"] == 3
    assert summary["feeds_by_spec"] == {"gtfs": 1, "gtfs-rt": 1, "gbfs": 1}
    assert summary["operators"] == 1
    assert summary["commit"] == "abc123"
    # A local archive is used as-is: nothing fetched it, so nothing can
    # vouch for the commit it claims to be.
    assert summary["commit_verified"] is False
    assert summary["archive_url"] is None
    assert summary["archive_sha256"] == sha256_of(archive)

    # The generation carries its own copy of the manifest, and the pointer
    # names the generation the artifacts actually live in.
    written = json.loads((generation / "manifest.json").read_text())
    assert written == summary
    assert generation.name == summary["generation"]


def test_archive_url_pins_the_commit():
    assert atlas.archive_url("deadbeef").endswith("/tar.gz/deadbeef")


def test_a_movable_ref_is_refused_before_any_download(tmp_path):
    # The CLI validates too, but a caller reaching ingest() directly must
    # not be able to fetch a branch and have it recorded as a verified pin.
    with pytest.raises(atlas.IngestError, match="not a full 40-character"):
        atlas.ingest(tmp_path / "cache", commit="main")


def test_duplicate_dmfr_basenames_are_refused(tmp_path):
    archive = tmp_path / "atlas.tar.gz"
    payload = json.dumps({"feeds": [{"id": "f-x", "spec": "gtfs"}]}).encode()
    with tarfile.open(archive, "w:gz") as tar:
        for prefix in ("transitland-atlas-abc123", "transitland-atlas-abc123/nested"):
            info = tarfile.TarInfo(f"{prefix}/feeds/mta.info.dmfr.json")
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))

    # The nested copy is outside the expected layout, so it is not read at
    # all; only the top-level one contributes.
    parsed = atlas.parse(archive)

    assert parsed["dmfr_files"] == 1
    assert [feed["source_file"] for feed in parsed["feeds"]] == ["mta.info.dmfr.json"]


def test_failed_download_leaves_nothing_behind(tmp_path, monkeypatch):
    attempts = []

    def explode(url, timeout=None):
        attempts.append(url)
        return FakeResponse(None)

    monkeypatch.setattr(atlas.urllib.request, "urlopen", explode)
    directory = store.open_directory(tmp_path)

    try:
        with pytest.raises(OSError):
            atlas.download_archive(directory, "atlas.tar.gz", commit="d" * 40)
    finally:
        directory.close()

    # A truncated file here would be treated as a cached archive forever,
    # and every retry must clean up after itself.
    assert len(attempts) == atlas.DOWNLOAD_ATTEMPTS
    assert not (tmp_path / "atlas.tar.gz").exists()
    assert list(tmp_path.glob(".tmp-*")) == []


def test_oversized_archive_is_refused(tmp_path, monkeypatch):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})
    monkeypatch.setattr(atlas, "MAX_ARCHIVE_BYTES", 16)
    cache = tmp_path / "cache"

    with pytest.raises(atlas.IngestError, match="archive ceiling"):
        atlas.ingest(cache, archive=archive, commit="abc123")

    assert list((cache / "raw").glob(".tmp-*")) == []


def test_a_fifo_archive_path_is_refused_not_blocked_on(tmp_path):
    # A path input skips the cache's descriptor guard, so parse() applies
    # its own: a FIFO substituted for the archive would otherwise wedge the
    # open indefinitely instead of failing.
    fifo = tmp_path / "archive.tar.gz"
    try:
        os.mkfifo(fifo)
    except (AttributeError, OSError):
        pytest.skip("this platform has no FIFOs")

    with pytest.raises(atlas.IngestError, match="not a regular file"):
        atlas.parse(fifo)


def _symlink_or_skip(link, target):
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("this platform will not let the test create a symlink")


def test_a_symlinked_archive_path_is_refused_by_parse(tmp_path):
    real = build_archive(tmp_path / "real.tar.gz", {"mta.info.dmfr.json": MTA})
    link = tmp_path / "link.tar.gz"
    _symlink_or_skip(link, real)

    # On POSIX O_NOFOLLOW refuses it; on the path fallback the lstat
    # precheck does. Either way it is refused, portably.
    with pytest.raises(atlas.IngestError, match="is a symlink"):
        atlas.parse(link)


def test_a_symlinked_archive_path_is_refused_by_ingest(tmp_path):
    real = build_archive(tmp_path / "real.tar.gz", {"mta.info.dmfr.json": MTA})
    link = tmp_path / "link.tar.gz"
    _symlink_or_skip(link, real)

    with pytest.raises(atlas.IngestError, match="is a symlink"):
        atlas.ingest(tmp_path / "cache", archive=link, commit="a" * 40)


def test_empty_archive_is_refused(tmp_path):
    archive = build_archive(tmp_path / "atlas.tar.gz", {})

    with pytest.raises(atlas.IngestError, match="no DMFR feeds found"):
        atlas.parse(archive)


def test_archive_with_no_feeds_is_refused(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz", {"empty.com.dmfr.json": {"feeds": []}}
    )

    with pytest.raises(atlas.IngestError, match="no DMFR feeds found"):
        atlas.parse(archive)


def test_a_refused_ingest_leaves_the_previous_generation_in_place(tmp_path):
    cache = tmp_path / "cache"
    good = build_archive(tmp_path / "good.tar.gz", {"mta.info.dmfr.json": MTA})
    first = atlas.ingest(cache, archive=good, commit="abc123")

    empty = build_archive(tmp_path / "empty.tar.gz", {})
    with pytest.raises(atlas.IngestError):
        atlas.ingest(cache, archive=empty, commit="abc123")

    generation, manifest = store.resolve(cache / "raw", "atlas.json")
    assert manifest == first
    assert generation.name == first["generation"]


def test_successful_download_is_stored_and_parsed(tmp_path, monkeypatch):
    archive = build_archive(tmp_path / "source.tar.gz", {"mta.info.dmfr.json": MTA})
    payload = archive.read_bytes()

    monkeypatch.setattr(
        atlas.urllib.request,
        "urlopen",
        lambda url, timeout=None: FakeResponse(payload),
    )
    cache = tmp_path / "cache"
    commit = "b" * 40

    summary = atlas.ingest(cache, commit=commit)

    # The whole download path: fetched, validated as a tarball, named,
    # digest-recorded, then parsed.
    assert summary["commit_verified"] is True
    assert summary["archive_sha256"] == sha256_of(archive)
    assert summary["feeds"] == 2
    cached = cache / "raw" / f"transitland-atlas-{commit}.tar.gz"
    assert cached.read_bytes() == payload
    assert (cache / "raw" / f"{cached.name}.sha256").read_text().strip() == summary[
        "archive_sha256"
    ]


def test_a_truncated_download_does_not_poison_the_cache(tmp_path, monkeypatch):
    archive = build_archive(tmp_path / "source.tar.gz", {"mta.info.dmfr.json": MTA})
    payload = archive.read_bytes()
    bodies = [payload[: len(payload) // 2], payload]

    truncated = [True]

    def urlopen(url, timeout=None):
        return FakeResponse(bodies[0] if truncated[0] else bodies[1])

    monkeypatch.setattr(atlas.urllib.request, "urlopen", urlopen)
    cache = tmp_path / "cache"
    commit = "c" * 40
    cached = cache / "raw" / f"transitland-atlas-{commit}.tar.gz"

    with pytest.raises(atlas.IngestError, match="not a tarball"):
        atlas.ingest(cache, commit=commit)

    # Nothing may survive that a later run would trust: a half archive
    # with a sidecar vouching for it is never fetched again.
    assert not cached.exists()
    assert not (cache / "raw" / f"{cached.name}.sha256").exists()

    truncated[0] = False
    summary = atlas.ingest(cache, commit=commit)

    assert summary["feeds"] == 2
    assert cached.read_bytes() == payload


def test_expansion_beyond_the_ceiling_is_refused(tmp_path, monkeypatch):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})
    # The ceiling is enforced on the decompressed stream, before any member
    # is yielded — which is where an expansion bomb would otherwise land.
    monkeypatch.setattr(atlas, "MAX_TOTAL_BYTES", 64)

    with pytest.raises(atlas.IngestError, match="expands past"):
        atlas.parse(archive)


def test_a_malformed_associated_feed_id_is_an_error(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {
            "odd.com.dmfr.json": {
                "operators": [
                    {
                        "onestop_id": "o-x",
                        "associated_feeds": [{"feed_onestop_id": "../escape"}],
                    }
                ],
                "feeds": [{"id": "f-x", "spec": "gtfs"}],
            }
        },
    )

    with pytest.raises(atlas.IngestError, match="path separator"):
        atlas.parse(archive)


def _with_associations(entries):
    return {
        "odd.com.dmfr.json": {
            "operators": [{"onestop_id": "o-x", "associated_feeds": entries}],
            "feeds": [{"id": "f-x", "spec": "gtfs"}],
        }
    }


@pytest.mark.parametrize("entry", [{}, {"feed_onestop_id": None}])
def test_an_empty_association_is_an_error(tmp_path, entry):
    archive = build_archive(tmp_path / "atlas.tar.gz", _with_associations([entry]))

    with pytest.raises(atlas.IngestError, match="neither a feed nor an agency"):
        atlas.parse(archive)


def test_an_agency_association_is_kept(tmp_path):
    # 1,217 entries in the pinned Atlas tie an operator to an agency inside
    # the containing feed rather than to another feed.
    archive = build_archive(
        tmp_path / "atlas.tar.gz", _with_associations([{"gtfs_agency_id": "2"}])
    )

    operator = atlas.parse(archive)["operators"][0]

    assert operator["associations"] == [
        {"feed_onestop_id": None, "gtfs_agency_id": "2"}
    ]
    assert operator["associated_feed_ids"] == []


def test_repeated_operator_ids_are_merged_and_counted(tmp_path):
    # Two of the 312 operators in the pinned Atlas are listed once per feed
    # group; the union is what later stages read, but each listing is kept.
    operator = {
        "onestop_id": "o-mie~kotsu",
        "name": "三重交通",
        "associated_feeds": [{"feed_onestop_id": "f-a"}],
    }
    second = dict(operator, associated_feeds=[{"feed_onestop_id": "f-b"}])
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {
            "jp.dmfr.json": {
                "operators": [operator, second],
                "feeds": [{"id": "f-a", "spec": "gtfs"}, {"id": "f-b", "spec": "gtfs"}],
            }
        },
    )

    parsed = atlas.parse(archive)

    assert parsed["operator_id_collisions"] == 1
    assert len(parsed["operators"]) == 1
    merged = parsed["operators"][0]
    assert merged["associated_feed_ids"] == ["f-a", "f-b"]
    # The first listing is this record; only the extra one is carried.
    assert [listing["associated_feed_ids"] for listing in merged["other_listings"]] == [
        ["f-b"]
    ]


def test_an_unrepeated_operator_carries_no_extra_listings(tmp_path):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})

    assert "other_listings" not in atlas.parse(archive)["operators"][0]


def test_a_null_collection_is_an_error(tmp_path):
    # A file whose feeds are explicitly null would otherwise be ingested as
    # if it had none, and the archive-wide check would not notice.
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {
            "good.com.dmfr.json": MTA,
            "null.com.dmfr.json": {"feeds": None},
        },
    )

    with pytest.raises(atlas.IngestError, match="feeds is NoneType"):
        atlas.parse(archive)


def test_an_inline_operator_with_only_an_association_is_kept(tmp_path):
    archive = build_archive(
        tmp_path / "atlas.tar.gz",
        {
            "odd.com.dmfr.json": {
                "feeds": [
                    {
                        "id": "f-x",
                        "spec": "gtfs",
                        "operators": [
                            {"associated_feeds": [{"feed_onestop_id": "f-x~rt"}]}
                        ],
                    }
                ]
            }
        },
    )

    feeds = atlas.parse(archive)["feeds"]

    assert feeds[0]["operators"][0]["associated_feed_ids"] == ["f-x~rt"]


def test_cli_ingest_runs_offline(tmp_path):
    archive = build_archive(tmp_path / "atlas.tar.gz", {"mta.info.dmfr.json": MTA})
    cache = tmp_path / "cache"

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--stage",
            "ingest",
            "--source",
            "atlas",
            "--cache-dir",
            str(cache),
            "--archive",
            str(archive),
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    summary = json.loads(completed.stdout)
    assert summary["feeds"] == 2
    generation, _ = store.resolve(cache / "raw", "atlas.json")
    assert (generation / "atlas_feeds.jsonl").exists()


def test_cli_rejects_a_commit_that_is_not_a_full_sha(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--stage",
            "ingest",
            "--cache-dir",
            str(tmp_path / "cache"),
            "--commit",
            "main",
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "not a full 40-character commit SHA" in completed.stderr


def test_cli_reports_a_missing_archive(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--stage",
            "ingest",
            "--cache-dir",
            str(tmp_path / "cache"),
            "--archive",
            str(tmp_path / "absent.tar.gz"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert "ingest:" in completed.stderr
