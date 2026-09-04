import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import publish_index  # noqa: E402
from index_build import publisher  # noqa: E402
from index_build import atlas, classify, licensing, publish  # noqa: E402
from test_index_coverage import _write_crawl  # noqa: E402
from test_index_publish import (  # noqa: E402
    PLACES,
    _atlas_archive,
    _build_index,
    _publish_audit,
    _publish_coverage,
    _publish_gen,
)
from transitio.index import release as contract  # noqa: E402

API = "https://api.example"
UPLOADS = "https://uploads.example"
DOWNLOADS = "https://objects.example"


class FakeGitHub:
    """The slice of the Releases API the publisher and a client touch, with
    GitHub's visibility rules: drafts and their assets need the token."""

    def __init__(self, *, corrupt=None, lose_publish_response=False, on_upload=None):
        self.releases = {}
        self.assets = {}
        self.blobs = {}
        self.corrupt = corrupt
        self.lose_publish_response = lose_publish_response
        self.on_upload = on_upload
        self.clock = 0
        self.published = 0
        self.latest = None

    def transport(self):
        return httpx.MockTransport(self.handle)

    def seed(self, tag, assets, *, draft=False):
        """A release that exists before the publisher runs; index releases
        were published the way the publisher does, never as latest."""
        make_latest = "false" if tag.startswith(contract.TAG_PREFIX) else "true"
        release = self._create(tag, draft, make_latest)
        for name, data in assets.items():
            self._upload(release, name, data)
        return release

    def _create(self, tag, draft, make_latest="true"):
        self.clock += 1
        release_id = 100 + self.clock
        if not draft and make_latest != "false":
            self.latest = release_id
        if not draft:
            self.published += 1
        release = {
            "id": release_id,
            "tag_name": tag,
            "draft": draft,
            "prerelease": False,
            "created_at": f"2026-09-{self.clock:02d}T00:00:00Z",
            "published_at": (
                None if draft else f"2026-10-{self.published:02d}T00:00:00Z"
            ),
            "upload_url": f"{UPLOADS}/repos/o/r/releases/{release_id}/assets{{?name,label}}",
            "assets": [],
        }
        self.releases[release_id] = release
        return release

    def _upload(self, release, name, data):
        asset_id = 1000 + len(self.assets) + 1
        stored = data[:-1] + b"?" if self.corrupt == name else data
        self.blobs[asset_id] = stored
        asset = {
            "id": asset_id,
            "name": name,
            "size": len(stored),
            "url": f"{API}/repos/o/r/releases/assets/{asset_id}",
            "browser_download_url": f"{DOWNLOADS}/{release['tag_name']}/{name}",
        }
        self.assets[asset_id] = (release["id"], asset)
        release["assets"].append(asset)
        return asset

    def handle(self, request):
        authed = "Authorization" in request.headers
        path = request.url.path
        if path.startswith("/repos/old/r/"):
            # A transferred repository: GitHub answers with its new home.
            location = f"{API}/repos/o/r/" + path[len("/repos/old/r/") :]
            return httpx.Response(301, headers={"Location": location})
        # The token reaches the API and upload hosts only: never the object
        # store the asset redirects lead to.
        assert authed == (request.url.host != "objects.example") or (
            request.url.host == "api.example" and not authed
        )
        if request.url.host == "objects.example":
            # The API redirect is a signed capability URL; the browser URL
            # of a draft's asset is not served to anyone.
            if path.startswith("/signed/"):
                return httpx.Response(
                    200, content=self.blobs[int(path.rsplit("/", 1)[1])]
                )
            _, tag, name = path.split("/", 2)
            for release_id, asset in self.assets.values():
                release = self.releases[release_id]
                if release["tag_name"] == tag and asset["name"] == name:
                    if release["draft"]:
                        return httpx.Response(404)
                    return httpx.Response(200, content=self.blobs[asset["id"]])
            return httpx.Response(404)
        if request.url.host == "uploads.example" and request.method == "POST":
            assert authed
            if self.on_upload is not None:
                self.on_upload()
            release_id = int(path.split("/")[-2])
            name = request.url.params["name"]
            asset = self._upload(self.releases[release_id], name, request.content)
            return httpx.Response(201, json=asset)
        if request.method == "GET" and path.startswith("/repos/o/r/releases/tags/"):
            tag = path.rsplit("/", 1)[1]
            for release in self.releases.values():
                if release["tag_name"] == tag and (authed or not release["draft"]):
                    return httpx.Response(200, json=release)
            return httpx.Response(404)
        if request.method == "GET" and path.startswith("/repos/o/r/releases/assets/"):
            asset_id = int(path.rsplit("/", 1)[1])
            release_id, asset = self.assets[asset_id]
            if self.releases[release_id]["draft"] and not authed:
                return httpx.Response(404)
            return httpx.Response(
                302, headers={"Location": f"{DOWNLOADS}/signed/{asset_id}"}
            )
        if request.method == "POST" and path == "/repos/o/r/releases":
            body = json.loads(request.content)
            release = self._create(body["tag_name"], body["draft"])
            return httpx.Response(201, json=release)
        if request.method == "PATCH" and path.startswith("/repos/o/r/releases/"):
            release = self.releases[int(path.rsplit("/", 1)[1])]
            body = json.loads(request.content)
            release["draft"] = body["draft"]
            if not release["draft"] and body.get("make_latest") != "false":
                self.latest = release["id"]
            if not release["draft"] and release.get("published_at") is None:
                self.published += 1
                release["published_at"] = f"2026-10-{self.published:02d}T00:00:00Z"
            if self.lose_publish_response:
                # Applied, but the response never arrived.
                return httpx.Response(502)
            return httpx.Response(200, json=release)
        if request.method == "GET" and path.startswith("/repos/o/r/releases/"):
            release = self.releases.get(int(path.rsplit("/", 1)[1]))
            if release is None or (release["draft"] and not authed):
                return httpx.Response(404)
            return httpx.Response(200, json=release)
        if request.method == "GET" and path == "/repos/o/r/releases":
            listing = [r for r in self.releases.values() if authed or not r["draft"]]
            listing.sort(key=lambda r: r["created_at"], reverse=True)
            return httpx.Response(200, json=listing)
        return httpx.Response(404)


def _index(tmp_path):
    """A releasable index: crosswalk feeds, places, classified edges and a
    NOTICE, published by the publish stage."""
    pytest.importorskip("geopandas")
    cache, _ = _build_index(tmp_path)
    audit = _publish_audit(cache)
    expanded = _publish_gen(
        cache,
        "expanded.json",
        "places_expanded.jsonl",
        PLACES,
        {
            "source": "expand",
            "overture_release": "2026-08-19.0",
            "places_overrides_sha256": None,
            "geometry_generation": audit["generation"],
        },
    )
    _publish_coverage(
        cache,
        {
            "overture_release": "2026-08-19.0",
            "expanded_generation": expanded["generation"],
        },
    )
    classify.classify(cache)
    licensing.license_index(cache)
    publish.publish(cache)
    return cache / "index"


def _manifest_bytes(**fields):
    manifest = {
        "snapshot_id": "0123456789abcdef",
        "schema_version": 3,
        "min_reader_version": "0.11.0",
        **fields,
    }
    return json.dumps(manifest).encode()


def test_pack_is_deterministic_and_lists_only_index_members(tmp_path):
    index_dir = _index(tmp_path)
    (index_dir / "stray.txt").write_text("not shipped")
    out = tmp_path / "out"
    out.mkdir()
    (out / "manifest.json").symlink_to(tmp_path / "elsewhere.json")
    assets, manifest = publisher.pack(index_dir, out)
    again, _ = publisher.pack(index_dir)
    name = contract.archive_name(manifest["snapshot_id"])
    archive = assets[name]
    assert archive == again[name]
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        assert tar.getnames() == list(contract.MEMBERS)
        assert all(m.mtime == 0 and m.uid == 0 and m.isreg() for m in tar.getmembers())
    digest = hashlib.sha256(archive).hexdigest()
    assert assets[name + ".sha256"] == f"{digest}  {name}\n".encode()
    assert manifest["archive"] == {
        "name": name,
        "sha256": digest,
        "bytes": len(archive),
    }
    assert set(manifest["members"]) == set(contract.MEMBERS)
    assert json.loads(assets["manifest.json"]) == manifest
    assert contract.compatible(manifest) == (True, None)
    # Written through the store: the planted symlink is replaced, not followed.
    assert (out / name).read_bytes() == archive
    assert not (out / "manifest.json").is_symlink()
    assert not (tmp_path / "elsewhere.json").exists()


def test_publish_drafts_verifies_publishes_and_round_trips(tmp_path):
    fake = FakeGitHub()
    software = fake.seed("v0.11.0", {})
    fake.seed("index-0000000000000001", {"manifest.json": _manifest_bytes()})
    # A newer but unreadable snapshot is passed over, never chosen.
    fake.seed(
        "index-0000000000000002",
        {"manifest.json": _manifest_bytes(schema_version=999)},
    )
    summary = publisher.publish_index(
        _index(tmp_path),
        cache_dir=tmp_path / "cache",
        repository="o/r",
        token="secret",
        api_url=API,
        out_dir=tmp_path / "out",
        transport=fake.transport(),
    )
    release = fake.releases[summary["release_id"]]
    assert release["draft"] is False and release["tag_name"] == summary["tag"]
    # Index data never becomes the repository's latest release.
    assert fake.latest == software["id"]
    assert set(summary["assets"]) == {
        contract.archive_name(summary["snapshot_id"]),
        contract.archive_name(summary["snapshot_id"]) + ".sha256",
        "manifest.json",
    }
    assert summary["skipped"] == []
    with httpx.Client(transport=fake.transport()) as anonymous:
        listed = anonymous.get(f"{API}/repos/o/r/releases").json()
    assert [r["tag_name"] for r in listed][0] == summary["tag"]
    # Seeded newer than ours by creation time: the round trip still resolved
    # ours because the newer one is incompatible.
    fake.seed(
        "index-0000000000000003", {"manifest.json": _manifest_bytes(schema_version=999)}
    )
    with httpx.Client(transport=fake.transport()) as anonymous:
        listing = anonymous.get(f"{API}/repos/o/r/releases").json()
        chosen, manifest, skipped = contract.newest_compatible(
            listing,
            lambda asset: json.loads(
                anonymous.get(asset["browser_download_url"]).content
            ),
        )
    assert manifest["snapshot_id"] == summary["snapshot_id"]
    assert skipped == [
        ("index-0000000000000003", "schema_version 999 is not one this transitio reads")
    ]


def test_a_corrupted_upload_leaves_the_release_a_draft(tmp_path):
    fake = FakeGitHub(corrupt="manifest.json")
    with pytest.raises(publisher.PublishIndexError, match="does not match"):
        publisher.publish_index(
            _index(tmp_path),
            cache_dir=tmp_path / "cache",
            repository="o/r",
            token="secret",
            api_url=API,
            out_dir=tmp_path / "out",
            transport=fake.transport(),
        )
    (release,) = fake.releases.values()
    assert release["draft"] is True
    # Anonymous clients never see the draft.
    with httpx.Client(transport=fake.transport()) as anonymous:
        assert anonymous.get(f"{API}/repos/o/r/releases").json() == []


def test_a_snapshot_is_published_once(tmp_path):
    fake = FakeGitHub()
    index_dir = _index(tmp_path)
    snapshot_id = json.loads((index_dir / "snapshot.json").read_text())["snapshot_id"]
    fake.seed(contract.release_tag(snapshot_id), {})
    with pytest.raises(publisher.PublishIndexError, match="already exists"):
        publisher.publish_index(
            index_dir,
            cache_dir=index_dir.parent,
            repository="o/r",
            token="secret",
            api_url=API,
            out_dir=tmp_path / "out",
            transport=fake.transport(),
        )
    assert len(fake.releases) == 1


@pytest.mark.parametrize(
    ("release", "reason"),
    [
        ({"tag_name": "index-0123456789abcdef", "draft": True}, None),
        ({"tag_name": "v1.0.0"}, None),
        ({"tag_name": "index-not-an-id"}, None),
        ({"tag_name": "index-0123456789abcdef", "assets": []}, "no readable manifest"),
    ],
)
def test_releases_a_client_must_not_take(release, reason):
    chosen, manifest, skipped = contract.newest_compatible([release], lambda a: None)
    assert chosen is None and manifest is None
    assert skipped == ([(release["tag_name"], reason)] if reason else [])


@pytest.mark.parametrize(
    ("manifest", "reason"),
    [
        ("text", "not an object"),
        (
            {"snapshot_id": "0123456789abcdef", "schema_version": [3]},
            "schema_version [3]",
        ),
        ({"schema_version": 3, "min_reader_version": "0.11.0"}, "no snapshot id"),
        ({"snapshot_id": "0123456789abcdef", "schema_version": 2}, "schema_version 2"),
        (
            {"snapshot_id": "0123456789abcdef", "schema_version": 3},
            "no min_reader_version",
        ),
        (
            {
                "snapshot_id": "0123456789abcdef",
                "schema_version": 3,
                "min_reader_version": "999.0.0",
            },
            "needs transitio >= 999.0.0",
        ),
    ],
)
def test_incompatible_manifests_say_why(manifest, reason):
    ok, why = contract.compatible(manifest)
    assert ok is False and reason in why


def test_the_cli_needs_a_token(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    assert publish_index.main(["--cache-dir", "nowhere"]) == 1
    assert "no token" in capsys.readouterr().err


def test_a_partial_index_is_not_releasable(tmp_path):
    cache, _ = _build_index(tmp_path)  # feeds only
    with pytest.raises(publisher.PublishIndexError, match="places.parquet: missing"):
        publisher.pack(cache / "index")


def test_a_lost_publish_response_is_reconciled_from_the_release(tmp_path):
    fake = FakeGitHub(lose_publish_response=True)
    summary = publisher.publish_index(
        _index(tmp_path),
        cache_dir=tmp_path / "cache",
        repository="o/r",
        token="secret",
        api_url=API,
        out_dir=tmp_path / "out",
        transport=fake.transport(),
    )
    assert fake.releases[summary["release_id"]]["draft"] is False


def test_an_index_built_from_superseded_generations_is_not_released(tmp_path):
    index_dir = _index(tmp_path)
    cache = index_dir.parent
    original = (index_dir / "snapshot.json").read_text()
    snapshot = json.loads(original)
    original_generations = dict(snapshot["generations"])
    assert set(snapshot["generations"]) >= {
        "raw/atlas.json",
        "crosswalk/feeds.json",
        "gazetteer/expanded.json",
        "classify/edges.json",
        "coverage/coverage.json",
    }
    publisher.pack(index_dir, cache_dir=cache)  # current: packs
    # An ancestor that moved, or an index recording no generations at all,
    # is refused before anything is packed.
    snapshot["generations"]["gazetteer/names.json"] = "old"
    (index_dir / "snapshot.json").write_text(json.dumps(snapshot))
    with pytest.raises(publisher.PublishIndexError, match="gazetteer/names.json"):
        publisher.pack(index_dir, cache_dir=cache)
    # The leaf that produced the edges, gone while its ancestors remain.
    snapshot["generations"].pop("license/licensed.json")
    (index_dir / "snapshot.json").write_text(json.dumps(snapshot))
    with pytest.raises(publisher.PublishIndexError, match="produced its edges"):
        publisher.pack(index_dir, cache_dir=cache)
    snapshot["generations"] = dict(original_generations)
    snapshot.pop("generations")
    (index_dir / "snapshot.json").write_text(json.dumps(snapshot))
    with pytest.raises(publisher.PublishIndexError, match="records no stage"):
        publisher.pack(index_dir, cache_dir=cache)
    (index_dir / "snapshot.json").write_text(original)
    # A leaf superseded after the publish stage wrote the index: no draft.
    _publish_gen(
        cache,
        "expanded.json",
        "places_expanded.jsonl",
        PLACES,
        {"source": "expand", "places_overrides_sha256": None},
    )
    fake = FakeGitHub()
    with pytest.raises(publisher.PublishIndexError, match="no longer current"):
        publisher.publish_index(
            index_dir,
            repository="o/r",
            token="secret",
            api_url=API,
            out_dir=tmp_path / "out",
            transport=fake.transport(),
            cache_dir=cache,
        )
    assert fake.releases == {}


def test_releases_are_ordered_by_publication_not_commit_time():
    manifests = {
        "index-0000000000000001": _manifest_bytes(snapshot_id="0000000000000001"),
        "index-0000000000000002": _manifest_bytes(snapshot_id="0000000000000002"),
    }
    releases = [
        {
            "id": 1,
            "tag_name": "index-0000000000000001",
            "created_at": "2026-09-02T00:00:00Z",  # a later commit,
            "published_at": "2026-09-01T00:00:00Z",  # published first
            "assets": [{"name": "manifest.json", "tag": "index-0000000000000001"}],
        },
        {
            "id": 2,
            "tag_name": "index-0000000000000002",
            "created_at": "2026-09-01T00:00:00Z",
            "published_at": "2026-09-03T00:00:00Z",
            "assets": [{"name": "manifest.json", "tag": "index-0000000000000002"}],
        },
    ]
    _, manifest, _ = contract.newest_compatible(
        releases, lambda asset: json.loads(manifests[asset["tag"]])
    )
    assert manifest["snapshot_id"] == "0000000000000002"


def test_overrides_edited_after_the_build_are_not_released(tmp_path):
    from test_index_place_overrides import write_overrides

    index_dir = _index(tmp_path)
    cache = index_dir.parent
    publisher.pack(index_dir, cache_dir=cache, overrides_dir=None)
    edited = write_overrides(tmp_path)
    (edited / "edges.yaml").write_text(
        "- feed: f-a\n  place: Q1757\n  remove_edge: true\n"
    )
    with pytest.raises(publisher.PublishIndexError, match="edges.yaml is not the one"):
        publisher.pack(index_dir, cache_dir=cache, overrides_dir=edited)


def test_a_stage_that_moves_during_upload_stops_the_flip(tmp_path):
    index_dir = _index(tmp_path)
    cache = index_dir.parent

    def supersede():
        _publish_gen(
            cache,
            "expanded.json",
            "places_expanded.jsonl",
            PLACES,
            {"source": "expand", "places_overrides_sha256": None},
        )

    fake = FakeGitHub(on_upload=supersede)
    with pytest.raises(publisher.PublishIndexError, match="no longer current"):
        publisher.publish_index(
            index_dir,
            repository="o/r",
            token="secret",
            api_url=API,
            out_dir=tmp_path / "out",
            transport=fake.transport(),
            cache_dir=cache,
        )
    (release,) = fake.releases.values()
    assert release["draft"] is True


def test_a_crawl_completed_after_the_build_is_not_released(tmp_path):
    index_dir = _index(tmp_path)
    cache = index_dir.parent
    publisher.pack(index_dir, cache_dir=cache)
    _write_crawl(cache, "f-a", ["s1,60.2,24.9\n"])
    with pytest.raises(publisher.PublishIndexError, match="the crawl changed"):
        publisher.pack(index_dir, cache_dir=cache)


def test_an_ingest_re_run_after_the_build_is_not_released(tmp_path):
    index_dir = _index(tmp_path)
    cache = index_dir.parent
    publisher.pack(index_dir, cache_dir=cache)
    (tmp_path / "again").mkdir()
    archive = _atlas_archive(
        tmp_path / "again",
        [{"id": "f-a", "spec": "gtfs", "name": "A", "urls": {"static_current": "x"}}],
    )
    atlas.ingest(cache, archive=archive, commit="b" * 40)
    with pytest.raises(publisher.PublishIndexError, match="raw/atlas.json"):
        publisher.pack(index_dir, cache_dir=cache)


def test_a_moved_repository_is_followed_when_reading_and_reported_when_writing(
    tmp_path,
):
    fake = FakeGitHub()
    fake.seed(
        "index-0000000000000001",
        {"manifest.json": _manifest_bytes(snapshot_id="0000000000000001")},
    )
    with httpx.Client(base_url=API, transport=fake.transport()) as anonymous:
        listing = contract.list_releases(anonymous, "old/r")
        _, manifest, _ = contract.newest_compatible(
            listing, lambda asset: contract.read_manifest(anonymous, asset)
        )
    assert manifest["snapshot_id"] == "0000000000000001"
    with pytest.raises(publisher.PublishIndexError, match="has moved to"):
        publisher.publish_index(
            _index(tmp_path),
            cache_dir=tmp_path / "cache",
            repository="old/r",
            token="secret",
            api_url=API,
            out_dir=tmp_path / "out",
            transport=fake.transport(),
        )
    assert [r["tag_name"] for r in fake.releases.values()] == ["index-0000000000000001"]


def test_an_altered_notice_is_not_released(tmp_path):
    index_dir = _index(tmp_path)
    publisher.pack(index_dir, cache_dir=index_dir.parent)
    (index_dir / "NOTICE").write_text("someone else's attribution\n")
    with pytest.raises(publisher.PublishIndexError, match="notice_sha256"):
        publisher.pack(index_dir, cache_dir=index_dir.parent)
