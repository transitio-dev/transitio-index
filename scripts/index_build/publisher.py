"""Publish a built index as a GitHub release: the producer side of refresh.

:func:`pack` turns ``<cache>/index`` into ``transitio-index-<snapshot_id>.tar.gz``
(gzip, deterministic: same index, same bytes), its ``.sha256`` and the
immutable ``manifest.json`` a client reads first. :func:`publish_index` then
creates the release ``index-<snapshot_id>`` as a **draft**, uploads the three
assets, downloads each one back and checks its digest, and only then flips
the draft to published — a plain release is listable the moment it exists, so
publishing the draft is the atomic commit. Last it lists the releases as an
anonymous client would and confirms the newest compatible one is the snapshot
just published.

The index is read through the reader first, so nothing the reader would
refuse can ship. The token is only ever sent to the API host and the upload
host GitHub names in the release it created.
"""

import contextlib
import gzip
import hashlib
import io
import json
import os
import pathlib
import shutil
import tarfile
import tempfile

import httpx

from index_build import crawl, store
from transitio.index import read_index
from transitio.index import release as contract

TIMEOUT = 60.0
# The stage locks the publish stage holds, in its order.
STAGE_LOCKS = (
    "raw",
    "crosswalk",
    "resolve",
    "gazetteer",
    "coverage",
    "classify",
    "curate",
    "prune",
)
USER_AGENT = "transitio-publish-index"


class PublishIndexError(RuntimeError):
    """The snapshot could not be packed or published."""


def _sha256(data):
    return hashlib.sha256(data).hexdigest()


def _members(index_dir):
    """The index files to pack, in a fixed order, as ``(name, bytes)``, read
    under the index's writer lock so a publish cannot interleave with the
    capture."""
    directory = store.open_directory(pathlib.Path(index_dir))
    try:
        with store.exclusive_writer(directory):
            found = []
            for name in contract.MEMBERS:
                try:
                    found.append((name, store.read_bytes(directory, name)))
                except (store.MissingEntry, FileNotFoundError):
                    raise PublishIndexError(
                        f"{index_dir / name}: missing; a release ships every member"
                    ) from None
            return found
    finally:
        directory.close()


def _archive(members):
    """A deterministic gzip tar of ``members``: fixed metadata, no timestamps."""
    sink = io.BytesIO()
    with gzip.GzipFile(fileobj=sink, mode="wb", mtime=0) as compressed:
        with tarfile.open(
            fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT
        ) as tar:
            for name, data in members:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o644
                info.mtime = 0
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                tar.addfile(info, io.BytesIO(data))
    return sink.getvalue()


def _lock_stages(stack, cache_dir):
    """Hold every lock the publish stage holds, in its order: the stages,
    then the crawl."""
    for subdir in STAGE_LOCKS:
        held = store.open_subdir(cache_dir, subdir)
        stack.callback(held.close)
        stack.enter_context(store.exclusive_writer(held))
    stack.enter_context(crawl.reading(cache_dir))


def _current(cache_dir, snapshot, overrides_dir):
    """Refuse an index that is no longer what a publish stage would write
    now: a recorded stage generation (the leaf that produced each shipped
    table, or any ancestor) that is not the current pointer, or an override
    file whose digest is not the one applied. An index recording none of
    this (built before it was recorded) cannot be checked and is refused."""
    from index_build import overrides

    generations = snapshot.get("generations")
    leaves = snapshot.get("leaves")
    if (
        not isinstance(generations, dict)
        or not isinstance(leaves, dict)
        or not all(
            isinstance(key, str) and "/" in key and isinstance(value, str)
            for key, value in generations.items()
        )
    ):
        raise PublishIndexError(
            "the index records no stage generations to check; re-run the publish "
            "stage"
        )
    if snapshot.get("licensed") is not True:
        raise PublishIndexError(
            "the index is not licensed; run the license stage before publishing"
        )
    tables = {"feeds"}
    if snapshot.get("edges_sha256"):
        tables.add("edges")
    if snapshot.get("places_sha256"):
        tables.add("places")
    for table in sorted(tables):
        leaf = leaves.get(table)
        if not isinstance(leaf, str) or leaf not in generations:
            raise PublishIndexError(
                f"the index records no generation for the stage that produced its "
                f"{table}; re-run the publish stage"
            )
    for key, generation in sorted(generations.items()):
        subdir, pointer = key.split("/", 1)
        path = cache_dir / subdir / pointer
        current = None
        if path.is_symlink() or path.exists():
            try:
                handle, manifest = store.resolve(cache_dir / subdir, pointer)
            except (store.StoreError, ValueError) as error:
                raise PublishIndexError(f"{key}: unreadable: {error}") from error
            with handle:
                current = manifest.get("generation")
        if current != generation:
            raise PublishIndexError(
                f"the index was built from {key} generation {generation!r}, which "
                f"is no longer current; re-run the publish stage"
            )
    if "crawl_digest" not in snapshot:
        raise PublishIndexError(
            "the index records no crawl digest; re-run the publish stage"
        )
    if crawl.states_digest(cache_dir) != snapshot["crawl_digest"]:
        raise PublishIndexError(
            "the crawl changed since the publish stage read it; re-run the "
            "publish stage"
        )
    for name, key, digest in (
        ("edges.yaml", "overrides_sha256", overrides.edges_digest),
        ("feeds.yaml", "feeds_overrides_sha256", overrides.feeds_digest),
        ("places.yaml", "places_overrides_sha256", overrides.places_digest),
    ):
        if key not in snapshot:
            raise PublishIndexError(
                f"the index records no {name} digest; re-run the publish stage"
            )
        if digest(overrides_dir) != snapshot[key]:
            raise PublishIndexError(
                f"{name} is not the one the index applied; re-run the publish stage"
            )


def pack(index_dir, out_dir=None, *, cache_dir=None, overrides_dir=None):
    """The three release assets, as ``(bytes by asset name, manifest)``, and
    written into ``out_dir`` when given. The members are read once, and the
    reader validates that very copy (staged privately), so a build replacing
    the index meanwhile cannot put unvalidated bytes under a validated id.
    With ``cache_dir``, the stage generations the index records must still
    be the current ones, checked under the stage locks the publish stage
    takes, in its order, so nothing moves between the check and the capture."""
    with contextlib.ExitStack() as stack:
        if cache_dir is not None:
            _lock_stages(stack, cache_dir)
        members = _members(index_dir)
        if cache_dir is not None:
            snapshot = json.loads(dict(members)["snapshot.json"].decode("utf-8"))
            _current(cache_dir, snapshot, overrides_dir)
            if snapshot.get("notice_sha256") != _sha256(dict(members)["NOTICE"]):
                raise PublishIndexError(
                    "NOTICE does not match the snapshot's notice_sha256; re-run the "
                    "publish stage"
                )
    staged = pathlib.Path(tempfile.mkdtemp(prefix="transitio-index-"))
    try:
        directory = store.open_directory(staged)
        try:
            for name, data in members:
                store.write_bytes(directory, name, data)
        finally:
            directory.close()
        snapshot = read_index(staged).snapshot
    finally:
        shutil.rmtree(staged, ignore_errors=True)
    snapshot_id = snapshot["snapshot_id"]
    if not contract.is_snapshot_id(snapshot_id):
        raise PublishIndexError(f"snapshot_id {snapshot_id!r} is not one publish mints")
    archive = _archive(members)
    name = contract.archive_name(snapshot_id)
    manifest = {
        "snapshot_id": snapshot_id,
        "schema_version": snapshot["schema_version"],
        "discovery_semantics_version": snapshot.get("discovery_semantics_version"),
        "min_reader_version": snapshot.get("min_reader_version"),
        "built_with": snapshot.get("built_with"),
        "built_at": snapshot.get("built_at"),
        "counts": snapshot.get("counts"),
        "archive": {"name": name, "sha256": _sha256(archive), "bytes": len(archive)},
        "members": {member: _sha256(data) for member, data in members},
        # What the index descends from, so the same check can run again
        # right before the release goes public.
        "lineage": {
            key: snapshot.get(key)
            for key in (
                "generations",
                "leaves",
                "edges_sha256",
                "places_sha256",
                "overrides_sha256",
                "feeds_overrides_sha256",
                "places_overrides_sha256",
                "crawl_digest",
                "licensed",
            )
        },
    }
    ok, reason = contract.compatible(manifest)
    if not ok:
        raise PublishIndexError(f"the snapshot would not be readable: {reason}")
    assets = {
        name: archive,
        name + contract.CHECKSUM_SUFFIX: f"{_sha256(archive)}  {name}\n".encode(),
        contract.MANIFEST_NAME: (
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        ).encode(),
    }
    if out_dir is not None:
        write_assets(assets, out_dir)
    return assets, manifest


def write_assets(assets, out_dir):
    """Write the assets into ``out_dir`` through the store: atomic replace,
    never through a symlink someone left under a fixed name."""
    os.makedirs(out_dir, exist_ok=True)
    directory = store.open_directory(pathlib.Path(out_dir))
    try:
        for name, data in assets.items():
            store.write_bytes(directory, name, data)
    finally:
        directory.close()


def _client(api_url, token, transport=None):
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(
        base_url=api_url,
        headers=headers,
        timeout=TIMEOUT,
        follow_redirects=False,
        transport=transport,
    )


def _check(response, what):
    if response.status_code in (301, 302, 307, 308):
        raise PublishIndexError(
            f"{what}: the repository has moved to "
            f"{response.headers.get('Location', 'another location')}; use its new "
            "name"
        )
    if response.status_code // 100 != 2:
        raise PublishIndexError(f"{what}: HTTP {response.status_code}")
    return response


def _download(client, url, what, limit=contract.MAX_ASSET_BYTES):
    try:
        return contract.download(client, url, what, limit)
    except contract.ReleaseError as error:
        raise PublishIndexError(str(error)) from error


def _release_state(client, repo, release_id):
    """``"published"``, ``"draft"`` or ``"unknown"``: the release as GitHub
    holds it now, for a flip whose response was lost."""
    try:
        response = client.get(f"{repo}/releases/{release_id}")
        if response.status_code == 200:
            return "draft" if response.json().get("draft") else "published"
    except (httpx.HTTPError, ValueError):
        pass
    return "unknown"


def publish_index(
    index_dir,
    *,
    repository,
    token,
    api_url=contract.API_URL,
    out_dir,
    transport=None,
    cache_dir,
    overrides_dir=None,
):
    """Pack ``index_dir`` and publish it as the release for its snapshot;
    returns a summary. A failure before the draft is flipped leaves it a
    draft (invisible to clients); a failed round trip afterwards reports a
    release that is already published."""
    assets, manifest = pack(
        index_dir, out_dir, cache_dir=cache_dir, overrides_dir=overrides_dir
    )
    snapshot_id = manifest["snapshot_id"]
    tag = contract.release_tag(snapshot_id)
    repo = f"/repos/{repository}"
    with _client(api_url, token, transport) as client:
        try:
            existing = client.get(f"{repo}/releases/tags/{tag}", follow_redirects=True)
        except httpx.HTTPError as error:
            raise PublishIndexError(f"listing the release: {error}") from error
        if existing.status_code == 200:
            raise PublishIndexError(
                f"release {tag} already exists; a snapshot is published once"
            )
        if existing.status_code != 404:
            _check(existing, "looking up the release")
        created = _check(
            client.post(
                f"{repo}/releases",
                json={
                    "tag_name": tag,
                    "name": f"Index snapshot {snapshot_id}",
                    "body": f"transitio feed index snapshot {snapshot_id}",
                    "draft": True,
                    "prerelease": False,
                    # Index data never displaces the software release
                    # GitHub shows and serves as the repository's latest.
                    "make_latest": "false",
                },
            ),
            "creating the draft release",
        ).json()
        release_id = created["id"]
        # The upload host is the one GitHub names, template stripped.
        upload_url = created["upload_url"].split("{", 1)[0]
        uploaded = {}
        for asset, data in assets.items():
            response = _check(
                client.post(
                    upload_url,
                    params={"name": asset},
                    content=data,
                    headers={"Content-Type": "application/octet-stream"},
                ),
                f"uploading {asset}",
            ).json()
            # Read back through the API, digest compared with the bytes just
            # sent: a truncated or altered upload never gets published.
            fetched = _download(
                client, response["url"], f"verifying {asset}", len(data)
            )
            if _sha256(fetched) != _sha256(data):
                raise PublishIndexError(
                    f"{asset}: the uploaded asset does not match; release left a draft"
                )
            uploaded[asset] = response.get("browser_download_url")
        # The flip is the commit, made under the stage locks with the
        # lineage checked once more: uploads take long enough for a stage to
        # have moved on. A lost response leaves the outcome ambiguous, so
        # the release is read back before deciding.
        with contextlib.ExitStack() as stack:
            _lock_stages(stack, cache_dir)
            _current(cache_dir, manifest["lineage"], overrides_dir)
            try:
                _check(
                    client.patch(
                        f"{repo}/releases/{release_id}",
                        json={"draft": False, "make_latest": "false"},
                    ),
                    "publishing the release",
                )
            except (PublishIndexError, httpx.HTTPError) as error:
                state = _release_state(client, repo, release_id)
                if state != "published":
                    raise PublishIndexError(
                        f"publishing release {tag} failed ({error}); its state is "
                        f"{state}"
                    ) from error
    # The round trip, as an anonymous client: the newest compatible release
    # must be the one just published. From here on the release is public.
    try:
        with _client(api_url, None, transport) as anonymous:
            listing = contract.list_releases(anonymous, repository)
            release, found, skipped = contract.newest_compatible(
                listing, lambda asset: contract.read_manifest(anonymous, asset)
            )
    except Exception as error:  # noqa: B902 - the release is public: say so
        raise PublishIndexError(
            f"release {tag} is published, but the round trip failed: {error}"
        ) from error
    if found is None or found.get("snapshot_id") != snapshot_id:
        raise PublishIndexError(
            f"release {tag} is published, but the round trip resolved "
            f"{found and found.get('snapshot_id')!r} instead of {snapshot_id!r}"
        )
    return {
        "snapshot_id": snapshot_id,
        "tag": tag,
        "release_id": release_id,
        "assets": uploaded,
        "skipped": [{"tag": name, "reason": reason} for name, reason in skipped],
    }
