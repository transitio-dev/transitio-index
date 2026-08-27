import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(REPO / "scripts"))

from index_build import store  # noqa: E402


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


def chunks(text):
    def write():
        yield text

    return write


def publish(directory, feeds="a\n", operators="b\n", manifest=None):
    return store.publish(
        directory,
        "atlas.json",
        {"feeds.jsonl": chunks(feeds), "operators.jsonl": chunks(operators)},
        manifest or {"source": "atlas"},
    )


@pytest.mark.parametrize(
    "name",
    [
        "feeds.jsonl",
        "atlas_operators.jsonl",
        ".lock",
        "gen-0011223344556677",
        "\u9752\u68ee\u5e02\u55b6.jsonl",
    ],
)
def test_ordinary_names_are_accepted(name):
    assert store.safe_component(name)


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "a/b",
        "a\\b",
        "/etc/passwd",
        "D:escape",
        "C:",
        "feeds:stream",
        "CON",
        "con.txt",
        "COM1",
        "COM\u00b9",
        "CONIN$",
        "lpt3.jsonl",
        "feeds.jsonl ",
        "feeds.jsonl.",
        "bell\x07.jsonl",
        "pipe|.jsonl",
    ],
)
def test_names_windows_would_reinterpret_are_refused(name):
    # Judged by Windows rules everywhere: a cache written on one platform
    # must not mean something different on another.
    assert not store.safe_component(name)


def test_directory_operations_refuse_an_unsafe_name(tmp_path):
    with store.open_directory(tmp_path) as directory:
        for call in (
            lambda: directory.open("../escape", os.O_RDONLY),
            lambda: directory.unlink("../escape"),
            lambda: directory.stat("../escape"),
            lambda: directory.mkdir("../escape"),
            lambda: directory.rmdir("../escape"),
            lambda: directory.replace("ok", "../escape"),
            lambda: directory.subdirectory("../escape"),
        ):
            with pytest.raises(store.StoreError, match="inside the cache"):
                call()


def test_publish_writes_a_generation_and_points_at_it(tmp_path):
    manifest = publish(tmp_path)

    generation, resolved = store.resolve(tmp_path, "atlas.json")

    assert generation.name == manifest["generation"]
    assert resolved == manifest
    assert (generation / "feeds.jsonl").read_text() == "a\n"
    assert set(manifest["digests"]) == {"feeds.jsonl", "operators.jsonl"}
    # The generation carries the same manifest the pointer does.
    assert json.loads((generation / "manifest.json").read_text()) == manifest


def test_pointers_sharing_a_tag_do_not_prune_each_other(tmp_path, monkeypatch):
    # A tag collision (a 32-bit hash, or two case-variant names on a
    # case-sensitive filesystem) must not let one pointer prune another's
    # live generation. Forced here by pinning the tag so the test is
    # deterministic on any filesystem.
    monkeypatch.setattr(store, "_generation_tag", lambda pointer: "deadbeef")
    other = store.publish(
        tmp_path, "mdb.json", {"m.jsonl": chunks("m")}, {"source": "mdb"}
    )
    for index in range(store.KEEP_GENERATIONS + 2):
        publish(tmp_path, feeds=f"{index}\n")

    generation, _ = store.resolve(tmp_path, "mdb.json")
    assert generation.name == other["generation"]
    with generation:
        assert generation.read_bytes("m.jsonl") == b"m"


def test_prune_fails_closed_when_a_pointer_is_unreadable(tmp_path, monkeypatch):
    # A transient read failure on another pointer must abort pruning, not
    # treat that pointer as referencing nothing and delete its generation.
    monkeypatch.setattr(store, "_generation_tag", lambda pointer: "deadbeef")
    other = store.publish(
        tmp_path, "mdb.json", {"m.jsonl": chunks("m")}, {"source": "mdb"}
    )
    real_read = store.read_bytes

    def flaky_read(directory, name):
        if name == "mdb.json":
            raise OSError("transient")
        return real_read(directory, name)

    monkeypatch.setattr(store, "read_bytes", flaky_read)
    for index in range(store.KEEP_GENERATIONS + 2):
        publish(tmp_path, feeds=f"{index}\n")
    monkeypatch.undo()

    # mdb.json's generation was never pruned despite the shared tag.
    assert (tmp_path / other["generation"] / "m.jsonl").read_text() == "m"


def test_cleanup_preserves_the_generation_when_pointer_state_is_unknown(
    tmp_path, monkeypatch
):
    # An async-style failure after the pointer swap but before activation,
    # combined with an unreadable pointer, must NOT delete the now-live
    # generation.
    first = publish(tmp_path, feeds="first\n")
    real_read = store.read_bytes
    state = {"swapped": False}
    real_write = store.write_file

    def watched_write(directory, name, write):
        result = real_write(directory, name, write)
        if name == "atlas.json":
            state["swapped"] = True
            raise RuntimeError("async-like error right after the swap")
        return result

    def flaky_read(directory, name):
        if state["swapped"] and name == "atlas.json":
            raise OSError("transient")
        return real_read(directory, name)

    monkeypatch.setattr(store, "write_file", watched_write)
    monkeypatch.setattr(store, "read_bytes", flaky_read)

    with pytest.raises(RuntimeError):
        publish(tmp_path, feeds="second\n")
    monkeypatch.undo()

    # The pointer swap did happen, and its generation survives (not deleted
    # by the finally despite the unreadable pointer).
    generation, manifest = store.resolve(tmp_path, "atlas.json")
    assert manifest["generation"] != first["generation"]
    with generation:
        assert generation.read_bytes("feeds.jsonl") == b"second\n"


def test_two_pointers_do_not_prune_each_other(tmp_path):
    # Two catalogue sources sharing one store: publishing through one must
    # not delete the generation the other still names.
    other = store.publish(
        tmp_path, "mdb.json", {"m.jsonl": chunks("m")}, {"source": "mdb"}
    )

    # Publish through "atlas.json" enough times to exceed KEEP_GENERATIONS.
    for index in range(store.KEEP_GENERATIONS + 2):
        publish(tmp_path, feeds=f"{index}\n")

    # mdb.json's generation survived and still resolves.
    generation, manifest = store.resolve(tmp_path, "mdb.json")
    assert generation.name == other["generation"]
    with generation:
        assert generation.read_bytes("m.jsonl") == b"m"


def test_a_sync_failure_after_activation_keeps_the_new_generation(
    tmp_path, monkeypatch
):
    # The pointer is already swapped when the directory sync runs; a sync
    # failure there must not delete the now-active generation.
    first = publish(tmp_path, feeds="first\n")
    calls = {"n": 0}
    real_sync = store.Directory.sync

    def flaky_sync(self):
        calls["n"] += 1
        # The final sync in publish is the top-level directory sync, after
        # the pointer write.
        if calls["n"] >= 2:
            raise OSError("sync failed")
        return real_sync(self)

    monkeypatch.setattr(store.Directory, "sync", flaky_sync)

    with pytest.raises(OSError):
        publish(tmp_path, feeds="second\n")

    monkeypatch.undo()
    # The pointer was swapped and its generation is intact and resolvable.
    generation, manifest = store.resolve(tmp_path, "atlas.json")
    assert manifest["generation"] != first["generation"]
    with generation:
        assert generation.read_bytes("feeds.jsonl") == b"second\n"


def test_a_name_collision_never_deletes_a_pre_existing_generation(
    tmp_path, monkeypatch
):
    good = publish(tmp_path, feeds="keep\n")
    existing = good["generation"]
    # A real directory the retry will collide with, with a marker inside it.
    tag = store._generation_tag("atlas.json")
    squatter = tmp_path / f"gen-{tag}-cccccccccccccccc"
    squatter.mkdir()
    (squatter / "marker.txt").write_text("do not touch\n")

    names = iter(["cccccccccccccccc", "dddddddddddddddd"])
    real_urandom = store.os.urandom

    def scripted(size, names=names, real=real_urandom):
        try:
            return bytes.fromhex(next(names))
        except StopIteration:
            return real(size)

    monkeypatch.setattr(store.os, "urandom", scripted)

    publish(tmp_path, feeds="new\n")

    # The colliding directory and the pre-existing generation both survive.
    assert (squatter / "marker.txt").read_text() == "do not touch\n"
    assert (tmp_path / existing / "feeds.jsonl").read_text() == "keep\n"


def test_a_failed_publish_leaves_no_orphan_generation(tmp_path, monkeypatch):
    publish(tmp_path, feeds="good\n")
    before = {p.name for p in tmp_path.iterdir() if p.name.startswith("gen-")}
    monkeypatch.setattr(store, "MAX_RESOLUTION_BYTES", 4)

    with pytest.raises(store.StoreError, match="ceiling"):
        publish(tmp_path, feeds="x" * 64)

    after = {p.name for p in tmp_path.iterdir() if p.name.startswith("gen-")}
    # No new generation directory was stranded by the failure.
    assert after == before


def test_a_new_generation_does_not_disturb_the_old_one(tmp_path):
    first = publish(tmp_path, feeds="old\n")
    second = publish(tmp_path, feeds="new\n")

    assert first["generation"] != second["generation"]
    assert (tmp_path / first["generation"] / "feeds.jsonl").read_text() == "old\n"
    generation, _ = store.resolve(tmp_path, "atlas.json")
    assert generation.name == second["generation"]


def test_resolve_rejects_a_tampered_artifact(tmp_path):
    manifest = publish(tmp_path)
    (tmp_path / manifest["generation"] / "feeds.jsonl").write_text("tampered\n")

    with pytest.raises(store.StoreError, match="digest mismatch"):
        store.resolve(tmp_path, "atlas.json")


@pytest.mark.parametrize(
    "generation",
    ["../escape", "/etc", "gen-nothex", "", "gen-0011223344556677/x", "D:escape"],
)
def test_resolve_rejects_a_manifest_naming_a_bad_generation(tmp_path, generation):
    publish(tmp_path)
    pointer = json.loads((tmp_path / "atlas.json").read_text())
    pointer["generation"] = generation
    (tmp_path / "atlas.json").write_text(json.dumps(pointer))

    with pytest.raises(store.StoreError, match="names no generation|inside the cache"):
        store.resolve(tmp_path, "atlas.json")


@pytest.mark.parametrize(
    "name", ["../atlas.json", "D:escape", "feeds:stream", "CON", "feeds.jsonl "]
)
def test_resolve_rejects_a_manifest_naming_a_file_outside_the_generation(
    tmp_path, name
):
    manifest = publish(tmp_path)
    pointer = json.loads((tmp_path / "atlas.json").read_text())
    pointer["digests"] = {name: manifest["digests"]["feeds.jsonl"]}
    (tmp_path / "atlas.json").write_text(json.dumps(pointer))

    with pytest.raises(store.StoreError, match="manifest names"):
        store.resolve(tmp_path, "atlas.json")


@pytest.mark.parametrize("digests", [None, {}, [], "nope", {"feeds.jsonl": "short"}])
def test_resolve_rejects_a_manifest_that_declares_nothing_verifiable(tmp_path, digests):
    # A manifest with no usable digests would otherwise "verify" a
    # generation by checking nothing at all.
    publish(tmp_path)
    pointer = json.loads((tmp_path / "atlas.json").read_text())
    pointer["digests"] = digests
    (tmp_path / "atlas.json").write_text(json.dumps(pointer))

    with pytest.raises(store.StoreError, match="declares no artifacts|not a SHA-256"):
        store.resolve(tmp_path, "atlas.json")


def test_resolve_rejects_a_pointer_that_disagrees_with_the_generation(tmp_path):
    manifest = publish(tmp_path)
    pointer = json.loads((tmp_path / "atlas.json").read_text())
    pointer["source"] = "tampered"
    (tmp_path / "atlas.json").write_text(json.dumps(pointer))

    with pytest.raises(store.StoreError, match="does not match the pointer manifest"):
        store.resolve(tmp_path, "atlas.json")

    assert manifest["generation"]


@pytest.mark.parametrize("name", ["../atlas.json", "manifest.json", "absent.jsonl"])
def test_the_generation_serves_only_what_it_verified(tmp_path, name):
    # Reads come from the descriptors resolve() hashed, so a name the
    # manifest never declared was never verified and is not readable.
    publish(tmp_path, feeds="held\n")

    with store.resolve(tmp_path, "atlas.json")[0] as generation:
        assert generation.read_bytes("feeds.jsonl") == b"held\n"
        with pytest.raises(store.StoreError, match="not a verified artifact"):
            generation.read_bytes(name)


def test_the_generation_survives_its_artifacts_being_replaced(tmp_path):
    # A held directory descriptor does not pin its children; the retained
    # Serving from those captured bytes is what makes this safe; Windows
    # additionally refuses to unlink a file that is open, a different route
    # to the same guarantee.
    manifest = publish(tmp_path, feeds="original\n")

    with store.resolve(tmp_path, "atlas.json")[0] as generation:
        target = tmp_path / manifest["generation"] / "feeds.jsonl"
        try:
            target.unlink()
        except OSError:
            assert os.name == "nt", "unlink refused on a platform that allows it"
        else:
            target.write_text("swapped\n")

        assert generation.read_bytes("feeds.jsonl") == b"original\n"


def test_the_recorded_digest_is_the_hash_of_the_bytes_on_disk(tmp_path):
    # Text mode would translate newlines on Windows and the digest would
    # describe something other than the file it names.
    manifest = publish(tmp_path, feeds="one\ntwo\n")

    written = (tmp_path / manifest["generation"] / "feeds.jsonl").read_bytes()

    assert written == b"one\ntwo\n"
    assert hashlib.sha256(written).hexdigest() == manifest["digests"]["feeds.jsonl"]


def test_subdirectory_never_creates(tmp_path):
    with store.open_directory(tmp_path) as directory:
        with pytest.raises(store.StoreError):
            directory.subdirectory("gen-0011223344556677")

    assert not (tmp_path / "gen-0011223344556677").exists()


def test_an_artifact_over_the_ceiling_is_refused_before_the_pointer_moves(
    tmp_path, monkeypatch
):
    # write_file must abort before the pointer swap, so a generation
    # resolve() would reject never becomes the published one.
    publish(tmp_path, feeds="good\n")
    good = json.loads((tmp_path / "atlas.json").read_text())
    monkeypatch.setattr(store, "MAX_ARTIFACT_BYTES", 8)

    with pytest.raises(store.StoreError, match="artifact ceiling"):
        publish(tmp_path, feeds="x" * 64)

    # The pointer on disk is byte-for-byte the earlier good one: the
    # oversized generation never became what it names.
    assert json.loads((tmp_path / "atlas.json").read_text()) == good


def test_a_generation_over_the_aggregate_ceiling_is_refused(tmp_path, monkeypatch):
    # Each file is under the per-artifact ceiling, but together they exceed
    # the whole-generation ceiling; publish must refuse before the pointer
    # moves, or resolve would then reject the active generation forever.
    publish(tmp_path, feeds="good\n")
    good = json.loads((tmp_path / "atlas.json").read_text())
    monkeypatch.setattr(store, "MAX_RESOLUTION_BYTES", 12)

    with pytest.raises(store.StoreError, match="generation over the"):
        publish(tmp_path, feeds="x" * 8, operators="y" * 8)

    assert json.loads((tmp_path / "atlas.json").read_text()) == good


@pytest.mark.parametrize(
    "artifacts",
    [
        {},
        {"manifest.json": lambda: ["x"]},
        {"Manifest.json": lambda: ["x"]},  # case-equivalent to the reserved name
        {".lock": lambda: ["x"]},
        {"../escape": lambda: ["x"]},
    ],
)
def test_publish_refuses_a_bad_artifact_mapping(tmp_path, artifacts):
    good = publish(tmp_path)
    pointer = json.loads((tmp_path / "atlas.json").read_text())

    with pytest.raises(store.StoreError):
        store.publish(tmp_path, "atlas.json", artifacts, {"source": "atlas"})

    # The bad input never became the published generation.
    assert json.loads((tmp_path / "atlas.json").read_text()) == pointer
    assert good["generation"]


@pytest.mark.parametrize("pointer", [".lock", "manifest.json", "../escape"])
def test_publish_refuses_a_reserved_pointer(tmp_path, pointer):
    good = publish(tmp_path)

    with pytest.raises(store.StoreError, match="pointer"):
        store.publish(
            tmp_path, pointer, {"feeds.jsonl": chunks("x")}, {"source": "atlas"}
        )

    assert good["generation"]


def test_publish_refuses_case_colliding_artifacts(tmp_path):
    with pytest.raises(store.StoreError, match="collide"):
        store.publish(
            tmp_path,
            "atlas.json",
            {"Feeds.jsonl": chunks("a"), "feeds.jsonl": chunks("b")},
            {"source": "atlas"},
        )


def test_open_regular_path_accepts_a_string_path(tmp_path):
    target = tmp_path / "file.txt"
    target.write_text("hi")

    handle = store.open_regular_path(str(target))
    try:
        assert os.read(handle, 8) == b"hi"
    finally:
        os.close(handle)


def test_open_regular_path_refuses_a_string_symlink(tmp_path):
    if not can_symlink(tmp_path):
        pytest.skip("this platform will not let the test create a symlink")
    target = tmp_path / "file.txt"
    target.write_text("hi")
    link = tmp_path / "link.txt"
    link.symlink_to(target)

    with pytest.raises(store.StoreError, match="is a symlink"):
        store.open_regular_path(str(link))


def test_a_symlinked_file_entry_is_refused(tmp_path):
    if not can_symlink(tmp_path):
        pytest.skip("this platform will not let the test create a symlink")
    publish(tmp_path)
    real = tmp_path / "atlas.json"
    moved = tmp_path / "real.json"
    real.rename(moved)
    real.symlink_to(moved)

    with pytest.raises(store.StoreError, match="is a symlink"):
        store.resolve(tmp_path, "atlas.json")


def test_resolve_rejects_an_artifact_that_grew_past_the_ceiling(tmp_path, monkeypatch):
    manifest = publish(tmp_path, feeds="small\n")
    monkeypatch.setattr(store, "MAX_ARTIFACT_BYTES", 2)
    monkeypatch.setattr(store, "MAX_RESOLUTION_BYTES", 2)

    with pytest.raises(store.StoreError, match="ceiling"):
        store.resolve(tmp_path, "atlas.json")

    assert manifest["generation"]


def test_resolve_without_a_published_generation(tmp_path):
    with pytest.raises(store.StoreError, match="no published generation"):
        store.resolve(tmp_path, "atlas.json")


def test_old_generations_are_pruned(tmp_path):
    kept = [publish(tmp_path, feeds=f"{index}\n") for index in range(5)]

    remaining = sorted(
        path.name for path in tmp_path.iterdir() if path.name.startswith("gen-")
    )

    assert len(remaining) == store.KEEP_GENERATIONS
    assert kept[-1]["generation"] in remaining
    assert kept[0]["generation"] not in remaining


def test_a_second_writer_is_refused(tmp_path):
    directory = store.open_directory(tmp_path)
    try:
        with store.exclusive_writer(directory):
            with pytest.raises(store.StoreError, match="another build"):
                publish(tmp_path)
    finally:
        directory.close()


class FakeMsvcrt:
    """Records the locking calls the Windows branch would make."""

    LK_NBLCK = 1
    LK_UNLCK = 0

    def __init__(self, fail=False):
        self.calls = []
        self._fail = fail

    def locking(self, handle, mode, length):
        self.calls.append((mode, length))
        if self._fail and mode == self.LK_NBLCK:
            raise OSError("already locked")


def test_the_lock_uses_msvcrt_where_there_is_no_fcntl(tmp_path, monkeypatch):
    # fcntl does not exist on Windows; this is the branch that took the
    # whole CI matrix down, so it is worth exercising off Windows too.
    fake = FakeMsvcrt()
    monkeypatch.setattr(store, "fcntl", None)
    monkeypatch.setattr(store, "msvcrt", fake)

    with store.open_directory(tmp_path) as directory:
        with store.exclusive_writer(directory):
            pass

    assert fake.calls == [(FakeMsvcrt.LK_NBLCK, 1), (FakeMsvcrt.LK_UNLCK, 1)]


def test_a_held_msvcrt_lock_refuses_the_second_writer(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "fcntl", None)
    monkeypatch.setattr(store, "msvcrt", FakeMsvcrt(fail=True))

    with store.open_directory(tmp_path) as directory:
        with pytest.raises(store.StoreError, match="another build"):
            with store.exclusive_writer(directory):
                pass


def can_symlink(tmp_path):
    """Windows needs Developer Mode or a privilege to create symlinks."""
    probe = tmp_path / ".symlink-probe"
    try:
        probe.symlink_to(tmp_path, target_is_directory=True)
    except (OSError, NotImplementedError):
        return False
    probe.unlink()
    return True


def test_a_symlinked_parent_cannot_redirect_the_cache(tmp_path):
    if not can_symlink(tmp_path):
        pytest.skip("this platform will not let the test create a symlink")
    # Opening only the leaf would follow a link planted at the parent.
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    link = tmp_path / "cache"
    link.symlink_to(elsewhere, target_is_directory=True)

    with pytest.raises(store.StoreError, match="is a symlink"):
        with store.open_directory(link) as parent:
            parent.child("raw")


def test_child_creates_once_then_opens(tmp_path):
    with store.open_directory(tmp_path) as parent:
        first = parent.child("raw")
        first.close()
        second = parent.child("raw")
        second.close()

    assert (tmp_path / "raw").is_dir()


def test_a_symlinked_directory_is_refused(tmp_path):
    if not can_symlink(tmp_path):
        pytest.skip("this platform will not let the test create a symlink")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    link = tmp_path / "link"
    link.symlink_to(elsewhere, target_is_directory=True)

    with pytest.raises(store.StoreError, match="is a symlink"):
        store.open_directory(link)


def test_a_failed_write_leaves_no_temporary_file(tmp_path):
    directory = store.open_directory(tmp_path)

    def explode():
        yield "partial"
        raise OSError("disk full")

    try:
        with pytest.raises(OSError):
            store.write_file(directory, "artifact.txt", explode)
    finally:
        directory.close()

    assert not (tmp_path / "artifact.txt").exists()
    assert list(tmp_path.glob(".tmp-*")) == []
