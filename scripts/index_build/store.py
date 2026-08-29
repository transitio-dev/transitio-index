"""Artifact store for the build cache: generations, manifests, locking.

Every stage publishes a *set* of files that only make sense together. Writing
them in place, one by one, lets a crash or a second writer leave a reader
holding a mixture of two runs. So a stage writes a complete generation into
its own directory and then swaps a pointer manifest in one step: readers
resolve through the pointer and never see a half-built set.

Where the platform has them, file operations go through a directory
descriptor opened ``O_NOFOLLOW``, so a symlink planted in the cache cannot
redirect a write and there is no check-then-use window. Windows has neither
``O_NOFOLLOW`` nor descriptor-relative operations, so there the same calls
run against full paths guarded by an ``lstat`` check — the guard still
refuses a symlink, but between the check and the open there is a window
this module cannot close. The residual is accepted: the build cache is
maintainer-only scratch, and the alternative is not running here at all.
"""

import contextlib
import errno
import hashlib
import json
import ntpath
import os
import re
import stat
import unicodedata

try:  # Unix
    import fcntl
except ImportError:  # pragma: no cover - exercised on Windows
    fcntl = None

try:  # Windows
    import msvcrt
except ImportError:  # pragma: no cover - exercised on Unix
    msvcrt = None

KEEP_GENERATIONS = 3
GENERATION_ATTEMPTS = 8

# Artifacts are JSONL of a few MB. `resolve` holds the verified bytes in
# memory, so this ceiling also bounds what one resolution can retain.
MAX_ARTIFACT_BYTES = 256 * 1024 * 1024
# `resolve` retains every artifact in memory at once, so the whole set is
# bounded as well as each file.
MAX_RESOLUTION_BYTES = 512 * 1024 * 1024

O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
O_BINARY = getattr(os, "O_BINARY", 0)

# Windows supports none of these. Probe `os.rename`, NOT `os.replace`:
# macOS advertises renameat through `os.rename` only, so checking `os.replace`
# silently reports "no descriptor support" on a platform that has it — which
# routed every operation here through the weaker fallback until it was caught.
# On POSIX `rename` already overwrites atomically, so it is what the
# descriptor branch uses.
HAVE_DIR_FD = all(
    function in os.supports_dir_fd
    for function in (os.open, os.rename, os.unlink, os.stat, os.mkdir, os.rmdir)
)

GENERATION_PATTERN = re.compile(r"\Agen-[0-9a-f]{8}-[0-9a-f]{16}\Z")


def _generation_tag(pointer):
    """A stable per-pointer prefix, so each pointer owns its generations.

    Pruning is scoped to this tag; without it, a second pointer sharing the
    store (a second catalogue source) would have its live generation
    deleted the next time the first pointer published.
    """
    return hashlib.sha256(canonical_key(pointer).encode("utf-8")).hexdigest()[:8]


class StoreError(RuntimeError):
    """The build cache could not be read or published as specified."""


class MissingEntry(StoreError):
    """A cache entry named by a manifest is no longer there.

    Distinguished from other store failures because it is the one a
    concurrent publish can cause and a retry can resolve.
    """


def _redirects(path):
    """Whether ``path`` is a link that would redirect the operation.

    ``Path.is_symlink`` does not classify a Windows directory junction or
    other reparse point, and a junction at the cache root would redirect
    every later operation outright — a worse failure than the check/open
    race this fallback already accepts. Accepts any path-like — ``os.lstat``
    rather than ``Path.lstat`` — so a plain string works too.
    """
    try:
        info = os.lstat(path)
    except OSError:
        return False
    if stat.S_ISLNK(info.st_mode):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _refuse_symlink(path):
    """Reject a redirecting entry by name, where ``O_NOFOLLOW`` is absent."""
    if _redirects(path):
        raise StoreError(f"{path}: cache entry is a symlink")
    return path


# Windows device names, which resolve to a device wherever they appear.
# `ntpath.isreserved` only arrived in 3.13 and the floor here is 3.10, so the
# rules are spelled out: console aliases and the superscript COM/LPT forms
# Windows also treats as devices.
_DEVICE_DIGITS = "123456789\u00b9\u00b2\u00b3"
RESERVED_NAMES = frozenset(
    ["CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"]
    + [f"COM{digit}" for digit in _DEVICE_DIGITS]
    + [f"LPT{digit}" for digit in _DEVICE_DIGITS]
)

# Windows rejects these outright; a name carrying one would behave
# differently depending on the platform that wrote the cache.
INVALID_IN_NAME = frozenset('<>:"|?*') | {chr(code) for code in range(32)}

DIGEST_PATTERN = re.compile(r"\A[0-9a-f]{64}\Z")


def canonical_key(name):
    """A filesystem-independent key for ``name``.

    Compared this way so a case-insensitive or Unicode-normalizing
    filesystem cannot slip ``Manifest.json`` past a reserved name or let two
    case-equivalent artifacts overwrite each other. It does not model
    Windows DOS 8.3 short-name aliases; those are disabled by default on
    modern volumes and this cache is maintainer-only scratch, so that
    collision is accepted rather than defended.
    """
    return unicodedata.normalize("NFC", name).casefold()


RESERVED_KEYS = {canonical_key(".lock"), canonical_key("manifest.json")}


def is_reserved(name):
    """Whether ``name`` collides with the store's own namespace.

    ``.lock`` is the writer lock, ``manifest.json`` the generation manifest,
    ``.tmp-`` the write temporaries, and ``gen-<hex>`` the generation
    directories — a published name matching any of them would corrupt the
    store's own bookkeeping.
    """
    key = canonical_key(name)
    if key in RESERVED_KEYS or key.startswith(".tmp-"):
        return True
    return bool(GENERATION_PATTERN.match(key))


def safe_component(name):
    """Whether ``name`` is a single path component that stays in the cache.

    Judged by Windows rules on every platform, because they are the
    strictest: ``D:escape`` is drive-*relative* rather than absolute, so an
    ``isabs`` test passes it and the join then resolves against whatever
    directory that drive happens to be on. A colon also opens an NTFS
    alternate data stream, and a reserved device name resolves to a device
    from any directory.
    """
    if not isinstance(name, str) or not name or name in (".", ".."):
        return False
    if ntpath.splitdrive(name)[0] or INVALID_IN_NAME.intersection(name):
        return False
    if "/" in name or "\\" in name or os.path.isabs(name):
        return False
    if name != name.rstrip(" ."):
        # Windows silently strips trailing dots and spaces, so two distinct
        # names could resolve to the same file.
        return False
    # Windows matches the device name after stripping trailing spaces from
    # the stem, so `CON .txt` is still the console.
    return name.split(".")[0].rstrip(" ").upper() not in RESERVED_NAMES


class Directory:
    """A cache directory, addressed by descriptor where that is possible.

    Every operation is relative to this object rather than to a bare path,
    so the descriptor-relative and path-based platforms differ in one place
    instead of at every call site.
    """

    def __init__(self, path, create=True, fd=None):
        self.path = path
        self._fd = fd
        if fd is not None:
            return
        if create:
            path.mkdir(parents=True, exist_ok=True)
        elif not path.exists():
            raise MissingEntry(f"{path}: no such directory in the cache")
        if HAVE_DIR_FD:
            try:
                self._fd = os.open(path, os.O_RDONLY | O_DIRECTORY | O_NOFOLLOW)
            except OSError as error:
                # Linux reports ELOOP for a symlink refused by O_NOFOLLOW;
                # macOS reports ENOTDIR because O_DIRECTORY is judged first.
                if error.errno in (errno.ELOOP, errno.ENOTDIR):
                    if path.is_symlink():
                        raise StoreError(
                            f"{path}: cache directory is a symlink"
                        ) from None
                    raise StoreError(f"{path}: cache path is not a directory") from None
                raise
        elif _redirects(path):
            raise StoreError(f"{path}: cache directory is a symlink")

    @property
    def fd(self):
        return self._fd

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False

    def _check(self, name):
        # Every name reaching a filesystem call is checked here rather than
        # at each call site: an absolute name ignores `dir_fd` entirely and
        # `..` escapes through `openat`, so one missed caller is a hole.
        if not safe_component(name):
            raise StoreError(f"{name!r}: not a name inside the cache")
        return name

    def _target(self, name):
        self._check(name)
        return name if self._fd is not None else str(self.path / name)

    def _kwargs(self):
        return {"dir_fd": self._fd} if self._fd is not None else {}

    def open(self, name, flags, mode=0o600):
        creating = flags & os.O_CREAT and flags & os.O_EXCL
        if self._fd is None and not creating:
            _refuse_symlink(self.path / name)
        try:
            return os.open(
                self._target(name), flags | O_NOFOLLOW, mode, **self._kwargs()
            )
        except OSError as error:
            # O_NOFOLLOW on a symlinked file entry raises ELOOP/ENOTDIR;
            # report it like a directory link rather than leaking the errno.
            if (
                not creating
                and error.errno in (errno.ELOOP, errno.ENOTDIR)
                and (self.path / name).is_symlink()
            ):
                raise StoreError(
                    f"{self.path / name}: cache entry is a symlink"
                ) from None
            raise

    def replace(self, source, destination):
        self._check(source)
        self._check(destination)
        if self._fd is None:
            os.replace(str(self.path / source), str(self.path / destination))
        else:
            # `os.rename` rather than `os.replace`: only rename advertises
            # renameat everywhere, and on POSIX it already overwrites.
            os.rename(source, destination, src_dir_fd=self._fd, dst_dir_fd=self._fd)

    def unlink(self, name):
        try:
            os.unlink(self._target(name), **self._kwargs())
        except FileNotFoundError:
            pass

    def listdir(self):
        return os.listdir(self._fd if self._fd is not None else self.path)

    def stat(self, name):
        return os.stat(self._target(name), **self._kwargs())

    def mkdir(self, name, mode=0o700):
        os.mkdir(self._target(name), mode, **self._kwargs())

    def rmdir(self, name):
        os.rmdir(self._target(name), **self._kwargs())

    def make_subdirectory(self, name, mode=0o700):
        """Create ``name`` and return it opened."""
        self.mkdir(name, mode)
        return self.subdirectory(name)

    def child(self, name, mode=0o700):
        """Open ``name``, creating it if it is not there.

        The create and the open both go through this directory's own
        descriptor, so a symlink or junction planted at the parent cannot
        redirect them — which opening the leaf path directly would allow.
        """
        try:
            return self.subdirectory(name)
        except MissingEntry:
            pass
        try:
            self.mkdir(name, mode)
        except FileExistsError:
            pass
        return self.subdirectory(name)

    def subdirectory(self, name):
        """Open an existing child, never creating one.

        Through the held descriptor where possible: re-resolving the full
        pathname would let a rename or a swapped link above this directory
        redirect the generation, which is the whole point of holding it.
        """
        self._check(name)
        if self._fd is None:
            return Directory(self.path / name, create=False)
        try:
            fd = os.open(name, os.O_RDONLY | O_DIRECTORY | O_NOFOLLOW, dir_fd=self._fd)
        except FileNotFoundError:
            raise MissingEntry(f"{self.path / name}: no such generation") from None
        except OSError as error:
            # O_NOFOLLOW turns a symlink into ELOOP on Linux, ENOTDIR on
            # macOS; report it the same way the top-level open does rather
            # than leaking the raw errno.
            if (
                error.errno in (errno.ELOOP, errno.ENOTDIR)
                and (self.path / name).is_symlink()
            ):
                raise StoreError(
                    f"{self.path / name}: cache entry is a symlink"
                ) from None
            raise StoreError(f"{self.path / name}: {error.strerror}") from None
        return Directory(self.path / name, fd=fd)

    def sync(self):
        """Flush the directory entry itself; a no-op where unsupported."""
        if self._fd is not None:
            os.fsync(self._fd)


def open_directory(path):
    """A :class:`Directory` for ``path``, refusing a symlinked directory."""
    return Directory(path)


def open_subdir(parent_path, name, *, create=True):
    """Open ``parent_path/name`` with ``parent_path`` itself guarded.

    ``open_directory(parent_path / name)`` only guards ``name``: a symlink at
    ``parent_path`` — the cache root — would still redirect the whole cache,
    because ``O_NOFOLLOW`` checks the final component only. Opening the parent
    first (which refuses a symlinked parent) and then reaching the child
    through its descriptor closes that, the same way a generation is opened
    beneath a held directory. ``create=False`` requires ``name`` to exist.
    """
    parent = open_directory(parent_path)
    try:
        return parent.child(name) if create else parent.subdirectory(name)
    finally:
        parent.close()


def _temporary_name():
    return f".tmp-{os.urandom(8).hex()}"


def write_file(directory, name, write):
    """Create ``name`` under ``directory``; return ``(sha256_hex, size)``.

    The content goes to a temporary file created ``O_EXCL``, is flushed and
    fsynced, then replaced into place — so the name either does not exist
    yet or already has its final content. ``size`` is the encoded byte
    count, which ``publish`` sums to bound a whole generation.

    Written in **binary**, hashing the same bytes that reach the disk. In
    text mode Windows would store ``\r\n`` where the digest saw ``\n``, so
    the recorded SHA-256 would not be the hash of the file it describes.
    """
    temporary = _temporary_name()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_BINARY
    handle = directory.open(temporary, flags)
    digest = hashlib.sha256()
    try:
        written = 0
        with os.fdopen(handle, "wb") as opened_file:
            handle = None
            for chunk in write():
                data = chunk.encode("utf-8")
                written += len(data)
                if written > MAX_ARTIFACT_BYTES:
                    # Refused here, before `replace`: a generation whose
                    # artifact `resolve` would reject must never become the
                    # thing the pointer names.
                    raise StoreError(
                        f"{directory.path / name}: over the "
                        f"{MAX_ARTIFACT_BYTES}-byte artifact ceiling"
                    )
                digest.update(data)
                opened_file.write(data)
            opened_file.flush()
            os.fsync(opened_file.fileno())
        directory.replace(temporary, name)
    except BaseException:
        if handle is not None:
            os.close(handle)
        directory.unlink(temporary)
        raise
    return digest.hexdigest(), written


def write_bytes(directory, name, data):
    """Create ``name`` under ``directory`` from ``data``; return its sha256.

    Like :func:`write_file` but for a binary payload produced whole rather than
    as text chunks (the Parquet index). Written to an ``O_EXCL`` temporary,
    fsynced, then replaced into place, so the name is never half-written.
    """
    if len(data) > MAX_ARTIFACT_BYTES:
        raise StoreError(
            f"{directory.path / name}: over the "
            f"{MAX_ARTIFACT_BYTES}-byte artifact ceiling"
        )
    temporary = _temporary_name()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_BINARY
    handle = directory.open(temporary, flags)
    try:
        with os.fdopen(handle, "wb") as opened_file:
            handle = None
            opened_file.write(data)
            opened_file.flush()
            os.fsync(opened_file.fileno())
        directory.replace(temporary, name)
    except BaseException:
        if handle is not None:
            os.close(handle)
        directory.unlink(temporary)
        raise
    return hashlib.sha256(data).hexdigest()


def unlink(directory, name):
    directory.unlink(name)


def create_temporary(directory, readable=False):
    """An exclusively created temporary file; the caller owns the handle.

    ``readable`` opens it ``O_RDWR`` so the caller can rewind and read back
    what it just wrote, without reopening the name — reopening is where a
    swapped path would slip in.
    """
    name = _temporary_name()
    access = os.O_RDWR if readable else os.O_WRONLY
    flags = access | os.O_CREAT | os.O_EXCL | O_BINARY
    return directory.open(name, flags), name


def open_regular_path(path):
    """Open ``path`` for reading, refusing a symlink or non-regular file.

    Portable across the split this module already lives with: ``O_NOFOLLOW``
    where the platform has it, an ``lstat``/reparse-point precheck where it
    does not, ``O_NONBLOCK`` so a FIFO cannot wedge the open, and an
    ``fstat`` on the descriptor to confirm a regular file.
    """
    if not O_NOFOLLOW and _redirects(path):
        raise StoreError(f"{path}: is a symlink")
    flags = os.O_RDONLY | O_BINARY | getattr(os, "O_NONBLOCK", 0) | O_NOFOLLOW
    try:
        handle = os.open(path, flags)
    except OSError as error:
        if error.errno in (errno.ELOOP, errno.ENOTDIR) and _redirects(path):
            raise StoreError(f"{path}: is a symlink") from None
        raise
    try:
        if not stat.S_ISREG(os.fstat(handle).st_mode):
            raise StoreError(f"{path}: not a regular file")
    except BaseException:
        os.close(handle)
        raise
    return handle


def open_regular(directory, name):
    """Open ``name`` for reading, refusing anything but a regular file.

    ``O_NONBLOCK`` so a FIFO substituted for an artifact cannot wedge the
    open, and the ``fstat`` happens on the descriptor rather than the name,
    so nothing can be swapped in between.
    """
    flags = os.O_RDONLY | O_BINARY | getattr(os, "O_NONBLOCK", 0)
    try:
        handle = directory.open(name, flags)
    except FileNotFoundError:
        raise MissingEntry(f"{directory.path / name}: no such artifact") from None
    try:
        info = os.fstat(handle)
        if not stat.S_ISREG(info.st_mode):
            raise StoreError(f"{directory.path / name}: not a regular file")
        if info.st_size > MAX_ARTIFACT_BYTES:
            raise StoreError(f"{directory.path / name}: larger than the ceiling")
    except BaseException:
        os.close(handle)
        raise
    return handle


def read_all(handle, limit=MAX_ARTIFACT_BYTES):
    """Every byte of ``handle``, refusing to read past ``limit``.

    The ceiling is enforced *while reading*, not from a prior ``st_size``:
    a file growing in place after the size check would otherwise be read
    without bound. A ``bytearray`` avoids the doubled peak that joining a
    list of chunks would cause.
    """
    os.lseek(handle, 0, os.SEEK_SET)
    buffer = bytearray()
    while True:
        chunk = os.read(handle, 1024 * 1024)
        if not chunk:
            return bytes(buffer)
        buffer += chunk
        if len(buffer) > limit:
            raise StoreError(f"artifact exceeds the {limit}-byte ceiling")


def read_bytes(directory, name):
    """The file's exact bytes — what the digest was taken over."""
    handle = open_regular(directory, name)
    try:
        return read_all(handle)
    finally:
        os.close(handle)


def read_text(directory, name):
    return read_bytes(directory, name).decode("utf-8")


def _lock(handle):
    if fcntl is not None:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:  # pragma: no cover - exercised on Windows
        os.lseek(handle, 0, os.SEEK_SET)
        msvcrt.locking(handle, msvcrt.LK_NBLCK, 1)


def _unlock(handle):
    if fcntl is not None:
        fcntl.flock(handle, fcntl.LOCK_UN)
    else:  # pragma: no cover - exercised on Windows
        os.lseek(handle, 0, os.SEEK_SET)
        msvcrt.locking(handle, msvcrt.LK_UNLCK, 1)


@contextlib.contextmanager
def exclusive_writer(directory, name=".lock"):
    """A non-blocking writer lock over the cache, so runs cannot interleave.

    Two ingests publishing at once would each swap the pointer under the
    other; refusing the second is more useful than serialising it, since
    the second is nearly always an accident.
    """
    handle = directory.open(name, os.O_RDWR | os.O_CREAT | O_BINARY)
    try:
        _lock(handle)
    except OSError:
        os.close(handle)
        raise StoreError("another build is publishing to this cache") from None
    try:
        yield
    finally:
        try:
            _unlock(handle)
        finally:
            os.close(handle)


def _generation_names(directory):
    """Generation directories, oldest first.

    Matched against the published shape, not merely the prefix: a
    hand-made `gen-x:` is a legal POSIX name that `safe_component` refuses,
    and letting it reach `stat` would fail housekeeping over a stray entry.
    """
    names = [name for name in directory.listdir() if GENERATION_PATTERN.match(name)]
    return sorted(names, key=lambda name: directory.stat(name).st_mtime)


def jsonl_chunks(records):
    """A :func:`publish` chunk function writing ``records`` as JSON Lines.

    Deterministic and non-ASCII-safe: sorted keys, no NaN/Infinity, one record
    per line. The bytes are what the digest is taken over, so they must be
    stable across runs.
    """

    def chunks():
        for record in records:
            yield json.dumps(
                record, ensure_ascii=False, sort_keys=True, allow_nan=False
            )
            yield "\n"

    return chunks


def parse_jsonl(raw):
    """Records from JSON Lines bytes, the inverse of :func:`jsonl_chunks`.

    Split only on the LF the writer inserts: ``str.splitlines()`` would also
    break on U+2028/U+2029/U+0085, which ``ensure_ascii=False`` writes raw
    inside a string, corrupting the record.
    """
    return [json.loads(line) for line in raw.decode("utf-8").split("\n") if line]


def read_jsonl(directory, pointer, artifact):
    """Resolve a generation and parse one JSONL artifact into ``(records, manifest)``."""
    generation, manifest = resolve(directory, pointer)
    with generation:
        return parse_jsonl(generation.read_bytes(artifact)), manifest


def publish(directory, pointer, artifacts, manifest, keep=KEEP_GENERATIONS, held=None):
    """Write ``artifacts`` as a new generation and point ``pointer`` at it.

    ``artifacts`` maps file name to a function yielding text chunks;
    ``manifest`` is a dict describing the run, to which this adds the
    generation name and each artifact's digest. Returns the manifest.

    ``held`` is a :class:`Directory` whose writer lock the caller already
    holds. A stage that touched shared state before publishing — a cached
    download, say — must hold one lock across the whole thing, or another
    run can change that state between the two.
    """
    # Snapshot the mapping once: iterating a second time to write could see
    # different entries from a custom or mutated mapping, past this check.
    items = tuple(artifacts.items())
    # Nothing is created until the whole snapshot is known good, so a bad
    # input cannot leave a half-generation or move the pointer.
    if not items:
        raise StoreError(f"{directory}: a generation must have an artifact")
    if not safe_component(pointer) or is_reserved(pointer):
        raise StoreError(f"{directory}: pointer name {pointer!r}")
    keys = set()
    for name, _ in items:
        if not safe_component(name):
            raise StoreError(f"{directory}: artifact name {name!r}")
        if is_reserved(name):
            raise StoreError(f"{directory}: artifact name {name!r} is reserved")
        key = canonical_key(name)
        if key in keys:
            raise StoreError(f"{directory}: two artifacts collide as {name!r}")
        keys.add(key)

    opened = open_directory(directory) if held is None else held
    # A caller passing `held` already holds the lock on that directory.
    lock = contextlib.nullcontext() if held is not None else exclusive_writer(opened)
    try:
        with lock:
            # Scoped to the pointer, so pruning one pointer's old generations
            # never deletes a generation another pointer still names. Created
            # with an exclusive mkdir and retried on the astronomically
            # unlikely name collision, so cleanup only ever removes the
            # directory this call made — never a pre-existing one.
            tag = _generation_tag(pointer)
            generation = None
            for _ in range(GENERATION_ATTEMPTS):
                candidate = f"gen-{tag}-{os.urandom(8).hex()}"
                try:
                    opened.mkdir(candidate)
                except FileExistsError:
                    continue
                generation = candidate
                break
            if generation is None:
                raise StoreError(f"{directory}: could not name a new generation")

            activated = False
            try:
                with opened.subdirectory(generation) as generation_dir:
                    digests = {}
                    total = 0
                    for name, write in items:
                        digest, size = write_file(generation_dir, name, write)
                        digests[name] = digest
                        total += size
                        if total > MAX_RESOLUTION_BYTES:
                            # The whole generation must be resolvable, not
                            # just each file. Refused before the manifest or
                            # pointer is written, so the previous pointer
                            # stays active.
                            raise StoreError(
                                f"{directory}: generation over the "
                                f"{MAX_RESOLUTION_BYTES}-byte ceiling"
                            )
                    published = dict(manifest, generation=generation, digests=digests)
                    # Serialized once: writing the same bytes to the
                    # generation and the pointer means a later mutation of a
                    # nested value in the caller's manifest cannot make the
                    # two disagree.
                    payload = json.dumps(published, indent=2, sort_keys=True) + "\n"

                    def write_manifest():
                        yield payload

                    _, manifest_size = write_file(
                        generation_dir, "manifest.json", write_manifest
                    )
                    if total + manifest_size > MAX_RESOLUTION_BYTES:
                        raise StoreError(
                            f"{directory}: generation over the "
                            f"{MAX_RESOLUTION_BYTES}-byte ceiling"
                        )
                    generation_dir.sync()

                # The pointer swap is the moment the generation becomes
                # visible; from here it is active, so a later durability
                # sync failing must not undo it.
                write_file(opened, pointer, write_manifest)
                activated = True
                opened.sync()
            finally:
                # Remove the generation this call created only if it did not
                # become live. `activated` covers the ordinary path;
                # re-reading the pointer covers the tiny window where the
                # swap succeeded but the flag was not yet set (an async
                # error between the two). A generation the pointer names is
                # never deleted here.
                if (
                    generation is not None
                    and not activated
                    and _pointer_names(opened, pointer, generation) is False
                ):
                    _remove_generation(opened, generation)

            # After the pointer swap the publish has succeeded. Collecting
            # old generations is housekeeping: on Windows a reader holding
            # one open makes deletion a sharing violation, and failing here
            # would report a publish that in fact happened as an error.
            try:
                _prune(opened, keep, generation, _generation_tag(pointer))
            except (OSError, StoreError):
                pass
        return published
    finally:
        if held is None:
            opened.close()


def _referenced_generations(directory):
    """Generation names any pointer in the store currently points at.

    Read fresh from disk, so pruning never deletes a generation a live
    pointer still names — even a different pointer than the one publishing,
    and even if two pointers happen to share a tag. This is what makes
    pruning correct without trusting the tag or an activation flag.
    """
    referenced = set()
    for name in directory.listdir():
        if is_reserved(name):
            continue
        try:
            raw = read_bytes(directory, name)
        except MissingEntry:
            continue  # vanished between listing and reading
        except StoreError:
            continue  # not a regular file — a directory, not a pointer
        except OSError as error:
            # A candidate that exists but cannot be read: fail closed rather
            # than treat a live pointer as referencing nothing and prune the
            # generation it names.
            raise StoreError(
                f"{directory.path / name}: unreadable during prune"
            ) from error
        try:
            manifest = json.loads(raw)
        except ValueError:
            continue  # readable but not JSON — a stray file, not a pointer
        generation = manifest.get("generation") if isinstance(manifest, dict) else None
        if isinstance(generation, str):
            referenced.add(generation)
    return referenced


def _pointer_names(directory, pointer, generation):
    """Whether ``pointer`` currently names ``generation``.

    ``True`` names it, ``False`` definitively does not (or is absent),
    ``None`` when the pointer's state could not be read — the caller must
    preserve the generation on ``None`` rather than assume it is an orphan.
    """
    try:
        raw = read_bytes(directory, pointer)
    except MissingEntry:
        return False  # no pointer yet — a failed first publish's orphan
    except (OSError, StoreError):
        return None  # unknown: do not treat as an orphan
    try:
        manifest = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(manifest, dict):
        return None
    return manifest.get("generation") == generation


def _remove_generation(directory, name):
    """Best-effort removal of one generation directory and its contents."""
    try:
        generation_dir = directory.subdirectory(name)
    except (OSError, StoreError):
        return
    try:
        for entry in generation_dir.listdir():
            try:
                generation_dir.unlink(entry)
            except OSError:
                pass
    finally:
        generation_dir.close()
    try:
        directory.rmdir(name)
    except OSError:
        pass


def _prune(directory, keep, active, tag):
    """Drop the oldest generations of this pointer, keeping ``keep``.

    A generation any pointer still references is never a candidate, so
    pruning cannot delete a live generation regardless of tag collisions;
    the tag only groups a pointer's own history for the keep count.
    """
    referenced = _referenced_generations(directory)
    referenced.add(active)
    prefix = f"gen-{tag}-"
    candidates = [
        name
        for name in _generation_names(directory)
        if name.startswith(prefix) and name not in referenced
    ]
    keep_besides_active = max(0, keep - 1)
    for name in candidates[: max(0, len(candidates) - keep_besides_active)]:
        _remove_generation(directory, name)


class Generation:
    """A resolved generation whose verified artifact bytes are held in hand.

    ``resolve`` hashes every declared artifact and keeps the exact bytes it
    hashed, handing them back through :meth:`read_bytes`. Serving from those
    captured bytes — rather than reopening by name — is what makes a read
    immune to a concurrent prune or in-place rewrite. ``path`` is exposed
    for messages and joins; it carries no such guarantee.
    """

    def __init__(self, directory, manifest, contents):
        self._directory = directory
        self._contents = contents
        self.manifest = manifest
        self.path = directory.path

    @property
    def name(self):
        return self.path.name

    def read_bytes(self, name):
        """Exactly the bytes verification hashed.

        Captured during resolution rather than re-read: a retained
        descriptor survives unlink-and-replace but not an in-place write,
        and two readers sharing one descriptor would race on its offset.
        Only artifacts the manifest declared are readable — an undeclared
        name was never verified.
        """
        content = self._contents.get(name)
        if content is None:
            raise StoreError(f"{name!r}: not a verified artifact of {self.name}")
        return content

    def close(self):
        self._contents = {}
        self._directory.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False

    def __truediv__(self, name):
        return self.path / name

    def __fspath__(self):
        return str(self.path)


def _check_digests(digests, generation, pointer):
    """A manifest that declares nothing must not verify as intact."""
    if not isinstance(digests, dict) or not digests:
        raise StoreError(f"{pointer}: {generation} declares no artifacts")
    for name, expected in digests.items():
        # The manifest decides what gets opened, so it does not get to
        # name anything outside its own generation.
        if not safe_component(name):
            raise StoreError(f"{generation}: manifest names {name!r}")
        if not isinstance(expected, str) or not DIGEST_PATTERN.match(expected):
            raise StoreError(f"{generation}/{name}: manifest digest is not a SHA-256")


RESOLVE_ATTEMPTS = 3


def resolve(directory, pointer):
    """Read the pointer manifest and verify the generation it names.

    Every artifact is hashed and compared with the manifest before a reader
    is told where they are, so a truncated or mixed generation is an error
    rather than something a later stage silently consumes. Returns
    ``(Generation, manifest)``; hold the :class:`Generation` open for the
    reads it covers.
    """
    for attempt in range(1, RESOLVE_ATTEMPTS + 1):
        try:
            return _resolve_once(directory, pointer)
        except MissingEntry:
            # A publisher swapped the pointer and pruned the generation
            # between reading the one and opening the other. Re-reading the
            # pointer picks up the generation that replaced it.
            if attempt == RESOLVE_ATTEMPTS:
                raise StoreError(
                    f"{directory / pointer}: generation vanished while reading it"
                ) from None
    raise AssertionError("unreachable")


def _resolve_once(directory, pointer):
    # Guard the cache root, not just the store directory: a symlink swapped in
    # at `directory.parent` would otherwise redirect the read.
    with open_subdir(directory.parent, directory.name) as opened:
        try:
            manifest = json.loads(read_text(opened, pointer))
        except MissingEntry:
            raise StoreError(
                f"{directory / pointer}: no published generation"
            ) from None
        generation = manifest.get("generation")
        if not isinstance(generation, str) or not GENERATION_PATTERN.match(generation):
            raise StoreError(f"{directory / pointer}: manifest names no generation")
        digests = manifest.get("digests")
        _check_digests(digests, generation, directory / pointer)

        generation_dir = opened.subdirectory(generation)
        contents = {}
        try:
            # The generation carries its own copy; if the two disagree, the
            # pointer is describing a generation it did not publish.
            own = json.loads(read_bytes(generation_dir, "manifest.json"))
            if own != manifest:
                raise StoreError(f"{generation}: does not match the pointer manifest")
            retained = 0
            for name, expected in digests.items():
                handle = open_regular(generation_dir, name)
                try:
                    # A per-artifact limit that also respects what is left
                    # of the whole-resolution budget.
                    content = read_all(
                        handle, min(MAX_ARTIFACT_BYTES, MAX_RESOLUTION_BYTES - retained)
                    )
                finally:
                    os.close(handle)
                if hashlib.sha256(content).hexdigest() != expected:
                    raise StoreError(f"{generation}/{name}: digest mismatch")
                retained += len(content)
                contents[name] = content
        except BaseException:
            generation_dir.close()
            raise
        return Generation(generation_dir, manifest, contents), manifest


def regular_file_size(directory, name):
    """``st_size`` when ``name`` is a regular file, else ``None``."""
    try:
        info = directory.stat(name)
    except OSError:
        return None
    return info.st_size if stat.S_ISREG(info.st_mode) else None
