"""Read single members out of a remote ZIP through byte ranges.

The crawler avoids downloading whole feeds where it can: a ZIP's directory
lives at its end, so three reads — the end-of-central-directory record, the
central directory, then the wanted member — extract one file from an archive
the crawler never fetched in full. The functions here are pure: they take a
``read(start, size) -> bytes`` callable and the archive's total size, so the
same code runs over HTTP ranges or local bytes in tests.

Anything odd falls back rather than being handled: ZIP64 markers, encryption,
data descriptors, an unknown compression method or a CRC mismatch raise
:class:`RangeReadError` with the reason, and the crawler downloads the whole
feed instead — the plan's contract, keeping this parser small enough to trust.
"""

import struct
import zlib

# End-of-central-directory: fixed 22 bytes, preceded at most by a 65535-byte
# archive comment.
_EOCD_SIGNATURE = b"PK\x05\x06"
_EOCD_SIZE = 22
_MAX_COMMENT = 65535

_CENTRAL_SIGNATURE = b"PK\x01\x02"
_CENTRAL_SIZE = 46
_LOCAL_SIGNATURE = b"PK\x03\x04"
_LOCAL_SIZE = 30

# General-purpose flag bits that route to the fallback: encryption and the
# streaming data descriptor.
_FLAG_ENCRYPTED = 0x0001
_FLAG_DATA_DESCRIPTOR = 0x0008
_FLAG_UTF8 = 0x0800

_STORED, _DEFLATED = 0, 8

# A value of all ones in a 16- or 32-bit size/offset field marks ZIP64.
_ZIP64_16, _ZIP64_32 = 0xFFFF, 0xFFFFFFFF

# Ceilings on attacker-declared sizes: they budget real memory, so an archive
# advertising internally consistent multi-gigabyte values falls back to the
# whole-file download (whose budget the HTTP layer owns) instead of being
# ranged. A GTFS central directory is kilobytes; the largest member the crawler
# ranges for is a national aggregate's stop_times.txt.
MAX_DIRECTORY_BYTES = 64 * 1024 * 1024
MAX_MEMBER_BYTES = 2 * 1024 * 1024 * 1024


class RangeReadError(RuntimeError):
    """The archive cannot be read through ranges; download it whole."""


def _read_exact(read, start, size, what):
    data = read(start, size)
    if len(data) != size:
        raise RangeReadError(f"short read of {what} ({len(data)} of {size} bytes)")
    return data


def end_of_central_directory(read, size):
    """The ``(entry_count, cd_offset, cd_size)`` triple from the archive tail.

    Scans backwards through at most one comment's worth of tail bytes for the
    record signature, so a commented archive still resolves; ZIP64 markers and
    a missing record raise :class:`RangeReadError`.
    """
    if size < _EOCD_SIZE:
        raise RangeReadError(f"archive too small ({size} bytes)")
    tail_size = min(size, _EOCD_SIZE + _MAX_COMMENT)
    tail_start = size - tail_size
    tail = _read_exact(read, tail_start, tail_size, "archive tail")
    # A comment may contain forged signature bytes, so scan backwards for a
    # candidate whose declared comment reaches exactly the end of the archive
    # and whose central directory ends exactly at the record.
    position = len(tail)
    while True:
        position = tail.rfind(_EOCD_SIGNATURE, 0, position)
        if position < 0:
            raise RangeReadError("no end-of-central-directory record")
        record = tail[position : position + _EOCD_SIZE]
        if len(record) < _EOCD_SIZE:
            continue
        disk, cd_disk, disk_entries, entries, cd_size, cd_offset, comment_size = (
            struct.unpack("<HHHHIIH", record[4:])
        )
        if position + _EOCD_SIZE + comment_size != len(tail):
            continue
        if disk != 0 or cd_disk != 0 or disk_entries != entries:
            raise RangeReadError("multi-disk archive")
        if entries == _ZIP64_16 or cd_size == _ZIP64_32 or cd_offset == _ZIP64_32:
            raise RangeReadError("zip64 archive")
        if cd_offset + cd_size != tail_start + position:
            raise RangeReadError(
                "central directory does not end at the end-of-central-directory "
                "record"
            )
        return entries, cd_offset, cd_size


def central_directory(read, size, *, max_directory_bytes=MAX_DIRECTORY_BYTES):
    """The member entries, keyed by name.

    Each entry carries what a member read needs: the compression method, the
    sizes, the CRC and the local-header offset. Encrypted or data-descriptor
    members, ZIP64 fields, duplicate names and malformed records raise
    :class:`RangeReadError`.
    """
    entries, cd_offset, cd_size = end_of_central_directory(read, size)
    if cd_size > max_directory_bytes:
        raise RangeReadError(
            f"central directory of {cd_size} bytes is over the "
            f"{max_directory_bytes}-byte ceiling"
        )
    data = _read_exact(read, cd_offset, cd_size, "central directory")
    directory = {}
    position = 0
    for _ in range(entries):
        if data[position : position + 4] != _CENTRAL_SIGNATURE:
            raise RangeReadError("malformed central-directory entry")
        if position + _CENTRAL_SIZE > len(data):
            raise RangeReadError("truncated central directory")
        (
            _versions,
            flags,
            method,
            _mtime,
            _mdate,
            crc32,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
            comment_size,
            disk_start,
            _internal,
            _external,
            header_offset,
        ) = struct.unpack(
            "<IHHHHIIIHHHHHII", data[position + 4 : position + _CENTRAL_SIZE]
        )
        name_start = position + _CENTRAL_SIZE
        raw_name = data[name_start : name_start + name_size]
        if len(raw_name) != name_size:
            raise RangeReadError("truncated central-directory name")
        if flags & _FLAG_ENCRYPTED:
            raise RangeReadError(f"encrypted member {raw_name!r}")
        if flags & _FLAG_DATA_DESCRIPTOR:
            raise RangeReadError(f"data-descriptor member {raw_name!r}")
        if _ZIP64_32 in (compressed_size, uncompressed_size, header_offset):
            raise RangeReadError(f"zip64 member {raw_name!r}")
        if method not in (_STORED, _DEFLATED):
            raise RangeReadError(f"unsupported compression {method} in {raw_name!r}")
        if disk_start != 0:
            raise RangeReadError(f"multi-disk member {raw_name!r}")
        if header_offset >= cd_offset:
            raise RangeReadError(
                f"member {raw_name!r} does not precede the central directory"
            )
        try:
            name = raw_name.decode("utf-8" if flags & _FLAG_UTF8 else "cp437")
        except UnicodeDecodeError:
            raise RangeReadError(f"undecodable member name {raw_name!r}")
        if name in directory:
            raise RangeReadError(f"duplicate member name {name!r}")
        directory[name] = {
            "name": name,
            "raw_name": raw_name,
            "flags": flags,
            "method": method,
            "crc32": crc32,
            "compressed_size": compressed_size,
            "uncompressed_size": uncompressed_size,
            "header_offset": header_offset,
            # Member bytes must end before the directory begins.
            "data_end": cd_offset,
        }
        position = name_start + name_size + extra_size + comment_size
        if position > len(data):
            raise RangeReadError("central-directory entry extends past the directory")
    if position != len(data):
        raise RangeReadError("trailing bytes after the central-directory entries")
    return directory


def read_member(read, entry, *, max_member_bytes=MAX_MEMBER_BYTES):
    """The decompressed bytes of one member, verified against its CRC.

    The local header must agree with the central-directory entry (flags, method,
    CRC, sizes, name): the directory is what was validated, so a header that
    disagrees is a malformed or hostile archive. Decompression is bounded by the
    declared size, so a stream claiming to be small cannot expand past it, and
    declared sizes over ``max_member_bytes`` fall back before anything is read.
    """
    if (
        entry["compressed_size"] > max_member_bytes
        or entry["uncompressed_size"] > max_member_bytes
    ):
        raise RangeReadError(
            f"member {entry['name']!r} is over the {max_member_bytes}-byte ceiling"
        )
    if entry["header_offset"] + _LOCAL_SIZE + entry["compressed_size"] > entry.get(
        "data_end", float("inf")
    ):
        raise RangeReadError(
            f"member {entry['name']!r} extends into the central directory"
        )
    header = _read_exact(read, entry["header_offset"], _LOCAL_SIZE, "local header")
    if header[:4] != _LOCAL_SIGNATURE:
        raise RangeReadError(f"malformed local header for {entry['name']!r}")
    flags, method = struct.unpack("<HH", header[6:10])
    crc32, compressed_size, uncompressed_size, name_size, extra_size = struct.unpack(
        "<IIIHH", header[14:30]
    )
    local_name = _read_exact(
        read, entry["header_offset"] + _LOCAL_SIZE, name_size, "local header name"
    )
    if (
        flags != entry["flags"]
        or method != entry["method"]
        or crc32 != entry["crc32"]
        or compressed_size != entry["compressed_size"]
        or uncompressed_size != entry["uncompressed_size"]
        or local_name != entry["raw_name"]
    ):
        raise RangeReadError(
            f"local header disagrees with the central directory for "
            f"{entry['name']!r}"
        )
    data_start = entry["header_offset"] + _LOCAL_SIZE + name_size + extra_size
    # Re-checked with the local header's variable-length fields known: a crafted
    # extra_size must not move the payload into the directory region either.
    if data_start + entry["compressed_size"] > entry.get("data_end", float("inf")):
        raise RangeReadError(
            f"member {entry['name']!r} extends into the central directory"
        )
    compressed = _read_exact(
        read, data_start, entry["compressed_size"], f"member {entry['name']!r}"
    )
    expected_size = entry["uncompressed_size"]
    if entry["method"] == _STORED:
        data = compressed
    else:
        decompressor = zlib.decompressobj(wbits=-15)
        try:
            # Bounded: one byte past the declared size is enough to prove a lie
            # without materialising an expansion bomb.
            data = decompressor.decompress(compressed, expected_size + 1)
        except zlib.error as error:
            raise RangeReadError(f"bad deflate data in {entry['name']!r}: {error}")
        if len(data) > expected_size or not decompressor.eof:
            raise RangeReadError(
                f"member {entry['name']!r} does not decompress to its declared "
                f"{expected_size} bytes"
            )
    if len(data) != expected_size:
        raise RangeReadError(
            f"member {entry['name']!r} decompressed to {len(data)} bytes, "
            f"expected {expected_size}"
        )
    if zlib.crc32(data) & 0xFFFFFFFF != entry["crc32"]:
        raise RangeReadError(f"CRC mismatch in {entry['name']!r}")
    return data


def bytes_reader(data):
    """A ``read(start, size)`` over in-memory bytes, for tests and fallbacks."""

    def read(start, size):
        return data[start : start + size]

    return read
