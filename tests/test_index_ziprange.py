import io
import struct
import sys
import zipfile
import zlib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import ziprange  # noqa: E402

STOPS = b"stop_id,stop_name\n1,Central\n2,Harbour\n"
ROUTES = b"route_id,route_type\nr1,3\n"


def _zip_bytes(members, *, compression=zipfile.ZIP_DEFLATED, comment=b""):
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=compression) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
        if comment:
            archive.comment = comment
    return sink.getvalue()


def _directory(data):
    return ziprange.central_directory(ziprange.bytes_reader(data), len(data))


def test_members_round_trip_deflated_and_stored():
    for compression in (zipfile.ZIP_DEFLATED, zipfile.ZIP_STORED):
        data = _zip_bytes(
            {"stops.txt": STOPS, "routes.txt": ROUTES}, compression=compression
        )
        read = ziprange.bytes_reader(data)
        directory = ziprange.central_directory(read, len(data))
        assert set(directory) == {"stops.txt", "routes.txt"}
        assert ziprange.read_member(read, directory["stops.txt"]) == STOPS
        assert ziprange.read_member(read, directory["routes.txt"]) == ROUTES


def test_an_archive_comment_does_not_hide_the_record():
    data = _zip_bytes({"stops.txt": STOPS}, comment=b"made by a vendor" * 3)
    directory = _directory(data)
    assert "stops.txt" in directory


def test_a_forged_record_inside_the_comment_is_skipped():
    # A comment carrying EOCD-shaped bytes whose declared comment length does
    # not reach the archive end; the scan must fall through to the real record.
    forged = b"PK\x05\x06" + struct.pack("<HHHHIIH", 0, 0, 9, 9, 100, 0, 0)
    data = _zip_bytes({"stops.txt": STOPS}, comment=forged + b"trailing")
    directory = _directory(data)
    assert set(directory) == {"stops.txt"}


def test_a_record_whose_directory_does_not_end_at_it_is_refused():
    # Prefix junk shifts the directory away from the record (a self-extracting
    # archive, say): fallback rather than trusting shifted offsets.
    data = b"JUNK" * 4 + _zip_bytes({"stops.txt": STOPS})
    with pytest.raises(ziprange.RangeReadError, match="does not end at"):
        ziprange.end_of_central_directory(ziprange.bytes_reader(data), len(data))


def test_a_utf8_member_name_decodes():
    data = _zip_bytes({"pysäkit.txt": STOPS})
    assert "pysäkit.txt" in _directory(data)


def test_garbage_input_reports_no_record():
    junk = b"not a zip at all" * 100
    with pytest.raises(ziprange.RangeReadError, match="no end-of-central"):
        ziprange.end_of_central_directory(ziprange.bytes_reader(junk), len(junk))


def test_a_tiny_input_is_refused():
    with pytest.raises(ziprange.RangeReadError, match="too small"):
        ziprange.end_of_central_directory(ziprange.bytes_reader(b"PK"), 2)


def test_a_zip64_marker_falls_back():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    # Stamp both EOCD entry counts with the ZIP64 sentinel, as a real ZIP64
    # archive does.
    position = data.rfind(b"PK\x05\x06")
    struct.pack_into("<HH", data, position + 8, 0xFFFF, 0xFFFF)
    with pytest.raises(ziprange.RangeReadError, match="zip64"):
        ziprange.end_of_central_directory(ziprange.bytes_reader(bytes(data)), len(data))


def _flip_central_flag(data, bit):
    data = bytearray(data)
    position = data.find(b"PK\x01\x02")
    flags = struct.unpack_from("<H", data, position + 8)[0]
    struct.pack_into("<H", data, position + 8, flags | bit)
    return bytes(data)


def test_an_encrypted_member_falls_back():
    data = _flip_central_flag(_zip_bytes({"stops.txt": STOPS}), 0x0001)
    with pytest.raises(ziprange.RangeReadError, match="encrypted"):
        _directory(data)


def test_a_data_descriptor_member_falls_back():
    data = _flip_central_flag(_zip_bytes({"stops.txt": STOPS}), 0x0008)
    with pytest.raises(ziprange.RangeReadError, match="data-descriptor"):
        _directory(data)


def test_an_unsupported_compression_method_falls_back():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    position = data.find(b"PK\x01\x02")
    struct.pack_into("<H", data, position + 10, 12)  # bzip2
    with pytest.raises(ziprange.RangeReadError, match="unsupported compression"):
        _directory(bytes(data))


def test_corrupt_member_bytes_fail_the_crc():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}, compression=zipfile.ZIP_STORED))
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    entry = directory["stops.txt"]
    # Corrupt one byte of the stored member payload.
    header = data[entry["header_offset"] :]
    name_size, extra_size = struct.unpack("<HH", header[26:30])
    payload_at = entry["header_offset"] + 30 + name_size + extra_size
    data[payload_at] ^= 0xFF
    corrupted = ziprange.bytes_reader(bytes(data))
    with pytest.raises(ziprange.RangeReadError, match="CRC mismatch"):
        ziprange.read_member(corrupted, entry)


def test_corrupt_deflate_data_falls_back():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    entry = directory["stops.txt"]
    # Corrupt the first byte of the deflate stream; whichever check catches it
    # (a decode error, the length, or the CRC), the member read must fall back.
    header = data[entry["header_offset"] :]
    name_size, extra_size = struct.unpack("<HH", header[26:30])
    payload_at = entry["header_offset"] + 30 + name_size + extra_size
    data[payload_at] ^= 0xFF
    with pytest.raises(ziprange.RangeReadError):
        ziprange.read_member(ziprange.bytes_reader(bytes(data)), entry)


def test_a_short_range_read_is_reported():
    data = _zip_bytes({"stops.txt": STOPS})

    def short_read(start, size):
        return ziprange.bytes_reader(data)(start, size)[:-1]

    directory = ziprange.central_directory(ziprange.bytes_reader(data), len(data))
    with pytest.raises(ziprange.RangeReadError, match="short read"):
        ziprange.read_member(short_read, directory["stops.txt"])


def test_a_truncated_central_directory_is_refused():
    data = _zip_bytes({"stops.txt": STOPS})
    reader = ziprange.bytes_reader(data)
    entries, cd_offset, cd_size = ziprange.end_of_central_directory(reader, len(data))

    def lying_read(start, size):
        if start == cd_offset:
            return b"\x00" * size  # a directory that is not one
        return reader(start, size)

    with pytest.raises(ziprange.RangeReadError, match="malformed central"):
        ziprange.central_directory(lying_read, len(data))


def test_an_undecodable_utf8_name_falls_back():
    data = bytearray(_zip_bytes({"sssss.txt": STOPS}))
    position = data.find(b"PK\x01\x02")
    flags = struct.unpack_from("<H", data, position + 8)[0]
    struct.pack_into("<H", data, position + 8, flags | 0x0800)
    name_at = position + 46
    data[name_at : name_at + 5] = b"\xff\xfe\xff\xfe\xff"
    with pytest.raises(ziprange.RangeReadError, match="undecodable"):
        _directory(bytes(data))


def test_a_local_header_disagreeing_with_the_directory_falls_back():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    entry = directory["stops.txt"]
    # Tamper with the local header's method field only.
    struct.pack_into("<H", data, entry["header_offset"] + 8, 12)
    with pytest.raises(ziprange.RangeReadError, match="disagrees"):
        ziprange.read_member(ziprange.bytes_reader(bytes(data)), entry)


def test_a_declared_size_smaller_than_the_stream_is_bounded():
    # An expansion bomb claims a tiny size in BOTH headers (so the cross-check
    # passes); decompression must stop at the declared size, not materialise
    # the full expansion.
    big = b"A" * 200_000
    data = bytearray(_zip_bytes({"stops.txt": big}))
    lied_crc = zlib.crc32(big[:10]) & 0xFFFFFFFF
    central = data.find(b"PK\x01\x02")
    struct.pack_into("<I", data, central + 16, lied_crc)  # crc32
    struct.pack_into("<I", data, central + 24, 10)  # uncompressed size
    local = data.find(b"PK\x03\x04")
    struct.pack_into("<I", data, local + 14, lied_crc)
    struct.pack_into("<I", data, local + 22, 10)
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    with pytest.raises(ziprange.RangeReadError, match="declared"):
        ziprange.read_member(read, directory["stops.txt"])


def test_declared_sizes_over_the_member_ceiling_fall_back():
    data = _zip_bytes({"stops.txt": STOPS})
    read = ziprange.bytes_reader(data)
    directory = ziprange.central_directory(read, len(data))
    with pytest.raises(ziprange.RangeReadError, match="ceiling"):
        ziprange.read_member(read, directory["stops.txt"], max_member_bytes=4)


def test_a_directory_over_its_ceiling_falls_back():
    data = _zip_bytes({"stops.txt": STOPS})
    read = ziprange.bytes_reader(data)
    with pytest.raises(ziprange.RangeReadError, match="ceiling"):
        ziprange.central_directory(read, len(data), max_directory_bytes=8)


def test_a_duplicate_member_name_falls_back():
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        archive.writestr("stops.txt", STOPS)
        with pytest.warns(UserWarning):
            archive.writestr("stops.txt", ROUTES)
    data = sink.getvalue()
    with pytest.raises(ziprange.RangeReadError, match="duplicate member"):
        _directory(data)


def test_a_crafted_local_extra_size_cannot_reach_the_directory():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    entry = directory["stops.txt"]
    # Inflate only the LOCAL header's extra_size, shifting the payload window
    # toward the directory; the post-parse bounds check must refuse it.
    struct.pack_into("<H", data, entry["header_offset"] + 28, 60000)
    with pytest.raises(ziprange.RangeReadError, match="extends into"):
        ziprange.read_member(ziprange.bytes_reader(bytes(data)), entry)


def test_local_flags_must_equal_the_directory_flags():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    entry = directory["stops.txt"]
    # Set the UTF-8 bit in the local header only.
    flags = struct.unpack_from("<H", data, entry["header_offset"] + 6)[0]
    struct.pack_into("<H", data, entry["header_offset"] + 6, flags | 0x0800)
    with pytest.raises(ziprange.RangeReadError, match="disagrees"):
        ziprange.read_member(ziprange.bytes_reader(bytes(data)), entry)


def test_a_member_claiming_bytes_inside_the_directory_falls_back():
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    # Inflate the central directory's compressed-size field so the member would
    # run past the directory start.
    central = data.find(b"PK\x01\x02")
    struct.pack_into("<I", data, central + 20, len(data))
    read = ziprange.bytes_reader(bytes(data))
    directory = ziprange.central_directory(read, len(data))
    with pytest.raises(ziprange.RangeReadError, match="extends into"):
        ziprange.read_member(read, directory["stops.txt"])


def test_the_crc32_of_a_large_member_verifies():
    big = (b"x" * 100 + b"\n") * 5000
    data = _zip_bytes({"stop_times.txt": big})
    read = ziprange.bytes_reader(data)
    directory = ziprange.central_directory(read, len(data))
    assert ziprange.read_member(read, directory["stop_times.txt"]) == big
    assert zlib.crc32(big) & 0xFFFFFFFF == directory["stop_times.txt"]["crc32"]
