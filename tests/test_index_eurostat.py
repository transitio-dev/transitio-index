import hashlib
import io
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import eurostat  # noqa: E402

PAYLOADS = {
    eurostat.COMPOSITION_FILE: b"PK\x03\x04 a workbook",
    eurostat.BOUNDARIES_FILE: b"PAR1 a parquet file",
}


def _inputs(tmp_path, payloads=PAYLOADS):
    tmp_path.mkdir(exist_ok=True)
    files = {}
    for name, data in payloads.items():
        files[name] = tmp_path / name
        files[name].write_bytes(data)
    expected = {
        name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()
    }
    return files, expected


def test_inputs_are_pinned_published_and_reused(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    manifest = eurostat.prepare_inputs(cache, files=files, expected=expected)
    assert manifest["digests"] == expected
    assert manifest["urls"] == {name: None for name in expected}
    assert manifest["nuts_version"] == eurostat.NUTS_VERSION
    # Published under the same digests: reused, not republished.
    again = eurostat.prepare_inputs(cache, files=files, expected=expected)
    assert again["generation"] == manifest["generation"]
    generation, resolved = eurostat.resolve_inputs(cache, expected=expected)
    with generation:
        assert resolved["generation"] == manifest["generation"]
        assert (
            generation.read_bytes(eurostat.COMPOSITION_FILE)
            == PAYLOADS[eurostat.COMPOSITION_FILE]
        )
    # Other pins than the generation carries: refused, whether they are this
    # module's real pins or a mismatching local file.
    with pytest.raises(eurostat.EurostatError, match="other inputs"):
        eurostat.resolve_inputs(cache)
    wrong = {**expected, eurostat.COMPOSITION_FILE: "0" * 64}
    with pytest.raises(eurostat.EurostatError, match="does not match the pinned"):
        eurostat.prepare_inputs(cache, files=files, expected=wrong)
    with pytest.raises(eurostat.EurostatError, match="no pinned URL"):
        eurostat.prepare_inputs(cache, files=files, expected={"other.bin": "0" * 64})


def test_a_generation_with_other_digests_is_republished(tmp_path):
    cache = tmp_path / "cache"
    files, expected = _inputs(tmp_path)
    first = eurostat.prepare_inputs(cache, files=files, expected=expected)
    newer = {**PAYLOADS, eurostat.COMPOSITION_FILE: b"PK\x03\x04 a reissued workbook"}
    files, expected = _inputs(tmp_path / "newer", newer)
    second = eurostat.prepare_inputs(cache, files=files, expected=expected)
    assert second["generation"] != first["generation"]
    assert second["digests"] == expected


class _Response:
    """A minimal ``urlopen`` result: a body with a Content-Length header."""

    def __init__(self, body):
        self._body = io.BytesIO(body)
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self, size=-1):
        return self._body.read(size)


def test_downloaded_inputs_are_verified_against_the_pins(tmp_path, monkeypatch):
    monkeypatch.setattr(
        eurostat.csv_source.urllib.request,
        "urlopen",
        lambda url, timeout=None: _Response(PAYLOADS[url.rsplit("/", 1)[-1]]),
    )
    cache = tmp_path / "cache"
    expected = {
        name: hashlib.sha256(data).hexdigest() for name, data in PAYLOADS.items()
    }
    # A download that does not match its pin is refused and leaves nothing
    # cached: no input file, no pointer.
    wrong = {**expected, eurostat.BOUNDARIES_FILE: "0" * 64}
    with pytest.raises(eurostat.EurostatError, match="does not match the pinned"):
        eurostat.prepare_inputs(cache, expected=wrong)
    assert [
        p.name for p in (cache / "raw").iterdir() if not p.name.startswith(".")
    ] == []
    manifest = eurostat.prepare_inputs(cache, expected=expected)
    assert manifest["urls"] == eurostat.URLS
    generation, _ = eurostat.resolve_inputs(cache, expected=expected)
    with generation:
        assert (
            generation.read_bytes(eurostat.BOUNDARIES_FILE)
            == PAYLOADS[eurostat.BOUNDARIES_FILE]
        )
