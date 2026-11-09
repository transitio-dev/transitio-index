"""Put the shared index fixture on the path, and fix the Overture release and
read no local Overture copy.

The build ships as the installed ``transitio_index`` package, so the suite
imports it directly. The publisher test also imports ``index_fixture``, whose
home is transitio; ``TRANSITIO_TESTS`` names a transitio checkout's ``tests``
directory when it is available.
"""

import importlib.util
import os
import sys

import pytest

_fixtures = os.environ.get("TRANSITIO_TESTS")
if _fixtures:
    sys.path.append(_fixtures)

# The release the fixtures name; no test looks the newest one up.
FIXTURE_RELEASE = "2026-08-19.0"


@pytest.fixture(autouse=True)
def overture_release(monkeypatch):
    """Fix every test's Overture release to the fixtures' release. Without
    pyarrow there is no Overture module to fix; the tests that need it skip."""
    if importlib.util.find_spec("pyarrow") is None:
        return
    from transitio_index import overture

    monkeypatch.setattr(overture, "_release", FIXTURE_RELEASE)
    # The bucket stand-ins are read directly unless a test names a local copy.
    monkeypatch.setattr(overture, "_local_root", None)
    monkeypatch.setattr(overture, "_local_earlier", None)
    monkeypatch.setattr(overture, "_local_copies", {})
    # Wikidata is never answered from the machine's own copy.
    monkeypatch.setattr(overture, "_wikidata_root", None)
    monkeypatch.setattr(overture, "_wikidata_warned", False)
