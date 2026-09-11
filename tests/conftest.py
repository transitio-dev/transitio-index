"""Put the shared index fixture on the path.

The build ships as the installed ``transitio_index`` package, so the suite
imports it directly. The publisher test also imports ``index_fixture``, whose
home is transitio; ``TRANSITIO_TESTS`` names a transitio checkout's ``tests``
directory when it is available.
"""

import os
import sys

_fixtures = os.environ.get("TRANSITIO_TESTS")
if _fixtures:
    sys.path.append(_fixtures)


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _skip_when_the_reader_predates_the_schema(monkeypatch):
    """Skip a test that reads a built index back through an installed
    transitio whose reader does not know the schema this build publishes: the
    reader release carrying the schema precedes the first publish, and the
    round trips resume once it is installed."""
    from transitio import index as reader
    from transitio.exceptions import IncompatibleIndexError
    from transitio_index import publish, publisher

    if publish.SCHEMA_VERSION in reader.SUPPORTED_SCHEMA_VERSIONS:
        return
    real = reader.read_index
    marker = f"schema_version {publish.SCHEMA_VERSION} is not one this transitio"

    def guarded(*args, **kwargs):
        try:
            return real(*args, **kwargs)
        except IncompatibleIndexError as error:
            if marker in str(error):
                pytest.skip(
                    f"installed transitio predates schema {publish.SCHEMA_VERSION}"
                )
            raise

    monkeypatch.setattr(reader, "read_index", guarded)
    monkeypatch.setattr(publisher, "read_index", guarded)
