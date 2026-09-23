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
