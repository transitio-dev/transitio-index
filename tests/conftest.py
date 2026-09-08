"""Put the build package and the shared index fixture on the path.

The suite runs from a checkout, not an installed package: ``scripts`` holds
the build (``index_build``) and the entry points. The publisher test also
imports ``index_fixture``, whose home is transitio; ``TRANSITIO_TESTS`` names
a transitio checkout's ``tests`` directory when it is available.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

_fixtures = os.environ.get("TRANSITIO_TESTS")
if _fixtures:
    sys.path.insert(0, _fixtures)
