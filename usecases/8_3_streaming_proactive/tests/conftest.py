"""Re-exports the shared semantic_api_server fixture, same pattern as
usecases/8_1_batch_reactive/tests/conftest.py -- this directory isn't a
descendant of tests/, so pytest's conftest auto-discovery doesn't see it
without an explicit import."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT))

from tests.conftest import semantic_api_server  # noqa: F401,E402
