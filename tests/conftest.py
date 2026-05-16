from pathlib import Path
import sys


# Ensure absolute imports like `from app...` work when tests are run via the
# `pytest` entrypoint (where sys.path[0] can point to the virtualenv bin dir).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
