"""Top-level CLI shim for the Phase A replay-fixture extractor.

The real logic lives at
``genie_space_optimizer.scripts.extract_replay_fixture_from_log`` so it is
importable via the package's standard dotted path. This shim is the
user-facing CLI entry point invoked as:

    cd packages/genie-space-optimizer && uv run python \\
        scripts/extract_replay_fixture_from_log.py \\
        <input_log> <output_json>
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add ``src`` to sys.path so the package is importable when this shim is
# run directly (i.e., not as ``python -m ...``).
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from genie_space_optimizer.scripts.extract_replay_fixture_from_log import (  # noqa: E402
    main,
)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
