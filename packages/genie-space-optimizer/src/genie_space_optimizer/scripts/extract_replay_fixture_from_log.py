"""Extract a Phase A replay fixture from a job log.

The Lever Loop emits the fixture to stderr between two marker lines:
  ===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===
  <one-line compact JSON>
  ===PHASE_A_REPLAY_FIXTURE_JSON_END===

Usage:
  uv run python -m genie_space_optimizer.scripts.extract_replay_fixture_from_log \\
      <input_log> <output_json>

The output is pretty-printed JSON suitable for committing to
tests/replay/fixtures/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_BEGIN = "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN==="
_END = "===PHASE_A_REPLAY_FIXTURE_JSON_END==="


def extract_fixture_from_log_text(text: str) -> dict[str, Any]:
    """Parse the JSON between the two BEGIN/END marker lines."""
    if _BEGIN not in text or _END not in text:
        raise ValueError(
            f"Could not find both PHASE_A_REPLAY_FIXTURE markers in log text. "
            f"Searched for {_BEGIN!r} and {_END!r}."
        )
    after_begin = text.split(_BEGIN, 1)[1]
    inner = after_begin.split(_END, 1)[0]
    json_text = inner.strip()
    if not json_text:
        raise ValueError("Markers found but no JSON body between them.")
    return json.loads(json_text)


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "usage: extract_replay_fixture_from_log.py "
            "<input_log> <output_json>",
            file=sys.stderr,
        )
        return 2
    in_path = Path(argv[1])
    out_path = Path(argv[2])
    fixture = extract_fixture_from_log_text(in_path.read_text())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(fixture, indent=2, sort_keys=True) + "\n",
    )
    print(
        f"Extracted fixture (id={fixture.get('fixture_id', '?')}) "
        f"with {len(fixture.get('iterations') or [])} iteration(s) to "
        f"{out_path}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
