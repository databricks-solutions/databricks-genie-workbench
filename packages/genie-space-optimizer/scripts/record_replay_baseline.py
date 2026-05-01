"""Record the canonical journey ledger into the replay fixture.

Run this once after a deliberate change to the fixture or to a producer that
legitimately changes the canonical ledger. Commit the updated fixture.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(fixture_path: str) -> int:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    p = Path(fixture_path)
    fixture = json.loads(p.read_text())
    result = run_replay(fixture)
    if not result.validation.is_valid:
        print("Refusing to record baseline: validation failed.", file=sys.stderr)
        for v in result.validation.violations:
            print(
                f"  qid={v.question_id} kind={v.kind} detail={v.detail}",
                file=sys.stderr,
            )
        return 1
    fixture["expected_canonical_journey"] = result.canonical_json
    p.write_text(json.dumps(fixture, indent=2) + "\n")
    print(f"Recorded canonical baseline ({len(result.events)} events) into {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
