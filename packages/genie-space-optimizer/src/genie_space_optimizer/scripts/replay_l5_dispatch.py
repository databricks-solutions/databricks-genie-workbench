"""CLI shim — replay an L5 forced-synthesis dispatch fixture and print
a one-line JSON summary.

Operator workflow:

    uv run python -m genie_space_optimizer.scripts.replay_l5_dispatch \\
        --fixture <path-to-fixture.json>

Exits 0 if the fixture parses and replay completes; non-zero otherwise.
Always prints exactly one JSON line to stdout.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    args = parser.parse_args(argv[1:])

    if not args.fixture.exists():
        print(json.dumps({
            "error": "fixture_not_found",
            "fixture_path": str(args.fixture),
        }))
        return 2

    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    fixture = json.loads(args.fixture.read_text())
    result = run_forced_synthesis_replay(fixture=fixture)
    total_attempted = sum(
        len(it.attempted_dispatches) for it in result.iterations
    )
    total_appended = sum(
        len(it.appended_proposals) for it in result.iterations
    )
    total_emitted = sum(
        len(it.emitted_decision_records) for it in result.iterations
    )
    print(json.dumps({
        "fixture_id": result.fixture_id,
        "iterations": len(result.iterations),
        "total_attempted_dispatches": total_attempted,
        "total_appended_proposals": total_appended,
        "total_emitted_decision_records": total_emitted,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
