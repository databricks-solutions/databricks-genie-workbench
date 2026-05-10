"""One-off: generate the airline-iter-01 transcript snapshot.

Run once after Phase 5 dataclasses + renderer are in place. Writes
to ``tests/integration/snapshots/operator_transcript_airline_iter01.txt``.
On future plan changes, re-run with the ``[fixture-refresh]`` PR
discipline.

Usage::

    uv run --project packages/genie-space-optimizer \
        python packages/genie-space-optimizer/scripts/generate_operator_transcript_snapshot.py
"""
from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.operator_process_transcript import (
    render_iteration_transcript,
)
from genie_space_optimizer.optimization.stages import STAGES


ANCHOR = "airline_1105451933925748_iter01"
FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "integration" / "fixtures" / ANCHOR


def main() -> None:
    typed_io: dict[str, tuple] = {}
    for entry in STAGES:
        d = FIXTURES / entry.stage_key
        if not (d / "input.json").exists() or not (d / "expected_output.json").exists():
            continue
        inp = entry.input_class.from_json(json.loads((d / "input.json").read_text()))
        out = entry.output_class.from_json(json.loads((d / "expected_output.json").read_text()))
        typed_io[entry.stage_key] = (inp, out, ())

    text = render_iteration_transcript(
        iteration=1,
        trace=None,  # snapshot exercises the typed-block path; legacy decision-record path skipped
        iteration_summary={"verdict": "accepted_with_attribution_drift"},
        typed_stage_io=typed_io,
        fixture_anchor=ANCHOR,
    )
    target = (
        Path(__file__).resolve().parents[1]
        / "tests" / "integration" / "snapshots"
        / "operator_transcript_airline_iter01.txt"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text + "\n")
    print(f"wrote: {target}")
    print(f"stages captured: {sorted(typed_io)}")


if __name__ == "__main__":
    main()
