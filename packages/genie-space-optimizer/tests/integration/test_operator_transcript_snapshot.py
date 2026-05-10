"""Snapshot test: operator transcript for airline anchor iter 01.

C15 Phase 5 Task 5.2. Exercises render_iteration_transcript() with the
typed_stage_io path using the airline anchor's boundary fixtures. Only
stages that have BOTH input.json and expected_output.json are included;
stages without fixtures are skipped (no synthetic shapes).

Stages captured in this snapshot (from Chunk D fixtures):
  - acceptance_decision
  - bundle_assembly
  - learning_next_action
  - run_manifest

Stages without fixtures (evaluation_state, rca_evidence, cluster_formation,
strategist_context, action_group_selection, proposal_generation, safety_gates,
applied_patches) render only the legacy placeholder text.

To refresh the snapshot after an intentional format change:
  1. Run scripts/generate_operator_transcript_snapshot.py.
  2. Include [fixture-refresh] in the PR title.
  3. Justify the change in the PR description.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.operator_process_transcript import (
    render_iteration_transcript,
)
from genie_space_optimizer.optimization.stages import STAGES


ANCHOR = "airline_1105451933925748_iter01"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / ANCHOR
SNAPSHOT = (
    Path(__file__).resolve().parent / "snapshots" / "operator_transcript_airline_iter01.txt"
)


def test_operator_transcript_matches_snapshot() -> None:
    if not SNAPSHOT.exists():
        pytest.skip(
            "snapshot not generated yet; run "
            "scripts/generate_operator_transcript_snapshot.py"
        )
    typed_io: dict[str, tuple] = {}
    for entry in STAGES:
        d = FIXTURES / entry.stage_key
        if not (d / "input.json").exists() or not (d / "expected_output.json").exists():
            continue
        inp = entry.input_class.from_json(json.loads((d / "input.json").read_text()))
        out = entry.output_class.from_json(
            json.loads((d / "expected_output.json").read_text())
        )
        typed_io[entry.stage_key] = (inp, out, ())
    actual = render_iteration_transcript(
        iteration=1,
        trace=None,
        iteration_summary={"verdict": "accepted_with_attribution_drift"},
        typed_stage_io=typed_io,
        fixture_anchor=ANCHOR,
    )
    expected = SNAPSHOT.read_text().rstrip("\n")
    assert actual.rstrip("\n") == expected, (
        "operator transcript drifted from snapshot. If this is intentional, "
        "regenerate with scripts/generate_operator_transcript_snapshot.py and "
        "include [fixture-refresh] in the PR title."
    )
