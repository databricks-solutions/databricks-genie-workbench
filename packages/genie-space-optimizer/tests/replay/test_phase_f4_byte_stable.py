"""Phase F4 byte-stability gate.

Asserts the post-F4 replay output is byte-identical to the pre-F4
snapshot. F4 is observability-only (a NEW stages/action_groups.py
module wrapping strategist_ag_records emission), so harness is
untouched and replay output should trivially match.
"""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import run_replay


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "before_f4.json"
FIXTURE_PATH = (
    Path(__file__).parents[1] / "replay" / "fixtures" / "airline_real_v1.json"
)


def test_phase_f4_replay_is_byte_stable() -> None:
    expected = json.loads(SNAPSHOT_PATH.read_text())

    with FIXTURE_PATH.open() as f:
        fixture = json.load(f)
    actual = run_replay(fixture)

    assert actual.canonical_json == expected["canonical_journey_json"]
    assert actual.canonical_decision_json == expected["canonical_decision_json"]
    assert actual.operator_transcript == expected["operator_transcript"]
    assert actual.validation.is_valid == expected["validation_is_valid"]
    assert (
        list(actual.validation.missing_qids)
        == expected["validation_missing_qids"]
    )
    assert list(actual.decision_validation) == expected["decision_validation"]
