"""Phase F8 byte-stability gate.

Asserts the post-F8 replay output is byte-identical to the pre-F8
snapshot. F8 is observability-only (a NEW stages/acceptance.py module
wrapping decide_control_plane_acceptance + ag_outcome_decision_record
+ post_eval_resolution_records), so harness is untouched and replay
output should trivially match.
"""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import run_replay


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "before_f8.json"
FIXTURE_PATH = (
    Path(__file__).parents[1] / "replay" / "fixtures" / "airline_real_v1.json"
)


def test_phase_f8_replay_is_byte_stable() -> None:
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
