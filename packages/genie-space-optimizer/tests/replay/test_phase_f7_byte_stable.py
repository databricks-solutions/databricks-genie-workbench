"""Phase F7 byte-stability gate.

Asserts the post-F7 replay output is byte-identical to the pre-F7
snapshot. F7 is observability-only (a NEW stages/application.py module
wrapping patch_applied_records emission), so harness is untouched and
replay output should trivially match.
"""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import run_replay


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "before_f7.json"
FIXTURE_PATH = (
    Path(__file__).parents[1] / "replay" / "fixtures" / "airline_real_v1.json"
)


def test_phase_f7_replay_is_byte_stable() -> None:
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
