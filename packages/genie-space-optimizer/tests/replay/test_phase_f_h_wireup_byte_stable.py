"""Phase F+H Harness Wire-up byte-stability gate.

Asserts replay output is byte-identical to the pre-wire-up snapshot.
Every Phase A + Phase B commit must keep this test passing. Phase C
commits also keep it passing — they ADD bundle assembly but do not
modify any decision-emission or journey-emit behavior.
"""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import run_replay


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "before_f_h_wireup.json"
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "airline_real_v1.json"


def test_phase_f_h_wireup_replay_is_byte_stable() -> None:
    expected = json.loads(SNAPSHOT_PATH.read_text())

    with FIXTURE_PATH.open() as f:
        fixture = json.load(f)
    actual = run_replay(fixture)

    assert actual.canonical_json == expected["canonical_journey_json"], (
        "F+H wire-up must not change the canonical journey JSON"
    )
    assert (
        actual.canonical_decision_json == expected["canonical_decision_json"]
    ), "F+H wire-up must not change the canonical decision JSON"
    assert actual.operator_transcript == expected["operator_transcript"], (
        "F+H wire-up must not change the operator transcript"
    )
    assert (
        actual.validation.is_valid == expected["validation_is_valid"]
    ), "F+H wire-up must not change journey validation outcome"
    assert (
        list(actual.validation.missing_qids)
        == expected["validation_missing_qids"]
    ), "F+H wire-up must not change missing_qids"
    assert (
        list(actual.decision_validation) == expected["decision_validation"]
    ), "F+H wire-up must not change decision_validation"
