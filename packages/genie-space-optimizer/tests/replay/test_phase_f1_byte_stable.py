"""Phase F1 byte-stability gate.

Runs the airline_real_v1 replay against the post-F1 codebase and asserts
the canonical journey / decision JSON, operator transcript, and
validation report are byte-identical to the pre-F1 snapshot captured in
Task 7.

The replay does NOT execute harness.py production code paths — it only
exercises lever_loop_replay.run_replay over the fixture, which is
itself a pure function over OptimizationTrace + JourneyValidationReport.
The byte-stability assertion catches any drift in the journey-emit /
decision-record schema introduced by F1, even though F1 didn't change
those producers directly.
"""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import run_replay


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "before_f1.json"
FIXTURE_PATH = (
    Path(__file__).parents[1] / "replay" / "fixtures" / "airline_real_v1.json"
)


def test_phase_f1_replay_is_byte_stable() -> None:
    expected = json.loads(SNAPSHOT_PATH.read_text())

    with FIXTURE_PATH.open() as f:
        fixture = json.load(f)
    actual = run_replay(fixture)

    assert actual.canonical_json == expected["canonical_journey_json"], (
        "Phase F1 must not change the canonical journey JSON"
    )
    assert (
        actual.canonical_decision_json == expected["canonical_decision_json"]
    ), "Phase F1 must not change the canonical decision JSON"
    assert actual.operator_transcript == expected["operator_transcript"], (
        "Phase F1 must not change the operator transcript"
    )
    assert (
        actual.validation.is_valid == expected["validation_is_valid"]
    ), "Phase F1 must not change journey validation outcome"
    assert (
        list(actual.validation.missing_qids)
        == expected["validation_missing_qids"]
    ), "Phase F1 must not change missing_qids"
    assert (
        list(actual.decision_validation)
        == expected["decision_validation"]
    ), "Phase F1 must not change decision_validation"
