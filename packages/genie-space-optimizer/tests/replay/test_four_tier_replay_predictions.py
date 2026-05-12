"""Phase 1 Action 1.2 — replay test that classifies the Phase 0 fixtures
through the four-tier classifier and asserts each lands in the
predicted tier."""
from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.acceptance_policy import (
    classify_acceptance_tier,
    tier_acceptance_policy_pilot_default,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "policy_replay"
PREDICTIONS = json.loads(
    (FIXTURES_DIR / "ccf1d60d_iter1_tier_prediction.json").read_text()
)


def _decision_from_phase0_fixture(payload: dict) -> ControlPlaneAcceptance:
    """Synthesize a ControlPlaneAcceptance from the Phase 0 replay
    fixture's bucket lists (which live at the top level of the
    fixture, not nested)."""
    baseline = float(payload.get("baseline_post_arbiter") or 0.0)
    candidate = float(payload.get("candidate_post_arbiter") or 0.0)
    return ControlPlaneAcceptance(
        accepted=bool(payload.get("accepted_in_recorded_run", False)),
        reason_code=str(payload.get("reason_code_in_recorded_run") or "ignored_for_test"),
        baseline_accuracy=baseline,
        candidate_accuracy=candidate,
        delta_pp=round(candidate - baseline, 2),
        target_qids=tuple(payload.get("target_qids") or ()),
        target_fixed_qids=tuple(payload.get("target_fixed_qids") or ()),
        target_still_hard_qids=tuple(payload.get("target_still_hard_qids") or ()),
        out_of_target_regressed_qids=tuple(payload.get("out_of_target_regressed_qids") or ()),
        passing_to_hard_regressed_qids=tuple(payload.get("passing_to_hard_regressed_qids") or ()),
        soft_to_hard_regressed_qids=tuple(payload.get("soft_to_hard_regressed_qids") or ()),
        unknown_to_hard_regressed_qids=tuple(payload.get("unknown_to_hard_regressed_qids") or ()),
    )


def test_ccf1d60d_iter1_lands_in_diagnostic_hold() -> None:
    fixture = json.loads((FIXTURES_DIR / "ccf1d60d_iter1.json").read_text())
    decision = _decision_from_phase0_fixture(fixture)
    verdict = classify_acceptance_tier(
        decision=decision, policy=tier_acceptance_policy_pilot_default(),
    )
    expected = PREDICTIONS["predictions"]["ccf1d60d_iter1"]
    assert verdict.accepted_class.value == expected["expected_accepted_class"], (
        f"Expected {expected['expected_accepted_class']}, got "
        f"{verdict.accepted_class.value}. Rationale: "
        f"{expected['rationale']}"
    )
    assert verdict.accept is expected["expected_accept"]
