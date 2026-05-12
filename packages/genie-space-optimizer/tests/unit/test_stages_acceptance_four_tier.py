from __future__ import annotations

import os
from dataclasses import dataclass, field
from unittest.mock import patch

from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    decide,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)


@dataclass
class _CtxRecorder:
    run_id: str = "r1"
    iteration: int = 1
    emitted: list = field(default_factory=list)

    def decision_emit(self, record):
        self.emitted.append(record)


def _make_canonical_for_ag(ag_id: str) -> ControlPlaneAcceptance:
    """Synthesize a ControlPlaneAcceptance whose buckets map to
    DIAGNOSTIC_HOLD under the spec's worked example for ccf1d60d."""
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=87.0,
        candidate_accuracy=91.3,
        delta_pp=4.3,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_026",),
        out_of_target_regressed_qids=("gs_012",),
        unknown_to_hard_regressed_qids=("gs_012",),
    )


def test_decide_emits_tier_classification_record_when_flag_on() -> None:
    canonical = _make_canonical_for_ag("AG1")
    inp = AcceptanceInput(
        ags=({"id": "AG1", "target_qids": ("gs_026",), "affected_questions": ("gs_026",)},),
        baseline_accuracy=87.0,
        candidate_accuracy=91.3,
        canonical_decisions_by_ag_id={"AG1": canonical},
    )
    ctx = _CtxRecorder()

    with patch.dict(os.environ, {"GSO_ACCEPTANCE_FOUR_TIER_GATE": "1"}, clear=True):
        decide(ctx, inp)

    reason_codes = [
        getattr(r.reason_code, "value", str(r.reason_code)) for r in ctx.emitted
    ]
    assert "tier_diagnostic_hold" in reason_codes, (
        f"expected tier_diagnostic_hold from worked example; "
        f"saw reason_codes={reason_codes}"
    )


def test_decide_does_not_emit_tier_record_when_flag_off() -> None:
    canonical = _make_canonical_for_ag("AG1")
    inp = AcceptanceInput(
        ags=({"id": "AG1", "target_qids": ("gs_026",), "affected_questions": ("gs_026",)},),
        baseline_accuracy=87.0,
        candidate_accuracy=91.3,
        canonical_decisions_by_ag_id={"AG1": canonical},
    )
    ctx = _CtxRecorder()

    with patch.dict(os.environ, {}, clear=True):
        decide(ctx, inp)

    reason_codes = [
        getattr(r.reason_code, "value", str(r.reason_code)) for r in ctx.emitted
    ]
    assert all(not str(rc).startswith("tier_") for rc in reason_codes), (
        f"flag OFF: no tier_* reason codes expected; saw {reason_codes}"
    )
