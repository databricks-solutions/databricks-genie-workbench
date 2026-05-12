"""Plan P-C — verifies that when the harness supplies a canonical
ControlPlaneAcceptance, stages.acceptance.decide reuses it instead
of recomputing the gate."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
)
from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    decide,
)


@dataclass
class _StubCtx:
    run_id: str = "run-1"
    iteration: int = 1
    emitted: list[Any] = field(default_factory=list)

    def decision_emit(self, record):
        self.emitted.append(record)


def test_decide_reuses_canonical_decision_when_supplied():
    canonical = ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=0.870,
        candidate_accuracy=0.870,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_012",),
        target_delta_states=(("gs_026", DeltaState.STILL_HARD.value),),
    )
    inp = AcceptanceInput(
        ags=({"id": "ag1", "target_qids": ["gs_026"]},),
        baseline_accuracy=0.870,
        candidate_accuracy=0.870,
        canonical_decisions_by_ag_id={"ag1": canonical},
    )
    ctx = _StubCtx()
    out = decide(ctx, inp)

    rec = out.outcomes_by_ag["ag1"]
    assert rec.reason_code == "target_qids_not_improved"
    assert rec.outcome == "rolled_back"


def test_decide_recomputes_when_canonical_not_supplied():
    """Backwards-compat: empty canonical_decisions_by_ag_id falls
    back to the recompute path so existing replay fixtures stay green."""
    inp = AcceptanceInput(
        ags=({"id": "ag1", "target_qids": ["gs_026"]},),
        baseline_accuracy=0.5,
        candidate_accuracy=0.5,
    )
    ctx = _StubCtx()
    out = decide(ctx, inp)
    assert "ag1" in out.outcomes_by_ag
