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


def test_reflection_payload_carries_soft_signal_pass_rate_when_observability_flag_on() -> None:
    """Phase 1 Addendum — when the observability flag is on, the
    tier_classification_record's metrics include
    ``soft_signal_pass_rate``. The verdict itself (accepted_class) is
    identical to the same run without the observability flag — gate
    behaviour is unchanged."""
    canonical = _make_canonical_for_ag("AG1")
    eval_rows_with_partial_soft_pass = (
        {"qid": "gs_a", "soft_signal_results": {"q": "pass", "r": "fail"}},  # 1/2
        {"qid": "gs_b", "soft_signal_results": {"q": "pass", "r": "pass"}},  # 2/2
    )
    inp = AcceptanceInput(
        ags=(
            {"id": "AG1", "target_qids": ("gs_026",), "affected_questions": ("gs_026",)},
        ),
        baseline_accuracy=87.0,
        candidate_accuracy=91.3,
        canonical_decisions_by_ag_id={"AG1": canonical},
        post_rows=eval_rows_with_partial_soft_pass,
    )
    ctx = _CtxRecorder()

    with patch.dict(
        os.environ,
        {
            "GSO_ACCEPTANCE_FOUR_TIER_GATE": "1",
            "GSO_TIER_GATE_SOFT_SIGNAL_OBSERVABILITY": "1",
        },
        clear=True,
    ):
        decide(ctx, inp)

    tier_records = [
        r for r in ctx.emitted
        if str(getattr(r.reason_code, "value", r.reason_code)).startswith("tier_")
    ]
    assert tier_records, "expected at least one tier_* record"
    metrics = dict(tier_records[0].metrics or {})
    # 3 passes / 4 total = 0.75
    assert metrics.get("soft_signal_pass_rate") == 0.75


def test_tier_record_omits_soft_signal_pass_rate_when_observability_flag_off() -> None:
    """Phase 1 Addendum — flag OFF → metrics omit the rate entirely.
    Required for replay byte-stability on every captured run that
    pre-dates the addendum."""
    canonical = _make_canonical_for_ag("AG1")
    inp = AcceptanceInput(
        ags=(
            {"id": "AG1", "target_qids": ("gs_026",), "affected_questions": ("gs_026",)},
        ),
        baseline_accuracy=87.0,
        candidate_accuracy=91.3,
        canonical_decisions_by_ag_id={"AG1": canonical},
        post_rows=(
            {"qid": "gs_a", "soft_signal_results": {"q": "pass"}},
        ),
    )
    ctx = _CtxRecorder()

    with patch.dict(
        os.environ,
        {"GSO_ACCEPTANCE_FOUR_TIER_GATE": "1"},
        clear=True,
    ):
        decide(ctx, inp)

    tier_records = [
        r for r in ctx.emitted
        if str(getattr(r.reason_code, "value", r.reason_code)).startswith("tier_")
    ]
    assert tier_records, "expected a tier_* record with the four-tier flag on"
    metrics = dict(tier_records[0].metrics or {})
    assert "soft_signal_pass_rate" not in metrics
