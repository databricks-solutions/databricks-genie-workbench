"""Phase H Fidelity Task 3 — pin acceptance-detail wiring on
``ag_outcome_decision_record``.

Run ``11110001-0000-4000-8000-000000000001`` exhibited a Stage 9
(Acceptance / Rollback) section that read ``(no decisions emitted for
this stage in this iteration)`` despite the run's stdout postmortem
clearly logging an acceptance decision: ``rollback`` with
``reason=target_qids_not_improved``, target qids ``gs_026``, no fixed
qids, and regressions ``gs_007`` / ``gs_022``.

The root cause is two-fold:

1. The ``ACCEPTANCE_DECIDED`` record collapsed every rolled-back outcome
   into ``reason_code=PATCH_SKIPPED`` with no ``reason_detail``,
   regression bucket counts, or attribution detail.
2. Even when the F8 stage ran successfully, the rich
   ``ControlPlaneAcceptance`` decision was discarded at the emitter
   boundary.

This test file pins the new contract: when a control-plane decision is
threaded into ``ag_outcome_decision_record`` via ``acceptance_detail``,
the resulting record carries the rich ``reason_code``, ``reason_detail``,
``regression_qids``, and per-bucket metric counts so the Stage 9
renderer can surface the actual rejection cause.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)
from genie_space_optimizer.optimization.decision_emitters import (
    ag_outcome_decision_record,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def _ag_dict(ag_id: str = "AG_001", *, target_qids=("gs_026",)) -> dict:
    return {
        "id": ag_id,
        "affected_questions": list(target_qids),
        "target_qids": list(target_qids),
        "source_cluster_ids": ["c1"],
        "root_cause_summary": "wrong filter",
    }


def _cp_decision(
    *,
    accepted: bool,
    reason_code: str,
    target_qids=("gs_026",),
    target_fixed_qids=(),
    target_still_hard_qids=("gs_026",),
    out_of_target_regressed_qids=("gs_007", "gs_022"),
) -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=accepted,
        reason_code=reason_code,
        baseline_accuracy=0.78,
        candidate_accuracy=0.78,
        delta_pp=0.0,
        target_qids=tuple(target_qids),
        target_fixed_qids=tuple(target_fixed_qids),
        target_still_hard_qids=tuple(target_still_hard_qids),
        out_of_target_regressed_qids=tuple(out_of_target_regressed_qids),
    )


# ── Backward compatibility ────────────────────────────────────────


def test_record_without_acceptance_detail_preserves_legacy_behavior() -> None:
    """When ``acceptance_detail`` is None, the rolled_back record still
    uses the legacy ``PATCH_SKIPPED`` reason code so existing callers and
    snapshots remain stable."""
    rec = ag_outcome_decision_record(
        run_id="r1",
        iteration=1,
        ag=_ag_dict(),
        outcome="rolled_back",
        regression_qids=("gs_007",),
    )
    assert rec is not None
    assert rec.decision_type == DecisionType.ACCEPTANCE_DECIDED
    assert rec.outcome == DecisionOutcome.ROLLED_BACK
    assert rec.reason_code == ReasonCode.PATCH_SKIPPED
    assert rec.reason_detail == ""
    assert rec.regression_qids == ("gs_007",)


# ── Rolled back: reason is preserved ──────────────────────────────


def test_rolled_back_record_preserves_target_qids_not_improved_reason() -> None:
    decision = _cp_decision(
        accepted=False,
        reason_code="target_qids_not_improved",
    )
    rec = ag_outcome_decision_record(
        run_id="r1",
        iteration=1,
        ag=_ag_dict(),
        outcome="rolled_back",
        acceptance_detail=decision,
    )
    assert rec is not None
    assert rec.outcome == DecisionOutcome.ROLLED_BACK
    assert rec.reason_code == ReasonCode.TARGET_QIDS_NOT_IMPROVED
    # reason_detail must include the actual reason and the target/regress qids
    assert "target_qids_not_improved" in rec.reason_detail
    assert "gs_026" in rec.reason_detail
    assert "gs_007" in rec.reason_detail
    # regression_qids must come from the control-plane decision when
    # acceptance_detail is supplied (overriding the explicit kwarg path).
    assert set(rec.regression_qids) == {"gs_007", "gs_022"}
    # metrics must surface the bucket counts so downstream consumers
    # can render them without re-parsing the detail string.
    assert int(rec.metrics.get("target_qids_count") or 0) == 1
    assert int(rec.metrics.get("target_fixed_count") or 0) == 0
    assert int(rec.metrics.get("target_still_hard_count") or 0) == 1
    assert int(rec.metrics.get("out_of_target_regressed_count") or 0) == 2


def test_rolled_back_record_unbounded_collateral_reason() -> None:
    decision = _cp_decision(
        accepted=False,
        reason_code="rejected_unbounded_collateral",
        target_fixed_qids=("gs_026",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_001", "gs_002", "gs_003", "gs_004"),
    )
    rec = ag_outcome_decision_record(
        run_id="r1",
        iteration=2,
        ag=_ag_dict(),
        outcome="rolled_back",
        acceptance_detail=decision,
    )
    assert rec is not None
    assert rec.reason_code == ReasonCode.REJECTED_UNBOUNDED_COLLATERAL
    assert "rejected_unbounded_collateral" in rec.reason_detail
    assert int(rec.metrics.get("out_of_target_regressed_count") or 0) == 4


# ── Accepted: detail still surfaces context ───────────────────────


def test_accepted_record_carries_acceptance_reason_detail_when_supplied() -> None:
    decision = _cp_decision(
        accepted=True,
        reason_code="accepted_pre_arbiter_improvement",
        target_fixed_qids=("gs_026",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
    )
    rec = ag_outcome_decision_record(
        run_id="r1",
        iteration=3,
        ag=_ag_dict(),
        outcome="accepted",
        acceptance_detail=decision,
    )
    assert rec is not None
    assert rec.outcome == DecisionOutcome.ACCEPTED
    # Accepted with a typed CP reason: the enum value upgrades from the
    # generic PATCH_APPLIED to the precise acceptance variant so the
    # operator can see *why* it was accepted (without having to parse
    # reason_detail).
    assert rec.reason_code == ReasonCode.ACCEPTED_PRE_ARBITER_IMPROVEMENT
    assert "accepted_pre_arbiter_improvement" in rec.reason_detail


# ── Unknown CP reason codes degrade gracefully ────────────────────


def test_record_with_unknown_cp_reason_falls_back_to_legacy_reason_code() -> None:
    """Reason strings that have no matching ``ReasonCode`` enum value
    must not crash the emitter. The record falls back to
    ``PATCH_SKIPPED`` for rolled_back / ``PATCH_APPLIED`` for accepted
    while still preserving the raw reason inside ``reason_detail``."""
    decision = _cp_decision(
        accepted=False,
        reason_code="some_future_reason_not_yet_typed",
    )
    rec = ag_outcome_decision_record(
        run_id="r1",
        iteration=1,
        ag=_ag_dict(),
        outcome="rolled_back",
        acceptance_detail=decision,
    )
    assert rec is not None
    assert rec.reason_code == ReasonCode.PATCH_SKIPPED
    assert "some_future_reason_not_yet_typed" in rec.reason_detail
