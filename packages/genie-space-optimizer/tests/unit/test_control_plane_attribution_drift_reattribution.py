"""Cycle 14-C T2 + T3 — ControlPlaneAcceptance field extension and
reattribution wire-up."""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)


def test_dataclass_accepts_new_fields_with_defaults() -> None:
    """Back-compat: legacy callers that don't pass the new fields
    still construct a ControlPlaneAcceptance successfully; the new
    fields default to empty tuples."""
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted",
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        delta_pp=12.5,
        target_qids=("gs_024",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
    )
    assert decision.accidentally_improved_qids == ()
    assert decision.unresolved_target_debt_qids == ()


def test_dataclass_accepts_new_fields_when_explicitly_passed() -> None:
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted_with_attribution_drift",
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        delta_pp=12.5,
        target_qids=("gs_024",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
        accidentally_improved_qids=("gs_007", "gs_009", "gs_013"),
        unresolved_target_debt_qids=("gs_024",),
    )
    assert decision.accidentally_improved_qids == ("gs_007", "gs_009", "gs_013")
    assert decision.unresolved_target_debt_qids == ("gs_024",)
