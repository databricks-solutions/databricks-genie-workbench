"""RCO-5 Task 6 — one-way projection from ParsedAcceptanceView to
ControlPlaneAcceptance.

The adapter fills observable fields from the view and sentinels the
unobservable ones (baseline/candidate accuracy floats become 0.0;
non-observable tuple buckets become ``()``). The projection is
intentionally lossy: it is a view-to-canonical-shape lift, not a
reconstruction of the original runtime decision.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)
from genie_space_optimizer.tools.lever_loop_stdout_parser import (
    ParsedAcceptanceView,
    parsed_view_to_control_plane,
)


def test_projection_returns_control_plane_acceptance() -> None:
    view = ParsedAcceptanceView(
        iteration=3,
        ag_id="AG_alpha",
        accepted=True,
        reason_code="accepted",
        target_qids=("q1", "q2"),
        target_fixed_qids=("q1",),
        target_still_hard_qids=("q2",),
        target_still_hard_qids_source="canonical_render",
    )
    out = parsed_view_to_control_plane(view)
    assert isinstance(out, ControlPlaneAcceptance)


def test_observable_fields_are_copied_through() -> None:
    view = ParsedAcceptanceView(
        iteration=3,
        ag_id="AG_alpha",
        accepted=True,
        reason_code="accepted_with_attribution_drift",
        target_qids=("q1", "q2"),
        target_fixed_qids=("q1",),
        target_still_hard_qids=("q2",),
        target_still_hard_qids_source="canonical_render",
    )
    out = parsed_view_to_control_plane(view)
    assert out.accepted is True
    assert out.reason_code == "accepted_with_attribution_drift"
    assert out.target_qids == ("q1", "q2")
    assert out.target_fixed_qids == ("q1",)
    assert out.target_still_hard_qids == ("q2",)


def test_unobservable_fields_are_sentinelled() -> None:
    view = ParsedAcceptanceView(
        iteration=3,
        ag_id="AG_alpha",
        accepted=False,
        reason_code="rejected_regression",
        target_qids=("q1",),
        target_fixed_qids=(),
        target_still_hard_qids=("q1",),
        target_still_hard_qids_source="canonical_render",
    )
    out = parsed_view_to_control_plane(view)
    # Float fields the stdout parser cannot observe are zeroed.
    assert out.baseline_accuracy == 0.0
    assert out.candidate_accuracy == 0.0
    assert out.delta_pp == 0.0
    # Every tuple bucket the parser does not observe is the empty tuple.
    assert out.out_of_target_regressed_qids == ()
    assert out.regression_debt_qids == ()
    assert out.protected_regressed_qids == ()
    assert out.soft_to_hard_regressed_qids == ()
    assert out.passing_to_hard_regressed_qids == ()
    assert out.unknown_to_hard_regressed_qids == ()
    assert out.target_delta_states == ()
    assert out.target_soft_passing_qids == ()
    assert out.accidentally_improved_qids == ()
    assert out.unresolved_target_debt_qids == ()
    assert out.existing_hard_still_hard_outside_target_qids == ()


def test_projection_is_pure() -> None:
    """The projection does not mutate the input view (it is frozen
    anyway, but check that calling twice produces equal outputs)."""
    view = ParsedAcceptanceView(
        iteration=3,
        ag_id="AG_alpha",
        accepted=True,
        reason_code="accepted",
        target_qids=("q1",),
        target_fixed_qids=("q1",),
        target_still_hard_qids=(),
        target_still_hard_qids_source="canonical_render",
    )
    a = parsed_view_to_control_plane(view)
    b = parsed_view_to_control_plane(view)
    assert a == b
