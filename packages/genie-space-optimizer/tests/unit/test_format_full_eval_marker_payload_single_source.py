"""Cycle 14-V Task 3 — canonical render must be contradiction-free.

When ``ControlPlaneAcceptance.target_delta_states`` is populated by
Cycle 14-T0, the rendered payload's ``target_fixed_qids`` /
``target_still_hard_qids`` MUST be derivable from
``target_delta_states`` alone, with zero same-QID disagreements
across rendered fields.

Anchored on:
- 7Now run 338386531912450 — gs_026 was rendered as both
  soft_to_hard (in target_delta_states) AND target_still_hard_qids.
  After T3, gs_026 appears in target_delta_states only; the
  legacy target_still_hard_qids tuple drops it.
- airline run 833709971504406 — gs_016 was rendered as
  unknown_to_hard_regressed_qids despite a known journey state.
  After T3, gs_016 is removed from unknown_to_hard when it appears
  as a target in target_delta_states.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    format_full_eval_marker_payload,
)


def _decision(
    *,
    target_qids: tuple[str, ...] = ("gs_026",),
    target_delta_states: tuple[tuple[str, str], ...] = (
        ("gs_026", DeltaState.SOFT_TO_HARD.value),
    ),
    target_fixed_qids: tuple[str, ...] = (),
    target_still_hard_qids: tuple[str, ...] = ("gs_026",),
    out_of_target_regressed_qids: tuple[str, ...] = ("gs_021", "gs_007"),
    unknown_to_hard_regressed_qids: tuple[str, ...] = ("gs_021", "gs_007"),
    soft_to_hard_regressed_qids: tuple[str, ...] = (),
    passing_to_hard_regressed_qids: tuple[str, ...] = (),
    accepted: bool = False,
    reason_code: str = "target_qids_not_improved",
) -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=accepted,
        reason_code=reason_code,
        baseline_accuracy=78.3,
        candidate_accuracy=87.0,
        delta_pp=8.7,
        target_qids=target_qids,
        target_fixed_qids=target_fixed_qids,
        target_still_hard_qids=target_still_hard_qids,
        target_delta_states=target_delta_states,
        out_of_target_regressed_qids=out_of_target_regressed_qids,
        regression_debt_qids=(),
        protected_regressed_qids=(),
        soft_to_hard_regressed_qids=soft_to_hard_regressed_qids,
        passing_to_hard_regressed_qids=passing_to_hard_regressed_qids,
        unknown_to_hard_regressed_qids=unknown_to_hard_regressed_qids,
    )


def test_target_delta_states_overrides_legacy_target_still_hard_qids_anchor_1():
    """Anchor 1: gs_026 classifies as soft_to_hard in delta_states.
    Pre-T3, the legacy bucket logic ALSO listed it in
    target_still_hard_qids.

    After T3: target_still_hard_qids is derived from
    target_delta_states (SOFT_TO_HARD ≠ STILL_HARD), so gs_026 does
    NOT appear in target_still_hard_qids."""
    decision = _decision(
        target_delta_states=(("gs_026", DeltaState.SOFT_TO_HARD.value),),
        target_still_hard_qids=("gs_026",),  # legacy says yes
    )

    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="ROLLBACK",
    )

    delta_qids = {q for q, _ in payload["target_delta_states"]}
    still_hard_qids = set(payload["target_still_hard_qids"])
    assert delta_qids == {"gs_026"}
    # Single source of truth — gs_026 is SOFT_TO_HARD, not STILL_HARD.
    assert still_hard_qids == set()


def test_target_delta_states_drives_target_fixed_qids():
    """When target_delta_states classifies a target as FIXED, the
    rendered target_fixed_qids reflects it even if legacy
    target_fixed_qids was empty."""
    decision = _decision(
        target_qids=("gs_024",),
        target_delta_states=(("gs_024", DeltaState.FIXED.value),),
        target_fixed_qids=(),  # legacy says nothing
        target_still_hard_qids=(),
    )

    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="ACCEPTED",
    )

    assert payload["target_fixed_qids"] == ["gs_024"]
    assert payload["target_still_hard_qids"] == []


def test_target_qids_subtracted_from_unknown_to_hard_anchor_2():
    """Anchor 2: gs_016 is a target QID exhaustively classified by
    target_delta_states. The legacy unknown_to_hard bucket also
    listed it (a sign the legacy pipeline drifted).

    After T3: any QID present in target_delta_states is removed
    from the rendered unknown_to_hard_regressed_qids — the helper
    treats target_delta_states as exhaustive over targets."""
    decision = _decision(
        target_qids=("gs_016",),
        target_delta_states=(("gs_016", DeltaState.SOFT_TO_HARD.value),),
        unknown_to_hard_regressed_qids=("gs_016", "gs_021"),
        target_still_hard_qids=(),
    )

    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="ROLLBACK",
    )

    # gs_016 is exhaustively classified by target_delta_states so it
    # does NOT appear in unknown_to_hard.
    assert "gs_016" not in payload["unknown_to_hard_regressed_qids"]
    # Non-target QIDs flow through unchanged.
    assert "gs_021" in payload["unknown_to_hard_regressed_qids"]


def test_legacy_fall_through_when_target_delta_states_empty():
    """Pre-T0 fixtures have empty target_delta_states. Render falls
    through to legacy fields verbatim (back-compat)."""
    decision = _decision(
        target_delta_states=(),
        target_still_hard_qids=("gs_026",),
        target_fixed_qids=(),
    )
    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="ROLLBACK",
    )
    assert payload["target_delta_states"] == []
    # Legacy rendering preserved for pre-T0 fixtures.
    assert payload["target_still_hard_qids"] == ["gs_026"]


def test_canonical_render_is_self_consistent_anchor_2_partial_harvest():
    """Anchor 2 (airline AG2 partial-harvest accept) — the rendered
    payload must NOT have any QID classified two different ways."""
    decision = _decision(
        target_qids=("gs_024",),
        target_delta_states=(("gs_024", DeltaState.FIXED.value),),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_016",),
        unknown_to_hard_regressed_qids=("gs_016",),
        accepted=True,
        reason_code="accepted_with_partial_harvest_debt",
    )

    payload = format_full_eval_marker_payload(
        decision, ag_id="AG2", iteration=2, accepted_label="ACCEPTED",
    )

    target_qid_set = {q for q, _ in payload["target_delta_states"]}
    fixed_set = set(payload["target_fixed_qids"])
    still_hard_set = set(payload["target_still_hard_qids"])

    # No QID can be simultaneously fixed AND still-hard.
    assert fixed_set.isdisjoint(still_hard_set)
    # gs_024 is FIXED — appears in target_delta_states + fixed_qids only.
    assert "gs_024" in fixed_set
    assert "gs_024" not in still_hard_set
