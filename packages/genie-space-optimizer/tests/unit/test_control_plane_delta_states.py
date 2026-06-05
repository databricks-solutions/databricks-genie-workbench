"""Cycle 14-T0 — DeltaState enum + compute_target_delta_states.

The helper is total over target_qids: every target QID lands in
exactly one DeltaState value, never silently absent. The new
LOOKUP_FAILED value is the explicit "I could not resolve this
target" answer and replaces the legacy set-arithmetic
fall-through that yielded target_fixed=() AND target_still=()
simultaneously (new anchor 76457773587391 F2).
"""

from __future__ import annotations


def _row(qid: str, rc: str, arbiter: str) -> dict:
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


PRE_HARD_ROW = _row("gs_026", "no", "ground_truth_correct")
POST_FIX_ROW = _row("gs_026", "yes", "both_correct")
# Actionable soft signal: arbiter rescued the row (rc=yes/both_correct) but
# a non-info-only judge still flagged "no". row_status -> "soft".
SOFT_ROW = {
    "question_id": "gs_018",
    "result_correctness": "yes",
    "arbiter": "both_correct",
    "feedback/sql_correctness/value": "no",
}
HARD_ROW = _row("gs_018", "no", "ground_truth_correct")
PASS_ROW = _row("gs_001", "yes", "both_correct")


def test_delta_state_enum_values_are_canonical() -> None:
    from genie_space_optimizer.optimization.control_plane import DeltaState

    expected = {
        "FIXED",
        "STILL_HARD",
        "SOFT_TO_HARD",
        "SOFT_PASSING",
        "REGRESSED_TO_UNKNOWN",
        "LOOKUP_FAILED",
    }
    assert {member.name for member in DeltaState} == expected
    # StrEnum: each member's value equals its lowercase name; this
    # is the JSON-serialised form persisted in ControlPlaneAcceptance.
    assert DeltaState.FIXED.value == "fixed"
    assert DeltaState.LOOKUP_FAILED.value == "lookup_failed"


def test_delta_states_total_over_target_qids() -> None:
    """Every declared target QID appears in the result dict exactly once."""
    from genie_space_optimizer.optimization.control_plane import (
        compute_target_delta_states,
    )

    targets = ("gs_026", "gs_018", "gs_999")
    result = compute_target_delta_states(
        target_qids=targets,
        pre_rows=(PRE_HARD_ROW, SOFT_ROW),
        post_rows=(POST_FIX_ROW, HARD_ROW),
        candidate_failed_qids=("gs_018",),
    )
    assert set(result.keys()) == set(targets)


def test_delta_states_resolves_baseline_hard_to_fixed() -> None:
    """gs_026 was hard at baseline, absent from candidate hard set -> FIXED."""
    from genie_space_optimizer.optimization.control_plane import (
        DeltaState,
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_026",),
        pre_rows=(PRE_HARD_ROW,),
        post_rows=(POST_FIX_ROW,),
        candidate_failed_qids=(),
    )
    assert result["gs_026"] == DeltaState.FIXED


def test_delta_states_resolves_still_hard() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        DeltaState,
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_026",),
        pre_rows=(PRE_HARD_ROW,),
        post_rows=(PRE_HARD_ROW,),  # still hard in candidate
        candidate_failed_qids=("gs_026",),
    )
    assert result["gs_026"] == DeltaState.STILL_HARD


def test_delta_states_resolves_soft_to_hard() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        DeltaState,
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_018",),
        pre_rows=(SOFT_ROW,),
        post_rows=(HARD_ROW,),
        candidate_failed_qids=("gs_018",),
    )
    assert result["gs_018"] == DeltaState.SOFT_TO_HARD


def test_delta_states_resolves_soft_passing() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        DeltaState,
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_018",),
        pre_rows=(SOFT_ROW,),
        post_rows=(PASS_ROW,),  # gs_001 only — gs_018 absent -> resolved-pass-via-omission
        candidate_failed_qids=(),  # gs_018 is not in candidate failed list
    )
    assert result["gs_018"] == DeltaState.SOFT_PASSING


def test_delta_states_lookup_failed_when_target_missing_from_both_sides() -> None:
    """gs_999 declared as a target but absent from BOTH pre and post rows.

    This is the new-anchor F2 case in its purest form: the target
    legitimately cannot be resolved from the inputs the helper
    received. Returning LOOKUP_FAILED rather than implicit-passing
    or implicit-hard is what makes I13 fire.
    """
    from genie_space_optimizer.optimization.control_plane import (
        DeltaState,
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_999",),
        pre_rows=(PRE_HARD_ROW,),  # only gs_026
        post_rows=(POST_FIX_ROW,),  # only gs_026
        candidate_failed_qids=(),
    )
    assert result["gs_999"] == DeltaState.LOOKUP_FAILED


def test_delta_states_lookup_failed_when_pre_missing_post_passing() -> None:
    """The exact new-anchor F2 reproduction: target was supposed to be
    baseline-hard but pre_rows passed in does not contain it; the
    candidate failed-question list also excludes it. Without a pre-row
    we cannot certify it as FIXED; return LOOKUP_FAILED so the
    downstream gate routes to target_resolution_failed.
    """
    from genie_space_optimizer.optimization.control_plane import (
        DeltaState,
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_026",),
        pre_rows=(),  # baseline rows missing for the target
        post_rows=(PASS_ROW,),  # candidate has unrelated rows
        candidate_failed_qids=(),
    )
    assert result["gs_026"] == DeltaState.LOOKUP_FAILED


def test_delta_states_handles_empty_targets() -> None:
    """Empty target tuple returns an empty dict; trivially total."""
    from genie_space_optimizer.optimization.control_plane import (
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=(),
        pre_rows=(PRE_HARD_ROW,),
        post_rows=(POST_FIX_ROW,),
        candidate_failed_qids=(),
    )
    assert result == {}


def test_delta_states_handles_duplicate_target_qids() -> None:
    """Duplicate target QIDs in input are de-duplicated in output keys."""
    from genie_space_optimizer.optimization.control_plane import (
        compute_target_delta_states,
    )

    result = compute_target_delta_states(
        target_qids=("gs_026", "gs_026"),
        pre_rows=(PRE_HARD_ROW,),
        post_rows=(POST_FIX_ROW,),
        candidate_failed_qids=(),
    )
    assert list(result.keys()) == ["gs_026"]


# ── Task 2: target_delta_states wired into ControlPlaneAcceptance ────


def test_acceptance_decision_carries_target_delta_states() -> None:
    """ControlPlaneAcceptance gains target_delta_states populated
    from compute_target_delta_states. The shape is a sorted tuple
    of (qid, state.value) pairs so the dataclass remains frozen
    and JSON-serialisable.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(PRE_HARD_ROW,),
        post_rows=(POST_FIX_ROW,),
    )
    assert decision.target_delta_states == (("gs_026", "fixed"),)


def test_acceptance_decision_target_delta_states_anchor_f2_repro() -> None:
    """Reproduce new anchor F2 exactly: target gs_026 was supposed
    to be baseline-hard but pre_rows did not contain it (or
    contained the wrong shape); candidate failed list excludes it.

    Pre-T0 behaviour: target_fixed=() AND target_still=() — the
    impossible state.
    Post-T0 behaviour: target_delta_states surfaces the
    LOOKUP_FAILED entry so the downstream gate can route to
    target_resolution_failed.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(),  # the bug: pre_rows missing the target
        post_rows=(_row("gs_018", "no", "ground_truth_correct"),),
    )
    # target_fixed and target_still must remain empty (back-compat),
    # but target_delta_states surfaces the lookup failure typed.
    assert decision.target_fixed_qids == ()
    assert decision.target_still_hard_qids == ()
    assert dict(decision.target_delta_states)["gs_026"] == "lookup_failed"


def test_acceptance_decision_target_delta_states_sorted_for_byte_stability() -> None:
    """Tuple is sorted by qid so MLflow byte-stable replay holds."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_zzz", "gs_aaa"),
        pre_rows=(
            _row("gs_zzz", "no", "ground_truth_correct"),
            _row("gs_aaa", "no", "ground_truth_correct"),
        ),
        post_rows=(
            _row("gs_zzz", "yes", "both_correct"),
            _row("gs_aaa", "yes", "both_correct"),
        ),
    )
    qids = [pair[0] for pair in decision.target_delta_states]
    assert qids == sorted(qids)


# ── Task 4: typed target_resolution_failed rollback reason ───────────


def test_lookup_failed_routes_to_target_resolution_failed_when_strict_on(
    monkeypatch,
) -> None:
    """The new typed rollback reason fires when any target lands in
    LOOKUP_FAILED and the legacy code would otherwise have said
    target_qids_not_improved (or missing_pre_rows). The accepted=
    flag is unchanged — only the reason code is upgraded.
    """
    monkeypatch.setenv("GSO_TARGET_DELTA_STRICT", "1")
    # Isolate the typed target-resolution-failed rollback from the later
    # attribution-drift-with-debt tier (Plan 9 T11.C, default-on), which
    # would re-accept this high-gain candidate as accepted_with_attribution_
    # drift_and_debt before the rollback reason is observable.
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "0")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(),  # F2 reproduction — pre missing the target
        post_rows=(_row("gs_018", "no", "ground_truth_correct"),),
    )
    assert decision.accepted is False
    assert decision.reason_code == "target_resolution_failed"


def test_lookup_failed_falls_back_to_legacy_reason_when_strict_off(
    monkeypatch,
) -> None:
    """Flag-off path retains the existing rollback reason (replay
    byte-stability on pre-T0 fixtures).
    """
    monkeypatch.setenv("GSO_TARGET_DELTA_STRICT", "0")
    # Pin the legacy missing_pre_rows path in isolation from the later
    # attribution-drift-with-debt tier (Plan 9 T11.C, default-on), which
    # would otherwise re-accept this high-gain candidate.
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "0")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(),
        post_rows=(_row("gs_018", "no", "ground_truth_correct"),),
    )
    assert decision.accepted is False
    # Legacy behaviour: missing_pre_rows fires before any other check.
    assert decision.reason_code == "missing_pre_rows"


def test_lookup_failed_does_not_flip_accepted_decisions(monkeypatch) -> None:
    """When the legacy decision is accepted=True, a LOOKUP_FAILED in
    the delta-state map alone must NOT change the outcome — it can
    only refine a rejection's reason code.
    """
    monkeypatch.setenv("GSO_TARGET_DELTA_STRICT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    # Construct a case where the candidate genuinely accepts (target
    # FIXED, no regressions, gain present) but a sibling target also
    # in the declared set lands in LOOKUP_FAILED. The acceptance
    # outcome must be preserved; only postmortem-visible state is
    # the new field.
    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026", "gs_lookup_fail"),
        pre_rows=(PRE_HARD_ROW,),  # only gs_026
        post_rows=(POST_FIX_ROW,),  # only gs_026
    )
    assert decision.accepted is True
    delta_dict = dict(decision.target_delta_states)
    assert delta_dict["gs_026"] == "fixed"
    assert delta_dict["gs_lookup_fail"] == "lookup_failed"
