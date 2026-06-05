from __future__ import annotations

from _harness_loop_source import lever_loop_source


def test_harness_marks_no_applied_bundle_as_dead_on_arrival() -> None:
    """Asserts the dead-on-arrival markers appear in the skip-eval
    branch (after ``_apply_skip = ...``) without using a fixed
    character window — the harness has grown such that these markers
    sit ~5000 chars after the skip entry, so any hard window risks
    drifting again.
    """
    source = lever_loop_source()
    skip_idx = source.index("_apply_skip = _should_skip_eval_for_patch_bundle(")

    # These must appear AFTER the skip-eval branch entry.
    for needle in (
        "deterministic_no_applied_patches",
        "all_selected_patches_dropped_by_applier",
        "pending_action_groups = []",
        "pending_strategy = None",
    ):
        n_idx = source.find(needle, skip_idx)
        assert n_idx > skip_idx, (
            f"{needle!r} must appear AFTER the skip-eval branch entry"
        )

    # These can appear anywhere in the function (declared outside the branch).
    assert "_dead_on_arrival_patch_signatures" in source
    assert "_dead_on_arrival_ag_ids" in source


def test_harness_blocks_retry_of_same_dead_patch_signature() -> None:
    source = lever_loop_source()
    assert "_selected_patch_signature = tuple(sorted(" in source
    assert "_selected_patch_signature in _dead_on_arrival_patch_signatures" in source
    assert "Skipping dead-on-arrival AG retry" in source


# ── Defect Plan 2 (2026-05-12) — wire rca_next_action into no_applied reflection ─


def test_no_applied_patches_branch_carries_rca_next_action_payload() -> None:
    """Defect Plan 2: the reflection appended in the no_applied_patches
    branch must carry an ``extra["rca_next_action"]``
    payload built via ``_next_grounding_action_payload`` so the
    strategist's next attempt sees the closed-loop hint.

    Pre-Defect-2 this branch passed no ``extra`` kwarg, so the
    strategist only saw the bare ``rollback_reason="no_applied_patches"``
    with no actionable next-step field.

    This test introspects the harness source rather than running a
    live optimizer (which would require LLM endpoints). It pins the
    structural pattern: the no_applied_patches branch must call
    ``_next_grounding_action_payload(rollback_reason=...)`` and pass
    the result through ``extra={"rca_next_action": ...}``.
    """
    src = lever_loop_source()

    # Locate the no_applied_patches branch.
    skip_idx = src.find('if _apply_skip.reason_code == "no_applied_patches":')
    assert skip_idx > 0, "no_applied_patches branch must exist"

    # Find the reflection_buffer.append in this branch. It is the
    # nearest occurrence AFTER skip_idx.
    refl_idx = src.find("reflection_buffer.append(_build_reflection_entry(", skip_idx)
    assert refl_idx > skip_idx, (
        "no_applied_patches branch must append a reflection entry"
    )

    # The append call ends at the next ")\n" that closes the outer call.
    # Take a generous window for the call body.
    window = src[refl_idx:refl_idx + 4000]

    assert '"rca_next_action"' in window, (
        "Defect Plan 2: the no_applied_patches reflection must include "
        "extra={'rca_next_action': _next_grounding_action_payload(...)}"
    )
    assert '_next_grounding_action_payload(' in window or \
           '_next_action_payload' in window, (
        "Defect Plan 2: the no_applied_patches reflection must call "
        "_next_grounding_action_payload to seed the strategist's next "
        "attempt"
    )
    assert 'rollback_reason=_apply_skip.reason_code' in window, (
        "the rca_next_action call must pass the no_applied_patches "
        "rollback reason"
    )


# ── Defect Plan 2 — admission predicate ────────────────


def test_no_applied_patches_reflection_admitted_to_forbidden_set() -> None:
    """Defect Plan 2 closure: a synthetic reflection that mirrors what
    the no_applied_patches branch emits must be admitted by
    ``_reflection_admitted_to_forbidden_set`` when ``admit_no_action=True``.

    Pre-Defect-2 the rollback_class field carried ``"other"`` (because
    ``classify_rollback_reason("no_applied_patches")`` returned OTHER),
    so this assertion failed: the predicate rejected the entry at the
    ``rollback_class not in admitted_classes`` check.

    After Task 1, the same entry built via ``_build_reflection_entry``
    carries ``rollback_class="no_action"`` and the predicate admits.
    """
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
        _reflection_admitted_to_forbidden_set,
    )

    entry = _build_reflection_entry(
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        accepted=False,
        levers=[1, 3],
        target_objects=[],
        prev_scores={"gs_009": 0.0},
        new_scores={"gs_009": 0.0},
        rollback_reason="no_applied_patches",
        patches=[],
        affected_question_ids=["gs_009"],
        prev_failure_qids={"gs_009"},
        new_failure_qids={"gs_009"},
        root_cause="missing_instruction_for_H001",
        blame_set=("gs_009",),
        source_cluster_ids=["c001"],
        source_cluster_signatures=["sig:c001:H001"],
    )

    # Predicate-only check (gated by admit_no_action=True at the call
    # site; GSO_FORBIDDEN_AG_ADMITS_NO_ACTION is default-ON).
    assert _reflection_admitted_to_forbidden_set(
        entry, admit_no_action=True,
    ) is True

    # Negative control: with admit_no_action=False (i.e. the legacy
    # pre-Cycle-14-W behaviour), the NO_ACTION class is excluded.
    assert _reflection_admitted_to_forbidden_set(
        entry, admit_no_action=False,
    ) is False


def test_no_applied_patches_reflection_classified_as_no_action() -> None:
    """Defect Plan 2 closure: confirm the rollback_class field on the
    entry is the right enum value, not just any non-OTHER value."""
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    entry = _build_reflection_entry(
        iteration=1,
        ag_id="AG_X",
        accepted=False,
        levers=[1],
        target_objects=[],
        prev_scores={},
        new_scores={},
        rollback_reason="no_applied_patches",
        patches=[],
        root_cause="root",
        blame_set=("q1",),
        source_cluster_ids=["c1"],
        source_cluster_signatures=["s1"],
    )
    assert entry.get("rollback_class") == RollbackClass.NO_ACTION.value


def test_no_applied_patches_reflection_excluded_when_identity_empty() -> None:
    """Defense in depth: even though the class is NO_ACTION, the
    predicate must still reject entries missing root_cause or lever_set
    (the identity-completeness check in ``_reflection_admitted_to_forbidden_set``)."""
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
        _reflection_admitted_to_forbidden_set,
    )

    # No root_cause.
    entry_no_rc = _build_reflection_entry(
        iteration=1,
        ag_id="AG_X",
        accepted=False,
        levers=[1],
        target_objects=[],
        prev_scores={},
        new_scores={},
        rollback_reason="no_applied_patches",
        patches=[],
        root_cause="",
        blame_set=("q1",),
        source_cluster_ids=["c1"],
        source_cluster_signatures=["s1"],
    )
    assert _reflection_admitted_to_forbidden_set(
        entry_no_rc, admit_no_action=True,
    ) is False

    # No lever_set.
    entry_no_lever = _build_reflection_entry(
        iteration=1,
        ag_id="AG_X",
        accepted=False,
        levers=[],
        target_objects=[],
        prev_scores={},
        new_scores={},
        rollback_reason="no_applied_patches",
        patches=[],
        root_cause="root",
        blame_set=("q1",),
        source_cluster_ids=["c1"],
        source_cluster_signatures=["s1"],
    )
    assert _reflection_admitted_to_forbidden_set(
        entry_no_lever, admit_no_action=True,
    ) is False


# ── Defect Plan 2 — forbidden-set end-to-end ───────────


def test_no_applied_patches_reflection_enters_compute_forbidden_ag_set() -> None:
    """Defect Plan 2 closure end-to-end: drop a synthetic reflection
    buffer containing exactly one no_applied_patches entry into
    ``_compute_forbidden_ag_set`` and assert the AG identity tuple is
    in the returned set under the default-ON
    ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION`` flag.

    Pre-Defect-2 the returned set was empty for this buffer because
    the classifier produced ``rollback_class=other``.
    """
    import os

    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
        _compute_forbidden_ag_set,
    )

    # Default-ON; pop explicitly so the test is order-independent.
    os.environ.pop("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", None)

    entry = _build_reflection_entry(
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        accepted=False,
        levers=[1, 3],
        target_objects=[],
        prev_scores={"gs_009": 0.0},
        new_scores={"gs_009": 0.0},
        rollback_reason="no_applied_patches",
        patches=[],
        affected_question_ids=["gs_009"],
        root_cause="missing_instruction_for_H001",
        blame_set=("gs_009",),
        source_cluster_ids=["c001"],
        source_cluster_signatures=["sig:c001:H001"],
    )

    forbidden = _compute_forbidden_ag_set([entry])
    # Tuple shape: (root_cause, normalised_blame_set, frozenset(lever_set))
    assert ("missing_instruction_for_H001", ("gs_009",),
            frozenset({1, 3})) in forbidden, (
        f"forbidden set must admit no_applied_patches reflection; "
        f"got: {forbidden}"
    )


def test_no_applied_patches_excluded_when_admit_no_action_off(monkeypatch) -> None:
    """Defense: the existing legacy escape hatch
    ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0`` continues to exclude the
    no_applied_patches entry. Replay-byte-stable for pre-14-W
    fixtures."""
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
        _compute_forbidden_ag_set,
    )

    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")
    entry = _build_reflection_entry(
        iteration=1,
        ag_id="AG_X",
        accepted=False,
        levers=[1],
        target_objects=[],
        prev_scores={},
        new_scores={},
        rollback_reason="no_applied_patches",
        patches=[],
        root_cause="root",
        blame_set=("q1",),
        source_cluster_ids=["c1"],
        source_cluster_signatures=["s1"],
    )
    assert _compute_forbidden_ag_set([entry]) == set()
