"""Regression tests for the Phase D2 strategist collision guard helpers.

``_compute_forbidden_ag_set`` pulls DO-NOT-RETRY tuples out of the
reflection buffer (CONTENT_REGRESSION entries only). ``_ag_collision_key``
builds the matching tuple for a fresh AG. The harness uses these two
helpers together to detect when the strategist re-proposed something it
had already rolled back and skip the iteration without burning budget.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _ag_collision_key,
    _build_reflection_entry,
    _compute_forbidden_ag_set,
)


def _rejected_content(
    root_cause: str, blame, lever_set: list[int]
) -> dict:
    return _build_reflection_entry(
        iteration=1,
        ag_id="AG",
        accepted=False,
        levers=lever_set,
        target_objects=[],
        prev_scores={"result_correctness": 95.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason="slice_gate: result_correctness",
        patches=[],
        root_cause=root_cause,
        blame_set=blame,
        source_cluster_ids=["C001"],
    )


def test_forbidden_set_includes_content_regression_only() -> None:
    buf = [
        _rejected_content("missing_filter", ["fact.is_active"], [5]),
        _build_reflection_entry(
            iteration=2, ag_id="AG", accepted=False,
            levers=[5], target_objects=[],
            prev_scores={}, new_scores={},
            rollback_reason="patch_deploy_failed: 500 Internal",
            patches=[],
            root_cause="missing_filter",
            blame_set=["fact.is_active"],
            source_cluster_ids=["C001"],
        ),  # INFRA_FAILURE — excluded from forbidden set
    ]
    forbidden = _compute_forbidden_ag_set(buf)
    assert (
        "missing_filter",
        ("fact.is_active",),
        frozenset({5}),
    ) in forbidden
    # Only one entry — infra rollback didn't contribute.
    assert len(forbidden) == 1


def test_collision_key_matches_reflection_entry_normalisation() -> None:
    buf = [
        _rejected_content("missing_filter", ["zone_name", "market_description"], [5, 6]),
    ]
    forbidden = _compute_forbidden_ag_set(buf)
    key = _ag_collision_key(
        ag={"source_cluster_ids": ["C001"]},
        ag_root_cause="missing_filter",
        ag_blame_set=["market_description", "zone_name"],
        lever_keys=["5", "6"],
    )
    assert key in forbidden


def test_collision_key_returns_none_without_identity() -> None:
    # No root cause.
    assert (
        _ag_collision_key(
            ag={}, ag_root_cause="", ag_blame_set=None, lever_keys=["5"],
        )
        is None
    )
    # No lever set.
    assert (
        _ag_collision_key(
            ag={}, ag_root_cause="missing_filter", ag_blame_set=None, lever_keys=[],
        )
        is None
    )


def test_forbidden_set_ignores_accepted_entries() -> None:
    accepted = _build_reflection_entry(
        iteration=1, ag_id="AG", accepted=True,
        levers=[5], target_objects=[],
        prev_scores={"result_correctness": 90.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason=None, patches=[],
        root_cause="missing_filter", blame_set=[],
        source_cluster_ids=["C001"],
    )
    assert _compute_forbidden_ag_set([accepted]) == set()


def test_forbidden_set_ignores_escalation_entries() -> None:
    esc = _build_reflection_entry(
        iteration=1, ag_id="AG", accepted=False,
        levers=[5], target_objects=[],
        prev_scores={}, new_scores={},
        rollback_reason="escalation:flag_for_review", patches=[],
        root_cause="missing_filter", blame_set=[],
        source_cluster_ids=["C001"],
        escalation_handled=True,
    )
    assert _compute_forbidden_ag_set([esc]) == set()


def test_different_lever_set_is_not_forbidden() -> None:
    """Q004 scenario — Lever 5 was rolled back for missing_filter; Lever 6
    on the same cluster must remain allowed."""
    buf = [_rejected_content("missing_filter", ["fact.is_active"], [5])]
    forbidden = _compute_forbidden_ag_set(buf)
    retry_key = _ag_collision_key(
        ag={"source_cluster_ids": ["C001"]},
        ag_root_cause="missing_filter",
        ag_blame_set=["fact.is_active"],
        lever_keys=["6"],
    )
    assert retry_key not in forbidden
