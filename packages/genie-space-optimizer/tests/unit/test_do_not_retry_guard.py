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


# ── Cycle 13: NO_ACTION admission, flag-gated ───────────────────────


def _no_proposals_entry(root_cause: str, blame, lever_set: list[int]) -> dict:
    return _build_reflection_entry(
        iteration=1,
        ag_id="AG1",
        accepted=False,
        levers=lever_set,
        target_objects=[],
        prev_scores={"result_correctness": 95.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason="no_proposals",
        patches=[],
        root_cause=root_cause,
        blame_set=blame,
        source_cluster_ids=["C001"],
    )


def test_no_proposals_in_forbidden_set_when_flag_on(monkeypatch) -> None:
    """Cycle 13 — when GSO_FORBIDDEN_AG_ADMITS_NO_ACTION is on,
    a no_proposals reflection contributes to the forbidden set."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    buf = [_no_proposals_entry("plural_top_n_collapse", ["zone_vp_name"], [5, 6])]
    forbidden = _compute_forbidden_ag_set(buf)
    assert (
        "plural_top_n_collapse",
        ("zone_vp_name",),
        frozenset({5, 6}),
    ) in forbidden


def test_no_proposals_not_in_forbidden_set_when_flag_off(monkeypatch) -> None:
    """Replay byte-stability: with the flag explicitly off
    (``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0``), the same entry is
    excluded — pre-C13 / pre-14-W-default-flip behaviour. Cycle
    14-W T4 flipped the default ON; this test now uses an
    explicit override."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")
    buf = [_no_proposals_entry("plural_top_n_collapse", ["zone_vp_name"], [5, 6])]
    assert _compute_forbidden_ag_set(buf) == set()


def test_ag_collision_in_forbidden_set_when_flag_on(monkeypatch) -> None:
    """Cycle 13 — ag_collision_with_forbidden_set reflections also
    classify as NO_ACTION and contribute when the flag is on. The
    existing call site (harness.py:17022) passes a non-empty
    levers list, so the entry has a complete identity."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    entry = _build_reflection_entry(
        iteration=2, ag_id="AG1", accepted=False,
        levers=[5, 6], target_objects=[],
        prev_scores={}, new_scores={},
        rollback_reason="ag_collision_with_forbidden_set",
        patches=[],
        root_cause="plural_top_n_collapse",
        blame_set=["zone_vp_name"],
        source_cluster_ids=["C001"],
    )
    forbidden = _compute_forbidden_ag_set([entry])
    assert (
        "plural_top_n_collapse",
        ("zone_vp_name",),
        frozenset({5, 6}),
    ) in forbidden


def test_no_proposals_with_empty_levers_excluded_even_when_flag_on(
    monkeypatch,
) -> None:
    """Defense in depth: the predicate's empty-lever_set rejection
    still applies. Even after C13 ships, a reflection entry with
    no lever set cannot contribute. T5 fixes the call site that
    emits levers=[] today; until then the entry is excluded as
    safety against silent identity loss."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    entry = _build_reflection_entry(
        iteration=1, ag_id="AG1", accepted=False,
        levers=[], target_objects=[],
        prev_scores={}, new_scores={},
        rollback_reason="no_proposals",
        patches=[],
        root_cause="plural_top_n_collapse",
        blame_set=["zone_vp_name"],
        source_cluster_ids=["C001"],
    )
    assert _compute_forbidden_ag_set([entry]) == set()


def test_forbidden_set_admits_accepted_with_debt_reflections() -> None:
    """Cycle 14B-T2: an iteration that accepted with debt is forbidden
    from being retried with the same AG signature (same root_cause +
    blame_set + lever_set). The reflection has accepted=True but the
    derived rollback_class is ACCEPTED_WITH_DEBT, picked up via the
    ``accepted_with_debt:`` rollback-reason prefix.
    """
    debt_accept = _build_reflection_entry(
        iteration=3,
        ag_id="AG7",
        accepted=True,
        levers=[5],
        target_objects=[],
        prev_scores={"result_correctness": 78.3},
        new_scores={"result_correctness": 95.7},
        rollback_reason="accepted_with_debt:gs_018",
        patches=[],
        root_cause="missing_filter",
        blame_set=["fact.region"],
        source_cluster_ids=["C014"],
    )
    forbidden = _compute_forbidden_ag_set([debt_accept])
    assert (
        "missing_filter",
        ("fact.region",),
        frozenset({5}),
    ) in forbidden
    # And the matching collision key blocks a retry.
    retry_key = _ag_collision_key(
        ag={"source_cluster_ids": ["C014"]},
        ag_root_cause="missing_filter",
        ag_blame_set=["fact.region"],
        lever_keys=["5"],
    )
    assert retry_key in forbidden
