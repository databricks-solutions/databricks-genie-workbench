"""Plan 12 — _apply_evidence_to_lever_policy routes wrong_aggregation
(and other generating-required evidence) AWAY from Lever 1 to Lever
5 (add_example_sql)."""


def _cluster_with(evidence_kind: str) -> dict:
    return {
        "cluster_id": "H001",
        "asi_failure_type": evidence_kind,
        "root_cause": evidence_kind,
    }


def test_gs_004_wrong_aggregation_routes_away_from_lever_1():
    from genie_space_optimizer.optimization.optimizer import (
        _apply_evidence_to_lever_policy,
    )
    chosen = _apply_evidence_to_lever_policy(
        target_lever=1,
        cluster_or_action_group=_cluster_with("wrong_aggregation"),
    )
    assert chosen != 1
    # 5b (example sql) is the preferred eligible family → integer lever 5.
    assert chosen == 5


def test_missing_filter_routes_to_lever_5():
    from genie_space_optimizer.optimization.optimizer import (
        _apply_evidence_to_lever_policy,
    )
    chosen = _apply_evidence_to_lever_policy(
        target_lever=1,
        cluster_or_action_group=_cluster_with("missing_filter"),
    )
    assert chosen == 5


def test_metadata_only_evidence_keeps_lever_1():
    """ambiguous_column_description IS metadata-only; routing keeps
    target_lever=1 even though 5b is the preferred eligible family
    (the policy's first element is for routing FROM 1; 1 itself is
    still acceptable when the evidence is genuinely metadata-only)."""
    from genie_space_optimizer.optimization.optimizer import (
        _apply_evidence_to_lever_policy,
    )
    chosen = _apply_evidence_to_lever_policy(
        target_lever=1,
        cluster_or_action_group=_cluster_with("ambiguous_column_description"),
    )
    assert chosen == 1


def test_non_lever_1_targets_pass_through_unchanged():
    """Target_lever 5/6/2 don't go through the routing fix — they're
    already generating lanes."""
    from genie_space_optimizer.optimization.optimizer import (
        _apply_evidence_to_lever_policy,
    )
    for target in (5, 6, 2, 4):
        chosen = _apply_evidence_to_lever_policy(
            target_lever=target,
            cluster_or_action_group=_cluster_with("wrong_aggregation"),
        )
        assert chosen == target, (
            f"target_lever={target} must pass through; got {chosen}"
        )


def test_empty_evidence_kind_routes_lever_1_to_lever_5():
    """Empty/missing evidence_kind is the "unknown" bucket; the policy
    refuses Lever 1 and re-routes to 5b (the safest default).
    This is exactly the postmortem-observed safety net for unknown
    failure modes."""
    from genie_space_optimizer.optimization.optimizer import (
        _apply_evidence_to_lever_policy,
    )
    chosen = _apply_evidence_to_lever_policy(
        target_lever=1,
        cluster_or_action_group={"cluster_id": "H001"},
    )
    assert chosen == 5


def test_falls_back_to_root_cause_when_asi_failure_type_missing():
    from genie_space_optimizer.optimization.optimizer import (
        _apply_evidence_to_lever_policy,
    )
    chosen = _apply_evidence_to_lever_policy(
        target_lever=1,
        cluster_or_action_group={
            "cluster_id": "H001",
            "root_cause": "wrong_aggregation",
        },
    )
    assert chosen == 5
