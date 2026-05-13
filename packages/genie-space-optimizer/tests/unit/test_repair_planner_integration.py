"""Integration tests for plan_repair (Phase 2 Action 2.1)."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card(
    *,
    root_cause: RcaKind = RcaKind.TOP_N_CARDINALITY_COLLAPSE,
    grounding_terms: frozenset[str] = frozenset(),
    qids: tuple[str, ...] = ("gs_026",),
    cluster_id: str = "H001",
) -> RCACard:
    return RCACard(
        card_id=f"card_{cluster_id}",
        cluster_id=cluster_id,
        qids=qids,
        root_cause=root_cause,
        grounding_terms=grounding_terms,
        intended_patch_shape="cardinality_preserving_top_n_guidance",
        allowed_patch_families=frozenset({"cardinality_preserving_top_n_guidance"}),
        forbidden_patch_families=frozenset(),
        rationale="test",
    )


def test_plan_repair_returns_kit_for_classified_cluster() -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    card = _card()
    cluster = {
        "cluster_id": "H001",
        "asi_question_intent": "plural",
        "question_ids": ["gs_026"],
    }
    kit = plan_repair(
        card=card,
        cluster=cluster,
        propagation_root_cause="unknown",
    )
    assert kit is not None
    assert kit["repair_archetype"] == "plural_top_n_collapse"
    assert kit["target_qids"] == ("gs_026",)
    assert kit["priority_step"] == "repair_kit"
    assert "ORDER BY" in kit["expected_causal_effect"]


def test_plan_repair_returns_none_when_no_archetype_matches() -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    card = _card(root_cause=RcaKind.UNKNOWN, grounding_terms=frozenset())
    cluster = {
        "cluster_id": "H_misc",
        "question_ids": ["gs_x"],
    }
    kit = plan_repair(
        card=card,
        cluster=cluster,
        propagation_root_cause="unknown",
    )
    assert kit is None


def test_plan_repair_promotes_priority_under_instruction_insufficient_force() -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    card = _card()
    cluster = {
        "cluster_id": "H001",
        "asi_question_intent": "plural",
        "question_ids": ["gs_026"],
    }
    kit = plan_repair(
        card=card,
        cluster=cluster,
        propagation_root_cause="instruction_insufficient_force",
    )
    assert kit is not None
    assert kit["priority_step"] == "narrow_l6_snippet"
    # Kit MUST require an L6 companion under this propagation outcome.
    assert "narrow_l6_snippet" in kit["required_companions"]


def test_plan_repair_inserts_propagation_verification_hook_under_propagation_lag() -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    card = _card()
    cluster = {
        "cluster_id": "H001",
        "asi_question_intent": "plural",
        "question_ids": ["gs_026"],
    }
    kit = plan_repair(
        card=card,
        cluster=cluster,
        propagation_root_cause="propagation_lag",
    )
    assert kit is not None
    assert kit["pre_eval_propagation_verification"] is True


def test_plan_repair_does_not_set_verification_hook_under_other_propagation_outcomes() -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    card = _card()
    cluster = {
        "cluster_id": "H001",
        "asi_question_intent": "plural",
        "question_ids": ["gs_026"],
    }
    for cause in (
        "unknown",
        "instruction_insufficient_force",
        "instruction_not_scoped_to_qid",
        "eval_cache_stale",
    ):
        kit = plan_repair(
            card=card,
            cluster=cluster,
            propagation_root_cause=cause,
        )
        assert kit is not None
        assert kit["pre_eval_propagation_verification"] is False, (
            f"propagation_root_cause={cause!r} unexpectedly set the hook"
        )


def test_plan_repair_carries_card_id_and_card_grounding_terms() -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    card = _card(
        root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        grounding_terms=frozenset({"time_window", "mtd"}),
        qids=("gs_021",),
        cluster_id="H_021",
    )
    cluster = {
        "cluster_id": "H_021",
        "question_ids": ["gs_021"],
    }
    kit = plan_repair(
        card=card,
        cluster=cluster,
        propagation_root_cause="unknown",
    )
    assert kit is not None
    assert kit["repair_archetype"] == "default_time_window_filter"
    assert kit["card_id"] == "card_H_021"
    assert kit["grounding_terms"] == ("mtd", "time_window")  # sorted tuple
