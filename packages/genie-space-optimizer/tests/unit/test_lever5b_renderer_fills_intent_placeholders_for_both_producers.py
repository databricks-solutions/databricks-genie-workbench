"""Plan 5 Task 10 — L5b renderer fills intent placeholders uniformly.

Two producers, one template:
  * Deterministic path: pick_archetype → intent_from_archetype →
    renderer fills {{ repair_intent_name }} = archetype.name.
  * LLM path: synthesize_repair_intent_for_cluster → renderer fills
    {{ repair_intent_name }} = LLM-emitted intent_name.

Either way the rendered prompt body uses the new placeholder slots.
Rollback semantics: flip the flag, deterministic path produces the
intent, template renders identically (modulo the swapped placeholder
labels — semantically a no-op).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _make_repair_intent(intent_name: str, intent_id: str = "intent_H001_AG3_001") -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id,
        intent_name=intent_name,
        intent_description=f"description for {intent_name}",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale=f"rationale for {intent_name}",
        confidence="high",
        source="deterministic_archetype_adapter",
        cluster_id="H001",
        target_qids=("gs_001",),
        blame_set=("sales.fact_sales.revenue",),
        rca_card_id="",
        ag_id="AG3",
    )


def test_render_lever_5b_intent_prompt_fills_intent_placeholders() -> None:
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_intent_prompt,
    )

    intent = _make_repair_intent("top_n_revenue_by_region")
    rendered = _render_lever_5b_intent_prompt(
        intent=intent,
        afs_block="cluster H001 ASF block here",
        identifier_allowlist="- sales.fact_sales.revenue\n- sales.fact_sales.region",
    )
    assert "top_n_revenue_by_region" in rendered
    assert "description for top_n_revenue_by_region" in rendered
    assert "top_n_by_metric" in rendered
    assert "rationale for top_n_revenue_by_region" in rendered
    assert "cluster H001 ASF block here" in rendered
    assert "sales.fact_sales.revenue" in rendered
    assert "{{ archetype_name }}" not in rendered
    assert "{{ repair_intent_name }}" not in rendered


def test_render_lever_5b_intent_prompt_works_for_archetype_derived_intent() -> None:
    """Deterministic-path smoke test: intent built from intent_from_archetype
    fills the same template the LLM path uses."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_intent_prompt,
    )

    cluster = FailureCluster(
        cluster_id="H001", target_qids=("gs_001",),
        root_cause="missing_top_n", asi_failure_type="missing_top_n",
        failure_keys=("missing_top_n",),
        blame_set_raw=("sales.fact_sales.revenue",),
        blame_set_normalized=("sales.fact_sales.revenue",),
        rca_card_id="", rca_card_summary="", is_grounded=False,
    )

    from genie_space_optimizer.optimization.archetypes import (
        ARCHETYPES,
    )
    from genie_space_optimizer.optimization.repair_intent import (
        intent_from_archetype,
    )
    top_n_archetype = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
    det_intent = intent_from_archetype(
        archetype=top_n_archetype, cluster=cluster, ag_id="AG3", seq=1,
    )

    rendered = _render_lever_5b_intent_prompt(
        intent=det_intent,
        afs_block="cluster H001 AFS",
        identifier_allowlist="- sales.fact_sales.revenue",
    )
    assert "top_n_by_metric" in rendered


def test_render_lever_5b_intent_prompt_works_for_llm_proposal_intent() -> None:
    """LLM-path smoke test: intent built from RepairProposal.to_repair_intent
    fills the same template."""
    from genie_space_optimizer.optimization.synthesis import (
        _render_lever_5b_intent_prompt,
    )

    proposal = RepairProposal(
        intent_id="intent_H001_AG3_001",
        intent_name="llm_invented_top_n_revenue",
        intent_description="LLM-authored intent description",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="LLM-authored rationale",
        confidence="high",
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
        blame_set=("sales.fact_sales.revenue",),
    )
    cluster = FailureCluster(
        cluster_id="H001", target_qids=("gs_001",),
        root_cause="x", asi_failure_type="x", failure_keys=(),
        blame_set_raw=("sales.fact_sales.revenue",),
        blame_set_normalized=("sales.fact_sales.revenue",),
        rca_card_id="", rca_card_summary="", is_grounded=False,
    )
    llm_intent = proposal.to_repair_intent(cluster=cluster, ag_id="AG3")
    rendered = _render_lever_5b_intent_prompt(
        intent=llm_intent,
        afs_block="cluster H001 AFS",
        identifier_allowlist="- sales.fact_sales.revenue",
    )
    assert "llm_invented_top_n_revenue" in rendered
    assert "LLM-authored intent description" in rendered
    assert "LLM-authored rationale" in rendered
