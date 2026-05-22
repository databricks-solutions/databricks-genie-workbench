"""Step §D of the production-seam wire-in plan.

``_invoke_stage3_llm`` now adapts
``stages.synthesize.run_plan11_synthesis_for_single_cluster`` into the
v3 RepairProposal-shaped duck the transformer consumes. Also writes
the typed RepairProposal into ``ctx.proposal_store`` so downstream
gates can look it up by ``intent_id``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    synthesize_llm as synth_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_clustered(qid: str = "gs_009"):
    s = build_initial_state(
        qid=qid, iteration=1,
        seen=HardQidSeenRecord(
            eval_row_id="r1", predicate="row_is_hard_failure",
            score=0.0, baseline_sql="SELECT 1",
            expected_shape="aggregate", iteration_first_seen=1,
        ),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            source="plan11_stage1",
            rca_kind_label="missing_filter",
            evidence_summary="judge: filter omitted",
            observed_failure="returned all rows",
            expected_sql_shape="aggregate with filter",
            confidence="high",
            rca_card_id="rca_card_gs_009_missing_filter",
        ),
    )
    return s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "llm",
        ),
        clustered=ClusterMembershipRecord(
            cluster_id="H001", ag_id="AG_H001",
            co_member_qids=("gs_009",),
            effective_target_lever=0,
            routing_evidence_kind="missing_filter_repair",
        ),
    )


def _build_cluster_synth_result(proposal_dict, skipped_reason=None):
    """Mirrors ``ClusterSynthesisResult`` enough for the adapter to read."""
    from dataclasses import dataclass

    @dataclass
    class _R:
        proposal: object = None
        attempted_archetypes: tuple = ()
        skipped_reason: str | None = None

    return _R(
        proposal=proposal_dict,
        attempted_archetypes=(),
        skipped_reason=skipped_reason,
    )


def _make_proposal_dict():
    """A minimal valid RepairProposal.to_json() dict."""
    return {
        "intent_id": "intent_a1",
        "intent_name": "add filter",
        "intent_description": "filter on status",
        "repair_shape": "filter_compose",
        "patch_type": "add_default_filter",
        "rationale": "missing where",
        "confidence": "high",
        "patch_body": {
            "table": "orders",
            "sql_expression": "status = 'active'",
        },
        "blame_set": ["catalog.schema.orders:status"],
        "target_objects": [
            {
                "asset_kind": "table",
                "identifier": "catalog.schema.orders",
                "columns": [],
            },
        ],
        "required_constructs": [],
        "repair_hypothesis": "missing_filter_repair",
        "target_qids": ["gs_009"],
    }


def test_happy_path_advances_to_proposed_and_stores_proposal(monkeypatch):
    """Stage 3 returns a valid RepairProposal dict → state advances to
    PROPOSED, ctx.proposal_store carries the typed proposal."""

    def fake_synth(*args, **kwargs):
        return _build_cluster_synth_result(_make_proposal_dict())

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.synthesize"
        ".run_plan11_synthesis_for_single_cluster",
        fake_synth,
    )

    s = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        w=None,
    )
    out = synth_module.plan11_stage3_synthesis.transform(s, ctx)

    assert out.current_stage == FunnelStage.PROPOSED
    assert len(out.proposals) == 1
    assert out.proposals[0].intent_id == "intent_a1"
    # Proposal store carries the typed proposal keyed by intent_id.
    stored = ctx.proposal_store.lookup("intent_a1")
    assert stored is not None
    assert stored.intent_id == "intent_a1"


def test_decline_terminates_state(monkeypatch):
    """Stage 3 returns proposal=None → state terminates with NO_CANDIDATES."""

    def fake_synth(*args, **kwargs):
        return _build_cluster_synth_result(
            proposal_dict=None,
            skipped_reason="exception:plan11_stage3_declined",
        )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.synthesize"
        ".run_plan11_synthesis_for_single_cluster",
        fake_synth,
    )

    s = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    out = synth_module.plan11_stage3_synthesis.transform(s, ctx)
    assert out.current_stage == FunnelStage.TERMINATED
    assert out.terminal.kind == "OPTIMIZER_NO_CANDIDATES"


def test_empty_patch_body_fails_contract_validation(monkeypatch):
    """Stage 3 returns a proposal with an empty patch_body → the v3
    contract validator raises StageThreeContractError → state
    terminates with OPTIMIZER_INVARIANT_VIOLATION."""

    proposal_dict = _make_proposal_dict()
    proposal_dict["patch_body"] = {}  # forces validator to fail on original_patch_body

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.synthesize"
        ".run_plan11_synthesis_for_single_cluster",
        lambda *a, **kw: _build_cluster_synth_result(proposal_dict),
    )

    s = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    out = synth_module.plan11_stage3_synthesis.transform(s, ctx)
    assert out.current_stage == FunnelStage.TERMINATED
    assert out.terminal.kind == "OPTIMIZER_INVARIANT_VIOLATION"
