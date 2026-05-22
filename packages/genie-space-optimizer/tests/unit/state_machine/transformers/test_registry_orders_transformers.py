"""Production registry maps stages to transformers in the correct order."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.registry import (
    build_production_state_machine,
)


def test_registry_has_diagnosis_at_hard_qid_seen():
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.HARD_QID_SEEN]
    assert [t.name for t in transformers] == ["plan11_stage1_diagnosis"]


def test_registry_has_clustering_at_diagnosed():
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.DIAGNOSED]
    assert [t.name for t in transformers] == ["plan11_stage2_clustering"]


def test_registry_has_routing_then_synthesis_at_clustered():
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.CLUSTERED]
    # Plan 12 routing decoration runs BEFORE Stage 3 synthesis so the
    # synthesizer reads effective_target_lever from the cluster record.
    assert [t.name for t in transformers] == ["plan12_routing_gate", "plan11_stage3_synthesis"]


def test_registry_has_structural_repair_at_proposed():
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.PROPOSED]
    assert "structural_repair_gate" in [t.name for t in transformers]


def test_registry_has_blast_radius_at_normalized():
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.NORMALIZED]
    # Phase 3 Task 1.6: narrow_replacement_gate is appended after
    # blast_radius_batch at NORMALIZED to take over on a collateral-risk drop.
    assert [t.name for t in transformers] == [
        "blast_radius_batch", "narrow_replacement_gate",
    ]


def test_normalized_stage_runs_blast_radius_then_narrow_replacement():
    from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
    from genie_space_optimizer.optimization.state_machine.registry import (
        PHASE3_REGISTRY,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch import (
        blast_radius_batch,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
        narrow_replacement_gate,
    )

    chain = PHASE3_REGISTRY[FunnelStage.NORMALIZED]
    assert chain == (blast_radius_batch, narrow_replacement_gate), (
        f"NORMALIZED chain must be (blast_radius_batch, narrow_replacement_gate); got {chain}"
    )


def test_registry_has_applier_at_applyable():
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.APPLYABLE]
    assert [t.name for t in transformers] == ["applier_gate"]


def test_registry_has_escalation_after_structural_repair_at_proposed():
    """Phase 3 PR 3.2: escalation_ladder is appended after
    structural_repair_gate at PROPOSED."""
    sm = build_production_state_machine()
    transformers = sm.transformers[FunnelStage.PROPOSED]
    names = [t.name for t in transformers]
    assert names == ["structural_repair_gate", "escalation_ladder"]
