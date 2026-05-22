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
    assert [t.name for t in transformers] == ["blast_radius_batch"]


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
