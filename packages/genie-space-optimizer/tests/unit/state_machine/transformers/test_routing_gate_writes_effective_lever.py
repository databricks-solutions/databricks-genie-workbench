"""Routing gate is the sole source of ClusterMembershipRecord.effective_target_lever."""
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
from genie_space_optimizer.optimization.state_machine.transformers.routing_gate import (
    routing_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_clustered_pending_routing(qid: str, evidence_kind: str):
    s = build_initial_state(
        qid=qid, iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
        diagnosed=DiagnosisRecord("plan11_stage1", evidence_kind, "x", "x", "x", "high", "r"),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
        clustered=ClusterMembershipRecord(
            cluster_id="H001",
            ag_id="AG_1",
            co_member_qids=(qid,),
            effective_target_lever=0,         # pre-routing sentinel
            routing_evidence_kind="",         # pre-routing sentinel
        ),
    )
    return s


def _ctx() -> TransformerContext:
    return TransformerContext(
        iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}),
    )


def test_plural_top_n_routes_to_lever_6():
    s = _state_at_clustered_pending_routing("gs_009", "plural_top_n_collapse")
    s2 = routing_gate.transform(s, _ctx())
    assert s2.current_stage == FunnelStage.CLUSTERED  # stays at clustered (decoration)
    assert s2.clustered.effective_target_lever == 6
    assert s2.clustered.routing_evidence_kind == "plural_top_n_collapse"


def test_missing_filter_routes_to_lever_6():
    s = _state_at_clustered_pending_routing("gs_024", "missing_filter")
    s2 = routing_gate.transform(s, _ctx())
    assert s2.clustered.effective_target_lever == 6


def test_column_disambiguation_routes_away_from_lever_1():
    s = _state_at_clustered_pending_routing("gs_004", "column_disambiguation")
    s2 = routing_gate.transform(s, _ctx())
    # Plan 12 policy: column_disambiguation goes to L5b, NOT L1.
    assert s2.clustered.effective_target_lever in (5, 6)  # never 1
