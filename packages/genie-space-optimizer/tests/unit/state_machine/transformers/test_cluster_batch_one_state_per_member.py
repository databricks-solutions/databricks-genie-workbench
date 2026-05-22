"""Stage 2 BatchTransformer.transform_batch returns one CLUSTERED state per input member."""
from dataclasses import dataclass
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (
    plan11_stage2_clustering,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


@dataclass(frozen=True)
class _ClusterMember:
    qid: str
    cluster_id: str
    ag_id: str
    co_member_qids: tuple
    routing_evidence_kind: str


@dataclass(frozen=True)
class _ClusterResponse:
    succeeded: bool = True
    parsed_output: object = None


def _diagnosed(qid: str):
    s = build_initial_state(
        qid=qid, iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    return s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
        diagnosed=DiagnosisRecord("plan11_stage1", "plural_top_n_collapse", "s", "f", "e", "high", "rca"),
    )


def _ctx() -> TransformerContext:
    return TransformerContext(
        iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}),
    )


def test_each_member_lands_at_clustered_with_membership_record():
    batch = (_diagnosed("gs_009"), _diagnosed("gs_026"))
    parsed = type("P", (), {"members": (
        _ClusterMember("gs_009", "H001", "AG_H001", ("gs_009", "gs_026"), "plural_top_n_collapse"),
        _ClusterMember("gs_026", "H001", "AG_H001", ("gs_009", "gs_026"), "plural_top_n_collapse"),
    )})()
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.cluster_batch._invoke_stage2_llm",
        return_value=_ClusterResponse(succeeded=True, parsed_output=parsed),
    ):
        out = plan11_stage2_clustering.transform_batch(batch, _ctx())

    assert len(out) == 2
    for s in out:
        assert s.current_stage == FunnelStage.CLUSTERED
        assert s.clustered is not None
        assert s.clustered.cluster_id == "H001"
        assert s.clustered.ag_id == "AG_H001"
        assert s.clustered.routing_evidence_kind == "plural_top_n_collapse"
        # effective_target_lever is the pre-routing sentinel; Plan 12 routing gate writes it.
        assert s.clustered.effective_target_lever == 0


def test_abstain_terminates_all_members():
    batch = (_diagnosed("gs_009"),)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.cluster_batch._invoke_stage2_llm",
        return_value=_ClusterResponse(succeeded=False, parsed_output=None),
    ):
        out = plan11_stage2_clustering.transform_batch(batch, _ctx())
    assert out[0].current_stage == FunnelStage.TERMINATED
    assert out[0].terminal.kind == "OPTIMIZER_NO_CANDIDATES"
