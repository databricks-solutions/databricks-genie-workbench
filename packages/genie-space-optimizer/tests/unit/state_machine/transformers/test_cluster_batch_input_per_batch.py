"""Stage 2 input builder packs all DIAGNOSED states into one LLM payload."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (
    build_stage2_batch_input,
)


def _diagnosed(qid: str, kind: str):
    s = build_initial_state(
        qid=qid, iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    return s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
        diagnosed=DiagnosisRecord("plan11_stage1", kind, "s", "f", "e", "high", "rca_" + qid),
    )


def test_input_contains_one_member_per_diagnosed_state():
    batch = (_diagnosed("gs_009", "plural_top_n_collapse"),
             _diagnosed("gs_026", "plural_top_n_collapse"))
    payload = build_stage2_batch_input(batch, forbidden_signatures=())
    assert len(payload.members) == 2
    assert {m.qid for m in payload.members} == {"gs_009", "gs_026"}
    assert {m.rca_kind_label for m in payload.members} == {"plural_top_n_collapse"}


def test_input_passes_forbidden_signatures_through():
    batch = (_diagnosed("gs_009", "plural_top_n_collapse"),)
    payload = build_stage2_batch_input(batch, forbidden_signatures=("h001|count_topN",))
    assert "h001|count_topN" in payload.forbidden_signatures
