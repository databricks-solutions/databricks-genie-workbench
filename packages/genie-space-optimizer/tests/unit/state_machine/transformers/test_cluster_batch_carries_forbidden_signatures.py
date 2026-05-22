"""Stage 2 batch input includes TransformerContext.forbidden_signatures verbatim."""
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


@dataclass
class _R:
    succeeded: bool = False
    declined: str = "stub"
    parsed_output: object = None


def test_forbidden_signatures_appear_in_llm_input():
    s = build_initial_state(
        qid="gs_009", iteration=2,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
        diagnosed=DiagnosisRecord("plan11_stage1", "k", "s", "f", "e", "high", "r"),
    )
    ctx = TransformerContext(
        iteration=2, run_id="r",
        validation_context=ValidationContext(2, "r", {}),
        forbidden_signatures=("h001|count_topN|gs_009",),
    )

    seen_input = {}

    def _capture(batch_input, ctx_, states=()):
        seen_input["payload"] = batch_input
        return _R()

    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.cluster_batch._invoke_stage2_llm",
        side_effect=_capture,
    ):
        plan11_stage2_clustering.transform_batch((s,), ctx)

    assert "h001|count_topN|gs_009" in seen_input["payload"].forbidden_signatures
