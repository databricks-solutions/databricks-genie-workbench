"""Stage 1 transformer terminates with INVARIANT_VIOLATION on LLM abstain (no silent fallback)."""
from dataclasses import dataclass
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    plan11_stage1_diagnosis,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


@dataclass(frozen=True)
class _AbstainResponse:
    succeeded: bool = False
    parsed_output: object = None
    declined: object = "abstain_no_evidence"


def _ctx() -> TransformerContext:
    return TransformerContext(
        iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}),
    )


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1)


def test_abstain_advances_to_terminated_with_typed_terminal():
    state = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm._invoke_stage1_llm",
        return_value=_AbstainResponse(),
    ):
        s2 = plan11_stage1_diagnosis.transform(state, _ctx())
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal is not None
    assert s2.terminal.kind == "OPTIMIZER_NO_CANDIDATES"
    assert "abstain" in s2.terminal.reason


@dataclass(frozen=True)
class _SuccessResponse:
    succeeded: bool = True
    parsed_output: object = None


@dataclass(frozen=True)
class _ParsedDiagnosis:
    rca_kind_label: str = "plural_top_n_collapse"
    evidence_summary: str = "top-N collapsed"
    observed_failure: str = "1 row instead of 3"
    expected_sql_shape: str = "ROW_NUMBER over COUNT"
    confidence: str = "high"
    rca_card_id: str = "rca_xyz"


def test_success_advances_to_diagnosed_with_record():
    state = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm._invoke_stage1_llm",
        return_value=_SuccessResponse(parsed_output=_ParsedDiagnosis()),
    ):
        s2 = plan11_stage1_diagnosis.transform(state, _ctx())
    assert s2.current_stage == FunnelStage.DIAGNOSED
    assert s2.diagnosed is not None
    assert s2.diagnosed.rca_kind_label == "plural_top_n_collapse"
    assert s2.diagnosed.source == "plan11_stage1"
