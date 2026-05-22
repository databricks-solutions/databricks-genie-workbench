"""Stage 1 output recorder writes DiagnosisRecord onto state at DIAGNOSED."""
from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    build_diagnosis_record_from_llm_result,
)


@dataclass(frozen=True)
class _FakeLlmResult:
    rca_kind_label: str
    evidence_summary: str
    observed_failure: str
    expected_sql_shape: str
    confidence: str
    rca_card_id: str


def test_output_recorder_returns_typed_diagnosis_record():
    state = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    result = _FakeLlmResult(
        rca_kind_label="plural_top_n_collapse",
        evidence_summary="top-N collapsed",
        observed_failure="1 row instead of 3",
        expected_sql_shape="ROW_NUMBER over COUNT(*)",
        confidence="high",
        rca_card_id="rca_xyz",
    )
    rec = build_diagnosis_record_from_llm_result(state, result)
    assert rec.source == "plan11_stage1"
    assert rec.rca_kind_label == "plural_top_n_collapse"
    assert rec.confidence == "high"
    assert rec.rca_card_id == "rca_xyz"
