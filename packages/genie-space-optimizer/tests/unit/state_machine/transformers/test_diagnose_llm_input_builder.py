"""Stage 1 diagnosis input_builder produces a complete LLM payload from QuestionStateInIteration."""
from genie_space_optimizer.optimization.state_machine.records import HardQidSeenRecord
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    build_stage1_llm_input,
)


def _state():
    return build_initial_state(
        qid="gs_009",
        iteration=1,
        seen=HardQidSeenRecord(
            eval_row_id="row_gs_009",
            predicate="row_is_hard_failure",
            score=0.0,
            baseline_sql="SELECT origin_city, COUNT(*) c FROM flights GROUP BY 1 ORDER BY c DESC",
            expected_shape="ROW_NUMBER OVER (ORDER BY COUNT(*) DESC) AS rank, WHERE rank <= 3",
            iteration_first_seen=1,
        ),
    )


def test_llm_input_contains_qid_and_baseline_sql():
    payload = build_stage1_llm_input(_state())
    assert payload.qid == "gs_009"
    assert "SELECT origin_city" in payload.baseline_sql
    assert "ROW_NUMBER" in payload.expected_shape


def test_llm_input_carries_eval_row_id_for_correlation():
    payload = build_stage1_llm_input(_state())
    assert payload.eval_row_id == "row_gs_009"


def test_llm_input_iteration_first_seen_passed_through():
    payload = build_stage1_llm_input(_state())
    assert payload.iteration_first_seen == 1
