"""Optimizer iteration loop calls build_initial_states_from_eval_rows once per iteration."""
from genie_space_optimizer.optimization.optimizer import (
    _build_state_machine_initial_states,
)


def test_dispatch_input_returns_states_from_eval_rows():
    eval_rows = [
        {"question_id": "gs_009", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "wrong", "sql": "S", "expected_shape": "x"},
    ]
    states = _build_state_machine_initial_states(eval_rows=eval_rows, iteration=1)
    assert len(states) == 1
    assert states[0].qid == "gs_009"


def test_dispatch_input_records_count_for_sm8_check():
    """The optimizer iteration loop records the eval-hard count + state qids
    so SM8 (dispatch input honesty) can be checked at end-of-iteration."""
    eval_rows = [
        {"question_id": "gs_009", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "wrong"},
        {"question_id": "gs_024", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "wrong"},
    ]
    states = _build_state_machine_initial_states(eval_rows=eval_rows, iteration=1)
    assert sorted(s.qid for s in states) == ["gs_009", "gs_024"]
