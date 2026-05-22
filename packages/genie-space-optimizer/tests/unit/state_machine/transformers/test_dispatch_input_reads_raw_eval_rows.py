"""Dispatch input gate creates QuestionStateInIteration from raw eval rows."""
from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
    build_initial_states_from_eval_rows,
)


def test_one_state_per_hard_eval_row():
    # row_is_hard_failure reads feedback/result_correctness/value (MLflow-flattened
    # form) — see optimization/evaluation.py:3619. Mirror that shape here.
    eval_rows = [
        {"question_id": "gs_009", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "wrong", "sql": "SELECT 1", "expected_shape": "ROW_NUMBER"},
        {"question_id": "gs_024", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "wrong", "sql": "SELECT 2", "expected_shape": "MTD"},
        {"question_id": "gs_003", "feedback/result_correctness/value": "yes",
         "feedback/arbiter/value": "both_correct", "sql": "SELECT 3", "expected_shape": ""},
    ]
    states = build_initial_states_from_eval_rows(eval_rows, iteration=1)
    qids = sorted(s.qid for s in states)
    assert qids == ["gs_009", "gs_024"]


def test_initial_state_carries_baseline_sql_and_expected_shape():
    eval_rows = [{
        "question_id": "gs_009",
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "wrong",
        "sql": "SELECT count_topN",
        "expected_shape": "ROW_NUMBER over COUNT(*)",
    }]
    states = build_initial_states_from_eval_rows(eval_rows, iteration=2)
    assert len(states) == 1
    s = states[0]
    assert s.seen.baseline_sql == "SELECT count_topN"
    assert s.seen.expected_shape == "ROW_NUMBER over COUNT(*)"
    assert s.iteration == 2
    assert s.seen.iteration_first_seen == 2


def test_no_states_when_no_hard_rows():
    eval_rows = [
        {"question_id": "gs_003", "feedback/result_correctness/value": "yes",
         "feedback/arbiter/value": "both_correct", "sql": "SELECT 3", "expected_shape": ""},
    ]
    assert build_initial_states_from_eval_rows(eval_rows, iteration=1) == ()
