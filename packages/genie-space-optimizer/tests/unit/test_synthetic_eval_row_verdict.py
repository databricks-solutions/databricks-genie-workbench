"""Synthetic eval rows derived from example SQLs must carry verdict
``synthetic_example`` (not ``ground_truth_correct``) so structural
mining can require explicit corroboration."""
from genie_space_optimizer.optimization.harness import (
    _example_sqls_to_positive_eval_rows,
)
from genie_space_optimizer.optimization import feature_mining


def test_synthetic_rows_use_synthetic_example_verdict():
    examples = [
        {"question": "q1", "expected_sql": "SELECT 1"},
        {"example_question": "q2", "example_sql": "SELECT 2"},
    ]
    rows = _example_sqls_to_positive_eval_rows(examples)
    assert len(rows) == 2
    for row in rows:
        assert row["arbiter/value"] == "synthetic_example"
        assert row["feedback/arbiter/value"] == "synthetic_example"
        assert row["question_id"].startswith("example_sql_")


def test_synthetic_example_is_not_in_structural_safe_verdicts():
    assert "synthetic_example" not in feature_mining._STRUCTURAL_SAFE_VERDICTS
    assert "ground_truth_correct" in feature_mining._STRUCTURAL_SAFE_VERDICTS
    assert "both_correct" in feature_mining._STRUCTURAL_SAFE_VERDICTS


def test_synthetic_example_recognized_as_synthetic_origin():
    row = {"arbiter/value": "synthetic_example"}
    assert feature_mining._row_arbiter_verdict(row) == "synthetic_example"
