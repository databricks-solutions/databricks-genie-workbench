"""Plan 12 — _build_plan11_failing_qids_from_raw must succeed even
when rca_evidence_typed is empty (the bug both postmortems hit).
"""
from genie_space_optimizer.optimization.optimizer import (
    _build_plan11_failing_qids_from_raw,
)


def test_builds_from_eval_results_when_no_rca_evidence_typed():
    eval_rows = [
        {
            "question_id": "gs_009",
            "question": "Top 10 customers by order count",
            "ground_truth_sql": "SELECT customer_id, COUNT(*) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            "generated_sql": "SELECT customer_id, RANK() OVER (ORDER BY COUNT(*) DESC) FROM orders GROUP BY 1",
            "judge_rationale": "Used RANK() without LIMIT; returns all rows tied at rank 1",
            "score": 0.0,
        },
        {
            "question_id": "gs_021",
            "question": "Revenue MTD",
            "ground_truth_sql": "WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE)",
            "generated_sql": "WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY",
            "judge_rationale": "Used trailing-30 instead of month-to-date",
            "score": 0.0,
        },
    ]

    out = _build_plan11_failing_qids_from_raw(
        failing_qids=["gs_009", "gs_021"],
        eval_rows=eval_rows,
    )

    assert len(out) == 2
    by_qid = {row["qid"]: row for row in out}
    assert by_qid["gs_009"]["question_text"]
    assert by_qid["gs_009"]["ground_truth_sql"]
    assert by_qid["gs_009"]["generated_sql"]
    assert "RANK()" in by_qid["gs_009"]["judge_rationale"]
    # rca_evidence is now an empty bundle (the typed evidence was unavailable),
    # NOT an entire absence of the qid.
    assert "rca_evidence" in by_qid["gs_009"]
    assert by_qid["gs_009"]["blame_set_seed"] == []


def test_skips_failing_qids_with_no_matching_eval_row():
    out = _build_plan11_failing_qids_from_raw(
        failing_qids=["gs_009", "gs_999_missing"],
        eval_rows=[
            {
                "question_id": "gs_009",
                "question": "q",
                "ground_truth_sql": "gt",
                "generated_sql": "gen",
                "judge_rationale": "rr",
                "score": 0.0,
            },
        ],
    )

    assert {row["qid"] for row in out} == {"gs_009"}


def test_empty_failing_qids_returns_empty_list():
    assert _build_plan11_failing_qids_from_raw(
        failing_qids=[],
        eval_rows=[{"question_id": "gs_009"}],
    ) == []
