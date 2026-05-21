"""Plan 12 — build_run_summary derives hard/soft failure counts from
eval_result.rows instead of trusting a stale write that diverged
under retry."""


def test_hard_failures_count_from_eval_result():
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    eval_result = {
        "rows": [
            {"question_id": "q1", "score": 0.0},  # hard
            {"question_id": "q2", "score": 0.5},  # soft
            {"question_id": "q3", "score": 1.0},  # pass
            {"question_id": "q4", "score": 0.0},  # hard
        ],
    }
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.0},
        terminal_state={"final_accuracy": 50.0},
        iteration_count=3,
        accuracy_delta_pp=50.0,
        eval_result=eval_result,
    )
    assert summary["hard_failures_count"] == 2
    assert summary["soft_failures_count"] == 1


def test_counts_zero_when_no_eval_result():
    """The eval_result kwarg is optional; when absent the counts are 0
    (preserves byte-stable replay against existing callers that
    haven't been wired yet)."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.0},
        terminal_state={"final_accuracy": 50.0},
        iteration_count=3,
        accuracy_delta_pp=50.0,
    )
    assert summary["hard_failures_count"] == 0
    assert summary["soft_failures_count"] == 0


def test_handles_malformed_score():
    """Non-numeric or missing score defaults to 0.0 (hard) so the count
    never under-reports failures."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    eval_result = {
        "rows": [
            {"question_id": "q1"},                 # missing score
            {"question_id": "q2", "score": "x"},   # unparseable
            {"question_id": "q3", "score": 1.0},   # pass
        ],
    }
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.0},
        terminal_state={"final_accuracy": 50.0},
        iteration_count=3,
        accuracy_delta_pp=50.0,
        eval_result=eval_result,
    )
    assert summary["hard_failures_count"] == 2
    assert summary["soft_failures_count"] == 0


def test_existing_fields_preserved():
    """The PR 7 extension must not regress the existing
    schema_version / baseline / terminal_state / iteration_count /
    accuracy_delta_pp fields."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.0},
        terminal_state={"final_accuracy": 50.0},
        iteration_count=3,
        accuracy_delta_pp=50.0,
    )
    assert "schema_version" in summary
    assert summary["baseline"] == {"overall_accuracy": 0.0}
    assert summary["terminal_state"] == {"final_accuracy": 50.0}
    assert summary["iteration_count"] == 3
    assert summary["accuracy_delta_pp"] == 50.0
