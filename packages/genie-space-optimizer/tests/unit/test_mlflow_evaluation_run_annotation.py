from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from genie_space_optimizer.optimization import evaluation


class _FakeMlflowClient:
    def __init__(self) -> None:
        self.tags: list[tuple[str, str, str]] = []
        self.metrics: list[tuple[str, str, float]] = []

    def set_tag(self, run_id: str, key: str, value: str) -> None:
        self.tags.append((run_id, key, value))

    def log_metric(self, run_id: str, key: str, value: float) -> None:
        self.metrics.append((run_id, key, value))


def test_annotate_mlflow_evaluation_run_sets_name_tags_and_metrics(monkeypatch):
    client = _FakeMlflowClient()
    monkeypatch.setattr(evaluation, "MlflowClient", lambda: client)

    annotated = evaluation.annotate_mlflow_evaluation_run(
        SimpleNamespace(run_id="eval-run-123"),
        canonical_name="iter_02 / full_eval / pass_1 / mlflow_eval / run_abcd1234",
        tags={
            "genie.run_id": "opt-123",
            "genie.stage": "full_eval",
            "genie.iteration": "02",
            "skip_none": None,
            "numeric_tag": 7,
        },
        metrics={
            "overall_accuracy": 95.83,
            "evaluated_count": 24,
            "excluded_count": 0,
            "correct_count": 23,
            "skip_text": "not-a-metric",
            "skip_bool": True,
            "skip_none": None,
        },
    )

    assert annotated == "eval-run-123"
    assert (
        "eval-run-123",
        "mlflow.runName",
        "iter_02 / full_eval / pass_1 / mlflow_eval / run_abcd1234",
    ) in client.tags
    assert ("eval-run-123", "genie.run_id", "opt-123") in client.tags
    assert ("eval-run-123", "genie.stage", "full_eval") in client.tags
    assert ("eval-run-123", "genie.iteration", "02") in client.tags
    assert ("eval-run-123", "numeric_tag", "7") in client.tags
    assert not any(key == "skip_none" for _, key, _ in client.tags)

    assert ("eval-run-123", "overall_accuracy", 95.83) in client.metrics
    assert ("eval-run-123", "evaluated_count", 24.0) in client.metrics
    assert ("eval-run-123", "excluded_count", 0.0) in client.metrics
    assert ("eval-run-123", "correct_count", 23.0) in client.metrics
    assert not any(key == "skip_text" for _, key, _ in client.metrics)
    assert not any(key == "skip_bool" for _, key, _ in client.metrics)
    assert not any(key == "skip_none" for _, key, _ in client.metrics)


def test_annotate_mlflow_evaluation_run_is_noop_without_run_id(monkeypatch):
    def _unexpected_client():
        raise AssertionError("MlflowClient should not be constructed without a run_id")

    monkeypatch.setattr(evaluation, "MlflowClient", _unexpected_client)

    annotated = evaluation.annotate_mlflow_evaluation_run(
        SimpleNamespace(),
        canonical_name="iter_00 / baseline / mlflow_eval / run_abcd1234",
        tags={"genie.run_id": "opt-123"},
        metrics={"overall_accuracy": 80.0},
    )

    assert annotated == ""


def test_annotate_mlflow_evaluation_runs_deduplicates_and_names_rows(monkeypatch):
    client = _FakeMlflowClient()
    monkeypatch.setattr(evaluation, "MlflowClient", lambda: client)

    annotated = evaluation._annotate_mlflow_evaluation_runs(
        SimpleNamespace(evaluation_run_ids=["eval-1", "eval-2", "eval-1"]),
        parent_run_name="iter_02 / full_eval / pass_1 / run_abcd1234",
        tags={"genie.run_id": "opt-123"},
        metrics={"overall_accuracy": 50.0},
    )

    assert annotated == ["eval-1", "eval-2"]
    run_names = [
        value
        for _, key, value in client.tags
        if key == "mlflow.runName"
    ]
    assert run_names == [
        "iter_02 / full_eval / pass_1 / mlflow_eval_row_001 / run_abcd1234",
        "iter_02 / full_eval / pass_1 / mlflow_eval_row_002 / run_abcd1234",
    ]


def test_sequential_fallback_preserves_row_evaluation_run_ids(monkeypatch):
    call_count = {"n": 0}

    def fake_evaluate(**_kwargs):
        call_count["n"] += 1
        return SimpleNamespace(
            run_id=f"eval-row-{call_count['n']}",
            metrics={"result_correctness/mean": 1.0},
            tables={
                "eval_results": pd.DataFrame(
                    [{"inputs/question_id": f"q{call_count['n']}", "trace_id": f"t{call_count['n']}"}],
                ),
            },
        )

    monkeypatch.setattr(evaluation.mlflow.genai, "evaluate", fake_evaluate, raising=False)
    monkeypatch.setattr(evaluation, "_patch_mlflow_harness_none_trace", lambda: None)

    result = evaluation._run_evaluate_sequential_fallback(
        evaluate_kwargs={
            "data": pd.DataFrame(
                [
                    {"inputs": {"question_id": "q1"}},
                    {"inputs": {"question_id": "q2"}},
                ],
            ),
            "scorers": [],
        },
    )

    assert result.evaluation_run_ids == ["eval-row-1", "eval-row-2"]
    assert call_count["n"] == 2


def test_annotate_mlflow_evaluation_runs_mirrors_headline_metrics(monkeypatch):
    client = _FakeMlflowClient()
    monkeypatch.setattr(evaluation, "MlflowClient", lambda: client)

    annotated = evaluation._annotate_mlflow_evaluation_runs(
        SimpleNamespace(run_id="eval-main-1"),
        parent_run_name="iter_05 / full_eval / pass_1 / run_abcd1234",
        tags={
            "genie.run_id": "opt-123",
            "genie.stage": "full_eval",
            "genie.iteration": "05",
            "genie.eval_scope": "full",
        },
        metrics={
            "overall_accuracy": 95.83,
            "pre_arbiter_accuracy": 66.67,
            "correct_count": 23,
            "total_questions": 24,
            "evaluated_count": 24,
            "excluded_count": 0,
            "failure_count": 1,
            "thresholds_passed": 1.0,
            "harness_retry_count": 0,
        },
    )

    assert annotated == ["eval-main-1"]
    assert (
        "eval-main-1",
        "mlflow.runName",
        "iter_05 / full_eval / pass_1 / mlflow_eval / run_abcd1234",
    ) in client.tags
    assert ("eval-main-1", "overall_accuracy", 95.83) in client.metrics
    assert ("eval-main-1", "pre_arbiter_accuracy", 66.67) in client.metrics
    assert ("eval-main-1", "evaluated_count", 24.0) in client.metrics
    assert ("eval-main-1", "excluded_count", 0.0) in client.metrics
