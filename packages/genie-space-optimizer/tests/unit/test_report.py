from __future__ import annotations

from pathlib import Path

import pandas as pd


def _minimal_iterations_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_id": "run-123",
                "iteration": 0,
                "lever": None,
                "eval_scope": "full",
                "timestamp": "2026-06-26T00:00:00Z",
                "overall_accuracy": 50.0,
                "total_questions": 2,
                "correct_count": 1,
                "scores_json": "{}",
                "failures_json": "[]",
                "remaining_failures": "[]",
                "repeatability_pct": None,
                "repeatability_json": None,
                "thresholds_met": False,
                "rows_json": "[]",
            },
        ],
    )


def _run_row() -> dict[str, object]:
    return {
        "run_id": "run-123",
        "space_id": "space-abc",
        "domain": "sales",
        "status": "CONVERGED",
        "started_at": "2026-06-26T00:00:00Z",
        "completed_at": "2026-06-26T00:05:00Z",
        "triggered_by": "test@example.com",
        "best_iteration": 0,
        "best_accuracy": 50.0,
        "convergence_reason": "threshold_met",
    }


def _patch_report_loaders(monkeypatch, iterations_df: pd.DataFrame) -> None:
    from genie_space_optimizer.optimization import report as report_mod

    monkeypatch.setattr(report_mod, "load_run", lambda *args, **kwargs: _run_row())
    monkeypatch.setattr(report_mod, "load_stages", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(report_mod, "load_iterations", lambda *args, **kwargs: iterations_df)
    monkeypatch.setattr(report_mod, "load_patches", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(report_mod, "_load_asi", lambda *args, **kwargs: pd.DataFrame())


def test_generate_report_tolerates_iterations_without_mlflow_pointer_columns(
    monkeypatch,
    mock_spark,
    tmp_path,
) -> None:
    from genie_space_optimizer.optimization import report as report_mod

    iterations_df = _minimal_iterations_df()
    assert "mlflow_run_id" not in iterations_df.columns
    assert "model_id" not in iterations_df.columns
    _patch_report_loaders(monkeypatch, iterations_df)

    report_path = report_mod.generate_report(
        mock_spark,
        "run-123",
        "sales",
        "catalog",
        "schema",
        output_dir=str(tmp_path),
    )

    assert report_path is not None
    report = Path(report_path).read_text(encoding="utf-8")
    assert "## Per-Iteration Detail" in report
    assert "## MLflow Links" in report
    assert "Evaluation Run IDs" not in report
    assert "Model IDs" not in report
