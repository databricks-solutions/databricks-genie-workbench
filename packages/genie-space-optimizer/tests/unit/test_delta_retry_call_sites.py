from __future__ import annotations

from typing import Any

import pytest


def test_create_evaluation_dataset_retries_merge_records(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.optimization import evaluation

    calls: list[str] = []

    class _FakeDataset:
        def merge_records(self, records):
            calls.append(f"merge:{len(records)}")

    class _FakeDatasets:
        def get_dataset(self, name: str):
            return _FakeDataset()

    def fake_retry(operation, **kwargs):
        assert kwargs["operation_name"] == "evaluation_dataset.merge_records"
        assert kwargs["table_name"] == "cat.sch.genie_benchmarks_sales"
        return operation()

    monkeypatch.setattr(evaluation.mlflow.genai, "datasets", _FakeDatasets())
    monkeypatch.setattr(evaluation, "retry_delta_write", fake_retry)

    result = evaluation.create_evaluation_dataset(
        object(),
        [{"id": "q1", "question": "How much revenue?", "expected_sql": "SELECT 1"}],
        "cat.sch",
        "sales",
    )

    assert result["dataset"] is not None
    assert result["table_name"] == "cat.sch.genie_benchmarks_sales"
    assert result["input_count"] == 1
    assert result["record_count"] == 1
    assert result["unique_question_id_count"] == 1
    assert calls == ["merge:1"]


def test_create_evaluation_dataset_rejects_duplicate_question_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.optimization import evaluation

    merge_calls: list[int] = []

    class _FakeDataset:
        def merge_records(self, records):
            merge_calls.append(len(records))

    class _FakeDatasets:
        def get_dataset(self, name: str):
            return _FakeDataset()

    monkeypatch.setattr(evaluation.mlflow.genai, "datasets", _FakeDatasets())

    with pytest.raises(RuntimeError, match="Duplicate benchmark question_id"):
        evaluation.create_evaluation_dataset(
            object(),
            [
                {"id": "dup_qid", "question": "How much revenue?", "expected_sql": "SELECT 1"},
                {"id": "dup_qid", "question": "How many stores?", "expected_sql": "SELECT 2"},
            ],
            "cat.sch",
            "sales",
        )

    assert merge_calls == []


def test_create_evaluation_dataset_rejects_duplicate_normalized_questions(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.optimization import evaluation

    merge_calls: list[int] = []

    class _FakeDataset:
        def merge_records(self, records):
            merge_calls.append(len(records))

    class _FakeDatasets:
        def get_dataset(self, name: str):
            return _FakeDataset()

    monkeypatch.setattr(evaluation.mlflow.genai, "datasets", _FakeDatasets())

    with pytest.raises(RuntimeError, match="Duplicate benchmark question text"):
        evaluation.create_evaluation_dataset(
            object(),
            [
                {"id": "q1", "question": "How much revenue?", "expected_sql": "SELECT 1"},
                {"id": "q2", "question": "  how much revenue?  ", "expected_sql": "SELECT 1"},
            ],
            "cat.sch",
            "sales",
        )

    assert merge_calls == []


def test_flag_for_human_review_uses_delta_write_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.optimization import labeling

    captured_sql: list[str] = []

    def fake_ensure(_spark, _catalog, _schema) -> None:
        return None

    def fake_execute(_spark, sql: str, **kwargs: Any) -> None:
        captured_sql.append(sql)
        assert kwargs["operation_name"] == "flag_for_human_review"
        assert kwargs["table_name"] == "cat.sch.genie_opt_flagged_questions"

    monkeypatch.setattr(labeling, "_ensure_flagged_questions_table", fake_ensure)
    monkeypatch.setattr(labeling, "execute_delta_write_with_retry", fake_execute)

    count = labeling.flag_for_human_review(
        object(),
        "run-1",
        "cat",
        "sch",
        "sales",
        [
            {
                "question_id": "q1",
                "question_text": "Which rows failed?",
                "reason": "ADDITIVE_LEVERS_EXHAUSTED",
                "iterations_failed": 2,
                "patches_tried": "lever 1, lever 3",
            }
        ],
    )

    assert count == 1
    assert len(captured_sql) == 1
    assert captured_sql[0].lstrip().startswith("MERGE INTO cat.sch.genie_opt_flagged_questions")


def test_write_scan_snapshot_retries_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.optimization import scan_snapshots

    captured_sql: list[str] = []

    def fake_ensure(_spark, _catalog, _schema) -> None:
        return None

    def fake_execute(_spark, sql: str, **kwargs: Any) -> None:
        captured_sql.append(sql)
        assert kwargs["operation_name"] == "write_scan_snapshot"
        assert kwargs["table_name"] == "cat.sch.genie_opt_scan_snapshots"

    monkeypatch.setattr(scan_snapshots, "_ensure_scan_snapshot_table", fake_ensure)
    monkeypatch.setattr(scan_snapshots, "execute_delta_write_with_retry", fake_execute)

    wrote = scan_snapshots.write_scan_snapshot(
        object(),
        "run-1",
        "space-1",
        "preflight",
        {
            "score": 9,
            "total": 12,
            "maturity": "Ready to Optimize",
            "checks": [],
            "findings": [],
            "warnings": [],
            "scanned_at": "2026-04-30T10:00:00+00:00",
        },
        "cat",
        "sch",
    )

    assert wrote is True
    assert len(captured_sql) == 1
    assert captured_sql[0].lstrip().startswith("MERGE INTO cat.sch.genie_opt_scan_snapshots")


def test_create_evaluation_dataset_persists_30_unique_topup_records(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.optimization import evaluation

    merged_records: list[dict] = []

    class _FakeDataset:
        def merge_records(self, records):
            merged_records.extend(records)

    class _FakeDatasets:
        def get_dataset(self, name: str):
            return _FakeDataset()

    def fake_retry(operation, **kwargs):
        return operation()

    monkeypatch.setattr(evaluation.mlflow.genai, "datasets", _FakeDatasets())
    monkeypatch.setattr(evaluation, "retry_delta_write", fake_retry)

    benchmarks = [
        {
            "id": f"sales_gs_{i + 1:03d}",
            "question": f"validated curated question {i + 1}",
            "expected_sql": "SELECT 1",
            "split": "train",
        }
        for i in range(18)
    ] + [
        {
            "id": f"sales_{i + 19:03d}",
            "question": f"validated synthetic top-up question {i + 1}",
            "expected_sql": "SELECT 1",
            "split": "held_out" if i < 5 else "train",
        }
        for i in range(12)
    ]

    result = evaluation.create_evaluation_dataset(
        object(),
        benchmarks,
        "cat.sch",
        "sales",
        max_benchmark_count=30,
    )

    assert result["record_count"] == 30
    assert len(merged_records) == 30
    assert len({r["inputs"]["question_id"] for r in merged_records}) == 30
    assert len({r["inputs"]["question"].lower().strip() for r in merged_records}) == 30
