"""Benchmark deduplication and direct Delta handoff contracts."""

from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.benchmarks import (
    _normalize_benchmark_row,
    build_benchmark_handoff_records,
    deduplicate_benchmark_corpus,
    duplicate_rejection_mutations,
    load_benchmark_corpus,
    persist_benchmark_corpus,
)


def test_handoff_prefers_reconciled_live_question_id() -> None:
    live_id = "a" * 32
    records = build_benchmark_handoff_records([{
        "id": "internal-q1",
        "space_question_id": live_id,
        "question": "What is revenue?",
        "expected_sql": "SELECT 1",
        "validation_status": "valid",
    }])

    assert records[0]["inputs"]["question_id"] == live_id


def test_nested_handoff_promotes_only_native_shaped_question_id() -> None:
    live_id = "b" * 32
    live = _normalize_benchmark_row({
        "inputs": {"question_id": live_id, "question": "Live question"},
        "expectations": {"expected_response": "SELECT 1"},
    })
    internal = _normalize_benchmark_row({
        "inputs": {"question_id": "internal-q1", "question": "Internal question"},
        "expectations": {"expected_response": "SELECT 2"},
    })

    assert live["space_question_id"] == live_id
    assert "space_question_id" not in internal


def test_optimizer_loader_keeps_all_eligible_benchmarks_in_working_window() -> None:
    benchmarks = [
        {
            "id": f"q-{index:02d}",
            "question": f"Question {index:02d}?",
            "expected_sql": f"SELECT {index}",
            "validation_status": "valid",
        }
        for index in range(37)
    ]

    loaded = load_benchmark_corpus(benchmarks, "unused", "unused")

    assert len(loaded) == 37
    assert [row["id"] for row in loaded] == [row["id"] for row in benchmarks]


def test_optimizer_loader_enforces_40_question_ceiling() -> None:
    benchmarks = [
        {
            "id": f"q-{index:02d}",
            "question": f"Question {index:02d}?",
            "expected_sql": f"SELECT {index}",
            "validation_status": "valid",
        }
        for index in range(41)
    ]

    loaded = load_benchmark_corpus(benchmarks, "unused", "unused")

    assert len(loaded) == 40
    assert loaded[-1]["id"] == "q-39"


def test_deduplication_uses_deterministic_retention_priority() -> None:
    rows = [
        {
            "id": "synthetic-valid",
            "question": "Revenue by month?",
            "expected_sql": "SELECT 1",
            "validation_status": "valid",
            "source": "llm_generated",
            "provenance": "synthetic",
            "priority": "P1",
        },
        {
            "id": "genie-user",
            "question": "  [AUTO-OPTIMIZE] Revenue   by month? ",
            "expected_sql": "",
            "validation_status": "question_only",
            "source": "genie_benchmark",
            "provenance": "curated",
            "priority": "P0",
        },
        {
            "id": "valid-sql",
            "question": "Margin by region?",
            "expected_sql": "SELECT 2",
            "validation_status": "valid",
            "source": "llm_generated",
            "provenance": "synthetic",
            "priority": "P1",
        },
        {
            "id": "curated-no-sql",
            "question": "margin BY region?",
            "expected_sql": "",
            "validation_status": "question_only",
            "source": "unknown",
            "provenance": "curated",
            "priority": "P0",
        },
        {
            "id": "stable-first",
            "question": "Units?",
            "expected_sql": "",
            "validation_status": "question_only",
            "source": "llm_generated",
            "provenance": "synthetic",
            "priority": "P1",
        },
        {
            "id": "stable-second",
            "question": " units? ",
            "expected_sql": "",
            "validation_status": "question_only",
            "source": "llm_generated",
            "provenance": "synthetic",
            "priority": "P1",
        },
    ]

    retained, rejected = deduplicate_benchmark_corpus(rows)

    assert [row["id"] for row in retained] == [
        "genie-user",
        "valid-sql",
        "stable-first",
    ]
    by_id = {row["id"]: row for row in rejected}
    assert by_id["synthetic-valid"]["duplicate_retained_question_id"] == "genie-user"
    assert by_id["curated-no-sql"]["duplicate_retained_question_id"] == "valid-sql"
    assert by_id["stable-second"]["duplicate_retained_question_id"] == "stable-first"
    assert all(
        row["validation_reason_code"] == "duplicate_normalized_question"
        for row in rejected
    )
    mutations = duplicate_rejection_mutations(rejected)
    assert all(row["op"] == "removed" for row in mutations)
    assert mutations[0]["reason"] == "duplicate_normalized_question"
    assert mutations[0]["before"]["retained_question_id"]


def test_direct_delta_persistence_preserves_full_working_window_and_nested_schema() -> None:
    calls: dict[str, object] = {}

    class Writer:
        def format(self, value):
            calls["format"] = value
            return self

        def mode(self, value):
            calls["mode"] = value
            return self

        def option(self, key, value):
            calls[("option", key)] = value
            return self

        def saveAsTable(self, value):
            calls["table"] = value

    class Frame:
        write = Writer()

    class Spark:
        def createDataFrame(self, records, schema):
            calls["records"] = records
            calls["schema"] = schema
            return Frame()

    benchmarks = [
        {
            "id": f"q{index}",
            "question": f"What is revenue metric {index}?",
            "expected_sql": f"SELECT {index} FROM cat.sch.sales",
            "expected_asset": "TABLE",
            "validation_status": "valid",
            "required_tables": ["cat.sch.sales"],
        }
        for index in range(1, 38)
    ]

    with patch(
        "genie_space_optimizer.optimization.benchmarks.retry_delta_write",
        side_effect=lambda operation, **_kwargs: operation(),
    ):
        result = persist_benchmark_corpus(
            Spark(),
            benchmarks,
            "cat.sch",
            "sales",
            space_id="space-1",
            catalog="cat",
            gold_schema="sch",
        )

    assert result["record_count"] == 37
    assert calls["format"] == "delta"
    assert calls["mode"] == "overwrite"
    assert calls[("option", "overwriteSchema")] == "true"
    assert calls["table"] == "`cat`.`sch`.`genie_benchmarks_sales`"
    assert len(calls["records"]) == 37
    record = calls["records"][0]
    assert record["inputs"]["question_id"] == "q1"
    assert record["inputs"]["space_id"] == "space-1"
    assert record["expectations"]["expected_response"].startswith("SELECT 1")
    assert record["expectations"]["required_tables"] == ["cat.sch.sales"]
