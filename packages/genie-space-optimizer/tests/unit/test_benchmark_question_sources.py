from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.evaluation import extract_genie_space_benchmarks


def _hex(seed: str) -> str:
    return (seed * 32)[:32]


def _space_config() -> dict:
    return {
        "_parsed_space": {
            "version": 2,
            "config": {
                "sample_questions": [
                    {
                        "id": _hex("a"),
                        "question": ["What were total sales yesterday?"],
                    }
                ]
            },
            "data_sources": {
                "tables": [{"identifier": "cat.sch.fact_sales"}],
                "metric_views": [],
            },
            "instructions": {
                "example_question_sqls": [
                    {
                        "id": _hex("b"),
                        "question": ["How should I use the example function?"],
                        "sql": ["SELECT cat.sch.fn_internal_example()"],
                    }
                ]
            },
            "benchmarks": {
                "questions": [
                    {
                        "id": _hex("c"),
                        "question": ["What is sales by market?"],
                        "answer": [
                            {
                                "format": "SQL",
                                "content": [
                                    "SELECT market, SUM(sales) "
                                    "FROM cat.sch.fact_sales GROUP BY market"
                                ],
                            }
                        ],
                    }
                ]
            },
        }
    }


def test_extracts_user_benchmarks_and_sample_questions_but_not_example_sqls() -> None:
    with patch(
        "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
        return_value=(True, ""),
    ):
        rows = extract_genie_space_benchmarks(
            _space_config(),
            spark=MagicMock(),
            catalog="cat",
            schema="sch",
        )

    questions = [row["question"] for row in rows]
    assert questions == [
        "What is sales by market?",
        "What were total sales yesterday?",
    ]

    by_question = {row["question"]: row for row in rows}
    assert by_question["What is sales by market?"]["source"] == "genie_benchmark"
    assert by_question["What is sales by market?"]["expected_sql"].startswith("SELECT market")
    assert by_question["What were total sales yesterday?"]["source"] == "sample_question"
    assert by_question["What were total sales yesterday?"]["expected_sql"] == ""
    assert all("example function" not in row["question"].lower() for row in rows)


def test_invalid_user_benchmark_sql_is_kept_question_only_for_sql_regeneration() -> None:
    with patch(
        "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
        return_value=(False, "TABLE_OR_VIEW_NOT_FOUND"),
    ):
        rows = extract_genie_space_benchmarks(
            _space_config(),
            spark=MagicMock(),
            catalog="cat",
            schema="sch",
        )

    user_row = next(row for row in rows if row["source"] == "genie_benchmark")
    assert user_row["question"] == "What is sales by market?"
    assert user_row["expected_sql"] == ""
    assert user_row["validation_status"] == "question_only"
    assert user_row["validation_reason_code"] == "invalid_source_sql"


def test_legacy_auto_optimize_prefix_is_normalized_when_reading_native_benchmarks() -> None:
    config = _space_config()
    config["_parsed_space"]["benchmarks"]["questions"] = [
        {
            "id": _hex("d"),
            "question": ["[auto-optimize] Which stores grew sales?"],
            "metadata": {
                "source": "gso_optimizer",
                "run_id": "legacy-run",
                "original_question": "Which stores grew sales?",
            },
            "answer": [
                {
                    "format": "SQL",
                    "content": ["SELECT store, SUM(sales) FROM cat.sch.fact_sales GROUP BY store"],
                }
            ],
        }
    ]

    with patch(
        "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
        return_value=(True, ""),
    ):
        rows = extract_genie_space_benchmarks(
            config,
            spark=MagicMock(),
            catalog="cat",
            schema="sch",
        )

    assert rows[0]["question"] == "Which stores grew sales?"
    assert rows[0]["source"] == "genie_benchmark"


def test_benchmark_rows_matching_example_sql_questions_are_filtered() -> None:
    from genie_space_optimizer.optimization.evaluation import (
        _filter_example_sql_mirrored_benchmarks,
    )

    config = {
        "_parsed_space": {
            "instructions": {
                "example_question_sqls": [
                    {
                        "question": ["What is example-only training?"],
                        "sql": ["SELECT 1"],
                    }
                ]
            }
        }
    }
    rows = [
        {"question": "What is example-only training?", "expected_sql": "SELECT 1"},
        {"question": "What is real benchmark?", "expected_sql": "SELECT 2"},
    ]

    filtered = _filter_example_sql_mirrored_benchmarks(rows, config)

    assert [row["question"] for row in filtered] == ["What is real benchmark?"]
