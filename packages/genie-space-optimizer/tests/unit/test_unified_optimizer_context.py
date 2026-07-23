from __future__ import annotations

import json

from genie_space_optimizer.optimization.unified_loop import (
    _llm_messages,
    _optimizer_context_pack,
)


def _table(name: str, *, columns: list[str] | None = None) -> dict:
    return {
        "identifier": f"cat.sch.{name}",
        "description": f"{name} description",
        "column_configs": [
            {
                "column_name": c,
                "description": f"{c} description",
                "data_type": "STRING",
            }
            for c in (columns or ["id"])
        ],
    }


def _eval(sql: str, *, question: str = "Why did this fail?") -> dict:
    return {
        "overall_accuracy": 20.0,
        "total_questions": 1,
        "correct_count": 0,
        "rows": [
            {
                "question_id": "q_late",
                "question": question,
                "assessment": "BAD",
                "expected_sql": sql,
                "generated_sql": "SELECT 1",
                "genie_equivalent_eval": {
                    "assessment_reasons": ["wrong asset or missing join context"]
                },
            }
        ],
    }


def test_relevant_table_after_old_position_cap_is_included() -> None:
    config = {
        "data_sources": {
            "tables": [_table(f"table_{i}") for i in range(25)],
        },
        "instructions": {},
    }

    context, stats, text = _optimizer_context_pack(
        config,
        _eval("SELECT * FROM cat.sch.table_24"),
    )

    identifiers = [a["identifier"] for a in context["space_context"]["assets"]]
    assert "cat.sch.table_24" in identifiers
    assert stats["included_counts"]["assets"] >= 1
    assert json.loads(text)["space_context"]["assets"]


def test_relevant_metric_view_after_old_position_cap_is_included() -> None:
    config = {
        "data_sources": {
            "metric_views": [_table(f"mv_{i}") for i in range(25)],
        },
        "instructions": {},
    }

    context, _stats, _text = _optimizer_context_pack(
        config,
        _eval("SELECT MEASURE(total_sales) FROM cat.sch.mv_24"),
    )

    identifiers = [a["identifier"] for a in context["space_context"]["assets"]]
    assert "cat.sch.mv_24" in identifiers


def test_relevant_column_after_old_position_cap_is_included() -> None:
    columns = [f"col_{i}" for i in range(90)] + ["late_revenue_flag"]
    config = {
        "data_sources": {
            "tables": [_table("wide_fact", columns=columns)],
        },
        "instructions": {},
    }

    context, _stats, _text = _optimizer_context_pack(
        config,
        _eval("SELECT late_revenue_flag FROM cat.sch.wide_fact"),
    )

    asset = context["space_context"]["assets"][0]
    included_columns = [c["column_name"] for c in asset["columns"]]
    assert "late_revenue_flag" in included_columns


def test_optimizer_uses_only_active_wide_schema_columns() -> None:
    original_columns = [f"col_{index}" for index in range(90)]
    config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [_table("wide_fact", columns=original_columns)],
            },
            "instructions": {},
        },
        "_uc_columns": [
            {
                "catalog_name": "cat",
                "schema_name": "sch",
                "table_name": "wide_fact",
                "column_name": f"col_{index}",
                "data_type": "STRING",
                "comment": f"UC comment {index}",
                "stable_rank": index + 1,
            }
            for index in range(50)
        ],
    }

    context, _stats, _text = _optimizer_context_pack(
        config,
        _eval("SELECT col_89 FROM `cat`.`sch`.`wide_fact`"),
    )

    columns = context["space_context"]["assets"][0]["columns"]
    assert len(columns) == 50
    assert {column["column_name"] for column in columns} == {
        f"col_{index}" for index in range(50)
    }
    assert "col_89" not in {column["column_name"] for column in columns}
    col_0 = next(column for column in columns if column["column_name"] == "col_0")
    assert col_0["description"] == "col_0 description"
    assert col_0["comment"] == "UC comment 0"
    assert col_0["stable_rank"] == 1


def test_relevant_join_spec_after_old_position_cap_is_included() -> None:
    join_specs = []
    for i in range(45):
        join_specs.append(
            {
                "left": {"identifier": f"cat.sch.left_{i}"},
                "right": {"identifier": f"cat.sch.right_{i}"},
                "sql": [f"left_{i}.id = right_{i}.id"],
            }
        )
    config = {
        "data_sources": {
            "tables": [_table("left_44"), _table("right_44")],
        },
        "instructions": {"join_specs": join_specs},
    }

    context, _stats, _text = _optimizer_context_pack(
        config,
        _eval("SELECT * FROM cat.sch.left_44 JOIN cat.sch.right_44 ON left_44.id = right_44.id"),
    )

    rendered = json.dumps(context["space_context"]["join_specs"])
    assert "left_44" in rendered
    assert "right_44" in rendered


def test_large_context_is_valid_json_with_omission_summary_and_no_raw_marker() -> None:
    config = {
        "description": "large config",
        "data_sources": {
            "tables": [
                _table(f"table_{i}", columns=[f"col_{j}" for j in range(120)])
                for i in range(60)
            ],
        },
        "instructions": {
            "text_instructions": [
                {"content": ["## PURPOSE\n" + ("Explain policy. " * 400)]}
            ]
        },
    }

    _context, _stats, text = _optimizer_context_pack(
        config,
        _eval("SELECT col_119 FROM cat.sch.table_59"),
        max_chars=12_000,
    )

    parsed = json.loads(text)
    assert parsed["omitted_context_summary"]["assets"]["omitted"] > 0
    assert "...<truncated>" not in text


def test_llm_messages_include_quality_rubric_and_scan_context() -> None:
    config = {
        "description": "",
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.orders",
                    "columns": [
                        {"name": "id", "type": "INT"},
                        {"name": "amount", "type": "DOUBLE"},
                    ],
                }
            ],
        },
        "instructions": {},
    }

    messages, _stats = _llm_messages(
        allowed_levers=[1, 5, 6],
        current_config=config,
        eval_result=_eval("SELECT SUM(amount) FROM cat.sch.orders"),
        reflections=[],
    )

    user = json.loads(messages[1]["content"])
    labels = {
        item["label"]
        for item in user["well_curated_space_rubric"]["config_quality_checks"]
    }
    assert "Space description" in labels
    assert "SQL guidance artifacts" in labels
    failed = {
        item["label"]
        for item in user["space_quality_scan"]["failed_checks"]
    }
    assert "Space description" in failed
    assert "SQL guidance artifacts" in failed
