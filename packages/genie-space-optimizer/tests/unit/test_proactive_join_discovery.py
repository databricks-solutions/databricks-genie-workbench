from __future__ import annotations


def _join_metadata_snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.fact_sales",
                    "column_configs": [
                        {"column_name": "location_id", "data_type": "BIGINT"},
                    ],
                },
                {
                    "identifier": "cat.sch.dim_location",
                    "column_configs": [
                        {"column_name": "location_id", "data_type": "BIGINT"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "instructions": {"join_specs": []},
    }


def test_example_sql_rows_are_converted_to_positive_eval_rows():
    from genie_space_optimizer.optimization.harness import (
        _example_sqls_to_positive_eval_rows,
    )

    rows = _example_sqls_to_positive_eval_rows([
        {
            "question": "Sales by location",
            "expected_sql": (
                "SELECT l.location_id, SUM(f.sales) "
                "FROM cat.sch.fact_sales f "
                "JOIN cat.sch.dim_location l ON f.location_id = l.location_id "
                "GROUP BY l.location_id"
            ),
        }
    ])

    assert rows[0]["arbiter/value"] == "ground_truth_correct"
    assert rows[0]["request"]["expected_sql"].startswith("SELECT l.location_id")
    assert rows[0]["inputs/expected_sql"].startswith("SELECT l.location_id")


def test_mine_example_sql_joins_reuses_proven_join_pipeline(monkeypatch):
    from genie_space_optimizer.optimization import harness as h_mod

    applied = {}

    def _fake_mine(w, spark, run_id, space_id, metadata_snapshot, eval_rows, catalog, schema, *, iteration=0):
        applied["rows"] = eval_rows
        return {
            "total_applied": 1,
            "new_specs": [
                {
                    "left": {"identifier": "cat.sch.fact_sales"},
                    "right": {"identifier": "cat.sch.dim_location"},
                }
            ],
            "joins_skipped_metric_view": 0,
            "extraction_diagnostics": {
                "total_rows": len(eval_rows),
                "positive_verdicts": len(eval_rows),
                "sql_with_join": 1,
            },
        }

    monkeypatch.setattr(h_mod, "_mine_and_apply_proven_joins", _fake_mine)

    result = h_mod._mine_and_apply_joins_from_example_sqls(
        w=None,
        spark=None,
        run_id="r1",
        space_id="s1",
        metadata_snapshot=_join_metadata_snapshot(),
        examples=[
            {
                "question": "Sales by location",
                "expected_sql": (
                    "SELECT l.location_id, SUM(f.sales) "
                    "FROM cat.sch.fact_sales f "
                    "JOIN cat.sch.dim_location l ON f.location_id = l.location_id "
                    "GROUP BY l.location_id"
                ),
            }
        ],
        catalog="cat",
        schema="sch",
    )

    assert result["total_applied"] == 1
    assert applied["rows"][0]["arbiter/value"] == "ground_truth_correct"


def test_example_sql_join_mining_combines_unified_and_fallback_examples(monkeypatch):
    from genie_space_optimizer.optimization import harness as h_mod

    calls = {}

    def _fake_mine(**kwargs):
        calls["examples"] = kwargs["examples"]
        return {"total_applied": 1, "joins_skipped_metric_view": 0}

    monkeypatch.setattr(h_mod, "_mine_and_apply_joins_from_example_sqls", _fake_mine)

    examples = h_mod._collect_examples_for_join_mining(
        unified_example_result={
            "accepted_examples": [
                {"question": "Q1", "expected_sql": "SELECT * FROM a JOIN b ON a.id=b.id"}
            ]
        },
        preflight_example_result={
            "accepted_examples": [
                {"example_question": "Q2", "example_sql": "SELECT * FROM c JOIN d ON c.id=d.id"}
            ]
        },
    )

    assert len(examples) == 2
    assert examples[0]["expected_sql"].startswith("SELECT * FROM a")
    assert examples[1]["expected_sql"].startswith("SELECT * FROM c")


def test_join_discovery_result_has_explicit_observability_fields():
    from genie_space_optimizer.optimization.harness import (
        _empty_join_discovery_result,
    )

    result = _empty_join_discovery_result()

    for key in (
        "fk_rows_available",
        "fk_candidates_built",
        "execution_candidates",
        "example_sql_join_candidates",
        "joins_skipped_metric_view",
        "type_incompatible",
        "spec_validation_rejected",
    ):
        assert key in result
