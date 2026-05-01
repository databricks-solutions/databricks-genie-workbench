"""Schema-aware example-SQL correctness arbiter (Task 5).

Verifies metadata_snapshot is rendered into the prompt and used to
catch asset-class routing errors (MV vs table)."""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.scorers import arbiter


def test_metadata_snapshot_renders_into_prompt():
    snap = {
        "_asset_semantics": {
            "main.s.dim_location": {
                "asset_type": "table",
                "columns": [{"name": "store_id"}, {"name": "city"}],
            },
            "main.s.store_count_day": {
                "asset_type": "metric_view",
                "measures": [{"name": "store_count"}],
                "dimensions": [{"name": "day"}],
            },
        },
    }
    captured = {}

    def fake_call(_w, prompt, *, prompt_name=None):
        captured["prompt"] = prompt
        return {"value": "yes", "rationale": "ok"}

    with patch.object(arbiter, "_call_llm_for_scoring", side_effect=fake_call):
        result = arbiter.score_example_sql_correctness(
            question="how many stores?",
            sql="SELECT COUNT(*) FROM main.s.dim_location",
            result_rows=[{"_c0": 12}],
            w=MagicMock(),
            metadata_snapshot=snap,
        )
    assert result["value"] == "yes"
    assert "main.s.dim_location" in captured["prompt"]
    assert "table" in captured["prompt"].lower()
    assert "main.s.store_count_day" in captured["prompt"]
    assert "metric_view" in captured["prompt"].lower()
    assert "MEASURE(" in captured["prompt"] or "metric view" in captured["prompt"].lower()


def test_no_snapshot_still_works():
    with patch.object(arbiter, "_call_llm_for_scoring",
                       return_value={"value": "yes", "rationale": "ok"}):
        result = arbiter.score_example_sql_correctness(
            question="q",
            sql="SELECT 1",
            result_rows=[],
            w=MagicMock(),
            metadata_snapshot=None,
        )
    assert result["value"] == "yes"


def test_teaching_safety_judge_returns_yes_no_uncertain():
    snap = {
        "_asset_semantics": {
            "main.s.orders": {"asset_type": "table",
                              "columns": [{"name": "total"}, {"name": "month"}]},
        },
    }
    with patch.object(arbiter, "_call_llm_for_scoring",
                       return_value={"value": "no",
                                     "rationale": "grain mismatch: question says monthly"}):
        result = arbiter.score_example_sql_teaching_safety(
            question="What is the average order total per month?",
            sql="SELECT YEAR(month), AVG(total) FROM main.s.orders GROUP BY YEAR(month)",
            w=MagicMock(),
            metadata_snapshot=snap,
        )
    assert result["value"] == "no"
    assert "grain mismatch" in result["rationale"]


def test_teaching_safety_uncertain_on_llm_failure():
    with patch.object(arbiter, "_call_llm_for_scoring",
                       side_effect=RuntimeError("endpoint down")):
        result = arbiter.score_example_sql_teaching_safety(
            question="q", sql="SELECT 1",
            w=MagicMock(), metadata_snapshot={},
        )
    assert result["value"] == "uncertain"
    assert "endpoint down" in result["rationale"]


def test_teaching_safety_includes_schema_block():
    snap = {
        "_asset_semantics": {
            "main.s.orders": {"asset_type": "table",
                              "columns": [{"name": "total"}]},
        },
    }
    captured = {}

    def fake(_w, prompt, *, prompt_name=None):
        captured["prompt"] = prompt
        captured["prompt_name"] = prompt_name
        return {"value": "yes", "rationale": "ok"}

    with patch.object(arbiter, "_call_llm_for_scoring", side_effect=fake):
        arbiter.score_example_sql_teaching_safety(
            question="q", sql="SELECT total FROM main.s.orders",
            w=MagicMock(), metadata_snapshot=snap,
        )
    assert "main.s.orders" in captured["prompt"]
    assert captured["prompt_name"] == "example_sql_teaching_safety"
