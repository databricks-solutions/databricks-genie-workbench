"""Example-derived joins require corroboration: UC FK or both_correct
baseline join pair."""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.harness import (
    _mine_and_apply_joins_from_example_sqls,
)


def _make_example(question, sql):
    return {"question": question, "expected_sql": sql}


def _snapshot_with_fk(left, right):
    return {
        "_asset_semantics": {
            left: {"asset_type": "table"},
            right: {"asset_type": "table"},
        },
        "_uc_foreign_keys": [
            {"left_table": left, "right_table": right,
             "left_columns": ["fk"], "right_columns": ["pk"]},
        ],
        "instructions": {"join_specs": []},
    }


def _snapshot_no_fk(left, right):
    snap = _snapshot_with_fk(left, right)
    snap["_uc_foreign_keys"] = []
    return snap


def test_corroborated_join_from_uc_fk_is_applied():
    snap = _snapshot_with_fk("main.s.orders", "main.s.customers")
    examples = [_make_example(
        "orders by customer",
        "SELECT * FROM main.s.orders o JOIN main.s.customers c ON o.cid=c.id",
    )]
    with patch(
        "genie_space_optimizer.optimization.harness._mine_and_apply_proven_joins",
        return_value={"total_applied": 1, "new_specs": [{"left": {}, "right": {}}],
                      "extraction_diagnostics": {}},
    ):
        result = _mine_and_apply_joins_from_example_sqls(
            w=MagicMock(), spark=MagicMock(), run_id="r", space_id="s",
            metadata_snapshot=snap, examples=examples,
            catalog="main", schema="s",
            baseline_both_correct_rows=[],
        )
    assert result["total_applied"] == 1
    assert result["corroboration_source"] in {"uc_fk", "mixed"}


def test_uncorroborated_join_is_dropped():
    snap = _snapshot_no_fk("main.s.orders", "main.s.customers")
    examples = [_make_example(
        "orders by customer",
        "SELECT * FROM main.s.orders o JOIN main.s.customers c ON o.cid=c.id",
    )]
    result = _mine_and_apply_joins_from_example_sqls(
        w=MagicMock(), spark=MagicMock(), run_id="r", space_id="s",
        metadata_snapshot=snap, examples=examples,
        catalog="main", schema="s",
        baseline_both_correct_rows=[],
    )
    assert result["total_applied"] == 0
    assert result["dropped_uncorroborated"] >= 1


def test_corroboration_via_both_correct_baseline_join():
    snap = _snapshot_no_fk("main.s.orders", "main.s.customers")
    baseline_rows = [{
        "arbiter/value": "both_correct",
        "request": {"question": "any",
                    "expected_sql": ("SELECT * FROM main.s.orders o "
                                     "JOIN main.s.customers c ON o.cid=c.id")},
        "response": {"response": ""},
    }]
    examples = [_make_example(
        "different question entirely",
        "SELECT * FROM main.s.orders o JOIN main.s.customers c ON o.cid=c.id",
    )]
    with patch(
        "genie_space_optimizer.optimization.harness._mine_and_apply_proven_joins",
        return_value={"total_applied": 1, "new_specs": [{"left": {}, "right": {}}],
                      "extraction_diagnostics": {}},
    ):
        result = _mine_and_apply_joins_from_example_sqls(
            w=MagicMock(), spark=MagicMock(), run_id="r", space_id="s",
            metadata_snapshot=snap, examples=examples,
            catalog="main", schema="s",
            baseline_both_correct_rows=baseline_rows,
        )
    assert result["total_applied"] == 1
    assert result["corroboration_source"] in {"baseline_both_correct", "mixed"}
