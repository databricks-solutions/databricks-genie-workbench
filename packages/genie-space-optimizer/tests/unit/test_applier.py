"""Unit tests for genie_space_optimizer.optimization.applier — pure functions."""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.applier import (
    _apply_action_to_config,
    _validate_example_sql_entry,
    classify_risk,
    render_patch,
    validate_patch_set,
)


class TestClassifyRisk:
    def test_low_risk(self):
        assert classify_risk("add_description") == "low"
        assert classify_risk("add_column_description") == "low"

    def test_medium_risk(self):
        assert classify_risk("update_instruction") == "medium"

    def test_dict_input(self):
        assert classify_risk({"type": "add_description"}) == "low"

    def test_unknown_defaults_to_medium(self):
        assert classify_risk("totally_unknown") == "medium"


class TestRenderPatch:
    def test_add_instruction(self):
        patch = {"type": "add_instruction", "new_text": "Always use UTC."}
        result = render_patch(patch, "space-1", {})
        assert result["action_type"] == "add_instruction"
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "instructions"
        assert cmd["new_text"] == "Always use UTC."

        rollback = json.loads(result["rollback_command"])
        assert rollback["op"] == "remove"

    def test_update_instruction(self):
        patch = {"type": "update_instruction", "old_text": "old", "new_text": "new"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "update"
        assert cmd["old_text"] == "old"
        assert cmd["new_text"] == "new"

    def test_add_description(self):
        patch = {"type": "add_description", "target": "orders", "new_text": "Order data table"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "descriptions"

    def test_hide_column(self):
        patch = {"type": "hide_column", "table": "orders", "column": "internal_id"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["visible"] is False

    def test_add_join_spec_api_format(self):
        join_spec = {
            "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
            "right": {"identifier": "catalog.schema.customers", "alias": "customers"},
            "sql": ["`orders`.`customer_id` = `customers`.`customer_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
        }
        patch = {"type": "add_join_spec", "join_spec": join_spec}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "join_specs"
        assert cmd["join_spec"]["left"]["identifier"] == "catalog.schema.orders"
        rollback_cmd = json.loads(result["rollback_command"])
        assert rollback_cmd["op"] == "remove"
        assert rollback_cmd["left_table"] == "catalog.schema.orders"
        assert rollback_cmd["right_table"] == "catalog.schema.customers"

    def test_add_join_spec_legacy_format(self):
        join_spec = {"left_table_name": "orders", "right_table_name": "customers"}
        patch = {"type": "add_join_spec", "join_spec": join_spec}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "join_specs"
        rollback_cmd = json.loads(result["rollback_command"])
        assert rollback_cmd["left_table"] == "orders"
        assert rollback_cmd["right_table"] == "customers"

    def test_unknown_type_returns_unknown_op(self):
        patch = {"type": "future_patch_type", "target": "x"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "unknown"

    def test_render_has_risk_level(self):
        patch = {"type": "add_instruction", "new_text": "test"}
        result = render_patch(patch, "space-1", {})
        assert "risk_level" in result


class TestApplyActionToConfig:
    def _make_config(self):
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "catalog.schema.orders",
                        "description": ["Order data"],
                        "column_configs": [
                            {"column_name": "order_id", "description": ["PK"]},
                            {"column_name": "amount", "description": ["Amount"]},
                        ],
                    }
                ]
            },
            "instructions": {
                "text_instructions": [
                    {"id": "instr_1", "content": ["Filter by active orders."]}
                ],
                "example_question_sqls": [],
            },
        }

    def test_add_instruction(self):
        config = self._make_config()
        action = {
            "command": json.dumps({"op": "add", "section": "instructions", "new_text": "Use UTC."}),
        }
        assert _apply_action_to_config(config, action) is True
        instructions = config["instructions"]["text_instructions"][0]["content"]
        assert any("UTC" in line for line in instructions)

    def test_update_instruction_with_guard(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "update",
                "section": "instructions",
                "old_text": "Filter by active orders.",
                "new_text": "Filter by status = active.",
            }),
        }
        assert _apply_action_to_config(config, action) is True

    def test_update_instruction_guard_fails(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "update",
                "section": "instructions",
                "old_text": "This text does not exist",
                "new_text": "replacement",
            }),
        }
        assert _apply_action_to_config(config, action) is False

    def test_add_column_description(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "add",
                "section": "column_configs",
                "table": "catalog.schema.orders",
                "column": "amount",
                "value": "Monetary value in USD",
            }),
        }
        assert _apply_action_to_config(config, action) is True

    def test_column_config_table_not_found(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "add",
                "section": "column_configs",
                "table": "nonexistent_table",
                "column": "col",
                "value": "desc",
            }),
        }
        assert _apply_action_to_config(config, action) is False

    def test_add_join_spec_api_format(self):
        config = self._make_config()
        js = {
            "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
            "right": {"identifier": "catalog.schema.items", "alias": "items"},
            "sql": ["`orders`.`item_id` = `items`.`item_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
        }
        action = {
            "command": json.dumps({"op": "add", "section": "join_specs", "join_spec": js}),
        }
        assert _apply_action_to_config(config, action) is True
        assert len(config["instructions"]["join_specs"]) == 1
        assert config["instructions"]["join_specs"][0]["left"]["identifier"] == "catalog.schema.orders"

    def test_remove_join_spec_api_format(self):
        config = self._make_config()
        js = {
            "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
            "right": {"identifier": "catalog.schema.items", "alias": "items"},
            "sql": ["`orders`.`item_id` = `items`.`item_id`"],
        }
        config["instructions"]["join_specs"] = [js]
        action = {
            "command": json.dumps({
                "op": "remove", "section": "join_specs",
                "left_table": "catalog.schema.orders",
                "right_table": "catalog.schema.items",
            }),
        }
        assert _apply_action_to_config(config, action) is True
        assert len(config["instructions"]["join_specs"]) == 0

    def test_update_join_spec_api_format(self):
        config = self._make_config()
        old_js = {
            "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
            "right": {"identifier": "catalog.schema.items", "alias": "items"},
            "sql": ["`orders`.`old_key` = `items`.`old_key`"],
        }
        config["instructions"]["join_specs"] = [old_js]
        new_js = {
            "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
            "right": {"identifier": "catalog.schema.items", "alias": "items"},
            "sql": ["`orders`.`item_id` = `items`.`item_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
        }
        action = {
            "command": json.dumps({
                "op": "update", "section": "join_specs",
                "left_table": "catalog.schema.orders",
                "right_table": "catalog.schema.items",
                "join_spec": new_js,
            }),
        }
        assert _apply_action_to_config(config, action) is True
        assert config["instructions"]["join_specs"][0]["sql"][-1] == "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"

    def test_add_join_spec_legacy_format(self):
        config = self._make_config()
        js = {"left_table_name": "orders", "right_table_name": "items", "join_type": "inner"}
        action = {
            "command": json.dumps({"op": "add", "section": "join_specs", "join_spec": js}),
        }
        assert _apply_action_to_config(config, action) is True
        assert len(config["instructions"]["join_specs"]) == 1

    def test_invalid_json_command(self):
        config = self._make_config()
        action = {"command": "not valid json"}
        assert _apply_action_to_config(config, action) is False


class TestValidatePatchSet:
    def test_delegates_to_optimizer(self):
        valid, errors = validate_patch_set([], {})
        assert valid is True
        assert errors == []


# ═══════════════════════════════════════════════════════════════════════
# Prompt Matching Auto-Config Tests
# ═══════════════════════════════════════════════════════════════════════

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.applier import (
    _entity_matching_score,
    _is_hidden,
    _is_measure_column,
    auto_apply_prompt_matching,
)


class TestIsMeasureColumn:
    def test_numeric_type_is_measure(self):
        assert _is_measure_column("revenue", "DOUBLE") is True
        assert _is_measure_column("count", "INT") is True
        assert _is_measure_column("total", "BIGINT") is True
        assert _is_measure_column("amount", "DECIMAL(10,2)") is True

    def test_string_type_is_not_measure(self):
        assert _is_measure_column("industry", "STRING") is False
        assert _is_measure_column("state", "STRING") is False

    def test_measure_name_prefix(self):
        assert _is_measure_column("avg_days_of_supply", "STRING") is True
        assert _is_measure_column("sum_revenue", "STRING") is True
        assert _is_measure_column("count_orders", "STRING") is True
        assert _is_measure_column("total_amount", "STRING") is True
        assert _is_measure_column("pct_growth", "STRING") is True

    def test_non_measure_name(self):
        assert _is_measure_column("state_code", "") is False
        assert _is_measure_column("category", "") is False


class TestIsHidden:
    def test_visible_false(self):
        assert _is_hidden({"visible": False}) is True

    def test_exclude_true(self):
        assert _is_hidden({"exclude": True}) is True

    def test_normal_column(self):
        assert _is_hidden({"column_name": "x"}) is False
        assert _is_hidden({}) is False


class TestEntityMatchingScore:
    def test_categorical_column_high_score(self):
        assert _entity_matching_score("industry") == 2
        assert _entity_matching_score("status_code") == 2
        assert _entity_matching_score("country") == 2

    def test_free_text_column_low_score(self):
        assert _entity_matching_score("description") == 0
        assert _entity_matching_score("email_address") == 0
        assert _entity_matching_score("user_notes") == 0

    def test_generic_column_medium_score(self):
        assert _entity_matching_score("order_id") == 1
        assert _entity_matching_score("created_at") == 1


class TestAutoApplyPromptMatching:
    def _make_config(
        self,
        tables=None,
        metric_views=None,
        uc_columns=None,
    ):
        tables = tables or []
        metric_views = metric_views or []
        parsed = {
            "data_sources": {
                "tables": tables,
                "metric_views": metric_views,
            },
        }
        return {
            "_parsed_space": parsed,
            "_uc_columns": uc_columns or [],
        }

    @patch("genie_space_optimizer.optimization.applier.patch_space_config")
    def test_enables_format_assistance_on_all_visible_columns(self, mock_patch):
        config = self._make_config(
            tables=[{
                "identifier": "cat.sch.orders",
                "column_configs": [
                    {"column_name": "order_id"},
                    {"column_name": "status"},
                    {"column_name": "amount", "enable_format_assistance": True},
                ],
            }],
            uc_columns=[
                {"table_name": "orders", "column_name": "order_id", "data_type": "BIGINT"},
                {"table_name": "orders", "column_name": "status", "data_type": "STRING"},
                {"table_name": "orders", "column_name": "amount", "data_type": "DOUBLE"},
            ],
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        fa_changes = [c for c in result["applied"] if c["type"] == "enable_example_values"]
        assert len(fa_changes) == 2
        assert result["format_assistance_count"] == 2

    @patch("genie_space_optimizer.optimization.applier.patch_space_config")
    def test_entity_matching_only_string_columns(self, mock_patch):
        config = self._make_config(
            tables=[{
                "identifier": "cat.sch.accounts",
                "column_configs": [
                    {"column_name": "industry"},
                    {"column_name": "revenue"},
                ],
            }],
            uc_columns=[
                {"table_name": "accounts", "column_name": "industry", "data_type": "STRING"},
                {"table_name": "accounts", "column_name": "revenue", "data_type": "DOUBLE"},
            ],
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        em_changes = [c for c in result["applied"] if c["type"] == "enable_value_dictionary"]
        assert len(em_changes) == 1
        assert em_changes[0]["column"] == "industry"

    @patch("genie_space_optimizer.optimization.applier.patch_space_config")
    def test_entity_matching_respects_120_cap(self, mock_patch):
        cols = [{"column_name": f"col_{i}"} for i in range(130)]
        uc = [{"table_name": "big", "column_name": f"col_{i}", "data_type": "STRING"} for i in range(130)]
        config = self._make_config(
            tables=[{"identifier": "cat.sch.big", "column_configs": cols}],
            uc_columns=uc,
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        em_count = result["entity_matching_count"]
        assert em_count <= 120

    @patch("genie_space_optimizer.optimization.applier.patch_space_config")
    def test_skips_hidden_columns(self, mock_patch):
        config = self._make_config(
            tables=[{
                "identifier": "cat.sch.t",
                "column_configs": [
                    {"column_name": "visible_col"},
                    {"column_name": "hidden_col", "visible": False},
                    {"column_name": "excluded_col", "exclude": True},
                ],
            }],
            uc_columns=[
                {"table_name": "t", "column_name": "visible_col", "data_type": "STRING"},
                {"table_name": "t", "column_name": "hidden_col", "data_type": "STRING"},
                {"table_name": "t", "column_name": "excluded_col", "data_type": "STRING"},
            ],
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        changed_cols = {c["column"] for c in result["applied"]}
        assert "visible_col" in changed_cols
        assert "hidden_col" not in changed_cols
        assert "excluded_col" not in changed_cols

    @patch("genie_space_optimizer.optimization.applier.patch_space_config")
    def test_metric_view_skips_measure_columns(self, mock_patch):
        config = self._make_config(
            metric_views=[{
                "identifier": "cat.sch.mv_health",
                "column_configs": [
                    {"column_name": "region"},
                    {"column_name": "avg_days_of_supply"},
                    {"column_name": "total_revenue"},
                ],
            }],
            uc_columns=[
                {"table_name": "mv_health", "column_name": "region", "data_type": "STRING"},
                {"table_name": "mv_health", "column_name": "avg_days_of_supply", "data_type": "DOUBLE"},
                {"table_name": "mv_health", "column_name": "total_revenue", "data_type": "DOUBLE"},
            ],
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        changed_cols = {c["column"] for c in result["applied"]}
        assert "region" in changed_cols
        assert "avg_days_of_supply" not in changed_cols
        assert "total_revenue" not in changed_cols

    def test_idempotent_no_changes(self):
        config = self._make_config(
            tables=[{
                "identifier": "cat.sch.t",
                "column_configs": [
                    {"column_name": "c1", "enable_format_assistance": True, "enable_entity_matching": True},
                ],
            }],
            uc_columns=[
                {"table_name": "t", "column_name": "c1", "data_type": "STRING"},
            ],
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        assert result["applied"] == []
        assert result["format_assistance_count"] == 0
        assert result["entity_matching_count"] == 0

    @patch("genie_space_optimizer.optimization.applier.patch_space_config")
    def test_prioritizes_categorical_over_generic(self, mock_patch):
        cols = [
            {"column_name": "industry"},
            {"column_name": "random_string_col"},
            {"column_name": "status"},
        ]
        uc = [
            {"table_name": "t", "column_name": "industry", "data_type": "STRING"},
            {"table_name": "t", "column_name": "random_string_col", "data_type": "STRING"},
            {"table_name": "t", "column_name": "status", "data_type": "STRING"},
        ]
        config = self._make_config(
            tables=[{"identifier": "cat.sch.t", "column_configs": cols}],
            uc_columns=uc,
        )
        w = MagicMock()
        result = auto_apply_prompt_matching(w, "space-1", config)
        em_changes = [c for c in result["applied"] if c["type"] == "enable_value_dictionary"]
        em_cols = [c["column"] for c in em_changes]
        assert "industry" in em_cols
        assert "status" in em_cols
        assert em_cols.index("industry") < em_cols.index("random_string_col")


class TestValidateExampleSqlEntry:
    def test_valid_entry_passes(self):
        entry = {"question": ["What?"], "sql": ["SELECT 1"], "id": "x"}
        assert _validate_example_sql_entry(entry) is True

    def test_invalid_entry_rejected(self):
        entry = {"bad_key": "value"}
        assert _validate_example_sql_entry(entry) is False

    def test_valid_sql_with_known_asset(self):
        entry = {"question": ["Q"], "sql": ["SELECT * FROM dim_property"], "id": "x"}
        config = {"data_sources": {"tables": [{"identifier": "cat.sch.dim_property"}]}}
        assert _validate_example_sql_entry(entry, config=config) is True

    def test_sql_with_unknown_asset_rejected(self):
        entry = {"question": ["Q"], "sql": ["SELECT * FROM hallucinated_table"], "id": "x"}
        config = {"data_sources": {"tables": [{"identifier": "cat.sch.dim_property"}]}}
        assert _validate_example_sql_entry(entry, config=config) is False

    def test_metric_view_sql_accepted(self):
        entry = {
            "question": ["What is total revenue?"],
            "sql": ["SELECT MEASURE(total_revenue) FROM cat.sch.mv_bookings"],
            "id": "x",
        }
        config = {
            "data_sources": {
                "tables": [{"identifier": "cat.sch.dim_property"}],
                "metric_views": [{"identifier": "cat.sch.mv_bookings"}],
            }
        }
        assert _validate_example_sql_entry(entry, config=config) is True

    def test_metric_view_fqn_accepted(self):
        entry = {
            "question": ["Revenue by month?"],
            "sql": ["SELECT MEASURE(revenue) FROM prashanth_catalog.my_schema.mv_sales"],
            "id": "x",
        }
        config = {
            "data_sources": {
                "tables": [],
                "metric_views": [{"identifier": "prashanth_catalog.my_schema.mv_sales"}],
            }
        }
        assert _validate_example_sql_entry(entry, config=config) is True

    def test_no_config_skips_asset_check(self):
        entry = {"question": ["Q"], "sql": ["SELECT * FROM anything"], "id": "x"}
        assert _validate_example_sql_entry(entry, config=None) is True
