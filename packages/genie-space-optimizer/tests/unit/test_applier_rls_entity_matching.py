"""Regression tests for RLS-aware entity matching in auto_apply_prompt_matching.

Guards against the latent bug where ``enable_entity_matching`` was set on
columns of tables governed by row-level security or column masks. In those
cases Genie silently disables the value dictionary, wasting one of the 120
per-space slots.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.applier import (
    _column_has_rls,
    _table_has_rls,
    auto_apply_prompt_matching,
)


class TestTableHasRls:
    def test_no_rls(self):
        assert _table_has_rls({"column_configs": [], "columns": []}) is False

    def test_table_level_row_filter(self):
        assert _table_has_rls({"row_filter": "region = 'AMER'"}) is True

    def test_table_level_column_mask(self):
        assert _table_has_rls({"column_mask": {"name": "mask_ssn"}}) is True

    def test_column_level_row_filter_via_column_configs(self):
        assert _table_has_rls(
            {"column_configs": [{"column_name": "x", "row_filter": "f"}]}
        ) is True

    def test_column_level_column_mask_via_columns(self):
        assert _table_has_rls(
            {"columns": [{"name": "ssn", "column_mask": "mask_ssn"}]}
        ) is True


class TestColumnHasRls:
    def test_plain_column(self):
        assert _column_has_rls({"column_name": "order_id"}) is False

    def test_column_mask(self):
        assert _column_has_rls({"column_name": "ssn", "column_mask": "mask_ssn"}) is True

    def test_row_filter_on_column(self):
        assert _column_has_rls({"column_name": "region", "row_filter": "f"}) is True


def _space_with_string_col(
    identifier: str = "main.analytics.orders",
    col_name: str = "region",
    *,
    table_rls: bool = False,
    column_rls: bool = False,
) -> dict:
    """Build a minimal parsed-space dict with one STRING column eligible for entity matching."""
    tbl: dict = {
        "identifier": identifier,
        "column_configs": [
            {
                "column_name": col_name,
                **({"column_mask": "mask_region"} if column_rls else {}),
            }
        ],
    }
    if table_rls:
        tbl["row_filter"] = "region = current_user()"
    return {
        "_parsed_space": {
            "data_sources": {"tables": [tbl], "metric_views": []},
            "instructions": {},
            "benchmarks": {},
        },
        "_uc_columns": [
            {
                "table_name": identifier.rsplit(".", 1)[-1],
                "column_name": col_name,
                "data_type": "STRING",
            }
        ],
    }


@pytest.fixture
def _stub_outputs():
    """Stub the side-effecting calls inside auto_apply_prompt_matching."""
    with patch(
        "genie_space_optimizer.optimization.applier.patch_space_config"
    ) as patch_space, patch(
        "genie_space_optimizer.optimization.applier.sort_genie_config"
    ), patch(
        "genie_space_optimizer.optimization.applier._enforce_instruction_limit"
    ):
        yield patch_space


class TestAutoApplyEntityMatchingRlsRegression:
    def test_plain_table_enables_entity_matching(self, _stub_outputs):
        config = _space_with_string_col()
        auto_apply_prompt_matching(MagicMock(), "space-1", config)
        cc = config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"][0]
        assert cc.get("enable_entity_matching") is True
        assert cc.get("enable_format_assistance") is True

    def test_table_with_rls_skips_entity_matching(self, _stub_outputs):
        config = _space_with_string_col(table_rls=True)
        auto_apply_prompt_matching(MagicMock(), "space-1", config)
        cc = config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"][0]
        # Format assistance is still safe; entity matching must be skipped.
        assert cc.get("enable_entity_matching") is not True
        assert cc.get("enable_format_assistance") is True

    def test_column_with_mask_skips_entity_matching(self, _stub_outputs):
        config = _space_with_string_col(column_rls=True)
        auto_apply_prompt_matching(MagicMock(), "space-1", config)
        cc = config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"][0]
        assert cc.get("enable_entity_matching") is not True

    def test_metric_view_with_rls_skips_entity_matching(self, _stub_outputs):
        config = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [],
                    "metric_views": [
                        {
                            "identifier": "main.analytics.orders_mv",
                            "row_filter": "region = current_user()",
                            "column_configs": [{"column_name": "category"}],
                        }
                    ],
                },
                "instructions": {},
                "benchmarks": {},
            },
            "_uc_columns": [
                {"table_name": "orders_mv", "column_name": "category", "data_type": "STRING"}
            ],
        }
        auto_apply_prompt_matching(MagicMock(), "space-1", config)
        cc = config["_parsed_space"]["data_sources"]["metric_views"][0]["column_configs"][0]
        assert cc.get("enable_entity_matching") is not True

    def test_mixed_rls_and_plain_only_skips_rls(self, _stub_outputs):
        config = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": "main.a.rls_tbl",
                            "row_filter": "x = 1",
                            "column_configs": [{"column_name": "region"}],
                        },
                        {
                            "identifier": "main.a.plain_tbl",
                            "column_configs": [{"column_name": "category"}],
                        },
                    ],
                    "metric_views": [],
                },
                "instructions": {},
                "benchmarks": {},
            },
            "_uc_columns": [
                {"table_name": "rls_tbl", "column_name": "region", "data_type": "STRING"},
                {"table_name": "plain_tbl", "column_name": "category", "data_type": "STRING"},
            ],
        }
        auto_apply_prompt_matching(MagicMock(), "space-1", config)
        tables = config["_parsed_space"]["data_sources"]["tables"]
        rls_cc = tables[0]["column_configs"][0]
        plain_cc = tables[1]["column_configs"][0]
        assert rls_cc.get("enable_entity_matching") is not True
        assert plain_cc.get("enable_entity_matching") is True
