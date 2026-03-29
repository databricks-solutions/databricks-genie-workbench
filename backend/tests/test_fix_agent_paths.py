"""Tests for config path navigation (backend/services/fix_agent.py).

Tests _get_value_at_path() and _set_value_at_path() — pure dict traversal
functions, no mocking required.
"""

import pytest

from backend.services.fix_agent import _get_value_at_path, _set_value_at_path


# ---------------------------------------------------------------------------
# Sample config fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "name": "My Space",
        "data_sources": {
            "tables": [
                {"name": "orders", "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "amount", "type": "decimal"},
                ]},
                {"name": "customers", "columns": [
                    {"name": "id", "type": "bigint"},
                ]},
            ]
        },
        "instructions": {
            "sql_snippets": {
                "filters": [
                    {"id": "f0", "sql": ["WHERE x=1"]},
                    {"id": "f1", "sql": ["WHERE y=2"]},
                    {"id": "f2", "sql": ["WHERE z=3", "AND w=4"]},
                ]
            }
        }
    }


# ---------------------------------------------------------------------------
# _get_value_at_path
# ---------------------------------------------------------------------------

class TestGetValueAtPath:
    def test_top_level_key(self, config):
        assert _get_value_at_path(config, "name") == "My Space"

    def test_nested_key(self, config):
        tables = _get_value_at_path(config, "data_sources.tables")
        assert isinstance(tables, list)
        assert len(tables) == 2

    def test_array_index(self, config):
        name = _get_value_at_path(config, "data_sources.tables[0].name")
        assert name == "orders"

    def test_second_array_index(self, config):
        name = _get_value_at_path(config, "data_sources.tables[1].name")
        assert name == "customers"

    def test_deep_nesting(self, config):
        val = _get_value_at_path(config, "instructions.sql_snippets.filters[2].sql[0]")
        assert val == "WHERE z=3"

    def test_missing_key_returns_none(self, config):
        assert _get_value_at_path(config, "nonexistent") is None

    def test_missing_nested_key_returns_none(self, config):
        assert _get_value_at_path(config, "data_sources.missing.deep") is None

    def test_out_of_bounds_index_returns_none(self, config):
        assert _get_value_at_path(config, "data_sources.tables[99]") is None


# ---------------------------------------------------------------------------
# _set_value_at_path
# ---------------------------------------------------------------------------

class TestSetValueAtPath:
    def test_set_existing_key(self, config):
        _set_value_at_path(config, "name", "New Name")
        assert config["name"] == "New Name"

    def test_set_nested_existing_key(self, config):
        _set_value_at_path(config, "data_sources.tables[0].name", "renamed")
        assert config["data_sources"]["tables"][0]["name"] == "renamed"

    def test_auto_creates_intermediate_dicts(self):
        config = {}
        _set_value_at_path(config, "a.b.c", "deep")
        assert config["a"]["b"]["c"] == "deep"

    def test_auto_creates_intermediate_lists(self):
        config = {}
        _set_value_at_path(config, "items[0]", "first")
        assert config["items"][0] == "first"

    def test_auto_extends_list(self):
        config = {"items": ["a"]}
        _set_value_at_path(config, "items[2]", "c")
        assert config["items"] == ["a", None, "c"]

    def test_mixed_dict_and_array_creation(self):
        config = {}
        _set_value_at_path(config, "data.items[0].name", "first")
        assert config["data"]["items"][0]["name"] == "first"

    def test_mutates_in_place(self, config):
        original_id = id(config)
        _set_value_at_path(config, "name", "changed")
        assert id(config) == original_id

    def test_set_deep_sql_value(self, config):
        _set_value_at_path(config, "instructions.sql_snippets.filters[1].sql[0]", "WHERE new=1")
        assert config["instructions"]["sql_snippets"]["filters"][1]["sql"][0] == "WHERE new=1"
