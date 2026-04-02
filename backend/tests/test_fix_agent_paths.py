"""Tests for config path navigation and ID sanitization (backend/services/fix_agent.py).

Tests _get_value_at_path(), _set_value_at_path(), and _sanitize_ids — pure
functions, no mocking required.
"""

import re

import pytest

from backend.services.fix_agent import _get_value_at_path, _set_value_at_path, _sanitize_ids


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


# ---------------------------------------------------------------------------
# _sanitize_ids
# ---------------------------------------------------------------------------

HEX32 = re.compile(r"^[0-9a-f]{32}$")


class TestSanitizeIds:
    def test_valid_id_unchanged(self):
        config = {"id": "a1b2c3d4e5f60000000000000000000a", "name": "test"}
        _sanitize_ids(config)
        assert config["id"] == "a1b2c3d4e5f60000000000000000000a"

    def test_invalid_hex_chars_replaced(self):
        bad_id = "2e41b27gb76746g438d3d00ff5e27fa03"  # contains 'g'
        config = {"id": bad_id, "name": "test"}
        _sanitize_ids(config)
        assert config["id"] != bad_id
        assert HEX32.match(config["id"])

    def test_hyphenated_uuid_replaced(self):
        config = {"id": "2e41b27b-b767-46e4-38d3-d00ff5e27fa0"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_too_short_id_replaced(self):
        config = {"id": "abc123"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_nested_ids_fixed(self):
        config = {
            "benchmarks": {
                "questions": [
                    {"id": "INVALID_NOT_HEX_AT_ALL_32CHARS!!", "question": ["Q1"]},
                    {"id": "a1b2c3d4e5f60000000000000000000b", "question": ["Q2"]},
                ]
            }
        }
        _sanitize_ids(config)
        assert HEX32.match(config["benchmarks"]["questions"][0]["id"])
        # Valid ID should be unchanged
        assert config["benchmarks"]["questions"][1]["id"] == "a1b2c3d4e5f60000000000000000000b"

    def test_non_id_fields_untouched(self):
        config = {"name": "NOT_HEX", "identifier": "catalog.schema.table"}
        _sanitize_ids(config)
        assert config["name"] == "NOT_HEX"
        assert config["identifier"] == "catalog.schema.table"

    def test_none_id_replaced(self):
        config = {"id": None, "name": "test"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_empty_string_id_replaced(self):
        config = {"id": "", "name": "test"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_missing_id_injected_in_known_array(self):
        config = {
            "instructions": {
                "example_question_sqls": [
                    {"question": ["What is X?"], "sql": ["SELECT 1"]},
                ]
            }
        }
        _sanitize_ids(config)
        entry = config["instructions"]["example_question_sqls"][0]
        assert "id" in entry
        assert HEX32.match(entry["id"])

    def test_missing_id_not_injected_in_unknown_array(self):
        config = {
            "some_custom_field": [
                {"name": "test"},
            ]
        }
        _sanitize_ids(config)
        assert "id" not in config["some_custom_field"][0]
