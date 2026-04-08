"""Tests for Genie Space config normalization (backend/genie_creator.py).

Tests _enforce_constraints(), _clean_config(), _sort_array(), and
_normalize_join_relationships() — pure dict transformation functions.
"""

from backend.genie_creator import (
    _enforce_constraints,
    _clean_config,
    _sort_array,
    _normalize_join_relationships,
    _truncate_oversized_strings,
    _MAX_STRING_CHARS,
    _MAX_TABLES,
)


# ---------------------------------------------------------------------------
# _enforce_constraints
# ---------------------------------------------------------------------------

class TestEnforceConstraints:
    def test_text_instructions_capped_at_1(self):
        config = {
            "instructions": {
                "text_instructions": [
                    {"content": ["first"]},
                    {"content": ["second"]},
                    {"content": ["third"]},
                ],
            },
            "data_sources": {"tables": []},
        }
        result = _enforce_constraints(config)
        assert len(result["instructions"]["text_instructions"]) == 1
        assert result["instructions"]["text_instructions"][0]["content"] == ["first"]

    def test_tables_capped_at_30(self):
        tables = [{"name": f"t{i}", "identifier": f"c.s.t{i}"} for i in range(35)]
        config = {"data_sources": {"tables": tables}, "instructions": {}}
        result = _enforce_constraints(config)
        assert len(result["data_sources"]["tables"]) == _MAX_TABLES

    def test_empty_sql_snippets_removed(self):
        config = {
            "instructions": {
                "sql_snippets": {
                    "filters": [
                        {"id": "f1", "sql": ["WHERE x=1"]},
                        {"id": "f2", "sql": [""]},        # empty string
                        {"id": "f3", "sql": []},           # empty list
                    ],
                    "measures": [
                        {"id": "m1", "sql": ["SUM(x)"]},
                    ],
                    "expressions": [],
                }
            },
            "data_sources": {"tables": []},
        }
        result = _enforce_constraints(config)
        filters = result["instructions"]["sql_snippets"]["filters"]
        assert len(filters) == 1
        assert filters[0]["id"] == "f1"

    def test_config_under_limits_passes_through(self):
        config = {
            "instructions": {
                "text_instructions": [{"content": ["short"]}],
            },
            "data_sources": {"tables": [{"name": "t1"}]},
        }
        result = _enforce_constraints(config)
        assert len(result["instructions"]["text_instructions"]) == 1
        assert len(result["data_sources"]["tables"]) == 1

    def test_does_not_mutate_original(self):
        config = {
            "instructions": {
                "text_instructions": [{"content": ["a"]}, {"content": ["b"]}],
            },
            "data_sources": {"tables": []},
        }
        original_len = len(config["instructions"]["text_instructions"])
        _enforce_constraints(config)
        assert len(config["instructions"]["text_instructions"]) == original_len

    def test_duplicate_column_configs_deduped(self):
        config = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.tbl",
                    "column_configs": [
                        {"column_name": "col_a", "description": ["first"]},
                        {"column_name": "col_a", "description": ["duplicate"]},
                        {"column_name": "col_b", "description": ["unique"]},
                    ],
                }],
            },
            "instructions": {},
        }
        result = _enforce_constraints(config)
        ccs = result["data_sources"]["tables"][0]["column_configs"]
        assert len(ccs) == 2
        assert ccs[0]["column_name"] == "col_a"
        assert ccs[0]["description"] == ["first"]  # keeps first
        assert ccs[1]["column_name"] == "col_b"

    def test_empty_column_name_removed(self):
        config = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.tbl",
                    "column_configs": [
                        {"column_name": "", "description": ["empty"]},
                        {"column_name": "col_a", "description": ["valid"]},
                    ],
                }],
            },
            "instructions": {},
        }
        result = _enforce_constraints(config)
        ccs = result["data_sources"]["tables"][0]["column_configs"]
        assert len(ccs) == 1
        assert ccs[0]["column_name"] == "col_a"

    def test_duplicate_instruction_ids_deduped(self):
        dup_id = "a" * 32
        config = {
            "instructions": {
                "text_instructions": [{"id": dup_id, "content": ["text"]}],
                "example_question_sqls": [{"id": dup_id, "question": ["Q?"]}],
            },
            "data_sources": {"tables": []},
        }
        result = _enforce_constraints(config)
        assert len(result["instructions"]["text_instructions"]) == 1
        # Duplicate in example_question_sqls should be removed
        assert len(result["instructions"]["example_question_sqls"]) == 0

    def test_join_specs_missing_left_right_removed(self):
        config = {
            "instructions": {
                "join_specs": [
                    {
                        "id": "a" * 32,
                        "left": {"identifier": "c.s.orders", "alias": "orders"},
                        "right": {"identifier": "c.s.customers", "alias": "customers"},
                        "sql": ["`orders`.`id` = `customers`.`id`",
                                "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
                    },
                    {
                        "id": "b" * 32,
                        "sql": ["`a`.`id` = `b`.`id`",
                                "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--"],
                    },
                ],
            },
            "data_sources": {"tables": []},
        }
        result = _enforce_constraints(config)
        join_specs = result["instructions"]["join_specs"]
        assert len(join_specs) == 1
        assert join_specs[0]["id"] == "a" * 32

    def test_join_spec_with_only_left_removed(self):
        config = {
            "instructions": {
                "join_specs": [{
                    "id": "a" * 32,
                    "left": {"identifier": "c.s.orders", "alias": "orders"},
                    "sql": ["`orders`.`id` = `customers`.`id`",
                            "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
                }],
            },
            "data_sources": {"tables": []},
        }
        result = _enforce_constraints(config)
        assert len(result["instructions"]["join_specs"]) == 0

    def test_join_spec_with_only_right_removed(self):
        config = {
            "instructions": {
                "join_specs": [{
                    "id": "a" * 32,
                    "right": {"identifier": "c.s.customers", "alias": "customers"},
                    "sql": ["`orders`.`id` = `customers`.`id`",
                            "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
                }],
            },
            "data_sources": {"tables": []},
        }
        result = _enforce_constraints(config)
        assert len(result["instructions"]["join_specs"]) == 0

    def test_duplicate_question_ids_deduped(self):
        dup_id = "b" * 32
        config = {
            "config": {
                "sample_questions": [
                    {"id": dup_id, "question": ["Q1"]},
                    {"id": "c" * 32, "question": ["Q2"]},
                ],
            },
            "benchmarks": {
                "questions": [{"id": dup_id, "question": ["Q1 again"]}],
            },
            "data_sources": {"tables": []},
            "instructions": {},
        }
        result = _enforce_constraints(config)
        assert len(result["config"]["sample_questions"]) == 2
        # Duplicate in benchmarks should be removed
        assert len(result["benchmarks"]["questions"]) == 0


# ---------------------------------------------------------------------------
# _truncate_oversized_strings
# ---------------------------------------------------------------------------

class TestTruncateOversizedStrings:
    def test_oversized_description_truncated(self):
        config = {"description": "x" * 30_000}
        _truncate_oversized_strings(config)
        assert len(config["description"]) == _MAX_STRING_CHARS

    def test_normal_string_untouched(self):
        config = {"description": "short desc"}
        _truncate_oversized_strings(config)
        assert config["description"] == "short desc"

    def test_non_checked_field_untouched(self):
        """Fields not in _SIZE_CHECKED_FIELDS should not be truncated."""
        config = {"name": "x" * 30_000}
        _truncate_oversized_strings(config)
        assert len(config["name"]) == 30_000

    def test_oversized_in_array(self):
        config = {"content": ["x" * 30_000]}
        _truncate_oversized_strings(config)
        assert len(config["content"][0]) == _MAX_STRING_CHARS

    def test_nested_truncation(self):
        config = {"data_sources": {"tables": [{"description": "x" * 30_000}]}}
        _truncate_oversized_strings(config)
        assert len(config["data_sources"]["tables"][0]["description"]) == _MAX_STRING_CHARS


# ---------------------------------------------------------------------------
# _normalize_join_relationships
# ---------------------------------------------------------------------------

class TestNormalizeJoinRelationships:
    def test_lowercase_to_uppercase(self):
        """The regex [^-]+ captures up to the first hyphen/terminator,
        then uppercases and replaces hyphens in the captured segment."""
        config = {
            "instructions": {
                "join_specs": [{
                    "sql": ["ON a.id = b.id --rt=FROM_RELATIONSHIP_TYPE_many_to_one--"]
                }]
            }
        }
        _normalize_join_relationships(config)
        assert "MANY_TO_ONE" in config["instructions"]["join_specs"][0]["sql"][0]

    def test_already_uppercase_unchanged(self):
        sql = "ON a.id = b.id --rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"
        config = {"instructions": {"join_specs": [{"sql": [sql]}]}}
        _normalize_join_relationships(config)
        assert config["instructions"]["join_specs"][0]["sql"][0] == sql

    def test_no_join_specs_is_noop(self):
        config = {"instructions": {}}
        _normalize_join_relationships(config)  # should not raise

    def test_empty_join_specs_is_noop(self):
        config = {"instructions": {"join_specs": []}}
        _normalize_join_relationships(config)

    def test_non_rt_sql_unchanged(self):
        sql = "ON a.id = b.id"
        config = {"instructions": {"join_specs": [{"sql": [sql]}]}}
        _normalize_join_relationships(config)
        assert config["instructions"]["join_specs"][0]["sql"][0] == sql


# ---------------------------------------------------------------------------
# _sort_array
# ---------------------------------------------------------------------------

class TestSortArray:
    def test_sort_by_single_key(self):
        items = [
            {"identifier": "z.table"},
            {"identifier": "a.table"},
            {"identifier": "m.table"},
        ]
        result = _sort_array(items, ("identifier",))
        assert [i["identifier"] for i in result] == ["a.table", "m.table", "z.table"]

    def test_sort_by_multiple_keys(self):
        items = [
            {"id": "2", "identifier": "b"},
            {"id": "1", "identifier": "a"},
            {"id": "1", "identifier": "c"},
        ]
        result = _sort_array(items, ("id", "identifier"))
        assert [(i["id"], i["identifier"]) for i in result] == [
            ("1", "a"), ("1", "c"), ("2", "b")
        ]

    def test_empty_list(self):
        assert _sort_array([], ("id",)) == []

    def test_non_dict_items_unchanged(self):
        items = ["a", "b", "c"]
        assert _sort_array(items, ("id",)) == ["a", "b", "c"]

    def test_missing_sort_key_uses_empty_string(self):
        items = [{"id": "b"}, {"id": "a"}, {}]
        result = _sort_array(items, ("id",))
        assert result[0] == {}  # empty string sorts first
        assert result[1]["id"] == "a"


# ---------------------------------------------------------------------------
# _clean_config
# ---------------------------------------------------------------------------

class TestCleanConfig:
    def test_removes_none_from_arrays(self):
        result = _clean_config([1, None, 2, None, 3])
        assert result == [1, 2, 3]

    def test_string_to_array_conversion(self):
        result = _clean_config("my description", key="description")
        assert result == ["my description"]

    def test_string_array_field_in_dict(self):
        result = _clean_config({"description": "text"})
        assert result["description"] == ["text"]

    def test_object_to_array_wrapping(self):
        """A dict in an object-array field should be wrapped in a list."""
        obj = {"id": "1", "content": ["text"]}
        result = _clean_config(obj, key="text_instructions")
        assert isinstance(result, list)
        assert result[0]["id"] == "1"

    def test_sorted_by_required_keys(self):
        items = [
            {"identifier": "z.table"},
            {"identifier": "a.table"},
        ]
        result = _clean_config(items, key="tables")
        assert result[0]["identifier"] == "a.table"

    def test_nested_cleaning(self):
        config = {
            "instructions": {
                "text_instructions": [
                    {"content": "should become array"},
                ]
            }
        }
        result = _clean_config(config)
        assert result["instructions"]["text_instructions"][0]["content"] == ["should become array"]

    def test_plain_value_passthrough(self):
        assert _clean_config(42) == 42
        assert _clean_config(True) is True
        assert _clean_config(None) is None
