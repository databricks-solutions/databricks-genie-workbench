from copy import deepcopy
from dataclasses import replace

import pytest

from backend.services.config_fingerprint import Canonicalizer
from backend.services.version_control.contracts import Comparison, DiffCategory, DiffChange


def test_semantic_diff_categorizes_by_id_and_flags_sql_review() -> None:
    adapter = Canonicalizer()
    space = {
        "data_sources": {"tables": [{
            "identifier": "cat.sch.sales",
            "column_configs": [{"column_name": "amount", "description": "Old"}],
        }]},
        "instructions": {
            "text_instructions": [{"id": "text/1", "content": ["Old"]}],
            "join_specs": [{"id": "join1", "sql": ["sales.id = users.id"]}],
            "sql_snippets": {
                "filters": [{"id": "filter1", "sql": ["amount > 0"]}],
                "expressions": [{"id": "expr1", "sql": ["SUM(amount)"]}],
            },
            "example_question_sqls": [{"id": "sql1", "sql": ["SELECT 1"]}],
        },
        "config": {
            "parameters": [{"id": "param1", "default_value": {"values": ["old"]}}],
            "sample_questions": [{"id": "question1", "question": ["Old?"]}],
            "warehouse_id": "warehouse-old",
        },
        "benchmarks": {"questions": [{
            "id": "bench1", "question": ["Total?"],
            "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
        }]},
    }
    changed = deepcopy(space)
    changed["data_sources"]["tables"][0]["column_configs"][0]["description"] = "New"
    changed["data_sources"]["tables"].insert(0, {"identifier": "cat.sch.new"})
    changed["instructions"]["text_instructions"][0]["content"] = ["New"]
    changed["instructions"]["join_specs"][0]["sql"] = ["sales.user_id = users.id"]
    changed["instructions"]["sql_snippets"]["filters"][0]["sql"] = ["amount > 10"]
    changed["instructions"]["sql_snippets"]["expressions"][0]["sql"] = ["AVG(amount)"]
    changed["instructions"]["example_question_sqls"] = []
    changed["config"]["parameters"][0]["default_value"]["values"] = ["new"]
    changed["config"]["sample_questions"][0]["question"] = ["New?"]
    changed["config"]["warehouse_id"] = "warehouse-new"
    changed["benchmarks"]["questions"][0]["answer"][0]["content"] = ["SELECT 2"]
    left = adapter.observe({"serialized_space": space, "description": "Old"})
    right = adapter.observe({"serialized_space": changed, "description": "New"})
    items = adapter.semantic_diff(left, right)
    assert {item.category for item in items} == set(DiffCategory)
    assert items == sorted(items, key=lambda item: (item.category.value, item.path, item.change.value))
    by_path = {item.path: item for item in items}
    assert by_path["/config/data_sources/sources/cat.sch.new"].change is DiffChange.ADDED
    assert by_path["/config/instructions/example_question_sqls/sql1"].change is DiffChange.REMOVED
    assert by_path["/config/instructions/text_instructions/text~11/content"].before == "Old"
    assert by_path["/config/instructions/text_instructions/text~11/content"].after == "New"
    assert by_path["/config/data_sources/sources/cat.sch.sales/column_configs/amount/description"].category is DiffCategory.COLUMNS
    for item in items:
        assert item.review_required is (item.category in {
            DiffCategory.SQL, DiffCategory.JOINS, DiffCategory.FILTERS, DiffCategory.BENCHMARKS,
        })
    reversed_space = deepcopy(space)
    reversed_space["data_sources"]["tables"].reverse()
    assert adapter.semantic_diff(left, adapter.observe({"serialized_space": reversed_space, "description": "Old"})) == []
    assert adapter.compare(left, right) is Comparison.DIFFERENT
    assert all(item.change in {DiffChange.ADDED, DiffChange.REMOVED, DiffChange.MODIFIED} for item in items)
    incompatible = replace(left, fingerprints=replace(left.fingerprints, canonicalizer_version="vc-c14n/99"))
    with pytest.raises(ValueError, match="canonicalizer"):
        adapter.semantic_diff(left, incompatible)
    assert Canonicalizer(dual_compute_version="vc-c14n/1").semantic_diff(left, incompatible) == []


def test_semantic_diff_retains_unaddressed_order_and_unknown_fields() -> None:
    adapter = Canonicalizer()
    before = {"instructions": {"future": [{"id": "b"}, {"id": "a"}]}}
    after = {"instructions": {"future": [{"id": "a"}, {"id": "b"}]}}
    items = adapter.semantic_diff(adapter.observe(before), adapter.observe(after))
    assert len(items) == 1
    assert items[0].path == "/config/instructions/future"
    assert items[0].review_required


def test_semantic_diff_distinguishes_missing_from_null_and_sql_additions() -> None:
    adapter = Canonicalizer()
    left = adapter.observe({"serialized_space": {"instructions": {}}})
    right = adapter.observe({"serialized_space": {"instructions": {
        "example_question_sqls": [{"id": "new", "sql": ["SELECT 1"]}],
    }}, "description": None})
    items = adapter.semantic_diff(left, right)
    assert next(item for item in items if item.category is DiffCategory.METADATA).change is DiffChange.ADDED
    assert next(item for item in items if item.category is DiffCategory.SQL).review_required
