from genie_space_optimizer.optimization.patch_semantic import (
    PatchSemantic,
    semantic_for_patch_type,
)


def test_patch_semantic_enum_has_three_values():
    assert PatchSemantic.STRUCTURAL == "structural"
    assert PatchSemantic.METADATA == "metadata"
    assert PatchSemantic.INSTRUCTION == "instruction"


def test_semantic_for_patch_type_classifies_sql_snippet_as_structural():
    assert semantic_for_patch_type("add_sql_snippet_filter") is PatchSemantic.STRUCTURAL
    assert semantic_for_patch_type("add_sql_snippet_expression") is PatchSemantic.STRUCTURAL


def test_semantic_for_patch_type_classifies_example_sql_as_structural():
    assert semantic_for_patch_type("add_example_sql") is PatchSemantic.STRUCTURAL


def test_semantic_for_patch_type_classifies_metadata_edits():
    assert semantic_for_patch_type("update_column_description") is PatchSemantic.METADATA
    assert semantic_for_patch_type("add_column_synonym") is PatchSemantic.METADATA


def test_semantic_for_patch_type_classifies_instruction_edits():
    assert semantic_for_patch_type("add_metric_view_instruction") is PatchSemantic.INSTRUCTION
    assert semantic_for_patch_type("add_table_instruction") is PatchSemantic.INSTRUCTION


def test_semantic_for_patch_type_raises_on_unknown_type():
    import pytest
    with pytest.raises(KeyError):
        semantic_for_patch_type("nonexistent_patch_type")
