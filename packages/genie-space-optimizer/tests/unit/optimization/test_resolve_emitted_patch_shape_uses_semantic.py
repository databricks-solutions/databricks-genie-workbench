from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    resolve_emitted_patch_shape,
)


def test_resolve_admits_add_sql_snippet_filter_as_structural():
    patches = [{"patch_type": "add_sql_snippet_filter"}]
    assert resolve_emitted_patch_shape(patches) is EmittedPatchShape.STRUCTURAL


def test_resolve_admits_add_sql_snippet_expression_as_structural():
    patches = [{"patch_type": "add_sql_snippet_expression"}]
    assert resolve_emitted_patch_shape(patches) is EmittedPatchShape.STRUCTURAL


def test_resolve_admits_add_example_sql_as_structural():
    patches = [{"patch_type": "add_example_sql"}]
    assert resolve_emitted_patch_shape(patches) is EmittedPatchShape.STRUCTURAL


def test_resolve_returns_metadata_for_column_description():
    patches = [{"patch_type": "update_column_description"}]
    assert resolve_emitted_patch_shape(patches) is EmittedPatchShape.METADATA


def test_resolve_returns_instruction_for_space_instruction():
    patches = [{"patch_type": "add_space_instruction"}]
    assert resolve_emitted_patch_shape(patches) is EmittedPatchShape.INSTRUCTION


def test_resolve_returns_absent_for_empty_input():
    assert resolve_emitted_patch_shape([]) is EmittedPatchShape.ABSENT


def test_resolve_returns_structural_when_mixed_with_metadata():
    # STRUCTURAL > METADATA > INSTRUCTION > ABSENT precedence.
    patches = [
        {"patch_type": "update_column_description"},
        {"patch_type": "add_sql_snippet_filter"},
    ]
    assert resolve_emitted_patch_shape(patches) is EmittedPatchShape.STRUCTURAL
