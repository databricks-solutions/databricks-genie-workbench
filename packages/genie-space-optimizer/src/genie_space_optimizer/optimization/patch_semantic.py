"""PatchSemantic: typed semantic class declared at the patch producer.

Replaces the substring classifier in terminal_signature.resolve_emitted_patch_shape
that misclassified add_sql_snippet_* patches as ABSENT. New patch_types are added
to PATCH_TYPE_SEMANTICS at the constructor; gates read .semantic_class directly.
"""
from __future__ import annotations

from enum import StrEnum


class PatchSemantic(StrEnum):
    STRUCTURAL = "structural"
    METADATA = "metadata"
    INSTRUCTION = "instruction"


PATCH_TYPE_SEMANTICS: dict[str, PatchSemantic] = {
    # Lever 5 — example SQL teachers.
    "add_example_sql": PatchSemantic.STRUCTURAL,
    "add_example_sql_negative": PatchSemantic.STRUCTURAL,
    "update_example_sql": PatchSemantic.STRUCTURAL,
    "remove_example_sql": PatchSemantic.STRUCTURAL,
    # Lever 6 — SQL snippet expressions and filters.
    "add_sql_snippet_filter": PatchSemantic.STRUCTURAL,
    "add_sql_snippet_expression": PatchSemantic.STRUCTURAL,
    "add_sql_snippet_measure": PatchSemantic.STRUCTURAL,
    "narrow_l6_filter": PatchSemantic.STRUCTURAL,
    "narrow_l6_expression": PatchSemantic.STRUCTURAL,
    # Lever 4 — join/routing/grain teachers.
    "add_join_rule": PatchSemantic.STRUCTURAL,
    "add_join_spec": PatchSemantic.STRUCTURAL,
    "update_join_spec": PatchSemantic.STRUCTURAL,
    "remove_join_spec": PatchSemantic.STRUCTURAL,
    "add_routing_rule": PatchSemantic.STRUCTURAL,
    "add_grain_rule": PatchSemantic.STRUCTURAL,
    "add_sql_pattern": PatchSemantic.STRUCTURAL,
    # Metric views (structural — they change SQL shape).
    "add_metric_view": PatchSemantic.STRUCTURAL,
    "add_mv_dimension": PatchSemantic.STRUCTURAL,
    "remove_mv_dimension": PatchSemantic.STRUCTURAL,
    "add_mv_measure": PatchSemantic.STRUCTURAL,
    "remove_mv_measure": PatchSemantic.STRUCTURAL,
    "update_mv_measure": PatchSemantic.STRUCTURAL,
    "update_mv_yaml": PatchSemantic.STRUCTURAL,
    # Filters (structural — they restrict SQL output).
    "add_default_filter": PatchSemantic.STRUCTURAL,
    "remove_default_filter": PatchSemantic.STRUCTURAL,
    "update_filter_condition": PatchSemantic.STRUCTURAL,
    # Tables (structural — schema changes).
    "add_table": PatchSemantic.STRUCTURAL,
    "remove_table": PatchSemantic.STRUCTURAL,
    # TVFs (structural — SQL templates).
    "add_tvf": PatchSemantic.STRUCTURAL,
    "remove_tvf": PatchSemantic.STRUCTURAL,
    "add_tvf_parameter": PatchSemantic.STRUCTURAL,
    "remove_tvf_parameter": PatchSemantic.STRUCTURAL,
    "update_tvf_sql": PatchSemantic.STRUCTURAL,
    # Lever 1 — metadata edits.
    "update_column_description": PatchSemantic.METADATA,
    "add_column_description": PatchSemantic.METADATA,
    "add_column_synonym": PatchSemantic.METADATA,
    "remove_column_synonym": PatchSemantic.METADATA,
    "update_table_description": PatchSemantic.METADATA,
    "add_description": PatchSemantic.METADATA,
    "update_description": PatchSemantic.METADATA,
    "add_tvf_description": PatchSemantic.METADATA,
    "rename_column_alias": PatchSemantic.METADATA,
    "hide_column": PatchSemantic.METADATA,
    "unhide_column": PatchSemantic.METADATA,
    # Genie feature toggles — metadata-only.
    "enable_example_values": PatchSemantic.METADATA,
    "disable_example_values": PatchSemantic.METADATA,
    "enable_value_dictionary": PatchSemantic.METADATA,
    "disable_value_dictionary": PatchSemantic.METADATA,
    # Lever 3 — instruction edits.
    "add_instruction": PatchSemantic.INSTRUCTION,
    "remove_instruction": PatchSemantic.INSTRUCTION,
    "update_instruction": PatchSemantic.INSTRUCTION,
    "update_instruction_section": PatchSemantic.INSTRUCTION,
    "rewrite_instruction": PatchSemantic.INSTRUCTION,
    "add_metric_view_instruction": PatchSemantic.INSTRUCTION,
    "add_table_instruction": PatchSemantic.INSTRUCTION,
    "add_space_instruction": PatchSemantic.INSTRUCTION,
}


def semantic_for_patch_type(patch_type: str) -> PatchSemantic:
    """Return the declared PatchSemantic for ``patch_type``.

    Raises KeyError on unknown types. Callers that add a new patch type
    must register it in PATCH_TYPE_SEMANTICS in this file. The KeyError
    is intentional: silent classification was the antipattern this
    replaces.
    """
    return PATCH_TYPE_SEMANTICS[str(patch_type).strip()]
