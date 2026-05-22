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
    # Lever 6 — SQL snippet expressions and filters.
    "add_sql_snippet_filter": PatchSemantic.STRUCTURAL,
    "add_sql_snippet_expression": PatchSemantic.STRUCTURAL,
    "narrow_l6_filter": PatchSemantic.STRUCTURAL,
    "narrow_l6_expression": PatchSemantic.STRUCTURAL,
    # Lever 4 — join/routing/grain teachers.
    "add_join_rule": PatchSemantic.STRUCTURAL,
    "add_routing_rule": PatchSemantic.STRUCTURAL,
    "add_grain_rule": PatchSemantic.STRUCTURAL,
    "add_sql_pattern": PatchSemantic.STRUCTURAL,
    "add_metric_view": PatchSemantic.STRUCTURAL,
    # Lever 1 — metadata edits.
    "update_column_description": PatchSemantic.METADATA,
    "add_column_synonym": PatchSemantic.METADATA,
    "update_table_description": PatchSemantic.METADATA,
    # Lever 3 — instruction edits.
    "add_metric_view_instruction": PatchSemantic.INSTRUCTION,
    "add_table_instruction": PatchSemantic.INSTRUCTION,
    "add_space_instruction": PatchSemantic.INSTRUCTION,
    "rewrite_instruction": PatchSemantic.INSTRUCTION,
}


def semantic_for_patch_type(patch_type: str) -> PatchSemantic:
    """Return the declared PatchSemantic for ``patch_type``.

    Raises KeyError on unknown types. Callers that add a new patch type
    must register it in PATCH_TYPE_SEMANTICS in this file. The KeyError
    is intentional: silent classification was the antipattern this
    replaces.
    """
    return PATCH_TYPE_SEMANTICS[str(patch_type).strip()]
