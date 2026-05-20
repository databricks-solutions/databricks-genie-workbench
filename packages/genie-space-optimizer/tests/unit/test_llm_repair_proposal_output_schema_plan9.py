"""Plan 9 Task 1 — extend LlmRepairProposalOutput Pydantic schema with
target_objects. Verifies strict-mode JSON Schema accepts the new
field, defaults to empty list for backward compatibility, and rejects
unknown asset_kind values.
"""
import pytest
from pydantic import ValidationError

from genie_space_optimizer.skills.repair_intent_synthesis.output_schema import (
    LlmRepairProposalOutput,
)


def test_llm_output_accepts_target_objects():
    out = LlmRepairProposalOutput(
        intent_name="top_n_repair",
        intent_description="...",
        repair_shape="top_n_by_metric",
        patch_type="add_example_sql",
        rationale="...",
        confidence="high",
        patch_body={
            "example_question": "Top 5?",
            "example_sql": "SELECT 1",
        },
        blame_set=["catalog.schema.orders"],
        target_objects=[
            {
                "asset_kind": "table",
                "identifier": "catalog.schema.orders",
                "columns": ["product", "amount"],
            },
        ],
    )
    assert out.target_objects[0].asset_kind == "table"
    assert out.target_objects[0].identifier == "catalog.schema.orders"
    assert list(out.target_objects[0].columns) == ["product", "amount"]


def test_llm_output_target_objects_defaults_to_empty_list():
    """For PR1 backward compat: missing target_objects is allowed.
    PR2 (post-catalog deletion) tightens this to require a non-empty
    target_objects when repair_shape != OTHER."""
    out = LlmRepairProposalOutput(
        intent_name="x",
        intent_description="...",
        repair_shape="other",
        patch_type="add_instruction",
        rationale="...",
        confidence="medium",
        patch_body={"instruction_text": "Do X."},
        blame_set=[],
    )
    assert out.target_objects == []


def test_llm_output_rejects_unknown_asset_kind():
    with pytest.raises(ValidationError):
        LlmRepairProposalOutput(
            intent_name="x",
            intent_description="...",
            repair_shape="top_n_by_metric",
            patch_type="add_example_sql",
            rationale="...",
            confidence="high",
            patch_body={
                "example_question": "?",
                "example_sql": "SELECT 1",
            },
            blame_set=[],
            target_objects=[
                {
                    "asset_kind": "view",  # not in LlmTargetObject
                    "identifier": "x.y.z",
                    "columns": [],
                },
            ],
        )
