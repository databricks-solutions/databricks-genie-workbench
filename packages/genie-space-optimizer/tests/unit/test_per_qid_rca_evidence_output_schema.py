"""Plan 3 Task 1 — PerQidRcaEvidenceOutput Pydantic contract."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
)
from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.skills.rca_evidence_extraction.output_schema import (
    PerQidRcaEvidenceOutput,
)


def test_pydantic_class_subclasses_llm_output_contract() -> None:
    assert issubclass(PerQidRcaEvidenceOutput, LLMOutputContract)


def test_required_fields_present_and_typed() -> None:
    fields = PerQidRcaEvidenceOutput.model_fields
    expected = {
        "qid", "observed_failure", "generated_sql_issue",
        "expected_sql_shape", "blame_set", "suggested_repair_family",
        "repair_hint_patch_type", "confidence", "quoted_evidence",
    }
    assert set(fields.keys()) == expected, (
        f"field drift: missing={expected - set(fields.keys())}, "
        f"unexpected={set(fields.keys()) - expected}"
    )


def test_repair_hint_patch_type_is_bound_to_patch_type_enum() -> None:
    """Closed-enum binding: LLM must pick a PatchType value."""
    inst = PerQidRcaEvidenceOutput(
        qid="gs_009",
        observed_failure="Generated SQL returned 1 row instead of top 3.",
        generated_sql_issue="Missing LIMIT 3 and ORDER BY revenue DESC.",
        expected_sql_shape="SELECT product, SUM(revenue) GROUP BY product ORDER BY 2 DESC LIMIT 3",
        blame_set=["sales.fact_sales.revenue"],
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=["judge: 'expected 3 rows, got 1'"],
    )
    assert inst.repair_hint_patch_type is PatchType.ADD_EXAMPLE_SQL


def test_confidence_field_is_literal_high_medium_low() -> None:
    with pytest.raises(ValidationError):
        PerQidRcaEvidenceOutput(
            qid="gs_001",
            observed_failure="x",
            generated_sql_issue="x",
            expected_sql_shape="x",
            blame_set=[],
            suggested_repair_family="x",
            repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
            confidence="very_high",
            quoted_evidence=[],
        )


def test_blame_set_and_quoted_evidence_are_lists_of_str() -> None:
    inst = PerQidRcaEvidenceOutput(
        qid="gs_001",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=["catalog.schema.table.col"],
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="medium",
        quoted_evidence=["judge: 'foo'"],
    )
    assert inst.blame_set == ["catalog.schema.table.col"]
    assert inst.quoted_evidence == ["judge: 'foo'"]


def test_extra_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        PerQidRcaEvidenceOutput(
            qid="gs_001",
            observed_failure="x",
            generated_sql_issue="x",
            expected_sql_shape="x",
            blame_set=[],
            suggested_repair_family="x",
            repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
            confidence="low",
            quoted_evidence=[],
            extra_garbage_field="should_be_rejected",
        )


def test_envelope_response_format_is_databricks_safe() -> None:
    """AbstainableEnvelope[PerQidRcaEvidenceOutput] must build a clean
    response_format (no anyOf/oneOf/$ref/pattern keywords).

    PR-C: assert via structural dict-key walk so we don't
    false-positive on description prose containing "pattern" now that
    descriptions are preserved through the strip.
    """
    from tests._schema_utils import assert_no_forbidden_schema_keys

    EnvCls = AbstainableEnvelope[PerQidRcaEvidenceOutput]
    fmt = build_response_format(EnvCls)
    assert_no_forbidden_schema_keys(fmt)
