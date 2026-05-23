"""Plan 4 Task 2 — LlmClusterSetOutput envelope-wrapper."""
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
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.skills.failure_clustering.output_schema import (
    LlmClusterOutput,
    LlmClusterSetOutput,
)


def test_set_output_subclasses_llm_output_contract() -> None:
    assert issubclass(LlmClusterSetOutput, LLMOutputContract)


def test_set_output_has_clusters_field_of_per_cluster_type() -> None:
    fields = LlmClusterSetOutput.model_fields
    assert set(fields.keys()) == {"clusters"}
    from typing import get_args, get_origin
    annot = fields["clusters"].annotation
    assert get_origin(annot) is list
    assert get_args(annot) == (LlmClusterOutput,)


def test_set_output_accepts_non_empty_list_of_clusters() -> None:
    inst = LlmClusterSetOutput(
        clusters=[
            LlmClusterOutput(
                semantic_theme="top-N collapse",
                member_qids=["gs_009", "gs_017"],
                unifying_evidence="both qids miss LIMIT/ORDER BY",
                suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
                primary_blame_set=["sales.fact_sales.revenue"],
                confidence="high",
            ),
            LlmClusterOutput(
                semantic_theme="missing join spec",
                member_qids=["gs_014"],
                unifying_evidence="cartesian product",
                suggested_repair_shape=RepairShape.JOIN_DISCOVERY,
                primary_blame_set=[
                    "crm.customer.customer_id",
                    "crm.orders.customer_id",
                ],
                confidence="high",
            ),
        ],
    )
    assert len(inst.clusters) == 2
    assert inst.clusters[0].suggested_repair_shape is RepairShape.TOP_N_BY_METRIC


def test_set_output_accepts_empty_list() -> None:
    inst = LlmClusterSetOutput(clusters=[])
    assert inst.clusters == []


def test_set_output_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        LlmClusterSetOutput(
            clusters=[],
            iteration_summary="extra field that should be rejected",
        )


def test_envelope_response_format_is_databricks_strict_safe() -> None:
    """No Databricks-unsupported JSON Schema keyword leaks as a dict key.

    The pre-PR-C version of this test did ``forbidden in repr(fmt)``,
    which also matches the word ``pattern`` inside description text.
    Post-PR-C the typed branches preserve their descriptions and the
    substring check would false-positive on prose like "failure
    pattern". ``assert_no_forbidden_schema_keys`` walks the schema and
    only flags STRUCTURAL dict keys.
    """
    from tests._schema_utils import assert_no_forbidden_schema_keys

    EnvCls = AbstainableEnvelope[LlmClusterSetOutput]
    fmt = build_response_format(EnvCls)
    assert_no_forbidden_schema_keys(fmt)
