"""Plan 11 — round-trip serialization tests for the four new stage types."""
import pytest
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
    PerQidDiagnosis,
    ValidationError,
    ValidationResult,
)


def test_per_qid_diagnosis_roundtrip():
    d = PerQidDiagnosis(
        qid="gs_009",
        rca_kind_label="top-N collapsed to single row",
        observed_failure="Query returned 1 row instead of 10",
        generated_sql_issue="RANK() not bounded by LIMIT",
        expected_sql_shape="ROW_NUMBER() with LIMIT 10",
        blame_set=("catalog.schema.orders.order_rank",),
        evidence_summary="The judge noted RANK() returns all rows with rank 1–10 across ties",
        confidence="high",
    )
    assert PerQidDiagnosis.from_json(d.to_json()) == d


def test_failure_cluster_roundtrip():
    fc = FailureCluster(
        cluster_id="H001",
        semantic_theme="top-N row limit failures",
        member_qids=("gs_009",),
        unifying_evidence="Both use ranking without LIMIT",
        repair_hypothesis="Replace RANK() with ROW_NUMBER() and add LIMIT 10",
        primary_blame_set=("catalog.schema.orders.rank_col",),
        confidence="high",
    )
    assert FailureCluster.from_json(fc.to_json()) == fc


def test_validation_error_roundtrip():
    ve = ValidationError(
        patch_id="intent_001",
        error_kind="genie_schema",
        error_detail="example_sql field is required",
        failing_location="patch_body.example_sql",
    )
    assert ValidationError.from_json(ve.to_json()) == ve


def test_validation_result_valid():
    vr = ValidationResult(patch_id="intent_001", is_valid=True, errors=())
    assert vr.is_valid is True
    assert vr.errors == ()
    assert ValidationResult.from_json(vr.to_json()) == vr


def test_validation_result_invalid():
    ve = ValidationError(
        patch_id="intent_001",
        error_kind="sql_execution",
        error_detail="Table not found: catalog.schema.missing_table",
        failing_location=None,
    )
    vr = ValidationResult(patch_id="intent_001", is_valid=False, errors=(ve,))
    assert vr.is_valid is False
    assert len(vr.errors) == 1
    assert ValidationResult.from_json(vr.to_json()) == vr
