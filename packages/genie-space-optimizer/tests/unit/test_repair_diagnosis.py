"""P4 C1 unit tests — RepairDiagnosis dataclass + structural gate predicate."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.llm_abstain import AbstainReason
from genie_space_optimizer.optimization.repair_diagnosis import (
    AssetRef,
    EvidenceRef,
    RepairDiagnosis,
    gate_repair_diagnosis_sufficient,
    repair_diagnosis_from_per_qid_diagnosis,
)
from genie_space_optimizer.optimization.stages.plan11_types import PerQidDiagnosis


def _diag(
    *,
    assets: tuple[AssetRef, ...] = (),
    sql_shape_delta: str | None = "rewrite as ORDER BY amount DESC LIMIT 3",
) -> RepairDiagnosis:
    return RepairDiagnosis(
        cluster_id="cluster_A",
        rca_freeform="generated SQL returned top 1 instead of top 3",
        behavior_delta="results need top-3 ordering by amount",
        sql_shape_delta=sql_shape_delta,
        implicated_assets=assets,
        evidence_citations=(),
        candidate_mechanisms=("example_sql",),
    )


def test_gate_admits_when_assets_and_sql_shape_delta_present():
    diag = _diag(
        assets=(AssetRef(catalog="main", schema="sales", table="orders"),),
    )
    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "admitted"
    assert verdict.missing_fields == ()
    assert verdict.feedback == ""


def test_gate_indeterminate_when_assets_empty():
    diag = _diag(assets=())
    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "indeterminate"
    assert "implicated_assets" in verdict.missing_fields
    assert "catalog.schema.table" in verdict.feedback


def test_gate_indeterminate_when_sql_shape_delta_empty():
    diag = _diag(
        assets=(AssetRef(catalog="main", schema="sales", table="orders"),),
        sql_shape_delta="",
    )
    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "indeterminate"
    assert "sql_shape_delta" in verdict.missing_fields


def test_gate_indeterminate_when_sql_shape_delta_whitespace_only():
    diag = _diag(
        assets=(AssetRef(catalog="main", schema="sales", table="orders"),),
        sql_shape_delta="   ",
    )
    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "indeterminate"
    assert "sql_shape_delta" in verdict.missing_fields


def test_gate_indeterminate_when_diagnosis_is_none():
    verdict = gate_repair_diagnosis_sufficient(None)
    assert verdict.outcome == "indeterminate"
    assert "implicated_assets" in verdict.missing_fields
    assert "sql_shape_delta" in verdict.missing_fields


def test_asset_ref_canonical_string():
    column_ref = AssetRef(
        catalog="main", schema="sales", table="orders", column="amount"
    )
    table_ref = AssetRef(catalog="main", schema="sales", table="orders")
    assert column_ref.canonical() == "main.sales.orders.amount"
    assert table_ref.canonical() == "main.sales.orders"


def test_repair_diagnosis_round_trip_to_json():
    diag = RepairDiagnosis(
        cluster_id="cluster_X",
        rca_freeform="judge says wrong ordering",
        behavior_delta="results need top-3 ordering by amount",
        sql_shape_delta="ORDER BY amount DESC LIMIT 3",
        implicated_assets=(
            AssetRef(
                catalog="main",
                schema="sales",
                table="orders",
                column="amount",
            ),
            AssetRef(catalog="main", schema="sales", table="customers"),
        ),
        evidence_citations=(
            EvidenceRef(source="judge_asi", ref_id="row_42", detail="x"),
        ),
        candidate_mechanisms=("example_sql", "sql_snippet"),
    )
    blob = diag.to_json()
    restored = RepairDiagnosis.from_json(blob)
    assert restored == diag


def test_repair_diagnosis_from_json_handles_legacy_payload():
    legacy_blob = {
        "cluster_id": "cluster_legacy",
        # missing rca_freeform, behavior_delta, candidate_mechanisms
        "sql_shape_delta": None,
        "implicated_assets": [],
        "evidence_citations": [],
    }
    diag = RepairDiagnosis.from_json(legacy_blob)
    assert diag.cluster_id == "cluster_legacy"
    assert diag.rca_freeform == ""
    assert diag.behavior_delta == ""
    assert diag.sql_shape_delta is None
    assert diag.implicated_assets == ()
    assert diag.evidence_citations == ()
    assert diag.candidate_mechanisms == ()


def test_repair_intent_indeterminate_is_typed_abstain_reason():
    """The plan requires a typed abstain reason so the indeterminate
    diagnosis cannot silently fall through to ``generic_judge_guidance``."""
    assert AbstainReason.REPAIR_INTENT_INDETERMINATE.value == (
        "repair_intent_indeterminate"
    )


def test_snippet_invalid_abstain_reason_present_for_c3():
    assert AbstainReason.SNIPPET_INVALID.value == "snippet_invalid"


def test_target_unresolvable_abstain_reason_present_for_c4():
    assert AbstainReason.TARGET_UNRESOLVABLE.value == "target_unresolvable"


def test_adapter_from_per_qid_diagnosis_synthesizes_behavior_delta():
    per_qid = PerQidDiagnosis(
        qid="gs_009",
        rca_kind_label="results need ordering by amount",
        observed_failure="returned top 1 row",
        generated_sql_issue="missing ORDER BY",
        expected_sql_shape="ORDER BY amount DESC LIMIT 3",
        blame_set=("main.sales.orders",),
        evidence_summary="judge row 42",
        confidence="high",
    )
    diag = repair_diagnosis_from_per_qid_diagnosis(
        cluster_id="cluster_009",
        per_qid=per_qid,
        asset_refs=(
            AssetRef(catalog="main", schema="sales", table="orders"),
        ),
        evidence_refs=(EvidenceRef(source="judge_asi", ref_id="row_42"),),
        candidate_mechanisms=("example_sql",),
    )
    assert diag.cluster_id == "cluster_009"
    assert diag.rca_freeform == "results need ordering by amount"
    assert "returned top 1 row" in diag.behavior_delta
    assert "expected ORDER BY amount DESC LIMIT 3" in diag.behavior_delta
    assert diag.sql_shape_delta == "ORDER BY amount DESC LIMIT 3"
    assert len(diag.implicated_assets) == 1
    assert diag.implicated_assets[0].canonical() == "main.sales.orders"

    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "admitted"
