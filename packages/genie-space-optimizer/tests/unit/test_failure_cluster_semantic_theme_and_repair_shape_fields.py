"""Plan 4 Task 4 — FailureCluster gains two new fields."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import RepairShape


def test_failure_cluster_has_semantic_theme_field() -> None:
    field_names = {f.name for f in dataclasses.fields(FailureCluster)}
    assert "semantic_theme" in field_names


def test_failure_cluster_has_suggested_repair_shape_field() -> None:
    field_names = {f.name for f in dataclasses.fields(FailureCluster)}
    assert "suggested_repair_shape" in field_names


def test_semantic_theme_defaults_to_empty_string() -> None:
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("gs_001",),
        root_cause="missing_top_n",
        asi_failure_type="missing_top_n",
        failure_keys=("missing_top_n",),
        blame_set_raw=("a.b.c",),
        blame_set_normalized=("a.b.c",),
        rca_card_id="",
        rca_card_summary="",
        is_grounded=False,
    )
    assert fc.semantic_theme == ""


def test_suggested_repair_shape_defaults_to_repair_shape_other() -> None:
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("gs_001",),
        root_cause="x",
        asi_failure_type="x",
        failure_keys=(),
        blame_set_raw=(),
        blame_set_normalized=(),
        rca_card_id="",
        rca_card_summary="",
        is_grounded=False,
    )
    assert fc.suggested_repair_shape is RepairShape.OTHER


def test_existing_from_legacy_constructor_keeps_working() -> None:
    cluster_dict = {
        "cluster_id": "H001",
        "question_ids": ["gs_001"],
        "root_cause": "missing_top_n",
        "asi_failure_type": "missing_top_n",
        "asi_blame_set": ["a.b.c"],
    }
    fc = FailureCluster.from_legacy(cluster_dict)
    assert fc.cluster_id == "H001"
    assert fc.target_qids == ("gs_001",)
    assert fc.semantic_theme == ""
    assert fc.suggested_repair_shape is RepairShape.OTHER


def test_from_llm_cluster_classmethod_stamps_new_fields() -> None:
    llm = LlmCluster(
        cluster_id="H003",
        semantic_theme="top-N collapse",
        member_qids=("gs_009", "gs_017"),
        unifying_evidence="both miss LIMIT/ORDER BY",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )
    fc = FailureCluster.from_llm_cluster(llm)
    assert fc.cluster_id == "H003"
    assert fc.target_qids == ("gs_009", "gs_017")
    assert fc.semantic_theme == "top-N collapse"
    assert fc.suggested_repair_shape is RepairShape.TOP_N_BY_METRIC
    assert fc.root_cause == "top-N collapse"
    assert fc.asi_failure_type == "top_n_by_metric"
    assert fc.blame_set_raw == ("sales.fact_sales.revenue",)
    assert fc.blame_set_normalized == ("sales.fact_sales.revenue",)
    assert fc.rca_card_id == ""
    assert fc.is_grounded is False
