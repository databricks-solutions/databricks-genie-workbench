"""Plan 4 Task 3 — LlmCluster frozen+slots+JsonRoundTrip dataclass."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_dataclass_is_frozen_with_slots() -> None:
    assert dataclasses.is_dataclass(LlmCluster)
    assert LlmCluster.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in LlmCluster.__dict__


def test_dataclass_mixes_in_json_round_trip() -> None:
    assert issubclass(LlmCluster, JsonRoundTrip)


def test_field_set_includes_cluster_id_plus_six_llm_fields() -> None:
    field_names = {f.name for f in dataclasses.fields(LlmCluster)}
    assert field_names == {
        "cluster_id",
        "semantic_theme", "member_qids", "unifying_evidence",
        "suggested_repair_shape", "primary_blame_set", "confidence",
    }


def test_round_trip_through_to_json_from_json() -> None:
    inst = LlmCluster(
        cluster_id="H001",
        semantic_theme="top-N collapse",
        member_qids=("gs_009", "gs_017"),
        unifying_evidence="both qids miss LIMIT/ORDER BY",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )
    payload = inst.to_json()
    assert payload["cluster_id"] == "H001"
    assert payload["suggested_repair_shape"] == "top_n_by_metric"
    assert payload["member_qids"] == ["gs_009", "gs_017"]
    rebuilt = LlmCluster.from_json(payload)
    assert rebuilt == inst


def test_sequence_fields_are_tuples_not_lists() -> None:
    inst = LlmCluster(
        cluster_id="H002",
        semantic_theme="x",
        member_qids=("gs_001",),
        unifying_evidence="x",
        suggested_repair_shape=RepairShape.OTHER,
        primary_blame_set=("a.b.c",),
        confidence="medium",
    )
    assert isinstance(inst.member_qids, tuple)
    assert isinstance(inst.primary_blame_set, tuple)
