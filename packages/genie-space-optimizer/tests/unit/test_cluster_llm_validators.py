"""Plan 4 Task 8 — deterministic post-LLM validators."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.cluster_llm import (
    _stamp_cluster_id,
    _validate_blame_set_in_schema,
    _validate_member_qids_in_input,
    _validate_no_qid_collision,
)
from genie_space_optimizer.optimization.cluster_typed import (
    ClusterValidationError,
    LlmCluster,
)
from genie_space_optimizer.optimization.repair_intent import RepairShape


def _make(
    member_qids: tuple[str, ...],
    blame: tuple[str, ...] = (),
) -> LlmCluster:
    return LlmCluster(
        cluster_id="H001",
        semantic_theme="x",
        member_qids=member_qids,
        unifying_evidence="x",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=blame,
        confidence="high",
    )


# ── _validate_member_qids_in_input ───────────────────────────────────


def test_member_qids_in_input_accepts_subset() -> None:
    cluster = _make(("gs_001", "gs_002"))
    _validate_member_qids_in_input(
        cluster, input_qids={"gs_001", "gs_002", "gs_003"},
    )


def test_member_qids_in_input_rejects_unknown_qid() -> None:
    cluster = _make(("gs_001", "gs_999"))
    with pytest.raises(ClusterValidationError) as excinfo:
        _validate_member_qids_in_input(
            cluster, input_qids={"gs_001", "gs_002"},
        )
    assert "gs_999" in str(excinfo.value)


def test_member_qids_in_input_rejects_when_all_unknown() -> None:
    cluster = _make(("gs_999", "gs_998"))
    with pytest.raises(ClusterValidationError):
        _validate_member_qids_in_input(cluster, input_qids={"gs_001"})


# ── _validate_no_qid_collision ───────────────────────────────────────


def test_no_qid_collision_accepts_disjoint_member_sets() -> None:
    c1 = _make(("gs_001", "gs_002"))
    c2 = _make(("gs_003", "gs_004"))
    _validate_no_qid_collision([c1, c2])


def test_no_qid_collision_rejects_when_qid_in_two_clusters() -> None:
    c1 = LlmCluster(
        cluster_id="H001", semantic_theme="x",
        member_qids=("gs_001", "gs_002"), unifying_evidence="x",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=(), confidence="high",
    )
    c2 = LlmCluster(
        cluster_id="H002", semantic_theme="y",
        member_qids=("gs_002", "gs_003"), unifying_evidence="y",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=(), confidence="high",
    )
    with pytest.raises(ClusterValidationError) as excinfo:
        _validate_no_qid_collision([c1, c2])
    assert "gs_002" in str(excinfo.value)


def test_no_qid_collision_accepts_empty_list() -> None:
    _validate_no_qid_collision([])


def test_no_qid_collision_accepts_single_cluster() -> None:
    _validate_no_qid_collision([_make(("gs_001",))])


# ── _validate_blame_set_in_schema ────────────────────────────────────


def test_blame_set_in_schema_accepts_subset() -> None:
    cluster = _make(("gs_001",), blame=("sales.fact_sales.revenue",))
    _validate_blame_set_in_schema(
        cluster,
        schema_columns={
            "sales.fact_sales.revenue", "sales.fact_sales.product",
        },
    )


def test_blame_set_in_schema_rejects_unknown_column() -> None:
    cluster = _make(
        ("gs_001",),
        blame=("sales.fact_sales.revenue", "nonexistent.bogus.col"),
    )
    with pytest.raises(ClusterValidationError) as excinfo:
        _validate_blame_set_in_schema(
            cluster,
            schema_columns={"sales.fact_sales.revenue"},
        )
    assert "nonexistent.bogus.col" in str(excinfo.value)


def test_blame_set_in_schema_accepts_empty_blame_set() -> None:
    """Empty primary_blame_set is acceptable (metadata-level
    failure). Vacuously valid against any schema."""
    cluster = _make(("gs_001",), blame=())
    _validate_blame_set_in_schema(cluster, schema_columns=set())


def test_blame_set_in_schema_is_case_sensitive() -> None:
    """UC identifiers are case-sensitive in Genie Spaces; the
    validator must NOT case-fold."""
    cluster = _make(("gs_001",), blame=("Sales.Fact_Sales.Revenue",))
    with pytest.raises(ClusterValidationError):
        _validate_blame_set_in_schema(
            cluster,
            schema_columns={"sales.fact_sales.revenue"},
        )


# ── _stamp_cluster_id ────────────────────────────────────────────────


def test_stamp_cluster_id_uses_namespace_prefix_with_zero_pad() -> None:
    assert _stamp_cluster_id(namespace="H", index=1) == "H001"
    assert _stamp_cluster_id(namespace="H", index=10) == "H010"
    assert _stamp_cluster_id(namespace="S", index=99) == "S099"


def test_stamp_cluster_id_handles_index_over_999() -> None:
    assert _stamp_cluster_id(namespace="H", index=1234) == "H1234"


def test_stamp_cluster_id_rejects_empty_namespace() -> None:
    with pytest.raises(ValueError):
        _stamp_cluster_id(namespace="", index=1)


def test_stamp_cluster_id_rejects_zero_or_negative_index() -> None:
    with pytest.raises(ValueError):
        _stamp_cluster_id(namespace="H", index=0)
    with pytest.raises(ValueError):
        _stamp_cluster_id(namespace="H", index=-1)
