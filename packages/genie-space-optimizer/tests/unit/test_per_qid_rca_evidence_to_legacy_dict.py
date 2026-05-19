"""Plan 3 Task 5 — projector from PerQidRcaEvidence to legacy dict."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca import (
    RcaKind,
    recommended_levers_for_rca_kind,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def _make_typed() -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid="gs_009",
        observed_failure="returned 1 row instead of top 3",
        generated_sql_issue="missing LIMIT 3 and ORDER BY revenue DESC",
        expected_sql_shape="GROUP BY 1 ORDER BY 2 DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.product"),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=("judge: 'expected 3 rows, got 1'",),
    )


def test_legacy_dict_carries_all_consumer_keys() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(
        judge={"verdict": "wrong_top_n_collapse"},
        asi={
            "counterfactual_fix": "add ORDER BY ... LIMIT 3",
            "actual_objects": ["sales.fact_sales.revenue_sum"],
        },
        sql="SELECT product, SUM(revenue) FROM sales.fact_sales GROUP BY 1",
    )
    expected_keys = {
        "rca_kind", "judge_verdict", "sql_diff", "counterfactual_fix",
        "asi_features", "expected_objects", "actual_objects",
        "recommended_levers", "rca_id",
    }
    assert set(legacy.keys()) == expected_keys


def test_legacy_dict_rca_kind_comes_from_repair_family_mapper() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    assert legacy["rca_kind"] == RcaKind.TOP_N_CARDINALITY_COLLAPSE.value


def test_legacy_dict_recommended_levers_are_derived_from_rca_kind() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    expected_levers = list(
        recommended_levers_for_rca_kind(RcaKind.TOP_N_CARDINALITY_COLLAPSE)
    )
    assert legacy["recommended_levers"] == expected_levers


def test_legacy_dict_expected_objects_come_from_blame_set() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    assert legacy["expected_objects"] == [
        "sales.fact_sales.revenue",
        "sales.fact_sales.product",
    ]


def test_legacy_dict_actual_objects_come_from_asi_metadata() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(
        judge={},
        asi={"actual_objects": ["sales.fact_sales.revenue_sum"]},
        sql="",
    )
    assert legacy["actual_objects"] == ["sales.fact_sales.revenue_sum"]


def test_legacy_dict_actual_objects_default_to_empty_list_when_absent() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    assert legacy["actual_objects"] == []


def test_legacy_dict_judge_verdict_falls_back_to_observed_failure() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    assert legacy["judge_verdict"] == "returned 1 row instead of top 3"


def test_legacy_dict_judge_verdict_uses_judge_when_present() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(
        judge={"verdict": "wrong_top_n_collapse"}, asi={}, sql="",
    )
    assert legacy["judge_verdict"] == "wrong_top_n_collapse"


def test_legacy_dict_sql_diff_passes_through() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(
        judge={}, asi={},
        sql="SELECT product, SUM(revenue) FROM sales.fact_sales GROUP BY 1",
    )
    assert legacy["sql_diff"] == (
        "SELECT product, SUM(revenue) FROM sales.fact_sales GROUP BY 1"
    )


def test_legacy_dict_rca_id_is_stable_and_includes_qid_and_kind() -> None:
    typed = _make_typed()
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    assert legacy["rca_id"] == "rca_llm_gs_009_top_n_cardinality_collapse"


def test_legacy_dict_unknown_repair_family_still_produces_full_dict() -> None:
    typed = PerQidRcaEvidence(
        qid="gs_x",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=(),
        suggested_repair_family="totally_novel_we_have_never_seen",
        repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
        confidence="low",
        quoted_evidence=(),
    )
    legacy = typed.to_legacy_dict(judge={}, asi={}, sql="")
    assert legacy["rca_kind"] == RcaKind.UNKNOWN.value
    assert legacy["rca_id"] == "rca_llm_gs_x_unknown"
    assert legacy["recommended_levers"] == list(
        recommended_levers_for_rca_kind(RcaKind.UNKNOWN)
    )
