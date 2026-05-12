from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_card_builder import (
    SelfGroundingResult,
    self_grounding_check,
)


def _asi(failure_type: str, blame: list[str]) -> dict:
    return {"failure_type": failure_type, "blame_set": blame}


def test_self_grounding_passes_when_root_matches_and_terms_in_asi_blame() -> None:
    asi_by_qid = {
        "gs_026": _asi("plural_top_n_collapse", ["zone_vp_name", "plural_top_n_collapse"]),
    }
    result = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        proposed_grounding_terms=frozenset({"zone_vp_name", "plural_top_n_collapse"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={"gs_026": "SELECT TOP 1 zone_combination FROM mv_7now_store_sales"},
        reference_sql_by_qid={"gs_026": "SELECT zone_vp_name, ... ORDER BY ... LIMIT 5"},
    )
    assert result.ok is True
    assert result.failure_reason is None


def test_self_grounding_fails_when_root_disagrees_with_dominant_asi() -> None:
    asi_by_qid = {
        "gs_026": _asi("missing_filter", ["region"]),
    }
    result = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        proposed_grounding_terms=frozenset({"region"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={},
        reference_sql_by_qid={},
    )
    assert result.ok is False
    assert result.failure_reason == "root_cause_disagrees_with_dominant_asi"


def test_self_grounding_fails_when_grounding_term_not_in_any_source() -> None:
    asi_by_qid = {
        "gs_026": _asi("plural_top_n_collapse", ["zone_vp_name"]),
    }
    result = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        proposed_grounding_terms=frozenset({"zone_vp_name", "completely_unrelated_term"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={"gs_026": "SELECT zone_combination FROM mv_7now_store_sales"},
        reference_sql_by_qid={"gs_026": "SELECT zone_vp_name FROM mv_esr_dim_location"},
    )
    assert result.ok is False
    assert result.failure_reason == "ungrounded_term"
    assert result.ungrounded_terms == ("completely_unrelated_term",)


def test_self_grounding_passes_when_term_grounded_in_generated_sql() -> None:
    asi_by_qid = {
        "gs_026": _asi("plural_top_n_collapse", []),  # blame_set empty
    }
    result = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        proposed_grounding_terms=frozenset({"zone_combination"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={"gs_026": "SELECT TOP 1 zone_combination FROM mv_7now_store_sales"},
        reference_sql_by_qid={},
    )
    assert result.ok is True


def test_self_grounding_passes_when_term_grounded_in_reference_sql() -> None:
    asi_by_qid = {
        "gs_026": _asi("plural_top_n_collapse", []),
    }
    result = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        proposed_grounding_terms=frozenset({"zone_vp_name"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={},
        reference_sql_by_qid={"gs_026": "SELECT zone_vp_name FROM mv_esr_dim_location"},
    )
    assert result.ok is True
