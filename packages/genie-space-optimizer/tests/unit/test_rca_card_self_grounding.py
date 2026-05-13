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


def test_self_grounding_passes_when_term_grounded_in_soft_cluster_blame() -> None:
    """Phase 1 Addendum — a grounding term that does NOT appear in
    hard-cluster ASI / generated SQL / reference SQL but DOES appear
    in a paired soft cluster's blame_set is grounded. The dominant
    root cause is still computed from hard ASI only."""
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.rca_card_builder import (
        self_grounding_check,
    )

    asi_by_qid = {"gs_021": _asi("missing_filter", [])}  # hard blame empty
    soft_grounding_sources = [
        {"asi_by_qid": {"gs_001": _asi("missing_filter", ["time_window"])}},
    ]
    result = self_grounding_check(
        proposed_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        proposed_grounding_terms=frozenset({"time_window"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={},
        reference_sql_by_qid={},
        soft_grounding_sources=soft_grounding_sources,
    )
    assert result.ok is True


def test_self_grounding_dominant_root_cause_uses_hard_only() -> None:
    """Phase 1 Addendum — adding soft sources MUST NOT change the
    dominant-root-cause check. Soft ASI is evidence, not authority."""
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.rca_card_builder import (
        self_grounding_check,
    )

    # Hard cluster says missing_filter; soft cluster says
    # plural_top_n_collapse (different root). Proposed root_cause must
    # equal the HARD dominant, not the soft one.
    asi_by_qid = {"gs_021": _asi("missing_filter", ["time_window"])}
    soft_grounding_sources = [
        {"asi_by_qid": {"gs_900": _asi("plural_top_n_collapse", [])}},
    ]
    bad = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,  # matches soft, not hard
        proposed_grounding_terms=frozenset({"time_window"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={},
        reference_sql_by_qid={},
        soft_grounding_sources=soft_grounding_sources,
    )
    assert bad.ok is False
    assert bad.failure_reason == "root_cause_disagrees_with_dominant_asi"


def test_self_grounding_default_no_soft_sources_is_byte_stable() -> None:
    """Phase 1 Addendum — when ``soft_grounding_sources`` is omitted
    (default), behaviour matches the original self-grounding check
    exactly."""
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.rca_card_builder import (
        self_grounding_check,
    )

    asi_by_qid = {"gs_026": _asi("plural_top_n_collapse", ["zone_vp_name"])}
    result = self_grounding_check(
        proposed_root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        proposed_grounding_terms=frozenset({"zone_vp_name"}),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={},
        reference_sql_by_qid={},
    )
    assert result.ok is True
