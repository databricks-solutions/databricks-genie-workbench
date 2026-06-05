"""Trial 19 B2 — LLM-emitted ``intended_patch_shape`` overrides closed dict."""
from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_card_builder import (
    intended_patch_shape_for_root_cause,
)


def test_back_compat_dict_returned_when_no_asi_supplied() -> None:
    """Legacy callers (no ``asi_by_qid``) still get the closed-dict value."""
    assert (
        intended_patch_shape_for_root_cause(RcaKind.TOP_N_CARDINALITY_COLLAPSE)
        == "enforce_explicit_top_n_cardinality"
    )


def test_back_compat_dict_returned_for_unknown_kind() -> None:
    assert (
        intended_patch_shape_for_root_cause(RcaKind.UNKNOWN)
        == "generic_judge_clarification"
    )


def test_llm_string_overrides_dict_when_present() -> None:
    """When Stage 1 emits ``intended_patch_shape``, it wins over the dict."""
    asi = {
        "gs_009": {
            "rca_kind_label": "top_n_cardinality_collapse",
            "intended_patch_shape": "force_inline_limit_clause",
        },
    }
    assert (
        intended_patch_shape_for_root_cause(
            RcaKind.TOP_N_CARDINALITY_COLLAPSE, asi_by_qid=asi,
        )
        == "force_inline_limit_clause"
    )


def test_invented_llm_intent_passes_through() -> None:
    """LLM is free to invent new intent strings — they pass through verbatim."""
    asi = {
        "gs_001": {
            "intended_patch_shape": "newly_invented_repair_intent_v2",
        },
    }
    assert (
        intended_patch_shape_for_root_cause(
            RcaKind.UNKNOWN, asi_by_qid=asi,
        )
        == "newly_invented_repair_intent_v2"
    )


def test_falls_back_to_dict_when_all_asi_strings_empty() -> None:
    """Pre-Trial-19 rows (no string emitted) fall back to the dict."""
    asi = {
        "gs_001": {"failure_type": "plural_top_n_collapse"},
        "gs_002": {"intended_patch_shape": ""},
    }
    assert (
        intended_patch_shape_for_root_cause(
            RcaKind.TOP_N_CARDINALITY_COLLAPSE, asi_by_qid=asi,
        )
        == "enforce_explicit_top_n_cardinality"
    )


def test_majority_wins_on_tied_string_emissions() -> None:
    asi = {
        "gs_001": {"intended_patch_shape": "enforce_explicit_top_n_cardinality"},
        "gs_002": {"intended_patch_shape": "enforce_explicit_top_n_cardinality"},
        "gs_003": {"intended_patch_shape": "alternative_shape"},
    }
    assert (
        intended_patch_shape_for_root_cause(
            RcaKind.TOP_N_CARDINALITY_COLLAPSE, asi_by_qid=asi,
        )
        == "enforce_explicit_top_n_cardinality"
    )


def test_tie_broken_lexically() -> None:
    asi = {
        "gs_001": {"intended_patch_shape": "shape_b"},
        "gs_002": {"intended_patch_shape": "shape_a"},
    }
    assert (
        intended_patch_shape_for_root_cause(
            RcaKind.UNKNOWN, asi_by_qid=asi,
        )
        == "shape_a"
    )
