"""Cycle 16 T1 — pure Branch C synthesizer contract.

build_l5_example_sql_replacement is the L5 example-SQL fallback for
add_sql_snippet_expression / add_sql_snippet_measure patches dropped at
blast-radius. Returns one add_example_sql patch per resolvable target
QID (a QID is *resolvable* iff both question_text and reference_sql are
non-empty for it). Pure: no I/O, no flag reads, no logger.
"""

from __future__ import annotations


def _h002_expression_patch() -> dict:
    return {
        "proposal_id": "L6:P001#3",
        "patch_type": "add_sql_snippet_expression",
        "target": "mv_esr_dim_location.zone_vp_name",
        "sql_expression": (
            "CASE WHEN role = 'VP' AND zone IS NOT NULL THEN name END"
        ),
        "rca_id": "RCA_H002_PLURAL_TOP_N",
        "rationale": "plural top-N collapse for zone-VP",
    }


def _h002_measure_patch() -> dict:
    return {
        "proposal_id": "L6:P002#1",
        "patch_type": "add_sql_snippet_measure",
        "target": "mv_esr_fct_orders.zone_vp_total_orders",
        "sql_expression": (
            "SUM(CASE WHEN role = 'VP' THEN order_count END)"
        ),
        "rca_id": "RCA_H002_PLURAL_TOP_N",
        "rationale": "VP order rollup",
    }


def test_returns_one_l5_patch_per_resolvable_target_qid() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    out = build_l5_example_sql_replacement(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("gs_024", "gs_026"),
        qid_to_question_text={
            "gs_024": "What is the total order count by zone VP?",
            "gs_026": "List all zone VPs with their order rollup.",
        },
        qid_to_reference_sql={
            "gs_024": "SELECT zone_vp, SUM(order_count) FROM mv_esr_fct_orders GROUP BY 1",
            "gs_026": "SELECT name FROM mv_esr_dim_location WHERE role = 'VP'",
        },
        root_cause="plural_top_n_collapse",
    )
    assert len(out) == 2
    patch_types = {p["patch_type"] for p in out}
    assert patch_types == {"add_example_sql"}
    proposal_ids = [p["proposal_id"] for p in out]
    assert proposal_ids == sorted(proposal_ids)
    assert len(set(proposal_ids)) == 2  # injective
    for p in out:
        assert p["proposal_id"].startswith("L6:P001#3#L5_BRANCH_C_")
        assert p["derived_from"] == "L6:P001#3"
        assert p["rca_id"] == "RCA_H002_PLURAL_TOP_N"
        assert p["root_cause"] == "plural_top_n_collapse"
        assert p["narrowing_strategy"] == "l5_example_sql_per_qid"
        assert p["narrow_replacement_branch"] == "C"
        assert p["example_question"]
        assert p["example_sql"]


def test_skips_qids_missing_question_text() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    out = build_l5_example_sql_replacement(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("gs_024", "gs_026"),
        qid_to_question_text={
            "gs_024": "What is the total order count by zone VP?",
            # gs_026 missing
        },
        qid_to_reference_sql={
            "gs_024": "SELECT zone_vp, SUM(order_count) ...",
            "gs_026": "SELECT name FROM mv_esr_dim_location ...",
        },
        root_cause="plural_top_n_collapse",
    )
    assert len(out) == 1
    assert out[0]["example_question"].startswith("What is the total")


def test_skips_qids_missing_reference_sql() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    out = build_l5_example_sql_replacement(
        original_patch=_h002_measure_patch(),
        ag_target_qids=("gs_024", "gs_026"),
        qid_to_question_text={
            "gs_024": "What is the total order count by zone VP?",
            "gs_026": "List all zone VPs with their order rollup.",
        },
        qid_to_reference_sql={
            "gs_024": "SELECT zone_vp, SUM(order_count) ...",
            # gs_026 missing
        },
        root_cause="plural_top_n_collapse",
    )
    assert len(out) == 1
    assert "gs_024" in out[0]["proposal_id"]


def test_returns_empty_tuple_when_zero_resolvable_qids() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    out = build_l5_example_sql_replacement(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("gs_024", "gs_026"),
        qid_to_question_text={},  # nothing resolvable
        qid_to_reference_sql={},
        root_cause="plural_top_n_collapse",
    )
    assert out == ()


def test_returns_empty_tuple_when_no_target_qids() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    out = build_l5_example_sql_replacement(
        original_patch=_h002_expression_patch(),
        ag_target_qids=(),
        qid_to_question_text={"gs_024": "q?"},
        qid_to_reference_sql={"gs_024": "SELECT 1"},
        root_cause="plural_top_n_collapse",
    )
    assert out == ()


def test_returns_empty_tuple_for_non_l6_patch_types() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    out = build_l5_example_sql_replacement(
        original_patch={
            "proposal_id": "L4:filter#1",
            "patch_type": "add_sql_snippet_filter",
            "rca_id": "RCA_H002",
        },
        ag_target_qids=("gs_024",),
        qid_to_question_text={"gs_024": "q?"},
        qid_to_reference_sql={"gs_024": "SELECT 1"},
        root_cause="plural_top_n_collapse",
    )
    assert out == ()


def test_emitted_patches_are_iteration_invariant_under_qid_order() -> None:
    """Same inputs must produce byte-identical output regardless of dict order.

    Ensures replay byte-stability when the harness threads dicts whose
    insertion order varies between runs.
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_l5_example_sql_replacement,
    )

    args_a = dict(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("gs_024", "gs_026"),
        qid_to_question_text={"gs_024": "qA", "gs_026": "qB"},
        qid_to_reference_sql={"gs_024": "SA", "gs_026": "SB"},
        root_cause="plural_top_n_collapse",
    )
    args_b = dict(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("gs_026", "gs_024"),
        qid_to_question_text={"gs_026": "qB", "gs_024": "qA"},
        qid_to_reference_sql={"gs_026": "SB", "gs_024": "SA"},
        root_cause="plural_top_n_collapse",
    )
    out_a = build_l5_example_sql_replacement(**args_a)
    out_b = build_l5_example_sql_replacement(**args_b)
    assert tuple(p["proposal_id"] for p in out_a) == tuple(
        p["proposal_id"] for p in out_b
    )
    for pa, pb in zip(out_a, out_b):
        assert pa == pb
