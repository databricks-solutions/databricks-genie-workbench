"""Cycle 16 T3 — wire Branch C synthesis into _run_narrow_l6_replacement_loop.

Tests target the loop's external contract:
  * Flag off → unchanged behavior (no Branch C records).
  * Flag on + L6 expression dropped + resolvable QIDs → N L5 survivors,
    one record + marker per survivor.
  * Flag on + L6 expression dropped + zero resolvable QIDs → zero
    survivors, zero Branch C records (the structural-drop halt in
    Task 4 picks this up).
"""

from __future__ import annotations


def _make_blast_dropped_expression(parent_pid: str = "L6:P001#3") -> dict:
    return {
        "proposal_id": parent_pid,
        "patch_type": "add_sql_snippet_expression",
        "reason": "high_collateral_risk_flagged",
        "passing_dependents_outside_target": ["gs_010"],
        "target": "mv_esr_dim_location.zone_vp_name",
        "original_patch": {
            "proposal_id": parent_pid,
            "patch_type": "add_sql_snippet_expression",
            "target": "mv_esr_dim_location.zone_vp_name",
            "sql_expression": (
                "CASE WHEN role = 'VP' THEN name END"
            ),
            "rca_id": "RCA_H002",
        },
    }


def test_flag_off_loop_emits_zero_branch_c_records(monkeypatch) -> None:
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", raising=False)
    from genie_space_optimizer.optimization.harness import (
        _run_narrow_l6_replacement_loop,
    )
    iter_inputs: dict = {}
    survivors = _run_narrow_l6_replacement_loop(
        blast_dropped=[_make_blast_dropped_expression()],
        blast_target_qids=("gs_024",),
        ag_root_cause="plural_top_n_collapse",
        run_id="r1",
        iteration=1,
        ag_id="AG1",
        cluster_id="H002",
        iter_inputs=iter_inputs,
        qid_to_question_text={"gs_024": "q?"},
        qid_to_reference_sql={"gs_024": "SELECT 1"},
    )
    branch_c_records = [
        r for r in iter_inputs.get("decision_records") or []
        if (
            isinstance(r, dict)
            and r.get("decision_type")
            == "narrow_replacement_branch_c_synthesized"
        )
    ]
    assert branch_c_records == []
    branch_c_markers = [
        m for m in iter_inputs.get("markers") or []
        if isinstance(m, str)
        and m.startswith("GSO_NARROW_REPLACEMENT_BRANCH_C_SYNTHESIZED_V1")
    ]
    assert branch_c_markers == []


def test_flag_on_loop_emits_branch_c_survivor_per_resolvable_qid(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", "1")

    # patch_blast_radius_is_safe accepts add_example_sql by default (no
    # passing_dependents stamped) — but we monkey-patch to be explicit.
    from genie_space_optimizer.optimization import proposal_grounding

    def _always_safe(candidate, *, ag_target_qids, max_outside_target, **kwargs):
        return {"safe": True, "reason": "no_outside_target_dependents"}

    monkeypatch.setattr(
        proposal_grounding,
        "patch_blast_radius_is_safe",
        _always_safe,
    )

    from genie_space_optimizer.optimization.harness import (
        _run_narrow_l6_replacement_loop,
    )
    iter_inputs: dict = {}
    survivors = _run_narrow_l6_replacement_loop(
        blast_dropped=[_make_blast_dropped_expression()],
        blast_target_qids=("gs_024", "gs_026"),
        ag_root_cause="plural_top_n_collapse",
        run_id="r1",
        iteration=1,
        ag_id="AG1",
        cluster_id="H002",
        iter_inputs=iter_inputs,
        qid_to_question_text={"gs_024": "q024?", "gs_026": "q026?"},
        qid_to_reference_sql={"gs_024": "SELECT 24", "gs_026": "SELECT 26"},
    )
    assert len(survivors) == 2
    sids = {p["proposal_id"] for p in survivors}
    assert sids == {
        "L6:P001#3#L5_BRANCH_C_gs_024",
        "L6:P001#3#L5_BRANCH_C_gs_026",
    }

    branch_c_records = [
        r for r in iter_inputs.get("decision_records") or []
        if isinstance(r, dict)
        and r.get("decision_type")
        == "narrow_replacement_branch_c_synthesized"
    ]
    assert len(branch_c_records) == 2
    assert {r["target_qid"] for r in branch_c_records} == {
        "gs_024", "gs_026",
    }
    assert all(r["branch"] == "C" for r in branch_c_records)

    branch_c_markers = [
        m for m in iter_inputs.get("markers") or []
        if isinstance(m, str)
        and m.startswith("GSO_NARROW_REPLACEMENT_BRANCH_C_SYNTHESIZED_V1")
    ]
    assert len(branch_c_markers) == 2


def test_flag_on_zero_resolvable_qids_emits_zero_survivors(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", "1")
    from genie_space_optimizer.optimization.harness import (
        _run_narrow_l6_replacement_loop,
    )
    iter_inputs: dict = {}
    survivors = _run_narrow_l6_replacement_loop(
        blast_dropped=[_make_blast_dropped_expression()],
        blast_target_qids=("gs_024",),
        ag_root_cause="plural_top_n_collapse",
        run_id="r1",
        iteration=1,
        ag_id="AG1",
        cluster_id="H002",
        iter_inputs=iter_inputs,
        qid_to_question_text={},  # nothing resolvable
        qid_to_reference_sql={},
    )
    assert survivors == []
    branch_c_records = [
        r for r in iter_inputs.get("decision_records") or []
        if isinstance(r, dict)
        and r.get("decision_type")
        == "narrow_replacement_branch_c_synthesized"
    ]
    assert branch_c_records == []
