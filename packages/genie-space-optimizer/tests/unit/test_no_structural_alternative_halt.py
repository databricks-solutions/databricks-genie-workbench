"""Cycle 16 T4 — halt wiring + reflection-entry shape contract.

We can't unit-test the harness's full blast-radius gate block without
a giant fixture, so this file pins the *invariants* the halt wiring
must preserve via a smaller integration boundary:

  1. The halt helper emits exactly one no_structural_alternative
     record per AG.
  2. The reflection entry has rollback_class=NO_ACTION,
     rollback_reason="no_structural_alternative",
     levers=ag.lever_set (non-empty), accepted=False, and is admitted
     by _reflection_admitted_to_forbidden_set(admit_no_action=True).
  3. The halt sets patches=[] without raising.

Tests target a thin helper _emit_no_structural_alternative_halt
defined in harness.py and verified end-to-end below.
"""

from __future__ import annotations


def _structural_drop_payload() -> dict:
    return {
        "ag_id": "AG_DECOMPOSED_H002",
        "cluster_id": "H002",
        "rca_id": "RCA_H002",
        "root_cause": "plural_top_n_collapse",
        "target_qids": ("gs_024", "gs_026"),
        "lever_set": (6,),
        "dropped_proposal_ids": ("L6:P001#3", "L6:P002#1"),
    }


def test_halt_helper_emits_one_no_structural_alternative_record() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_no_structural_alternative_halt,
    )
    iter_inputs: dict = {}
    reflection_buffer: list[dict] = []
    payload = _structural_drop_payload()
    _emit_no_structural_alternative_halt(
        run_id="r1",
        iteration=1,
        ag=payload,
        iter_inputs=iter_inputs,
        reflection_buffer=reflection_buffer,
    )
    halt_records = [
        r for r in iter_inputs.get("decision_records") or []
        if isinstance(r, dict)
        and r.get("reason_code") == "no_structural_alternative"
    ]
    assert len(halt_records) == 1
    assert halt_records[0]["ag_id"] == "AG_DECOMPOSED_H002"
    assert halt_records[0]["outcome"] == "retired"
    assert halt_records[0]["decision_type"] == "ag_retired"


def test_halt_helper_appends_no_action_reflection_entry() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_no_structural_alternative_halt,
        _reflection_admitted_to_forbidden_set,
    )
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )
    iter_inputs: dict = {}
    reflection_buffer: list[dict] = []
    payload = _structural_drop_payload()
    _emit_no_structural_alternative_halt(
        run_id="r1",
        iteration=1,
        ag=payload,
        iter_inputs=iter_inputs,
        reflection_buffer=reflection_buffer,
    )
    assert len(reflection_buffer) == 1
    entry = reflection_buffer[0]
    assert entry["rollback_class"] == RollbackClass.NO_ACTION.value
    assert entry["rollback_reason"] == "no_structural_alternative"
    assert entry["accepted"] is False
    assert entry["root_cause"] == "plural_top_n_collapse"
    assert tuple(entry["lever_set"]) == (6,)
    # C13 admission predicate must admit this entry under the flag.
    assert _reflection_admitted_to_forbidden_set(
        entry, admit_no_action=True,
    ) is True
    # And reject it under the flag-off path (matches C13 byte-stability).
    assert _reflection_admitted_to_forbidden_set(
        entry, admit_no_action=False,
    ) is False


def test_halt_helper_emits_marker() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_no_structural_alternative_halt,
    )
    iter_inputs: dict = {}
    reflection_buffer: list[dict] = []
    payload = _structural_drop_payload()
    _emit_no_structural_alternative_halt(
        run_id="r1",
        iteration=1,
        ag=payload,
        iter_inputs=iter_inputs,
        reflection_buffer=reflection_buffer,
    )
    markers = [
        m for m in iter_inputs.get("markers") or []
        if isinstance(m, str)
        and m.startswith("GSO_NO_STRUCTURAL_ALTERNATIVE_V1")
    ]
    assert len(markers) == 1


def test_halt_helper_emits_one_structural_causal_dropped_per_orphan() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_structural_causal_dropped_records,
    )
    from genie_space_optimizer.optimization.stages.gates import (
        StructuralCausalDrop,
    )
    iter_inputs: dict = {}
    drops = (
        StructuralCausalDrop(
            ag_rca_id="RCA_H002",
            original_proposal_id="L6:P001#3",
            original_patch_type="add_sql_snippet_expression",
            original_target="mv_esr_dim_location.zone_vp_name",
            drop_reason="high_collateral_risk_flagged",
            target_qids=("gs_024",),
        ),
        StructuralCausalDrop(
            ag_rca_id="RCA_H002",
            original_proposal_id="L6:P002#1",
            original_patch_type="add_sql_snippet_measure",
            original_target="mv_esr_fct_orders.zone_vp_total_orders",
            drop_reason="high_collateral_risk_flagged",
            target_qids=("gs_024",),
        ),
    )
    _emit_structural_causal_dropped_records(
        run_id="r1",
        iteration=1,
        ag_id="AG1",
        cluster_id="H002",
        rca_id="RCA_H002",
        root_cause="plural_top_n_collapse",
        drops=drops,
        iter_inputs=iter_inputs,
    )
    drop_records = [
        r for r in iter_inputs.get("decision_records") or []
        if isinstance(r, dict)
        and r.get("reason_code") == "structural_causal_dropped"
    ]
    assert len(drop_records) == 2
    assert {r["proposal_id"] for r in drop_records} == {
        "L6:P001#3", "L6:P002#1",
    }
    drop_markers = [
        m for m in iter_inputs.get("markers") or []
        if isinstance(m, str)
        and m.startswith("GSO_STRUCTURAL_CAUSAL_DROPPED_V1")
    ]
    assert len(drop_markers) == 2
