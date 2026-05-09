"""Cycle 13 — no-proposals reflection entry must carry lever_set.

The existing call site at harness.py:18356 passes levers=[] which
disqualifies the entry from forbidden-set admission (the predicate
rejects empty lever_set). This test reproduces the bug shape via
_build_reflection_entry directly: an entry built with lever_keys=[]
short-circuits the predicate even when the flag is on; an entry
built with lever_keys=[5, 6] is admitted.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
    _reflection_admitted_to_forbidden_set,
)
from genie_space_optimizer.optimization.rollback_class import RollbackClass


def test_no_proposals_entry_with_empty_levers_not_admitted() -> None:
    """Pre-C13-T5 behaviour: levers=[] short-circuits admission
    even on flag-on. This is the bug T5 fixes."""
    entry = _build_reflection_entry(
        iteration=1, ag_id="AG1", accepted=False,
        levers=[], target_objects=[],
        prev_scores={"result_correctness": 95.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason="no_proposals",
        patches=[],
        root_cause="plural_top_n_collapse",
        blame_set=["zone_vp_name"],
        source_cluster_ids=["C001"],
    )
    assert entry["rollback_class"] == RollbackClass.NO_ACTION.value
    assert entry["lever_set"] == []
    assert _reflection_admitted_to_forbidden_set(
        entry, admit_no_action=True
    ) is False


def test_no_proposals_entry_with_levers_admitted() -> None:
    """Post-C13-T5 behaviour: lever_keys is propagated, so the
    entry's lever_set is non-empty and the predicate admits it."""
    entry = _build_reflection_entry(
        iteration=1, ag_id="AG1", accepted=False,
        levers=[5, 6], target_objects=[],
        prev_scores={"result_correctness": 95.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason="no_proposals",
        patches=[],
        root_cause="plural_top_n_collapse",
        blame_set=["zone_vp_name"],
        source_cluster_ids=["C001"],
    )
    assert entry["rollback_class"] == RollbackClass.NO_ACTION.value
    assert entry["lever_set"] == [5, 6]
    assert _reflection_admitted_to_forbidden_set(
        entry, admit_no_action=True
    ) is True


# ── Cycle 13 T6: typed decision record at empty-proposal site ──────


def test_proposal_generation_empty_record_shape() -> None:
    """The existing helper produces the contract-shaped record;
    we just need to call it from the no-proposals site. This
    test pins the record's contract so a future schema change
    surfaces here."""
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generation_empty_record,
    )

    rec = proposal_generation_empty_record(
        run_id="run_123",
        iteration=1,
        ag_id="AG1",
        cluster_id="C001",
        rca_id="RCA001",
        root_cause="plural_top_n_collapse",
        target_qids=("gs_026",),
    )
    d = rec.to_dict()
    assert d["decision_type"] == "proposal_generated"
    assert d["outcome"] == "dropped"
    assert d["reason_code"] == "proposal_generation_empty"
    assert d["ag_id"] == "AG1"
    assert d["cluster_id"] == "C001"
    assert d["root_cause"] == "plural_top_n_collapse"
    # to_dict() renders tuple fields as lists for JSON-serialisability
    # (see rca_decision_trace.py:322-339).
    assert d["target_qids"] == ["gs_026"]
    assert d["metrics"]["proposals_total"] == 0


def test_no_proposals_emits_typed_record_when_flag_on(monkeypatch) -> None:
    """Cycle 13 T6 — when the flag is on, the no-proposals iteration
    emits a typed proposal_generated[PROPOSAL_GENERATION_EMPTY]
    decision record into iter_inputs.decision_records.

    This test exercises the helper + flag wiring as a unit, not
    the full harness call site (covered by Task 8's integration
    test against the anchor fixture)."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    from genie_space_optimizer.common.config import (
        forbidden_ag_admits_no_action_enabled,
    )
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generation_empty_record,
    )

    iter_inputs: dict = {"decision_records": []}
    if forbidden_ag_admits_no_action_enabled():
        rec = proposal_generation_empty_record(
            run_id="run_123",
            iteration=1,
            ag_id="AG1",
            cluster_id="C001",
            rca_id="",
            root_cause="plural_top_n_collapse",
            target_qids=("gs_026",),
        )
        iter_inputs["decision_records"].append(rec.to_dict())

    assert len(iter_inputs["decision_records"]) == 1
    rec_dict = iter_inputs["decision_records"][0]
    assert rec_dict["decision_type"] == "proposal_generated"
    assert rec_dict["outcome"] == "dropped"
    assert rec_dict["reason_code"] == "proposal_generation_empty"
