from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.gates import (
    GatesInput,
    GateOutcome,
    GateDrop,
    GATE_PIPELINE_ORDER,
)


def _stub_ctx() -> StageContext:
    return StageContext(
        run_id="r1",
        iteration=3,
        space_id="s1",
        domain="airline",
        catalog="main",
        schema="gso",
        apply_mode="real",
        journey_emit=MagicMock(),
        decision_emit=MagicMock(),
        mlflow_anchor_run_id=None,
        feature_flags={},
    )


def _basic_inp(proposals_by_ag, **overrides):
    inp_kwargs = dict(
        proposals_by_ag=proposals_by_ag,
        ags=({"ag_id": "AG_001", "target_qids": ("q1",)},),
        rca_evidence={"q1": {"rca_kind": "wrong_join_spec"}},
        applied_history=(),
        rolled_back_content_fingerprints=set(),
        forbidden_signatures=set(),
        space_snapshot={"id": "s1"},
    )
    inp_kwargs.update(overrides)
    return GatesInput(**inp_kwargs)


def test_gates_input_required_fields() -> None:
    inp = _basic_inp({"AG_001": (
        {"proposal_id": "P_001", "patch_text": "...", "content_fingerprint": "abc"},
    )})
    assert "AG_001" in inp.proposals_by_ag


def test_gate_outcome_required_fields() -> None:
    outcome = GateOutcome(
        survived_by_ag={"AG_001": ({"proposal_id": "P_001"},)},
        dropped=(GateDrop(proposal_id="P_002", gate="blast_radius", reason="too_wide"),),
        new_dead_on_arrival_signatures=("doa_xyz",),
    )
    assert outcome.dropped[0].gate == "blast_radius"


def test_gate_pipeline_order_is_pinned() -> None:
    """Phase H Completion Task 3 — F6 Path C alignment. The pipeline
    order matches the harness's actual inline gate firing order:
    lever5_structural → rca_groundedness → blast_radius (the three
    harness-emit sites), then content_fingerprint_dedup →
    dead_on_arrival as F6-only observability sub-handlers.

    Cycle 2 Task 1 prepends ``intra_ag_dedup`` as a safety pre-pass
    that collapses proposals carrying identical body text under
    different ``patch_type`` values."""
    assert GATE_PIPELINE_ORDER == (
        "intra_ag_dedup",
        "lever5_structural",
        "rca_groundedness",
        "blast_radius",
        "content_fingerprint_dedup",
        "dead_on_arrival",
    )


def test_content_fingerprint_dedup_drops_rolled_back_repeats() -> None:
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp(
        {"AG_001": (
            {"proposal_id": "P_dup", "patch_text": "...",
             "content_fingerprint": "fp_xyz"},
            {"proposal_id": "P_new", "patch_text": "...",
             "content_fingerprint": "fp_new"},
        )},
        rolled_back_content_fingerprints={"fp_xyz"},
    )
    out = g.run_gate("content_fingerprint_dedup", ctx, inp)
    surviving_ids = [p["proposal_id"] for p in out.survived_by_ag["AG_001"]]
    assert "P_dup" not in surviving_ids
    assert "P_new" in surviving_ids
    assert any(d.gate == "content_fingerprint_dedup" and d.proposal_id == "P_dup"
               for d in out.dropped)


def test_lever5_structural_drops_proposals_missing_patch_text() -> None:
    """Lever-5 structural sub-handler: proposals missing patch_text/value
    are structurally invalid and dropped."""
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp({"AG_001": (
        {"proposal_id": "P_ok", "patch_text": "ALTER ...", "target_qids": ("q1",)},
        {"proposal_id": "P_empty", "patch_text": "", "target_qids": ("q1",)},
    )})
    out = g.run_gate("lever5_structural", ctx, inp)
    surviving_ids = [p["proposal_id"] for p in out.survived_by_ag["AG_001"]]
    assert "P_ok" in surviving_ids
    assert "P_empty" not in surviving_ids
    assert any(d.gate == "lever5_structural" and d.proposal_id == "P_empty"
               for d in out.dropped)


def test_rca_groundedness_drops_proposals_without_rca_id() -> None:
    """RCA-groundedness sub-handler: a proposal must carry an rca_id
    linking it to a clustered RCA finding."""
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp({"AG_001": (
        {"proposal_id": "P_grounded", "patch_text": "x",
         "target_qids": ("q1",), "rca_id": "rca_001"},
        {"proposal_id": "P_orphan", "patch_text": "x",
         "target_qids": ("q1",), "rca_id": ""},
    )})
    out = g.run_gate("rca_groundedness", ctx, inp)
    surviving_ids = [p["proposal_id"] for p in out.survived_by_ag["AG_001"]]
    assert "P_grounded" in surviving_ids
    assert "P_orphan" not in surviving_ids
    assert any(d.gate == "rca_groundedness" for d in out.dropped)


def test_blast_radius_drops_proposals_with_excessive_affected_tables() -> None:
    """Blast-radius sub-handler: proposals touching too many tables get
    dropped (default threshold is 5)."""
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp({"AG_001": (
        {"proposal_id": "P_narrow", "affected_tables": ["t1", "t2"]},
        {"proposal_id": "P_broad",
         "affected_tables": ["t1", "t2", "t3", "t4", "t5", "t6", "t7"]},
    )})
    out = g.run_gate("blast_radius", ctx, inp)
    surviving_ids = [p["proposal_id"] for p in out.survived_by_ag["AG_001"]]
    assert "P_narrow" in surviving_ids
    assert "P_broad" not in surviving_ids
    assert any(d.gate == "blast_radius" for d in out.dropped)


def test_dead_on_arrival_drops_noop_proposals() -> None:
    """DOA sub-handler: proposals flagged as no-ops are dropped + their
    signatures are recorded for cross-iteration deduplication."""
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp({"AG_001": (
        {"proposal_id": "P_real", "patch_text": "ALTER ..."},
        {"proposal_id": "P_noop", "patch_text": "ALTER ...", "noop": True,
         "doa_signature": "sig_xyz"},
    )})
    out = g.run_gate("dead_on_arrival", ctx, inp)
    surviving_ids = [p["proposal_id"] for p in out.survived_by_ag["AG_001"]]
    assert "P_real" in surviving_ids
    assert "P_noop" not in surviving_ids
    assert "sig_xyz" in out.new_dead_on_arrival_signatures


def test_filter_runs_full_pipeline_in_order() -> None:
    """The full filter() pipeline runs all 5 sub-handlers and accumulates
    drops from each."""
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp(
        {"AG_001": (
            {"proposal_id": "P_dup", "patch_text": "x",
             "content_fingerprint": "fp_xyz", "rca_id": "rca_001"},
            {"proposal_id": "P_clean", "patch_text": "ALTER ...",
             "rca_id": "rca_001"},
        )},
        rolled_back_content_fingerprints={"fp_xyz"},
    )
    out = g.filter(ctx, inp)
    surviving_ids = [
        p["proposal_id"] for ps in out.survived_by_ag.values() for p in ps
    ]
    assert "P_dup" not in surviving_ids
    assert "P_clean" in surviving_ids


def test_run_gate_unknown_name_raises() -> None:
    from genie_space_optimizer.optimization.stages import gates as g
    ctx = _stub_ctx()
    inp = _basic_inp({"AG_001": ()})
    try:
        g.run_gate("nonexistent_gate", ctx, inp)
    except ValueError as e:
        assert "nonexistent_gate" in str(e)
    else:
        raise AssertionError("expected ValueError")
