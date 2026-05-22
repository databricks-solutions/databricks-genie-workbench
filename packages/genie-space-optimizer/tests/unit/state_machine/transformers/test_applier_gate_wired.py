"""Step §G of the production-seam wire-in plan.

``_apply_via_genie_api`` now adapts ``applier.apply_patch_set``. Reads
the typed proposal from ``ctx.proposal_store``; uses
``ctx.space_id`` + ``ctx.metadata_snapshot`` for the apply call.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    applier_gate as applier_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _make_proposal(intent_id="intent_1"):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    return RepairProposal(
        intent_id=intent_id,
        intent_name="n", intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_COLUMN_SYNONYM,
        rationale="r", confidence="high",
        patch_body={"object_id": "t:c", "synonym": "alias"},
        blame_set=("t:c",),
        target_qids=("q1",),
    )


def _state_at_applyable(intent_id="intent_1"):
    s = build_initial_state(
        qid="q1", iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "S", "x", 1,
        ),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            "plan11_stage1", "k", "s", "f", "e", "high", "rca",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "llm",
        ),
        clustered=ClusterMembershipRecord(
            "H001", "AG_H001", ("q1",), 0, "ek",
        ),
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=(ProposalAttempt(
            attempt_index=0, intent_id=intent_id,
            patch_type="add_column_synonym",
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied", outcome_reason="pending_gates",
        ),),
    )
    s = s.advance(
        FunnelStage.NORMALIZED,
        StageTransition(
            FunnelStage.PROPOSED, FunnelStage.NORMALIZED, 4, "t", "gate",
        ),
    )
    return s.advance(
        FunnelStage.APPLYABLE,
        StageTransition(
            FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, 5, "t", "gate",
        ),
    )


def _ctx(rp, **kw) -> TransformerContext:
    base = dict(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        space_id="space_abc",
        metadata_snapshot={"schema_columns": ["t.c"]},
    )
    base.update(kw)
    ctx = TransformerContext(**base)
    ctx.proposal_store.remember(rp)
    return ctx


def test_apply_success_advances_to_applied(monkeypatch):
    captured = {}

    def fake_apply(w, space_id, patches, metadata_snapshot, **kwargs):
        captured.update(
            w=w, space_id=space_id, patches=patches,
            metadata_snapshot=metadata_snapshot, kwargs=kwargs,
        )
        return {
            "space_id": space_id,
            "patch_deployed": True,
            "patch_error": "",
            "applied": [{"intent_id": "intent_1"}],
            "patched_objects": ["t"],
            "pre_snapshot": {}, "post_snapshot": {},
            "queued_high": [], "rollback_commands": [],
            "deploy_target": None, "validation_errors": [],
            "dropped_patches": [], "applier_decisions": [],
        }

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.applier.apply_patch_set",
        fake_apply,
    )

    rp = _make_proposal()
    s = _state_at_applyable(intent_id=rp.intent_id)
    out = applier_module.applier_gate.transform(s, _ctx(rp))

    assert out.current_stage == FunnelStage.APPLIED
    assert out.applied is not None
    assert out.applied.apply_call_id != ""
    assert captured["space_id"] == "space_abc"
    # Force-apply is on for v3 — the typed gates upstream already
    # enforced the v3 risk policy.
    assert captured["kwargs"].get("force_apply") is True
    # patch_type stamped on the patch dict.
    assert captured["patches"][0]["patch_type"] == "add_column_synonym"


def test_apply_failure_rejects_to_proposed(monkeypatch):
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.applier.apply_patch_set",
        lambda w, space_id, patches, metadata_snapshot, **kw: {
            "patch_deployed": False,
            "patch_error": "validation_failed_on_join_spec",
            "applied": [], "queued_high": [], "rollback_commands": [],
            "patched_objects": [], "deploy_target": None,
            "validation_errors": ["bad spec"], "dropped_patches": [],
            "applier_decisions": [], "pre_snapshot": {},
            "post_snapshot": {}, "space_id": "x",
        },
    )

    rp = _make_proposal()
    s = _state_at_applyable(intent_id=rp.intent_id)
    out = applier_module.applier_gate.transform(s, _ctx(rp))

    assert out.current_stage == FunnelStage.PROPOSED  # cycle back
    assert out.proposals[-1].outcome == "applyability_rejected"
    assert "validation_failed_on_join_spec" in out.proposals[-1].outcome_reason


def test_apply_raises_treated_as_failure(monkeypatch):
    """If apply_patch_set raises, the gate should treat it as a
    rejection rather than crash the whole state machine."""

    def boom(*a, **kw):
        raise RuntimeError("genie api unavailable")

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.applier.apply_patch_set",
        boom,
    )

    rp = _make_proposal()
    s = _state_at_applyable(intent_id=rp.intent_id)
    out = applier_module.applier_gate.transform(s, _ctx(rp))

    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].outcome == "applyability_rejected"
    assert "genie api unavailable" in out.proposals[-1].outcome_reason


def test_proposal_store_miss_treated_as_failure():
    s = _state_at_applyable(intent_id="intent_missing")
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        space_id="x",
        metadata_snapshot={},
    )
    out = applier_module.applier_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.PROPOSED
    assert "proposal_store_miss" in out.proposals[-1].outcome_reason
