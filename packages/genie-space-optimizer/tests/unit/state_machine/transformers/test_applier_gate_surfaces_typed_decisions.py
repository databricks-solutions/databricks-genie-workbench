"""Trial 15 — ``applier_gate`` must surface typed
``ApplierDecision.decision:reason`` instead of the opaque
``apply_failed_no_reason`` sentinel.

Why this test exists:
    Postmortems dc89d1a9 + 98ec8950 showed dozens of applier
    rejections (36 + 27 = 63 across the two runs) all bucketed under
    ``apply_failed_no_reason`` — useless for operators trying to learn
    why ``add_column_description`` patches were dropped. The applier
    already emitted typed reasons via ``build_applier_decision`` (see
    ``applier_audit.py``) into ``apply_log['applier_decisions']``, but
    the gate ignored that audit and defaulted to the sentinel. Trial
    15 Part B1 fixes the gate to read the audit; this test pins that
    behavior so a future regression cannot silently downgrade the
    surface back to the sentinel.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout
from unittest.mock import patch

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
from genie_space_optimizer.optimization.state_machine.transformers.applier_gate import (
    applier_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_applyable():
    s = build_initial_state(
        qid="gs_009",
        iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            "plan11_stage1", "k", "s", "f", "e", "high", "r",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch",
        ),
        clustered=ClusterMembershipRecord(
            "H001", "AG_1", ("gs_009",), 6, "k",
        ),
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=(
            ProposalAttempt(
                0, "intent_xyz", "add_column_description",
                FunnelStage.PROPOSED, "applied", "pending_gates",
            ),
        ),
    )
    s = s.advance(
        FunnelStage.NORMALIZED,
        StageTransition(
            FunnelStage.PROPOSED, FunnelStage.NORMALIZED, 4, "structural",
            "validation_gate",
        ),
    )
    return s.advance(
        FunnelStage.APPLYABLE,
        StageTransition(
            FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, 5, "blast", "batch",
        ),
    )


def _build_ctx_with_proposal_store():
    """Build a ctx whose ``proposal_store`` carries a minimal
    ``RepairProposal`` so ``_apply_via_genie_api`` can look it up."""
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        PatchType,
        RepairProposal,
        RepairShape,
    )
    from genie_space_optimizer.optimization.state_machine.proposal_store import (
        ProposalStore,
    )

    proposal = RepairProposal(
        intent_id="intent_xyz",
        intent_name="add_column_description",
        intent_description="add a column description",
        repair_shape=RepairShape.COLUMN_DESCRIPTION,
        patch_type=PatchType.ADD_COLUMN_DESCRIPTION,
        rationale="r",
        confidence="high",
        patch_body={
            "type": "add_column_description",
            "target_object": "catalog.schema.orders",
            "column_name": "amount",
            "description": "Order amount in USD.",
        },
        blame_set=("catalog.schema.orders.amount",),
    )
    store = ProposalStore()
    store.remember(proposal)
    ctx = TransformerContext(
        iteration=1,
        run_id="trial15-d3",
        validation_context=ValidationContext(1, "trial15-d3", {}),
        proposal_store=store,
        space_id="space-trial15",
        metadata_snapshot={"_seed_only_field": "1"},
    )
    return ctx


def test_applier_gate_surfaces_typed_decision_reason():
    """When ``apply_patch_set`` returns ``patch_deployed=False`` plus a
    typed ``applier_decisions`` row, the gate's ``ProposalAttempt``
    outcome_reason must carry ``<decision>:<reason>`` from the audit,
    NOT ``apply_failed_no_reason``."""
    state = _state_at_applyable()
    ctx = _build_ctx_with_proposal_store()

    apply_log_stub = {
        "patch_deployed": False,
        "patch_error": "",
        "validation_errors": [],
        "applier_decisions": [
            {
                "patch_type": "add_column_description",
                "decision": "dropped_validation",
                "reason": "target_column_not_found",
                "error_excerpt": "",
            },
        ],
    }

    buf = io.StringIO()
    with patch(
        "genie_space_optimizer.optimization.applier.apply_patch_set",
        return_value=apply_log_stub,
    ), redirect_stdout(buf):
        result = applier_gate.transform(state, ctx)
    stdout = buf.getvalue()

    # The applier reject transition should leave the state at PROPOSED
    # (escalation cycle) with the latest ProposalAttempt's
    # outcome_reason carrying the typed decision string.
    assert result.current_stage == FunnelStage.PROPOSED
    latest = result.proposals[-1]
    assert latest.outcome_reason == "dropped_validation:target_column_not_found", (
        f"Expected typed decision:reason on the rejected ProposalAttempt, "
        f"got outcome_reason={latest.outcome_reason!r}. The applier audit "
        f"trail must propagate through to the gate's ProposalAttempt."
    )
    assert "apply_failed_no_reason" not in stdout, (
        f"Sentinel apply_failed_no_reason leaked into gate markers — "
        f"Trial 15 Part B1 should have eliminated it. Stdout:\n{stdout}"
    )


def test_applier_gate_falls_back_to_apply_no_decision_emitted_sentinel():
    """When ``apply_patch_set`` returns ``patch_deployed=False`` AND
    ``applier_decisions`` is empty AND no ``patch_error`` /
    ``validation_errors`` are populated, the gate emits the tighter
    ``apply_no_decision_emitted`` sentinel — a genuine applier-side
    bug signal operators can grep for.
    """
    state = _state_at_applyable()
    ctx = _build_ctx_with_proposal_store()

    apply_log_stub = {
        "patch_deployed": False,
        "patch_error": "",
        "validation_errors": [],
        "applier_decisions": [],
    }

    buf = io.StringIO()
    with patch(
        "genie_space_optimizer.optimization.applier.apply_patch_set",
        return_value=apply_log_stub,
    ), redirect_stdout(buf):
        result = applier_gate.transform(state, ctx)

    latest = result.proposals[-1]
    assert latest.outcome_reason == "apply_no_decision_emitted", (
        f"Expected the Trial 15 tighter sentinel "
        f"'apply_no_decision_emitted'; got {latest.outcome_reason!r}. "
        f"The post-B1 sentinel must be distinct from the legacy "
        f"'apply_failed_no_reason' so operators can tell the two "
        f"failure modes apart in postmortems."
    )
