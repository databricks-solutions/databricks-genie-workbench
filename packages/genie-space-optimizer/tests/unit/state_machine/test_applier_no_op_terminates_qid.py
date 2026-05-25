"""Trial 16 RC3 — ``applier_gate`` must terminate (not recycle) when
the applier rejects a no-op patch.

Why this test exists:
    Production postmortems 575892594490176 and 319530250904653 both
    showed the same applier-rejection pathology: a single ``add_*``
    proposal that the dispatcher returns ``False`` on (because the
    target column already carries the proposed description, or the
    target table is missing, etc.) bounces between APPLYABLE and
    PROPOSED. ``applier_gate`` is configured
    ``to_stage_on_reject=FunnelStage.PROPOSED`` (line 230), so each
    rejection sends the state back to PROPOSED, where the synthesize
    LLM regenerates the same proposal and we loop again.

    The postmortem counts: 9 to 32 retries of the SAME no-op patch
    per qid before the SM ran out of internal iterations. Combined
    with RC1's full-eval cost per ``APPLIED``, the budget vanished.

    The fix: change ``applier_gate.to_stage_on_reject`` to
    ``FunnelStage.TERMINATED`` and rewrite ``_predicate``'s reject
    path to return ``GateVerdict.reject_terminal(TerminalRecord(...))``
    carrying a typed ``forbidden_signature`` so the strategist's
    learning channel (``ctx.forbidden_signatures`` →
    ``cluster_batch.py:264``) sees the dead end in the next
    iteration.

This test drives the SM step-by-step from APPLYABLE through
``applier_gate.transform`` and asserts:
  1. The final stage is TERMINATED (not PROPOSED).
  2. The applier stub was called exactly once (no recycling).
  3. The terminal record carries a non-empty ``forbidden_signature``
     so the strategist can learn from it.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.repair_proposal_typed import (
    PatchType,
    RepairProposal,
    RepairShape,
)
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.proposal_store import (
    ProposalStore,
)
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
    """Drive the state to APPLYABLE with a fully-typed
    ProposalAttempt + ClusterMembershipRecord so applier_gate can
    look up the latest attempt and surface a forbidden signature."""
    s = build_initial_state(
        qid="gs_024",
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "SELECT BASELINE", "x", 1,
        ),
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
            "H001", "AG_1", ("gs_024",), 6, "k",
        ),
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=(
            ProposalAttempt(
                attempt_index=0,
                intent_id="intent_noop_xyz",
                patch_type="add_column_description",
                deepest_stage_in_attempt=FunnelStage.PROPOSED,
                outcome="applied",
                outcome_reason="pending_gates",
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


# Trial 16 RC3 — Chunk 2 changed applier_gate so applier no-op
# rejections terminate the qid with a typed forbidden_signature
# instead of cycling back to PROPOSED. Up to Trial 15 the gate
# used ``to_stage_on_reject=FunnelStage.PROPOSED`` so a single
# dead-end patch could re-enter synthesize up to 32× per qid until
# max_iterations consumed the budget. Now the reject path returns
# ``GateVerdict.reject_terminal(TerminalRecord(...))`` carrying
# ``<patch_type>:<applier_reason>`` so cluster_batch's
# ``ctx.forbidden_signatures`` channel can teach the next-iteration
# strategist to avoid the same shape.
def test_applier_no_op_rejection_terminates_with_forbidden_signature() -> None:
    """A single applier no-op rejection must:
    * advance the state to TERMINATED (not PROPOSED),
    * be observed exactly once (no recycling),
    * carry a non-empty ``forbidden_signature`` on the TerminalRecord.

    The applier stub returns ``(call_id, False, "dropped_no_op:...")``
    — the exact shape ``apply_patch_set`` produces when the dispatcher
    returns ``False`` (Trial 15 typed-decision surface).
    """
    state = _state_at_applyable()

    call_count = {"n": 0}

    def _stub_applier(*, state, ctx, proposal):
        call_count["n"] += 1
        return (
            f"apply_{ctx.iteration}_{proposal.intent_id}",
            False,
            "dropped_no_op:target_column_not_found:orders.amount",
        )

    # The applier adapter looks the typed RepairProposal up by
    # intent_id in ``ctx.proposal_store`` before invoking the stub.
    # Production wires this in via Stage 3 (synthesize_llm); here we
    # remember a minimal RepairProposal so the lookup succeeds and the
    # stub is reached.
    proposal_store = ProposalStore()
    proposal_store.remember(
        RepairProposal(
            intent_id="intent_noop_xyz",
            intent_name="noop",
            intent_description="noop",
            repair_shape=RepairShape.COLUMN_DESCRIPTION,
            patch_type=PatchType.ADD_COLUMN_DESCRIPTION,
            rationale="r",
            confidence="high",
            patch_body={
                "patch_type": "add_column_description",
                "table_full_name": "main.sales.orders",
                "column_name": "amount",
                "description": "noop",
            },
            blame_set=("orders.amount",),
        )
    )

    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc3",
        validation_context=ValidationContext(1, "trial16-rc3", {}),
        proposal_store=proposal_store,
        extras={"applier": _stub_applier, "synthesize_llm": True},
    )

    # Drive the gate exactly once. With the bug, the gate sends the
    # state back to PROPOSED and the recycling happens at the
    # orchestrator layer; we still assert here that ONE call to the
    # applier produces a TERMINATED state, not a PROPOSED one — that
    # is the contract change.
    result = applier_gate.transform(state, ctx)

    assert call_count["n"] == 1, (
        f"applier stub was called {call_count['n']} times in a single "
        f"transform; the gate must invoke the side effect exactly once."
    )
    assert result.current_stage == FunnelStage.TERMINATED, (
        f"applier no-op rejection should terminate the qid, but the "
        f"state is at {result.current_stage}. Today's "
        f"to_stage_on_reject=FunnelStage.PROPOSED feeds the same dead-end "
        f"patch back through the synthesize lane up to 32× until "
        f"max_iterations consumes the budget."
    )
    assert result.terminal is not None, (
        "Terminated state must carry a TerminalRecord so the trajectory "
        "aggregator can classify the run."
    )
    assert result.terminal.forbidden_signature, (
        f"TerminalRecord.forbidden_signature is empty "
        f"({result.terminal.forbidden_signature!r}). Without a typed "
        f"signature, the strategist's forbidden_signatures channel "
        f"(consumed at cluster_batch.py:264) never sees the dead end "
        f"and may regenerate the same proposal next iteration. The "
        f"signature should incorporate the patch_type and the applier's "
        f"typed reason (e.g. 'add_column_description:dropped_no_op:"
        f"target_column_not_found')."
    )
