"""Trial 17 Step 6 — gs_024 missing-table preflight workbench test.

Postmortem 289767602715184 shows gs_024 was rejected at
``applier_gate`` THREE times with ``add_column_description`` patches
targeting a table the live metadata snapshot did not contain. The
applier dispatched the call, returned ``patch_deployed=False`` with a
typed decision (``dropped_no_op:missing_table``), and the gate emitted
a flat forbidden_signature of
``"add_column_description:dropped_no_op:missing_table"``.

Trial 17 step 4 contract (this test pins it):
- Metadata patches (``add_column_description`` etc.) are **preflighted**
  before the apply call. ``check_patch_applyability`` resolves the
  target table/column against the metadata snapshot and rejects up
  front when the target is missing.
- The terminal ``forbidden_signature`` carries the new format:
  ``"<lever>:<patch_type>:preflight_target_missing:table=<X>"``.

This is RED before Trial 17 steps 3 + 4 land and GREEN afterwards.
"""
from __future__ import annotations

from unittest.mock import patch as mock_patch

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
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


_MISSING_TABLE = "main.demo.airline_ticketing_missing_tbl"


def _missing_table_proposal(intent_id: str = "H001_000") -> RepairProposal:
    """Build an LLM proposal that targets a table not present in the
    metadata snapshot — the canonical gs_024 dead-end shape.
    """
    return RepairProposal(
        intent_id=intent_id,
        intent_name="describe_missing_column",
        intent_description="add description to column on a missing table",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_COLUMN_DESCRIPTION,
        rationale="empirical",
        confidence="high",
        patch_body={
            "table": _MISSING_TABLE,
            "column": "fare_class",
            "description": "Fare class for the leg",
        },
        blame_set=(f"{_MISSING_TABLE}.fare_class",),
        target_qids=("gs_024",),
        selected_lever="lever-1",
    )


def _ctx_with_proposal(
    proposal: RepairProposal,
    *,
    metadata_snapshot: dict,
) -> TransformerContext:
    """Build a TransformerContext with the proposal registered and a
    metadata snapshot wired (so preflight runs)."""
    ctx = TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        space_id="trial17-gs024-space",
        metadata_snapshot=metadata_snapshot,
    )
    ctx.proposal_store.remember(proposal)
    return ctx


def _gs024_state_at_applyable():
    s = build_initial_state(
        qid="gs_024",
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1
        ),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"
        ),
        diagnosed=DiagnosisRecord(
            "plan11_stage1",
            "table_or_column_misroute",
            "rca_gs_024_misroute",
            "f",
            "e",
            "high",
            "r",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"
        ),
        clustered=ClusterMembershipRecord(
            "H001",
            "AG_H001",
            ("gs_024",),
            6,
            "table_or_column_misroute",
        ),
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"
        ),
        proposals=(
            ProposalAttempt(
                0,
                "H001_000",
                "add_column_description",
                FunnelStage.PROPOSED,
                "applied",
                "pending_gates",
            ),
        ),
    )
    s = s.advance(
        FunnelStage.NORMALIZED,
        StageTransition(
            FunnelStage.PROPOSED,
            FunnelStage.NORMALIZED,
            4,
            "structural",
            "validation_gate",
        ),
    )
    return s.advance(
        FunnelStage.APPLYABLE,
        StageTransition(
            FunnelStage.NORMALIZED,
            FunnelStage.APPLYABLE,
            5,
            "blast",
            "batch",
        ),
    )


def test_trial17_gs024_applier_gate_preflight_rejects_missing_table():
    """RED before Trial 17 steps 3 + 4. GREEN after.

    Asserts two contracts at once:
    1. For metadata patches whose target table doesn't exist, the apply
       side effect (``_apply_via_genie_api``) MUST NOT be invoked —
       preflight short-circuits first.
    2. The resulting terminal forbidden_signature follows the Trial 17
       enriched format: ``lever-1:add_column_description:
       preflight_target_missing:table=<X>``.
    """
    s = _gs024_state_at_applyable()
    proposal = _missing_table_proposal(intent_id="H001_000")

    # Metadata snapshot that does NOT contain the proposal's target
    # table. The Trial 17 preflight reads
    # ``ctx.metadata_snapshot`` and rejects up-front. The snapshot
    # shape mirrors what ``check_patch_applyability`` understands:
    # the applier walks ``tables`` and matches by identifier.
    snapshot = {
        "tables": [
            {
                "name": "main.demo.other_table",
                "identifier": "main.demo.other_table",
                "columns": [{"name": "id"}],
            },
        ],
        "schema_columns": ["main.demo.other_table.id"],
    }
    ctx = _ctx_with_proposal(proposal, metadata_snapshot=snapshot)

    # Sentinel for the live apply call. Preflight must short-circuit
    # BEFORE this is invoked; if it isn't, the test fails noisily.
    applier_calls: list[tuple] = []

    def _apply_stub(state, ctx):
        applier_calls.append((state.qid, "applier_invoked"))
        return ("", False, "dropped_no_op:missing_table")

    with mock_patch(
        "genie_space_optimizer.optimization.state_machine.transformers.applier_gate._apply_via_genie_api",
        side_effect=_apply_stub,
    ):
        s2 = applier_gate.transform(s, ctx)

    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal is not None

    sig = s2.terminal.forbidden_signature or ""

    # Trial 17 step 4 contract: preflight short-circuit means the
    # apply side effect was NOT invoked for this unresolvable target.
    # Until step 4 lands, ``_apply_via_genie_api`` IS invoked and this
    # list is non-empty.
    assert applier_calls == [], (
        f"preflight failed to short-circuit; apply was invoked: "
        f"{applier_calls!r}; sig={sig!r}"
    )

    # Trial 17 step 3 contract: enriched signature with lever +
    # preflight_target_missing token.
    assert "lever-1" in sig, f"sig missing lever-1 token: {sig!r}"
    assert "add_column_description" in sig, (
        f"sig missing patch_type: {sig!r}"
    )
    assert "preflight_target_missing" in sig, (
        f"sig missing preflight token: {sig!r}"
    )
    assert "table=" in sig, f"sig missing table= token: {sig!r}"
