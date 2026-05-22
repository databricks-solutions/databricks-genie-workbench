"""ProposalStore — in-iteration bridge from intent_id → typed RepairProposal.

Stage 3 (synthesize) writes; structural / blast / applier / escalation
gates read. Per-iteration scoped — TransformerContext owns one.
"""
from __future__ import annotations


def _make_proposal(intent_id: str = "intent_abc"):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    return RepairProposal(
        intent_id=intent_id,
        intent_name="add a synonym",
        intent_description="alias for column A",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_COLUMN_SYNONYM,
        rationale="missing alias surfaced as null",
        confidence="high",
        patch_body={
            "object_id": "catalog.schema.table:colA",
            "synonym": "alias_for_a",
        },
        blame_set=("catalog.schema.table:colA",),
        target_qids=("q1",),
    )


def test_empty_store_lookup_returns_none():
    from genie_space_optimizer.optimization.state_machine.proposal_store import (
        ProposalStore,
    )
    store = ProposalStore()
    assert store.lookup("missing_intent") is None


def test_remember_then_lookup_round_trips():
    from genie_space_optimizer.optimization.state_machine.proposal_store import (
        ProposalStore,
    )
    store = ProposalStore()
    proposal = _make_proposal("intent_a1")
    store.remember(proposal)
    retrieved = store.lookup("intent_a1")
    assert retrieved is not None
    assert retrieved.intent_id == "intent_a1"
    assert retrieved.patch_type.value == "add_column_synonym"


def test_two_stores_do_not_share_state():
    from genie_space_optimizer.optimization.state_machine.proposal_store import (
        ProposalStore,
    )
    a = ProposalStore()
    b = ProposalStore()
    a.remember(_make_proposal("intent_a"))
    assert b.lookup("intent_a") is None
    assert a.lookup("intent_a") is not None


def test_last_write_wins_for_same_intent_id():
    """Phase 4 may legitimately rebuild a proposal under the same
    intent_id (e.g., narrow-replacement rebuild). The store must allow
    overwrite without raising."""
    from genie_space_optimizer.optimization.state_machine.proposal_store import (
        ProposalStore,
    )
    store = ProposalStore()
    original = _make_proposal("intent_x")
    store.remember(original)

    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    rebuilt = RepairProposal(
        intent_id="intent_x",
        intent_name="teaching example",
        intent_description="example SQL for clarity",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="add an example",
        confidence="medium",
        patch_body={
            "example_question": "How many rows?",
            "example_sql": "SELECT COUNT(*) FROM t",
        },
        blame_set=(),
        target_qids=("q1",),
    )
    store.remember(rebuilt)
    assert store.lookup("intent_x").patch_type.value == "add_example_sql"


def test_transformer_context_carries_default_proposal_store():
    """Every TransformerContext gets a fresh ProposalStore by default
    so transformers can write into it without explicit wiring in tests."""
    from genie_space_optimizer.optimization.state_machine.proposal_store import (
        ProposalStore,
    )
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext,
        ValidationContext,
    )
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    assert isinstance(ctx.proposal_store, ProposalStore)
    # Default is empty
    assert ctx.proposal_store.lookup("anything") is None


def test_two_contexts_have_independent_proposal_stores():
    """Per-iteration scoping — TransformerContexts must not share a
    proposal store via a shared default mutable."""
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext,
        ValidationContext,
    )
    ctx_a = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    ctx_b = TransformerContext(
        iteration=2, run_id="r",
        validation_context=ValidationContext(2, "r", {}),
    )
    ctx_a.proposal_store.remember(_make_proposal("intent_in_a_only"))
    assert ctx_b.proposal_store.lookup("intent_in_a_only") is None
