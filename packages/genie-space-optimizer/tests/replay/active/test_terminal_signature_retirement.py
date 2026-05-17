"""Phase 4 Test 2 (2026-05-16) — terminal-signature retirement
active replay against Run B (59a173d3) iter 1 → iter 3.

User-spec contract (Phase 2 exit criterion):
  Run B's airline fixture replayed past iter 1 →
  compute_retired_signatures returns non-empty →
  iter-3 AG selection rejects the H001 signature because it's
  retired → iter 3 picks a different AG or emits a typed terminal.

This test drives the full Phase 2 producer-consumer chain:

    capture_iter_ag_context          (Phase 1)
        ↓
    terminal_signature_for_iteration (Phase 2 adapter)
        ↓
    _build_reflection_entry(...,     (Phase 2 wired kwarg)
        terminal_signature=...)
        ↓
    compute_retired_signatures       (existing consumer,
                                      asserted against Phase-2-
                                      produced buffer)
        ↓
    iter-3 AG-selection check        (signature membership in
                                      retired set)

Unlike ``tests/replay/test_ccf1d60d_repeated_zero_proposal_retires.py``
which constructs the reflection entry manually, this test invokes
the actual producer chain with inputs derived from the real
postmortem fixture (Run B's H001 cluster identity).

**Failure mode on current main (before Phase 2):**
  ``TypeError: _build_reflection_entry() got an unexpected keyword
  argument 'terminal_signature'`` (Phase 2 Task 3 adds the kwarg).

**Pass mode after Phase 2 (current state):**
  The producer chain emits an entry whose ``terminal_signature``
  matches the H001 identity; ``compute_retired_signatures`` returns
  it; iter-3's candidate signature membership check hits.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
    compute_retired_signatures,
)
from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
)
from genie_space_optimizer.optimization.iteration_ag_context import (
    capture_iter_ag_context,
)
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
)
from genie_space_optimizer.optimization.terminal_signature_iter import (
    terminal_signature_for_iteration,
)

from tests.replay.active._postmortem_fixtures import (
    get_cluster_by_id,
    get_iteration,
    load_run_b_59a173d3,
)


# Run B (59a173d3) iter-1 H001 anchor identity values — confirmed
# by reading the fixture during Phase 4 audit.
RUN_B_AG_ID = "AG_PIPELINE"
RUN_B_H001_QID = "airline_ticketing_and_fare_analysis_gs_009"
RUN_B_H001_ROOT_CAUSE = "plural_top_n_collapse"
# The H001 cluster's RCA dictates lever 5 (example-SQL synthesis).
RUN_B_H001_LEVER = 5
# Canonical blame asset for the airline domain; the fixture doesn't
# record this directly, so we pin the value the Phase 2 wiring
# captures from the AG dict.
RUN_B_BLAME_ASSET = "catalog.airline.fact_bookings"


def _iter_locals_for_h001(iteration: dict) -> dict:
    """Derive the iter-locals dict ``capture_iter_ag_context`` would
    produce for the H001 cluster's AG slice. Run B's strategist
    emitted ``AG_PIPELINE`` covering both H001 and H002 clusters;
    for the retirement test, we focus on H001 because the user-spec
    contract asserts iter-3 cannot re-target H001's signature."""
    cluster = get_cluster_by_id(iteration, "H001")
    target_qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if str(q)
    )
    ag = {
        "id": RUN_B_AG_ID,
        "source_cluster_ids": [str(cluster.get("cluster_id") or "H001")],
        "target_qids": list(target_qids),
        "affected_questions": list(target_qids),
        "lever_directives": {str(RUN_B_H001_LEVER): {}},
        "root_cause_summary": str(cluster.get("root_cause") or ""),
        "blame_set": [RUN_B_BLAME_ASSET],
    }
    return capture_iter_ag_context(ag=ag, ag_id=RUN_B_AG_ID)


def _build_terminal_reflection_entry(
    iter_locals: dict, *, iteration: int,
) -> dict:
    """Drive the producer chain end-to-end:
    iter_locals → TerminalSignature → reflection-buffer entry.

    Run B's iter 1 emitted a ``proposal_failure_decided`` record
    with ``reason_code=request_evidence_gathering``. The closest
    TerminalReason for "strategist returned an AG but no proposals
    were generated for it" is PROPOSAL_GENERATION_EMPTY."""
    sig = terminal_signature_for_iteration(
        iter_locals=iter_locals,
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    return _build_reflection_entry(
        iteration=iteration,
        ag_id=iter_locals["ag_id"],
        accepted=False,
        levers=list(iter_locals["levers"]),
        target_objects=list(iter_locals["target_qids"]),
        prev_scores={iter_locals["target_qids"][0]: 0.0},
        new_scores={iter_locals["target_qids"][0]: 0.0},
        rollback_reason="request_evidence_gathering",
        patches=[],
        affected_question_ids=list(iter_locals["target_qids"]),
        prev_failure_qids=set(iter_locals["target_qids"]),
        new_failure_qids=set(iter_locals["target_qids"]),
        reflection_text=(
            "AG_PIPELINE proposal_failure_decided with "
            "reason=request_evidence_gathering — no proposals "
            "generated, no patches applied."
        ),
        refinement_mode="out_of_plan",
        escalation_handled=False,
        root_cause=iter_locals["root_cause"],
        blame_set=tuple(iter_locals["blame_set"]),
        source_cluster_ids=list(iter_locals["cluster_ids"]),
        terminal_signature=sig,
    )


def test_iter_locals_capture_run_b_h001_identity():
    """Phase 1+2 producer chain step 1: ``capture_iter_ag_context``
    extracts the H001 identity fields from the AG."""
    it1 = get_iteration(load_run_b_59a173d3(), 1)
    iter_locals = _iter_locals_for_h001(it1)
    assert iter_locals["ag_id"] == RUN_B_AG_ID
    assert iter_locals["target_qids"] == (RUN_B_H001_QID,)
    assert iter_locals["levers"] == (RUN_B_H001_LEVER,)
    assert iter_locals["root_cause"] == RUN_B_H001_ROOT_CAUSE
    assert iter_locals["blame_set"] == (RUN_B_BLAME_ASSET,)


def test_reflection_entry_carries_terminal_signature_from_chain():
    """Phase 2 producer chain step 2: the reflection entry the
    producer chain emits carries a populated ``terminal_signature``
    with the H001 identity."""
    it1 = get_iteration(load_run_b_59a173d3(), 1)
    iter_locals = _iter_locals_for_h001(it1)
    entry = _build_terminal_reflection_entry(iter_locals, iteration=1)

    assert entry["accepted"] is False
    assert "terminal_signature" in entry, (
        "_build_reflection_entry must surface the terminal_signature "
        "kwarg in the entry dict (Phase 2 Task 3 contract)."
    )
    sig = entry["terminal_signature"]
    assert isinstance(sig, TerminalSignature)
    assert sig.target_qids == frozenset({RUN_B_H001_QID})
    assert sig.lever_set == frozenset({RUN_B_H001_LEVER})
    assert sig.root_cause == RUN_B_H001_ROOT_CAUSE
    assert sig.blame_set_norm == (RUN_B_BLAME_ASSET,)


def test_compute_retired_signatures_consumes_producer_output():
    """Phase 2 producer-consumer chain step 3: the entries the
    producer chain emits, when fed to ``compute_retired_signatures``,
    retire the H001 signature."""
    fixture = load_run_b_59a173d3()
    it1 = get_iteration(fixture, 1)
    it2 = get_iteration(fixture, 2)

    iter1_locals = _iter_locals_for_h001(it1)
    iter2_locals = _iter_locals_for_h001(it2)

    iter1_entry = _build_terminal_reflection_entry(iter1_locals, iteration=1)
    iter2_entry = _build_terminal_reflection_entry(iter2_locals, iteration=2)

    # The producer chain emits identical signatures for identical
    # AG identity inputs.
    assert iter1_entry["terminal_signature"] == iter2_entry["terminal_signature"]

    retired = compute_retired_signatures(
        reflection_buffer=[iter1_entry, iter2_entry],
    )
    assert iter1_entry["terminal_signature"] in retired, (
        f"compute_retired_signatures did NOT retire the H001 "
        f"signature. Retired set: {list(retired)}"
    )


def test_iter_3_candidate_signature_matches_retired_h001():
    """Phase 2 producer-consumer chain step 4: when iter 3's
    AG-selection consults the forbidden set, a candidate AG that
    would target H001 has a TerminalSignature that IS in the retired
    set. This is the user-spec exit criterion verbatim."""
    fixture = load_run_b_59a173d3()
    it1 = get_iteration(fixture, 1)
    it2 = get_iteration(fixture, 2)
    it3 = get_iteration(fixture, 3)

    iter1_entry = _build_terminal_reflection_entry(
        _iter_locals_for_h001(it1), iteration=1,
    )
    iter2_entry = _build_terminal_reflection_entry(
        _iter_locals_for_h001(it2), iteration=2,
    )
    retired = compute_retired_signatures(
        reflection_buffer=[iter1_entry, iter2_entry],
    )
    assert len(retired) >= 1, (
        "Expected at least one retired signature after iter 1+2 "
        "produce identical terminals. The forbidden set is empty; "
        "Phase 2's producer chain or the consumer is broken."
    )

    # Iter 3 AG-selection's forbidden-set check: derive the H001
    # candidate's TerminalSignature and assert it IS in the retired
    # set — the harness would reject this AG.
    iter3_locals = _iter_locals_for_h001(it3)
    iter3_candidate_sig = terminal_signature_for_iteration(
        iter_locals=iter3_locals,
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    assert iter3_candidate_sig in retired, (
        f"Iter 3's H001 candidate signature is NOT in the retired "
        f"set. iter3_sig={iter3_candidate_sig!r}; "
        f"retired={list(retired)}. The forbidden-set check would "
        f"WRONGLY admit iter 3 to re-select the same shape."
    )


def test_retired_set_excludes_accepted_entries():
    """Defensive: an accepted entry with the same signature must
    NOT contribute to the retired set. Pins ``compute_retired_
    signatures`` contract: only ``accepted=False`` entries retire."""
    it1 = get_iteration(load_run_b_59a173d3(), 1)
    iter_locals = _iter_locals_for_h001(it1)
    entry = _build_terminal_reflection_entry(iter_locals, iteration=1)

    accepted_clone = dict(entry)
    accepted_clone["accepted"] = True
    retired = compute_retired_signatures(
        reflection_buffer=[accepted_clone, accepted_clone],
    )
    assert retired == frozenset(), (
        f"compute_retired_signatures retired a signature from an "
        f"accepted entry. retired={retired!r}"
    )
