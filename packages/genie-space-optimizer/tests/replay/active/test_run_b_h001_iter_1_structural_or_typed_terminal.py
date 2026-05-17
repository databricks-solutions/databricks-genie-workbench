"""Phase 3 (2026-05-16) — Run B H001 iter-1 outcome contract.

Run B's iter-1 selected ``AG_DECOMPOSED_H001`` with target
``gs_009`` and lever L5 (rich-synthesis primary under
``GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE=1``). The user-spec
contract requires either:

(a) Rich path produces a structural candidate → reflection entry
    with ``accepted=True`` and at least one applied
    ``add_example_sql`` (or other structural-shape) patch.
(b) Iteration emits a typed terminal → reflection entry with
    ``accepted=False`` AND a populated ``terminal_signature``
    whose ``target_qids == frozenset({"gs_009"})`` and
    ``lever_set == frozenset({5})``.

Either is a valid recovery; the contract is "structural candidate
OR retired signature, never repeat shape across iterations".

Because the rich-synthesis path requires live model serving, this
test exercises the contract on the **reflection-buffer entry shape**.
It constructs both candidate outcomes through the actual
``_build_reflection_entry`` (Phase 2) and asserts (i) each is
well-formed under its respective branch, (ii) the typed-terminal
branch carries the exact H001 identity fields the user spec
requires.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
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


# Run B H001 iter-1 fixture constants (anchored in the Phase 3 spec).
H001_TARGET_QID = "gs_009"
H001_LEVER = 5
H001_AG_ID = "AG_DECOMPOSED_H001"
H001_CLUSTER_ID = "H001"
H001_ROOT_CAUSE = "missing_example_sql_for_route_revenue"
H001_BLAME_ASSET = "catalog.airline.fact_bookings"


def _h001_iter_locals() -> dict:
    """Phase 1's ``capture_iter_ag_context`` output for H001 iter-1."""
    return {
        "ag_id": H001_AG_ID,
        "cluster_ids": (H001_CLUSTER_ID,),
        "target_qids": (H001_TARGET_QID,),
        "levers": (H001_LEVER,),
        "root_cause": H001_ROOT_CAUSE,
        "blame_set": (H001_BLAME_ASSET,),
    }


def _path_a_entry_structural_candidate_accepted() -> dict:
    """Outcome (a) — rich path produced a structural candidate.

    Entry shape: accepted=True, structural-shape patch applied."""
    return _build_reflection_entry(
        iteration=1,
        ag_id=H001_AG_ID,
        accepted=True,
        levers=[H001_LEVER],
        target_objects=[H001_TARGET_QID],
        prev_scores={H001_TARGET_QID: 0.0, "gs_010": 1.0},
        new_scores={H001_TARGET_QID: 1.0, "gs_010": 1.0},
        rollback_reason=None,
        patches=[
            {
                "patch_type": "add_example_sql",
                "target": "ROUTE_REVENUE",
                "example_question": "What is the revenue by route in Q3?",
                "example_sql": (
                    "SELECT route_id, SUM(amount) AS revenue "
                    "FROM fact_bookings WHERE quarter = 3 "
                    "GROUP BY route_id"
                ),
                "proposal_id": "P_L5_RICH_H001_GS_009",
            },
        ],
        affected_question_ids=[H001_TARGET_QID],
        prev_failure_qids={H001_TARGET_QID},
        new_failure_qids=set(),
        reflection_text=(
            "Rich-synthesis primary produced a structural "
            "candidate for AG_DECOMPOSED_H001; gs_009 fixed."
        ),
        refinement_mode="in_plan",
        escalation_handled=False,
        root_cause=H001_ROOT_CAUSE,
        blame_set=(H001_BLAME_ASSET,),
        source_cluster_ids=[H001_CLUSTER_ID],
        # No terminal_signature: accepted path does not carry one.
    )


def _path_b_entry_typed_terminal() -> dict:
    """Outcome (b) — iteration emitted a typed terminal.

    Entry shape: accepted=False, terminal_signature with the
    user-spec identity fields. NO_STRUCTURAL_CANDIDATE is a
    plausible terminal reason for the case where rich synthesis
    produces no structural-shape proposal."""
    return _build_reflection_entry(
        iteration=1,
        ag_id=H001_AG_ID,
        accepted=False,
        levers=[H001_LEVER],
        target_objects=[H001_TARGET_QID],
        prev_scores={H001_TARGET_QID: 0.0},
        new_scores={H001_TARGET_QID: 0.0},
        rollback_reason="no_structural_candidate",
        patches=[],
        affected_question_ids=[H001_TARGET_QID],
        prev_failure_qids={H001_TARGET_QID},
        new_failure_qids={H001_TARGET_QID},
        reflection_text=(
            "Rich-synthesis primary returned zero structural "
            "candidates for AG_DECOMPOSED_H001."
        ),
        refinement_mode="out_of_plan",
        escalation_handled=False,
        root_cause=H001_ROOT_CAUSE,
        blame_set=(H001_BLAME_ASSET,),
        source_cluster_ids=[H001_CLUSTER_ID],
        terminal_signature=terminal_signature_for_iteration(
            iter_locals=_h001_iter_locals(),
            terminal_reason=TerminalReason.NO_STRUCTURAL_CANDIDATE,
        ),
    )


def test_path_a_entry_is_accepted_and_carries_structural_patch():
    entry = _path_a_entry_structural_candidate_accepted()
    assert entry["accepted"] is True
    assert entry["ag_id"] == H001_AG_ID
    # No terminal_signature on accepted entries (per Phase 2 contract).
    assert "terminal_signature" not in entry
    # The accepted entry must record the target as "fixed".
    assert H001_TARGET_QID in entry.get("fixed_questions", [])


def test_path_b_entry_carries_terminal_signature_with_h001_identity():
    """The user-spec assertion for outcome (b): the typed terminal's
    ``terminal_signature.target_qids`` must equal
    ``frozenset({"gs_009"})`` AND ``lever_set`` must equal
    ``frozenset({5})``."""
    entry = _path_b_entry_typed_terminal()
    assert entry["accepted"] is False
    assert "terminal_signature" in entry, (
        "_build_reflection_entry must surface 'terminal_signature' "
        "when caller passes the kwarg. Got keys: "
        f"{sorted(entry.keys())}"
    )
    sig: TerminalSignature = entry["terminal_signature"]
    assert isinstance(sig, TerminalSignature)

    # User-spec exact assertion #1.
    assert sig.target_qids == frozenset({H001_TARGET_QID}), (
        f"terminal_signature.target_qids must equal frozenset"
        f"({{{H001_TARGET_QID!r}}}); got {sig.target_qids!r}"
    )

    # User-spec exact assertion #2.
    assert sig.lever_set == frozenset({H001_LEVER}), (
        f"terminal_signature.lever_set must equal frozenset"
        f"({{{H001_LEVER}}}); got {sig.lever_set!r}"
    )

    # Anchor the other three signature fields so a regression in
    # any one of them surfaces here too.
    assert sig.root_cause == H001_ROOT_CAUSE
    assert sig.blame_set_norm == (H001_BLAME_ASSET,)
    assert sig.terminal_reason == TerminalReason.NO_STRUCTURAL_CANDIDATE.value


def test_one_of_paths_must_be_well_formed():
    """The OR contract: at least one of the two paths must be
    well-formed. This pins the spec's "structural candidate OR
    retired signature" guarantee for the entry shape."""
    path_a_ok = False
    path_b_ok = False
    try:
        entry_a = _path_a_entry_structural_candidate_accepted()
        path_a_ok = bool(entry_a["accepted"]) and bool(entry_a["ag_id"])
    except Exception:
        pass
    try:
        entry_b = _path_b_entry_typed_terminal()
        sig = entry_b.get("terminal_signature")
        path_b_ok = (
            entry_b["accepted"] is False
            and isinstance(sig, TerminalSignature)
            and sig.target_qids == frozenset({H001_TARGET_QID})
            and sig.lever_set == frozenset({H001_LEVER})
        )
    except Exception:
        pass

    assert path_a_ok or path_b_ok, (
        "Neither path-A (accepted+structural) nor path-B (typed "
        "terminal with H001 signature) was well-formed."
    )


def test_path_b_entry_retires_via_compute_retired_signatures():
    """End-to-end: feeding the path-B entry through
    ``compute_retired_signatures`` must yield the H001 signature
    in the retired set."""
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    entry = _path_b_entry_typed_terminal()
    retired = compute_retired_signatures(
        reflection_buffer=[entry],
    )
    assert isinstance(retired, frozenset)
    assert len(retired) >= 1
    matching = [
        s for s in retired
        if s.target_qids == frozenset({H001_TARGET_QID})
        and s.lever_set == frozenset({H001_LEVER})
    ]
    assert matching, (
        f"Expected the retired set to contain a signature with "
        f"target_qids={{'gs_009'}} and lever_set={{5}}. Got: "
        f"{list(retired)}"
    )


def test_path_a_entry_does_not_retire_signature():
    """Defensive cross-check: the accepted path does NOT retire
    anything. ``compute_retired_signatures`` skips accepted entries."""
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    entry = _path_a_entry_structural_candidate_accepted()
    retired = compute_retired_signatures(
        reflection_buffer=[entry],
    )
    assert retired == frozenset(), (
        f"Accepted path-A entry must not contribute to the retired "
        f"set. Got: {retired!r}"
    )
