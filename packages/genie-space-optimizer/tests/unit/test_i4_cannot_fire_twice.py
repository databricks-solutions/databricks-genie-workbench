"""Cycle 13 — I4 cannot fire twice in one run after C13 ships.

I4 (consecutive_empty_proposals_same_ag) fires when iteration N+1
re-emits the same AG signature that produced zero proposals on
iteration N. After C13 + GSO_FORBIDDEN_AG_ADMITS_NO_ACTION:

  iter 1: AG1 picked, 0 proposals -> reflection entry classifies
          as NO_ACTION, contributes (root_cause, blame, levers)
          to forbidden set.

  iter 2: strategist emits same AG1; harness collision guard
          intercepts (AG_COLLISION_SKIPPED), the iteration
          terminates with reason="ag_collision_with_forbidden_set"
          BEFORE the proposer runs. Zero proposals are NOT
          emitted on iter 2 because the proposer never runs.
          I4 cannot fire on iter 2.

This test exercises the predicate + collision-key together to
prove the structural property without booting a full harness.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _ag_collision_key,
    _build_reflection_entry,
    _compute_forbidden_ag_set,
)


def test_i4_cannot_fire_twice_in_one_run_after_cycle_13(monkeypatch) -> None:
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")

    # Iter 1: AG1 produces zero proposals. Reflection entry is
    # built per the post-T5 call site (lever_keys propagated).
    iter1_entry = _build_reflection_entry(
        iteration=1, ag_id="AG1", accepted=False,
        levers=[5, 6], target_objects=[],
        prev_scores={"result_correctness": 78.3},
        new_scores={"result_correctness": 78.3},
        rollback_reason="no_proposals",
        patches=[],
        root_cause="plural_top_n_collapse",
        blame_set=["zone_vp_name"],
        source_cluster_ids=["C001"],
    )
    reflection_buffer = [iter1_entry]

    # Forbidden set after iter 1: must contain AG1's signature.
    forbidden = _compute_forbidden_ag_set(reflection_buffer)
    assert (
        "plural_top_n_collapse",
        ("zone_vp_name",),
        frozenset({5, 6}),
    ) in forbidden

    # Iter 2: strategist emits the same AG identity. Harness
    # collision-key check fires before the proposer runs.
    iter2_collision_key = _ag_collision_key(
        ag={"source_cluster_ids": ["C001"]},
        ag_root_cause="plural_top_n_collapse",
        ag_blame_set=["zone_vp_name"],
        lever_keys=["5", "6"],
    )
    assert iter2_collision_key is not None
    assert iter2_collision_key in forbidden

    # I4 cannot fire on iter 2 because the proposer never runs:
    # the harness path at harness.py:16994-17065 short-circuits
    # to AG_COLLISION_SKIPPED before reaching the proposer.
    # Asserting "I4 cannot fire" via the structural-precondition
    # check is the minimum-viable proof; the alternative (full
    # harness boot) is covered by Task 8.


def test_i4_can_still_fire_on_flag_off(monkeypatch) -> None:
    """Replay byte-stability: with the flag explicitly off
    (``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0``), the legacy
    behaviour (I4 fires on iter 2) is preserved. Cycle 14-W T4
    flipped the default ON; the explicit override exercises the
    pre-14-W behaviour."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")
    iter1_entry = _build_reflection_entry(
        iteration=1, ag_id="AG1", accepted=False,
        levers=[5, 6], target_objects=[],
        prev_scores={"result_correctness": 78.3},
        new_scores={"result_correctness": 78.3},
        rollback_reason="no_proposals",
        patches=[],
        root_cause="plural_top_n_collapse",
        blame_set=["zone_vp_name"],
        source_cluster_ids=["C001"],
    )
    reflection_buffer = [iter1_entry]
    forbidden = _compute_forbidden_ag_set(reflection_buffer)
    # Pre-C13 / flag-off: NO_ACTION reflection is excluded; the
    # iter 2 same-signature AG is NOT in the forbidden set, so
    # the proposer would run again and (most likely) emit zero
    # proposals again, firing I4 a second time.
    assert forbidden == set()
