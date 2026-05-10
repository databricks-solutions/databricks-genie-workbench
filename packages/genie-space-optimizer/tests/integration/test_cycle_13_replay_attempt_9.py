"""Cycle 13 — anchor 644133565666745 attempt 9 replay.

Reproduces the iter 1 → iter 2 transition that today wastes
budget on AG1 re-emission. Constructs a minimum-viable reflection
buffer matching the anchor's iter 1 identity (per postmortem F6)
and asserts:

1. _compute_forbidden_ag_set returns AG1's signature with the
   flag on.
2. The same signature is rejected from the set with the flag off
   (replay byte-stability).
3. _ag_collision_key produces the same tuple iter 2's strategist
   would emit, so the harness collision guard intercepts.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _ag_collision_key,
    _build_reflection_entry,
    _compute_forbidden_ag_set,
)


# Identity fields for AG1 per anchor 644133565666745 attempt 9
# postmortem F6 (zone-VP plural top-N failure on H002 / gs_026).
ANCHOR_AG1_ROOT_CAUSE = "plural_top_n_collapse"
ANCHOR_AG1_BLAME = ["mv_esr_dim_location.zone_vp_name"]
ANCHOR_AG1_LEVERS = [1, 5, 6]
ANCHOR_AG1_CLUSTER_IDS = ["H002"]


def _anchor_iter1_no_proposals_entry() -> dict:
    """Iter 1 of attempt 9 produced 0 proposals after the L6 SQL
    expression was dropped at patch cap. Per post-T5 fix, the
    reflection entry now carries the AG's lever_set."""
    return _build_reflection_entry(
        iteration=1, ag_id="AG1", accepted=False,
        levers=ANCHOR_AG1_LEVERS,
        target_objects=["mv_esr_dim_location"],
        prev_scores={"result_correctness": 78.3},
        new_scores={"result_correctness": 78.3},
        rollback_reason="no_proposals",
        patches=[],
        root_cause=ANCHOR_AG1_ROOT_CAUSE,
        blame_set=ANCHOR_AG1_BLAME,
        source_cluster_ids=ANCHOR_AG1_CLUSTER_IDS,
    )


def test_anchor_iter1_to_iter2_intercepted_with_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    reflection_buffer = [_anchor_iter1_no_proposals_entry()]

    forbidden = _compute_forbidden_ag_set(reflection_buffer)
    expected_signature = (
        ANCHOR_AG1_ROOT_CAUSE,
        ("mv_esr_dim_location.zone_vp_name",),
        frozenset({1, 5, 6}),
    )
    assert expected_signature in forbidden

    # Iter 2: strategist re-emits AG1 (anchor F6 says it did
    # this 4 times in a row). The collision-key matches.
    iter2_key = _ag_collision_key(
        ag={"source_cluster_ids": ANCHOR_AG1_CLUSTER_IDS},
        ag_root_cause=ANCHOR_AG1_ROOT_CAUSE,
        ag_blame_set=ANCHOR_AG1_BLAME,
        lever_keys=[str(l) for l in ANCHOR_AG1_LEVERS],
    )
    assert iter2_key == expected_signature
    assert iter2_key in forbidden


def test_anchor_iter1_to_iter2_unchanged_with_flag_off(monkeypatch) -> None:
    """Replay byte-stability: pre-C13 fixtures still pass under
    explicit ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0`` override.
    Cycle 14-W T4 flipped the default to ON; this test exercises
    the operator-override escape hatch for replaying pre-14-W
    fixtures byte-stable."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")
    reflection_buffer = [_anchor_iter1_no_proposals_entry()]
    assert _compute_forbidden_ag_set(reflection_buffer) == set()
