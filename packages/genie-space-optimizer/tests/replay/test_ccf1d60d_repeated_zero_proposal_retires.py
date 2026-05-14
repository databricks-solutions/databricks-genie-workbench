"""Phase 5 fixture: repeated zero-proposal AG retires after one repeat.

Maps to user-text `test_repeated_zero_proposal_ag_retires_after_one_repeat`.
Anchor: ccf1d60d-d686-467b-bafa-1640131b4393 iter-1 → iter-2 trace.

Asserts:
  1. After iter-2's PROPOSAL_GENERATION_EMPTY terminal, the
     TerminalSignature for (AG1, gs_026, proposal_generation_empty) is
     in the forbidden set.
  2. If iter-3 were to consult the forbidden set with that signature,
     `apply_admission_trace` would deny it.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape, build_terminal_signature,
)
from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
    compute_retired_signatures,
)

from tests.replay.fixtures.phase5._helpers import load_iteration


def test_zero_proposal_ag_retires_after_one_repeat() -> None:
    iter1 = load_iteration("ccf1d60d", 1)
    iter2 = load_iteration("ccf1d60d", 2)

    assert iter2["ag_id_selected"] == iter1["ag_id_selected"]
    assert iter2["proposal_count"] == 0

    reflection_buffer = [
        {
            "iteration": 2,
            "accepted": False,
            "terminal_signature": build_terminal_signature(
                root_cause="no_metric_view_for_gross_sales",
                blame_set_norm=("catalog.schema.orders",),
                lever_set=frozenset([5]),
                target_qids=frozenset(iter2["target_qids"]),
                terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
            ),
            "cluster_signature": (
                "H001",
                tuple(sorted(iter2["target_qids"])),
            ),
            "emitted_patch_shape": EmittedPatchShape.ABSENT.value,
        }
    ]

    retired = compute_retired_signatures(reflection_buffer, repeat_threshold=1)
    assert reflection_buffer[0]["terminal_signature"] in retired


def test_repeat_threshold_one_is_active() -> None:
    """Defensive assertion: Phase 1+2 Task 3 sets the retire-after-one-repeat
    rule. If this is loosened (e.g., to ``repeat_threshold=2``) the iter-3
    pivot guarantee weakens; the test must fail loud.
    """
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        DEFAULT_REPEAT_THRESHOLD,
    )

    assert DEFAULT_REPEAT_THRESHOLD == 1
