"""Phase 5 fixture: AG that produces no_applied_patches twice gets retired.

Maps to user-text ``test_repeated_no_applied_patches_retires``.
Anchor: 31ecd96f iter-2 -> iter-4 H002 alternation.

API drift from the plan's literal source (Task 10, plan
2026-05-14-final-closeout-phase-5-6-offline-gates.md):

* ``build_terminal_signature`` takes the keyword ``blame_set`` (the
  constructor performs sort+normalize internally and stores the
  result on the ``blame_set_norm`` field). The plan's snippet wrote
  ``blame_set_norm=`` directly, which is the field name, not the
  parameter name.

* ``compute_retired_signatures`` is keyword-only
  (``*, reflection_buffer=``) and the retire rule is "one prior
  non-accepted appearance" (see ``forbidden_ag_set_v2.py`` docstring
  and Phase 1.3 spec Section 4) -- there is no ``repeat_threshold``
  parameter. The plan's ``repeat_threshold=1`` lines up with the
  production "one prior" semantics for this fixture: the two-entry
  reflection_buffer here still retires the H002 signature, because
  the iter-2 entry alone is sufficient by the time iter-4 hits
  admission. We keep the two-entry buffer to preserve the
  alternation framing from the postmortem.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
    compute_retired_signatures,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    build_terminal_signature,
)

from tests.replay.fixtures.phase5._helpers import load


def test_h002_retires_after_two_no_applied_patches() -> None:
    alt = load("31ecd96f_iter2_iter4_alternation.json")
    assert alt["ag_selection_sequence"] == ["H002", "H001", "H002"]

    h002_sig = build_terminal_signature(
        root_cause="missing_measure",
        blame_set=("catalog.schema.tkt_payment",),
        lever_set=frozenset([6]),
        target_qids=frozenset(alt["h002_target_qids"]),
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )

    reflection_buffer = [
        {
            "iteration": 2,
            "accepted": False,
            "terminal_signature": h002_sig,
            "cluster_signature": ("H002", tuple(alt["h002_target_qids"])),
            "emitted_patch_shape": EmittedPatchShape.ABSENT.value,
        },
        {
            "iteration": 3,
            "accepted": False,
            "terminal_signature": build_terminal_signature(
                root_cause="no_rca_ground",
                blame_set=(),
                lever_set=frozenset(),
                target_qids=frozenset(["gs_009"]),
                terminal_reason=TerminalReason.NO_RCA_GROUND,
            ),
            "cluster_signature": ("H001", ("gs_009",)),
            "emitted_patch_shape": EmittedPatchShape.ABSENT.value,
        },
    ]

    retired = compute_retired_signatures(reflection_buffer=reflection_buffer)
    assert h002_sig in retired, (
        "H002 with NO_APPLIED_PATCHES from iter 2 must be retired "
        "by the time iter 4 reaches admission; "
        f"retired set: {retired}"
    )
