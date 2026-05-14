"""Phase 5 cross-run invariant: a single run never repeats a
TerminalSignature across iterations once Phase 1+2 retire memory is on.

Maps to user-text ``test_terminal_signature_never_repeated``. Spans
BOTH the ccf1d60d and 31ecd96f anchors so the gate covers both the
target-not-improved (iter1) -> proposal-generation-empty (iter2)
sequence and the H002 ``no_applied_patches`` alternation sequence.

The contract being enforced is: across the iterations of a single run,
either (a) no ``TerminalSignature`` repeats, or (b) any repeated
signature is in the retired-set as soon as it would re-fire (because
Phase 1+2 Task 6 ``compute_retired_signatures`` admits a signature
after one prior non-accepted appearance). The two clauses are
equivalent in the steady state — clause (b) is the testable form,
because the RECORDED fixtures themselves are pre-Phase-1+2-retire and
DO contain duplicates.

API drift notes (real APIs vs. plan text):
  * ``build_terminal_signature(blame_set=...)`` — the constructor
    parameter is ``blame_set``; ``blame_set_norm`` is the resulting
    tuple field name on the produced :class:`TerminalSignature`.
  * ``compute_retired_signatures(*, reflection_buffer=...)`` is
    keyword-only and takes no ``repeat_threshold`` parameter — the
    semantics is "one prior non-accepted appearance retires."
  * ``terminal_reason`` on the returned ``TerminalSignature`` is
    stored as ``str`` (the enum's ``.value``), not as the
    :class:`TerminalReason` enum itself.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
    compute_retired_signatures,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
    build_terminal_signature,
)

from tests.replay.fixtures.phase5._helpers import load_iteration


def _signature_for_ccf1d60d_iter(payload: dict) -> TerminalSignature:
    """Build the TerminalSignature the run *would* have emitted for a
    ccf1d60d iteration payload. iter1 uses the recorded
    ``reason_code_in_recorded_run``; later iters use
    ``terminal_reason_in_recorded_run``.
    """
    reason = (
        payload.get("terminal_reason_in_recorded_run")
        or payload.get("reason_code_in_recorded_run")
        or "proposal_generation_empty"
    )
    return build_terminal_signature(
        root_cause=payload.get("root_cause", "unknown"),
        blame_set=tuple(payload.get("blame_set_norm", ())),
        lever_set=frozenset(payload.get("lever_set", [])),
        target_qids=frozenset(payload.get("target_qids", [])),
        terminal_reason=TerminalReason(reason),
    )


@pytest.mark.parametrize(
    "anchor,iters",
    [
        ("ccf1d60d", (1, 2)),
        ("31ecd96f", ("alt",)),
    ],
)
def test_no_terminal_signature_repeats_within_run(anchor, iters) -> None:
    """For each anchor, derive the TerminalSignature(s) the run emitted
    and assert no exact duplicate survives without being captured by
    the Phase 1+2 retire set.

    The RECORDED runs (pre-Phase-1+2-retire) DID repeat signatures
    (notably the 31ecd96f H002 ``no_applied_patches`` alternation).
    This test confirms the retire-set closes the loop: any duplicate
    that appears in the synthetic reflection_buffer derived from the
    fixtures MUST be in the frozenset returned by
    :func:`compute_retired_signatures`.
    """
    signatures_seen: list[TerminalSignature] = []
    if anchor == "ccf1d60d":
        for it in iters:
            payload = load_iteration("ccf1d60d", it)
            signatures_seen.append(_signature_for_ccf1d60d_iter(payload))
    else:
        payload = load_iteration("31ecd96f", "alt")
        repeats_in_recorded = payload["terminal_reason_sequence_for_h002"]
        h002_qids = frozenset(payload["h002_target_qids"])
        for r in repeats_in_recorded:
            signatures_seen.append(
                build_terminal_signature(
                    root_cause="missing_measure",
                    blame_set=("catalog.schema.tkt_payment",),
                    lever_set=frozenset([6]),
                    target_qids=h002_qids,
                    terminal_reason=TerminalReason(r),
                )
            )

    duplicates = [
        s for s in signatures_seen if signatures_seen.count(s) > 1
    ]

    if not duplicates:
        # Clause (a) of the contract holds: no repeats at all.
        return

    # Clause (b): every repeat must be in the retired-set. Synthesize a
    # reflection_buffer that mirrors the recorded sequence and feed it
    # to ``compute_retired_signatures``.
    reflection = [
        {
            "iteration": i,
            "accepted": False,
            "terminal_signature": sig,
            "cluster_signature": (anchor, ()),
            "emitted_patch_shape": "absent",
        }
        for i, sig in enumerate(signatures_seen, start=1)
    ]
    retired = compute_retired_signatures(reflection_buffer=reflection)

    for dup in set(duplicates):
        assert dup in retired, (
            f"signature {dup!r} repeated in {anchor} run but NOT in "
            f"retired set — retire memory failed to close the loop"
        )
