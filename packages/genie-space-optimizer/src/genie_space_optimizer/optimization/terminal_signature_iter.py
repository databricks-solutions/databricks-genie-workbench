"""Phase 2 (2026-05-16) — thin adapter that converts Phase 1's
``capture_iter_ag_context`` output dict + a ``TerminalReason`` into
a fully populated :class:`TerminalSignature`.

Eight non-accepted reflection-buffer-write call sites in
``harness.py`` need a ``TerminalSignature`` populated from the same
five iter-locals. Without this adapter each site repeats the same
five-kwarg ``build_terminal_signature`` invocation; the adapter
collapses the pattern to a single import and a single call.

The adapter intentionally does NOT read harness module-level state.
Callers MUST pass the iter-locals dict explicitly. Empty / missing
keys are treated as empty tuples / frozensets — never raised.
"""
from __future__ import annotations

from typing import Any, Mapping

from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
    build_terminal_signature,
)


def terminal_signature_for_iteration(
    *,
    iter_locals: Mapping[str, Any],
    terminal_reason: "TerminalReason | str",
) -> TerminalSignature:
    """Build a ``TerminalSignature`` from Phase 1 iter-locals.

    Args:
        iter_locals: Dict with keys ``root_cause``, ``blame_set``,
            ``levers``, ``target_qids`` (the four signature-relevant
            keys from ``capture_iter_ag_context``). Missing keys are
            treated as empty.
        terminal_reason: ``TerminalReason`` enum value or its
            ``.value`` string. Unknown strings raise ``ValueError``
            (propagated from ``build_terminal_signature``).

    Returns:
        A canonical ``TerminalSignature`` (Section 4.2 of the
        final-closeout contract spec).
    """
    return build_terminal_signature(
        root_cause=iter_locals.get("root_cause") or "",
        blame_set=iter_locals.get("blame_set") or (),
        lever_set=iter_locals.get("levers") or (),
        target_qids=iter_locals.get("target_qids") or (),
        terminal_reason=terminal_reason,
        # Phase 2 P2.5 — forward the kit-aware fields when the
        # acceptance gate has stamped them on the iter-locals dict.
        # ``prior_lever_set`` is the kit composition the LLM
        # actually emitted (lever_id strings) and ``prior_patch_family``
        # is the patch_family of the patch that triggered the
        # terminal. Empty defaults preserve byte-stable behaviour for
        # pre-P2.5 harness callsites that have not yet stamped these
        # keys.
        prior_lever_set=iter_locals.get("prior_lever_set") or (),
        prior_patch_family=iter_locals.get("prior_patch_family") or "",
    )
