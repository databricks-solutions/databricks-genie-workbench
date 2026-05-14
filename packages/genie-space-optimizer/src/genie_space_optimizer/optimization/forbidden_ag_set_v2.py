"""Phase 1.3 — terminal-signature-keyed retry memory.

Pure helpers used by ``_compute_forbidden_ag_set`` in harness.py.
The legacy patch-only memory (CONTENT_REGRESSION filter) stays in
the harness for the flag-off path; this module is the flag-on path.

The retire predicate is keyed on :class:`TerminalSignature` (the
5-tuple from spec Section 4) so signatures that differ in
``target_qids`` or ``terminal_reason`` no longer collapse into the
same forbidden entry — eliminating the iter-2..iter-5 retry storm
on the same ``NO_APPLIED_PATCHES`` / ``PROPOSAL_GENERATION_EMPTY``
strategy that today's patch-only memory permits.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
)


def _is_accepted_reason(terminal_reason: str) -> bool:
    """Defensive guard. ``TerminalReason`` enumerates *no-candidate*
    paths only (spec Section 3.2: 'For accepted / rolled-back
    outcomes, use ``AcceptanceTier`` ... The two vocabularies are
    disjoint and complementary.'). A value present on a
    :class:`TerminalSignature` is therefore always a non-accepted
    cause. We keep this hook so future producers that widen the
    vocabulary have a single guarded admission point.
    """
    # Currently no TerminalReason value is an accepted reason. Kept
    # as a predicate to preserve the call-site shape from the plan.
    try:
        TerminalReason(terminal_reason)
    except ValueError:
        # Unknown reasons are treated as non-accepted (conservative:
        # still admit to the retired set).
        return False
    return False


def compute_retired_signatures(
    *,
    reflection_buffer: Sequence[Mapping[str, Any]],
) -> frozenset[TerminalSignature]:
    """Return the set of TerminalSignatures retired by the block rule
    (one prior non-accepted appearance).

    Reads ``GSO_TERMINAL_SIGNATURE_RETIRE`` env flag for the legacy
    fallback path (when off, only signatures whose ``rollback_class``
    is ``content_regression`` are retired — pre-Phase-1 behavior).
    """
    import os
    flag_on = str(os.environ.get("GSO_TERMINAL_SIGNATURE_RETIRE", "1")) != "0"

    retired: set[TerminalSignature] = set()
    for entry in (reflection_buffer or ()):
        if bool(entry.get("accepted")):
            continue
        sig = entry.get("terminal_signature")
        if not isinstance(sig, TerminalSignature):
            continue
        if _is_accepted_reason(sig.terminal_reason):
            continue
        if flag_on:
            retired.add(sig)
        else:
            # Legacy: only retire on CONTENT_REGRESSION
            rollback = str(entry.get("rollback_class") or "")
            if rollback == "content_regression":
                retired.add(sig)
    return frozenset(retired)


def is_signature_retired(
    *,
    candidate_signature: TerminalSignature,
    retired: frozenset[TerminalSignature],
) -> bool:
    """True iff ``candidate_signature`` appears in ``retired``."""
    return candidate_signature in retired
