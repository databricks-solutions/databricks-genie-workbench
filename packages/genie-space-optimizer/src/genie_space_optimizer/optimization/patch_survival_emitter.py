"""Plan 12 — single canonical emission point for GSO_PATCH_OUTCOME_V1.

Enforces idempotency per ``(optimization_run_id, iteration, ag_id,
intent_id)`` via a process-local set. The harness MUST call
``reset_patch_outcome_emitter()`` at the start of every iteration so
the cache does not bleed across replay tests or iteration boundaries.

Invariant I22 enforces 1:1 coverage between Stage 3 ``proposal_id`` and
``GSO_PATCH_OUTCOME_V1`` markers; this emitter prevents the double-emit
half of the I22 violation surface (a missing emit at any terminal site
is still surfaced by I22).
"""
from __future__ import annotations

import threading
from typing import Iterable

from genie_space_optimizer.optimization.patch_outcome import (
    PatchOutcomeKind,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    patch_outcome_marker,
)

_EMITTED_KEYS: set[tuple[str, int, str, str]] = set()
_LOCK = threading.Lock()


def reset_patch_outcome_emitter() -> None:
    """Clear the idempotency cache. Call at iteration start so the
    next iteration's intent_ids are not silently suppressed."""
    with _LOCK:
        _EMITTED_KEYS.clear()


def emit_patch_outcome(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    intent_id: str,
    outcome_kind: PatchOutcomeKind,
    terminal_reason: str = "",
    validator_errors: Iterable[str] = (),
    collateral_qids: Iterable[str] = (),
    narrow_replacement_attempted: bool = False,
    narrow_outcome: str = "",
    applied_patch_id: str = "",
) -> bool:
    """Emit exactly one ``GSO_PATCH_OUTCOME_V1`` per
    ``(run_id, iteration, ag_id, intent_id)``.

    Returns ``True`` if this call produced output, ``False`` if the key
    was already emitted (silent suppress). Returning ``False`` is the
    intended behavior — terminal sites can call this defensively without
    coordinating with each other.
    """
    key = (
        str(optimization_run_id),
        int(iteration),
        str(ag_id),
        str(intent_id),
    )
    with _LOCK:
        if key in _EMITTED_KEYS:
            return False
        _EMITTED_KEYS.add(key)

    print(
        patch_outcome_marker(
            optimization_run_id=str(optimization_run_id),
            iteration=int(iteration),
            ag_id=str(ag_id),
            cluster_id=str(cluster_id),
            intent_id=str(intent_id),
            outcome_kind=str(outcome_kind.value),
            terminal_reason=str(terminal_reason),
            validator_errors=[str(e) for e in (validator_errors or ())],
            collateral_qids=[str(q) for q in (collateral_qids or ())],
            narrow_replacement_attempted=bool(narrow_replacement_attempted),
            narrow_outcome=str(narrow_outcome),
            applied_patch_id=str(applied_patch_id),
        )
    )
    return True
