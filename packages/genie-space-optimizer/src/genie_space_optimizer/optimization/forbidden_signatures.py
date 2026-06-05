"""Trial 16.3 — SM-lane typed-feedback channel helpers.

This module owns the harvest contract between the SM lane's
``TerminalRecord.forbidden_signature`` strings (set by ``applier_gate``,
``evaluated_gate``, ``acceptance_gate``, and ``synthesize_llm`` when
they construct typed terminals) and the next-iteration
``TransformerContext.forbidden_signatures: tuple[str, ...]`` channel
that downstream LLM-prompt sites consume.

Why it lives in its own module:
    * The harness loop is 37k lines; embedding the harvest logic
      inline made it untestable. A tiny, well-named module gives the
      seam a clear contract and lets the harness call a single
      ``extend_sm_forbidden_signatures(running_set,
      harvest_sm_forbidden_signatures(final_states))`` line.
    * The legacy lane's ``_forbidden_set`` is a
      ``set[TerminalSignature]`` (5-field NamedTuple) consumed by
      ``decide_iteration_terminal_action``. We intentionally do NOT
      reuse that set — keeping the SM-lane channel as a separate
      ``set[str]`` matches the ``ctx.forbidden_signatures`` type
      contract and avoids the silent type-mismatch the analyst's
      review flagged as Gap C.
    * Pure-function helpers are testable without spinning up the SM
      iterator (see ``tests/unit/test_forbidden_signatures_harvest.py``).

The helpers are deliberately narrow: harvest is read-only over the
state objects' ``.terminal.forbidden_signature`` field, with empty /
whitespace-only strings filtered and identical signatures deduped.
Output is a deterministic sorted tuple so cross-iteration carryover
is reproducible regardless of state arrival order.
"""
from __future__ import annotations

from typing import Any, Iterable, Protocol


class _HasForbiddenSignature(Protocol):
    """Structural typing for the ``terminal`` attribute the harvest
    helper expects. The real producer is
    ``state_machine.records.TerminalRecord``."""

    forbidden_signature: str


class _HasTerminal(Protocol):
    """Structural typing for the SM final-state objects the harvest
    helper iterates. The real producer is
    ``state_machine.state.QuestionStateInIteration``."""

    terminal: _HasForbiddenSignature | None


def harvest_sm_forbidden_signatures(
    final_states: Iterable[_HasTerminal] | None,
) -> tuple[str, ...]:
    """Return the deduped, sorted, non-empty
    ``terminal.forbidden_signature`` strings carried by the SM
    final-state objects.

    States with ``terminal is None`` (still in-progress, or accepted
    with no terminal) are skipped. Empty and whitespace-only signature
    strings are skipped. Identical signatures collapse to one entry.

    Output ordering is canonical-sorted so callers get a deterministic
    sequence suitable for cross-iteration accumulator updates,
    structured marker emission, and unit-test assertions.

    Args:
        final_states: iterable of objects with an optional
            ``.terminal.forbidden_signature`` attribute.

    Returns:
        Sorted, deduped tuple of ``str`` signatures.
    """
    if not final_states:
        return ()
    out: set[str] = set()
    for s in final_states:
        terminal = getattr(s, "terminal", None)
        if terminal is None:
            continue
        sig = getattr(terminal, "forbidden_signature", "") or ""
        sig = str(sig).strip()
        if not sig:
            continue
        out.add(sig)
    return tuple(sorted(out))


def extend_sm_forbidden_signatures(
    running: set[str],
    harvested: tuple[str, ...] | Iterable[str],
) -> None:
    """Merge harvested signatures into the running cross-iteration
    accumulator.

    The accumulator is a ``set[str]`` so duplicate signatures from
    later iterations have no effect. Empty / whitespace-only strings
    are defensively filtered in case a caller passes raw iterable
    contents that bypassed ``harvest_sm_forbidden_signatures``.

    Args:
        running: cross-iteration accumulator owned by the harness
            loop. Mutated in-place.
        harvested: signatures to add (typically the output of
            ``harvest_sm_forbidden_signatures`` for the current
            iteration's final states).
    """
    for sig in harvested or ():
        s = str(sig).strip()
        if s:
            running.add(s)


# ── Trial 18 — insufficient_repair_signature harvest (sibling channel) ──


class _HasAccepted(Protocol):
    """Structural typing for the ``accepted`` attribute the Trial 18
    harvest helper expects."""

    accepted: Any | None  # AcceptanceDecisionRecord at runtime


def harvest_sm_insufficient_repair_signatures(
    final_states: Iterable[Any] | None,
) -> tuple[str, ...]:
    """Trial 18 — return the deduped, sorted, non-empty
    ``accepted.insufficient_repair_signature`` strings carried by the
    SM final-state objects.

    Symmetric to :func:`harvest_sm_forbidden_signatures` but reads
    from ``state.accepted`` (the ``KEPT_INSUFFICIENT`` lane lives on
    ``AcceptanceDecisionRecord``, not on ``TerminalRecord``).

    States with ``accepted is None`` or with an empty signature are
    skipped. Output ordering is canonical-sorted so cross-iteration
    carry-over is reproducible regardless of state arrival order.
    """
    if not final_states:
        return ()
    out: set[str] = set()
    for s in final_states:
        accepted = getattr(s, "accepted", None)
        if accepted is None:
            continue
        sig = getattr(accepted, "insufficient_repair_signature", "") or ""
        sig = str(sig).strip()
        if not sig:
            continue
        out.add(sig)
    return tuple(sorted(out))


def extend_sm_insufficient_repair_signatures(
    running: set[str],
    harvested: tuple[str, ...] | Iterable[str],
) -> None:
    """Trial 18 — merge harvested insufficient-repair signatures into
    the running cross-iteration accumulator. Mirrors
    :func:`extend_sm_forbidden_signatures` semantics."""
    for sig in harvested or ():
        s = str(sig).strip()
        if s:
            running.add(s)
