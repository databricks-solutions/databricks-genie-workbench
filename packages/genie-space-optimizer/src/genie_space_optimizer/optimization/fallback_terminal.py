"""Phase 3 P3.2 — typed terminal classification for the strategist
generic structural fallback path.

Background — the strategist has two distinct "zero action groups" exit
conditions that the harness historically conflated into a single
``no_action_group_emitted`` terminal reason:

  1. The strategist LLM returned zero AGs on a fresh call (genuine
     "no idea what to do" — emits :data:`TerminalReason.NO_ACTION_GROUP_EMITTED`);
  2. The strategist (or AG regenerator) DID produce candidates, but
     every candidate's :class:`TerminalSignature` collided with the
     union of ``forbidden_signatures`` + ``insufficient_repair_signatures``
     accumulated across prior iterations.

Treating case (2) as "no_action_group_emitted" was the immediate
cause of the Trial 20 ``PLAN12_NO_PIVOT_AFTER_KEPT_INSUFFICIENT``
postmortem: the harness "looked stuck" because the strategist kept
re-emitting AGs that were dead on arrival, and there was no typed
signal upstream to tell the next iteration "stop reaching for the
same lever family — pivot to a different patch family".

This module provides :func:`classify_zero_ag_terminal_reason` —
called by the harness immediately before the existing
``no_action_group_emitted`` marker emit point. When it returns
:data:`TerminalReason.FALLBACK_NO_NEW_STRATEGY`, the harness MUST:

  * Record the iteration as non-applied (no patches applied);
  * Skip the acceptance gate entirely (there is nothing to accept);
  * Stamp the typed terminal in the candidate ledger so Plan 12 can
    consult the next-iteration pivot helper.

The function is deliberately deterministic and side-effect free.
"""
from __future__ import annotations

from typing import Iterable, Sequence

from genie_space_optimizer.optimization.terminal_reason import TerminalReason


def classify_zero_ag_terminal_reason(
    *,
    regenerator_returned_candidates: bool,
    expanded_forbidden_count: int,
    insufficient_repair_signatures_count: int,
    prior_terminal_signatures_count: int,
) -> TerminalReason:
    """Classify a zero-AG iteration into the most specific
    :class:`TerminalReason` value.

    Decision logic — checked top-to-bottom:

      * If the AG regenerator returned ≥1 candidate but every one was
        filtered by the forbidden+insufficient signature union, and
        the union itself had at least one entry coming in, the cause
        is :data:`TerminalReason.FALLBACK_NO_NEW_STRATEGY`. This is
        the "we tried everything we know about and exhausted the
        admissible action space" case — it tells the next iteration's
        pivot helper to walk the patch-family graph instead of
        re-emitting the same AG family.

      * Otherwise the cause is the catch-all
        :data:`TerminalReason.NO_ACTION_GROUP_EMITTED` — either the
        strategist LLM returned zero candidates on a clean slate, or
        there was no signature history to exhaust.

    Args:
      regenerator_returned_candidates: ``True`` iff the AG regenerator
        successfully produced ≥1 raw candidate before the
        signature-collision filter. Equivalent to ``len(regenerated) > 0``
        at the call site (i.e. ``bool(regenerated)``).
      expanded_forbidden_count: Size of the expanded forbidden_set
        passed to the regenerator (forbidden ∪ insufficient ∪
        prior_terminal_signatures).
      insufficient_repair_signatures_count: Size of the
        ``insufficient_repair_signatures`` slice contributed by the
        outer-loop kept-insufficient harvest.
      prior_terminal_signatures_count: Size of the prior-terminal
        signature slice contributed by previous iterations'
        forbidden_signatures.

    The ``expanded_forbidden_count`` parameter is currently unused
    by the decision (it's implied by the other two slices when both
    are non-zero) but accepted so the call site can pass through the
    same dict it logs to the ``GSO_FALLBACK_NO_NEW_STRATEGY_V1`` marker
    without rebuilding it.
    """
    del expanded_forbidden_count  # Captured for marker logging; not part of the verdict.

    has_prior_signatures = (
        insufficient_repair_signatures_count > 0
        or prior_terminal_signatures_count > 0
    )

    # Case (2) — the regenerator either ran and was fully suppressed
    # by the signature filter (regenerator_returned_candidates=False
    # with a non-empty signature history), or it returned candidates
    # that the filter then erased. Either path signals exhaustion of
    # the known admissible action space.
    if has_prior_signatures and not regenerator_returned_candidates:
        return TerminalReason.FALLBACK_NO_NEW_STRATEGY

    # Case (1) — genuine zero-AG with no signature history to exhaust.
    return TerminalReason.NO_ACTION_GROUP_EMITTED


def should_skip_acceptance_gate(terminal_reason: TerminalReason) -> bool:
    """Phase 3 P3.2 — acceptance-gate skip predicate.

    Returns ``True`` for terminal reasons that signal the iteration
    has nothing to accept (no patches were applied) and the
    acceptance-gate machinery should be bypassed entirely.

    For
    :data:`TerminalReason.FALLBACK_NO_NEW_STRATEGY` the harness MUST
    skip the acceptance gate — there is no candidate to accept and
    invoking the gate would emit a spurious
    ``acceptance_decision`` record that pollutes the next iteration's
    ``insufficient_repair_signatures`` slot.

    Other zero-applied terminal reasons (``NO_ACTION_GROUP_EMITTED``,
    ``NO_APPLIED_PATCHES``, …) also have no candidate to accept, but
    the legacy harness already skips the gate for them through other
    code paths; this predicate keeps the new fallback symmetric
    without disturbing the pre-existing behaviour.
    """
    return terminal_reason is TerminalReason.FALLBACK_NO_NEW_STRATEGY


# Closed list — kept here so the test suite can pin the policy
# without re-running the predicate against every TerminalReason
# value. Future P3.x expansions append to this tuple.
SKIPS_ACCEPTANCE_GATE: tuple[TerminalReason, ...] = (
    TerminalReason.FALLBACK_NO_NEW_STRATEGY,
)


def fallback_marker_payload(
    *,
    expanded_forbidden_count: int,
    insufficient_repair_signatures_count: int,
    prior_terminal_signatures_count: int,
) -> dict[str, int]:
    """Helper — return the canonical payload dict for the
    ``GSO_FALLBACK_NO_NEW_STRATEGY_V1`` stdout marker. Centralizing
    the schema here lets postmortems pin the keys without grepping
    the harness for the JSON literal.
    """
    return {
        "expanded_forbidden_count": int(expanded_forbidden_count),
        "insufficient_signatures_count": int(insufficient_repair_signatures_count),
        "prior_terminal_signatures_count": int(prior_terminal_signatures_count),
    }


def expanded_signature_union_size(
    *,
    forbidden_signatures: Iterable[str] | Sequence[str] | None,
    insufficient_repair_signatures: Iterable[str] | Sequence[str] | None,
) -> int:
    """Utility — size of the expanded forbidden∪insufficient set
    that the regenerator consumed. Returns 0 when both inputs are
    None / empty. Used by the marker payload helper to compute
    ``expanded_forbidden_count`` without re-iterating the
    underlying collections.
    """
    f_set = set(forbidden_signatures or ())
    i_set = set(insufficient_repair_signatures or ())
    return len(f_set | i_set)
