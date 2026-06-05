"""Trial 22 W4 — iteration-terminal taxonomy helper.

The harness loop used to emit ``TerminalReason.NO_APPLIED_PATCHES`` as
a catch-all whenever the iteration ended without an applied patch.
The Trial 21 postmortem showed this masking real root causes: Stage 3
returned valid proposals but the slate compiler dropped every one with
``bundle_invariant_violated``; the harness then emitted
``NO_APPLIED_PATCHES`` and the next iteration's strategist never
learned what the compiler complained about.

Trial 22 fixes the taxonomy honesty in two ways:

  1. The :class:`TerminalReason` enum gains
     :attr:`~TerminalReason.SLATE_COMPILER_EMPTY`,
     :attr:`~TerminalReason.STAGE3_RETURNED_NONE`, and
     :attr:`~TerminalReason.APPLIER_NO_OUTCOMES`. The enum stays
     closed-vocabulary — no colon-suffixed strings.
  2. :class:`IterationTerminalVerdict` carries the structured fields
     (``top_drop_reason``, ``drop_reason_counts``,
     ``originating_intent_id``) that pin the root cause. Markers /
     ledger rows carry both the enum AND the structured fields.

The helper :func:`compute_iteration_terminal_reason` is the single
producer. Hard-coded ``_TerminalReason.NO_APPLIED_PATCHES`` emit
sites in :mod:`harness` are migrated to call this helper instead.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from genie_space_optimizer.optimization.terminal_reason import TerminalReason


@dataclass(frozen=True, slots=True)
class IterationTerminalVerdict:
    """Closed-vocab enum + structured root-cause fields.

    ``terminal_reason`` is always a member of :class:`TerminalReason`
    (never a dynamic string). The dynamic root cause lives in
    ``top_drop_reason`` (a string-form :class:`DropReason`) and
    ``drop_reason_counts`` (per-reason counts), with the originating
    proposal pinned by ``originating_intent_id`` for cascade-drop
    attribution.
    """

    terminal_reason: TerminalReason
    top_drop_reason: str = ""
    drop_reason_counts: dict[str, int] = field(default_factory=dict)
    originating_intent_id: str = ""

    def to_marker_payload(self) -> dict[str, object]:
        """Project to a flat dict suitable for marker / ledger payloads.

        Keys mirror the ``GSO_TRIAL22_ITERATION_TERMINAL_V1`` marker
        contract.
        """
        return {
            "terminal_reason": str(self.terminal_reason.value),
            "top_drop_reason": str(self.top_drop_reason),
            "drop_reason_counts": dict(self.drop_reason_counts),
            "originating_intent_id": str(self.originating_intent_id),
        }


def compute_iteration_terminal_reason(
    *,
    stage3_proposal_count: int = -1,
    compiler_surviving_count: int = -1,
    compiler_top_drop_reason: str = "",
    compiler_drop_reason_counts: dict[str, int] | None = None,
    compiler_first_originating_intent_id: str = "",
    applied_outcome_count: int = -1,
    kept_insufficient_count: int = -1,
    fallback: TerminalReason = TerminalReason.NO_APPLIED_PATCHES,
) -> IterationTerminalVerdict:
    """Single producer of :class:`IterationTerminalVerdict`.

    Sentinel ``-1`` on integer parameters means "not measured" — the
    caller does not have that signal (typical for harness escalation
    paths like ``flag_for_review`` and ``gt_repair``, where Stage 3
    didn't run for the AG in question). When all integer inputs are
    ``-1`` the helper returns ``fallback`` directly.

    Decision order (each branch returns a closed-vocab enum value;
    structured fields populate only when relevant):

      * Trial 23 W1 — ``kept_insufficient_count > 0`` →
        :attr:`TerminalReason.KEPT_INSUFFICIENT`. **Highest precedence.**
        When the iteration applied >= 1 patch and the acceptance gate
        classified >= 1 of them as ``kept_insufficient`` (behaviour-
        unchanged-but-harmless), the iteration learned something real:
        the tested repair was insufficient. Labelling it
        ``NO_APPLIED_PATCHES`` is false (patches WERE applied) and
        corrupts the pivot/learning signal. Callers gate this by
        passing the sentinel ``-1`` when
        ``trial23_kept_insufficient_authoritative_enabled()`` is OFF,
        so the pre-Trial-23 behaviour is byte-stable.
      * ``stage3_proposal_count > 0`` AND
        ``compiler_surviving_count == 0`` →
        :attr:`TerminalReason.SLATE_COMPILER_EMPTY`. Structured fields
        carry the compiler's ``top_drop_reason`` /
        ``drop_reason_counts`` / ``first_originating_intent_id``.
        **This is the Trial 21 postmortem case** — every Stage 3
        proposal was dropped as ``bundle_invariant_violated``; the
        harness used to emit ``NO_APPLIED_PATCHES`` here.
      * ``stage3_proposal_count > 0`` AND
        ``compiler_surviving_count > 0`` AND
        ``applied_outcome_count == 0`` →
        :attr:`TerminalReason.APPLIER_NO_OUTCOMES`. No structured
        fields.
      * ``stage3_proposal_count == 0`` (Stage 3 measured and returned
        zero proposals) → :attr:`TerminalReason.STAGE3_RETURNED_NONE`.
        No structured fields.
      * Otherwise (sentinel ``-1`` on all three counts → caller has
        no measurement) → ``fallback`` (default
        :attr:`TerminalReason.NO_APPLIED_PATCHES`). Used by harness
        escalation paths where Stage 3 / compiler / applier didn't
        run for the AG in question.

    The first branch that matches wins.

    The enum value is ALWAYS one of :class:`TerminalReason`'s defined
    members — no string concatenation, no colon-suffix. The dynamic
    root cause is in the structured fields.
    """
    counts = dict(compiler_drop_reason_counts or {})

    # Trial 23 W1 — kept_insufficient is authoritative. An applied +
    # kept-insufficient iteration MUST NOT be reported as
    # NO_APPLIED_PATCHES. This branch is first so it wins over the
    # compiler/applier/stage3 branches below.
    if kept_insufficient_count > 0:
        return IterationTerminalVerdict(
            terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
        )

    if stage3_proposal_count > 0 and compiler_surviving_count == 0:
        return IterationTerminalVerdict(
            terminal_reason=TerminalReason.SLATE_COMPILER_EMPTY,
            top_drop_reason=str(compiler_top_drop_reason),
            drop_reason_counts=counts,
            originating_intent_id=str(compiler_first_originating_intent_id),
        )

    if (
        stage3_proposal_count > 0
        and compiler_surviving_count > 0
        and applied_outcome_count == 0
    ):
        return IterationTerminalVerdict(
            terminal_reason=TerminalReason.APPLIER_NO_OUTCOMES,
        )

    if stage3_proposal_count == 0:
        # Stage 3 was measured and explicitly returned zero. Distinct
        # from the sentinel ``-1`` (escalation paths) which falls
        # through to ``fallback`` below.
        return IterationTerminalVerdict(
            terminal_reason=TerminalReason.STAGE3_RETURNED_NONE,
        )

    return IterationTerminalVerdict(terminal_reason=fallback)


__all__ = (
    "IterationTerminalVerdict",
    "compute_iteration_terminal_reason",
)
