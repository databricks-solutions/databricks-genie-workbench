"""Plan 2 — per-iteration LLM token-budget meter.

``IterationTokenBudget`` tracks cumulative input + output tokens
across every reasoning call dispatched within a single lever-loop
iteration. It mirrors the Databricks Foundation Model API's
pre-admission / credit-back semantics so the framework can mint a
typed ``CONTEXT_TOKEN_BUDGET_EXCEEDED`` abstain BEFORE a call hits
the actual 429.

Per-call accounting:

  1. The runner calls ``reserve(input_tokens=I, max_output_tokens=O)``
     before dispatching. This optimistically charges ``I`` and ``O``
     against the meter and returns a ``Reservation`` token.
  2. After the call returns, the runner calls
     ``reconcile(reservation, actual_input, actual_output)`` to
     credit back any unused output budget (Databricks does the same
     server-side; we mirror it so subsequent calls within the same
     iteration see realistic remaining headroom).
  3. ``would_exceed(input_tokens=I, max_output_tokens=O)`` is the
     pre-admission check the runner consults before reserve.

Why a ContextVar:

  The lever-loop driver sets and resets the meter at iteration
  boundaries. Reasoning calls dispatched inside that scope pick up
  the active meter automatically; calls outside any iteration (unit
  tests, ad-hoc scripts) see the default no-op meter with
  effectively infinite limits.
"""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass

from genie_space_optimizer.optimization.llm_abstain import (
    AbstainReason,
    AbstainVerdict,
)


@dataclass(frozen=True, slots=True)
class Reservation:
    """Opaque token returned by ``reserve``, consumed by ``reconcile``.

    Tracks the reserved input/output amounts so reconcile can compute
    the credit-back delta without the runner re-passing them.
    """

    reserved_input: int
    reserved_output: int


class IterationTokenBudget:
    """Mutable per-iteration token meter (NOT thread-safe).

    The lever loop is single-threaded within an iteration; concurrent
    per-qid calls fan out via sequential awaits or explicit gather
    where the gather itself is wrapped in the same iteration scope.
    """

    __slots__ = (
        "itpm_limit",
        "otpm_limit",
        "_reserved_in",
        "_reserved_out",
        "_actual_in",
        "_actual_out",
    )

    def __init__(self, *, itpm_limit: int, otpm_limit: int) -> None:
        self.itpm_limit = int(itpm_limit)
        self.otpm_limit = int(otpm_limit)
        self._reserved_in = 0
        self._reserved_out = 0
        self._actual_in = 0
        self._actual_out = 0

    @property
    def reserved_input_tokens(self) -> int:
        return self._reserved_in

    @property
    def reserved_output_tokens(self) -> int:
        return self._reserved_out

    @property
    def actual_input_tokens(self) -> int:
        return self._actual_in

    @property
    def actual_output_tokens(self) -> int:
        return self._actual_out

    def would_exceed(
        self, *, input_tokens: int, max_output_tokens: int
    ) -> bool:
        """Return True iff a reservation of these sizes would push
        either reserved aggregate over its limit."""
        next_in = self._reserved_in + int(input_tokens)
        next_out = self._reserved_out + int(max_output_tokens)
        return next_in > self.itpm_limit or next_out > self.otpm_limit

    def reserve(
        self, *, input_tokens: int, max_output_tokens: int
    ) -> Reservation:
        """Charge a pre-admission reservation against the meter."""
        ri = int(input_tokens)
        ro = int(max_output_tokens)
        self._reserved_in += ri
        self._reserved_out += ro
        return Reservation(reserved_input=ri, reserved_output=ro)

    def reconcile(
        self,
        reservation: Reservation,
        *,
        actual_input: int,
        actual_output: int,
    ) -> None:
        """Credit back any unused output budget after the call
        returned. Updates ``actual_*`` totals and reduces
        ``reserved_*`` by the unused portion."""
        ai = int(actual_input)
        ao = int(actual_output)
        in_delta = ai - reservation.reserved_input
        out_delta = ao - reservation.reserved_output
        self._reserved_in += in_delta
        self._reserved_out += out_delta
        self._actual_in += ai
        self._actual_out += ao

    def reset(self) -> None:
        """Clear all accounting — called at iteration boundary."""
        self._reserved_in = 0
        self._reserved_out = 0
        self._actual_in = 0
        self._actual_out = 0

    def make_overflow_abstain(
        self, *, input_tokens: int, max_output_tokens: int
    ) -> AbstainVerdict:
        """Mint a typed CONTEXT_TOKEN_BUDGET_EXCEEDED verdict naming
        which limit (ITPM vs OTPM) the call would have crossed."""
        next_in = self._reserved_in + int(input_tokens)
        next_out = self._reserved_out + int(max_output_tokens)
        if next_in > self.itpm_limit:
            limit_type = "input_tokens_per_minute"
            limit = self.itpm_limit
            attempted = next_in
        else:
            limit_type = "output_tokens_per_minute"
            limit = self.otpm_limit
            attempted = next_out
        explanation = (
            f"Per-iteration {limit_type} {attempted} would exceed "
            f"limit {limit}"
        )
        explanation = explanation[:200]
        return AbstainVerdict(
            reason=AbstainReason.CONTEXT_TOKEN_BUDGET_EXCEEDED,
            explanation=explanation,
            needed_evidence=(),
            suggested_next_step="defer_to_next_iteration",
        )


# ── No-op default budget for code paths outside any iteration. ────────
_NO_OP_BUDGET = IterationTokenBudget(
    itpm_limit=10**9,
    otpm_limit=10**9,
)

_REASONING_TOKEN_BUDGET: ContextVar[IterationTokenBudget] = ContextVar(
    "_REASONING_TOKEN_BUDGET", default=_NO_OP_BUDGET,
)
