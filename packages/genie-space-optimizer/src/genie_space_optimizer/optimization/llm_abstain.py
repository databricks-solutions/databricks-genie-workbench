"""Plan 2 — first-class abstain contract for LLM reasoning calls.

Every LLM call dispatched through ``LlmReasoningCall.invoke`` (see
``llm_reasoning_call.py``) may return EITHER a typed parsed output OR
an ``AbstainVerdict``. The verdict is the LLM's structured way to
say "I cannot answer this responsibly" — preferred over hallucinating
a weak answer. Every caller's deterministic fallback consumes the
verdict to decide whether to retry, escalate, or skip the operation.

Vocabulary policy:
  * ``AbstainReason`` is CLOSED. New members require a cross-caller
    review because every fallback inspects the value. ``OTHER`` is
    the escape hatch for unexpected situations that don't justify a
    new member yet.
  * The reviewer-required set (missing_schema_context,
    ambiguous_failure, unsafe_patch, no_applicable_patch_type) is
    pinned; we add two framework-internal reasons
    (insufficient_blame_set, context_token_budget_exceeded) because
    they recur in every plan and deserve typed identity.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


class AbstainReason(StrEnum):
    """Closed vocabulary of reasons an LLM may decline.

    Stable strings — the value is the JSON wire format. Renaming a
    member is a breaking change to every plan's fallback logic.
    """

    MISSING_SCHEMA_CONTEXT = "missing_schema_context"
    AMBIGUOUS_FAILURE = "ambiguous_failure"
    UNSAFE_PATCH = "unsafe_patch"
    NO_APPLICABLE_PATCH_TYPE = "no_applicable_patch_type"
    INSUFFICIENT_BLAME_SET = "insufficient_blame_set"
    CONTEXT_TOKEN_BUDGET_EXCEEDED = "context_token_budget_exceeded"
    OTHER = "other"


_MAX_EXPLANATION_CHARS = 200


@dataclass(frozen=True, slots=True)
class AbstainVerdict(JsonRoundTrip):
    """Typed payload an LLM emits when it declines to answer.

    ``explanation`` is constrained to ≤200 chars so the payload stays
    small enough to inline in MLflow span attributes and postmortem
    summaries without truncation. ``needed_evidence`` is a tuple of
    short labels naming evidence types upstream stages should provide
    next iteration. ``suggested_next_step`` is a short imperative the
    deterministic fallback can route on.
    """

    reason: AbstainReason
    explanation: str
    needed_evidence: tuple[str, ...]
    suggested_next_step: str

    def __post_init__(self) -> None:
        if len(self.explanation) > _MAX_EXPLANATION_CHARS:
            raise ValueError(
                f"explanation must be ≤{_MAX_EXPLANATION_CHARS} chars; "
                f"got {len(self.explanation)}"
            )

    @classmethod
    def from_json(cls, payload: dict) -> "AbstainVerdict":  # type: ignore[override]
        return cls(
            reason=AbstainReason(payload["reason"]),
            explanation=str(payload["explanation"]),
            needed_evidence=tuple(payload.get("needed_evidence") or ()),
            suggested_next_step=str(payload.get("suggested_next_step") or ""),
        )
