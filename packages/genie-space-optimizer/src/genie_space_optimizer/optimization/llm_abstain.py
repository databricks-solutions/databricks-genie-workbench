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
    pinned; we add four framework-internal reasons
    (insufficient_blame_set, context_token_budget_exceeded,
    optimizer_capacity_starved, prompt_too_large) because they
    recur in every plan and deserve typed identity.
    ``OPTIMIZER_CAPACITY_STARVED`` is emitted by the rate-limit
    retry handler in ``llm_client`` / ``optimizer._traced_llm_call``
    when every retry returned a 429 / ``REQUEST_LIMIT_EXCEEDED``.
    ``PROMPT_TOO_LARGE`` is emitted by the pre-admission size cap
    in :class:`LlmReasoningCall` when the assembled prompt exceeds
    ``MAX_PROMPT_INPUT_TOKENS`` (40k) — the caller's
    ``_build_request`` is responsible for compacting before retry.
    Distinct from ``CONTEXT_TOKEN_BUDGET_EXCEEDED`` (the iteration
    aggregate is full but this individual call would fit) and
    ``OTHER`` (caller-side parse/validate errors).
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
    OPTIMIZER_CAPACITY_STARVED = "optimizer_capacity_starved"
    PROMPT_TOO_LARGE = "prompt_too_large"
    # P4 C1 — Stage 1 produced a RepairDiagnosis missing required
    # evidence (implicated_assets empty, sql_shape_delta empty) AND a
    # one-shot retry with sharpened feedback did not recover. The
    # structural-repair lane MUST NOT fall through to a generic
    # ``generic_judge_guidance`` shape; the cluster is routed to this
    # typed abstain instead. See
    # :func:`repair_diagnosis.gate_repair_diagnosis_sufficient`.
    REPAIR_INTENT_INDETERMINATE = "repair_intent_indeterminate"
    # P4 C3 — Synthesizer ran ``validate_sql_snippet`` on the LLM
    # snippet output and validation failed. The proposal is NOT
    # emitted; the cluster is routed to the mechanism-repeat pivot
    # path so a different mechanism can be tried.
    SNIPPET_INVALID = "snippet_invalid"
    # Trial 24 Follow-on B — the snippet SQL is a TAUTOLOGY
    # (normalized to ``true`` / ``1=1``). This is the LLM's failed
    # attempt to express a filter-REMOVAL as a positive snippet: the
    # producer validator declines it with this typed reason (distinct
    # from ``SNIPPET_INVALID``) so the synthesizer can degrade a
    # filter-removal kit to an instruction-only solo rather than
    # cascade the whole bundle. See
    # :func:`producer_snippet_validator.validate_and_stamp_snippet_patch_body`.
    SNIPPET_NOOP_SUPPRESSION = "snippet_noop_suppression"
    # P4 C4 — Synthesizer ran the metadata-target preflight on the
    # LLM proposal and the canonical ``catalog.schema.table.column``
    # path could not be resolved. The proposal is NOT emitted.
    TARGET_UNRESOLVABLE = "target_unresolvable"
    OTHER = "other"


# Soft cap (in characters) on ``AbstainVerdict.explanation``. Raised
# from 200 to 1000 after Trial 10 (PR-3) when production LLM outputs of
# 207 and 335 chars crashed the SM. The cap is a *client-side
# ergonomics* constraint to keep payloads small enough to inline in
# MLflow span attributes and postmortem summaries; it is NOT a wire
# contract — the Databricks endpoint rejects JSON Schema ``maxLength``
# (see ``prompt_io.py :: _UNSUPPORTED_KEYWORDS``), so the LLM cannot be
# told about it via response_format. Past the cap we truncate
# gracefully instead of raising; a raise here cascades into
# ``GSO_PLAN_V4_SM_FAILED`` and a downstream
# ``InputProjectionContractViolation`` in the legacy fallback path.
_MAX_EXPLANATION_CHARS = 1000

# Sentinel appended to a truncated explanation so the truncation is
# observable in markers and replay fixtures without adding a new field
# (which would break the ``JsonRoundTrip`` wire format pinned in
# ``test_abstain_verdict_round_trips_to_and_from_json``).
_TRUNCATION_MARKER = "..."


@dataclass(frozen=True, slots=True)
class AbstainVerdict(JsonRoundTrip):
    """Typed payload an LLM emits when it declines to answer.

    ``explanation`` carries a free-text rationale, soft-capped at
    ``_MAX_EXPLANATION_CHARS`` (1000) chars to keep the payload small
    enough to inline in MLflow span attributes and postmortem
    summaries. Past the cap the dataclass truncates the explanation in
    place and appends ``"..."`` rather than raising — see the module
    docstring above ``_MAX_EXPLANATION_CHARS`` for the rationale.
    ``needed_evidence`` is a tuple of short labels naming evidence
    types upstream stages should provide next iteration.
    ``suggested_next_step`` is a short imperative the deterministic
    fallback can route on.
    """

    reason: AbstainReason
    explanation: str
    needed_evidence: tuple[str, ...]
    suggested_next_step: str

    def __post_init__(self) -> None:
        if len(self.explanation) > _MAX_EXPLANATION_CHARS:
            head_len = _MAX_EXPLANATION_CHARS - len(_TRUNCATION_MARKER)
            truncated = self.explanation[:head_len] + _TRUNCATION_MARKER
            object.__setattr__(self, "explanation", truncated)

    @classmethod
    def from_json(cls, payload: dict) -> "AbstainVerdict":  # type: ignore[override]
        return cls(
            reason=AbstainReason(payload["reason"]),
            explanation=str(payload["explanation"]),
            needed_evidence=tuple(payload.get("needed_evidence") or ()),
            suggested_next_step=str(payload.get("suggested_next_step") or ""),
        )
