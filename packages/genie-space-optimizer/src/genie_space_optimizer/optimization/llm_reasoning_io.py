"""Plan 2 — typed I/O contracts for LLM reasoning calls.

Three contracts live here:

  * ``AbstainableEnvelope[T]`` — the Pydantic generic the LLM's
    response_format is derived from. Every reasoning call's prompt
    instructs the LLM to fill EITHER ``result`` (typed T) OR
    ``declined`` (an AbstainVerdict). Exactly one must be populated.
  * ``LlmReasoningRequest`` — the typed input to
    ``LlmReasoningCall.invoke``. Frozen + slots so it can travel
    safely through ContextVars and be logged to MLflow. (Added in
    Task 3.)
  * ``LlmReasoningResponse`` — the typed output from
    ``LlmReasoningCall.invoke``. Frozen + slots + JsonRoundTrip so
    it can be persisted by Plans 3-7 stage I/O carriers without
    re-parsing. (Added in Task 4.)

The envelope shape is deliberately:

  {"result": <T> | null, "declined": <AbstainVerdict> | null}

rather than a tagged union, because Databricks Foundation Model API
strict-mode response_format does NOT support ``anyOf`` / ``oneOf`` /
``$ref`` (see ``prompt_io._UNSUPPORTED_KEYWORDS``). The XOR semantics
are enforced post-parse by ``parse_envelope``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict

from genie_space_optimizer.optimization.llm_abstain import (
    AbstainReason,
    AbstainVerdict,
)
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract

_T = TypeVar("_T", bound=LLMOutputContract)


class _AbstainVerdictModel(BaseModel):
    """Pydantic mirror of ``AbstainVerdict`` for response_format binding.

    Pydantic ``BaseModel`` is required because Databricks response_format
    is generated from Pydantic. ``parse_envelope`` converts an instance
    to the dataclass ``AbstainVerdict`` so callers never see this type.
    """

    model_config = ConfigDict(extra="forbid")

    reason: AbstainReason
    explanation: str
    needed_evidence: list[str]
    suggested_next_step: str


class AbstainableEnvelope(BaseModel, Generic[_T]):
    """Generic Pydantic envelope wrapping a typed result OR an abstain
    verdict.

    Both fields default to ``None`` so the schema fits Databricks
    strict mode without ``anyOf``/``oneOf``. The XOR rule is enforced
    by ``parse_envelope``.
    """

    model_config = ConfigDict(extra="forbid")

    result: _T | None = None
    declined: _AbstainVerdictModel | None = None


class EnvelopeContractError(ValueError):
    """Raised when the LLM's envelope response violates the XOR rule
    or is not valid JSON / does not match the envelope schema."""


def parse_envelope(
    raw_text: str, result_cls: type[_T]
) -> _T | AbstainVerdict:
    """Parse ``raw_text`` as an ``AbstainableEnvelope[result_cls]``.

    Returns either an instance of ``result_cls`` (when ``result`` is
    the populated branch) or an ``AbstainVerdict`` dataclass (when
    ``declined`` is the populated branch). Raises
    ``EnvelopeContractError`` when neither or both branches are
    populated, when the JSON is malformed, or when Pydantic
    validation of either branch fails.
    """
    from genie_space_optimizer.optimization.prompt_io import (
        _extract_json_text,
    )

    try:
        json_text = _extract_json_text(raw_text)
    except Exception as exc:
        raise EnvelopeContractError(
            f"envelope JSON extraction failed: {exc}"
        ) from exc

    EnvCls = AbstainableEnvelope[result_cls]
    try:
        env = EnvCls.model_validate_json(json_text)
    except Exception as exc:
        raise EnvelopeContractError(
            f"envelope did not validate against AbstainableEnvelope"
            f"[{result_cls.__name__}]: {exc}"
        ) from exc

    populated_result = env.result is not None
    populated_declined = env.declined is not None
    if populated_result and populated_declined:
        raise EnvelopeContractError(
            "exactly one of 'result' / 'declined' must be populated; "
            "both were"
        )
    if not populated_result and not populated_declined:
        raise EnvelopeContractError(
            "exactly one of 'result' / 'declined' must be populated; "
            "neither was"
        )

    if populated_result:
        return env.result  # type: ignore[return-value]

    decl_model = env.declined
    assert decl_model is not None
    return AbstainVerdict(
        reason=decl_model.reason,
        explanation=decl_model.explanation,
        needed_evidence=tuple(decl_model.needed_evidence),
        suggested_next_step=decl_model.suggested_next_step,
    )


# ── LlmReasoningRequest ───────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class LlmReasoningRequest:
    """Typed input to ``LlmReasoningCall.invoke``.

    Required:
      * ``call_id`` — stable per-invocation identifier (the framework's
        token-budget meter keys on this; postmortems join on it).
      * ``skill_id`` — the skill folder under ``src/.../skills/<id>/``
        whose SKILL.md body is the system prompt.
      * ``system_msg`` — the rendered system prompt (skill body with
        template variables substituted).
      * ``user_prompt`` — the rendered user prompt for this call.
      * ``result_cls`` — Pydantic ``LLMOutputContract`` subclass that
        the envelope's ``result`` branch is bound to.
      * ``max_tokens`` — non-optional per the Databricks Foundation
        Model API limits doc. Claude defaults to 1000 otherwise and
        silently truncates responses.

    Plan 8 Task 11 — ``model_override`` removed; the framework uses the
    system-wide ``LLM_ENDPOINT`` for every call.

    The request is NOT a JsonRoundTrip — it carries a class reference
    (``result_cls``) that cannot be serialized portably.
    """

    call_id: str
    skill_id: str
    system_msg: str
    user_prompt: str
    result_cls: type
    max_tokens: int

    def __post_init__(self) -> None:
        if not self.call_id:
            raise ValueError("call_id must be a non-empty string")
        if not self.skill_id:
            raise ValueError("skill_id must be a non-empty string")
        if self.max_tokens <= 0:
            raise ValueError(
                f"max_tokens must be > 0; got {self.max_tokens}"
            )


# ── LlmReasoningResponse ──────────────────────────────────────────────


from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class LlmReasoningResponse(JsonRoundTrip):
    """Typed output from ``LlmReasoningCall.invoke``.

    Carries one of three mutually-exclusive states:

      * Success: ``succeeded=True``, ``parsed_output`` is a dict
        (from ``result_cls.model_dump()``), ``declined=None``,
        ``error=None``.
      * Abstain: ``succeeded=False``, ``parsed_output=None``,
        ``declined`` is an AbstainVerdict, ``error=None``.
      * Error: ``succeeded=False``, ``parsed_output=None``,
        ``declined=None``, ``error`` is a non-empty string.

    Other fields are unconditional breadcrumbs for postmortems:
      * ``raw_text`` — always captured (empty string on hard failure
        before the LLM returned).
      * ``tokens_input`` / ``tokens_output`` — sourced from the
        OpenAI response object's ``usage`` field; 0 when the HTTP
        call did not complete or when the override path (tape
        replay) is in effect.
      * ``duration_ms`` — wall-clock from invoke entry to invoke
        exit.
    """

    call_id: str
    skill_id: str
    succeeded: bool
    parsed_output: dict | None
    declined: AbstainVerdict | None
    raw_text: str
    tokens_input: int
    tokens_output: int
    duration_ms: int
    error: str | None

    def __post_init__(self) -> None:
        if self.succeeded and self.parsed_output is None:
            raise ValueError(
                "succeeded=True requires parsed_output to be set"
            )
        if self.succeeded and self.declined is not None:
            raise ValueError(
                "declined is set but succeeded=True — mutually exclusive"
            )

    @classmethod
    def from_json(cls, payload: dict) -> "LlmReasoningResponse":  # type: ignore[override]
        declined_payload = payload.get("declined")
        declined = (
            AbstainVerdict.from_json(declined_payload)
            if isinstance(declined_payload, dict)
            else None
        )
        return cls(
            call_id=str(payload["call_id"]),
            skill_id=str(payload["skill_id"]),
            succeeded=bool(payload["succeeded"]),
            parsed_output=(
                dict(payload["parsed_output"])
                if isinstance(payload.get("parsed_output"), dict)
                else None
            ),
            declined=declined,
            raw_text=str(payload.get("raw_text") or ""),
            tokens_input=int(payload.get("tokens_input") or 0),
            tokens_output=int(payload.get("tokens_output") or 0),
            duration_ms=int(payload.get("duration_ms") or 0),
            error=(
                str(payload["error"])
                if payload.get("error") is not None
                else None
            ),
        )
