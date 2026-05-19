"""Plan 2 — LlmReasoningCall runner.

Single public surface: ``LlmReasoningCall().invoke(w, request)``.

The runner is intentionally a thin wrapper over
``optimization.optimizer._traced_llm_call`` — it adds:

  * Envelope binding (the response_format is derived from
    ``AbstainableEnvelope[request.result_cls]``, not the bare result
    contract).
  * Abstain-aware response parsing via ``parse_envelope`` — returns
    a typed parsed_output OR a typed declined verdict.
  * Per-iteration token budget enforcement via the
    ``_REASONING_TOKEN_BUDGET`` ContextVar.
  * Model-override propagation (frontmatter ``model_override`` lets
    callers pin a cheaper / smaller model per skill).

What it deliberately does NOT do:

  * It does NOT instrument MLflow itself — ``_traced_llm_call``
    already wraps every call in a ``mlflow.start_span(name=span_name)``
    and ``mlflow.openai.autolog()`` instruments the OpenAI side.
  * It does NOT install its own retry loop — ``_traced_llm_call``
    already retries up to ``LLM_MAX_RETRIES`` on RPC failures and
    validator errors.
  * It does NOT touch the production recorder — ``_traced_llm_call``
    consults ``_LLM_CALL_RECORDER`` after every successful call, so
    the recorder picks up reasoning calls automatically.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any

from genie_space_optimizer.optimization.llm_abstain import AbstainVerdict
from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
    EnvelopeContractError,
    LlmReasoningRequest,
    LlmReasoningResponse,
    parse_envelope,
)
from genie_space_optimizer.optimization.llm_token_budget import (
    _REASONING_TOKEN_BUDGET,
)


class LlmReasoningCall:
    """Stateless runner. Construct once; invoke many times."""

    def invoke(
        self, *, w: Any, request: LlmReasoningRequest
    ) -> LlmReasoningResponse:
        from genie_space_optimizer.optimization import optimizer

        # ── 1. Pre-admission budget check ─────────────────────────────
        budget = _REASONING_TOKEN_BUDGET.get()
        est_input = max(
            1, (len(request.system_msg) + len(request.user_prompt)) // 4
        )
        if budget.would_exceed(
            input_tokens=est_input, max_output_tokens=request.max_tokens
        ):
            verdict = budget.make_overflow_abstain(
                input_tokens=est_input,
                max_output_tokens=request.max_tokens,
            )
            return LlmReasoningResponse(
                call_id=request.call_id,
                skill_id=request.skill_id,
                succeeded=False,
                parsed_output=None,
                declined=verdict,
                raw_text="",
                tokens_input=0,
                tokens_output=0,
                duration_ms=0,
                error=None,
            )

        reservation = budget.reserve(
            input_tokens=est_input,
            max_output_tokens=request.max_tokens,
        )

        # ── 2. Derive envelope response_format ────────────────────────
        envelope_cls = AbstainableEnvelope[request.result_cls]
        span_name = f"reasoning_call.{request.skill_id}"

        # ── 3. Dispatch through _traced_llm_call ──────────────────────
        t0 = time.monotonic()
        raw_text = ""
        tokens_input = 0
        tokens_output = 0
        try:
            with _model_override_scope(request.model_override):
                raw_text, response_obj = optimizer._traced_llm_call(
                    w,
                    request.system_msg,
                    request.user_prompt,
                    span_name=span_name,
                    max_tokens=request.max_tokens,
                    response_model=envelope_cls,
                )
            tokens_input, tokens_output = _extract_token_usage(response_obj)
        except Exception as exc:
            duration_ms = int((time.monotonic() - t0) * 1000)
            budget.reconcile(
                reservation, actual_input=0, actual_output=0,
            )
            return LlmReasoningResponse(
                call_id=request.call_id,
                skill_id=request.skill_id,
                succeeded=False,
                parsed_output=None,
                declined=None,
                raw_text=raw_text or "",
                tokens_input=0,
                tokens_output=0,
                duration_ms=duration_ms,
                error=f"{type(exc).__name__}: {exc}",
            )

        duration_ms = int((time.monotonic() - t0) * 1000)
        budget.reconcile(
            reservation,
            actual_input=tokens_input,
            actual_output=tokens_output,
        )

        # ── 4. Parse the envelope ─────────────────────────────────────
        try:
            parsed = parse_envelope(raw_text, request.result_cls)
        except EnvelopeContractError as exc:
            return LlmReasoningResponse(
                call_id=request.call_id,
                skill_id=request.skill_id,
                succeeded=False,
                parsed_output=None,
                declined=None,
                raw_text=raw_text,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                duration_ms=duration_ms,
                error=f"EnvelopeContractError: {exc}",
            )

        if isinstance(parsed, AbstainVerdict):
            return LlmReasoningResponse(
                call_id=request.call_id,
                skill_id=request.skill_id,
                succeeded=False,
                parsed_output=None,
                declined=parsed,
                raw_text=raw_text,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                duration_ms=duration_ms,
                error=None,
            )

        return LlmReasoningResponse(
            call_id=request.call_id,
            skill_id=request.skill_id,
            succeeded=True,
            parsed_output=parsed.model_dump(),
            declined=None,
            raw_text=raw_text,
            tokens_input=tokens_input,
            tokens_output=tokens_output,
            duration_ms=duration_ms,
            error=None,
        )


# ── Helpers ───────────────────────────────────────────────────────────


def _extract_token_usage(response_obj: Any) -> tuple[int, int]:
    """Best-effort extraction of (input_tokens, output_tokens) from
    the OpenAI response object. Returns (0, 0) when unavailable."""
    try:
        usage = getattr(response_obj, "usage", None)
        if usage is None:
            return (0, 0)
        prompt_tokens = getattr(usage, "prompt_tokens", None)
        completion_tokens = getattr(usage, "completion_tokens", None)
        ti = int(prompt_tokens) if prompt_tokens is not None else 0
        to = int(completion_tokens) if completion_tokens is not None else 0
        return (ti, to)
    except Exception:
        return (0, 0)


# ── Model-override scope ──────────────────────────────────────────────
# We need a way to make ``_traced_llm_call`` use a different model
# without modifying it. llm_client.py binds LLM_ENDPOINT at module
# import time (line 20), so we temporarily monkey-patch BOTH
# ``config.LLM_ENDPOINT`` (covers re-imports) and
# ``llm_client.LLM_ENDPOINT`` (covers the already-bound name) for the
# duration of the call. Restoration is guaranteed by the finally
# clause even on exceptions. Plan 8 cleanup adds a proper override
# parameter to ``_traced_llm_call``.


@contextmanager
def _model_override_scope(model_override: str | None):
    """Temporarily set LLM_ENDPOINT for the duration of the with-block.

    When ``model_override`` is None, the function is a no-op.
    """
    if model_override is None:
        yield
        return
    from genie_space_optimizer.common import config
    from genie_space_optimizer.optimization import llm_client, optimizer

    # _traced_llm_call binds LLM_ENDPOINT inside the optimizer module
    # (line 392); llm_client.call_llm uses it from its own module
    # namespace. Override all three so the next call site picks up
    # the new value regardless of which path executes.
    original_config = config.LLM_ENDPOINT
    original_client = llm_client.LLM_ENDPOINT
    original_optimizer = optimizer.LLM_ENDPOINT
    config.LLM_ENDPOINT = model_override
    llm_client.LLM_ENDPOINT = model_override
    optimizer.LLM_ENDPOINT = model_override
    try:
        yield
    finally:
        config.LLM_ENDPOINT = original_config
        llm_client.LLM_ENDPOINT = original_client
        optimizer.LLM_ENDPOINT = original_optimizer
