"""The single, wheel-native low-level LLM client (MV-D65).

Every LLM call in the workspace — the optimization pipeline, the ontology
enrichers, and the app's backend callers — funnels through this module so
that ``mlflow.openai.autolog()`` can instrument them uniformly (token usage,
cost, latency on every span) and so there is exactly one transport, one config
resolver, and one retry policy.

The ``openai.OpenAI`` client is configured to point at the Databricks
serving-endpoints URL and uses bearer-token auth extracted from an **injected**
``WorkspaceClient`` — the app passes its OBO client, the job passes its
``run_as`` client. This module never resolves identity itself (no
``backend.services.auth`` import); the caller owns identity.

Higher-level concerns — prompt packing, JSON parsing, span linking, the
optimization ``fit_messages`` step — live in the callers, not here.
"""

from __future__ import annotations

import logging
import os
import time
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    LLM_MAX_RETRIES,
    get_llm_endpoint,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_LLM_TIMEOUT_SECONDS_DEFAULT = 600


def eval_llm_timeout_seconds() -> int:
    """Per-request HTTP timeout for LLM calls.

    Defaults to 600s (production-on). Override via env when debugging.
    Floors at 30s to avoid pathological zero/negative values.
    """
    raw = os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_LLM_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return _LLM_TIMEOUT_SECONDS_DEFAULT
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "Invalid GENIE_SPACE_OPTIMIZER_EVAL_LLM_TIMEOUT_SECONDS=%r; using %d",
            raw,
            _LLM_TIMEOUT_SECONDS_DEFAULT,
        )
        return _LLM_TIMEOUT_SECONDS_DEFAULT
    return max(30, value)


_openai_client_cache: dict[str, Any] = {}


def _message_content_text(content: Any) -> str:
    """Normalize OpenAI-compatible message content into plain text.

    Databricks serving endpoints may return either the traditional string or
    structured content blocks.  Joining block text without a separator keeps
    JSON responses valid when an endpoint splits one document across blocks.
    """
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, (list, tuple)):
        return "".join(_message_content_text(part) for part in content)
    if isinstance(content, dict):
        for key in ("text", "content", "value"):
            if key in content:
                return _message_content_text(content[key])
        return ""

    for attribute in ("text", "content", "value"):
        value = getattr(content, attribute, None)
        if value is not None and value is not content:
            return _message_content_text(value)
    return str(content)


def _resolve_bearer_token(wc: "WorkspaceClient") -> str:
    """Extract a bearer token from the workspace client's auth chain.

    Tries ``config.token`` first (covers PAT / env-var auth).  Falls back
    to ``config.authenticate()`` which invokes the SDK's full credential
    chain — OAuth M2M, Azure MI, Google SA, etc. — and returns fresh
    ``Authorization`` headers.
    """
    token = wc.config.token
    if token:
        return token

    try:
        headers = wc.config.authenticate()
        auth_value = headers.get("Authorization", "")
        if auth_value.lower().startswith("bearer "):
            return auth_value[len("Bearer "):]
    except Exception:
        pass

    raise RuntimeError(
        "Cannot resolve a bearer token from the WorkspaceClient. "
        "Ensure DATABRICKS_TOKEN is set or that OAuth/service-principal "
        "credentials are configured."
    )


def get_openai_client(w: "WorkspaceClient | None") -> Any:
    """Return an OpenAI client pointing at the Databricks FMAPI endpoint.

    Caches the client by host but **refreshes the bearer token on every
    call** so that OAuth token rotation is handled transparently.

    ``mlflow.openai.autolog()`` must be called once before first use
    to enable automatic token/cost tracking on all spans.
    """
    from databricks.sdk import WorkspaceClient as _WC
    from openai import OpenAI

    wc = w if w is not None else _WC()
    host = wc.config.host.rstrip("/")
    token = _resolve_bearer_token(wc)

    if host not in _openai_client_cache:
        _openai_client_cache[host] = OpenAI(
            api_key=token,
            base_url=f"{host}/serving-endpoints",
        )
    else:
        _openai_client_cache[host].api_key = token
    return _openai_client_cache[host]


def call_llm_core(
    w: "WorkspaceClient | None",
    *,
    messages: list[dict[str, str]],
    model: str | None = None,
    max_tokens: int | None = None,
    response_format: dict[str, Any] | None = None,
    max_retries: int = LLM_MAX_RETRIES,
) -> tuple[str, Any]:
    """Call an LLM via the OpenAI SDK with retry + exponential backoff.

    Returns ``(content_text, response_object)`` on success.
    Raises the last exception if all retries are exhausted.

    This is the low-level building block — callers are responsible for
    prompt packing, JSON parsing, prompt linking, span wrapping, etc.

    ``model=None`` resolves to :func:`get_llm_endpoint` (``GSO_LLM_ENDPOINT``
    → ``LLM_MODEL`` → default). Temperature is never sent, because some
    supported reasoning/frontier endpoints reject the parameter.
    """
    if model is None:
        model = get_llm_endpoint()

    client = get_openai_client(w)

    call_kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "timeout": eval_llm_timeout_seconds(),
    }
    # Do not send temperature: Claude Opus 4.7/4.8 and some GPT 5.x endpoints reject it.
    if max_tokens is not None:
        call_kwargs["max_tokens"] = max_tokens
    if response_format is not None:
        call_kwargs["response_format"] = response_format

    last_err: Exception | None = None
    retried_without_response_format = False
    total_attempts = max_retries + (1 if response_format is not None else 0)
    for attempt in range(total_attempts):
        try:
            response = client.chat.completions.create(**call_kwargs)
            if not response.choices:
                raise ValueError("LLM response had no choices")
            content = _message_content_text(response.choices[0].message.content).strip()
            if not content:
                raise ValueError("LLM response content is empty")
            return content, response
        except Exception as exc:
            if response_format is not None and not retried_without_response_format:
                message = str(exc).lower()
                if "response_format" in message or "json" in message:
                    logger.info(
                        "LLM endpoint rejected response_format; retrying without it: %s",
                        exc,
                    )
                    call_kwargs.pop("response_format", None)
                    retried_without_response_format = True
                    continue
            last_err = exc
            if attempt < total_attempts - 1:
                time.sleep(2**attempt)

    raise last_err  # type: ignore[misc]
