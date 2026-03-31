"""Shared OpenAI-based LLM client for Databricks Foundation Model API.

All LLM calls in the optimization pipeline should go through this module
so that ``mlflow.openai.autolog()`` can instrument them uniformly —
capturing token usage, cost, and latency on every span.

The ``openai.OpenAI`` client is configured to point at the Databricks
serving-endpoints URL and uses bearer-token auth extracted from
``WorkspaceClient``.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_LLM_TIMEOUT_SECONDS = 600

_openai_client_cache: dict[str, Any] = {}


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


def call_llm(
    w: "WorkspaceClient | None",
    *,
    messages: list[dict[str, str]],
    max_retries: int = LLM_MAX_RETRIES,
    temperature: float = LLM_TEMPERATURE,
    max_tokens: int | None = None,
) -> tuple[str, Any]:
    """Call an LLM via the OpenAI SDK with retry + exponential backoff.

    Returns ``(content_text, response_object)`` on success.
    Raises the last exception if all retries are exhausted.

    This is the low-level building block — callers are responsible for
    JSON parsing, prompt linking, span wrapping, etc.
    """
    client = get_openai_client(w)

    call_kwargs: dict[str, Any] = {
        "model": LLM_ENDPOINT,
        "messages": messages,
        "temperature": temperature,
    }
    if max_tokens is not None:
        call_kwargs["max_tokens"] = max_tokens

    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(**call_kwargs)
            if not response.choices:
                raise ValueError("LLM response had no choices")
            content = response.choices[0].message.content
            if not content or not content.strip():
                raise ValueError("LLM response content is empty")
            return str(content).strip(), response
        except Exception as exc:
            last_err = exc
            if attempt < max_retries - 1:
                time.sleep(2**attempt)

    raise last_err  # type: ignore[misc]
