"""Optimization-pipeline LLM client — a thin wrapper over the shared client.

The single low-level transport now lives in
:mod:`genie_space_optimizer.common.llm` (MV-D65). This module re-exports it and
adds the optimization-only concerns: ``fit_messages`` prompt packing and the
``_gso_prompt_pack_stats`` attached to each response. GSO callers import
``call_llm`` / ``get_openai_client`` from here exactly as before — behavior is
unchanged.

All LLM calls in the optimization pipeline should go through this module so
that ``mlflow.openai.autolog()`` can instrument them uniformly — capturing
token usage, cost, and latency on every span.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
)

# Re-export the shared low-level client so existing import sites
# (``from ...llm_client import get_openai_client`` / ``call_llm``) keep working.
from genie_space_optimizer.common.llm import (  # noqa: F401
    _message_content_text,
    _openai_client_cache,
    _resolve_bearer_token,
    call_llm_core,
    eval_llm_timeout_seconds,
    get_openai_client,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def call_llm(
    w: "WorkspaceClient | None",
    *,
    messages: list[dict[str, str]],
    max_retries: int = LLM_MAX_RETRIES,
    temperature: float = LLM_TEMPERATURE,
    max_tokens: int | None = None,
    response_format: dict[str, Any] | None = None,
    prompt_metadata: dict[str, Any] | None = None,
) -> tuple[str, Any]:
    """Call an LLM via the OpenAI SDK with retry + exponential backoff.

    Returns ``(content_text, response_object)`` on success.
    Raises the last exception if all retries are exhausted.

    Packs the messages with :func:`fit_messages` (the wide-schema prompt
    budget) before delegating to :func:`common.llm.call_llm_core`, and attaches
    ``_gso_prompt_pack_stats`` to the response for downstream telemetry.

    ``temperature`` is accepted for backwards-compatible call sites but is
    not sent to Databricks, because some supported reasoning/frontier
    endpoints reject the parameter.
    """
    from genie_space_optimizer.optimization.wide_schema_prompt import fit_messages

    messages, pack_stats = fit_messages(messages)
    if prompt_metadata:
        pack_stats.update({
            key: value
            for key, value in prompt_metadata.items()
            if key in {"plan_hash", "inventory_hash", "included_counts", "omitted_counts"}
        })
    logger.info("GSO LLM request packed: %s", pack_stats)

    content, response = call_llm_core(
        w,
        messages=messages,
        max_tokens=max_tokens,
        response_format=response_format,
        max_retries=max_retries,
    )
    try:
        response._gso_prompt_pack_stats = pack_stats
    except Exception:
        pass
    return content, response
