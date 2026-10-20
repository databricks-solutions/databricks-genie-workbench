"""Unity Gateway model service catalog helpers for user-selectable LLM models."""

from __future__ import annotations

import logging
import os
import threading
import time
from urllib.parse import urlencode

from databricks.sdk import WorkspaceClient

from backend.models import LLMModelInfo
from backend.services.llm_utils import get_llm_model, normalize_model_name
from genie_space_optimizer.common.ai_gateway import CHAT_COMPLETIONS_API_TYPE

logger = logging.getLogger(__name__)

_CURATED_COMPATIBLE_CHAT_MODELS: tuple[tuple[str, str], ...] = (
    ("system.ai.claude-opus-5", "Claude Opus 5"),
    ("system.ai.claude-sonnet-5", "Claude Sonnet 5"),
    ("system.ai.claude-opus-4-8", "Claude Opus 4.8"),
    ("system.ai.claude-opus-4-7", "Claude Opus 4.7"),
    ("system.ai.claude-sonnet-4-6", "Claude Sonnet 4.6"),
    # GPT 5.5 currently requires Responses API for function-tool flows.
    # Create Agent uses Chat Completions tools, so keep it out of the shared picker.
    ("system.ai.gpt-5-4", "GPT-5.4"),
    ("system.ai.gpt-5-2", "GPT-5.2"),
)
_CURATED_COMPATIBLE_CHAT_MODEL_NAMES = {
    name for name, _display_name in _CURATED_COMPATIBLE_CHAT_MODELS
}

# Which system.ai model services exist varies by cloud/region, so the picker is
# filtered against the workspace listing. Cached in-process; failures are
# cached briefly and fail open to the full curated list.
_AVAILABILITY_TTL_SECONDS = 600
_AVAILABILITY_FAILURE_TTL_SECONDS = 60
_AVAILABILITY_LOCK = threading.Lock()
_AVAILABILITY_CACHE: tuple[float, float, set[str] | None] | None = None
_MODEL_SERVICES_PATH = "/api/2.1/unity-catalog/model-services"


def _optimizer_prompt_budget_chars() -> int:
    raw = os.getenv("GSO_OPTIMIZER_PROMPT_MAX_CHARS", "60000").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        logger.warning("Invalid GSO_OPTIMIZER_PROMPT_MAX_CHARS=%r; using 60000", raw)
        return 60_000


class ModelCatalogError(RuntimeError):
    """Raised when model catalog metadata cannot be read."""


class ModelValidationError(ValueError):
    """Raised when a selected model is not usable for chat completions."""


def _curated_model_infos(default_model: str) -> list[LLMModelInfo]:
    """Databricks-hosted ``system.ai`` model services supported by this app.

    The Create Agent uses streaming chat completions with tools, while GSO
    uses OpenAI-compatible chat calls for prompt optimization. Keep this list
    intentionally narrow so users do not select models that are chat-like but
    incompatible with one of those request shapes.
    """
    return [
        LLMModelInfo(
            name=name,
            displayName=display_name,
            isDefault=name == default_model,
            optimizerPromptBudgetChars=_optimizer_prompt_budget_chars(),
            contextTier="long",
        )
        for name, display_name in _CURATED_COMPATIBLE_CHAT_MODELS
    ]


def _sort_models(models: list[LLMModelInfo]) -> list[LLMModelInfo]:
    return sorted(
        models,
        key=lambda m: (not m.isDefault, m.displayName.lower(), m.name.lower()),
    )


def _list_system_ai_chat_models(client: WorkspaceClient) -> set[str]:
    """Names of ``system.ai`` model services that accept chat completions."""
    names: set[str] = set()
    page_token: str | None = None
    while True:
        query = {"catalog_name": "system", "schema_name": "ai", "max_results": 200}
        if page_token:
            query["page_token"] = page_token
        resp = client.api_client.do("GET", f"{_MODEL_SERVICES_PATH}?{urlencode(query)}") or {}
        for service in resp.get("model_services") or []:
            name = str(service.get("name") or "").removeprefix("model-services/")
            api_types = service.get("supported_api_types")
            if name and (not api_types or CHAT_COMPLETIONS_API_TYPE in api_types):
                names.add(name)
        page_token = resp.get("next_page_token")
        if not page_token:
            return names


def _available_system_ai_models(client: WorkspaceClient | None) -> set[str] | None:
    """Cached chat model service names, or ``None`` if listing failed."""
    global _AVAILABILITY_CACHE
    with _AVAILABILITY_LOCK:
        if _AVAILABILITY_CACHE is not None:
            ts, ttl, cached = _AVAILABILITY_CACHE
            if time.monotonic() - ts < ttl:
                return cached

    try:
        if client is None:
            from backend.services.auth import get_service_principal_client

            client = get_service_principal_client()
        result: set[str] | None = _list_system_ai_chat_models(client)
        ttl = _AVAILABILITY_TTL_SECONDS
    except Exception as exc:
        logger.warning("Could not list system.ai model services; showing full curated list: %s", exc)
        result = None
        ttl = _AVAILABILITY_FAILURE_TTL_SECONDS

    with _AVAILABILITY_LOCK:
        _AVAILABILITY_CACHE = (time.monotonic(), ttl, result)
    return result


def _reset_availability_cache() -> None:
    global _AVAILABILITY_CACHE
    with _AVAILABILITY_LOCK:
        _AVAILABILITY_CACHE = None


def list_chat_models(
    *,
    client: WorkspaceClient | None = None,
    allow_sp_fallback: bool = False,
) -> list[LLMModelInfo]:
    """Return curated chat models that exist as model services in this workspace.

    Availability is listed as the service principal unless ``client`` is given.
    If listing fails, or matches none of the curated models, the full curated
    list is returned. The configured default is always kept.
    """
    default_model = get_llm_model()
    if default_model not in _CURATED_COMPATIBLE_CHAT_MODEL_NAMES:
        logger.warning(
            "Configured LLM_MODEL=%s is not in the curated selectable model list",
            default_model,
        )
    models = _curated_model_infos(default_model)
    available = _available_system_ai_models(client)
    if available is not None:
        filtered = [m for m in models if m.name in available or m.isDefault]
        if any(m.name in available for m in models):
            models = filtered
        else:
            logger.warning(
                "No curated chat models found among %d system.ai model services; "
                "showing full curated list",
                len(available),
            )
    return _sort_models(models)


def validate_chat_model(
    model_name: str | None,
    *,
    client: WorkspaceClient | None = None,
) -> str | None:
    """Validate a selected model against the curated list; returns the normalized name.

    Legacy ``databricks-<model>`` names (old sessions, stale clients) map to
    ``system.ai.<model>`` before the check.
    """
    selected = normalize_model_name(model_name)
    if not selected:
        return None
    if selected in _CURATED_COMPATIBLE_CHAT_MODEL_NAMES:
        return selected

    raise ModelValidationError(
        f"Model '{model_name.strip() if model_name else selected}' is not in the curated list of supported chat models."
    )
