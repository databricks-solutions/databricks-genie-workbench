"""Serving endpoint catalog helpers for user-selectable LLM models."""

from __future__ import annotations

import logging
import os
import re

from databricks.sdk import WorkspaceClient

from backend.models import LLMModelInfo
from backend.services.llm_route import LLMRoute, get_llm_route
from backend.services.llm_utils import get_llm_model

logger = logging.getLogger(__name__)

_CURATED_COMPATIBLE_CHAT_MODELS: tuple[tuple[str, str], ...] = (
    ("databricks-claude-opus-5", "Claude Opus 5"),
    ("databricks-claude-sonnet-5", "Claude Sonnet 5"),
    ("databricks-claude-opus-4-8", "Claude Opus 4.8"),
    ("databricks-claude-opus-4-7", "Claude Opus 4.7"),
    ("databricks-claude-sonnet-4-6", "Claude Sonnet 4.6"),
    # GPT 5.5 currently requires Responses API for function-tool flows.
    # Create Agent uses Chat Completions tools, so keep it out of the shared picker.
    ("databricks-gpt-5-4", "GPT-5.4"),
    ("databricks-gpt-5-2", "GPT-5.2"),
)
_CURATED_COMPATIBLE_CHAT_MODEL_NAMES = {
    name for name, _display_name in _CURATED_COMPATIBLE_CHAT_MODELS
}


def _optimizer_prompt_budget_chars() -> int:
    raw = os.getenv("GSO_OPTIMIZER_PROMPT_MAX_CHARS", "60000").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        logger.warning("Invalid GSO_OPTIMIZER_PROMPT_MAX_CHARS=%r; using 60000", raw)
        return 60_000


_UC_MODEL_ID_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){2}$")
_BYOK_TRUTHY = {"1", "true", "yes", "on"}


def _byok_enabled() -> bool:
    """GENIE_BYOK_ENABLED gate (spec §2, Decision 2). Off unless explicitly enabled."""
    return (os.environ.get("GENIE_BYOK_ENABLED") or "").strip().lower() in _BYOK_TRUTHY


def _is_wellformed_uc_model_id(name: str) -> bool:
    """A BYOK model is a full 3-level UC id (catalog.schema.name) — never a bare
    endpoint/registered-model name (the entity_name trap, §1.4). Requires exactly
    two dots and three non-empty UC-identifier segments."""
    return bool(_UC_MODEL_ID_RE.match(name))


class ModelCatalogError(RuntimeError):
    """Raised when model catalog metadata cannot be read."""


class ModelValidationError(ValueError):
    """Raised when a selected model is not usable for chat completions."""


def _curated_model_infos(default_model: str) -> list[LLMModelInfo]:
    """Databricks-hosted pay-per-token endpoints supported by this app.

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


def list_chat_models(
    *,
    client: WorkspaceClient | None = None,
    allow_sp_fallback: bool = False,
) -> list[LLMModelInfo]:
    """Return curated chat models known to work with this app's LLM calls."""
    default_model = get_llm_model()
    if default_model not in _CURATED_COMPATIBLE_CHAT_MODEL_NAMES:
        logger.warning(
            "Configured LLM_MODEL=%s is not in the curated selectable model list",
            default_model,
        )
    return _sort_models(_curated_model_infos(default_model))


def validate_chat_model(
    model_name: str | None,
    *,
    client: WorkspaceClient | None = None,
) -> str | None:
    """Validate a selected model name against the curated compatible list."""
    selected = (model_name or "").strip()
    if not selected:
        return None
    if selected in _CURATED_COMPATIBLE_CHAT_MODEL_NAMES:
        return selected

    # BYOK (spec §1.4/§2, Decision 2): on the gateway route with BYOK enabled,
    # additionally accept a customer's full 3-level UC model id, returned
    # verbatim (no strip — the seam passes 3-level ids through unchanged).
    # Strictly additive; the classic route and BYOK-off keep rejecting.
    if (
        get_llm_route() is LLMRoute.GATEWAY
        and _byok_enabled()
        and _is_wellformed_uc_model_id(selected)
    ):
        return selected

    raise ModelValidationError(
        f"Model '{selected}' is not in the curated list of supported chat models."
    )
