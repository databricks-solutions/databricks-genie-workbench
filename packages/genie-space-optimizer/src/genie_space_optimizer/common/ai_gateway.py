"""Unity Gateway (AI Gateway v3) routing helpers shared by the app and GSO.

All LLM and embedding traffic goes through the workspace Unity Gateway route
``/ai-gateway/mlflow/v1`` and addresses models by their Unity Catalog model
service name (``system.ai.<model>``). The legacy pay-per-token serving
endpoints (``/serving-endpoints/databricks-<model>``, "v1") are being retired
and are rejected outright in workspaces with "Enforce Unity Gateway" enabled.

Model names persisted before the migration (Lakebase sessions, GSO run rows,
``.env.deploy`` / ``LLM_MODEL`` values) use the legacy ``databricks-<model>``
form; ``normalize_model_name`` maps them to ``system.ai.<model>``.

Stdlib only: the deploy library imports this before project deps are installed.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

AI_GATEWAY_BASE_PATH = "/ai-gateway/mlflow/v1"
SYSTEM_AI_PREFIX = "system.ai."
LEGACY_PREFIX = "databricks-"

DEFAULT_CHAT_MODEL = "system.ai.claude-sonnet-4-6"
DEFAULT_EMBEDDING_MODEL = "system.ai.bge-large-en"

CHAT_COMPLETIONS_API_TYPE = "mlflow/v1/chat/completions"

MODEL_MISSING = "MODEL_MISSING"
GATEWAY_NOT_ENABLED = "GATEWAY_NOT_ENABLED"
PERMISSION_DENIED = "PERMISSION_DENIED"
SCOPE_MISSING = "SCOPE_MISSING"
AUTH = "AUTH"
TRANSIENT = "TRANSIENT"
BAD_REQUEST = "BAD_REQUEST"
UNKNOWN = "UNKNOWN"

_logged_legacy_names: set[str] = set()


def normalize_model_name(name: str | None) -> str:
    """Map a legacy ``databricks-<model>`` endpoint name to ``system.ai.<model>``.

    ``None``/blank returns ``""``. Names already in ``system.ai.*`` form and
    any other name (custom model services, test doubles) pass through unchanged.
    """
    value = (name or "").strip()
    if not value.startswith(LEGACY_PREFIX):
        return value
    normalized = SYSTEM_AI_PREFIX + value[len(LEGACY_PREFIX):]
    if value not in _logged_legacy_names:
        _logged_legacy_names.add(value)
        logger.info("Mapped legacy model name %s -> %s", value, normalized)
    return normalized


def ai_gateway_base_url(host: str) -> str:
    """OpenAI-compatible base URL for Databricks-hosted model services."""
    return f"{(host or '').rstrip('/')}{AI_GATEWAY_BASE_PATH}"


def chat_completions_url(host: str) -> str:
    return f"{ai_gateway_base_url(host)}/chat/completions"


def embeddings_url(host: str) -> str:
    return f"{ai_gateway_base_url(host)}/embeddings"


def parse_gateway_error(text: str | None) -> tuple[str | None, str]:
    """Return ``(error_code, message)`` from a gateway error body.

    Unity Gateway errors look like ``{"error_code": "NOT_FOUND", "message": "..."}``.
    Non-JSON bodies return ``(None, text[:500])``.
    """
    raw = text or ""
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None, raw[:500]
    if not isinstance(payload, dict):
        return None, raw[:500]
    error = payload.get("error")
    if isinstance(error, dict):
        code = error.get("code") or error.get("type")
        message = error.get("message") or raw[:500]
        return (str(code) if code else None), str(message)[:500]
    code = payload.get("error_code")
    message = payload.get("message") or raw[:500]
    return (str(code) if code else None), str(message)[:500]


def classify_gateway_failure(status: int, text: str | None, model: str | None = None) -> str:
    """Bucket a failed Unity Gateway response into an actionable failure kind."""
    code, message = parse_gateway_error(text)
    lowered = message.lower()
    if status == 404:
        if code == "NOT_FOUND" and (
            "does not exist" in lowered or (model and model.lower() in lowered)
        ):
            return MODEL_MISSING
        return GATEWAY_NOT_ENABLED
    if status in (401, 403) and "scope" in lowered:
        return SCOPE_MISSING
    if status == 403:
        return PERMISSION_DENIED
    if status == 401:
        return AUTH
    if status == 429 or status >= 500:
        return TRANSIENT
    if status == 400:
        return BAD_REQUEST
    return UNKNOWN


def format_gateway_error(status: int, text: str | None, model: str | None) -> str:
    """One-line error for logs and exceptions."""
    code, message = parse_gateway_error(text)
    detail = f"{status} {code}" if code else f"{status}"
    return f"Unity Gateway returned {detail} for model={model}: {message}"


def scope_missing_message(model: str | None) -> str:
    return (
        f"Your sign-in is missing the 'ai-gateway' permission required to call {model}. "
        "Sign out of the app and sign back in to grant the updated permissions."
    )


def grant_hint(model: str, principal: str) -> str:
    """Commands that grant ``principal`` query access to a system.ai model service.

    By default all account users have EXECUTE on ``system.ai`` model services,
    so a 403 usually means an admin added a DENY or revoked the default grant.
    """
    changes = json.dumps({"changes": [{"principal": principal, "add": ["EXECUTE"]}]})
    return "\n".join([
        f"Grant {principal} access to {model} (requires a metastore/catalog admin):",
        f"  GRANT USE CATALOG ON CATALOG system TO `{principal}`;",
        f"  GRANT USE SCHEMA ON SCHEMA system.ai TO `{principal}`;",
        f"  databricks grants update model_service {model} --json '{changes}'",
        "Note: account users have EXECUTE on system.ai model services by default;",
        "a 403 usually means an admin added a DENY on the model service or system.ai.",
    ])
