"""Unity Gateway (AI Gateway v3) deploy preflight shared by both install paths.

The app and GSO job call ``/ai-gateway/mlflow/v1`` with ``system.ai.*`` model
service names. Seeing ``system.ai`` in Catalog Explorer does not prove the
route is enabled for the workspace, so the preflight sends real (tiny)
inference requests as the deployer.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests

from genie_space_optimizer.common.ai_gateway import (
    AUTH,
    BAD_REQUEST,
    DEFAULT_EMBEDDING_MODEL,
    GATEWAY_NOT_ENABLED,
    MODEL_MISSING,
    PERMISSION_DENIED,
    SCOPE_MISSING,
    TRANSIENT,
    chat_completions_url,
    chat_probe_body,
    describe_gateway_failure,
    embeddings_url,
    grant_hint,
    normalize_model_name,
)

_PROBE_RETRIES = 2
_PROBE_TIMEOUT_SECONDS = 60


@dataclass
class ProbeResult:
    model: str
    status: int | None
    kind: str | None = None
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == 200


@dataclass
class AiGatewayPreflight:
    chat: ProbeResult
    embedding: ProbeResult
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "chat_model": self.chat.model,
            "chat_status": self.chat.status,
            "embedding_model": self.embedding.model,
            "embedding_status": self.embedding.status,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


def _post(w, url: str, body: dict[str, Any], model: str) -> ProbeResult:
    last: ProbeResult | None = None
    for attempt in range(_PROBE_RETRIES + 1):
        try:
            resp = requests.post(
                url,
                json=body,
                headers=w.config.authenticate(),
                timeout=_PROBE_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            last = ProbeResult(model=model, status=None, kind=TRANSIENT, detail=str(exc))
        else:
            if resp.status_code == 200:
                return ProbeResult(model=model, status=200)
            failure = describe_gateway_failure(resp.status_code, resp.text, model)
            last = ProbeResult(
                model=model, status=resp.status_code, kind=failure.kind, detail=failure.detail,
            )
            if last.kind != TRANSIENT:
                return last
        if attempt < _PROBE_RETRIES:
            time.sleep(2 ** attempt)
    assert last is not None
    return last


def probe_chat(w, model: str) -> ProbeResult:
    return _post(w, chat_completions_url(w.config.host), chat_probe_body(model), model)


def probe_embeddings(w, model: str) -> ProbeResult:
    body = {"model": model, "input": ["ping"]}
    return _post(w, embeddings_url(w.config.host), body, model)


def _deployer_principal(w) -> str:
    try:
        me = w.current_user.me()
        return me.user_name or me.display_name or "<deployer>"
    except Exception:
        return "<deployer>"


def _chat_error(w, result: ProbeResult) -> str:
    model = result.model
    if result.kind == MODEL_MISSING:
        return (
            f"Model service {model} does not exist in this workspace's system.ai catalog "
            f"({result.detail}). Set GENIE_LLM_MODEL (or the llm_model widget) to an "
            "available system.ai chat model."
        )
    if result.kind == GATEWAY_NOT_ENABLED:
        return (
            f"Unity Gateway route /ai-gateway/mlflow/v1 is not reachable ({result.detail}). "
            "Unity Gateway may not be enabled for this workspace; system.ai being visible in "
            "Catalog Explorer is not enough. Ask your Databricks account team to enable it."
        )
    if result.kind == SCOPE_MISSING:
        return (
            f"The deploy credential lacks the 'ai-gateway' scope ({result.detail}). "
            "Re-authenticate the CLI profile (databricks auth login) or use a token with the "
            "ai-gateway or all-apis scope."
        )
    if result.kind == PERMISSION_DENIED:
        return f"{result.detail}\n{grant_hint(model, _deployer_principal(w))}"
    if result.kind == AUTH:
        return f"Authentication to Unity Gateway failed ({result.detail}). Re-authenticate the CLI profile."
    return f"Unity Gateway chat probe failed: {result.detail}"


def run_ai_gateway_preflight(
    w,
    chat_model: str,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
) -> AiGatewayPreflight:
    """Probe the default chat model (hard requirement) and embeddings (warning only)."""
    chat_model = normalize_model_name(chat_model)
    embedding_model = normalize_model_name(embedding_model)

    chat = probe_chat(w, chat_model)
    embedding = probe_embeddings(w, embedding_model)
    report = AiGatewayPreflight(chat=chat, embedding=embedding)

    if not chat.ok:
        if chat.kind in (BAD_REQUEST, TRANSIENT):
            report.warnings.append(
                f"Unity Gateway chat probe for {chat_model} was inconclusive: {chat.detail or 'no response'}"
            )
        else:
            report.errors.append(_chat_error(w, chat))

    if not embedding.ok:
        report.warnings.append(
            f"Embedding probe for {embedding_model} failed ({embedding.detail or 'no response'}); "
            "the Auto-Optimize leakage firewall falls back to n-gram matching."
        )
    return report
