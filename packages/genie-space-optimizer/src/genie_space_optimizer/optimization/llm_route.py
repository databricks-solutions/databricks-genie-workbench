from __future__ import annotations
import json, os
from dataclasses import dataclass
from enum import Enum


class LLMRoute(str, Enum):
    CLASSIC = "classic"
    GATEWAY = "gateway"


_GATEWAY_CHAT  = "/ai-gateway/mlflow/v1/chat/completions"
_GATEWAY_EMBED = "/ai-gateway/mlflow/v1/embeddings"
_TAG_HEADER    = "Databricks-Ai-Gateway-Request-Tags"
_APPLICATION   = "genie-workbench"


def get_llm_route() -> LLMRoute:
    """GENIE_LLM_ROUTE env; anything but 'gateway' (incl. unset/typo) -> CLASSIC (fail-safe)."""
    return LLMRoute.GATEWAY if (os.environ.get("GENIE_LLM_ROUTE") or "").strip().lower() == "gateway" \
        else LLMRoute.CLASSIC


def gateway_model_name(endpoint: str) -> str:
    """Classic endpoint name -> gateway model id.
       '<cat>.<sch>.<name>' (BYOK 3-level UC id) -> unchanged;
       already 'system.ai.*' -> unchanged;
       'databricks-<x>' -> 'system.ai.<x>' (validated for chat AND embeddings)."""
    if "." in endpoint or endpoint.startswith("system.ai."):
        return endpoint
    return "system.ai." + endpoint.removeprefix("databricks-")


def request_tags(component: str, *, run_id: str | None = None,
                 space_id: str | None = None) -> dict[str, str]:
    tags = {"application": _APPLICATION, "component": component}
    if run_id:   tags["run_id"] = run_id       # scoped extras (Decision 1); empties dropped
    if space_id: tags["space_id"] = space_id
    return tags


def tag_header(component: str, **scope) -> dict[str, str]:
    return {_TAG_HEADER: json.dumps(request_tags(component, **scope))}


@dataclass(frozen=True)
class ResolvedCall:
    url: str                        # full endpoint URL ("" == "keep the legacy SDK path", embeddings)
    model: str | None               # model id for the BODY (gateway); None when it's in the URL (classic)
    extra_headers: dict[str, str]   # merge into the request (tag header on gateway; {} on classic)
    use_legacy_sdk: bool = False    # embeddings classic branch -> keep w.serving_endpoints.query()


def resolve_chat(host: str, endpoint: str, component: str, *, route: LLMRoute | None = None,
                 run_id: str | None = None, space_id: str | None = None) -> ResolvedCall:
    host = host.rstrip("/"); r = route or get_llm_route()
    if r is LLMRoute.CLASSIC:
        return ResolvedCall(f"{host}/serving-endpoints/{endpoint}/invocations", None, {})
    return ResolvedCall(f"{host}{_GATEWAY_CHAT}", gateway_model_name(endpoint),
                        tag_header(component, run_id=run_id, space_id=space_id))


def resolve_embeddings(host: str, endpoint: str, component: str, *, route: LLMRoute | None = None,
                       run_id: str | None = None, space_id: str | None = None) -> ResolvedCall:
    host = host.rstrip("/"); r = route or get_llm_route()
    if r is LLMRoute.CLASSIC:
        return ResolvedCall("", None, {}, use_legacy_sdk=True)   # keep SDK query()
    return ResolvedCall(f"{host}{_GATEWAY_EMBED}", gateway_model_name(endpoint),
                        tag_header(component, run_id=run_id, space_id=space_id))


def is_reasoning_effort_400(status: int, body_text: str) -> bool:
    """Retry trigger for the tool path (§2): Claude 400s on the flag, reasoning models require it."""
    return status == 400 and "reasoning_effort" in body_text


MODEL_UNAVAILABLE_MESSAGE = "Model unavailable or access not granted."


def is_model_unavailable_404(status: int, body_text: str) -> bool:
    """Downgrade trigger (§3): the gateway returns 404 for BOTH not-entitled and
    unknown-model (indistinguishable at the HTTP layer). Body-agnostic; callers
    MUST gate on the gateway route so a classic 404 keeps its raw-body error."""
    return status == 404
