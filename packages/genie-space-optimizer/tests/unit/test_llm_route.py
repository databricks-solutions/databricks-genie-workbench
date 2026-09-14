"""Phase 0 unit tests for the AI Gateway seam factory (GSO copy).

Byte-identical to backend/services/llm_route.py (pinned by
backend/tests/test_llm_route_parity.py); the assertions mirror
backend/tests/test_llm_route.py against the GSO import path.
"""

import json

from genie_space_optimizer.optimization import llm_route as lr


def test_get_llm_route_defaults_classic(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    assert lr.get_llm_route() is lr.LLMRoute.CLASSIC


def test_get_llm_route_gateway_exact(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    assert lr.get_llm_route() is lr.LLMRoute.GATEWAY


def test_gateway_model_name_rules():
    assert lr.gateway_model_name("databricks-claude-sonnet-4-6") == "system.ai.claude-sonnet-4-6"
    assert lr.gateway_model_name("system.ai.gte-large-en") == "system.ai.gte-large-en"
    assert lr.gateway_model_name("cat.sch.my_model") == "cat.sch.my_model"


def test_resolve_chat_classic_byte_identical():
    rc = lr.resolve_chat(
        "https://x/", "databricks-claude-sonnet-4-6", "gso-optimize", route=lr.LLMRoute.CLASSIC
    )
    assert rc.url == "https://x/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
    assert rc.model is None
    assert rc.extra_headers == {}


def test_resolve_chat_gateway_tags_run_id():
    rc = lr.resolve_chat(
        "https://x", "databricks-claude-sonnet-4-6", "gso-optimize",
        route=lr.LLMRoute.GATEWAY, run_id="r7",
    )
    assert rc.url == "https://x/ai-gateway/mlflow/v1/chat/completions"
    assert rc.model == "system.ai.claude-sonnet-4-6"
    assert json.loads(rc.extra_headers["Databricks-Ai-Gateway-Request-Tags"]) == {
        "application": "genie-workbench",
        "component": "gso-optimize",
        "run_id": "r7",
    }


def test_resolve_embeddings_classic_keeps_sdk():
    rc = lr.resolve_embeddings("https://x", "databricks-gte-large-en", "leakage-embed",
                               route=lr.LLMRoute.CLASSIC)
    assert rc.use_legacy_sdk is True
    assert rc.url == ""


def test_is_reasoning_effort_400():
    assert lr.is_reasoning_effort_400(400, "reasoning_effort not permitted") is True
    assert lr.is_reasoning_effort_400(400, "something else") is False
    assert lr.is_reasoning_effort_400(200, "reasoning_effort") is False
