"""Phase 0 unit tests for the AI Gateway seam factory (backend copy).

The classic branch MUST be byte-identical to today's call sites; the gateway
branch is validated shape-only (no network). See
scripts/ai-gateway-validation/ai-gateway-migration-spec.md Appendix A.1.
"""

import json

import pytest

from backend.services import llm_route as lr


class TestGetLlmRoute:
    def test_unset_defaults_classic(self, monkeypatch):
        monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
        assert lr.get_llm_route() is lr.LLMRoute.CLASSIC

    def test_typo_defaults_classic(self, monkeypatch):
        monkeypatch.setenv("GENIE_LLM_ROUTE", "gatway")
        assert lr.get_llm_route() is lr.LLMRoute.CLASSIC

    def test_gateway_exact(self, monkeypatch):
        monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
        assert lr.get_llm_route() is lr.LLMRoute.GATEWAY

    def test_gateway_case_and_whitespace_normalized(self, monkeypatch):
        monkeypatch.setenv("GENIE_LLM_ROUTE", "  GATEWAY ")
        assert lr.get_llm_route() is lr.LLMRoute.GATEWAY


class TestGatewayModelName:
    def test_databricks_prefix_stripped(self):
        assert lr.gateway_model_name("databricks-claude-sonnet-4-6") == "system.ai.claude-sonnet-4-6"
        assert lr.gateway_model_name("databricks-gte-large-en") == "system.ai.gte-large-en"

    def test_system_ai_idempotent(self):
        assert lr.gateway_model_name("system.ai.gte-large-en") == "system.ai.gte-large-en"

    def test_byok_three_level_passthrough(self):
        # A 3-level UC id (contains a dot) must pass through unchanged.
        assert lr.gateway_model_name("cat.sch.my_model") == "cat.sch.my_model"


class TestRequestTags:
    def test_base_only(self):
        assert lr.request_tags("iq-scan") == {
            "application": "genie-workbench",
            "component": "iq-scan",
        }

    def test_scoped_extras_dropped_when_empty(self):
        assert lr.request_tags("gso-optimize", run_id=None, space_id="") == {
            "application": "genie-workbench",
            "component": "gso-optimize",
        }

    def test_scoped_extras_present(self):
        assert lr.request_tags("create-agent", space_id="sp1", run_id="r1") == {
            "application": "genie-workbench",
            "component": "create-agent",
            "run_id": "r1",
            "space_id": "sp1",
        }

    def test_tag_header_is_json_under_the_gateway_key(self):
        header = lr.tag_header("mv-suggest", space_id="sp9")
        assert set(header) == {"Databricks-Ai-Gateway-Request-Tags"}
        assert json.loads(header["Databricks-Ai-Gateway-Request-Tags"]) == {
            "application": "genie-workbench",
            "component": "mv-suggest",
            "space_id": "sp9",
        }


class TestResolveChat:
    def test_classic_is_byte_identical_to_today(self):
        # Mirrors backend/services/llm_utils.py:86 exactly:
        #   host = client.config.host.rstrip("/")
        #   url  = f"{host}/serving-endpoints/{model}/invocations"
        rc = lr.resolve_chat(
            "https://example.databricks.com/",
            "databricks-claude-sonnet-4-6",
            "workbench",
            route=lr.LLMRoute.CLASSIC,
        )
        assert rc.url == "https://example.databricks.com/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
        assert rc.model is None
        assert rc.extra_headers == {}
        assert rc.use_legacy_sdk is False

    def test_gateway_shape(self):
        rc = lr.resolve_chat(
            "https://example.databricks.com",
            "databricks-claude-sonnet-4-6",
            "create-agent",
            route=lr.LLMRoute.GATEWAY,
            space_id="sp1",
        )
        assert rc.url == "https://example.databricks.com/ai-gateway/mlflow/v1/chat/completions"
        assert rc.model == "system.ai.claude-sonnet-4-6"
        assert json.loads(rc.extra_headers["Databricks-Ai-Gateway-Request-Tags"]) == {
            "application": "genie-workbench",
            "component": "create-agent",
            "space_id": "sp1",
        }


class TestResolveEmbeddings:
    def test_classic_keeps_legacy_sdk(self):
        rc = lr.resolve_embeddings(
            "https://x", "databricks-gte-large-en", "leakage-embed", route=lr.LLMRoute.CLASSIC
        )
        assert rc.use_legacy_sdk is True
        assert rc.url == ""
        assert rc.model is None
        assert rc.extra_headers == {}

    def test_gateway_shape(self):
        rc = lr.resolve_embeddings(
            "https://x/", "databricks-gte-large-en", "leakage-embed", route=lr.LLMRoute.GATEWAY
        )
        assert rc.url == "https://x/ai-gateway/mlflow/v1/embeddings"
        assert rc.model == "system.ai.gte-large-en"
        assert rc.use_legacy_sdk is False


class TestIsReasoningEffort400:
    def test_true_only_on_400_mentioning_flag(self):
        assert lr.is_reasoning_effort_400(400, 'Extra inputs: reasoning_effort') is True

    def test_false_on_other_400(self):
        assert lr.is_reasoning_effort_400(400, "response_format not permitted") is False

    def test_false_on_non_400(self):
        assert lr.is_reasoning_effort_400(200, "reasoning_effort") is False


def test_is_model_unavailable_404_true_only_on_404():
    from backend.services.llm_route import is_model_unavailable_404
    assert is_model_unavailable_404(404, '{"error_code":"NOT_FOUND","message":"x does not exist"}') is True
    assert is_model_unavailable_404(404, "") is True            # body-agnostic (R8)
    assert is_model_unavailable_404(403, "forbidden") is False  # 403 is NOT the trigger (§3)
    assert is_model_unavailable_404(400, "reasoning_effort") is False
    assert is_model_unavailable_404(200, "") is False


def test_model_unavailable_message_is_the_single_copy():
    from backend.services.llm_route import MODEL_UNAVAILABLE_MESSAGE
    assert MODEL_UNAVAILABLE_MESSAGE == "Model unavailable or access not granted."
