"""Unity Gateway (AI Gateway v3) routing helper tests."""
from __future__ import annotations

import pytest

from genie_space_optimizer.common import ai_gateway as ag


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("databricks-claude-sonnet-4-6", "system.ai.claude-sonnet-4-6"),
        ("databricks-bge-large-en", "system.ai.bge-large-en"),
        ("  databricks-gpt-5-4  ", "system.ai.gpt-5-4"),
        ("system.ai.claude-opus-5", "system.ai.claude-opus-5"),
        ("main.models.my_service", "main.models.my_service"),
        ("custom-chat", "custom-chat"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_model_name(raw, expected):
    assert ag.normalize_model_name(raw) == expected


def test_normalize_model_name_is_idempotent():
    once = ag.normalize_model_name("databricks-claude-opus-4-7")
    assert ag.normalize_model_name(once) == once


def test_url_builders_strip_trailing_slash():
    host = "https://example.cloud.databricks.com/"
    assert ag.ai_gateway_base_url(host) == "https://example.cloud.databricks.com/ai-gateway/mlflow/v1"
    assert ag.chat_completions_url(host).endswith("/ai-gateway/mlflow/v1/chat/completions")
    assert ag.embeddings_url(host).endswith("/ai-gateway/mlflow/v1/embeddings")


NOT_FOUND_BODY = '{"error_code":"NOT_FOUND","message":"\'system.ai.nope\' does not exist."}'


def test_parse_gateway_error_json_and_text():
    assert ag.parse_gateway_error(NOT_FOUND_BODY) == ("NOT_FOUND", "'system.ai.nope' does not exist.")
    assert ag.parse_gateway_error("<html>nope</html>") == (None, "<html>nope</html>")
    assert ag.parse_gateway_error(None) == (None, "")
    nested = '{"error": {"code": "rate_limit", "message": "slow down"}}'
    assert ag.parse_gateway_error(nested) == ("rate_limit", "slow down")


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (404, NOT_FOUND_BODY, ag.MODEL_MISSING),
        (404, "<html>Not Found</html>", ag.GATEWAY_NOT_ENABLED),
        (404, '{"error_code":"ENDPOINT_NOT_FOUND","message":"route"}', ag.GATEWAY_NOT_ENABLED),
        (403, '{"error_code":"PERMISSION_DENIED","message":"User does not have EXECUTE"}', ag.PERMISSION_DENIED),
        (403, '{"error_code":"PERMISSION_DENIED","message":"required scopes: ai-gateway"}', ag.SCOPE_MISSING),
        (401, '{"message":"invalid token"}', ag.AUTH),
        (429, "", ag.TRANSIENT),
        (503, "", ag.TRANSIENT),
        (400, '{"error_code":"BAD_REQUEST","message":"max_tokens"}', ag.BAD_REQUEST),
        (418, "", ag.UNKNOWN),
    ],
)
def test_describe_gateway_failure_kind(status, body, expected):
    assert ag.describe_gateway_failure(status, body, "system.ai.nope").kind == expected


def test_describe_gateway_failure_detail_includes_code_and_model():
    detail = ag.describe_gateway_failure(404, NOT_FOUND_BODY, "system.ai.nope").detail
    assert detail == "Unity Gateway returned 404 NOT_FOUND for model=system.ai.nope: 'system.ai.nope' does not exist."
    assert ag.describe_gateway_failure(502, "bad gateway", "m").detail == "Unity Gateway returned 502 for model=m: bad gateway"


def test_grant_hint_mentions_catalog_schema_and_model_service():
    hint = ag.grant_hint("system.ai.claude-sonnet-4-6", "app-sp-id")
    assert "GRANT USE CATALOG ON CATALOG system TO `app-sp-id`;" in hint
    assert "GRANT USE SCHEMA ON SCHEMA system.ai TO `app-sp-id`;" in hint
    assert "databricks grants update model_service system.ai.claude-sonnet-4-6" in hint
    assert '"add": ["EXECUTE"]' in hint
