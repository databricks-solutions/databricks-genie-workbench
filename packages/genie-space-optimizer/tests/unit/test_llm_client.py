"""Unit tests for genie_space_optimizer.optimization.llm_client.

Covers the shared OpenAI-based LLM helpers (get_openai_client,
_resolve_bearer_token, call_llm) and verifies that migrated call sites
no longer use ``serving_endpoints.query``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_config(
    *,
    host: str = "https://my-workspace.cloud.databricks.com",
    token: str | None = "dapi1234567890abcdef",
    auth_headers: dict[str, str] | None = None,
    authenticate_raises: Exception | None = None,
) -> MagicMock:
    cfg = MagicMock()
    cfg.host = host
    type(cfg).token = PropertyMock(return_value=token)
    if authenticate_raises:
        cfg.authenticate.side_effect = authenticate_raises
    elif auth_headers is not None:
        cfg.authenticate.return_value = auth_headers
    else:
        cfg.authenticate.return_value = (
            {"Authorization": f"Bearer {token}"} if token else {}
        )
    return cfg


def _make_mock_ws(config: MagicMock | None = None, **kwargs) -> MagicMock:
    ws = MagicMock()
    ws.config = config or _make_mock_config(**kwargs)
    return ws


def _make_openai_response(content: str = '{"key": "value"}', *, prompt_tokens: int = 100, completion_tokens: int = 50) -> MagicMock:
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content=content))]
    resp.usage = MagicMock(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
    )
    return resp


# ═══════════════════════════════════════════════════════════════════════════
# _resolve_bearer_token (in llm_client.py)
# ═══════════════════════════════════════════════════════════════════════════

class TestResolveBearerTokenShared:
    def _resolve(self, wc):
        from genie_space_optimizer.optimization.llm_client import _resolve_bearer_token
        return _resolve_bearer_token(wc)

    def test_pat_token_returned(self):
        ws = _make_mock_ws(token="dapi_abc123")
        assert self._resolve(ws) == "dapi_abc123"

    def test_oauth_fallback(self):
        ws = _make_mock_ws(
            token=None,
            auth_headers={"Authorization": "Bearer oauth_tok"},
        )
        assert self._resolve(ws) == "oauth_tok"

    def test_no_auth_raises(self):
        ws = _make_mock_ws(token=None, auth_headers={})
        with pytest.raises(RuntimeError, match="Cannot resolve a bearer token"):
            self._resolve(ws)


# ═══════════════════════════════════════════════════════════════════════════
# get_openai_client
# ═══════════════════════════════════════════════════════════════════════════

class TestGetOpenaiClientShared:
    def setup_method(self):
        from genie_space_optimizer.optimization import llm_client
        llm_client._openai_client_cache.clear()

    @patch("openai.OpenAI")
    def test_creates_client_with_correct_base_url(self, MockOpenAI):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        ws = _make_mock_ws(
            host="https://test.cloud.databricks.com",
            token="dapi_tok",
        )
        get_openai_client(ws)
        MockOpenAI.assert_called_once_with(
            api_key="dapi_tok",
            base_url="https://test.cloud.databricks.com/serving-endpoints",
        )

    @patch("openai.OpenAI")
    def test_caches_by_host(self, MockOpenAI):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        ws = _make_mock_ws(token="tok1")
        c1 = get_openai_client(ws)
        c2 = get_openai_client(ws)
        assert c1 is c2
        assert MockOpenAI.call_count == 1

    @patch("openai.OpenAI")
    def test_refreshes_token(self, MockOpenAI):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client

        ws1 = _make_mock_ws(token="v1")
        get_openai_client(ws1)

        ws2 = _make_mock_ws(token="v2")
        get_openai_client(ws2)
        assert mock_client.api_key == "v2"


# ═══════════════════════════════════════════════════════════════════════════
# call_llm
# ═══════════════════════════════════════════════════════════════════════════

class TestCallLlm:
    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_returns_text_and_response(self, mock_get_client):
        from genie_space_optimizer.optimization.llm_client import call_llm

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response('{"ok": true}')
        mock_get_client.return_value = mock_client

        text, resp = call_llm(None, messages=[{"role": "user", "content": "hi"}])
        assert text == '{"ok": true}'
        assert resp.usage.prompt_tokens == 100

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_retries_on_failure(self, mock_get_client):
        from genie_space_optimizer.optimization.llm_client import call_llm

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = [
            ValueError("transient"),
            _make_openai_response("ok"),
        ]
        mock_get_client.return_value = mock_client

        text, _ = call_llm(
            None,
            messages=[{"role": "user", "content": "hi"}],
            max_retries=2,
        )
        assert text == "ok"
        assert mock_client.chat.completions.create.call_count == 2

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_raises_after_exhausted_retries(self, mock_get_client):
        from genie_space_optimizer.optimization.llm_client import call_llm

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("persistent")
        mock_get_client.return_value = mock_client

        with pytest.raises(RuntimeError, match="persistent"):
            call_llm(
                None,
                messages=[{"role": "user", "content": "hi"}],
                max_retries=2,
            )

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_empty_response_raises(self, mock_get_client):
        from genie_space_optimizer.optimization.llm_client import call_llm

        resp = MagicMock()
        resp.choices = []
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = resp
        mock_get_client.return_value = mock_client

        with pytest.raises(ValueError, match="no choices"):
            call_llm(
                None,
                messages=[{"role": "user", "content": "hi"}],
                max_retries=1,
            )


# ═══════════════════════════════════════════════════════════════════════════
# Regression: _call_llm_for_scoring uses OpenAI SDK
# ═══════════════════════════════════════════════════════════════════════════

class TestCallLlmForScoringMigrated:
    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_uses_openai_not_serving_endpoints(self, mock_get_client):
        from genie_space_optimizer.optimization.evaluation import _call_llm_for_scoring

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(
            '{"score": 5, "rationale": "good"}'
        )
        mock_get_client.return_value = mock_client

        ws = _make_mock_ws()
        result = _call_llm_for_scoring(ws, "Rate this.")

        mock_client.chat.completions.create.assert_called()
        ws.serving_endpoints.query.assert_not_called()
        assert result["score"] == 5

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_returns_parsed_json(self, mock_get_client):
        from genie_space_optimizer.optimization.evaluation import _call_llm_for_scoring

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(
            '{"verdict": "correct", "rationale": "matches"}'
        )
        mock_get_client.return_value = mock_client

        result = _call_llm_for_scoring(_make_mock_ws(), "Evaluate this.")
        assert result["verdict"] == "correct"
        assert result["rationale"] == "matches"


# ═══════════════════════════════════════════════════════════════════════════
# Regression: optimizer LLM calls use OpenAI SDK
# ═══════════════════════════════════════════════════════════════════════════

class TestOptimizerLlmMigrated:
    """Verify that optimizer's proposal/join/holistic functions use OpenAI."""

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_call_llm_for_proposal_uses_openai(self, mock_get_client):
        from genie_space_optimizer.optimization.optimizer import _call_llm_for_proposal

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(
            '{"proposed_value": "new desc", "rationale": "better"}'
        )
        mock_get_client.return_value = mock_client

        ws = _make_mock_ws()
        result = _call_llm_for_proposal(
            cluster={"cluster_id": "C1", "root_cause": "wrong_table", "question_ids": ["q1"]},
            metadata_snapshot={"tables": [], "instructions": {}, "data_sources": {}},
            patch_type="update_description",
            lever=1,
            w=ws,
        )

        mock_client.chat.completions.create.assert_called()
        ws.serving_endpoints.query.assert_not_called()
        assert result.get("proposed_value") == "new desc"

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_call_llm_for_join_discovery_uses_openai(self, mock_get_client):
        from genie_space_optimizer.optimization.optimizer import _call_llm_for_join_discovery

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(
            '{"join_specs": [], "rationale": "none found"}'
        )
        mock_get_client.return_value = mock_client

        ws = _make_mock_ws()
        result = _call_llm_for_join_discovery(
            metadata_snapshot={"tables": [], "instructions": {}, "data_sources": {}},
            hints=[],
            w=ws,
        )

        mock_client.chat.completions.create.assert_called()
        ws.serving_endpoints.query.assert_not_called()
        assert isinstance(result, list)

    @patch("genie_space_optimizer.optimization.llm_client.get_openai_client")
    def test_call_llm_for_holistic_uses_openai(self, mock_get_client):
        from genie_space_optimizer.optimization.optimizer import _call_llm_for_holistic_instructions

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(
            '{"instruction_text": "Use UTC.", "example_sql_proposals": [], "rationale": "best practice"}'
        )
        mock_get_client.return_value = mock_client

        ws = _make_mock_ws()
        result = _call_llm_for_holistic_instructions(
            all_clusters=[],
            metadata_snapshot={"tables": [], "instructions": {}, "data_sources": {}},
            w=ws,
        )

        mock_client.chat.completions.create.assert_called()
        ws.serving_endpoints.query.assert_not_called()
        assert result["instruction_text"] == "Use UTC."


# ═══════════════════════════════════════════════════════════════════════════
# Regression: _traced_llm_call logs token usage
# ═══════════════════════════════════════════════════════════════════════════

class TestTracedLlmCallTokenUsage:
    @patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
    def test_logs_token_usage_on_span(self, mock_get_client):
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(
            "hello", prompt_tokens=42, completion_tokens=10
        )
        mock_get_client.return_value = mock_client

        text, resp = _traced_llm_call(
            None,
            "You are helpful.",
            "Say hello",
            span_name="test_span",
            max_retries=1,
        )

        assert text == "hello"
        assert resp.usage.prompt_tokens == 42

    @patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
    def test_no_usage_no_crash(self, mock_get_client):
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call

        resp = _make_openai_response("ok")
        resp.usage = None
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = resp
        mock_get_client.return_value = mock_client

        text, _ = _traced_llm_call(
            None, "system", "user", span_name="test_no_usage", max_retries=1
        )
        assert text == "ok"


# ═══════════════════════════════════════════════════════════════════════════
# Backward compatibility: optimizer re-exports
# ═══════════════════════════════════════════════════════════════════════════

class TestBackwardCompatReexports:
    """Ensure optimizer.py still exposes the moved symbols."""

    def test_resolve_bearer_token_accessible(self):
        from genie_space_optimizer.optimization.optimizer import _resolve_bearer_token
        assert callable(_resolve_bearer_token)

    def test_get_openai_client_accessible(self):
        from genie_space_optimizer.optimization.optimizer import _get_openai_client
        assert callable(_get_openai_client)

    def test_openai_client_cache_accessible(self):
        from genie_space_optimizer.optimization.optimizer import _openai_client_cache
        assert isinstance(_openai_client_cache, dict)
