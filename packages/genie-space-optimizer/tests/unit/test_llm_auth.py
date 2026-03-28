"""Unit tests for the LLM auth helpers in optimizer.py.

Covers _resolve_bearer_token, _ws_with_timeout, and _get_openai_client
across PAT, OAuth/SP, and broken-auth scenarios.
"""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers to build mock WorkspaceClient / Config objects
# ---------------------------------------------------------------------------

def _make_mock_config(
    *,
    host: str = "https://my-workspace.cloud.databricks.com",
    token: str | None = "dapi1234567890abcdef",
    auth_headers: dict[str, str] | None = None,
    authenticate_raises: Exception | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> MagicMock:
    """Build a mock ``Config`` that simulates various auth methods."""
    cfg = MagicMock()
    cfg.host = host
    type(cfg).token = PropertyMock(return_value=token)
    cfg.client_id = client_id
    cfg.client_secret = client_secret
    cfg._credentials_strategy = MagicMock()

    if authenticate_raises:
        cfg.authenticate.side_effect = authenticate_raises
    elif auth_headers is not None:
        cfg.authenticate.return_value = auth_headers
    else:
        cfg.authenticate.return_value = {"Authorization": f"Bearer {token}"} if token else {}

    return cfg


def _make_mock_ws(config: MagicMock | None = None, **kwargs) -> MagicMock:
    """Build a mock ``WorkspaceClient`` wrapping *config*."""
    ws = MagicMock()
    ws.config = config or _make_mock_config(**kwargs)
    return ws


# ═══════════════════════════════════════════════════════════════════════════
# _resolve_bearer_token
# ═══════════════════════════════════════════════════════════════════════════

class TestResolveBearerToken:
    def _resolve(self, wc):
        from genie_space_optimizer.optimization.optimizer import _resolve_bearer_token
        return _resolve_bearer_token(wc)

    def test_pat_token_returned_directly(self):
        ws = _make_mock_ws(token="dapi_abc123_longtoken")
        assert self._resolve(ws) == "dapi_abc123_longtoken"
        ws.config.authenticate.assert_not_called()

    def test_none_token_falls_back_to_authenticate(self):
        ws = _make_mock_ws(
            token=None,
            auth_headers={"Authorization": "Bearer oauth_fresh_token_xyz"},
        )
        assert self._resolve(ws) == "oauth_fresh_token_xyz"

    def test_empty_string_token_falls_back_to_authenticate(self):
        ws = _make_mock_ws(
            token="",
            auth_headers={"Authorization": "Bearer from_authenticate"},
        )
        assert self._resolve(ws) == "from_authenticate"

    def test_oauth_bearer_prefix_case_insensitive(self):
        ws = _make_mock_ws(
            token=None,
            auth_headers={"Authorization": "BEARER upper_case_token"},
        )
        assert self._resolve(ws) == "upper_case_token"

    def test_no_auth_raises_runtime_error(self):
        ws = _make_mock_ws(token=None, auth_headers={})
        with pytest.raises(RuntimeError, match="Cannot resolve a bearer token"):
            self._resolve(ws)

    def test_authenticate_exception_falls_through_to_runtime_error(self):
        ws = _make_mock_ws(
            token=None,
            authenticate_raises=OSError("IMDS unreachable"),
        )
        with pytest.raises(RuntimeError, match="Cannot resolve a bearer token"):
            self._resolve(ws)

    def test_non_bearer_auth_header_raises(self):
        ws = _make_mock_ws(
            token=None,
            auth_headers={"Authorization": "Basic dXNlcjpwYXNz"},
        )
        with pytest.raises(RuntimeError, match="Cannot resolve a bearer token"):
            self._resolve(ws)


# ═══════════════════════════════════════════════════════════════════════════
# _ws_with_timeout
# ═══════════════════════════════════════════════════════════════════════════

class TestWsWithTimeout:
    def _ws_with_timeout(self, w, timeout=600):
        from genie_space_optimizer.optimization.optimizer import _ws_with_timeout
        return _ws_with_timeout(w, timeout)

    @patch("genie_space_optimizer.optimization.optimizer.WorkspaceClient")
    @patch("databricks.sdk.config.Config")
    def test_none_w_creates_from_env(self, MockConfig, MockWC):
        self._ws_with_timeout(None)
        MockConfig.assert_called_once()
        call_kwargs = MockConfig.call_args
        assert call_kwargs.kwargs.get("http_timeout_seconds") == 600

    @patch("genie_space_optimizer.optimization.optimizer.WorkspaceClient")
    @patch("databricks.sdk.config.Config")
    def test_cloned_config_preserves_host(self, MockConfig, MockWC):
        orig_config = MagicMock()
        orig_config.host = "https://my-ws.databricks.com"
        orig_config.token = "dapi_test"
        orig_config._credentials_strategy = MagicMock()

        # Make Config.attributes() return mock attrs with .name
        attr_host = MagicMock()
        attr_host.name = "host"
        attr_token = MagicMock()
        attr_token.name = "token"
        MockConfig.attributes.return_value = [attr_host, attr_token]

        orig_ws = MagicMock()
        orig_ws.config = orig_config

        self._ws_with_timeout(orig_ws)

        call_kwargs = MockConfig.call_args.kwargs
        assert call_kwargs["host"] == "https://my-ws.databricks.com"
        assert call_kwargs["http_timeout_seconds"] == 600

    @patch("genie_space_optimizer.optimization.optimizer.WorkspaceClient")
    @patch("databricks.sdk.config.Config")
    def test_credentials_strategy_forwarded(self, MockConfig, MockWC):
        creds = MagicMock()
        orig_config = MagicMock()
        orig_config._credentials_strategy = creds
        MockConfig.attributes.return_value = []

        orig_ws = MagicMock()
        orig_ws.config = orig_config

        self._ws_with_timeout(orig_ws)

        call_kwargs = MockConfig.call_args.kwargs
        assert call_kwargs["credentials_strategy"] is creds

    @patch("genie_space_optimizer.optimization.optimizer.WorkspaceClient")
    @patch("databricks.sdk.config.Config")
    def test_none_attributes_not_copied(self, MockConfig, MockWC):
        """Attributes that are None on the source config are skipped."""
        orig_config = MagicMock()
        orig_config.token = None
        orig_config.host = "https://x.com"
        orig_config._credentials_strategy = MagicMock()

        attr_host = MagicMock()
        attr_host.name = "host"
        attr_token = MagicMock()
        attr_token.name = "token"
        MockConfig.attributes.return_value = [attr_host, attr_token]

        orig_ws = MagicMock()
        orig_ws.config = orig_config

        self._ws_with_timeout(orig_ws)

        call_kwargs = MockConfig.call_args.kwargs
        assert "token" not in call_kwargs
        assert call_kwargs["host"] == "https://x.com"

    @patch("genie_space_optimizer.optimization.optimizer.WorkspaceClient")
    @patch("databricks.sdk.config.Config")
    def test_timeout_overrides_source(self, MockConfig, MockWC):
        """Even if source has http_timeout_seconds, the requested value wins."""
        orig_config = MagicMock()
        orig_config.http_timeout_seconds = 30
        orig_config.host = "https://x.com"
        orig_config._credentials_strategy = MagicMock()

        attr_timeout = MagicMock()
        attr_timeout.name = "http_timeout_seconds"
        attr_host = MagicMock()
        attr_host.name = "host"
        MockConfig.attributes.return_value = [attr_timeout, attr_host]

        orig_ws = MagicMock()
        orig_ws.config = orig_config

        self._ws_with_timeout(orig_ws, timeout=900)

        call_kwargs = MockConfig.call_args.kwargs
        assert call_kwargs["http_timeout_seconds"] == 900


# ═══════════════════════════════════════════════════════════════════════════
# _get_openai_client
# ═══════════════════════════════════════════════════════════════════════════

class TestGetOpenaiClient:
    """Tests for get_openai_client (now in llm_client, re-exported by optimizer)."""

    def setup_method(self):
        from genie_space_optimizer.optimization import llm_client
        llm_client._openai_client_cache.clear()

    @patch("openai.OpenAI")
    def test_client_created_with_correct_base_url(self, MockOpenAI):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        ws = _make_mock_ws(
            host="https://my-workspace.cloud.databricks.com",
            token="dapi_test_token",
        )
        get_openai_client(ws)

        MockOpenAI.assert_called_once_with(
            api_key="dapi_test_token",
            base_url="https://my-workspace.cloud.databricks.com/serving-endpoints",
        )

    @patch("openai.OpenAI")
    def test_client_cached_by_host(self, MockOpenAI):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        ws = _make_mock_ws(token="dapi_tok1")
        client1 = get_openai_client(ws)
        client2 = get_openai_client(ws)

        assert client1 is client2
        assert MockOpenAI.call_count == 1

    @patch("openai.OpenAI")
    def test_token_refreshed_on_cached_client(self, MockOpenAI):
        """Simulates OAuth token rotation: second call updates api_key."""
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client

        ws1 = _make_mock_ws(token="token_v1")
        get_openai_client(ws1)
        assert mock_client.api_key != "token_v2"

        ws2 = _make_mock_ws(token="token_v2")
        get_openai_client(ws2)
        assert mock_client.api_key == "token_v2"

    @patch("openai.OpenAI")
    def test_oauth_token_used_when_config_token_is_none(self, MockOpenAI):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        ws = _make_mock_ws(
            token=None,
            auth_headers={"Authorization": "Bearer oauth_tok_abc"},
        )
        get_openai_client(ws)

        MockOpenAI.assert_called_once_with(
            api_key="oauth_tok_abc",
            base_url="https://my-workspace.cloud.databricks.com/serving-endpoints",
        )

    def test_no_auth_raises_runtime_error(self):
        from genie_space_optimizer.optimization.llm_client import get_openai_client

        ws = _make_mock_ws(token=None, auth_headers={})
        with pytest.raises(RuntimeError, match="Cannot resolve a bearer token"):
            get_openai_client(ws)
