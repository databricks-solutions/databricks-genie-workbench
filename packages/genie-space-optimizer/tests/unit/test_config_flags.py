"""Active model-endpoint configuration tests."""
from __future__ import annotations


def test_llm_endpoint_default_is_sonnet(monkeypatch):
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.delenv("GSO_LLM_ENDPOINT", raising=False)
    from genie_space_optimizer.common.config import get_llm_endpoint

    assert get_llm_endpoint() == "databricks-claude-sonnet-4-6"


def test_llm_endpoint_uses_app_model(monkeypatch):
    monkeypatch.delenv("GSO_LLM_ENDPOINT", raising=False)
    monkeypatch.setenv("LLM_MODEL", "custom-model")
    from genie_space_optimizer.common.config import get_llm_endpoint

    assert get_llm_endpoint() == "custom-model"


def test_llm_endpoint_gso_override_wins(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "app-model")
    monkeypatch.setenv("GSO_LLM_ENDPOINT", "gso-override")
    from genie_space_optimizer.common.config import get_llm_endpoint

    assert get_llm_endpoint() == "gso-override"
