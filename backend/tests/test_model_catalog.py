from types import SimpleNamespace
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models import LLMModelInfo
from backend.routers import analysis
from backend.services import model_catalog


def _endpoint(
    name: str,
    *,
    task: str | None = "chat.completion",
    ready: str = "READY",
    display_name: str | None = None,
    external_task: str | None = None,
    foundation: bool = True,
):
    entity = SimpleNamespace()
    if foundation:
        entity.foundation_model = SimpleNamespace(
            display_name=display_name,
            name=display_name or name,
        )
        entity.external_model = None
    else:
        entity.foundation_model = None
        entity.external_model = SimpleNamespace(
            name=display_name or name,
            task=external_task,
        )
    return SimpleNamespace(
        name=name,
        task=task,
        state=SimpleNamespace(ready=ready),
        config=SimpleNamespace(served_entities=[entity], served_models=[]),
    )


def test_list_chat_models_returns_curated_compatible_models(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "databricks-claude-sonnet-4-6")
    ws = MagicMock()
    ws.serving_endpoints.list.return_value = [
        _endpoint("chat-default", display_name="Default Chat"),
        _endpoint("chat-alt", task="CHAT_COMPLETION", display_name="Alt Chat"),
        _endpoint("embedding", task="embedding", display_name="Embedding"),
        _endpoint("not-ready", ready="NOT_READY", display_name="Not Ready"),
        _endpoint("text", task="text_completion", display_name="Text"),
    ]

    models = model_catalog.list_chat_models(client=ws)

    names = [m.name for m in models]
    assert models[0] == LLMModelInfo(
        name="databricks-claude-sonnet-4-6",
        displayName="Claude Sonnet 4.6",
        isDefault=True,
        optimizerPromptBudgetChars=60_000,
        contextTier="long",
    )
    assert "databricks-gpt-5-4" in names
    assert "databricks-gpt-5-4-mini" not in names
    assert "databricks-gpt-5-5" not in names
    assert "databricks-gpt-5-5-pro" not in names
    assert "databricks-claude-opus-5" in names
    assert "databricks-claude-sonnet-5" in names
    assert "databricks-claude-opus-4-8" in names
    assert "databricks-claude-opus-4-7" in names
    assert "databricks-claude-sonnet-4-5" not in names
    assert "databricks-claude-haiku-4-5" not in names
    assert "chat-default" not in names
    assert "chat-alt" not in names
    assert models[0].isDefault is True


def test_list_chat_models_does_not_require_endpoint_listing(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "databricks-claude-sonnet-4-6")
    ws = MagicMock()
    ws.serving_endpoints.list.side_effect = AssertionError("should not list")

    models = model_catalog.list_chat_models(client=ws)

    names = [m.name for m in models]
    assert names[0] == "databricks-claude-sonnet-4-6"
    assert "databricks-gpt-5-4" in names


def test_list_chat_models_uses_default_when_curated(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "databricks-claude-sonnet-4-6")
    ws = MagicMock()
    ws.serving_endpoints.list.return_value = []

    models = model_catalog.list_chat_models(client=ws)

    assert models[0] == LLMModelInfo(
        name="databricks-claude-sonnet-4-6",
        displayName="Claude Sonnet 4.6",
        isDefault=True,
        optimizerPromptBudgetChars=60_000,
        contextTier="long",
    )
    assert any(m.name == "databricks-gpt-5-4" for m in models)


def test_validate_chat_model_accepts_curated_models_without_listing():
    ws = MagicMock()
    ws.serving_endpoints.list.side_effect = AssertionError("should not list")

    for model_name in (
        "databricks-claude-opus-5",
        "databricks-claude-sonnet-5",
        "databricks-claude-opus-4-8",
    ):
        assert model_catalog.validate_chat_model(model_name, client=ws) == model_name


def test_validate_chat_model_rejects_non_curated_model():
    ws = MagicMock()
    ws.serving_endpoints.list.return_value = [
        _endpoint("databricks-claude-opus-4-1", task="chat.completion"),
    ]

    try:
        model_catalog.validate_chat_model("databricks-claude-opus-4-1", client=ws)
    except model_catalog.ModelValidationError as exc:
        assert "curated list" in str(exc)
    else:
        raise AssertionError("expected ModelValidationError")


def test_validate_chat_model_rejects_gpt_5_5_until_responses_api():
    try:
        model_catalog.validate_chat_model("databricks-gpt-5-5")
    except model_catalog.ModelValidationError as exc:
        assert "curated list" in str(exc)
    else:
        raise AssertionError("expected ModelValidationError")


def test_models_route_returns_bare_array(monkeypatch):
    app = FastAPI()
    app.include_router(analysis.router)
    monkeypatch.setattr(
        analysis,
        "list_chat_models",
        lambda allow_sp_fallback: [
            LLMModelInfo(name="chat", displayName="Chat", isDefault=True),
        ],
    )

    resp = TestClient(app).get("/api/models")

    assert resp.status_code == 200
    assert resp.json() == [
        {
            "name": "chat",
            "displayName": "Chat",
            "isDefault": True,
            "optimizerPromptBudgetChars": None,
            "contextTier": None,
        },
    ]


def test_models_include_optimizer_prompt_budget_metadata(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "databricks-claude-sonnet-4-6")
    monkeypatch.setenv("GSO_OPTIMIZER_PROMPT_MAX_CHARS", "75000")

    models = model_catalog.list_chat_models(client=MagicMock())

    assert models[0].optimizerPromptBudgetChars == 75_000
    assert models[0].contextTier == "long"
