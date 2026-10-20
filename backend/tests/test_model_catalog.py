from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models import LLMModelInfo
from backend.routers import analysis
from backend.services import model_catalog

CHAT = "mlflow/v1/chat/completions"
ALL_CURATED = [name for name, _ in model_catalog._CURATED_COMPATIBLE_CHAT_MODELS]


@pytest.fixture(autouse=True)
def _reset_cache():
    model_catalog._reset_availability_cache()
    yield
    model_catalog._reset_availability_cache()


def _service(name: str, api_types: list[str] | None = None) -> dict:
    return {
        "name": f"model-services/{name}",
        "securable_type": "MODEL_SERVICE",
        "supported_api_types": api_types if api_types is not None else [CHAT],
    }


def _ws_listing(*pages: list[dict]) -> MagicMock:
    responses = []
    for i, services in enumerate(pages):
        page = {"model_services": services}
        if i < len(pages) - 1:
            page["next_page_token"] = f"tok{i + 1}"
        responses.append(page)
    ws = MagicMock()
    ws.api_client.do.side_effect = responses
    return ws


def test_list_chat_models_returns_curated_compatible_models(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "databricks-claude-sonnet-4-6")
    ws = _ws_listing([_service(n) for n in ALL_CURATED] + [
        _service("system.ai.gpt-5-5"),
        _service("system.ai.claude-haiku-4-5"),
        _service("system.ai.bge-large-en", ["mlflow/v1/embeddings"]),
    ])

    models = model_catalog.list_chat_models(client=ws)

    names = [m.name for m in models]
    assert models[0] == LLMModelInfo(
        name="system.ai.claude-sonnet-4-6",
        displayName="Claude Sonnet 4.6",
        isDefault=True,
        optimizerPromptBudgetChars=60_000,
        contextTier="long",
    )
    assert sorted(names) == sorted(ALL_CURATED)
    assert "system.ai.gpt-5-5" not in names
    assert "system.ai.claude-haiku-4-5" not in names
    assert "system.ai.bge-large-en" not in names


def test_list_chat_models_hides_models_missing_from_workspace(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = _ws_listing([_service("system.ai.claude-sonnet-4-6"), _service("system.ai.gpt-5-4")])

    names = [m.name for m in model_catalog.list_chat_models(client=ws)]

    assert names == ["system.ai.claude-sonnet-4-6", "system.ai.gpt-5-4"]


def test_list_chat_models_follows_pagination(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = _ws_listing(
        [_service("system.ai.claude-sonnet-4-6")],
        [_service("system.ai.claude-opus-5")],
    )

    names = {m.name for m in model_catalog.list_chat_models(client=ws)}

    assert names == {"system.ai.claude-sonnet-4-6", "system.ai.claude-opus-5"}
    second_call_path = ws.api_client.do.call_args_list[1].args[1]
    assert "page_token=tok1" in second_call_path
    assert "catalog_name=system" in second_call_path and "schema_name=ai" in second_call_path


def test_list_chat_models_requires_chat_api_type(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = _ws_listing([
        _service("system.ai.claude-sonnet-4-6"),
        _service("system.ai.gpt-5-4", ["openai/v1/responses"]),
    ])

    names = [m.name for m in model_catalog.list_chat_models(client=ws)]

    assert names == ["system.ai.claude-sonnet-4-6"]


def test_list_chat_models_keeps_default_even_if_not_listed(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = _ws_listing([_service("system.ai.gpt-5-4")])

    names = [m.name for m in model_catalog.list_chat_models(client=ws)]

    assert names == ["system.ai.claude-sonnet-4-6", "system.ai.gpt-5-4"]


def test_list_chat_models_fails_open_when_listing_errors(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = MagicMock()
    ws.api_client.do.side_effect = RuntimeError("PERMISSION_DENIED")

    names = [m.name for m in model_catalog.list_chat_models(client=ws)]

    assert sorted(names) == sorted(ALL_CURATED)


def test_list_chat_models_fails_open_when_no_curated_models_listed(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = _ws_listing([_service("system.ai.some-other-model")])

    names = [m.name for m in model_catalog.list_chat_models(client=ws)]

    assert sorted(names) == sorted(ALL_CURATED)


def test_list_chat_models_caches_listing(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    ws = MagicMock()
    ws.api_client.do.return_value = {"model_services": [_service("system.ai.claude-sonnet-4-6")]}

    model_catalog.list_chat_models(client=ws)
    model_catalog.list_chat_models(client=ws)

    assert ws.api_client.do.call_count == 1


def test_list_chat_models_failure_cache_expires(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "system.ai.claude-sonnet-4-6")
    now = [1000.0]
    monkeypatch.setattr(model_catalog.time, "monotonic", lambda: now[0])
    ws = MagicMock()
    ws.api_client.do.side_effect = [
        RuntimeError("boom"),
        {"model_services": [_service("system.ai.claude-sonnet-4-6")]},
    ]

    assert len(model_catalog.list_chat_models(client=ws)) == len(ALL_CURATED)
    now[0] += model_catalog._AVAILABILITY_FAILURE_TTL_SECONDS + 1
    names = [m.name for m in model_catalog.list_chat_models(client=ws)]

    assert names == ["system.ai.claude-sonnet-4-6"]


def test_validate_chat_model_accepts_curated_models_without_listing():
    ws = MagicMock()
    ws.api_client.do.side_effect = AssertionError("should not list")

    for model_name in (
        "system.ai.claude-opus-5",
        "system.ai.claude-sonnet-5",
        "system.ai.claude-opus-4-8",
    ):
        assert model_catalog.validate_chat_model(model_name, client=ws) == model_name


def test_validate_chat_model_normalizes_legacy_names():
    assert model_catalog.validate_chat_model("databricks-claude-opus-4-7") == "system.ai.claude-opus-4-7"
    assert model_catalog.validate_chat_model("  databricks-gpt-5-4 ") == "system.ai.gpt-5-4"
    assert model_catalog.validate_chat_model("  ") is None


def test_validate_chat_model_rejects_non_curated_model():
    try:
        model_catalog.validate_chat_model("databricks-claude-opus-4-1")
    except model_catalog.ModelValidationError as exc:
        assert "curated list" in str(exc)
        assert "databricks-claude-opus-4-1" in str(exc)
    else:
        raise AssertionError("expected ModelValidationError")


def test_validate_chat_model_rejects_gpt_5_5_until_responses_api():
    try:
        model_catalog.validate_chat_model("system.ai.gpt-5-5")
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
    ws = _ws_listing([_service("system.ai.claude-sonnet-4-6")])

    models = model_catalog.list_chat_models(client=ws)

    assert models[0].optimizerPromptBudgetChars == 75_000
    assert models[0].contextTier == "long"
