"""Unity Gateway deploy preflight (scripts/deploy_lib/ai_gateway.py) and scope parity."""

import ast
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts.deploy_lib import ai_gateway as preflight
from scripts.deploy_lib import install as install_mod
from scripts.deploy_lib.apps import APP_SCOPES
from scripts.deploy_lib.config import InstallConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
NOT_FOUND = '{"error_code":"NOT_FOUND","message":"\'system.ai.nope\' does not exist."}'


def _ws():
    return SimpleNamespace(
        config=SimpleNamespace(
            host="https://example.cloud.databricks.com",
            authenticate=lambda: {"Authorization": "Bearer t"},
        ),
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name="me@example.com", display_name="Me")),
    )


def _stub_post(monkeypatch, chat, embed=(200, "{}")):
    """Route requests.post by URL suffix; ``chat`` may be a list for retries."""
    chat_responses = list(chat) if isinstance(chat, list) else [chat]
    calls = []

    def fake_post(url, *, json, headers, timeout):
        calls.append((url, json))
        if url.endswith("/embeddings"):
            status, text = embed
        else:
            status, text = chat_responses.pop(0) if len(chat_responses) > 1 else chat_responses[0]
        return SimpleNamespace(status_code=status, text=text)

    monkeypatch.setattr(preflight.requests, "post", fake_post)
    monkeypatch.setattr(preflight.time, "sleep", lambda s: None)
    return calls


def test_preflight_passes_and_uses_v3_routes(monkeypatch):
    calls = _stub_post(monkeypatch, (200, "{}"))

    report = preflight.run_ai_gateway_preflight(_ws(), "databricks-claude-sonnet-4-6")

    assert report.ok and not report.warnings
    chat_url, chat_body = calls[0]
    assert chat_url == "https://example.cloud.databricks.com/ai-gateway/mlflow/v1/chat/completions"
    assert chat_body["model"] == "system.ai.claude-sonnet-4-6"
    assert chat_body["max_tokens"] == 16
    embed_url, embed_body = calls[1]
    assert embed_url.endswith("/ai-gateway/mlflow/v1/embeddings")
    assert embed_body == {"model": "system.ai.bge-large-en", "input": ["ping"]}
    assert report.as_dict()["chat_status"] == 200


@pytest.mark.parametrize(
    ("chat", "fragment"),
    [
        ((404, NOT_FOUND), "does not exist in this workspace's system.ai catalog"),
        ((404, "<html>Not Found</html>"), "may not be enabled for this workspace"),
        ((403, '{"error_code":"PERMISSION_DENIED","message":"no EXECUTE"}'), "databricks grants update model_service system.ai.nope"),
        ((403, '{"error_code":"PERMISSION_DENIED","message":"required scopes: ai-gateway"}'), "lacks the 'ai-gateway' scope"),
        ((401, '{"message":"bad token"}'), "Authentication to Unity Gateway failed"),
    ],
)
def test_preflight_hard_failures(monkeypatch, chat, fragment):
    _stub_post(monkeypatch, chat)

    report = preflight.run_ai_gateway_preflight(_ws(), "system.ai.nope")

    assert not report.ok
    assert fragment in report.errors[0]


def test_preflight_permission_denied_names_deployer(monkeypatch):
    _stub_post(monkeypatch, (403, '{"error_code":"PERMISSION_DENIED","message":"no EXECUTE"}'))

    report = preflight.run_ai_gateway_preflight(_ws(), "system.ai.nope")

    assert "`me@example.com`" in report.errors[0]


def test_preflight_bad_request_is_a_warning(monkeypatch):
    _stub_post(monkeypatch, (400, '{"error_code":"BAD_REQUEST","message":"max_tokens"}'))

    report = preflight.run_ai_gateway_preflight(_ws(), "system.ai.gpt-5-4")

    assert report.ok
    assert "inconclusive" in report.warnings[0]


def test_preflight_retries_transient_then_passes(monkeypatch):
    calls = _stub_post(monkeypatch, [(429, ""), (200, "{}")])

    report = preflight.run_ai_gateway_preflight(_ws(), "system.ai.claude-sonnet-4-6")

    assert report.ok and not report.warnings
    assert sum(1 for url, _ in calls if url.endswith("/chat/completions")) == 2


def test_preflight_embedding_failure_only_warns(monkeypatch):
    _stub_post(monkeypatch, (200, "{}"), embed=(404, NOT_FOUND))

    report = preflight.run_ai_gateway_preflight(_ws(), "system.ai.claude-sonnet-4-6")

    assert report.ok
    assert "n-gram" in report.warnings[0]


def test_run_install_stops_before_creating_app_when_preflight_fails(monkeypatch):
    failing = preflight.AiGatewayPreflight(
        chat=preflight.ProbeResult(model="system.ai.nope", status=404),
        embedding=preflight.ProbeResult(model="system.ai.bge-large-en", status=200),
        errors=["Model service system.ai.nope does not exist"],
    )
    monkeypatch.setattr(install_mod, "run_ai_gateway_preflight", lambda w, model: failing)
    monkeypatch.setattr(install_mod, "get_deployer_user", lambda w: pytest.fail("ran past preflight"))
    monkeypatch.setattr(install_mod, "ensure_app", lambda w, cfg: pytest.fail("ran past preflight"))
    cfg = InstallConfig(
        app_name="genie-workbench", catalog="main", warehouse_id="wh", llm_model="system.ai.nope", repo_root="/tmp",
    )

    with pytest.raises(RuntimeError, match="Unity Gateway preflight failed"):
        install_mod.run_install(object(), cfg, status_fn=lambda msg: None)


def test_install_config_normalizes_legacy_llm_model():
    cfg = InstallConfig(
        app_name="genie-workbench", catalog="main", warehouse_id="wh", llm_model=" databricks-gpt-5-4 ",
    ).normalized()
    assert cfg.llm_model == "system.ai.gpt-5-4"
    assert InstallConfig(app_name="a", catalog="c", warehouse_id="w").normalized().llm_model == "system.ai.claude-sonnet-4-6"


def test_oauth_scopes_match_across_install_paths():
    app_yaml_scopes = yaml.safe_load((REPO_ROOT / "app.yaml").read_text())["user_api_scopes"]
    deploy_sh = (REPO_ROOT / "scripts" / "deploy.sh").read_text()
    match = re.search(r"scopes = (\[[^\]]*\])", deploy_sh)
    assert match, "scopes list not found in deploy.sh"
    deploy_sh_scopes = ast.literal_eval(match.group(1))

    assert set(app_yaml_scopes) == set(APP_SCOPES) == set(deploy_sh_scopes)
    assert "ai-gateway" in APP_SCOPES
