from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pandas as pd
import pytest

import genie_space_optimizer.common.genie_client as _genie_client
from genie_space_optimizer.integration import trigger
from genie_space_optimizer.integration.config import IntegrationConfig


def _snapshot(table_count: int, *, title: str = "Revenue Space") -> dict:
    return {
        "title": title,
        "_parsed_space": {
            "version": 2,
            "data_sources": {
                "tables": [
                    {"identifier": f"cat.sch.table_{idx}", "column_configs": []}
                    for idx in range(table_count)
                ],
                "metric_views": [],
                "functions": [],
            },
            "config": {"sample_questions": []},
            "instructions": {"text_instructions": []},
        },
    }


def _client(label: str) -> MagicMock:
    client = MagicMock(name=label)
    client.config.host = "https://workspace.example"
    client.get_workspace_id.return_value = 123
    return client


def _patch_trigger_happy_path(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    create_run = MagicMock(name="wh_create_run")
    monkeypatch.setattr(trigger, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(trigger, "sql_warehouse_query", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(trigger, "wh_reconcile_active_runs", lambda *a, **k: False)
    monkeypatch.setattr(trigger, "wh_create_run", create_run)
    monkeypatch.setattr(trigger, "sql_warehouse_execute", lambda *a, **k: None)
    monkeypatch.setattr(_genie_client, "user_can_edit_space", lambda *a, **k: True)
    monkeypatch.setattr(_genie_client, "sp_can_manage_space", lambda *a, **k: True)
    monkeypatch.setattr(
        "genie_space_optimizer.common.sp_permissions.get_sp_principal_aliases",
        lambda _sp_ws: {"sp"},
    )
    monkeypatch.setattr(
        "genie_space_optimizer.backend.job_launcher.submit_optimization",
        lambda *a, **k: (987, 654),
    )
    return create_run


def _trigger(ws: MagicMock, sp_ws: MagicMock):
    return trigger.trigger_optimization(
        "space-1",
        ws,
        sp_ws,
        IntegrationConfig(
            catalog="cat",
            schema_name="sch",
            warehouse_id="wh",
            job_id=654,
        ),
        user_email="user@example.com",
    )


def test_trigger_capture_uses_sp_first_and_persists_sp_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _client("obo")
    sp_ws = _client("sp")
    sp_snapshot = _snapshot(9)
    obo_snapshot = _snapshot(0, title="Filtered OBO Export")
    calls: list[str] = []

    def fake_fetch(client: MagicMock, _space_id: str) -> dict:
        calls.append(client._mock_name)
        return sp_snapshot if client is sp_ws else obo_snapshot

    create_run = _patch_trigger_happy_path(monkeypatch)
    monkeypatch.setattr(_genie_client, "fetch_space_config", fake_fetch)

    result = _trigger(ws, sp_ws)

    assert result.status == "IN_PROGRESS"
    assert calls == ["sp"]
    assert create_run.call_args.kwargs["config_snapshot"] is sp_snapshot
    persisted_tables = create_run.call_args.kwargs["config_snapshot"][
        "_parsed_space"
    ]["data_sources"]["tables"]
    assert len(persisted_tables) == 9


def test_trigger_capture_rejects_empty_sp_then_uses_non_empty_obo_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _client("obo")
    sp_ws = _client("sp")
    sp_empty = _snapshot(0)
    obo_populated = _snapshot(9)
    calls: list[str] = []

    def fake_fetch(client: MagicMock, _space_id: str) -> dict:
        calls.append(client._mock_name)
        return sp_empty if client is sp_ws else obo_populated

    create_run = _patch_trigger_happy_path(monkeypatch)
    monkeypatch.setattr(_genie_client, "fetch_space_config", fake_fetch)

    result = _trigger(ws, sp_ws)

    assert result.status == "IN_PROGRESS"
    assert calls == ["sp", "obo"]
    persisted = create_run.call_args.kwargs["config_snapshot"]
    assert persisted is obo_populated
    assert len(persisted["_parsed_space"]["data_sources"]["tables"]) == 9


def test_trigger_capture_rejects_empty_snapshots_and_never_persists_them(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ws = _client("obo")
    sp_ws = _client("sp")
    empty_snapshot = _snapshot(0)
    calls: list[str] = []

    def fake_fetch(client: MagicMock, _space_id: str) -> dict:
        calls.append(client._mock_name)
        return empty_snapshot

    create_run = _patch_trigger_happy_path(monkeypatch)
    monkeypatch.setattr(_genie_client, "fetch_space_config", fake_fetch)

    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match="non-empty"):
        _trigger(ws, sp_ws)

    assert calls == ["sp", "obo"]
    create_run.assert_not_called()
    assert "Rejecting empty Genie space snapshot" in caplog.text
