"""Unit tests for the integration revert (champion / baseline rollback) path.

Covers the target-selection logic that the Workbench router tests mock out:

* ``target="champion"`` — PATCHes the champion iteration's ``config_json``
  (the row stamped ``is_champion = true``). No baseline fallback: if the
  champion config is unavailable (legacy table / no champion row / unparseable)
  it raises ``ValueError`` so the caller can surface a clear error and the user
  can fall back to the baseline button explicitly.
* ``target="baseline"`` — PATCHes the run's ``config_snapshot`` (the pre-run
  serialized space). Errors when no snapshot was captured.
* guard rails: refuses non-terminal runs, raises on run-not-found.
* a champion config that fails serialized_space validation is promoted to a
  ``RuntimeError`` (router → 422, not 409).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from genie_space_optimizer.integration import revert
from genie_space_optimizer.integration.config import IntegrationConfig


# ── Fixtures ────────────────────────────────────────────────────────────


def _config() -> IntegrationConfig:
    return IntegrationConfig(
        catalog="main",
        schema_name="gso_test",
        warehouse_id="wh-test",
        job_id=12345,
    )


def _ws() -> MagicMock:
    ws = MagicMock(name="ws")
    # patch_space_config probe (OBO) — keep OBO path so _pick_genie_client
    # returns the user client.
    ws.genie.list_spaces.return_value = MagicMock()
    return ws


def _champion_config() -> dict:
    return {
        "title": "Revenue Space — optimized",
        "serialized_space": {
            "version": 2,
            "data_sources": {"tables": []},
            "instructions": {
                "text_instructions": [{"content": "champion-only hint"}],
            },
        },
    }


def _baseline_config() -> dict:
    return {
        "title": "Revenue Space",
        "serialized_space": {
            "version": 2,
            "data_sources": {"tables": []},
            "instructions": {"text_instructions": []},
        },
    }


def _patch_patch_space_config(monkeypatch, *, live_config: dict | None = None) -> MagicMock:
    patch_mock = MagicMock(name="patch_space_config")
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        patch_mock,
    )
    live = live_config or {
        "_parsed_space": {
            "version": 2,
            "data_sources": {"tables": []},
        },
    }
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        lambda *a, **k: live,
    )
    return patch_mock


def _live_benchmark(qid: str, question: str, sql: str) -> dict:
    return {
        "id": qid,
        "question": [question],
        "answer": [{"format": "SQL", "content": [sql]}],
    }


# ── target="champion" ───────────────────────────────────────────────────


def test_revert_champion_uses_champion_iteration_config(monkeypatch) -> None:
    """Champion target PATCHes the is_champion row's config_json live."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r1", "space_id": "space-1", "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }
    champion_df = pd.DataFrame([{"config_json": json.dumps(_champion_config())}])

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(revert, "sql_warehouse_query", lambda *a, **k: champion_df)
    patch_mock = _patch_patch_space_config(monkeypatch)

    result = revert.revert_optimization("r1", ws, sp_ws, cfg, target="champion")

    assert result.status == "reverted"
    assert "champion" in result.message
    # The champion config (not the baseline) must be what's PATCHed live.
    patched_config = patch_mock.call_args.args[2]
    assert patched_config["version"] == 2
    assert "serialized_space" not in patched_config
    assert "title" not in patched_config
    assert patched_config["instructions"]["text_instructions"][0]["content"] == "champion-only hint"
    # OBO client preferred (the user ws).
    assert patch_mock.call_args.args[0] is ws
    assert patch_mock.call_args.args[1] == "space-1"


def test_revert_champion_backfills_version_for_legacy_projected_config(monkeypatch) -> None:
    """Legacy config_json rows projected a parsed serialized_space but dropped
    the required top-level version. Revert repairs that shape before PATCH."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r1-legacy", "space_id": "space-legacy", "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }
    legacy_champion = {
        "data_sources": {"tables": []},
        "instructions": {"text_instructions": [{"content": "legacy champion"}]},
    }
    champion_df = pd.DataFrame([{"config_json": json.dumps(legacy_champion)}])

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(revert, "sql_warehouse_query", lambda *a, **k: champion_df)
    patch_mock = _patch_patch_space_config(monkeypatch)

    revert.revert_optimization("r1-legacy", ws, sp_ws, cfg, target="champion")

    patched_config = patch_mock.call_args.args[2]
    assert patched_config["version"] == 2
    assert patched_config["instructions"]["text_instructions"][0]["content"] == "legacy champion"


def test_revert_preserves_live_benchmarks_instead_of_historical_champion(monkeypatch) -> None:
    """Reverting config must not roll benchmark ground truth backward."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r-bench",
        "space_id": "space-bench",
        "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }
    champion = {
        "version": 2,
        "data_sources": {"tables": []},
        "benchmarks": {
            "questions": [
                _live_benchmark(
                    "old",
                    "complaint rate",
                    "SELECT COUNT_IF(category = Complaint) FROM cat.sch.t",
                ),
            ],
        },
    }
    live = {
        "_parsed_space": {
            "version": 2,
            "data_sources": {"tables": []},
            "benchmarks": {
                "questions": [
                    _live_benchmark(
                        "live",
                        "complaint rate",
                        "SELECT COUNT_IF(category = 'Complaint') FROM cat.sch.t",
                    ),
                ],
            },
        },
    }

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(
        revert,
        "sql_warehouse_query",
        lambda *a, **k: pd.DataFrame([
            {"config_json": json.dumps(champion)},
        ]),
    )
    patch_mock = _patch_patch_space_config(monkeypatch, live_config=live)

    revert.revert_optimization("r-bench", ws, sp_ws, cfg, target="champion")

    patched_config = patch_mock.call_args.args[2]
    patched_questions = patched_config["benchmarks"]["questions"]
    assert len(patched_questions) == 1
    assert patched_questions[0]["id"] == "live"
    assert "'Complaint'" in patched_questions[0]["answer"][0]["content"][0]
    assert "category = Complaint" not in patched_questions[0]["answer"][0]["content"][0]


def test_revert_preserves_live_benchmarks_byte_for_byte(monkeypatch) -> None:
    """Revert skips benchmark edits by carrying live benchmarks forward."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r-prune",
        "space_id": "space-prune",
        "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }
    champion = {
        "version": 2,
        "data_sources": {"tables": []},
        "benchmarks": {"questions": []},
    }
    live = {
        "_parsed_space": {
            "version": 2,
            "data_sources": {"tables": []},
            "benchmarks": {
                "questions": [
                    _live_benchmark(
                        "bad",
                        "deposit volume",
                        "SELECT * FROM t WHERE transaction_type = Deposit",
                    ),
                    _live_benchmark(
                        "good",
                        "deposit volume quoted",
                        "SELECT * FROM t WHERE transaction_type = 'Deposit'",
                    ),
                ],
            },
        },
    }

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(
        revert,
        "sql_warehouse_query",
        lambda *a, **k: pd.DataFrame([
            {"config_json": json.dumps(champion)},
        ]),
    )
    patch_mock = _patch_patch_space_config(monkeypatch, live_config=live)

    revert.revert_optimization("r-prune", ws, sp_ws, cfg, target="champion")

    patched_questions = patch_mock.call_args.args[2]["benchmarks"]["questions"]
    assert patched_questions == live["_parsed_space"]["benchmarks"]["questions"]


def test_revert_champion_errors_when_no_champion_row(monkeypatch) -> None:
    """No is_champion row → ValueError (NOT a silent baseline fallback).

    With two explicit buttons, the champion button must not silently revert to
    baseline; the user gets a clear error and can click the baseline button.
    """
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r2", "space_id": "space-2", "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(revert, "sql_warehouse_query", lambda *a, **k: pd.DataFrame())
    patch_mock = _patch_patch_space_config(monkeypatch)

    with pytest.raises(ValueError, match="champion configuration is not available"):
        revert.revert_optimization("r2", ws, sp_ws, cfg, target="champion")
    patch_mock.assert_not_called()


def test_revert_champion_errors_on_legacy_schema(monkeypatch) -> None:
    """A pre-Phase-4 iterations table (UNRESOLVED_COLUMN) → ValueError, not a
    silent baseline fallback."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r3", "space_id": "space-3", "status": "APPLIED",
        "config_snapshot": json.dumps(_baseline_config()),
    }

    def _legacy_err(*a, **k):
        raise RuntimeError("UNRESOLVED_COLUMN: is_champion not found")

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(revert, "sql_warehouse_query", _legacy_err)
    patch_mock = _patch_patch_space_config(monkeypatch)

    with pytest.raises(ValueError, match="champion configuration is not available"):
        revert.revert_optimization("r3", ws, sp_ws, cfg, target="champion")
    patch_mock.assert_not_called()


# ── target="baseline" ───────────────────────────────────────────────────


def test_revert_baseline_uses_config_snapshot(monkeypatch) -> None:
    """Baseline target PATCHes the run's pre-run config_snapshot."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r4", "space_id": "space-4", "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    # sql_warehouse_query must NOT be called for the baseline target.
    query_mock = MagicMock(name="sql_warehouse_query")
    monkeypatch.setattr(revert, "sql_warehouse_query", query_mock)
    patch_mock = _patch_patch_space_config(monkeypatch)

    result = revert.revert_optimization("r4", ws, sp_ws, cfg, target="baseline")

    assert result.status == "reverted"
    assert "baseline" in result.message
    patched_config = patch_mock.call_args.args[2]
    assert patched_config["version"] == 2
    assert "serialized_space" not in patched_config
    assert "title" not in patched_config
    assert patched_config["instructions"]["text_instructions"] == []
    query_mock.assert_not_called()


def test_revert_baseline_errors_when_no_snapshot(monkeypatch) -> None:
    """No config_snapshot → ValueError (nothing to revert to)."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {"run_id": "r5", "space_id": "space-5", "status": "FAILED"}

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(revert, "sql_warehouse_query", lambda *a, **k: pd.DataFrame())
    patch_mock = _patch_patch_space_config(monkeypatch)

    with pytest.raises(ValueError, match="no baseline configuration snapshot"):
        revert.revert_optimization("r5", ws, sp_ws, cfg, target="baseline")
    patch_mock.assert_not_called()


# ── Guard rails ─────────────────────────────────────────────────────────


def test_revert_refuses_non_terminal_run(monkeypatch) -> None:
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {"run_id": "r6", "space_id": "space-6", "status": "IN_PROGRESS"}

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(revert, "sql_warehouse_query", lambda *a, **k: pd.DataFrame())
    patch_mock = _patch_patch_space_config(monkeypatch)

    with pytest.raises(ValueError, match="still in progress"):
        revert.revert_optimization("r6", ws, sp_ws, cfg, target="champion")
    patch_mock.assert_not_called()


def test_revert_raises_when_run_not_found(monkeypatch) -> None:
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: None)
    monkeypatch.setattr(revert, "sql_warehouse_query", lambda *a, **k: pd.DataFrame())
    patch_mock = _patch_patch_space_config(monkeypatch)

    with pytest.raises(ValueError, match="Run not found"):
        revert.revert_optimization("r7", ws, sp_ws, cfg, target="baseline")
    patch_mock.assert_not_called()


def test_revert_promotes_validation_failure_to_runtime_error(monkeypatch) -> None:
    """A config that fails serialized_space validation → RuntimeError
    (router → 422, not 409), for either target."""
    cfg = _config()
    ws, sp_ws = _ws(), MagicMock(name="sp_ws")
    run = {
        "run_id": "r8", "space_id": "space-8", "status": "CONVERGED",
        "config_snapshot": json.dumps(_baseline_config()),
    }

    def _patch_fail(*a, **k):
        raise ValueError("serialized_space missing data_sources")

    monkeypatch.setattr(revert, "wh_load_run", lambda *a, **k: run)
    monkeypatch.setattr(
        revert,
        "sql_warehouse_query",
        lambda *a, **k: pd.DataFrame([
            {"config_json": json.dumps(_champion_config())},
        ]),
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        _patch_fail,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        lambda *a, **k: {
            "_parsed_space": {
                "version": 2,
                "data_sources": {"tables": []},
            },
        },
    )

    with pytest.raises(RuntimeError, match="invalid"):
        revert.revert_optimization("r8", ws, sp_ws, cfg, target="champion")
