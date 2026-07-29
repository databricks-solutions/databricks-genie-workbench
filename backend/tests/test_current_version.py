"""Tests for GET /api/auto-optimize/spaces/{space_id}/current-version.

Style mirrors backend/tests/test_auto_optimize_router.py: FastAPI TestClient +
monkeypatching, no real Databricks connectivity. Delta reads are faked via
``auto_optimize._delta_query`` (the endpoint's async wrapper resolves it from
module globals at call time); the live Genie fetch is faked via
``auto_optimize.get_genie_space``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from genie_space_optimizer.common import warehouse as gso_warehouse

SPACE_ID = "space-abc123"


def _space(*, instruction: str = "Be helpful") -> dict:
    return {
        "version": 2,
        "data_sources": {"tables": [{"identifier": "cat.sch.t1"}]},
        "config": {"sample_questions": [{"id": "q1", "question": ["What is revenue?"]}]},
        "instructions": {"text_instructions": [{"id": "a1", "content": [instruction]}]},
    }


def _snapshot_wrapper(space: dict) -> str:
    """config_snapshot as stored in Delta: full GET response, serialized_space
    embedded as a JSON string."""
    return json.dumps({"serialized_space": json.dumps(space), "title": "My Agent"})


def _run_row(
    run_id: str,
    *,
    started_at: str,
    status: str = "CONVERGED",
    best_iteration: int | None = 0,
    best_accuracy: str | None = "80.0",
    config_snapshot: str | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "started_at": started_at,
        "status": status,
        "best_iteration": best_iteration,
        "best_accuracy": best_accuracy,
        "config_snapshot": config_snapshot,
    }


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    auto_optimize._live_fp_cache.clear()
    # Deterministic principal for the cache key; no real WorkspaceClient.
    _set_principal(monkeypatch, "test-token")

    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def _set_principal(monkeypatch, token: str | None) -> None:
    """Point the cache-key principal at a fake token-bearing client."""
    client = SimpleNamespace(config=SimpleNamespace(token=token))
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: client)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: client)


@pytest.fixture(autouse=True)
def _clear_cache_after_test():
    yield
    auto_optimize._live_fp_cache.clear()


def _stub_delta(monkeypatch, *, runs: list[dict], champions: list[dict] | None = None) -> None:
    """Fake _delta_query, dispatching on the SQL's target table."""

    def fake(sql: str, *, strict: bool = False) -> list[dict]:
        if "genie_opt_iterations" in sql:
            return list(champions or [])
        if "genie_opt_runs" in sql:
            return list(runs)
        return []

    monkeypatch.setattr(auto_optimize, "_delta_query", fake)


def _stub_reconcile(monkeypatch, *, changed: bool) -> None:
    """Fake the Jobs-API zombie reconciliation used when active-looking rows
    exist. ``changed=True`` simulates zombies being stamped terminal."""
    monkeypatch.setattr(
        gso_warehouse, "wh_reconcile_active_runs",
        lambda *a, **k: changed,
    )


def _stub_live(monkeypatch, space: dict | None, *, update_time: str | None = None) -> None:
    if space is None:
        def fail(space_id: str) -> dict:
            raise RuntimeError("genie API down")
        monkeypatch.setattr(auto_optimize, "get_genie_space", fail)
    else:
        payload = {"serialized_space": json.dumps(space)}
        if update_time:
            payload["update_time"] = update_time
        monkeypatch.setattr(auto_optimize, "get_genie_space", lambda space_id: payload)


# ── Status branches ──────────────────────────────────────────────────────


def test_unconfigured_returns_no_known_versions(client, monkeypatch) -> None:
    monkeypatch.setattr(auto_optimize, "_is_configured", lambda: False)
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    assert resp.status_code == 200
    assert resp.json()["status"] == "no_known_versions"


def test_no_runs_returns_no_known_versions(client, monkeypatch) -> None:
    _stub_delta(monkeypatch, runs=[])
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    assert resp.json()["status"] == "no_known_versions"


def test_active_run_short_circuits(client, monkeypatch) -> None:
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00", status="IN_PROGRESS")],
    )
    # Reconciliation confirms the run is genuinely active (no rows changed).
    _stub_reconcile(monkeypatch, changed=False)
    # Live fetch must not even be attempted while a run is active.
    monkeypatch.setattr(
        auto_optimize,
        "get_genie_space",
        lambda space_id: pytest.fail("live fetch during active run"),
    )
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    assert resp.json()["status"] == "optimization_in_progress"


def test_zombie_run_reconciled_then_matching_proceeds(client, monkeypatch) -> None:
    """A stale IN_PROGRESS row whose job actually died must not suppress
    matching: reconciliation stamps it terminal and the check proceeds."""
    space = _space()
    state = {
        "runs": [
            _run_row("r1", started_at="2026-07-01 10:00:00", status="IN_PROGRESS",
                     config_snapshot=_snapshot_wrapper(space)),
        ],
    }

    def fake_delta(sql: str, *, strict: bool = False) -> list[dict]:
        if "genie_opt_iterations" in sql:
            return []
        if "SELECT run_id, status FROM" in sql:
            # Post-reconcile status re-read (small payload, merged client-side)
            return [{"run_id": "r1", "status": "FAILED"}]
        if "genie_opt_runs" in sql:
            return list(state["runs"])
        return []

    def fake_reconcile(*a, **k) -> bool:
        # Reconciliation stamps the zombie terminal; the endpoint's status
        # re-read then observes the flip.
        state["runs"] = [
            _run_row("r1", started_at="2026-07-01 10:00:00", status="FAILED",
                     config_snapshot=_snapshot_wrapper(space)),
        ]
        return True

    monkeypatch.setattr(auto_optimize, "_delta_query", fake_delta)
    monkeypatch.setattr(gso_warehouse, "wh_reconcile_active_runs", fake_reconcile)
    _stub_live(monkeypatch, space)

    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    data = resp.json()
    assert data["status"] == "matched"
    assert data["current"]["target"] == "baseline"


def test_runs_without_matchable_configs_returns_history_incomplete(client, monkeypatch) -> None:
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00", config_snapshot=None)],
    )
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    assert resp.json()["status"] == "history_incomplete"


def test_matched_baseline(client, monkeypatch) -> None:
    space = _space()
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00",
                       config_snapshot=_snapshot_wrapper(space))],
    )
    _stub_live(monkeypatch, space, update_time="2026-07-28T15:00:00Z")
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    data = resp.json()
    assert data["status"] == "matched"
    assert data["current"]["run_id"] == "r1"
    assert data["current"]["target"] == "baseline"
    assert data["current"]["best_accuracy"] == 80.0
    assert data["also_matches"] == []
    assert data["live_update_time"] == "2026-07-28T15:00:00Z"


def test_matched_champion(client, monkeypatch) -> None:
    live = _space(instruction="Optimized instructions")
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00",
                       config_snapshot=_snapshot_wrapper(_space()))],
        champions=[{
            "run_id": "r1",
            "config_json": json.dumps(_space(instruction="Submitted representation")),
            "observed_config_json": json.dumps(live),
        }],
    )
    _stub_live(monkeypatch, live)
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    data = resp.json()
    assert data["status"] == "matched"
    assert data["current"]["run_id"] == "r1"
    assert data["current"]["target"] == "champion"


def test_drifted_when_live_matches_nothing(client, monkeypatch) -> None:
    known = _space()
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00",
                       config_snapshot=_snapshot_wrapper(known))],
        champions=[{
            "run_id": "r1",
            "config_json": json.dumps(known),
            "observed_config_json": json.dumps(known),
        }],
    )
    _stub_live(monkeypatch, _space(instruction="Edited in the Genie UI"),
               update_time="2026-07-28T16:00:00Z")
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    data = resp.json()
    assert data["status"] == "drifted"
    assert data["current"] is None
    assert data["live_update_time"] == "2026-07-28T16:00:00Z"


def test_missing_observed_champion_can_match_semantically(client, monkeypatch) -> None:
    """Legacy submitted configs remain usable across fragment normalization."""
    submitted = _space(instruction="PURPOSE:\n- Answer banking questions")
    live = _space()
    live["instructions"]["text_instructions"][0]["content"] = [
        "PURPOSE:\n",
        "- Answer banking questions",
    ]
    _stub_delta(
        monkeypatch,
        runs=[_run_row(
            "r1",
            started_at="2026-07-01 10:00:00",
            config_snapshot=_snapshot_wrapper(_space(instruction="Baseline")),
        )],
        champions=[{
            "run_id": "r1",
            "config_json": json.dumps(submitted),
            "observed_config_json": None,
        }],
    )
    _stub_live(monkeypatch, live)

    data = client.get(
        f"/api/auto-optimize/spaces/{SPACE_ID}/current-version"
    ).json()
    assert data["status"] == "matched"
    assert data["current"]["target"] == "champion"


def test_legacy_schema_can_still_produce_positive_champion_match(client, monkeypatch) -> None:
    champion = _space(instruction="Legacy champion")
    runs = [_run_row(
        "r1",
        started_at="2026-07-01 10:00:00",
        config_snapshot=_snapshot_wrapper(_space()),
    )]

    def fake(sql: str, *, strict: bool = False) -> list[dict]:
        if "genie_opt_iterations" in sql and "observed_config_json" in sql:
            raise RuntimeError("UNRESOLVED_COLUMN: observed_config_json")
        if "genie_opt_iterations" in sql:
            return [{"run_id": "r1", "config_json": json.dumps(champion)}]
        if "genie_opt_runs" in sql:
            return runs
        return []

    monkeypatch.setattr(auto_optimize, "_delta_query", fake)
    _stub_live(monkeypatch, champion)
    data = client.get(
        f"/api/auto-optimize/spaces/{SPACE_ID}/current-version"
    ).json()
    assert data["status"] == "matched"
    assert data["current"]["target"] == "champion"


def test_champion_query_failure_is_unavailable_not_drifted(client, monkeypatch) -> None:
    runs = [_run_row(
        "r1",
        started_at="2026-07-01 10:00:00",
        config_snapshot=_snapshot_wrapper(_space()),
    )]

    def fake(sql: str, *, strict: bool = False) -> list[dict]:
        if "genie_opt_iterations" in sql:
            raise RuntimeError("warehouse unavailable")
        if "genie_opt_runs" in sql:
            return runs
        return []

    monkeypatch.setattr(auto_optimize, "_delta_query", fake)
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    assert resp.json()["status"] == "unavailable"


def test_unavailable_when_live_fetch_fails(client, monkeypatch) -> None:
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00",
                       config_snapshot=_snapshot_wrapper(_space()))],
    )
    _stub_live(monkeypatch, None)
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    assert resp.json()["status"] == "unavailable"


def test_multi_match_picks_most_recent_and_lists_equivalents(client, monkeypatch) -> None:
    """Run 2's baseline is byte-identical to run 1's champion (nothing changed
    between runs) — badge the most recent, keep the other as an equivalent."""
    space = _space()
    _stub_delta(
        monkeypatch,
        runs=[
            _run_row("r2", started_at="2026-07-10 10:00:00",
                     config_snapshot=_snapshot_wrapper(space)),
            _run_row("r1", started_at="2026-07-01 10:00:00",
                     config_snapshot=_snapshot_wrapper(_space())),
        ],
        champions=[{
            "run_id": "r1",
            "config_json": json.dumps(space),
            "observed_config_json": json.dumps(space),
        }],
    )
    _stub_live(monkeypatch, space)
    resp = client.get(f"/api/auto-optimize/spaces/{SPACE_ID}/current-version")
    data = resp.json()
    assert data["status"] == "matched"
    assert data["current"] == {
        "run_id": "r2",
        "target": "baseline",
        "started_at": "2026-07-10 10:00:00",
        "best_accuracy": 80.0,
    }
    equivalents = {(m["run_id"], m["target"]) for m in data["also_matches"]}
    assert equivalents == {("r1", "baseline"), ("r1", "champion")}


# ── Live fingerprint cache ───────────────────────────────────────────────


def test_live_fingerprint_cached_across_calls(client, monkeypatch) -> None:
    space = _space()
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00",
                       config_snapshot=_snapshot_wrapper(space))],
    )
    calls = {"n": 0}

    def counting_fetch(space_id: str) -> dict:
        calls["n"] += 1
        return {"serialized_space": json.dumps(space)}

    monkeypatch.setattr(auto_optimize, "get_genie_space", counting_fetch)
    url = f"/api/auto-optimize/spaces/{SPACE_ID}/current-version"
    assert client.get(url).json()["status"] == "matched"
    assert client.get(url).json()["status"] == "matched"
    assert calls["n"] == 1


def test_live_fingerprint_cache_scoped_by_principal(client, monkeypatch) -> None:
    """A second user's request must NOT reuse the first user's cached
    fingerprint — each principal's first call performs its own (OBO-
    authorized) live fetch."""
    space = _space()
    _stub_delta(
        monkeypatch,
        runs=[_run_row("r1", started_at="2026-07-01 10:00:00",
                       config_snapshot=_snapshot_wrapper(space))],
    )
    calls = {"n": 0}

    def counting_fetch(space_id: str) -> dict:
        calls["n"] += 1
        return {"serialized_space": json.dumps(space)}

    monkeypatch.setattr(auto_optimize, "get_genie_space", counting_fetch)
    url = f"/api/auto-optimize/spaces/{SPACE_ID}/current-version"

    _set_principal(monkeypatch, "user-a-token")
    assert client.get(url).json()["status"] == "matched"
    assert client.get(url).json()["status"] == "matched"
    assert calls["n"] == 1

    _set_principal(monkeypatch, "user-b-token")
    assert client.get(url).json()["status"] == "matched"
    assert client.get(url).json()["status"] == "matched"
    assert calls["n"] == 2


def test_invalidate_live_fingerprint_for_run_clears_cache(client, monkeypatch) -> None:
    auto_optimize._live_fp_cache[f"{SPACE_ID}:p1"] = (float("inf"), "stale-fp", None)
    auto_optimize._live_fp_cache[f"{SPACE_ID}:p2"] = (float("inf"), "stale-fp", None)
    auto_optimize._live_fp_cache["other-space:p1"] = (float("inf"), "keep-fp", None)

    def fake(sql: str, *, strict: bool = False) -> list[dict]:
        if "SELECT space_id FROM" in sql:
            return [{"space_id": SPACE_ID}]
        return []

    monkeypatch.setattr(auto_optimize, "_delta_query", fake)
    auto_optimize._invalidate_live_fingerprint_for_run(
        "11111111-2222-3333-4444-555555555555"
    )
    assert f"{SPACE_ID}:p1" not in auto_optimize._live_fp_cache
    assert f"{SPACE_ID}:p2" not in auto_optimize._live_fp_cache
    assert auto_optimize._live_fp_cache["other-space:p1"] == (float("inf"), "keep-fp", None)
