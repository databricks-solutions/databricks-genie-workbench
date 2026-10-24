"""MV-D109: every mounted route is either gated at its level or exempt with a reason.

The deny harness replaces every service a handler could touch with a tripwire, so a
clean 403 proves the gate asked Genie first — for the right space, at the right level.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.services import space_access
from backend.services.space_access import SpaceAccessLevel as L
from genie_space_optimizer.common.genie_client import SpaceAccessCheck

pytestmark = pytest.mark.real_space_access

SPACE = "gatespace0000000000000000000001"
RUN = "11111111-2222-4333-8444-555555555555"
SUGGESTION = "sugg-gate-1"


@dataclass(frozen=True)
class Gated:
    level: L
    body: dict | None = None
    query: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Exempt:
    reason: str


AO = "/api/auto-optimize"

TABLE: dict[tuple[str, str], Gated | Exempt] = {
    # ── /api/auto-optimize ──
    ("GET", f"{AO}/health"): Exempt("names no space"),
    ("GET", f"{AO}/levers"): Exempt("static lever metadata"),
    ("GET", f"{AO}/permissions/{{space_id}}"): Gated(L.EDIT),
    ("POST", f"{AO}/mv/probe"): Gated(
        L.EDIT,
        body={"catalog": "c", "schema": "s", "space_id": SPACE, "source_tables": []},
    ),
    ("POST", f"{AO}/trigger"): Gated(L.EDIT, body={"space_id": SPACE}),
    ("GET", f"{AO}/runs/{{run_id}}/mv-proposals"): Gated(L.EDIT),
    ("GET", f"{AO}/spaces/{{space_id}}/mv-proposals"): Gated(L.EDIT),
    ("POST", f"{AO}/spaces/{{space_id}}/mv/suggest"): Gated(L.EDIT),
    ("POST", f"{AO}/spaces/{{space_id}}/mv/suggest/stream"): Gated(L.EDIT),
    ("POST", f"{AO}/spaces/{{space_id}}/mv/register"): Gated(
        L.EDIT, body={"full_name": "c.s.v"}),
    ("POST", f"{AO}/spaces/{{space_id}}/mv/create"): Gated(
        L.EDIT, body={"suggestion_id": SUGGESTION, "probe_id": "probe-1"}),
    ("GET", f"{AO}/spaces/{{space_id}}/semantic-graph"): Gated(L.EDIT),
    ("GET", f"{AO}/spaces/{{space_id}}/join-candidates"): Gated(L.EDIT),
    ("GET", f"{AO}/spaces/{{space_id}}/join-advice"): Gated(L.EDIT),
    ("POST", f"{AO}/spaces/{{space_id}}/join-advice"): Gated(
        L.EDIT, body={"seeds": []}),
    ("GET", f"{AO}/runs/{{run_id}}/mv-ddl"): Gated(L.EDIT),
    ("POST", f"{AO}/mv/proposals/{{suggestion_id}}/decision"): Gated(
        L.EDIT, body={"space_id": SPACE, "decision": "approved"}),
    ("POST", f"{AO}/mv/created/{{suggestion_id}}/drop"): Gated(
        L.EDIT, body={"run_id": RUN, "confirm": True}),
    ("GET", f"{AO}/runs/{{run_id}}/mv-created"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/status"): Gated(L.VIEW),
    ("POST", f"{AO}/runs/{{run_id}}/apply"): Gated(L.EDIT),
    ("POST", f"{AO}/runs/{{run_id}}/discard"): Gated(L.EDIT),
    ("POST", f"{AO}/runs/{{run_id}}/revert"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/revert-options"): Gated(L.EDIT),
    ("GET", f"{AO}/spaces/{{space_id}}/current-version"): Gated(L.EDIT),
    ("GET", f"{AO}/spaces/{{space_id}}/active-run"): Gated(L.VIEW),
    ("GET", f"{AO}/spaces/{{space_id}}/runs"): Gated(L.VIEW),
    ("DELETE", f"{AO}/runs/{{run_id}}/history-entry"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/iterations"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/loop-state"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/publish"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/debug-data"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/eval-results"): Gated(
        L.EDIT, query={"iteration": 1}),
    ("GET", f"{AO}/runs/{{run_id}}/question-results"): Gated(
        L.EDIT, query={"iteration": 1}),
    ("GET", f"{AO}/runs/{{run_id}}/patches"): Gated(L.EDIT),
    ("GET", f"{AO}/runs/{{run_id}}/benchmark-changes"): Gated(L.EDIT),
    # ── /api (spaces, analysis, create) ──
    ("GET", "/api/spaces"): Exempt("Genie filters the list to the caller (M1c-D3)"),
    ("GET", "/api/spaces/{space_id}"): Gated(L.VIEW),
    ("POST", "/api/spaces/{space_id}/scan"): Gated(L.EDIT),
    ("GET", "/api/spaces/{space_id}/history"): Gated(L.VIEW),
    ("GET", "/api/spaces/{space_id}/access"): Exempt("answers the level (M1b)"),
    ("PUT", "/api/spaces/{space_id}/star"): Gated(L.VIEW, body={"starred": True}),
    ("POST", "/api/space/fetch"): Gated(L.EDIT, body={"genie_space_id": SPACE}),
    ("POST", "/api/space/parse"): Exempt("client-pasted JSON; no Databricks read"),
    ("POST", "/api/create/agent/chat"): Gated(
        L.EDIT, body={"message": "hi", "space_id": SPACE}),
    # ── remaining /api mounts (M1c-D1 exempt) ──
    ("GET", "/api/settings"): Exempt("app settings; names no space"),
    ("GET", "/api/models"): Exempt("serving-endpoint catalog; names no space"),
    ("GET", "/api/debug/auth"): Exempt("auth debug; names no space"),
    ("GET", "/api/auth/me"): Exempt("caller identity (out of M1)"),
    ("GET", "/api/auth/status"): Exempt("auth status (out of M1)"),
    ("GET", "/api/admin/dashboard"): Exempt("admin aggregate over the caller's own listing (MV-D119)"),
    ("GET", "/api/admin/leaderboard"): Exempt("admin aggregate over the caller's own listing (MV-D119)"),
    ("GET", "/api/admin/alerts"): Exempt("admin aggregate over the caller's own listing (MV-D119)"),
    ("GET", "/api/create/preflight"): Exempt("create discovery; no existing space"),
    ("GET", "/api/create/discover/catalogs"): Exempt("create discovery; no existing space"),
    ("GET", "/api/create/discover/schemas"): Exempt("create discovery; no existing space"),
    ("GET", "/api/create/discover/tables"): Exempt("create discovery; no existing space"),
    ("GET", "/api/create/discover/columns"): Exempt("create discovery; no existing space"),
    ("GET", "/api/create/discover/search"): Exempt("create discovery; no existing space"),
    ("POST", "/api/create/validate"): Exempt("create validation; no existing space"),
    ("POST", "/api/create"): Exempt("creates a new space; no existing space"),
    ("GET", "/api/create/agent/sessions/{session_id}"): Exempt(
        "session keyed by unguessable id; no space path"),
    ("DELETE", "/api/create/agent/sessions/{session_id}"): Exempt(
        "session keyed by unguessable id; no space path"),
    ("GET", "/api/watch/overview"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/cost"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/cost/conversations"): Exempt(
        "GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/cost/top-queries"): Exempt(
        "GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/feedback"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/resources"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/traffic-gaps"): Exempt(
        "GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/spaces/{space_id}/usage"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/cost/top"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/feedback"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/feedback/comments"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/resources/graph"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/resources/rollup"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/resources/spaces"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("GET", "/api/watch/settings/health"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("POST", "/api/watch/admin/refresh-rollup"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("POST", "/api/watch/settings/cache/refresh"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
    ("POST", "/api/watch/spaces/refresh"): Exempt("GenieWatch; out of M1 (M1c-D1)"),
}

_PARAMS = {"space_id": SPACE, "run_id": RUN, "suggestion_id": SUGGESTION}


def _mounted_routes() -> set[tuple[str, str]]:
    from backend.main import app

    routes: set[tuple[str, str]] = set()
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.path.startswith("/api/"):
            continue  # static SPA routes exist only when frontend/dist is built
        for method in route.methods - {"HEAD", "OPTIONS"}:
            routes.add((method, route.path))
    return routes


def test_every_mounted_api_route_is_classified():
    mounted = _mounted_routes()
    assert not (mounted - TABLE.keys()), "unclassified routes: gate them or list them as Exempt"
    assert not (TABLE.keys() - mounted), "the table names routes that no longer exist"
    assert all(entry.reason.strip() for entry in TABLE.values() if isinstance(entry, Exempt))


class _Tripwire:
    """Records every touch, then raises; a handler's ``except Exception`` can swallow
    the raise but not the record."""

    def __init__(self, name: str, hits: list[str]):
        self._name = name
        self._hits = hits

    def _trip(self, what: str):
        self._hits.append(what)
        raise AssertionError(f"{what} ran before the access gate")

    def __call__(self, *args, **kwargs):
        self._trip(self._name)

    def __getattr__(self, attr):
        self._trip(f"{self._name}.{attr}")


_TRIPWIRES = (
    ("backend.routers.auto_optimize", (
        "_is_configured", "_build_gso_config", "get_service_principal_client",
        "get_workspace_client", "require_obo_workspace_client", "gso_lakebase",
        "workbench_lakebase", "_delta_query", "_delta_query_async", "_offload",
        "get_genie_space", "get_serialized_space", "load_runs_with_fallback",
        "mv_create", "mv_entitlement", "trigger_optimization", "apply_optimization",
        "discard_optimization", "revert_optimization", "preview_revert_options",
    )),
    ("backend.routers.spaces", (
        "scan_space", "star_space", "get_latest_score", "is_space_starred",
        "get_score_history", "load_runs_with_fallback", "get_workspace_client",
        "require_obo_workspace_client", "list_genie_spaces", "get_service_principal_client",
    )),
    ("backend.routers.analysis", ("get_serialized_space",)),
    ("backend.services.create_agent", ("get_create_agent",)),
    ("backend.services.create_agent_session", ("create_session", "get_session_async")),
)


@pytest.fixture
def denying_app(monkeypatch):
    import importlib

    from backend.main import app
    from backend.routers import auto_optimize

    asked: list[tuple[str, L]] = []
    hits: list[str] = []

    def deny(client, space_id, level):
        asked.append((space_id, L(level)))
        return SpaceAccessCheck(False, 403, 'You need "Can Edit" permission to perform this action')

    async def run_envelope(run_id):
        return {"run_id": run_id, "space_id": SPACE, "status": "CONVERGED"}

    monkeypatch.setattr(space_access, "check_space_access", deny)
    monkeypatch.setattr(
        space_access, "require_obo_workspace_client",
        lambda: SimpleNamespace(config=SimpleNamespace(token="route-table")),
    )
    # Keep this fixture about access, not SDK cloning of the fake OBO client.
    monkeypatch.setattr(space_access, "bounded_read_client", lambda c, **k: c)
    monkeypatch.setattr(auto_optimize, "_load_run_envelope", run_envelope)
    for module_name, names in _TRIPWIRES:
        module = importlib.import_module(module_name)
        for name in names:
            if hasattr(module, name):
                monkeypatch.setattr(module, name, _Tripwire(f"{module_name}.{name}", hits))
    return TestClient(app, raise_server_exceptions=True), asked, hits


_GATED = sorted((key, entry) for key, entry in TABLE.items() if isinstance(entry, Gated))


def test_every_gated_entry_names_a_level():
    assert all(isinstance(entry.level, L) for _, entry in _GATED)


@pytest.mark.parametrize(
    ("key", "entry"),
    [pytest.param(key, entry, id=f"{key[0]} {key[1]}") for key, entry in _GATED],
)
def test_route_asks_for_its_level_before_any_work(denying_app, key, entry):
    client, asked, hits = denying_app
    method, template = key
    path = template.format(**{k: v for k, v in _PARAMS.items() if f"{{{k}}}" in template})
    response = client.request(method, path, json=entry.body, params=entry.query)
    _assert_gate_ran_first(response, asked, hits, entry.level)


def _assert_gate_ran_first(response, asked, hits, level):
    assert hits == [], f"touched before the access gate: {hits}"
    assert response.status_code == 403, response.text
    assert response.json()["detail"]["code"] == "space_access_denied"
    assert asked == [(SPACE, level)]


def _harness_selftest_client(*, swallow_pre_gate_work: bool) -> TestClient:
    from fastapi import FastAPI

    from backend.routers import auto_optimize

    app = FastAPI()

    @app.get("/selftest/{space_id}")
    async def selftest(space_id: str):
        if swallow_pre_gate_work:
            try:
                auto_optimize._build_gso_config()
            except Exception:
                pass
        await auto_optimize.require_space_access(space_id, L.EDIT)
        auto_optimize._build_gso_config()

    return TestClient(app, raise_server_exceptions=True)


def test_harness_flags_pre_gate_work_swallowed_by_except_exception(denying_app):
    _, asked, hits = denying_app
    response = _harness_selftest_client(swallow_pre_gate_work=True).get(f"/selftest/{SPACE}")
    assert response.status_code == 403
    with pytest.raises(AssertionError, match="touched before the access gate"):
        _assert_gate_ran_first(response, asked, hits, L.EDIT)


def test_harness_passes_a_route_that_gates_first(denying_app):
    _, asked, hits = denying_app
    response = _harness_selftest_client(swallow_pre_gate_work=False).get(f"/selftest/{SPACE}")
    _assert_gate_ran_first(response, asked, hits, L.EDIT)
