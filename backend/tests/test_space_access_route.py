"""GET /api/spaces/{space_id}/access: the level Genie grants the signed-in user (MV-D109)."""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.routers import spaces
from backend.services.space_access import SpaceAccessLevel as L

pytestmark = pytest.mark.real_space_access

_SPACE = "01f19f413ccc1ea3a42055a66e886302"


def _client(monkeypatch, answer):
    def resolve(space_id):
        assert space_id == _SPACE
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(spaces, "resolve_space_access_level", resolve)
    app = FastAPI()
    app.include_router(spaces.router)
    return TestClient(app)


@pytest.mark.parametrize("held,level", [(L.MANAGE, "manage"), (L.EDIT, "edit"), (L.VIEW, "view"), (None, None)])
def test_the_route_reports_the_level_genie_grants(monkeypatch, held, level):
    response = _client(monkeypatch, held).get(f"/api/spaces/{_SPACE}/access")
    assert response.status_code == 200
    assert response.json() == {"space_id": _SPACE, "level": level}


def test_a_refusal_other_than_denial_reaches_the_client(monkeypatch):
    refusal = HTTPException(503, detail={
        "code": "space_access_unavailable", "required": "edit",
        "message": "Could not verify your access to this Genie Agent. Try again shortly.",
        "platform_message": ""})
    response = _client(monkeypatch, refusal).get(f"/api/spaces/{_SPACE}/access")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "space_access_unavailable"


def test_space_detail_scope_error_does_not_call_the_service_principal(monkeypatch):
    """M1c-D3: a scope error from the user's client must not retry as the SP.

    The handler's catch-all returns 500; Task 6's VIEW gate answers before it
    for a denial. This test opts out of the default grant (``real_space_access``),
    so the gate is stubbed open to reach the OBO read under test.
    """
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    user_calls: list[str] = []

    def user_do(method, path, query=None):
        user_calls.append(path)
        raise RuntimeError("insufficient_scope: genie")

    async def allow(_space_id, _level):
        return None

    user = SimpleNamespace(api_client=SimpleNamespace(do=user_do))
    sp_client = MagicMock(name="sp")

    monkeypatch.setattr(spaces, "require_space_access", allow)
    monkeypatch.setattr(spaces, "require_obo_workspace_client", lambda: user)
    # SP must not be consulted for this read (spaces.py no longer imports it;
    # pin auth so a regression that reintroduces the call is visible).
    monkeypatch.setattr(
        "backend.services.auth.get_service_principal_client", lambda: sp_client
    )

    app = FastAPI()
    app.include_router(spaces.router)
    resp = TestClient(app).get(f"/api/spaces/{_SPACE}")
    assert resp.status_code == 500
    assert user_calls
    sp_client.api_client.do.assert_not_called()


def _stored_scan() -> dict:
    from backend.services.scanner import calculate_score

    cols = [{"name": f"business_col_{i}", "description": "Useful business column"} for i in range(14)]
    cols += [{"name": n} for n in ["etl_batch_id", "raw_payload_json", "debug_flag", "audit_user", "col_1", "load_timestamp"]]
    return calculate_score({
        "data_sources": {"tables": [{"name": "t", "columns": cols}]},
        "instructions": {"text_instructions": [{"content": ["Use SELECT * FROM zq_orders WHERE region = 'AMER' for American orders."]}]},
        "benchmarks": {},
    })


def _detail_client(monkeypatch, *, can_edit: bool):
    from types import SimpleNamespace

    stored = _stored_scan()

    async def allow(_space_id, _level):
        return None

    async def latest(_space_id):
        return stored

    async def starred(_space_id):
        return False

    held: list = []

    def held_check(space_id, level):
        held.append((space_id, level))
        return can_edit

    monkeypatch.setattr(spaces, "require_space_access", allow)
    monkeypatch.setattr(spaces, "space_access_held", held_check)
    monkeypatch.setattr(spaces, "require_obo_workspace_client",
                        lambda: SimpleNamespace(api_client=SimpleNamespace(do=lambda **_: {"space_id": _SPACE})))
    monkeypatch.setattr(spaces, "get_latest_score", latest)
    monkeypatch.setattr(spaces, "is_space_starred", starred)
    app = FastAPI()
    app.include_router(spaces.router)
    return TestClient(app), held


def test_space_detail_redacts_the_stored_scan_below_can_edit(monkeypatch):
    client, held = _detail_client(monkeypatch, can_edit=False)
    body = client.get(f"/api/spaces/{_SPACE}").json()
    assert held == [(_SPACE, L.EDIT)]
    text = str(body["scan_result"])
    assert "zq_orders" not in text and "etl_batch_id" not in text
    assert "6/20 visible columns look internal/noisy" in body["scan_result"]["findings"]


def test_space_detail_serves_the_stored_scan_verbatim_to_an_editor(monkeypatch):
    client, _ = _detail_client(monkeypatch, can_edit=True)
    body = client.get(f"/api/spaces/{_SPACE}").json()
    assert "zq_orders" in str(body["scan_result"]["warnings"])
    assert any("etl_batch_id" in f for f in body["scan_result"]["findings"])


def test_history_in_memory_mode_carries_no_scan_content_to_a_viewer(monkeypatch):
    import asyncio

    from backend.services import lakebase

    monkeypatch.setattr(lakebase, "_lakebase_available", False)
    monkeypatch.setattr(lakebase, "_pool", None)
    monkeypatch.setattr(lakebase, "_memory_store", {**lakebase._memory_store, "scans": {}, "history": {}, "seen": set()})
    stored = _stored_scan()
    assert "First offender" in str(stored["warnings"])  # positive control
    asyncio.run(lakebase.save_scan_result(_SPACE, stored))

    levels: list = []

    async def allow(_space_id, level):
        levels.append(level)

    async def no_runs(_space_id):
        return []

    monkeypatch.setattr(spaces, "require_space_access", allow)
    monkeypatch.setattr(spaces, "load_runs_with_fallback", no_runs)
    app = FastAPI()
    app.include_router(spaces.router)
    body = TestClient(app).get(f"/api/spaces/{_SPACE}/history").json()

    assert levels == [L.VIEW]
    assert len(body["scans"]) == 1
    assert not {"findings", "warnings", "checks"} & set(body["scans"][0])
    assert "First offender" not in str(body) and "zq_orders" not in str(body)
