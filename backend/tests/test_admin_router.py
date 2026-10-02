"""The admin summaries list the caller's agents and never quote space content (MV-D119)."""

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import admin

_ROUTES = ["/api/admin/dashboard", "/api/admin/leaderboard", "/api/admin/alerts"]


def _client(monkeypatch, *, spaces=(), summaries=(), fail: Exception | None = None):
    calls: list[dict] = []

    def listing(**kwargs):
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()  # the listing blocks, so it runs in a worker thread
        calls.append(kwargs)
        if fail:
            raise fail
        return [dict(s) for s in spaces]

    async def all_summaries():
        return [dict(s) for s in summaries]

    monkeypatch.setattr(admin, "list_genie_spaces", listing)
    monkeypatch.setattr(admin, "get_all_scan_summaries", all_summaries)
    app = FastAPI()
    app.include_router(admin.router)
    return TestClient(app), calls


def _summary(space_id, findings, score=2):
    return {"space_id": space_id, "score": score, "maturity": "Not Ready",
            "findings": findings, "scanned_at": "2026-09-30T10:00:00+00:00"}


@pytest.mark.parametrize("path", _ROUTES)
def test_every_admin_route_lists_under_the_caller_s_token(monkeypatch, path):
    client, calls = _client(monkeypatch)
    assert client.get(path).status_code == 200
    assert calls == [{"sp_fallback": False}]


@pytest.mark.parametrize("path", _ROUTES)
def test_a_listing_failure_is_a_503_with_no_exception_text(monkeypatch, caplog, path):
    client, _ = _client(monkeypatch, fail=RuntimeError("insufficient_scope zq_secret"))
    response = client.get(path)
    assert response.status_code == 503
    assert response.json()["detail"] == admin.LISTING_UNAVAILABLE
    assert "zq_secret" not in response.text and "zq_secret" not in caplog.text


@pytest.mark.parametrize("path", _ROUTES)
def test_a_summary_failure_is_a_500_with_no_exception_text(monkeypatch, caplog, path):
    client, _ = _client(monkeypatch, spaces=[{"space_id": "s1"}])

    async def failing():
        raise RuntimeError("lakebase zq_secret")

    monkeypatch.setattr(admin, "get_all_scan_summaries", failing)
    response = client.get(path)
    assert response.status_code == 500
    assert "zq_secret" not in response.text and "zq_secret" not in caplog.text


def test_top_finding_is_the_count_only_form(monkeypatch):
    client, _ = _client(monkeypatch, spaces=[{"space_id": "s1", "title": "Sales"}], summaries=[
        _summary("s1", ["6/20 visible columns look internal/noisy (zq_secret, etl_batch_id)"]),
    ])
    body = client.get("/api/admin/alerts").json()
    assert body[0]["top_finding"] == "6/20 visible columns look internal/noisy"
    assert "zq_secret" not in str(body)


def test_top_finding_skips_text_with_no_viewer_safe_form(monkeypatch):
    client, _ = _client(monkeypatch, spaces=[{"space_id": "s1"}, {"space_id": "s2"}], summaries=[
        _summary("s1", ["Legacy zq_secret wording", "No benchmark questions configured"]),
        _summary("s2", ["Legacy zq_secret wording"], score=1),
    ])
    by_id = {a["space_id"]: a["top_finding"] for a in client.get("/api/admin/alerts").json()}
    assert by_id == {"s1": "No benchmark questions configured", "s2": None}


def test_a_space_the_caller_cannot_list_is_not_counted(monkeypatch):
    client, _ = _client(monkeypatch, spaces=[{"space_id": "s1"}], summaries=[
        _summary("s1", []), _summary("hidden", [], score=0),
    ])
    stats = client.get("/api/admin/dashboard").json()
    assert (stats["total_spaces"], stats["scanned_spaces"]) == (1, 1)
    assert [a["space_id"] for a in client.get("/api/admin/alerts").json()] == ["s1"]
    board = client.get("/api/admin/leaderboard").json()
    assert {e["space_id"] for e in board["top"] + board["bottom"]} == {"s1"}
