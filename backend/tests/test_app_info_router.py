"""/api/admin/version and /api/admin/update-check (spec 5.4 to 5.6)."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import app_info

ADMIN = {"X-Forwarded-Groups": "users,admins", "X-Forwarded-Email": "admin@example.com", "X-Forwarded-User": "1"}
NON_ADMIN = {"X-Forwarded-Groups": "users", "X-Forwarded-Email": "user@example.com", "X-Forwarded-User": "2"}


@pytest.fixture
def store(monkeypatch):
    """A fake Lakebase settings store (available by default)."""
    state = {"available": True, "rows": {}, "read_error": None, "write_error": None, "writes": []}

    async def get_app_setting(key):
        if state["read_error"]:
            raise state["read_error"]
        return state["rows"].get(key) if state["available"] else None

    async def set_app_setting(key, value, updated_by):
        if state["write_error"]:
            raise state["write_error"]
        if not state["available"]:
            return False
        state["rows"][key] = value
        state["writes"].append((key, value, updated_by))
        return True

    monkeypatch.setattr(app_info.lakebase, "get_app_setting", get_app_setting)
    monkeypatch.setattr(app_info.lakebase, "set_app_setting", set_app_setting)
    monkeypatch.setattr(app_info.lakebase, "is_available", lambda: state["available"])
    for name in ("UPDATE_CHECK_ENABLED", "DEV_ADMIN", "DEV_USER_EMAIL", "GWB_BUILD_COMMIT", "GWB_DEPLOYED_AT", "GWB_INSTALL_METHOD"):
        monkeypatch.delenv(name, raising=False)
    return state


@pytest.fixture
def client(store):
    app = FastAPI()
    app.include_router(app_info.router)
    return TestClient(app)


def test_version_reports_the_version_file_and_build_env(client, monkeypatch):
    monkeypatch.setenv("GWB_BUILD_COMMIT", "99e7783ab12c")
    monkeypatch.setenv("GWB_DEPLOYED_AT", "2026-11-05T15:04:00Z")
    monkeypatch.setenv("GWB_INSTALL_METHOD", "cli")
    body = client.get("/api/admin/version", headers=NON_ADMIN).json()
    assert body["version"] == "0.2.1"
    assert (body["commit"], body["deployed_at"], body["install_method"]) == ("99e7783ab12c", "2026-11-05T15:04:00Z", "cli")
    assert body["releases_repo"] == "databricks-solutions/databricks-genie-workbench"
    assert body["release_url"].endswith("/releases/tag/0.2.1")


@pytest.mark.parametrize(
    "install_env,available,stored,expected",
    [
        ("true", True, None, ("on", "install", True, True)),
        ("false", True, "on", ("off", "install", True, True)),
        ("", True, "on", ("on", "app", False, True)),
        ("", True, "off", ("off", "app", False, True)),
        ("", True, None, ("ask", "none", False, True)),
        ("", False, None, ("ask", "none", False, False)),
        ("", True, "true", ("ask", "none", False, True)),  # junk stored value
        ("__UPDATE_CHECK_ENABLED__", True, None, ("ask", "none", False, True)),  # Review Focus 2
    ],
)
def test_update_check_resolution(client, store, monkeypatch, install_env, available, stored, expected):
    monkeypatch.setenv("UPDATE_CHECK_ENABLED", install_env)
    store["available"] = available
    if stored is not None:
        store["rows"]["update_check"] = stored
    uc = client.get("/api/admin/version").json()["update_check"]
    assert (uc["mode"], uc["source"], uc["locked"], uc["persisted"]) == expected


def test_a_lakebase_read_error_fails_closed_without_leaking(client, store, caplog):
    store["read_error"] = RuntimeError("password zq_secret")
    response = client.get("/api/admin/version")
    uc = response.json()["update_check"]
    assert (uc["mode"], uc["source"]) == ("off", "error")
    assert "zq_secret" not in response.text and "zq_secret" not in caplog.text


def test_non_admins_cannot_change_the_setting(client, store):
    response = client.put("/api/admin/update-check", json={"enabled": True}, headers=NON_ADMIN)
    assert response.status_code == 403
    assert store["writes"] == []


def test_an_install_time_answer_locks_the_setting(client, store, monkeypatch):
    monkeypatch.setenv("UPDATE_CHECK_ENABLED", "false")
    response = client.put("/api/admin/update-check", json={"enabled": True}, headers=ADMIN)
    assert response.status_code == 409
    assert response.json()["detail"] == app_info.LOCKED_DETAIL
    assert store["writes"] == []


def test_an_admin_answer_is_stored_app_wide(client, store):
    response = client.put("/api/admin/update-check", json={"enabled": True}, headers=ADMIN)
    assert response.status_code == 200
    assert response.json() == {"mode": "on", "source": "app", "locked": False, "persisted": True}
    assert store["writes"] == [("update_check", "on", "admin@example.com")]
    uc = client.get("/api/admin/version").json()["update_check"]
    assert (uc["mode"], uc["source"]) == ("on", "app")


def test_without_lakebase_the_answer_is_not_persisted(client, store):
    store["available"] = False
    response = client.put("/api/admin/update-check", json={"enabled": False}, headers=ADMIN)
    assert response.json() == {"mode": "off", "source": "app", "locked": False, "persisted": False}
    assert store["rows"] == {}


def test_a_save_failure_is_a_503_without_leaking(client, store, caplog):
    store["write_error"] = RuntimeError("password zq_secret")
    response = client.put("/api/admin/update-check", json={"enabled": True}, headers=ADMIN)
    assert response.status_code == 503
    assert response.json()["detail"] == app_info.SAVE_FAILED_DETAIL
    assert "zq_secret" not in response.text and "zq_secret" not in caplog.text
