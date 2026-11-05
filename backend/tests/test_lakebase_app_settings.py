"""App-wide settings live in Lakebase only; there is no in-memory fallback (spec 5.5)."""

import inspect

import pytest

from backend.services import lakebase


class _Conn:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self.executed: list[tuple[str, tuple]] = []

    async def fetchrow(self, sql, *args):
        self.executed.append((sql, args))
        return self.rows.get(args[0])

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        if "INSERT INTO genie.app_settings" in sql:
            self.rows[args[0]] = {"value": args[1]}


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self):
        self.conn = _Conn()

    def acquire(self):
        return _Acquire(self.conn)


@pytest.fixture
def pool(monkeypatch):
    p = _Pool()
    monkeypatch.setattr(lakebase, "_pool", p)
    monkeypatch.setattr(lakebase, "_lakebase_available", True)
    return p


@pytest.fixture
def no_lakebase(monkeypatch):
    monkeypatch.setattr(lakebase, "_pool", None)
    monkeypatch.setattr(lakebase, "_lakebase_available", False)


async def test_unset_setting_reads_none(pool):
    assert await lakebase.get_app_setting("update_check") is None


async def test_set_then_get_round_trips_and_records_who(pool):
    assert await lakebase.set_app_setting("update_check", "on", "admin@example.com") is True
    assert await lakebase.get_app_setting("update_check") == "on"
    sql, args = pool.conn.executed[0]
    assert "ON CONFLICT (key) DO UPDATE" in sql
    assert args == ("update_check", "on", "admin@example.com")


async def test_without_lakebase_nothing_is_stored_in_memory(no_lakebase):
    assert await lakebase.set_app_setting("update_check", "on", "a@example.com") is False
    assert await lakebase.get_app_setting("update_check") is None


async def test_read_errors_propagate_so_callers_can_fail_closed(pool, monkeypatch):
    async def boom(sql, *args):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(pool.conn, "fetchrow", boom)
    with pytest.raises(RuntimeError):
        await lakebase.get_app_setting("update_check")


def test_schema_creates_app_settings():
    src = inspect.getsource(lakebase._ensure_schema)
    assert "CREATE TABLE IF NOT EXISTS genie.app_settings" in src
