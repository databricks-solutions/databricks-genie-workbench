"""Times written by lakebase.py carry their UTC offset (MV-D119)."""

import inspect
from datetime import datetime, timedelta

import pytest

from backend.services import lakebase


@pytest.fixture
def memory(monkeypatch):
    monkeypatch.setattr(lakebase, "_lakebase_available", False)
    monkeypatch.setattr(lakebase, "_pool", None)
    monkeypatch.setattr(lakebase, "_memory_store", {
        **lakebase._memory_store, "scans": {}, "history": {}, "seen": set(), "optimization_runs": {},
        "join_advice": {}, "watch_space_cache": {}, "watch_sync_watermark": {},
    })


def _is_utc(value: str) -> bool:
    return datetime.fromisoformat(value).utcoffset() == timedelta(0)


async def test_a_scan_saved_without_a_time_is_stamped_in_utc(memory):
    await lakebase.save_scan_result("s1", {"score": 1, "maturity": "Not Ready"})
    assert _is_utc(lakebase._memory_store["scans"]["s1"]["scanned_at"])


async def test_an_optimization_run_is_stamped_in_utc(memory):
    await lakebase.save_optimization_run("s1", 10, 5)
    assert _is_utc((await lakebase.get_latest_optimization_run("s1"))["created_at"])


async def test_join_advice_returns_a_utc_time(memory):
    record = await lakebase.save_join_advice("s1", [{"left": "a", "right": "b"}], seeded_by="u")
    assert _is_utc(record["updated_at"])


async def test_watch_writes_are_stamped_in_utc(memory):
    await lakebase.watch_upsert_space({"space_id": "s1", "title": "t"})
    await lakebase.watch_set_watermark("spaces", "ok")
    assert _is_utc(lakebase._memory_store["watch_space_cache"]["s1"]["updated_at"])
    assert _is_utc((await lakebase.watch_get_watermark("spaces"))["last_synced_at"])


def test_lakebase_never_calls_utcnow():
    assert "utcnow" not in inspect.getsource(lakebase)
