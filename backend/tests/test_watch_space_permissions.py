import asyncio
import threading
import time

from backend.watch.routers import spaces
from backend.watch.services import genie_client


def test_permissions_endpoint_uses_service_principal(monkeypatch):
    calls = []

    class ApiClient:
        def do(self, **kwargs):
            calls.append(kwargs)
            return {"access_control_list": []}

    class Client:
        api_client = ApiClient()

    client = Client()
    monkeypatch.setattr(genie_client, "get_service_principal_client", lambda: client)
    monkeypatch.setattr(
        genie_client,
        "get_workspace_client",
        lambda: (_ for _ in ()).throw(AssertionError("OBO client must not be used")),
    )

    assert genie_client.list_space_permissions("abc") == {"access_control_list": []}
    assert calls == [{
        "method": "GET",
        "path": "/api/2.0/permissions/dashboards.genie/abc",
    }]


def test_normalizes_only_managers_for_all_principal_types_and_inheritance():
    payload = {"access_control_list": [
        {"user_name": "a@example.com", "all_permissions": [
            {"permission_level": "CAN_VIEW"},
            {"permission_level": "CAN_MANAGE"},
        ]},
        {"group_name": "admins", "all_permissions": [
            {"permission_level": "CAN_MANAGE", "inherited": True},
        ]},
        {"service_principal_name": "automation", "all_permissions": [
            {"permission_level": "CAN_MANAGE", "inherited_from_object": ["root"]},
        ]},
        {"user_name": "viewer@example.com", "all_permissions": [
            {"permission_level": "CAN_VIEW"},
        ]},
    ]}

    assert [p.model_dump() for p in spaces._manager_permissions(payload)] == [
        {"principal": "a@example.com", "permission_level": "CAN_MANAGE", "principal_type": "user", "inherited": False},
        {"principal": "admins", "permission_level": "CAN_MANAGE", "principal_type": "group", "inherited": True},
        {"principal": "automation", "permission_level": "CAN_MANAGE", "principal_type": "service_principal", "inherited": True},
    ]


def test_permission_population_reuses_cache_and_preserves_it_on_forced_failure(monkeypatch):
    cached = [{"principal": "cached", "permission_level": "CAN_MANAGE"}]

    async def list_cached():
        return [{"space_id": "one", "permissions": cached}]

    monkeypatch.setattr(spaces.lakebase, "watch_list_cached_spaces", list_cached)
    calls = []

    def fetch(space_id):
        calls.append(space_id)
        raise RuntimeError("denied")

    monkeypatch.setattr(spaces.genie_client, "list_space_permissions", fetch)
    summaries = [{"space_id": "one", "permissions": []}]
    asyncio.run(spaces._populate_permissions(summaries))
    assert calls == []
    assert summaries[0]["permissions"] == cached

    asyncio.run(spaces._populate_permissions(summaries, force=True))
    assert calls == ["one"]
    assert summaries[0]["permissions"] == cached


def test_permission_population_failure_without_cache_is_empty(monkeypatch):
    async def list_cached():
        return []

    monkeypatch.setattr(spaces.lakebase, "watch_list_cached_spaces", list_cached)
    monkeypatch.setattr(
        spaces.genie_client, "list_space_permissions", lambda _: (_ for _ in ()).throw(RuntimeError("denied")),
    )
    summaries = [{"space_id": "one", "permissions": []}]
    asyncio.run(spaces._populate_permissions(summaries))
    assert summaries[0]["permissions"] == []


def test_permission_population_limits_concurrency_to_eight(monkeypatch):
    async def list_cached():
        return []

    active = 0
    peak = 0
    calls = 0
    lock = threading.Lock()

    def fetch(_):
        nonlocal active, peak, calls
        with lock:
            calls += 1
            active += 1
            peak = max(peak, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return {"access_control_list": []}

    monkeypatch.setattr(spaces.lakebase, "watch_list_cached_spaces", list_cached)
    monkeypatch.setattr(spaces.genie_client, "list_space_permissions", fetch)
    summaries = [{"space_id": str(i), "permissions": []} for i in range(20)]
    asyncio.run(spaces._populate_permissions(summaries))
    assert calls == 20
    assert peak <= 8


def test_permission_population_isolates_per_agent_failures(monkeypatch):
    cached = [{"principal": "cached@example.com", "permission_level": "CAN_MANAGE"}]

    async def list_cached():
        return [{"space_id": "fails", "permissions": cached}]

    def fetch(space_id):
        if space_id == "fails":
            raise RuntimeError("denied")
        return {"access_control_list": [{
            "group_name": "fresh-admins",
            "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": True}],
        }]}

    monkeypatch.setattr(spaces.lakebase, "watch_list_cached_spaces", list_cached)
    monkeypatch.setattr(spaces.genie_client, "list_space_permissions", fetch)
    summaries = [
        {"space_id": "succeeds", "permissions": []},
        {"space_id": "fails", "permissions": []},
    ]

    asyncio.run(spaces._populate_permissions(summaries, force=True))

    assert summaries[0]["permissions"] == [{
        "principal": "fresh-admins",
        "permission_level": "CAN_MANAGE",
        "principal_type": "group",
        "inherited": True,
    }]
    assert summaries[1]["permissions"] == cached


def test_summary_keeps_owner_field_without_populating_it():
    summary = spaces._to_summary({
        "id": "agent-1",
        "owner_email": "legacy@example.com",
        "creator": {"user_name": "creator@example.com"},
    })

    assert "owner_email" in summary
    assert summary["owner_email"] is None


def test_refresh_route_forces_permission_refresh(monkeypatch):
    force_values = []

    async def refresh(*, force_permissions=False):
        force_values.append(force_permissions)
        return [{"space_id": "one"}]

    monkeypatch.setattr(spaces, "_refresh_cache_with_live_listing", refresh)

    assert asyncio.run(spaces.refresh_spaces()) == {"refreshed": 1}
    assert force_values == [True]
