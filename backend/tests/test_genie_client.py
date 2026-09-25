import json
from types import SimpleNamespace

from backend.services.genie_client import get_serialized_space, normalize_metric_view_sources


class _TablesClient:
    def __init__(self, table_types: dict[str, str | None]):
        self._table_types = table_types

    def get(self, *, full_name: str):
        table_type = self._table_types.get(full_name)
        if table_type is None:
            raise RuntimeError("not found")
        return SimpleNamespace(table_type=table_type)


def _client(table_types: dict[str, str | None]):
    return SimpleNamespace(tables=_TablesClient(table_types))


def test_normalize_metric_view_sources_uses_uc_table_type():
    space_data = {
        "data_sources": {
            "tables": [
                {"identifier": "cat.sch.orders"},
                {"identifier": "cat.sch.sales_metrics"},
            ],
            "metric_views": [],
        }
    }

    normalize_metric_view_sources(
        space_data,
        client=_client({
            "cat.sch.orders": "MANAGED",
            "cat.sch.sales_metrics": "METRIC_VIEW",
        }),
    )

    assert [t["identifier"] for t in space_data["data_sources"]["tables"]] == ["cat.sch.orders"]
    assert [m["identifier"] for m in space_data["data_sources"]["metric_views"]] == ["cat.sch.sales_metrics"]


def test_normalize_metric_view_sources_respects_uc_non_metric_type():
    space_data = {
        "data_sources": {
            "tables": [{"identifier": "cat.sch.mv_orders"}],
            "metric_views": [],
        }
    }

    normalize_metric_view_sources(
        space_data,
        client=_client({"cat.sch.mv_orders": "MANAGED"}),
    )

    assert [t["identifier"] for t in space_data["data_sources"]["tables"]] == ["cat.sch.mv_orders"]
    assert space_data["data_sources"]["metric_views"] == []


def test_normalize_metric_view_sources_falls_back_to_mv_prefix_when_type_unknown():
    space_data = {
        "data_sources": {
            "tables": [
                {"identifier": "cat.sch.orders"},
                {"identifier": "cat.sch.mv_retail_sales"},
            ]
        }
    }

    normalize_metric_view_sources(space_data)

    assert [t["identifier"] for t in space_data["data_sources"]["tables"]] == ["cat.sch.orders"]
    assert [m["identifier"] for m in space_data["data_sources"]["metric_views"]] == ["cat.sch.mv_retail_sales"]


def test_get_serialized_space_does_not_copy_top_level_description_by_default(monkeypatch):
    monkeypatch.setattr(
        "backend.services.genie_client.get_genie_space",
        lambda genie_space_id=None: {
            "description": "Useful top-level description for this sales analytics space.",
            "serialized_space": json.dumps({
                "data_sources": {"tables": []},
                "instructions": {},
                "benchmarks": {},
            }),
        },
    )
    monkeypatch.setattr(
        "backend.services.genie_client.get_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("no workspace client")),
    )

    space_data = get_serialized_space("space-1")

    assert "description" not in space_data


def test_get_serialized_space_can_copy_top_level_description_for_scoring(monkeypatch):
    monkeypatch.setattr(
        "backend.services.genie_client.get_genie_space",
        lambda genie_space_id=None: {
            "description": "Useful top-level description for this sales analytics space.",
            "serialized_space": json.dumps({
                "description": "TBD",
                "data_sources": {"tables": []},
                "instructions": {},
                "benchmarks": {},
            }),
        },
    )
    monkeypatch.setattr(
        "backend.services.genie_client.get_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("no workspace client")),
    )

    space_data = get_serialized_space("space-1", include_top_level_description=True)

    assert space_data["description"] == "Useful top-level description for this sales analytics space."
import pytest


@pytest.mark.parametrize("method", ["create", "config", "description"])
@pytest.mark.parametrize("outcome", ["success", "timeout", "503", "redirect"])
def test_transport_disables_hidden_sdk_retries_for_post_and_both_patches(monkeypatch, method, outcome):
    import requests
    from unittest.mock import Mock
    from backend.services.genie_client import GenieTransport
    from backend.services.version_control import contracts as vc
    from backend.tests.vc_fakes.fixtures import binding_fixture, executor_fixture
    from backend.tests._vc_rig import uid
    binding, executor = binding_fixture(), executor_fixture()
    coordination = Mock(spec=vc.Coordination)
    registry = Mock(spec=vc.Registry)
    registry.resolve.return_value = binding
    flags = Mock()
    flags.enabled.return_value = True
    intent = vc.CreateIntentRef(uid(), uid(), binding.binding_id, 1, "a" * 64)
    claim = vc.AdmissionClaim(binding.binding_id, 1, uid(), 1, 1,
        vc.RequestIdentity(intent.operation_id, "send", "a" * 64), intent, None, None)
    if method == "create":
        from dataclasses import replace
        binding = replace(binding, space_id=None)
        registry.resolve.return_value = binding
    sends = []
    def send(adapter, request, **kwargs):
        assert adapter.max_retries.total == 0
        sends.append(request)
        if outcome == "timeout":
            raise requests.ReadTimeout("may have applied")
        response = requests.Response()
        response.request = request
        response.url = request.url
        response.status_code = {"success": 200, "503": 503, "redirect": 307}[outcome]
        response._content = b'{"space_id":"created"}'
        if outcome == "redirect":
            response.headers["Location"] = "https://other.invalid/leak"
        return response
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    transport = GenieTransport(executor=executor, authenticate=lambda context: {"Authorization": "Bearer test-only"},
        coordination=coordination, registry=registry, flags=flags, warehouse_id="warehouse")
    call = {"create": lambda: transport.create_once(vc.CreatePayload("Sales", {}, "new"), claim),
            "config": lambda: transport.patch_config_once(binding, {"version": 2}, claim),
            "description": lambda: transport.patch_description_once(binding, "new", claim)}[method]
    if outcome == "success":
        call()
    else:
        with pytest.raises((requests.RequestException, RuntimeError)):
            call()
    assert len(sends) == 1, "Exactly one HTTP send, with no SDK retry or redirect replay"
    assert sends[0].method == ("POST" if method == "create" else "PATCH")
    coordination.assert_owner.assert_called_once_with(claim)


def test_transport_get_pins_by_value_not_object_identity():
    """Callers re-derive a freshly *verified* executor per operation (a new object
    for the same principal), so the GET pin must accept value-equal executors -- the
    promotion observer path depends on it -- while still rejecting a different
    principal/workspace/execution_ref. The credential used is always self.executor."""
    from dataclasses import replace
    from unittest.mock import Mock

    from backend.services.genie_client import GenieTransport
    from backend.services.version_control import contracts as vc
    from backend.tests.vc_fakes.fixtures import binding_fixture, executor_fixture

    binding, executor = binding_fixture(), executor_fixture()
    registry = Mock(spec=vc.Registry)
    registry.resolve.return_value = binding
    flags = Mock()
    flags.enabled.return_value = True
    transport = GenieTransport(executor=executor, authenticate=lambda ctx: {"Authorization": "x"},
                               coordination=Mock(spec=vc.Coordination), registry=registry, flags=flags)
    transport._request = lambda *a, **k: {"serialized_space": {"x": 1}}
    twin = replace(executor)  # value-equal, new object (fresh live verification)
    assert twin is not executor and twin == executor
    assert transport.get(binding, twin) == {"serialized_space": {"x": 1}}
    for bad in (replace(executor, principal_id="intruder"),
                replace(executor, workspace_id="999"),
                replace(executor, execution_ref="job/999")):
        with pytest.raises(PermissionError, match="pinned"):
            transport.get(binding, bad)
