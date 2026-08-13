from __future__ import annotations

import pytest
from databricks.sdk.errors import PermissionDenied
from fastapi import HTTPException

from backend.watch.models import TrafficGapAnalysis
from backend.watch.routers import traffic_gaps
from backend.watch.services.traffic_gap_reader import IncompleteTrafficRead


SPACE_ID = "b" * 32


@pytest.mark.asyncio
async def test_endpoint_requires_user_authorization(monkeypatch) -> None:
    monkeypatch.setattr(
        traffic_gaps,
        "require_obo_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("no user")),
    )

    with pytest.raises(HTTPException) as exc:
        await traffic_gaps.get_traffic_gaps(SPACE_ID)

    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_endpoint_maps_manager_permission_failure_to_403(monkeypatch) -> None:
    monkeypatch.setattr(traffic_gaps, "require_obo_workspace_client", object)
    monkeypatch.setattr(
        traffic_gaps,
        "read_traffic_gap_analysis",
        lambda **_kwargs: (_ for _ in ()).throw(PermissionDenied("denied")),
    )

    with pytest.raises(HTTPException) as exc:
        await traffic_gaps.get_traffic_gaps(SPACE_ID)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_endpoint_returns_explicit_unavailable_for_incomplete_read(monkeypatch) -> None:
    monkeypatch.setattr(traffic_gaps, "require_obo_workspace_client", object)
    monkeypatch.setattr(
        traffic_gaps,
        "read_traffic_gap_analysis",
        lambda **_kwargs: (_ for _ in ()).throw(IncompleteTrafficRead("bad page")),
    )

    with pytest.raises(HTTPException) as exc:
        await traffic_gaps.get_traffic_gaps(SPACE_ID)

    assert exc.value.status_code == 503
    assert "complete traffic" in exc.value.detail


@pytest.mark.asyncio
async def test_endpoint_returns_analysis(monkeypatch) -> None:
    marker = TrafficGapAnalysis(
        scanned_message_count=0,
        family_count=0,
        covered_family_count=0,
    )
    client = object()
    monkeypatch.setattr(traffic_gaps, "require_obo_workspace_client", lambda: client)
    monkeypatch.setattr(
        traffic_gaps,
        "read_traffic_gap_analysis",
        lambda *, client, space_id: marker,
    )

    assert await traffic_gaps.get_traffic_gaps(SPACE_ID) is marker
