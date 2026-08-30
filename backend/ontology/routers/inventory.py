"""Ontology inventory (OBO fast-path, MV-D43).

`GET /api/ontology/inventory` returns the cheap ``information_schema`` counts that
render in-request on first load, scoped to the catalog allowlist (MV-D42). Metric
views + governed tags are counted as the user (OBO, auto-filtered, no grant); the
Genie Agent count reuses the existing space list. An empty allowlist returns zero
counts and scans nothing.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.ontology.models import OntologyInventory
from backend.ontology.services import inventory, ont_settings
from backend.services import genie_client
from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001 — fast-path never fails the page
        logger.info("%s failed: %s", getattr(fn, "__name__", fn), e)
        return None


@router.get("/inventory")
async def get_inventory() -> dict:
    settings = await ont_settings.get_settings()
    allowlist = settings.catalog_allowlist

    # Resolve the OBO client in the request context, then hand the client (which
    # carries the user's token) into worker threads — no ContextVar propagation.
    client = get_workspace_client()

    mv_count, tag_count, spaces = await asyncio.gather(
        asyncio.to_thread(_safe, inventory.metric_view_count, client, allowlist),
        asyncio.to_thread(_safe, inventory.governed_tag_count, client, allowlist),
        asyncio.to_thread(_safe, genie_client.list_genie_spaces),
    )

    return OntologyInventory(
        catalogs_scanned=list(allowlist),
        metric_view_count=int(mv_count or 0),
        genie_agent_count=len(spaces or []),
        governed_tag_count=int(tag_count or 0),
        as_of=datetime.now(timezone.utc).isoformat(),
    ).model_dump(mode="json")
