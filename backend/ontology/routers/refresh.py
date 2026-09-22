"""Ontology refresh routes (Phase 2, MV-D41).

`GET /api/ontology/refresh` → freshness + last-run state (drives the freshness chip
and the button label). `POST /api/ontology/refresh` → trigger the materialize job
(idempotent while running). The POST triggers a job; it does NOT write UC or the
snapshot tables — the job does. No governed-tag writes here.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter

from backend.ontology.services import refresh

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


@router.get("/refresh")
async def get_refresh_status() -> dict:
    return (await refresh.get_status()).model_dump(mode="json")


@router.post("/refresh")
async def trigger_refresh() -> dict:
    return (await refresh.trigger()).model_dump(mode="json")


@router.get("/scan-stats")
async def get_scan_stats() -> dict:
    # Read-only last-run stats for the "How it works" explainer (MV-D108). No write path.
    return (await refresh.get_scan_stats()).model_dump(mode="json")
