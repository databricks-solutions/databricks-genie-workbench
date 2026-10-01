"""Ontology settings (company name + catalog allowlist, MV-D42).

`GET /api/ontology/settings` reads; `PUT /api/ontology/settings` writes — the ONLY
Phase-1 write, and it writes **our** config (Lakebase), never Unity Catalog. This
module issues no governed-tag DDL and imports no tag-write tool; the read-only
firewall test (`test_ontology_firewall.py`) pins that.

After a successful save the PUT also mirrors the saved Settings to the Delta row the
nightly job reads (`settings_mirror.write_mirror`, SP, MV-D109 P5). Lakebase stays the
source of truth; a mirror failure logs and never fails the save (MV-D43).
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, Request

from backend.ontology.models import OntologySettings
from backend.ontology.routers.drafts import _obo_email
from backend.ontology.services import ont_settings, settings_mirror
from backend.services.admin_gate import require_admin

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology", dependencies=[Depends(require_admin)])


@router.get("/settings")
async def read_settings() -> dict:
    return (await ont_settings.get_settings()).model_dump(mode="json")


@router.put("/settings")
async def write_settings(payload: OntologySettings, request: Request) -> dict:
    saved = await ont_settings.save_settings(payload)
    try:
        await asyncio.to_thread(
            settings_mirror.write_mirror,
            saved,
            workspace_id=ont_settings._workspace_id(),
            metastore_id=ont_settings._metastore_id(),
            updated_by=_obo_email(request),
        )
    except Exception as e:  # noqa: BLE001 — the mirror is best-effort; the save stands
        logger.warning("ontology settings mirror write failed (nightly keeps prior row): %s", e)
    return saved.model_dump(mode="json")
