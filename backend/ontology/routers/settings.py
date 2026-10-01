"""Ontology settings (company name + catalog allowlist, MV-D42).

`GET /api/ontology/settings` reads; `PUT /api/ontology/settings` writes — the ONLY
Phase-1 write, and it writes **our** config (Lakebase), never Unity Catalog. This
module issues no governed-tag DDL and imports no tag-write tool; the read-only
firewall test (`test_ontology_firewall.py`) pins that.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from backend.ontology.models import OntologySettings
from backend.ontology.services import ont_settings
from backend.services.admin_gate import require_admin

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology", dependencies=[Depends(require_admin)])


@router.get("/settings")
async def read_settings() -> dict:
    return (await ont_settings.get_settings()).model_dump(mode="json")


@router.put("/settings")
async def write_settings(payload: OntologySettings) -> dict:
    saved = await ont_settings.save_settings(payload)
    return saved.model_dump(mode="json")
