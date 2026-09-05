"""Ontology apply routes (Phase 5 / 17i) — the consented governed-tag apply.

POST /api/ontology/apply/preview: dry-run, returns ApplyPlan with statements + diff + plan_hash.
POST /api/ontology/apply/execute: execute the plan under OBO, audit to genie_ont_applied,
                                   flip consent approved→applied, return applied/failed/blocked.

Phase-5 apply is the ONLY governed-tag writer in the product: it applies only consented
decisions from the ledger, under OBO, and audits each action. The batch materializer never
writes tags. This router is pre-seeded and registered in main.py; the Phase-5 lane fills it.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from backend.ontology import models
from backend.ontology.services import apply as apply_service
from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


@router.post("/apply/preview")
async def preview_apply() -> dict:
    """Dry-run apply: build plan from approved consents, return statements + diff + plan_hash.
    Writes nothing. OBO (uses viewer's identity to resolve metastore scope, but apply itself
    is still dry-run)."""
    try:
        client = get_workspace_client()
        # TODO: resolve metastore_id from scope or workspace context
        metastore_id = "default_metastore"  # placeholder

        plan = await apply_service.build_apply_plan(metastore_id)
        return plan.model_dump(mode="json")
    except Exception as e:
        logger.exception("preview_apply failed: %s", e)
        # Degrade-not-hang: return empty plan.
        empty_plan = models.ApplyPlan(
            items=[],
            executable_count=0,
            blocked_count=0,
            plan_hash="",
            source="cold",
            as_of="",
        )
        return empty_plan.model_dump(mode="json")


@router.post("/apply/execute")
async def execute_apply(req: models.ApplyExecuteRequest) -> dict:
    """Execute the consented apply: run statements under OBO, audit to genie_ont_applied,
    flip consent approved→applied. plan_hash + confirm=true gate. Per-statement fail-soft.
    """
    if not req.confirm:
        raise HTTPException(status_code=400, detail="confirm must be true")

    try:
        client = get_workspace_client()
        # TODO: resolve metastore_id, workspace_id, applied_by email
        metastore_id = "default_metastore"
        workspace_id = "default_workspace"
        applied_by = "user@example.com"

        # Rebuild plan to verify plan_hash (consent gate).
        plan = await apply_service.build_apply_plan(metastore_id)
        if plan.plan_hash != req.plan_hash:
            raise HTTPException(
                status_code=409,
                detail=f"plan_hash mismatch: expected {req.plan_hash}, got {plan.plan_hash}",
            )

        # Execute the plan.
        result = await apply_service.execute_apply_plan(
            plan=plan,
            metastore_id=metastore_id,
            workspace_id=workspace_id,
            applied_by=applied_by,
        )
        return result.model_dump(mode="json")
    except Exception as e:
        logger.exception("execute_apply failed: %s", e)
        # Degrade-not-hang: return empty result.
        empty_result = models.ApplyResult(as_of="")
        return empty_result.model_dump(mode="json")
