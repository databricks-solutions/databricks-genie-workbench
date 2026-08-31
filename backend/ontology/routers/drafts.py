"""Ontology drafts — serve the ranked proposals + record human decisions (Phase 3d).

`GET /api/ontology/drafts` serves the L6-ranked, surfaced, tiered Domain / Sub-Domain
+ Page drafts (including 17e ``reassign`` rows) from the mirror, ordered HIGH → LOW,
with the zero-burden ``why``/``reason`` + evidence chips assembled server-side
(MV-D23). Mirror-first, degrade-not-hang: not-fresh / empty / failed → a typed empty
``OntologyDrafts(source="cold")``, never a 500.

`POST /api/ontology/decision` records the human adjudication under **OBO** in the
consent / suppression ledger (MV-D26) — ``approve`` / accepted ``reassign`` → consent;
``dismiss`` / rejected ``reassign`` → suppression. It writes app-state Delta rows via
the SQL warehouse; it is the **only** new POST and the **only** new router. It applies
**nothing** to Unity Catalog — no tag apply, no apply route (that is 17i).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request

from backend.ontology.models import (
    DecisionRequest,
    DecisionResponse,
    DomainDraft,
    OntologyDrafts,
    PageDraft,
)
from backend.ontology.services import decisions, mirror, ont_settings, refresh
from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cold() -> dict:
    """The typed empty payload (MV-D43 degrade) — never a 500, never a hang."""
    return OntologyDrafts(domains=[], pages=[], source="cold", as_of=_now()).model_dump(mode="json")


@router.get("/drafts")
async def get_drafts() -> dict:
    ms = ont_settings._metastore_id()
    try:
        # Drafts are materialized by the batch job — there is no live fallback (unlike
        # taxonomy). If the mirror is not fresh, serve an honest cold payload.
        if not await refresh.mirror_is_fresh(ms):
            return _cold()
        domains, pages, run = await asyncio.gather(
            mirror.read_domain_drafts(ms),
            mirror.read_page_drafts(ms),
            mirror.latest_succeeded_run(ms),
        )
        as_of = str((run or {}).get("as_of") or _now())
        return OntologyDrafts(
            domains=[DomainDraft(**d) for d in domains],
            pages=[PageDraft(**p) for p in pages],
            source="mirror",
            as_of=as_of,
        ).model_dump(mode="json")
    except Exception:  # noqa: BLE001 — degrade-not-hang: a typed empty payload, never 500
        logger.info("ontology drafts read failed; serving cold payload", exc_info=True)
        return _cold()


def _obo_email(request: Request) -> str:
    """The deciding human's email (OBO attribution, MV-D50). Prefers the Apps
    forwarded-identity headers (cheap, no round trip); falls back to the OBO SDK
    identity; ``"unknown"`` only if both are unavailable."""
    for header in ("x-forwarded-email", "x-forwarded-preferred-username"):
        value = request.headers.get(header)
        if value and value.strip():
            return value.strip()
    try:
        return (get_workspace_client().current_user.me().user_name or "").strip() or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


@router.post("/decision")
async def post_decision(req: DecisionRequest, request: Request) -> dict:
    """Record one human decision in the ledger under OBO (idempotent). No UC write."""
    ms = ont_settings._metastore_id()
    ws = ont_settings._workspace_id()
    decided_by = _obo_email(request)
    recorded = await asyncio.to_thread(
        decisions.record_decision,
        kind=req.kind,
        proposal_id=req.proposal_id,
        action=req.action,
        metastore_id=ms,
        workspace_id=ws,
        decided_by=decided_by,
    )
    return DecisionResponse(ok=True, recorded=recorded, as_of=decisions.now_iso()).model_dump(mode="json")
