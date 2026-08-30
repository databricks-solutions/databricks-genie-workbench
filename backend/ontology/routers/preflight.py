"""Ontology preflight + health.

`GET /api/ontology/preflight` resolves the five permission tiers (MV-D37/D38
capability→permission matrix) and drives frame 17.0a. Phase 1 exercises tiers
1–3; tiers 4–5 are informational (``not_exercised``). It never raises — a blocked
tag_graph tier degrades the page to the banner's grant CTA, not an error
(degrade-not-hang, MV-D43).

`GET /api/ontology/health` mirrors `backend/watch/routers/settings.py::health`.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.ontology.models import OntologyPreflight, PermissionTier
from backend.ontology.services import ont_settings, tag_graph
from backend.services import lakebase
from backend.services.auth import get_databricks_host
from backend.watch.services import system_tables

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")

# Copy-ready grant / entitlement lines surfaced by the banner (frame 17.0a).
_SIGNALS_GRANTS = [
    "GRANT USE CATALOG ON CATALOG system TO `<app-service-principal>`",
    "GRANT SELECT ON TABLE system.query.history TO `<app-service-principal>`",
    "GRANT SELECT ON TABLE system.billing.usage TO `<app-service-principal>`",
    "GRANT SELECT ON TABLE system.access.table_lineage TO `<app-service-principal>`",
    "GRANT SELECT ON TABLE system.access.audit TO `<app-service-principal>`",
]
_TAG_GRAPH_GRANTS = [
    "GRANT USE CATALOG ON CATALOG system TO `<app-service-principal>`",
    "GRANT USE SCHEMA ON SCHEMA system.tags TO `<app-service-principal>`",
    "GRANT SELECT ON TABLE system.tags.governed_tags TO `<app-service-principal>`",
]
# Informational only — Phase 1 never exercises these (write / enrichment tiers).
_MEMBERSHIP_WRITE_GRANTS = [
    "MANAGE DISCOVERY + ASSIGN on each governed tag",
    "APPLY TAG / USE SCHEMA / USE CATALOG on target assets (OBO)",
]
_ENRICHMENT_GRANTS = [
    "EXECUTE on the enabled Unity AI Gateway MCP services (opt-in, default OFF)",
]


@router.get("/preflight")
async def preflight() -> dict:
    settings = await ont_settings.get_settings()

    # Tier probes are cheap SP round-trips; run them off the event loop.
    signals_status = system_tables.system_tables_status()
    tag_ok = await asyncio.to_thread(tag_graph.probe)

    empty_scope = not settings.catalog_allowlist

    inventory_tier = PermissionTier(
        id="inventory",
        label="Metric-view + tag inventory",
        identity="obo",
        status="ok",
        grants=[],
        reason=(
            "No catalogs selected yet — choose catalogs in Settings to scope the ontology."
            if empty_scope
            else None
        ),
    )
    # signals only weights ranking (not exercised in Phase 1's read-only spine),
    # so it is informational: ok unless GenieWatch has already observed a denial.
    signals_tier = PermissionTier(
        id="signals",
        label="Usage / lineage / cost ranking",
        identity="sp",
        status="degraded" if signals_status is False else "ok",
        grants=_SIGNALS_GRANTS,
        reason=(
            "System-table grants missing — the page still renders; ranking would be weaker."
            if signals_status is False
            else None
        ),
    )
    tag_tier = PermissionTier(
        id="tag_graph",
        label="Governed-tag graph (dedupe)",
        identity="sp",
        status="ok" if tag_ok else "blocked",
        grants=_TAG_GRAPH_GRANTS,
        reason=(
            None
            if tag_ok
            else "Grant SELECT on system.tags.governed_tags to the app service principal to render the taxonomy and tags lens."
        ),
    )
    membership_tier = PermissionTier(
        id="membership_write",
        label="Membership write (optional apply)",
        identity="obo",
        status="not_exercised",
        grants=_MEMBERSHIP_WRITE_GRANTS,
        reason="Not used in Phase 1 — Ontology is read-only; nothing is written to Unity Catalog.",
    )
    enrichment_tier = PermissionTier(
        id="external_enrichment",
        label="Context sources (external enrichment)",
        identity="batch",
        status="not_exercised",
        grants=_ENRICHMENT_GRANTS,
        reason="Not used in Phase 1 — no external context / web-search path exists yet.",
    )

    return OntologyPreflight(
        tiers=[inventory_tier, signals_tier, tag_tier, membership_tier, enrichment_tier],
        can_render_taxonomy=tag_ok,
        company_name=settings.company_name,
        catalog_allowlist=settings.catalog_allowlist,
        as_of=datetime.now(timezone.utc).isoformat(),
    ).model_dump(mode="json")


@router.get("/health")
async def health() -> dict:
    try:
        host = get_databricks_host()
    except Exception:
        host = None
    return {
        "lakebase_available": lakebase.is_available(),
        "warehouse_id": os.environ.get("SQL_WAREHOUSE_ID"),
        "workspace_host": host,
        "tag_graph_accessible": tag_graph.tag_graph_status(),
        "system_tables_accessible": system_tables.system_tables_status(),
    }
