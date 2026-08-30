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
from backend.ontology.services import grants, inventory, ont_settings, tag_graph
from backend.services import lakebase
from backend.services.auth import get_databricks_host, get_workspace_client
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


def _with_sp(lines: list[str], sp: str | None) -> list[str]:
    """Substitute the resolved app SP into the ``<app-service-principal>`` placeholder
    so every grant line in the banner is genuinely copy-paste ready."""
    if not sp:
        return lines
    return [ln.replace("<app-service-principal>", sp) for ln in lines]


@router.get("/preflight")
async def preflight() -> dict:
    settings = await ont_settings.get_settings()

    # Tier probes are cheap SP round-trips; run them off the event loop.
    signals_status = system_tables.system_tables_status()
    tag_ok = await asyncio.to_thread(tag_graph.probe)
    sp_id = grants.app_service_principal()

    empty_scope = not settings.catalog_allowlist

    # BROWSE differential (MV-D42): the taxonomy tree comes from the account-level
    # governed-tag catalog, but member counts come from privilege-filtered
    # information_schema. If the SP sees zero assignments in scope while the admin
    # (OBO) sees some, the SP is missing BROWSE — surface the exact grant instead of
    # a silent "0 members". Both probes are best-effort and never block the page.
    browse_needed = False
    browse_grants: list[str] = []
    if tag_ok and not empty_scope:
        sp_seen = await asyncio.to_thread(tag_graph.sp_assignment_count, settings.catalog_allowlist)
        obo_seen = 0
        if sp_seen == 0:
            try:
                obo_seen = await asyncio.to_thread(
                    inventory.governed_tag_count, get_workspace_client(), settings.catalog_allowlist
                )
            except Exception as e:  # noqa: BLE001 — preflight never raises
                logger.info("OBO governed-tag probe failed: %s", e)
        browse_needed = grants.browse_needed(
            tag_ok=tag_ok,
            allowlist=settings.catalog_allowlist,
            sp_seen=sp_seen,
            obo_seen=obo_seen,
        )
        if browse_needed:
            browse_grants = [
                grants.browse_grant_line(c, sp_id) for c in settings.catalog_allowlist
            ]

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
        grants=_with_sp(_SIGNALS_GRANTS, sp_id),
        reason=(
            "System-table grants missing — the page still renders; ranking would be weaker."
            if signals_status is False
            else None
        ),
    )
    if not tag_ok:
        tag_status = "blocked"
        tag_grants = _with_sp(_TAG_GRAPH_GRANTS, sp_id)
        tag_reason = (
            "Grant SELECT on system.tags.governed_tags to the app service principal "
            "to render the taxonomy and tags lens."
        )
    elif browse_needed:
        # Tree renders, but members read 0 — the actionable, common case.
        tag_status = "degraded"
        tag_grants = browse_grants
        tag_reason = (
            "Domains render, but members show 0 because the app service principal "
            "cannot see governed-tag assignments in the selected catalogs. Grant "
            "BROWSE (metadata-only — no data access) so member counts populate."
        )
    else:
        tag_status = "ok"
        tag_grants = _with_sp(_TAG_GRAPH_GRANTS, sp_id)
        tag_reason = None
    tag_tier = PermissionTier(
        id="tag_graph",
        label="Governed-tag graph (dedupe)",
        identity="sp",
        status=tag_status,
        grants=tag_grants,
        reason=tag_reason,
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
