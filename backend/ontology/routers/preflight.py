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

from backend.ontology.models import ExternalContext, OntologyPreflight, PermissionTier, SourceStatus
from backend.ontology.services import context_sources, grants, inventory, ont_settings, tag_graph
from backend.services import lakebase
from backend.services.auth import (
    get_databricks_host,
    get_service_principal_client,
    get_workspace_client,
)
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


# The two foundation reads (signals + tag_graph) default to OBO (the viewing
# admin, MV-D50). The banner frames the SP grants below as an OPTIONAL UPGRADE —
# a shared cross-user cache / consumer-safe serving — not a prerequisite to view.
_OPTIONAL_UPGRADE = (
    "These service-principal grants are an optional upgrade (a shared cross-user "
    "cache / consumer-safe serving) — not required to view. The taxonomy renders "
    "as the signed-in admin (OBO)."
)


def _probe_client():
    """The app service principal client used to probe EXECUTE on the enabled MCP
    sources (the enrichment tier runs at batch identity, MV-D50). Degrades to ``None``
    off-platform / on any resolution failure — the probe then reports ``unavailable``."""
    try:
        return get_service_principal_client()
    except Exception as e:  # noqa: BLE001 — preflight never raises (MV-D43)
        logger.info("enrichment tier: no SP client for EXECUTE probe: %s", e)
        return None


def _build_enrichment_tier(ext: ExternalContext, sp_id: str | None) -> PermissionTier:
    """Make tier-5 real (Stage A, 17h). DEFAULT OFF (MV-D44): when ``ext.enabled`` is
    false the tier is a plain disabled reason with NO probing and NO egress — the
    engine stays byte-identical estate-only. When enabled, probe each enabled source
    for EXECUTE and report per-source ``{class, tier, influence, execute_status}`` + a
    copy-ready GRANT EXECUTE when missing. Never raises (MV-D43)."""
    label = "Context sources (external enrichment)"
    if not ext.enabled:
        return PermissionTier(
            id="external_enrichment",
            label=label,
            identity="batch",
            status="not_exercised",
            grants=_with_sp(_ENRICHMENT_GRANTS, sp_id),
            reason=(
                "External context is off (the default) — the engine runs estate-only. "
                "Turn it on in Settings to enrich naming with opt-in context sources."
            ),
            sources=[],
        )

    client = _probe_client()
    statuses: list[SourceStatus] = []
    for entry in context_sources.CONTEXT_SOURCES:
        if not ext.sources.get(entry.id, entry.default_enabled):
            continue
        try:
            statuses.append(context_sources.probe_source(entry, client, sp=sp_id))
        except Exception as e:  # noqa: BLE001 — a probe never blocks the banner
            logger.info("enrichment probe for %s failed: %s", entry.id, e)
            statuses.append(
                SourceStatus(
                    id=entry.id, label=entry.label, klass=entry.klass,
                    provenance_tier=entry.provenance_tier, influence=entry.influence,
                    execute_status="unavailable",
                    grant_line=context_sources.grant_execute_line(entry, sp_id),
                    reason="This source could not be probed.",
                )
            )

    if not statuses:
        status: str = "not_exercised"
        reason: str | None = (
            "External context is on, but no context sources are enabled. "
            "Enable a source in Settings."
        )
    elif any(s.execute_status == "ok" for s in statuses):
        status = "ok"
        reason = None
    else:
        status = "degraded"
        reason = (
            "External context needs a source the app can EXECUTE; none is available "
            "in this workspace. Run the copy-ready GRANT EXECUTE below, or the engine "
            "stays estate-only."
        )
    grant_lines = [s.grant_line for s in statuses if s.grant_line]
    return PermissionTier(
        id="external_enrichment",
        label=label,
        identity="batch",
        status=status,  # type: ignore[arg-type]
        grants=grant_lines or _with_sp(_ENRICHMENT_GRANTS, sp_id),
        reason=reason,
        sources=statuses,
    )


@router.get("/preflight")
async def preflight() -> dict:
    settings = await ont_settings.get_settings()
    read_identity = settings.read_identity

    # Resolve the active read identity for the tier probes (MV-D50). "auto" reads
    # as the SP only when the SP probe succeeds (a cheap SP round-trip); otherwise
    # the reads run as the viewing admin (OBO) — the default.
    sp_probe_ok = False
    if read_identity == "auto":
        sp_probe_ok = await asyncio.to_thread(tag_graph.probe, "sp")
    active_is_sp = read_identity == "sp" or (read_identity == "auto" and sp_probe_ok)

    # Tier probes are cheap round-trips; run them off the event loop.
    signals_status = system_tables.system_tables_status()
    tag_ok = await asyncio.to_thread(
        tag_graph.probe, read_identity, sp_probe_ok=sp_probe_ok
    )
    sp_id = grants.app_service_principal()

    empty_scope = not settings.catalog_allowlist

    # BROWSE differential (MV-D42) — only meaningful when the SP is the *active*
    # read identity (the opt-in SP upgrade). Under the OBO default the graph is read
    # as the admin, so there is no "SP is blind" gap to detect and the SP is never
    # touched here. The taxonomy tree comes from the account-level governed-tag
    # catalog, but member counts come from privilege-filtered information_schema; if
    # the SP sees zero assignments while the admin (OBO) sees some, the SP is missing
    # BROWSE — surface the exact grant instead of a silent "0 members". Best-effort;
    # never blocks the page.
    browse_needed = False
    browse_grants: list[str] = []
    if tag_ok and not empty_scope and active_is_sp:
        sp_seen = await asyncio.to_thread(
            tag_graph.sp_assignment_count, settings.catalog_allowlist, "sp"
        )
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
    # signals reads as the viewing admin by default (MV-D50); the SP system-table
    # grants below are an optional upgrade (shared cache / stronger ranking), never
    # required to view. It never gates rendering (MV-D44).
    signals_tier = PermissionTier(
        id="signals",
        label="Usage / lineage / cost ranking",
        identity="sp",
        status="degraded" if signals_status is False else "ok",
        grants=_with_sp(_SIGNALS_GRANTS, sp_id),
        reason=(
            "The page still renders and ranking is best-effort. " + _OPTIONAL_UPGRADE
            if signals_status is False
            else None
        ),
    )
    # The tag_graph grant lines are ALWAYS surfaced as the optional SP upgrade
    # (shown on every state, incl. ok, so an admin can copy them to enable the
    # shared cache). The taxonomy itself renders as the viewing admin (OBO).
    tag_grants = _with_sp(_TAG_GRAPH_GRANTS, sp_id)
    if not tag_ok:
        tag_status = "blocked"
        tag_reason = (
            "The signed-in identity can't read governed tags — a metastore-complete "
            "ontology needs an account/metastore admin (as the OBO viewer or the "
            "batch run_as identity). " + _OPTIONAL_UPGRADE
        )
    elif browse_needed:
        # Tree renders, but members read 0 under the SP upgrade — the actionable case.
        tag_status = "degraded"
        tag_grants = browse_grants
        tag_reason = (
            "Domains render, but members show 0 because the app service principal "
            "cannot see governed-tag assignments in the selected catalogs. Grant "
            "BROWSE (metadata-only — no data access) so member counts populate. This "
            "applies only to the optional SP read path; the OBO admin view is unaffected."
        )
    else:
        tag_status = "ok"
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
    # Tier-5 (Stage A, 17h): DEFAULT OFF ⇒ a plain disabled reason with no probing;
    # when enabled, the EXECUTE probes are cheap SDK round-trips run off the event loop.
    if settings.external_context.enabled:
        enrichment_tier = await asyncio.to_thread(
            _build_enrichment_tier, settings.external_context, sp_id
        )
    else:
        enrichment_tier = _build_enrichment_tier(settings.external_context, sp_id)

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
