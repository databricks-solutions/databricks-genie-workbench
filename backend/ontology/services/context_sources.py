"""Context Sources registry + firewall-by-class + capability probe (Phase 4).

The **safe backbone** of the external-context tier (MV-D38/D44/D46/D47) — with **no
egress**. Stage A shipped the registry + firewall + probe here; Stage B single-sourced
the two *pure* pieces into the wheel so the batch resolver
(``genie_space_optimizer.ontology.context_pack``) and this backend tier share ONE
definition (no drift between the banner and the batch):

1. The **registry** (:data:`CONTEXT_SOURCES`, :class:`ContextSource`, :func:`get_source`,
   :func:`grant_execute_line`, :data:`EXCLUDED_SOURCE_IDS`) now lives in
   ``genie_space_optimizer.ontology.context_registry`` and is re-exported here so every
   backend caller and test keeps importing it from this module unchanged.
2. The **firewall-by-class** (:func:`influence_allows`, :data:`FORBIDDEN_TARGETS`, MV-D38)
   now lives in ``genie_space_optimizer.ontology.context_firewall`` — external / overlay
   MCPs may steer ONLY naming / description / synonym / gap_hypothesis / recent_context;
   internal UC-backed MCPs may additionally reach structural_signal / validation; NONE may
   ever reach a membership / measure / certification writer ("FORBIDDEN checked first").
3. The **capability probe** (:func:`probe_source`, MV-D43/D45) stays HERE (it needs the
   backend ``WorkspaceClient`` + grants + the ``SourceStatus`` Pydantic model): it resolves
   EXECUTE on the source's securable for the app service principal and *degrades* — a source
   with no EXECUTE is ``unavailable`` / ``missing`` with a copy-ready ``GRANT EXECUTE`` line —
   and **never raises**.

``system.ai.web_search`` appears in the (re-exported) registry as a securable name, not an
egress call; the actual egress path is the wheel's ``web_search`` / ``context_pack`` modules,
firewalled out of every *other* ontology module (``test_ontology_firewall.py``).
"""

from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import Privilege, SecurableType

from genie_space_optimizer.common.sp_permissions import (
    _effective_privileges_for_principal,
    get_sp_principal_aliases,
)
from genie_space_optimizer.ontology.context_firewall import (
    FORBIDDEN_TARGETS,
    influence_allows,
)
from genie_space_optimizer.ontology.context_registry import (
    CONTEXT_SOURCES,
    EXCLUDED_SOURCE_IDS,
    ContextSource,
    get_source,
    grant_execute_line,
)

from backend.ontology.models import ExecuteStatus, SourceStatus
from backend.ontology.services.grants import app_service_principal

logger = logging.getLogger(__name__)

# Re-exported so ``from ... import context_sources as cs`` keeps every Stage-A name
# reachable on this module (the registry + firewall now live in the wheel, MV-D38/D47).
__all__ = [
    "CONTEXT_SOURCES",
    "ContextSource",
    "EXCLUDED_SOURCE_IDS",
    "FORBIDDEN_TARGETS",
    "get_source",
    "grant_execute_line",
    "influence_allows",
    "probe_source",
]


def _securable_type(entry: ContextSource) -> str | None:
    """Best-effort securable type for the EXECUTE probe, or ``None`` when the source
    is not a resolvable UC securable (a workspace MCP path). The AI-Gateway MCP
    services are dotted UC names resolved as functions; internal MCP *endpoints*
    (``/api/2.0/mcp/*``) are not grants-API securables — Stage B's resolver owns their
    reachability, so here they degrade to ``unavailable`` rather than pretend."""
    if entry.mcp_fqn.startswith("/") or "." not in entry.mcp_fqn:
        return None
    return SecurableType.FUNCTION.value


def _resolve_execute(entry: ContextSource, client: WorkspaceClient | None) -> tuple[ExecuteStatus, str]:
    """Resolve EXECUTE for the app SP on ``entry``'s securable. NEVER raises (MV-D43):
    returns ``(status, plain_reason)`` — degrading to ``unavailable`` on any failure."""
    if client is None:
        return "unavailable", "No workspace client is available to resolve EXECUTE."
    securable = _securable_type(entry)
    if securable is None:
        return (
            "unavailable",
            "This source has no resolvable EXECUTE securable yet; it activates with the resolver.",
        )
    try:
        aliases = get_sp_principal_aliases(client)
        eff = client.grants.get_effective(securable_type=securable, full_name=entry.mcp_fqn)
        privs = _effective_privileges_for_principal(
            getattr(eff, "privilege_assignments", None), aliases
        )
    except Exception as e:  # noqa: BLE001 — probe-and-degrade, never raise
        msg = str(e).lower()
        if any(
            s in msg
            for s in ("permission denied", "does not have", "not authorized", "access denied")
        ):
            return "blocked", "Resolving EXECUTE was denied for the app service principal."
        logger.info("EXECUTE probe for %s degraded: %s", entry.id, e)
        return "unavailable", "This source's EXECUTE grant could not be resolved."
    if Privilege.EXECUTE in privs or Privilege.ALL_PRIVILEGES in privs:
        return "ok", ""
    return "missing", "The app service principal has no EXECUTE grant on this source."


def probe_source(
    entry: ContextSource, client: WorkspaceClient | None, *, sp: str | None = None
) -> SourceStatus:
    """Probe one source's capability and return its :class:`SourceStatus` (MV-D43/D45).

    Resolves EXECUTE for the app service principal; a non-``ok`` outcome carries a
    copy-ready ``GRANT EXECUTE`` line. Never raises — a source that cannot be resolved
    degrades to ``unavailable`` with a plain reason, and the engine stays estate-only."""
    sp = sp if sp is not None else app_service_principal()
    status, reason = _resolve_execute(entry, client)
    grant = None if status == "ok" else grant_execute_line(entry, sp)
    return SourceStatus(
        id=entry.id,
        label=entry.label,
        klass=entry.klass,
        provenance_tier=entry.provenance_tier,
        influence=entry.influence,
        execute_status=status,
        grant_line=grant,
        reason=reason,
    )
