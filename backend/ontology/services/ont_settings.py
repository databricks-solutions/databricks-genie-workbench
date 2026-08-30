"""Ontology settings: company name + catalog allowlist (MV-D42).

Durable via the app's existing Lakebase (`backend/services/lakebase.py`,
`genie.genie_ont_settings`), with the in-memory fallback when ``LAKEBASE_HOST``
is unset. Keyed by workspace id (one row per workspace/app instance) — resolved
once via the SP client, mirroring `watch/services/system_tables._current_workspace_id`.

The allowlist scopes every downstream reader (inventory, tag graph, taxonomy).
An empty allowlist is meaningful: the page prompts the admin to choose catalogs
rather than scanning the whole account (MV-D42).
"""

from __future__ import annotations

import logging

from backend.ontology.models import OntologySettings
from backend.services import lakebase
from backend.services.auth import get_service_principal_client

logger = logging.getLogger(__name__)

_WS_ID: str | None = None
_WS_ID_RESOLVED = False


def _workspace_id() -> str:
    """This app's workspace id, resolved once via the SDK (cached).

    Falls back to the literal ``"default"`` if resolution fails so a single
    settings row is still readable/writable (the app is per-workspace anyway).
    """
    global _WS_ID, _WS_ID_RESOLVED
    if not _WS_ID_RESOLVED:
        _WS_ID_RESOLVED = True
        try:
            _WS_ID = str(get_service_principal_client().get_workspace_id())
        except Exception as e:  # noqa: BLE001 — never break settings on this
            logger.warning("could not resolve workspace id for ontology settings: %s", e)
            _WS_ID = None
    return _WS_ID or "default"


async def get_settings() -> OntologySettings:
    """Read the stored company name + catalog allowlist (defaults if unset)."""
    row = await lakebase.ont_get_settings(_workspace_id())
    if not row:
        return OntologySettings()
    return OntologySettings(
        company_name=row.get("company_name"),
        catalog_allowlist=list(row.get("catalog_allowlist") or []),
    )


async def save_settings(settings: OntologySettings) -> OntologySettings:
    """Persist the company name + catalog allowlist. Returns the stored value."""
    company = (settings.company_name or "").strip() or None
    # Normalize: drop blanks, dedupe, preserve order.
    seen: set[str] = set()
    allowlist: list[str] = []
    for c in settings.catalog_allowlist:
        name = (c or "").strip()
        if name and name not in seen:
            seen.add(name)
            allowlist.append(name)
    await lakebase.ont_upsert_settings(_workspace_id(), company, allowlist)
    return OntologySettings(company_name=company, catalog_allowlist=allowlist)
