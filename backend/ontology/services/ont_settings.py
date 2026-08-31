"""Ontology settings: company name + catalog allowlist (MV-D42).

Durable via the app's existing Lakebase (`backend/services/lakebase.py`,
`genie.genie_ont_settings`), with the in-memory fallback when ``LAKEBASE_HOST``
is unset. Settings are keyed by workspace id (one config row per workspace/app
instance) — resolved once via the SP client, mirroring
`watch/services/system_tables._current_workspace_id`.

The ontology snapshots themselves are keyed by **metastore** (MV-D49): the
mirror reads scope by :func:`_metastore_id` (the app's metastore, resolved once
and cached), so a per-workspace app reads the one metastore ontology.
``workspace_id`` stays the settings key (per-install config) and rides the
materialized rows as provenance only.

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
_METASTORE_ID: str | None = None
_METASTORE_ID_RESOLVED = False


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


def _metastore_id() -> str:
    """This app's metastore id — the ontology grain (MV-D49), resolved once (cached).

    The app runs in one workspace → one metastore, so this is resolved once via
    the SP client's ``metastores.current()`` and threaded as the mirror scope.
    Degrades to the stable literal ``"default"`` if resolution fails (MV-D43), so
    a missing metastore id never blocks a read — it just scopes to a stable id.
    """
    global _METASTORE_ID, _METASTORE_ID_RESOLVED
    if not _METASTORE_ID_RESOLVED:
        _METASTORE_ID_RESOLVED = True
        try:
            current = get_service_principal_client().metastores.current()
            _METASTORE_ID = str(current.metastore_id) if current and current.metastore_id else None
        except Exception as e:  # noqa: BLE001 — degrade-not-hang (MV-D43)
            logger.warning("could not resolve metastore id for ontology mirror: %s", e)
            _METASTORE_ID = None
    return _METASTORE_ID or "default"


async def get_settings() -> OntologySettings:
    """Read the stored company name + catalog allowlist (defaults if unset)."""
    row = await lakebase.ont_get_settings(_workspace_id())
    if not row:
        return OntologySettings()
    # read_identity is additive/defaulted (MV-D50): an old row without the field
    # (or a NULL) falls through to the model default "obo".
    return OntologySettings(
        company_name=row.get("company_name"),
        catalog_allowlist=list(row.get("catalog_allowlist") or []),
        read_identity=row.get("read_identity") or "obo",
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
    read_identity = settings.read_identity or "obo"
    await lakebase.ont_upsert_settings(_workspace_id(), company, allowlist, read_identity)
    # NOTE: we deliberately do NOT auto-grant BROWSE to the app SP here. The app's
    # OBO token is scoped read-only for Unity Catalog (catalog.*:read + sql, no
    # UC-write scope), so the REST permissions API is blocked under OBO — an
    # in-app grant would silently fail. The preflight banner instead surfaces a
    # copy-ready GRANT BROWSE for an admin to run. See services/grants.py.
    return OntologySettings(
        company_name=company, catalog_allowlist=allowlist, read_identity=read_identity
    )
