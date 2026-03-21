"""Service-principal permission helpers.

Lightweight module for SP identity resolution and UC privilege probing.
Importable without the GSO framework core (no ``_metadata`` dependency),
so the main Genie Workbench app can use these from ``auto_optimize.py``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import Privilege, SecurableType

logger = logging.getLogger(__name__)

_ALL_PRIV = Privilege.ALL_PRIVILEGES


# ── SP identity helpers ──────────────────────────────────────────────


def _get_sp_client_id(ws: WorkspaceClient) -> str:
    """Return the SP's client_id from config or environment."""
    return ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")


def get_sp_principal_aliases(sp_ws: WorkspaceClient) -> set[str]:
    """Return known principal aliases for the app service principal."""
    aliases: set[str] = set()
    cid = _get_sp_client_id(sp_ws)
    if cid:
        aliases.add(cid.lower())
    try:
        me = sp_ws.current_user.me()
        for attr in ("user_name", "display_name", "application_id", "id"):
            value = getattr(me, attr, None)
            if value:
                aliases.add(str(value).lower())
    except Exception:
        logger.debug("Could not resolve SP aliases from current_user.me()", exc_info=True)
    return aliases


# ── UC privilege probing ─────────────────────────────────────────────


def _effective_privileges_for_principal(
    privilege_assignments: Any,
    principal_aliases: set[str],
) -> set[Privilege]:
    """Extract effective privileges for a specific principal from assignment rows."""
    effective: set[Privilege] = set()
    if not privilege_assignments:
        return effective
    for assignment in privilege_assignments:
        principal = str(getattr(assignment, "principal", "") or "").lower().strip()
        if not principal or principal not in principal_aliases:
            continue
        for granted in getattr(assignment, "privileges", None) or []:
            priv = getattr(granted, "privilege", None)
            if priv:
                effective.add(priv)
    return effective


def probe_sp_required_access(
    sp_ws: WorkspaceClient,
    schemas: set[tuple[str, str]],
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """Return schemas where the SP has read / write privileges.

    Returns ``(read_granted, write_granted)`` — sets of ``(catalog, schema)``
    pairs where the SP has the corresponding access level.
    """
    if not schemas:
        return set(), set()

    by_catalog: dict[str, list[str]] = {}
    for cat, sch in schemas:
        by_catalog.setdefault(cat, []).append(sch)

    aliases = get_sp_principal_aliases(sp_ws)
    logger.info("SP aliases for privilege probing: %s", aliases)
    read_granted: set[tuple[str, str]] = set()
    write_granted: set[tuple[str, str]] = set()

    for cat, schema_list in by_catalog.items():
        catalog_privs: set[Privilege] = set()
        try:
            cat_eff = sp_ws.grants.get_effective(
                securable_type=SecurableType.CATALOG.value,
                full_name=cat,
            )
            catalog_privs = _effective_privileges_for_principal(
                cat_eff.privilege_assignments,
                aliases,
            )
            if catalog_privs:
                logger.info("SP catalog-level privs on %s: %s", cat, catalog_privs)
        except Exception:
            logger.debug("Could not read effective grants for catalog %s", cat, exc_info=True)

        for sch in schema_list:
            schema_privs: set[Privilege] = set()
            try:
                sch_eff = sp_ws.grants.get_effective(
                    securable_type=SecurableType.SCHEMA.value,
                    full_name=f"{cat}.{sch}",
                )
                schema_privs = _effective_privileges_for_principal(
                    sch_eff.privilege_assignments,
                    aliases,
                )
                logger.info(
                    "SP schema-level privs on %s.%s: %s",
                    cat, sch, schema_privs,
                )
            except Exception:
                logger.debug(
                    "Could not read effective grants for schema %s.%s",
                    cat, sch, exc_info=True,
                )

            all_privs = catalog_privs | schema_privs

            has_manage = (Privilege.MANAGE in all_privs) or (_ALL_PRIV in all_privs)
            has_catalog_access = (Privilege.USE_CATALOG in all_privs) or has_manage
            has_schema_access = (Privilege.USE_SCHEMA in all_privs) or has_manage
            has_select = (Privilege.SELECT in all_privs) or has_manage
            has_modify = (Privilege.MODIFY in all_privs) or has_manage

            key = (cat.lower(), sch.lower())
            if has_catalog_access and has_schema_access and has_select:
                read_granted.add(key)
            if has_modify:
                write_granted.add(key)

    read_granted |= write_granted

    return read_granted, write_granted
