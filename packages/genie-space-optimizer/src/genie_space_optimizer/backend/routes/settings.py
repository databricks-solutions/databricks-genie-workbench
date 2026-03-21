"""Settings endpoints: advisor-only permission dashboard.

The app is an **advisor**, not an executor.  It reads UC metadata and
Genie Space permissions using OBO read scopes, shows the user exactly
what's granted and what's missing, and provides copyable SQL / sharing
instructions.  It never attempts to execute GRANT/REVOKE or modify
Genie Space ACLs on the user's behalf.

Client convention
-----------------
- **Genie Space ACL reads:** OBO REST ``GET /api/2.0/permissions/genie/{id}``,
  SP REST fallback.
- **Genie data APIs** (list spaces, fetch config): OBO first, SP fallback.
- **UC privilege probing** (``grants.get_effective``): SP (M2M — no scope issues).
"""

from __future__ import annotations

import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    Privilege,
    SecurableType,
)

from ..core import Dependencies, create_router
from ..models import (
    PermissionDashboard,
    SchemaPermission,
    SpacePermissions,
)
from ..utils import get_sp_principal as _get_sp_principal

router = create_router()
logger = logging.getLogger(__name__)

_ALL_PRIV = Privilege.ALL_PRIVILEGES


# ── SP identity helpers ──────────────────────────────────────────────


def _get_sp_display_name(ws: WorkspaceClient) -> str:
    """Return the app SP display name when available."""
    try:
        me = ws.current_user.me()
        for attr in ("display_name", "user_name"):
            value = getattr(me, attr, None)
            if value and str(value).strip():
                return str(value).strip()
    except Exception:
        logger.debug("Could not resolve service principal display name", exc_info=True)
    return ""


def _get_sp_application_id(ws: WorkspaceClient) -> str:
    """Return the SP's ``application_id`` — the principal name UC SQL accepts."""
    try:
        me = ws.current_user.me()
        app_id = getattr(me, "application_id", None)
        if app_id and str(app_id).strip():
            return str(app_id).strip()
    except Exception:
        logger.debug("Could not resolve SP application_id", exc_info=True)
    return ""


def get_sp_principal_aliases(sp_ws: WorkspaceClient) -> set[str]:
    """Return known principal aliases for the app service principal."""
    aliases: set[str] = set()
    aliases.add(_get_sp_principal(sp_ws).lower())
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
    privilege_assignments,
    principal_aliases: set[str],
) -> set[Privilege]:
    """Extract effective privileges for a specific principal from assignment rows.

    Only includes privileges where the ``principal`` field explicitly matches
    one of the provided aliases.  Entries with empty/missing principals are
    skipped to avoid accidentally counting inherited grants from other users.
    """
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


def _probe_sp_required_access(
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


# ── ACL helpers ──────────────────────────────────────────────────────


def _sp_has_manage_from_rest_acl(acl_response: dict, sp_aliases: set[str]) -> bool:
    """Extract SP CAN_MANAGE from a REST ACL dict."""
    from genie_space_optimizer.common.genie_client import _check_sp_manage_from_rest_acl
    return _check_sp_manage_from_rest_acl(acl_response, sp_aliases)


# ── Permission Dashboard (read-only, advisor) ────────────────────────


@router.get(
    "/settings/permissions",
    response_model=PermissionDashboard,
    operation_id="getPermissionDashboard",
)
def get_permission_dashboard(
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
    space_id: str | None = None,
    metadata_only: bool = False,
):
    """Per-space permission overview — detect status and advise on missing grants.

    When *metadata_only* is ``True``, return only SP identity and framework
    resource metadata with an empty ``spaces`` list (used for fast initial
    Settings page load).

    When *space_id* is supplied, only that single space is checked (fast path
    used by the detail page and lazy accordion expand).

    Without either parameter the full list of visible spaces is scanned.
    """
    sp_aliases = get_sp_principal_aliases(sp_ws)
    sp_display = _get_sp_display_name(sp_ws)
    sp_app_id = _get_sp_application_id(sp_ws)

    if metadata_only:
        sp_id = _get_sp_principal(sp_ws)
        from ..job_launcher import get_job_url
        job_url = get_job_url(sp_ws, job_id=config.job_id)
        host = (sp_ws.config.host or "").rstrip("/")
        workspace_id: int | str | None = None
        try:
            workspace_id = sp_ws.get_workspace_id()
        except Exception:
            workspace_id = None
        ws_host_with_o = f"{host}?o={workspace_id}" if host and workspace_id else host or None
        return PermissionDashboard(
            spaces=[],
            spPrincipalId=sp_id,
            spPrincipalDisplayName=sp_display or None,
            frameworkCatalog=config.catalog,
            frameworkSchema=config.schema_name,
            experimentBasePath="/Shared/genie-space-optimizer/",
            jobName="genie-space-optimizer-job",
            jobUrl=job_url,
            workspaceHost=ws_host_with_o,
        )

    from genie_space_optimizer.common.genie_client import (
        list_spaces, fetch_space_config, user_can_edit_space,
        get_space_permissions_rest,
    )
    from genie_space_optimizer.common.uc_metadata import (
        extract_genie_space_table_refs,
        get_unique_schemas,
    )

    caller_email = headers.user_email or headers.user_name or ""

    if space_id:
        # Fast path: only check permissions for one space.
        all_spaces = [{"id": space_id, "title": space_id}]
        spaces_source = "single"
    else:
        all_spaces = []
        spaces_source = ""
        for _label, client in [("OBO", ws), ("SP", sp_ws)]:
            try:
                all_spaces = list_spaces(client)
                spaces_source = _label
                logger.info("Listed %d spaces via %s client", len(all_spaces), _label)
                break
            except Exception:
                logger.info("list_spaces via %s failed, trying next", _label)

    _perm_cache: dict[str, dict | None] = {}

    def _cached_perms_rest(sid: str) -> dict | None:
        if sid not in _perm_cache:
            for client in [ws, sp_ws]:
                resp = get_space_permissions_rest(client, sid)
                if resp is not None:
                    _perm_cache[sid] = resp
                    break
            else:
                _perm_cache[sid] = None
        return _perm_cache[sid]

    listed_by_obo = spaces_source == "OBO"

    user_spaces = []
    for s in all_spaces:
        sid = s["id"]
        cached = _cached_perms_rest(sid)

        if space_id:
            # Single-space mode: always include (permission details shown in UI)
            user_spaces.append(s)
        elif cached is not None:
            can_edit = user_can_edit_space(
                ws, sid, user_email=caller_email, acl_client=sp_ws,
                cached_perms=cached,
            )
            if can_edit:
                user_spaces.append(s)
        elif listed_by_obo:
            user_spaces.append(s)

    all_schemas: set[tuple[str, str]] = set()
    space_schemas: dict[str, list[tuple[str, str]]] = {}
    for space in user_spaces:
        sid = space["id"]
        cfg: dict = {}
        for c_label, c in [("OBO", ws), ("SP", sp_ws)]:
            try:
                cfg = fetch_space_config(c, sid)
                if cfg.get("title"):
                    space["title"] = cfg["title"]
                break
            except Exception:
                continue
        try:
            refs = extract_genie_space_table_refs(cfg)
        except Exception:
            refs = []
        unique = get_unique_schemas(refs)
        normalized = [(c.lower(), s.lower()) for c, s in unique]
        for key in normalized:
            all_schemas.add(key)
        space_schemas[sid] = normalized

    sp_read_granted, sp_write_granted = _probe_sp_required_access(sp_ws, all_schemas)

    # UC SQL requires the application_id (UUID), not the display name.
    sp_sql_name = sp_app_id or _get_sp_principal(sp_ws)
    sp_sql_name = sp_sql_name.replace("`", "")
    # Human-readable name for sharing dialog instructions.
    sp_human_name = sp_display or sp_sql_name

    space_perms: list[SpacePermissions] = []
    for space in user_spaces:
        sid = space["id"]
        title = space.get("title", sid)

        cached_perms = _cached_perms_rest(sid)
        sp_has_manage = (
            _sp_has_manage_from_rest_acl(cached_perms, sp_aliases)
            if cached_perms is not None
            else False
        )

        sp_grant_instructions: str | None = None
        if not sp_has_manage:
            sp_grant_instructions = (
                f'Open the Genie Space "{title}" sharing dialog and add '
                f'`{sp_human_name}` with CAN_MANAGE permission.'
            )

        schemas_out: list[SchemaPermission] = []
        for cat, sch in space_schemas.get(sid, []):
            key = (cat, sch)
            is_read = key in sp_read_granted
            is_write = key in sp_write_granted

            read_cmd: str | None = None
            if not is_read:
                read_cmd = (
                    f"-- Catalog-level (run once per catalog)\n"
                    f"GRANT USE CATALOG ON CATALOG `{cat}` TO `{sp_sql_name}`;\n"
                    f"\n"
                    f"-- Schema-level\n"
                    f"GRANT USE SCHEMA ON SCHEMA `{cat}`.`{sch}` TO `{sp_sql_name}`;\n"
                    f"GRANT SELECT ON SCHEMA `{cat}`.`{sch}` TO `{sp_sql_name}`;\n"
                    f"GRANT EXECUTE ON SCHEMA `{cat}`.`{sch}` TO `{sp_sql_name}`;"
                )

            write_cmd: str | None = None
            if not is_write:
                write_cmd = (
                    f"GRANT MODIFY ON SCHEMA `{cat}`.`{sch}` TO `{sp_sql_name}`;"
                )

            schemas_out.append(SchemaPermission(
                catalog=cat,
                schema_name=sch,
                readGranted=is_read,
                writeGranted=is_write,
                readGrantCommand=read_cmd,
                writeGrantCommand=write_cmd,
            ))

        all_read = all(s.readGranted for s in schemas_out) if schemas_out else True
        if not sp_has_manage or not all_read:
            status = "not_configured" if not sp_has_manage and not all_read else "action_needed"
        else:
            status = "ready"

        space_perms.append(SpacePermissions(
            spaceId=sid,
            title=title,
            spHasManage=sp_has_manage,
            schemas=schemas_out,
            status=status,
            spGrantInstructions=sp_grant_instructions,
            spDisplayName=sp_human_name if not sp_has_manage else None,
        ))

    sp_id = _get_sp_principal(sp_ws)

    from ..job_launcher import get_job_url
    job_url = get_job_url(sp_ws, job_id=config.job_id)

    host = (sp_ws.config.host or "").rstrip("/")
    workspace_id: int | str | None = None
    try:
        workspace_id = sp_ws.get_workspace_id()
    except Exception:
        workspace_id = None
    ws_host_with_o = f"{host}?o={workspace_id}" if host and workspace_id else host or None

    return PermissionDashboard(
        spaces=space_perms,
        spPrincipalId=sp_id,
        spPrincipalDisplayName=sp_display or None,
        frameworkCatalog=config.catalog,
        frameworkSchema=config.schema_name,
        experimentBasePath="/Shared/genie-space-optimizer/",
        jobName="genie-space-optimizer-job",
        jobUrl=job_url,
        workspaceHost=ws_host_with_o,
    )
