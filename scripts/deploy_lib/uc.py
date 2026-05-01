"""Unity Catalog provisioning for notebook installs."""

from __future__ import annotations

import time
from urllib.parse import quote
from typing import Any

from .config import InstallConfig


SP_CATALOG_PRIVILEGES = {"USE_CATALOG"}
SP_SCHEMA_PRIVILEGES = {
    "USE_SCHEMA",
    "SELECT",
    "MODIFY",
    "CREATE_TABLE",
    "CREATE_FUNCTION",
    "CREATE_MODEL",
    "CREATE_VOLUME",
    "EXECUTE",
    "MANAGE",
}


def api_do(w, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    return w.api_client.do(method=method, path=path, body=body)


def await_sql(w, result: dict[str, Any], *, timeout: int = 60) -> dict[str, Any]:
    terminal = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
    state = ((result.get("status") or {}).get("state") or "").upper()
    if state in terminal:
        return result

    statement_id = result.get("statement_id")
    if not statement_id:
        return result

    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(5)
        result = api_do(w, "GET", f"/api/2.0/sql/statements/{statement_id}")
        state = ((result.get("status") or {}).get("state") or "").upper()
        if state in terminal:
            return result
    return result


def sql_exec(w, warehouse_id: str, statement: str, *, timeout: int = 60) -> dict[str, Any]:
    result = api_do(
        w,
        "POST",
        "/api/2.0/sql/statements",
        {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "50s",
        },
    )
    result = await_sql(w, result, timeout=timeout)
    state = ((result.get("status") or {}).get("state") or "").upper()
    if state != "SUCCEEDED":
        err = ((result.get("status") or {}).get("error") or {}).get("message") or "unknown"
        raise RuntimeError(f"SQL statement failed ({state}): {err}")
    return result


def ensure_schema(w, catalog: str, schema: str, warehouse_id: str) -> None:
    sql_exec(
        w,
        warehouse_id,
        (
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} "
            "COMMENT 'Genie Space Optimizer state tables, prompts, and benchmarks'"
        ),
    )


def ensure_volume(w, catalog: str, schema: str, warehouse_id: str) -> None:
    sql_exec(w, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.app_artifacts")


def ensure_tables(w, catalog: str, schema: str, warehouse_id: str) -> None:
    from genie_space_optimizer.optimization.ddl import _ALL_DDL

    for table_name, ddl_template in _ALL_DDL.items():
        stmt = ddl_template.replace("{catalog}", catalog).replace("{schema}", schema)
        sql_exec(w, warehouse_id, stmt)
        enable_change_data_feed(w, catalog, schema, table_name, warehouse_id)


def enable_change_data_feed(
    w,
    catalog: str,
    schema: str,
    table: str,
    warehouse_id: str,
) -> None:
    try:
        sql_exec(
            w,
            warehouse_id,
            f"ALTER TABLE {catalog}.{schema}.{table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)",
        )
    except Exception:
        # CDF helps downstream syncs but should not block first install.
        pass


def update_grants(
    w,
    *,
    securable_type: str,
    full_name: str,
    principal: str,
    add: list[str],
) -> dict[str, Any]:
    encoded = quote(full_name, safe="")
    return api_do(
        w,
        "PATCH",
        f"/api/2.1/unity-catalog/permissions/{securable_type}/{encoded}",
        {"changes": [{"principal": principal, "add": add}]},
    )


def get_grants(w, *, securable_type: str, full_name: str) -> dict[str, Any]:
    encoded = quote(full_name, safe="")
    return api_do(w, "GET", f"/api/2.1/unity-catalog/permissions/{securable_type}/{encoded}")


def extract_principal_privileges(grants: dict[str, Any], principal: str) -> set[str]:
    target = principal.strip().lower()
    for assignment in grants.get("privilege_assignments") or []:
        if str(assignment.get("principal") or "").strip().lower() != target:
            continue
        values: set[str] = set()
        for privilege in assignment.get("privileges") or []:
            if isinstance(privilege, str):
                values.add(privilege.strip().upper())
            elif isinstance(privilege, dict):
                raw = privilege.get("privilege") or privilege.get("name") or privilege.get("value")
                if raw:
                    values.add(str(raw).strip().upper())
        return values
    return set()


def grant_schema_privileges(w, catalog: str, schema: str, principal: str) -> None:
    try:
        update_grants(
            w,
            securable_type="catalog",
            full_name=catalog,
            principal=principal,
            add=sorted(SP_CATALOG_PRIVILEGES),
        )
    except Exception as exc:
        msg = str(exc).lower()
        if "manage" not in msg and "permission" not in msg:
            raise
    update_grants(
        w,
        securable_type="schema",
        full_name=f"{catalog}.{schema}",
        principal=principal,
        add=sorted(SP_SCHEMA_PRIVILEGES),
    )


def grant_volume_privileges(w, catalog: str, schema: str, principal: str) -> None:
    try:
        update_grants(
            w,
            securable_type="volume",
            full_name=f"{catalog}.{schema}.app_artifacts",
            principal=principal,
            add=["READ_VOLUME", "WRITE_VOLUME"],
        )
    except Exception:
        pass


def verify_required_privileges(w, catalog: str, schema: str, principal: str) -> dict[str, Any]:
    catalog_grants = get_grants(w, securable_type="catalog", full_name=catalog)
    schema_grants = get_grants(w, securable_type="schema", full_name=f"{catalog}.{schema}")
    have_catalog = extract_principal_privileges(catalog_grants, principal)
    have_schema = extract_principal_privileges(schema_grants, principal)
    return {
        "missing_catalog": sorted(SP_CATALOG_PRIVILEGES - have_catalog),
        "missing_schema": sorted(SP_SCHEMA_PRIVILEGES - have_schema),
    }


def ensure_uc_objects_and_grants(w, cfg: InstallConfig, app_sp_client_id: str) -> dict[str, Any]:
    ensure_schema(w, cfg.catalog, cfg.gso_schema, cfg.warehouse_id)
    ensure_volume(w, cfg.catalog, cfg.gso_schema, cfg.warehouse_id)
    ensure_tables(w, cfg.catalog, cfg.gso_schema, cfg.warehouse_id)
    grant_schema_privileges(w, cfg.catalog, cfg.gso_schema, app_sp_client_id)
    grant_volume_privileges(w, cfg.catalog, cfg.gso_schema, app_sp_client_id)
    return verify_required_privileges(w, cfg.catalog, cfg.gso_schema, app_sp_client_id)

