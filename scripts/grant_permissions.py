"""Grant required Unity Catalog privileges to the Genie Workbench app service principal.

Adapted from packages/genie-space-optimizer/resources/grant_app_uc_permissions.py
for the unified Genie Workbench bundle.

Usage:
    python scripts/grant_permissions.py \
        --profile DEFAULT \
        --app-name genie-workbench \
        --catalog main \
        --schema genie_space_optimizer \
        --warehouse-id <warehouse-id>
"""

import argparse
import json
import os
import subprocess
import sys

# Allow importing from the GSO package (packages/genie-space-optimizer/src)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_GSO_SRC = os.path.join(_SCRIPT_DIR, os.pardir, "packages", "genie-space-optimizer", "src")
if os.path.isdir(_GSO_SRC) and _GSO_SRC not in sys.path:
    sys.path.insert(0, os.path.abspath(_GSO_SRC))

# SP privileges on the GSO optimization schema — the SP runs optimization jobs
# and needs full write access to state tables, MLflow models, and prompts.
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


def _run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n{stderr}",
        )
    return (result.stdout or "").strip()


def _run_json(cmd: list[str]) -> dict:
    out = _run(cmd)
    if not out:
        return {}
    return json.loads(out)


def _ensure_schema(*, profile: str, catalog: str, schema: str, warehouse_id: str) -> None:
    """Create the optimization schema if it doesn't exist."""
    schema_fqn = f"{catalog}.{schema}"
    stmt = (
        f"CREATE SCHEMA IF NOT EXISTS {schema_fqn} "
        f"COMMENT 'Genie Space Optimizer state tables, prompts, and benchmarks'"
    )
    payload = json.dumps({
        "warehouse_id": warehouse_id,
        "statement": stmt,
        "wait_timeout": "30s",
    })
    result = _run_json([
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", profile,
        "--json", payload,
    ])
    state = (result.get("status") or {}).get("state", "")
    if state != "SUCCEEDED":
        err_msg = (result.get("status") or {}).get("error", {}).get("message", "unknown")
        raise RuntimeError(f"Schema creation failed ({state}): {err_msg}")
    print(f"[grant-permissions] Schema ensured: {schema_fqn}")


def _ensure_volume(*, profile: str, catalog: str, schema: str, warehouse_id: str) -> None:
    """Create the managed artifact volume if it doesn't exist."""
    vol_fqn = f"{catalog}.{schema}.app_artifacts"
    stmt = f"CREATE VOLUME IF NOT EXISTS {vol_fqn}"
    payload = json.dumps({
        "warehouse_id": warehouse_id,
        "statement": stmt,
        "wait_timeout": "30s",
    })
    result = _run_json([
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", profile,
        "--json", payload,
    ])
    state = (result.get("status") or {}).get("state", "")
    if state != "SUCCEEDED":
        err_msg = (result.get("status") or {}).get("error", {}).get("message", "unknown")
        raise RuntimeError(f"Volume creation failed ({state}): {err_msg}")
    print(f"[grant-permissions] Volume ensured: {vol_fqn}")


def _sql_exec(*, profile: str, warehouse_id: str, statement: str) -> dict:
    """Execute a SQL statement via the Statement Execution API.

    Writes the JSON payload to a temp file to avoid shell escaping issues
    with multiline DDL statements.
    """
    import tempfile

    payload = json.dumps({
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
    })
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(payload)
        tmp_path = f.name
    try:
        return _run_json([
            "databricks", "api", "post", "/api/2.0/sql/statements",
            "--profile", profile,
            "--json", f"@{tmp_path}",
        ])
    finally:
        import os as _os
        _os.unlink(tmp_path)


def _ensure_tables(*, profile: str, catalog: str, schema: str, warehouse_id: str) -> None:
    """Create all GSO Delta tables if they don't exist (idempotent)."""
    from genie_space_optimizer.optimization.state import _ALL_DDL

    failed: list[str] = []
    for table_name, ddl_template in _ALL_DDL.items():
        stmt = ddl_template.replace("{catalog}", catalog).replace("{schema}", schema)
        result = _sql_exec(
            profile=profile, warehouse_id=warehouse_id, statement=stmt,
        )
        state = (result.get("status") or {}).get("state", "")
        if state != "SUCCEEDED":
            err_msg = (result.get("status") or {}).get("error", {}).get("message", "unknown")
            failed.append(table_name)
            print(
                f"[grant-permissions] ERROR: Table {catalog}.{schema}.{table_name} "
                f"creation failed ({state}): {err_msg}",
                file=sys.stderr,
            )
        else:
            print(f"[grant-permissions] Table ensured: {catalog}.{schema}.{table_name}")
            cdf_stmt = (
                f"ALTER TABLE {catalog}.{schema}.{table_name} "
                f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )
            cdf_result = _sql_exec(
                profile=profile, warehouse_id=warehouse_id, statement=cdf_stmt,
            )
            cdf_state = (cdf_result.get("status") or {}).get("state", "")
            if cdf_state == "SUCCEEDED":
                print(f"[grant-permissions] CDF enabled: {catalog}.{schema}.{table_name}")
            else:
                print(f"[grant-permissions] WARNING: CDF enablement failed for {table_name} (non-fatal)")

    if failed:
        raise RuntimeError(
            f"Failed to create {len(failed)} table(s): {', '.join(failed)}. "
            f"Check warehouse accessibility and permissions on {catalog}.{schema}."
        )


def _update_grants(
    *,
    profile: str,
    securable_type: str,
    full_name: str,
    principal: str,
    add: list[str],
) -> dict:
    payload = {
        "changes": [
            {
                "principal": principal,
                "add": add,
            },
        ],
    }
    return _run_json(
        [
            "databricks", "grants", "update",
            securable_type, full_name,
            "--profile", profile,
            "--json", json.dumps(payload),
            "-o", "json",
        ],
    )


def _get_grants(*, profile: str, securable_type: str, full_name: str) -> dict:
    return _run_json(
        [
            "databricks", "grants", "get",
            securable_type, full_name,
            "--profile", profile,
            "-o", "json",
        ],
    )


def _extract_principal_privileges(grants: dict, principal: str) -> set[str]:
    assignments = grants.get("privilege_assignments") or []
    target = principal.strip().lower()
    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        assignee = str(assignment.get("principal") or "").strip().lower()
        if assignee != target:
            continue
        values: set[str] = set()
        for priv in assignment.get("privileges") or []:
            if isinstance(priv, str):
                values.add(priv.strip().upper())
            elif isinstance(priv, dict):
                raw = priv.get("privilege") or priv.get("name") or priv.get("value")
                if raw:
                    values.add(str(raw).strip().upper())
        return values
    return set()


def _verify_required_privileges(
    *, profile: str, principal: str, catalog: str, schema: str,
) -> None:
    schema_fqn = f"{catalog}.{schema}"
    catalog_grants = _get_grants(profile=profile, securable_type="catalog", full_name=catalog)
    schema_grants = _get_grants(profile=profile, securable_type="schema", full_name=schema_fqn)
    have_catalog = _extract_principal_privileges(catalog_grants, principal)
    have_schema = _extract_principal_privileges(schema_grants, principal)
    missing_catalog = sorted(SP_CATALOG_PRIVILEGES - have_catalog)
    missing_schema = sorted(SP_SCHEMA_PRIVILEGES - have_schema)
    if missing_catalog or missing_schema:
        raise RuntimeError(
            f"Grant verification failed for SP {principal}. "
            f"Missing catalog privileges={missing_catalog or '[]'} on {catalog}; "
            f"missing schema privileges={missing_schema or '[]'} on {schema_fqn}."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Grant Unity Catalog privileges to Genie Workbench app SP.",
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", default=None)
    args = parser.parse_args()

    # Create schema and volume if warehouse ID provided
    if args.warehouse_id:
        _ensure_schema(
            profile=args.profile, catalog=args.catalog,
            schema=args.schema, warehouse_id=args.warehouse_id,
        )
        _ensure_volume(
            profile=args.profile, catalog=args.catalog,
            schema=args.schema, warehouse_id=args.warehouse_id,
        )
        _ensure_tables(
            profile=args.profile, catalog=args.catalog,
            schema=args.schema, warehouse_id=args.warehouse_id,
        )
    else:
        print(
            "[grant-permissions] WARNING: --warehouse-id not provided, "
            "skipping schema and volume creation",
            file=sys.stderr,
        )

    # Resolve app SP
    try:
        app = _run_json([
            "databricks", "apps", "get", args.app_name,
            "--profile", args.profile, "-o", "json",
        ])
    except Exception as err:
        msg = str(err).lower()
        if "does not exist" in msg or "resource_does_not_exist" in msg:
            print(
                f"[grant-permissions] WARNING: App '{args.app_name}' not found — "
                "grants NOT applied. Run deploy again after app is created.",
                file=sys.stderr,
            )
            return 0
        raise

    principal = (
        app.get("service_principal_client_id")
        or app.get("service_principal_name")
        or ""
    ).strip()
    if not principal:
        raise RuntimeError(
            "Could not resolve app service principal from `databricks apps get` output.",
        )

    schema_fqn = f"{args.catalog}.{args.schema}"

    # Catalog grants
    _update_grants(
        profile=args.profile, securable_type="catalog",
        full_name=args.catalog, principal=principal,
        add=sorted(SP_CATALOG_PRIVILEGES),
    )

    # Schema grants
    try:
        _update_grants(
            profile=args.profile, securable_type="schema",
            full_name=schema_fqn, principal=principal,
            add=sorted(SP_SCHEMA_PRIVILEGES),
        )
    except Exception as err:
        if "does not exist" in str(err).lower():
            print(
                f"[grant-permissions] WARNING: Schema '{schema_fqn}' does not exist — "
                "schema grants NOT applied. Run deploy again after the schema exists.",
                file=sys.stderr,
            )
            return 0
        raise

    # Verify grants
    try:
        _verify_required_privileges(
            profile=args.profile, principal=principal,
            catalog=args.catalog, schema=args.schema,
        )
    except Exception as err:
        if "does not exist" in str(err).lower():
            print(
                f"[grant-permissions] WARNING: Verification skipped — {err}",
                file=sys.stderr,
            )
            return 0
        raise

    # Volume grants
    vol_fqn = f"{schema_fqn}.app_artifacts"
    try:
        _update_grants(
            profile=args.profile, securable_type="volume",
            full_name=vol_fqn, principal=principal,
            add=["READ_VOLUME", "WRITE_VOLUME"],
        )
    except Exception as err:
        if "does not exist" in str(err).lower():
            print(
                f"[grant-permissions] WARNING: Volume '{vol_fqn}' does not exist — "
                "volume grants NOT applied.",
                file=sys.stderr,
            )
        else:
            print(
                f"[grant-permissions] WARNING: Could not grant volume privileges: {err}",
                file=sys.stderr,
            )

    print(f"[grant-permissions] SP grants applied: principal={principal} on {schema_fqn}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[grant-permissions] ERROR: {exc}", file=sys.stderr)
        raise
