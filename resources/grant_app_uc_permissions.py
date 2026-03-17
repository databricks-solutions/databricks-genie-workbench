import argparse
import json
import subprocess
import sys

# SP privileges on the optimizer's own schema — the SP runs optimization jobs
# and needs full write access to state tables, MLflow models, and prompts.
# Access to *user* schemas (Genie space assets) is granted separately via
# the app's Data Access Settings page.
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

CATALOG_PRIVILEGES = SP_CATALOG_PRIVILEGES
SCHEMA_PRIVILEGES = SP_SCHEMA_PRIVILEGES


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
    """Create the optimization schema if it doesn't exist (deployer credentials)."""
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
    print(f"[grant-app-sp] Schema ensured: {schema_fqn}")


_VOLUME_NAME = "app_artifacts"


def _ensure_volume(*, profile: str, catalog: str, schema: str, warehouse_id: str) -> None:
    """Create the managed artifact volume if it doesn't exist (deployer credentials)."""
    vol_fqn = f"{catalog}.{schema}.{_VOLUME_NAME}"
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
    print(f"[grant-app-sp] Volume ensured: {vol_fqn}")


def _is_app_missing(err: Exception) -> bool:
    msg = str(err).lower()
    return "does not exist" in msg or "resource_does_not_exist" in msg


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
            "databricks",
            "grants",
            "update",
            securable_type,
            full_name,
            "--profile",
            profile,
            "--json",
            json.dumps(payload),
            "-o",
            "json",
        ],
    )


def _get_grants(
    *,
    profile: str,
    securable_type: str,
    full_name: str,
) -> dict:
    return _run_json(
        [
            "databricks",
            "grants",
            "get",
            securable_type,
            full_name,
            "--profile",
            profile,
            "-o",
            "json",
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
    *,
    profile: str,
    principal: str,
    catalog: str,
    schema: str,
) -> None:
    schema_fqn = f"{catalog}.{schema}"

    catalog_grants = _get_grants(
        profile=profile,
        securable_type="catalog",
        full_name=catalog,
    )
    schema_grants = _get_grants(
        profile=profile,
        securable_type="schema",
        full_name=schema_fqn,
    )

    have_catalog = _extract_principal_privileges(catalog_grants, principal)
    have_schema = _extract_principal_privileges(schema_grants, principal)

    missing_catalog = sorted(CATALOG_PRIVILEGES - have_catalog)
    missing_schema = sorted(SCHEMA_PRIVILEGES - have_schema)
    if missing_catalog or missing_schema:
        raise RuntimeError(
            "Grant verification failed for app service principal "
            f"{principal}. Missing catalog privileges={missing_catalog or '[]'} "
            f"on {catalog}; missing schema privileges={missing_schema or '[]'} "
            f"on {schema_fqn}."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Grant required Unity Catalog privileges to Databricks App SP.",
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", default=None)
    args = parser.parse_args()

    if args.warehouse_id:
        _ensure_schema(
            profile=args.profile,
            catalog=args.catalog,
            schema=args.schema,
            warehouse_id=args.warehouse_id,
        )
        _ensure_volume(
            profile=args.profile,
            catalog=args.catalog,
            schema=args.schema,
            warehouse_id=args.warehouse_id,
        )
    else:
        print(
            "[grant-app-sp] WARNING: --warehouse-id not provided, "
            "skipping schema and volume creation",
            file=sys.stderr,
        )

    try:
        app = _run_json(
            [
                "databricks",
                "apps",
                "get",
                args.app_name,
                "--profile",
                args.profile,
                "-o",
                "json",
            ],
        )
    except Exception as err:
        if _is_app_missing(err):
            print(
                f"[grant-app-sp] WARNING: App '{args.app_name}' not found yet — "
                "grants NOT applied. They will be applied automatically at app "
                "startup, or on the next 'make deploy'.",
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

    # ── SP grants (read-only on optimizer schema) ──────────────────────
    _update_grants(
        profile=args.profile,
        securable_type="catalog",
        full_name=args.catalog,
        principal=principal,
        add=sorted(SP_CATALOG_PRIVILEGES),
    )
    try:
        _update_grants(
            profile=args.profile,
            securable_type="schema",
            full_name=schema_fqn,
            principal=principal,
            add=sorted(SP_SCHEMA_PRIVILEGES),
        )
    except Exception as err:
        if "does not exist" in str(err).lower():
            print(
                f"[grant-app-sp] WARNING: Schema '{schema_fqn}' does not exist yet — "
                "schema grants NOT applied. The app will create the schema on first "
                "startup and self-grant via _UCGrantBootstrap. Alternatively, run "
                "'make deploy' again after the schema exists.",
                file=sys.stderr,
            )
            return 0
        raise
    try:
        _verify_required_privileges(
            profile=args.profile,
            principal=principal,
            catalog=args.catalog,
            schema=args.schema,
        )
    except Exception as err:
        if "does not exist" in str(err).lower():
            print(
                f"[grant-app-sp] WARNING: Schema '{schema_fqn}' does not exist yet — "
                "skipping grant verification. Grants will be applied at app startup.",
                file=sys.stderr,
            )
            return 0
        raise
    vol_fqn = f"{schema_fqn}.app_artifacts"
    try:
        _update_grants(
            profile=args.profile,
            securable_type="volume",
            full_name=vol_fqn,
            principal=principal,
            add=["READ_VOLUME", "WRITE_VOLUME"],
        )
    except Exception as err:
        if "does not exist" in str(err).lower():
            print(
                f"[grant-app-sp] WARNING: Volume '{vol_fqn}' does not exist yet — "
                "volume grants NOT applied. They will be applied at app startup.",
                file=sys.stderr,
            )
        else:
            print(
                f"[grant-app-sp] WARNING: Could not grant volume privileges on "
                f"'{vol_fqn}': {err}",
                file=sys.stderr,
            )

    print(
        f"[grant-app-sp] SP grants applied: principal={principal} "
        f"on {schema_fqn}",
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[grant-app-sp] ERROR: {exc}", file=sys.stderr)
        raise
