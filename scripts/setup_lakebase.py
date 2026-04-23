"""Set up Lakebase Autoscaling for the Genie Workbench app.

Creates the Lakebase project (if needed), provisions a Postgres role for
the app's service principal, and grants the minimum required database
permissions. All operations are idempotent — safe to re-run on every deploy.

Usage:
    python scripts/setup_lakebase.py \
        --profile DEFAULT \
        --project-name louis-genie-workbench \
        --sp-client-id 6b205849-2e28-41a3-bafe-7105b151ffc2
"""

import argparse


def _get_client(profile: str):
    """Create a WorkspaceClient from a CLI profile."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=profile)


def _ensure_project(w, project_name: str) -> str:
    """Create or get the Lakebase Autoscaling project. Returns the project resource name."""
    from databricks.sdk.errors import NotFound, AlreadyExists
    from databricks.sdk.service.postgres import Project, ProjectSpec

    resource_name = f"projects/{project_name}"
    try:
        w.postgres.get_project(name=resource_name)
        print(f"  ✓ Lakebase project exists: {project_name}")
        return resource_name
    except NotFound:
        pass

    print(f"  Creating Lakebase project '{project_name}' (this may take 1-2 minutes)...")
    try:
        op = w.postgres.create_project(
            project=Project(spec=ProjectSpec()),
            project_id=project_name,
        )
        # wait() blocks until the long-running operation completes
        op.wait()
        print(f"  ✓ Lakebase project ready: {project_name}")
    except AlreadyExists:
        print(f"  ✓ Project already exists: {project_name}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            print(f"  ✓ Project already exists: {project_name}")
        else:
            raise

    return resource_name


def _ensure_role(w, project_name: str, sp_client_id: str):
    """Create a Postgres role for the app's service principal."""
    from databricks.sdk.errors import AlreadyExists
    from databricks.sdk.service.postgres import (
        Role, RoleRoleSpec, RoleIdentityType, RoleAuthMethod,
    )

    branch_path = f"projects/{project_name}/branches/production"

    # role_id must match ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$ — prefix with "sp-"
    # if the client ID starts with a digit
    role_id = sp_client_id if sp_client_id[0].isalpha() else f"sp-{sp_client_id}"

    print(f"  Creating Postgres role for SP '{sp_client_id[:8]}...'...")
    try:
        op = w.postgres.create_role(
            parent=branch_path,
            role=Role(spec=RoleRoleSpec(
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                postgres_role=sp_client_id,
            )),
            role_id=role_id,
        )
        # wait() blocks until role is created
        op.wait()
        print(f"  ✓ Postgres role created for SP")
    except AlreadyExists:
        print(f"  ✓ Postgres role already exists for SP")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"  ✓ Postgres role already exists for SP")
        else:
            raise


def _grant_permissions(w, project_name: str, sp_client_id: str, endpoint_name: str) -> bool:
    """Grant database permissions to the SP via a direct Postgres connection.

    Uses psycopg to connect as the deployer and run GRANT commands.
    Requires psycopg[binary] to be installed.
    Returns True if grants succeeded, False otherwise.
    """
    endpoint_path = f"projects/{project_name}/branches/production/endpoints/{endpoint_name}"

    # Resolve endpoint host
    print(f"  Resolving endpoint DNS...")
    endpoint = w.postgres.get_endpoint(name=endpoint_path)
    hosts = endpoint.status and endpoint.status.hosts
    host = hosts.host if hosts else None
    if not host:
        print(f"  ⚠ Endpoint has no host yet — grants will be applied on next deploy.")
        return False

    # Generate deployer credential
    cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
    token = cred.token
    if not token:
        print(f"  ⚠ Could not generate database credential — grants will need manual setup.")
        return False

    # Determine deployer username (human user email or SP client ID)
    deployer_user = w.config.client_id
    if not deployer_user:
        try:
            me = w.current_user.me()
            deployer_user = me.user_name or (me.emails[0].value if me.emails else "")
        except Exception:
            pass
    if not deployer_user:
        print(f"  ⚠ Could not determine deployer username — grants will need manual setup.")
        return False

    try:
        import psycopg
    except ImportError:
        print(f"  ⚠ psycopg not installed. Install with: pip install 'psycopg[binary]'")
        print(f"  Run these commands manually in the Lakebase SQL Editor:")
        print(f'    GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}";')
        print(f'    GRANT CREATE ON DATABASE databricks_postgres TO "{sp_client_id}";')
        return False

    print(f"  Connecting to Lakebase as {deployer_user[:12]}... to run GRANTs...")
    try:
        conn = psycopg.connect(
            host=host, port=5432, dbname="databricks_postgres",
            user=deployer_user, password=token, sslmode="require",
        )
        conn.autocommit = True

        # Database-level grants. On a fresh Lakebase the app creates the
        # genie schema and tables at startup via _ensure_schema() — since
        # the SP executes those DDL statements, it owns everything it
        # creates. On a re-used Lakebase (different SP than the previous
        # install), we also reconcile ownership below.
        grants = [
            f'GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}"',
            f'GRANT CREATE ON DATABASE databricks_postgres TO "{sp_client_id}"',
        ]
        for grant in grants:
            try:
                conn.execute(grant)
            except Exception as e:
                if "already" in str(e).lower():
                    pass
                else:
                    print(f"    ⚠ {grant}: {e}")

        _reconcile_schema_ownership(conn, new_sp=sp_client_id, project_name=project_name)
        conn.close()
        print(f"  ✓ Database permissions granted to SP")
        return True
    except Exception as e:
        print(f"  ⚠ Could not connect to Lakebase for GRANTs: {e}")
        print(f"  Run these commands manually in the Lakebase SQL Editor:")
        print(f'    GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}";')
        print(f'    GRANT CREATE ON DATABASE databricks_postgres TO "{sp_client_id}";')
        return False


def _reconcile_schema_ownership(conn, *, new_sp: str, project_name: str) -> None:
    """Transfer ownership of the `genie` schema + its tables/sequences to
    the new SP, if the schema already exists and is owned by someone else.

    This is the re-install scenario: the user is redeploying the app
    (possibly under a new name, which gets a brand-new SP) against a
    Lakebase project that already has a `genie` schema + data from a
    previous install. Without ownership transfer, the new SP gets
    "permission denied for schema genie" when it tries to write.

    Strategy (in order):
    1. `REASSIGN OWNED BY old TO new` — single atomic statement; requires
       the deployer to be a member of both roles (typical for project
       admins who created both SPs).
    2. Per-object `ALTER ... OWNER TO new` — partial-success path in case
       REASSIGN isn't allowed but individual ALTERs are.

    If neither works, surfaces remediation SQL the deployer can run in
    the Lakebase SQL Editor after granting themselves the right role.
    Never fails the install — the app falls back to in-memory until the
    ownership is fixed.
    """
    # Does the schema exist, and if so who owns it?
    row = conn.execute(
        "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = 'genie'"
    ).fetchone()
    if not row:
        # Fresh Lakebase — nothing to reconcile; the app SP will create
        # and own `genie` at startup.
        return

    current_owner = (row[0] or "").strip('"')
    if current_owner == new_sp:
        print(f"  ✓ genie schema already owned by SP")
        return

    print(f"  genie schema exists, owned by {current_owner[:12]}... — reconciling to new SP...")

    # Strategy 1: single REASSIGN covers schema + all tables + all sequences
    reassign = f'REASSIGN OWNED BY "{current_owner}" TO "{new_sp}"'
    try:
        conn.execute(reassign)
        print(f"  ✓ Reassigned all objects owned by previous SP to new SP")
        return
    except Exception as e:
        reassign_err = str(e).strip().splitlines()[0]

    # Strategy 2: per-object ALTER (may partially succeed)
    statements: list[str] = [f'ALTER SCHEMA genie OWNER TO "{new_sp}"']
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'genie'"
        ).fetchall()]
        for t in tables:
            statements.append(f'ALTER TABLE genie."{t}" OWNER TO "{new_sp}"')
        seqs = [r[0] for r in conn.execute(
            "SELECT sequencename FROM pg_sequences WHERE schemaname = 'genie'"
        ).fetchall()]
        for s in seqs:
            statements.append(f'ALTER SEQUENCE genie."{s}" OWNER TO "{new_sp}"')
    except Exception as e:
        print(f"    ⚠ Could not enumerate tables/sequences: {e}")

    succeeded, failed_perms = 0, False
    for stmt in statements:
        try:
            conn.execute(stmt)
            succeeded += 1
        except Exception as e:
            msg = str(e).lower()
            if "must be owner" in msg or "permission denied" in msg or "must be member" in msg:
                failed_perms = True
                break
            print(f"    ⚠ {stmt}: {e}")

    if failed_perms:
        print(f"    ✗ Ownership transfer blocked: REASSIGN failed ({reassign_err}); "
              f"per-object ALTER also blocked by permissions.")
        print(f"      The deployer's Postgres role must be a member of the previous SP role")
        print(f"      ({current_owner[:24]}...) OR have the databricks_superuser role on")
        print(f"      Lakebase project '{project_name}'.")
        print(f"      Fix it manually — in the Lakebase SQL Editor run ONE of:")
        print(f"        -- Option A: reassign (preserves data)")
        print(f"        {reassign};")
        print(f"        -- Option B: drop and let the new SP recreate (LOSES data)")
        print(f"        DROP SCHEMA genie CASCADE;")
        return

    if succeeded == len(statements):
        print(f"  ✓ Transferred ownership of genie schema ({succeeded} objects) to SP")
    else:
        print(f"  ⚠ Transferred ownership partially ({succeeded}/{len(statements)} objects). "
              f"Re-run deploy to retry.")


def main():
    parser = argparse.ArgumentParser(description="Set up Lakebase Autoscaling for the app")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--project-name", required=True, help="Lakebase project name")
    parser.add_argument("--sp-client-id", required=True, help="App service principal client ID")
    parser.add_argument("--endpoint-name", default="primary", help="Compute endpoint name")
    args = parser.parse_args()

    w = _get_client(args.profile)

    # Step 1: Ensure project exists
    _ensure_project(w, args.project_name)

    # Step 2: Ensure Postgres role for SP
    _ensure_role(w, args.project_name, args.sp_client_id)

    # Step 3: Grant database permissions
    grants_ok = _grant_permissions(w, args.project_name, args.sp_client_id, args.endpoint_name)

    # Output the endpoint path for resource attachment
    endpoint_path = f"projects/{args.project_name}/branches/production/endpoints/{args.endpoint_name}"
    print(f"\n  Endpoint path: {endpoint_path}")

    if not grants_ok:
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
