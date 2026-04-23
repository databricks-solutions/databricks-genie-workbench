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


def _quote_ident(value: str) -> str:
    """Quote a Postgres identifier such as a role or table name."""
    return '"' + value.replace('"', '""') + '"'


def _one_line_error(exc: Exception) -> str:
    return str(exc).strip().splitlines()[0] if str(exc).strip() else repr(exc)


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
        grants_ok = True
        for grant in grants:
            try:
                conn.execute(grant)
            except Exception as e:
                if "already" in str(e).lower():
                    pass
                else:
                    print(f"    ⚠ {grant}: {e}")
                    grants_ok = False

        schema_ok = _reconcile_schema_ownership(
            conn, new_sp=sp_client_id, project_name=project_name,
        )
        conn.close()
        if grants_ok and schema_ok:
            print(f"  ✓ Database permissions granted to SP")
            return True
        print(f"  ⚠ Database permissions incomplete for SP")
        return False
    except Exception as e:
        print(f"  ⚠ Could not connect to Lakebase for GRANTs: {e}")
        print(f"  Run these commands manually in the Lakebase SQL Editor:")
        print(f'    GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}";')
        print(f'    GRANT CREATE ON DATABASE databricks_postgres TO "{sp_client_id}";')
        return False


def _fetch_genie_schema_state(conn) -> dict | None:
    """Return owner metadata for the app-owned `genie` schema."""
    row = conn.execute(
        "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'genie'"
    ).fetchone()
    if not row:
        return None

    tables = conn.execute("""
        SELECT c.relname, pg_get_userbyid(c.relowner)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'genie'
          AND c.relkind IN ('r', 'p')
        ORDER BY c.relname
    """).fetchall()
    sequences = conn.execute("""
        SELECT c.relname, pg_get_userbyid(c.relowner)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'genie'
          AND c.relkind = 'S'
        ORDER BY c.relname
    """).fetchall()

    return {
        "schema_owner": (row[0] or "").strip('"'),
        "tables": [(r[0], (r[1] or "").strip('"')) for r in tables],
        "sequences": [(r[0], (r[1] or "").strip('"')) for r in sequences],
    }


def _owners_in_state(state: dict, *, exclude: str = "") -> set[str]:
    owners = {state["schema_owner"]}
    owners.update(owner for _, owner in state["tables"])
    owners.update(owner for _, owner in state["sequences"])
    return {owner for owner in owners if owner and owner != exclude}


def _is_member_of_role(conn, role: str) -> bool:
    try:
        row = conn.execute(
            "SELECT pg_has_role(CURRENT_USER, %s, 'MEMBER')",
            (role,),
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _grant_temporary_memberships(conn, roles: list[str]) -> tuple[list[str], list[str]]:
    """Grant CURRENT_USER temporary membership in roles it does not already have."""
    granted: list[str] = []
    errors: list[str] = []
    for role in roles:
        if not role or _is_member_of_role(conn, role):
            continue
        stmt = f"GRANT {_quote_ident(role)} TO CURRENT_USER"
        try:
            conn.execute(stmt)
            granted.append(role)
        except Exception as e:
            errors.append(f"{stmt}: {_one_line_error(e)}")
    return granted, errors


def _revoke_temporary_memberships(conn, roles: list[str]) -> None:
    for role in reversed(roles):
        try:
            conn.execute(f"REVOKE {_quote_ident(role)} FROM CURRENT_USER")
        except Exception:
            pass


def _transfer_genie_ownership(conn, state: dict, *, new_sp: str) -> int:
    """Transfer only the app-owned `genie` schema objects to the new SP."""
    changed = 0
    if state["schema_owner"] != new_sp:
        conn.execute(f"ALTER SCHEMA genie OWNER TO {_quote_ident(new_sp)}")
        changed += 1

    for table, owner in state["tables"]:
        if owner != new_sp:
            conn.execute(
                f"ALTER TABLE genie.{_quote_ident(table)} OWNER TO {_quote_ident(new_sp)}"
            )
            changed += 1

    for sequence, owner in state["sequences"]:
        if owner != new_sp:
            conn.execute(
                f"ALTER SEQUENCE genie.{_quote_ident(sequence)} OWNER TO {_quote_ident(new_sp)}"
            )
            changed += 1

    return changed


def _state_owned_by(state: dict, *, owner: str) -> bool:
    return (
        state["schema_owner"] == owner
        and all(table_owner == owner for _, table_owner in state["tables"])
        and all(seq_owner == owner for _, seq_owner in state["sequences"])
    )


def _grant_existing_genie_access(conn, *, new_sp: str) -> bool:
    """Fallback for existing schemas when ownership cannot be transferred.

    This preserves existing data and gives the new app SP enough access to
    read/write the current app state. Owner-only migrations remain best-effort
    at startup.
    """
    grants = [
        f"GRANT USAGE, CREATE ON SCHEMA genie TO {_quote_ident(new_sp)}",
        (
            "GRANT SELECT, INSERT, UPDATE, DELETE "
            f"ON ALL TABLES IN SCHEMA genie TO {_quote_ident(new_sp)}"
        ),
        (
            "GRANT USAGE, SELECT, UPDATE "
            f"ON ALL SEQUENCES IN SCHEMA genie TO {_quote_ident(new_sp)}"
        ),
    ]
    ok = True
    for stmt in grants:
        try:
            conn.execute(stmt)
        except Exception as e:
            ok = False
            print(f"    ⚠ {stmt}: {_one_line_error(e)}")
    return ok


def _print_manual_schema_repair(
    *, state: dict, new_sp: str, project_name: str, errors: list[str],
) -> None:
    owners = sorted(_owners_in_state(state, exclude=new_sp))
    print("    ✗ Existing `genie` schema is not accessible to the new app SP.")
    if errors:
        print("      Automated repair errors:")
        for err in errors:
            print(f"        - {err}")
    print(f"      Fix it in the Lakebase SQL Editor for project '{project_name}':")
    for owner in owners:
        print(f"        GRANT {_quote_ident(owner)} TO CURRENT_USER;")
    print(f"        GRANT {_quote_ident(new_sp)} TO CURRENT_USER;")
    if state["schema_owner"] != new_sp:
        print(f"        ALTER SCHEMA genie OWNER TO {_quote_ident(new_sp)};")
    for table, owner in state["tables"]:
        if owner != new_sp:
            print(
                f"        ALTER TABLE genie.{_quote_ident(table)} "
                f"OWNER TO {_quote_ident(new_sp)};"
            )
    for sequence, owner in state["sequences"]:
        if owner != new_sp:
            print(
                f"        ALTER SEQUENCE genie.{_quote_ident(sequence)} "
                f"OWNER TO {_quote_ident(new_sp)};"
            )
    print(f"        REVOKE {_quote_ident(new_sp)} FROM CURRENT_USER;")
    for owner in reversed(owners):
        print(f"        REVOKE {_quote_ident(owner)} FROM CURRENT_USER;")
    print("      If ownership cannot be transferred, preserve data with grants:")
    print(f"        GRANT USAGE, CREATE ON SCHEMA genie TO {_quote_ident(new_sp)};")
    print(
        "        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES "
        f"IN SCHEMA genie TO {_quote_ident(new_sp)};"
    )
    print(
        "        GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES "
        f"IN SCHEMA genie TO {_quote_ident(new_sp)};"
    )


def _reconcile_schema_ownership(conn, *, new_sp: str, project_name: str) -> bool:
    """Transfer ownership of the `genie` schema + its tables/sequences to
    the new SP, if the schema already exists and is owned by someone else.

    This is the re-install scenario: the user is redeploying the app
    (possibly under a new name, which gets a brand-new SP) against a
    Lakebase project that already has a `genie` schema + data from a
    previous install. Without ownership transfer, the new SP gets
    "permission denied for schema genie" when it tries to write.

    Only the app-owned `genie` schema is touched. We never use
    REASSIGN OWNED because that can transfer unrelated objects owned by a
    previous app SP in the same Lakebase project.
    """
    state = _fetch_genie_schema_state(conn)
    if not state:
        # Fresh Lakebase — nothing to reconcile; the app SP will create
        # and own `genie` at startup.
        return True

    if _state_owned_by(state, owner=new_sp):
        print(f"  ✓ genie schema already owned by SP")
        return True

    owners = sorted(_owners_in_state(state, exclude=new_sp))
    print(
        "  genie schema exists, owned by "
        f"{', '.join(o[:12] + '...' for o in owners)} — reconciling to new SP..."
    )

    temp_roles: list[str] = []
    errors: list[str] = []
    try:
        temp_roles, grant_errors = _grant_temporary_memberships(
            conn, owners + [new_sp],
        )
        errors.extend(grant_errors)
        try:
            changed = _transfer_genie_ownership(conn, state, new_sp=new_sp)
            updated = _fetch_genie_schema_state(conn)
            if updated and _state_owned_by(updated, owner=new_sp):
                print(f"  ✓ Transferred ownership of genie schema ({changed} objects) to SP")
                return True
            errors.append("Ownership transfer did not update every genie object")
        except Exception as e:
            errors.append(f"Ownership transfer failed: {_one_line_error(e)}")

        # Fallback: preserve data and give the new app SP read/write access.
        if _grant_existing_genie_access(conn, new_sp=new_sp):
            print(
                "  ✓ Granted new SP access to existing genie schema "
                "(ownership transfer was blocked)"
            )
            return True
    finally:
        _revoke_temporary_memberships(conn, temp_roles)

    _print_manual_schema_repair(
        state=state, new_sp=new_sp, project_name=project_name, errors=errors,
    )
    return False


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
