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


def _purge_soft_deleted_project(w, project_name: str) -> bool:
    """Purge a soft-deleted project that still reserves this name, if any.

    Deleted Lakebase projects enter a 7-day soft-deleted state during which
    their IDs stay reserved, so a same-name create fails with ALREADY_EXISTS
    while get_project keeps returning NotFound.
    """
    resource_name = f"projects/{project_name}"
    soft_deleted = next(
        (
            p
            for p in w.postgres.list_projects(show_deleted=True)
            if p.name == resource_name and p.delete_time
        ),
        None,
    )
    if not soft_deleted:
        return False

    print(
        f"  Project '{project_name}' is soft-deleted and its name is reserved. "
        "Permanently deleting it (data is unrecoverable) before recreating..."
    )
    try:
        w.postgres.delete_project(name=resource_name, purge=True).wait()
    except Exception as e:
        raise RuntimeError(
            f"Could not purge soft-deleted Lakebase project '{project_name}': {e}. "
            "Purge it manually with "
            f"`databricks postgres delete-project projects/{project_name} --purge`, "
            "restore it with "
            f"`databricks postgres undelete-project projects/{project_name}`, "
            "or pass a different --project-name."
        ) from e
    return True


def _ensure_project(w, project_name: str) -> str:
    """Create or get the Lakebase Autoscaling project. Returns the project resource name."""
    from databricks.sdk.errors import NotFound, AlreadyExists
    from databricks.sdk.service.postgres import Project, ProjectSpec

    resource_name = f"projects/{project_name}"
    try:
        project = w.postgres.get_project(name=resource_name)
        # get_project returns soft-deleted projects as if live (delete_time
        # set); only their sub-resources (branches, roles) 404. Treat them as
        # absent so the name gets purged and recreated.
        if not project.delete_time:
            print(f"  ✓ Lakebase project exists: {project_name}")
            return resource_name
        _purge_soft_deleted_project(w, project_name)
    except NotFound:
        _purge_soft_deleted_project(w, project_name)

    print(f"  Creating Lakebase project '{project_name}' (this may take 1-2 minutes)...")
    try:
        op = w.postgres.create_project(
            project=Project(spec=ProjectSpec()),
            project_id=project_name,
        )
        # wait() blocks until the long-running operation completes
        op.wait()
        print(f"  ✓ Lakebase project ready: {project_name}")
    except Exception as e:
        is_already_exists = isinstance(e, AlreadyExists) or "ALREADY_EXISTS" in str(e)
        if not is_already_exists:
            raise
        # ALREADY_EXISTS after a NotFound get means the name is reserved by a
        # project we cannot see (e.g., soft-deleted under another owner).
        try:
            w.postgres.get_project(name=resource_name)
            print(f"  ✓ Project already exists: {project_name}")
        except NotFound:
            raise RuntimeError(
                f"Lakebase project name '{project_name}' is reserved but the project "
                "is not accessible — most likely a soft-deleted project owned by "
                "another user (deleted project IDs are reserved for 7 days). "
                "Ask its owner to purge it with "
                f"`databricks postgres delete-project projects/{project_name} --purge`, "
                "or pass a different --project-name."
            ) from e

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

        # Only database-level grants. The app creates the genie schema and
        # tables at startup via _ensure_schema() — since the SP executes
        # those DDL statements, it owns everything it creates.
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
        conn.close()
        print(f"  ✓ Database permissions granted to SP")
        return True
    except Exception as e:
        print(f"  ⚠ Could not connect to Lakebase for GRANTs: {e}")
        print(f"  Run these commands manually in the Lakebase SQL Editor:")
        print(f'    GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}";')
        print(f'    GRANT CREATE ON DATABASE databricks_postgres TO "{sp_client_id}";')
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
