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
import sys
import time


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
        project = w.postgres.get_project(name=resource_name)
        print(f"  ✓ Lakebase project exists: {project_name}")
        return resource_name
    except NotFound:
        pass

    print(f"  Creating Lakebase project '{project_name}'...")
    try:
        w.postgres.create_project(
            project=Project(spec=ProjectSpec()),
            project_id=project_name,
        )
    except AlreadyExists:
        print(f"  ✓ Project already exists (race condition): {project_name}")
        return resource_name

    # Poll until project is ready (up to 3 minutes)
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(10)
        try:
            project = w.postgres.get_project(name=resource_name)
            status = project.status
            if status and hasattr(status, 'state') and str(status.state) == 'ACTIVE':
                print(f"  ✓ Lakebase project ready: {project_name}")
                return resource_name
            state_str = str(status.state) if status and hasattr(status, 'state') else 'UNKNOWN'
            print(f"    ... project state: {state_str}")
        except Exception as e:
            print(f"    ... waiting for project: {e}")

    print(f"  ⚠ Project may not be fully ready after 3 minutes. Continuing anyway.")
    return resource_name


def _ensure_role(w, project_name: str, sp_client_id: str):
    """Create a Postgres role for the app's service principal."""
    from databricks.sdk.errors import AlreadyExists
    from databricks.sdk.service.postgres import (
        Role, RoleRoleSpec, RoleIdentityType, RoleAuthMethod, RoleAttributes,
    )

    branch_path = f"projects/{project_name}/branches/production"

    print(f"  Creating Postgres role for SP '{sp_client_id[:8]}...'...")
    try:
        w.postgres.create_role(
            parent=branch_path,
            role=Role(spec=RoleRoleSpec(
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                postgres_role=sp_client_id,
                attributes=RoleAttributes(createdb=True),
            )),
            role_id=sp_client_id,
        )
        print(f"  ✓ Postgres role created for SP")
    except AlreadyExists:
        print(f"  ✓ Postgres role already exists for SP")
    except Exception as e:
        # Some SDK versions may have different error types
        if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"  ✓ Postgres role already exists for SP")
        else:
            raise


def _grant_permissions(w, project_name: str, sp_client_id: str, endpoint_name: str):
    """Grant database permissions to the SP via a direct Postgres connection.

    Uses psycopg to connect as the deployer and run GRANT commands.
    Requires psycopg[binary] to be installed.
    """
    endpoint_path = f"projects/{project_name}/branches/production/endpoints/{endpoint_name}"

    # Resolve endpoint host
    print(f"  Resolving endpoint DNS...")
    endpoint = w.postgres.get_endpoint(name=endpoint_path)
    host = endpoint.status and endpoint.status.hosts and endpoint.status.hosts.host
    if not host:
        print(f"  ⚠ Endpoint has no host yet — grants will be applied on next deploy.")
        return

    # Generate deployer credential
    cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
    token = cred.token
    if not token:
        print(f"  ⚠ Could not generate database credential — grants will need manual setup.")
        return

    # Determine deployer username
    deployer_user = w.config.client_id
    if not deployer_user:
        try:
            me = w.current_user.me()
            deployer_user = me.user_name or me.emails[0].value if me.emails else ""
        except Exception:
            pass
    if not deployer_user:
        print(f"  ⚠ Could not determine deployer username — grants will need manual setup.")
        return

    try:
        import psycopg
    except ImportError:
        print(f"  ⚠ psycopg not installed. Install with: pip install 'psycopg[binary]'")
        print(f"  Run these GRANT commands manually in the Lakebase SQL Editor:")
        print(f'    GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}";')
        print(f'    GRANT CREATE, USAGE ON SCHEMA public TO "{sp_client_id}";')
        return

    print(f"  Connecting to Lakebase as {deployer_user[:12]}... to run GRANTs...")
    try:
        conn = psycopg.connect(
            host=host, port=5432, dbname="databricks_postgres",
            user=deployer_user, password=token, sslmode="require",
        )
        conn.autocommit = True

        grants = [
            f'GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}"',
            f'GRANT CREATE, USAGE ON SCHEMA public TO "{sp_client_id}"',
        ]
        for grant in grants:
            try:
                conn.execute(grant)
            except Exception as e:
                # Grant may already exist or role may already have the privilege
                if "already" in str(e).lower():
                    pass
                else:
                    print(f"    ⚠ {grant}: {e}")
        conn.close()
        print(f"  ✓ Database permissions granted to SP")
    except Exception as e:
        print(f"  ⚠ Could not connect to Lakebase for GRANTs: {e}")
        print(f"  Run these commands manually in the Lakebase SQL Editor:")
        print(f'    GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}";')
        print(f'    GRANT CREATE, USAGE ON SCHEMA public TO "{sp_client_id}";')


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
    _grant_permissions(w, args.project_name, args.sp_client_id, args.endpoint_name)

    # Output the endpoint path for resource attachment
    endpoint_path = f"projects/{args.project_name}/branches/production/endpoints/{args.endpoint_name}"
    print(f"\n  Endpoint path: {endpoint_path}")


if __name__ == "__main__":
    main()
