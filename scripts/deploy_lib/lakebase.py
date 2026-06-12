"""Lakebase Autoscaling provisioning for notebook installs."""

from __future__ import annotations

from .config import InstallConfig, LakebaseInfo


def find_soft_deleted_project(w, project_name: str):
    """Return the soft-deleted project holding this name, if any.

    Deleted Lakebase projects enter a 7-day soft-deleted state during which
    their IDs stay reserved, so a same-name create fails with ALREADY_EXISTS
    while get_project keeps returning NotFound.
    """
    resource_name = f"projects/{project_name}"
    for project in w.postgres.list_projects(show_deleted=True):
        if project.name == resource_name and project.delete_time:
            return project
    return None


def purge_soft_deleted_project(w, project_name: str) -> None:
    resource_name = f"projects/{project_name}"
    print(
        f"Lakebase project '{project_name}' is soft-deleted and its name is reserved. "
        "Permanently deleting it (data is unrecoverable) before recreating..."
    )
    try:
        w.postgres.delete_project(name=resource_name, purge=True).wait()
    except Exception as exc:
        raise RuntimeError(
            f"Could not purge soft-deleted Lakebase project '{project_name}': {exc}. "
            "Purge it manually with "
            f"`databricks postgres delete-project projects/{project_name} --purge`, "
            "restore it with "
            f"`databricks postgres undelete-project projects/{project_name}`, "
            "or set the lakebase_project_name widget to a different name."
        ) from exc


def ensure_project(w, project_name: str) -> str:
    from databricks.sdk.errors import AlreadyExists, NotFound
    from databricks.sdk.service.postgres import Project, ProjectSpec

    resource_name = f"projects/{project_name}"
    try:
        w.postgres.get_project(name=resource_name)
        return resource_name
    except NotFound:
        pass

    if find_soft_deleted_project(w, project_name):
        purge_soft_deleted_project(w, project_name)

    try:
        op = w.postgres.create_project(
            project=Project(spec=ProjectSpec()),
            project_id=project_name,
        )
        op.wait()
    except Exception as exc:
        is_already_exists = isinstance(exc, AlreadyExists) or (
            "ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower()
        )
        if not is_already_exists:
            raise
        # ALREADY_EXISTS after a NotFound get means the name is reserved by a
        # project we cannot see (e.g., soft-deleted under another owner).
        try:
            w.postgres.get_project(name=resource_name)
        except NotFound:
            raise RuntimeError(
                f"Lakebase project name '{project_name}' is reserved but the project "
                "is not accessible — most likely a soft-deleted project owned by "
                "another user (deleted project IDs are reserved for 7 days). "
                "Ask its owner to purge it with "
                f"`databricks postgres delete-project projects/{project_name} --purge`, "
                "or set the lakebase_project_name widget to a different name."
            ) from exc
    return resource_name


def require_project(w, project_name: str) -> str:
    from databricks.sdk.errors import NotFound

    resource_name = f"projects/{project_name}"
    try:
        w.postgres.get_project(name=resource_name)
    except NotFound as exc:
        raise RuntimeError(
            f"Lakebase project '{project_name}' does not exist. "
            "Choose lakebase_mode=create to create a new project, or fix the "
            "lakebase_project_name widget to an existing project name."
        ) from exc
    return resource_name


def ensure_role(w, project_name: str, sp_client_id: str) -> None:
    from databricks.sdk.errors import AlreadyExists
    from databricks.sdk.service.postgres import (
        Role,
        RoleAuthMethod,
        RoleIdentityType,
        RoleRoleSpec,
    )

    branch_path = f"projects/{project_name}/branches/production"
    role_id = sp_client_id if sp_client_id[0].isalpha() else f"sp-{sp_client_id}"
    try:
        op = w.postgres.create_role(
            parent=branch_path,
            role=Role(
                spec=RoleRoleSpec(
                    identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                    auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                    postgres_role=sp_client_id,
                ),
            ),
            role_id=role_id,
        )
        op.wait()
    except AlreadyExists:
        pass
    except Exception as exc:
        if "ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc).lower():
            raise


def _api_do(w, method: str, path: str, body: dict | None = None):
    return w.api_client.do(method=method, path=path, body=body)


def get_database_resource(w, project_name: str) -> str | None:
    data = _api_do(
        w,
        "GET",
        f"/api/2.0/postgres/projects/{project_name}/branches/production/databases",
    )
    databases = data.get("databases") or []
    if not databases:
        return None
    return databases[0].get("name")


def grant_database_permissions(
    w,
    project_name: str,
    sp_client_id: str,
    *,
    endpoint_name: str = "primary",
) -> bool:
    endpoint_resource = f"projects/{project_name}/branches/production/endpoints/{endpoint_name}"
    try:
        endpoint = w.postgres.get_endpoint(name=endpoint_resource)
        hosts = endpoint.status and endpoint.status.hosts
        host = hosts.host if hosts else None
        if not host:
            return False

        credential = w.postgres.generate_database_credential(endpoint=endpoint_resource)
        token = credential.token
        if not token:
            return False

        deployer_user = w.config.client_id
        if not deployer_user:
            me = w.current_user.me()
            deployer_user = me.user_name or (me.emails[0].value if me.emails else "")
        if not deployer_user:
            return False

        import psycopg

        conn = psycopg.connect(
            host=host,
            port=5432,
            dbname="databricks_postgres",
            user=deployer_user,
            password=token,
            sslmode="require",
        )
        conn.autocommit = True
        try:
            conn.execute(f'GRANT CONNECT ON DATABASE databricks_postgres TO "{sp_client_id}"')
            conn.execute(f'GRANT CREATE ON DATABASE databricks_postgres TO "{sp_client_id}"')
        finally:
            conn.close()
        return True
    except Exception:
        return False


def ensure_lakebase(w, cfg: InstallConfig, app_sp_client_id: str) -> LakebaseInfo:
    if not cfg.lakebase_instance:
        raise ValueError("lakebase_instance is required")
    if cfg.lakebase_mode == "existing":
        require_project(w, cfg.lakebase_instance)
    else:
        ensure_project(w, cfg.lakebase_instance)
    ensure_role(w, cfg.lakebase_instance, app_sp_client_id)
    database_resource = get_database_resource(w, cfg.lakebase_instance)
    grants_applied = grant_database_permissions(w, cfg.lakebase_instance, app_sp_client_id)
    branch_resource = f"projects/{cfg.lakebase_instance}/branches/production"
    endpoint_resource = f"{branch_resource}/endpoints/primary"
    return LakebaseInfo(
        project_name=cfg.lakebase_instance,
        branch_resource=branch_resource,
        database_resource=database_resource,
        endpoint_resource=endpoint_resource,
        grants_applied=grants_applied,
    )
