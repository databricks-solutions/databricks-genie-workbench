import logging
import os
from typing import Any

from databricks.sdk.service.iam import User as UserOut

from .core import Dependencies, create_router
from .models import HealthStatus, VersionOut

router = create_router()
_logger = logging.getLogger(__name__)


@router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@router.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(user_ws: Dependencies.UserClient):
    return user_ws.current_user.me()


@router.get("/health", response_model=HealthStatus, operation_id="getHealth")
def health_check(config: Dependencies.Config, ws: Dependencies.Client):
    """Probe catalog, schema, tables, SP access, and job ownership.

    Never raises -- always returns a HealthStatus so the frontend can
    render an actionable banner.
    """
    cat = config.catalog or "main"
    sch = config.schema_name or "genie_optimization"
    fqn = f"{cat}.{sch}"

    sp_client_id = ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
    sp_ref = sp_client_id or "<service_principal_application_id>"
    base: dict[str, Any] = dict(catalog=cat, schema=sch, spClientId=sp_client_id or None)

    # ── Spark connectivity ──────────────────────────────────────────
    try:
        from ._spark import get_spark
        spark = get_spark()
    except Exception as exc:
        _logger.warning("Health check: cannot get Spark session: %s", exc)
        return HealthStatus(
            healthy=False,
            catalogExists=False,
            schemaExists=False,
            tablesReady=False,
            tablesAccessible=False,
            message=(
                "Cannot connect to Spark. The app may still be starting up. "
                "Refresh the page in a minute."
            ),
            **base,
        )

    # ── Catalog existence ───────────────────────────────────────────
    catalog_exists = False
    try:
        spark.sql(f"DESCRIBE CATALOG `{cat}`")
        catalog_exists = True
    except Exception as exc:
        exc_str = str(exc)
        if "CATALOG_NOT_FOUND" in exc_str or "CATALOG_DOES_NOT_EXIST" in exc_str:
            return HealthStatus(
                healthy=False,
                catalogExists=False,
                schemaExists=False,
                tablesReady=False,
                tablesAccessible=False,
                message=(
                    f"Catalog '{cat}' does not exist. "
                    f"Create it in your workspace, or change the CATALOG "
                    f"setting in databricks.yml and re-deploy."
                ),
                **base,
            )
        _logger.debug("Health check: DESCRIBE CATALOG error: %s", exc_str[:200])
        catalog_exists = True

    # ── Schema existence ────────────────────────────────────────────
    try:
        spark.sql(f"DESCRIBE SCHEMA {fqn}")
    except Exception as exc:
        exc_str = str(exc)
        if "SCHEMA_NOT_FOUND" in exc_str:
            cmd = f"CREATE SCHEMA IF NOT EXISTS {fqn}"
            return HealthStatus(
                healthy=False,
                catalogExists=catalog_exists,
                schemaExists=False,
                tablesReady=False,
                tablesAccessible=False,
                message=(
                    f"The optimization schema {fqn} does not exist. "
                    f"Create it with the SQL command below, then re-deploy the app."
                ),
                createSchemaCommand=cmd,
                **base,
            )
        if "PERMISSION_DENIED" in exc_str or "ACCESS_DENIED" in exc_str:
            cmd = f"CREATE SCHEMA IF NOT EXISTS {fqn}"
            return HealthStatus(
                healthy=False,
                catalogExists=catalog_exists,
                schemaExists=False,
                tablesReady=False,
                tablesAccessible=False,
                message=(
                    f"Cannot verify schema {fqn} (permission denied). "
                    f"If it does not exist, create it with the SQL command below."
                ),
                createSchemaCommand=cmd,
                **base,
            )
        _logger.warning("Health check: unexpected error probing schema: %s", exc_str[:200])
        return HealthStatus(
            healthy=False, catalogExists=catalog_exists,
            schemaExists=False, tablesReady=False,
            tablesAccessible=False,
            message=f"Unexpected error checking schema: {exc_str[:200]}",
            **base,
        )

    # ── Table readiness ─────────────────────────────────────────────
    tables_ready = False
    try:
        df = spark.sql(
            f"SHOW TABLES IN {fqn} LIKE 'genie_opt_*'"
        ).toPandas()
        tables_ready = len(df) >= 3
    except Exception as exc:
        _logger.debug("Health check: SHOW TABLES failed: %s", str(exc)[:200])

    if not tables_ready:
        return HealthStatus(
            healthy=False,
            catalogExists=catalog_exists,
            schemaExists=True,
            tablesReady=False,
            tablesAccessible=False,
            message=(
                f"The optimization tables in {fqn} have not been created yet. "
                f"They are created automatically on the first optimization run. "
                f"If this persists, check that the service principal has "
                f"CREATE TABLE permission on the schema."
            ),
            grantCommand=(
                f"GRANT CREATE TABLE, CREATE VOLUME, USE SCHEMA, SELECT, MODIFY "
                f"ON SCHEMA {fqn} TO `{sp_ref}`; "
                f"GRANT READ_VOLUME, WRITE_VOLUME "
                f"ON VOLUME {fqn}.app_artifacts TO `{sp_ref}`"
            ),
            **base,
        )

    # ── Table access ────────────────────────────────────────────────
    tables_accessible = False
    try:
        spark.sql(f"SELECT 1 FROM {fqn}.genie_opt_runs LIMIT 1").collect()
        tables_accessible = True
    except Exception as exc:
        exc_str = str(exc)
        if "PERMISSION_DENIED" in exc_str or "ACCESS_DENIED" in exc_str:
            return HealthStatus(
                healthy=False,
                catalogExists=catalog_exists,
                schemaExists=True,
                tablesReady=True,
                tablesAccessible=False,
                message=(
                    f"The service principal cannot read the optimization tables "
                    f"in {fqn}. Run 'make deploy WAREHOUSE_ID=...' again to "
                    f"apply UC grants, or run the GRANT command below."
                ),
                grantCommand=(
                    f"GRANT USE CATALOG ON CATALOG `{cat}` TO `{sp_ref}`; "
                    f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {fqn} TO `{sp_ref}`; "
                    f"GRANT READ_VOLUME, WRITE_VOLUME ON VOLUME {fqn}.app_artifacts TO `{sp_ref}`"
                ),
                **base,
            )
        if "TABLE_OR_VIEW_NOT_FOUND" in exc_str:
            tables_accessible = True

    # ── Volume readiness ──────────────────────────────────────────────
    volume_ready = False
    try:
        spark.sql(f"DESCRIBE VOLUME {fqn}.app_artifacts")
        volume_ready = True
    except Exception as exc:
        exc_str = str(exc)
        if "VOLUME_NOT_FOUND" in exc_str or "VOLUME_DOES_NOT_EXIST" in exc_str:
            return HealthStatus(
                healthy=False,
                catalogExists=catalog_exists,
                schemaExists=True,
                tablesReady=tables_ready,
                tablesAccessible=tables_accessible,
                volumeReady=False,
                message=(
                    f"The artifact volume {fqn}.app_artifacts does not exist. "
                    f"It is required for storing optimization job wheels. "
                    f"Create it with the SQL command below, or re-run "
                    f"'make deploy WAREHOUSE_ID=...' which creates it automatically."
                ),
                grantCommand=f"CREATE VOLUME IF NOT EXISTS {fqn}.app_artifacts",
                **base,
            )
        if "PERMISSION_DENIED" in exc_str or "ACCESS_DENIED" in exc_str:
            return HealthStatus(
                healthy=False,
                catalogExists=catalog_exists,
                schemaExists=True,
                tablesReady=tables_ready,
                tablesAccessible=tables_accessible,
                volumeReady=False,
                message=(
                    f"The service principal cannot access the artifact volume "
                    f"in {fqn}. Grant CREATE VOLUME permission, or create the "
                    f"volume manually with the command below."
                ),
                grantCommand=f"CREATE VOLUME IF NOT EXISTS {fqn}.app_artifacts",
                **base,
            )
        _logger.debug("Health check: DESCRIBE VOLUME error: %s", exc_str[:200])
        volume_ready = True

    # ── Function/Prompt permission probe ────────────────────────────
    try:
        spark.sql(f"SHOW FUNCTIONS IN {fqn} LIKE 'genie_opt_*'")
    except Exception as exc:
        exc_str = str(exc)
        if "PERMISSION_DENIED" in exc_str or "ACCESS_DENIED" in exc_str:
            return HealthStatus(
                healthy=False,
                catalogExists=catalog_exists,
                schemaExists=True,
                tablesReady=True,
                tablesAccessible=True,
                message=(
                    f"The service principal cannot manage functions/prompts in {fqn}. "
                    f"Optimization runs will fail during prompt registration. "
                    f"Run the GRANT command below to fix."
                ),
                grantCommand=(
                    f"GRANT CREATE FUNCTION, EXECUTE, MANAGE ON SCHEMA {fqn} TO `{sp_ref}`"
                ),
                **base,
            )

    # ── Job health ────────────────────────────────────────────────
    from .job_launcher import check_job_health

    job_ok, job_msg = check_job_health(ws, sp_client_id, job_id=config.job_id)
    if not job_ok:
        return HealthStatus(
            healthy=False,
            catalogExists=catalog_exists,
            schemaExists=True,
            tablesReady=True,
            tablesAccessible=tables_accessible,
            jobHealthy=False,
            jobMessage=job_msg,
            **base,
        )

    return HealthStatus(
        healthy=True,
        catalogExists=catalog_exists,
        schemaExists=True,
        tablesReady=True,
        tablesAccessible=tables_accessible,
        volumeReady=volume_ready,
        jobHealthy=True,
        **base,
    )
