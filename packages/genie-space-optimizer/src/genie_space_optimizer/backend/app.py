import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from .core import create_app
from .core._base import LifespanDependency
from .router import router

# Import route modules so their @router decorators register on the singleton.
# The singleton router (from create_router()) collects all routes under /api/genie.
from .routes import spaces as _spaces  # noqa: F401
from .routes import runs as _runs  # noqa: F401
from .routes import activity as _activity  # noqa: F401
from .routes import settings as _settings  # noqa: F401
from .routes import trigger as _trigger  # noqa: F401
from .routes import suggestions as _suggestions  # noqa: F401

logger = logging.getLogger(__name__)


class _DeltaTableBootstrap(LifespanDependency):
    """Create optimization Delta tables on app startup so read paths never 404."""

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            from ._spark import get_spark
            from genie_space_optimizer.optimization.state import ensure_optimization_tables

            config = app.state.config
            spark = get_spark()
            ensure_optimization_tables(spark, config.catalog, config.schema_name)
            logger.info("Optimization Delta tables verified at startup")
        except Exception:
            logger.warning(
                "Could not verify optimization tables at startup — will create on first use",
                exc_info=True,
            )
        yield

    @staticmethod
    def __call__() -> None:
        return None


def _apply_uc_grants(ws, sp: str, catalog: str, schema: str) -> None:
    """Idempotent best-effort UC grants so the SP can manage tables, prompts, and models."""
    from databricks.sdk.service.catalog import (
        Privilege,
        PermissionsChange,
        SecurableType,
    )

    fqn = f"{catalog}.{schema}"

    try:
        ws.grants.update(
            securable_type=SecurableType.CATALOG,
            full_name=catalog,
            changes=[PermissionsChange(principal=sp, add=[Privilege.USE_CATALOG])],
        )
    except Exception as exc:
        logger.debug("UC grant on catalog %s: %s", catalog, str(exc)[:200])

    try:
        ws.grants.update(
            securable_type=SecurableType.SCHEMA,
            full_name=fqn,
            changes=[
                PermissionsChange(
                    principal=sp,
                    add=[
                        Privilege.USE_SCHEMA,
                        Privilege.SELECT,
                        Privilege.MODIFY,
                        Privilege.CREATE_TABLE,
                        Privilege.CREATE_FUNCTION,
                        Privilege.CREATE_MODEL,
                        Privilege.CREATE_VOLUME,
                        Privilege.EXECUTE,
                        Privilege.MANAGE,
                    ],
                )
            ],
        )
        logger.info("UC grants applied: SP=%s on %s", sp, fqn)
    except Exception as exc:
        logger.warning("UC grant on schema %s failed: %s", fqn, str(exc)[:200])

    vol_fqn = f"{fqn}.app_artifacts"
    try:
        ws.grants.update(
            securable_type=SecurableType.VOLUME,
            full_name=vol_fqn,
            changes=[
                PermissionsChange(
                    principal=sp,
                    add=[Privilege.READ_VOLUME, Privilege.WRITE_VOLUME],
                )
            ],
        )
        logger.info("UC volume grants applied: SP=%s on %s", sp, vol_fqn)
    except Exception as exc:
        logger.debug("UC grant on volume %s: %s", vol_fqn, str(exc)[:200])


class _UCGrantBootstrap(LifespanDependency):
    """Best-effort self-grant of required UC privileges at startup."""

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            ws = app.state.workspace_client
            config = app.state.config
            sp = ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
            cat = config.catalog or "main"
            sch = config.schema_name or "genie_optimization"
            if sp:
                _apply_uc_grants(ws, sp, cat, sch)
            else:
                logger.warning("UC self-grant skipped — could not determine SP client ID")
        except Exception:
            logger.warning("UC self-grant failed at startup", exc_info=True)
        yield

    @staticmethod
    def __call__() -> None:
        return None


class _JobRunAsBootstrap(LifespanDependency):
    """Verify the bundle-managed runner job's run_as matches the current SP.

    This provides self-healing after fresh deploys where deploy.sh may not
    have been run, or when the app's SP rotates.
    """

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            ws = app.state.workspace_client
            config = app.state.config
            sp_client_id = ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
            job_id = getattr(config, "job_id", None)
            if sp_client_id and job_id:
                from .job_launcher import ensure_job_run_as

                ensure_job_run_as(ws, job_id, sp_client_id)
                logger.info("Job run_as verified at startup (job=%s, SP=%s)", job_id, sp_client_id)
            elif not job_id:
                logger.warning(
                    "Job run_as check skipped — GENIE_SPACE_OPTIMIZER_JOB_ID not set"
                )
            else:
                logger.warning(
                    "Job run_as check skipped — could not determine SP client ID"
                )
        except Exception:
            logger.warning(
                "Job run_as check failed at startup — run deploy.sh to set permissions",
                exc_info=True,
            )
        yield

    @staticmethod
    def __call__() -> None:
        return None


app = create_app(routers=[router])
