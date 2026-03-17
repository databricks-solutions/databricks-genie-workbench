"""Lakebase (Databricks Database) integration: config, engine, session, and dependency."""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncGenerator, TypeAlias

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from fastapi import FastAPI, Request
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Engine, create_engine, event
from sqlmodel import Session, SQLModel, text

from ._base import LifespanDependency
from ._config import logger


# --- Database Config ---


class DatabaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="")

    port: int = Field(
        description="The port of the database", default=5432, validation_alias="PGPORT"
    )
    database_name: str = Field(
        description="The name of the database", default="databricks_postgres"
    )
    instance_name: str = Field(
        description="The name of the database instance", validation_alias="PGAPPNAME"
    )


# --- Engine creation ---


def _get_dev_db_port() -> int | None:
    """Check for APX_DEV_DB_PORT environment variable for local development."""
    port = os.environ.get("APX_DEV_DB_PORT")
    return int(port) if port else None


def _build_engine_url(
    db_config: DatabaseConfig, ws: WorkspaceClient, dev_port: int | None
) -> str:
    """Build the database engine URL for dev or production mode."""
    if dev_port:
        logger.info(f"Using local dev database at localhost:{dev_port}")
        username = "postgres"
        password = os.environ.get("APX_DEV_DB_PWD")
        if password is None:
            raise ValueError(
                "APX server didn't provide a password, please check the dev server logs"
            )
        return f"postgresql+psycopg://{username}:{password}@localhost:{dev_port}/postgres?sslmode=disable"

    # Production mode: use Databricks Database
    logger.info(f"Using Databricks database instance: {db_config.instance_name}")
    instance = ws.database.get_database_instance(db_config.instance_name)
    prefix = "postgresql+psycopg"
    host = instance.read_write_dns
    port = db_config.port
    database = db_config.database_name
    username = (
        ws.config.client_id if ws.config.client_id else ws.current_user.me().user_name
    )
    return f"{prefix}://{username}:@{host}:{port}/{database}"


def create_db_engine(db_config: DatabaseConfig, ws: WorkspaceClient) -> Engine:
    """
    Create a SQLAlchemy engine.

    In dev mode: no SSL, no password callback.
    In production: require SSL and use Databricks credential callback.
    """
    dev_port = _get_dev_db_port()
    engine_url = _build_engine_url(db_config, ws, dev_port)

    engine_kwargs: dict[str, Any] = {"pool_size": 4, "pool_recycle": 45 * 60}

    if not dev_port:
        engine_kwargs["connect_args"] = {"sslmode": "require"}

    engine = create_engine(engine_url, **engine_kwargs)

    def before_connect(dialect, conn_rec, cargs, cparams):
        cred = ws.database.generate_database_credential(
            instance_names=[db_config.instance_name]
        )
        cparams["password"] = cred.token

    if not dev_port:
        event.listens_for(engine, "do_connect")(before_connect)

    return engine


def validate_db(engine: Engine, db_config: DatabaseConfig) -> None:
    """Validate that the database connection works."""
    dev_port = _get_dev_db_port()

    if dev_port:
        logger.info(f"Validating local dev database connection at localhost:{dev_port}")
    else:
        logger.info(
            f"Validating database connection to instance {db_config.instance_name}"
        )
        try:
            ws = WorkspaceClient()
            ws.database.get_database_instance(db_config.instance_name)
        except NotFound:
            raise ValueError(
                f"Database instance {db_config.instance_name} does not exist"
            )

    try:
        with Session(engine) as session:
            session.connection().execute(text("SELECT 1"))
            session.close()
    except Exception:
        raise ConnectionError("Failed to connect to the database")

    if dev_port:
        logger.info("Local dev database connection validated successfully")
    else:
        logger.info(
            f"Database connection to instance {db_config.instance_name} validated successfully"
        )


def initialize_models(engine: Engine) -> None:
    """Create all SQLModel tables."""
    logger.info("Initializing database models")
    SQLModel.metadata.create_all(engine)
    logger.info("Database models initialized successfully")


# --- Dependency ---


class _LakebaseDependency(LifespanDependency):
    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            db_config = DatabaseConfig()  # ty: ignore[missing-argument]
            ws = app.state.workspace_client

            engine = create_db_engine(db_config, ws)
            validate_db(engine, db_config)
            initialize_models(engine)

            app.state.engine = engine
        except Exception:
            logger.warning(
                "Lakebase database unavailable — app will run without DB features. "
                "Check that the database resource is provisioned and the SP has access.",
                exc_info=True,
            )
            app.state.engine = None

        yield

        engine = getattr(app.state, "engine", None)
        if engine is not None:
            engine.dispose()

    @staticmethod
    def __call__(request: Request) -> Generator[Session, None, None]:
        with Session(bind=request.app.state.engine) as session:
            yield session


LakebaseDependency: TypeAlias = Annotated[Session, _LakebaseDependency.depends()]
