"""Settings mirror write (MV-D109 P5) — the ONLY writer of ``genie_ont_settings_mirror``.

Lakebase (``genie.genie_ont_settings``, via ``ont_settings.save_settings``) stays the
source of truth and the app never reads this table. After every successful Settings
save the route also writes the saved Settings to one Delta row per workspace, so the
param-less nightly job (which cannot read Lakebase) scopes to the saved catalogs.

Identity: the write runs as the SERVICE PRINCIPAL — the SP owns app-state bookkeeping
tables, as for the apply audit row (``apply._write_audit_row``). ``updated_by`` records
the OBO email of the admin who saved. Every value is bound as a
``StatementParameterListItem``; nothing is interpolated into the SQL text. The job only
reads the row (``genie_space_optimizer.ontology.settings_mirror.mirror_read_sql``).

``write_mirror`` raises on failure; the caller (the Settings PUT) logs and continues,
so a mirror failure never fails the save (MV-D43).
"""

from __future__ import annotations

import json
import logging
import os
import time

from backend.ontology.models import OntologySettings

logger = logging.getLogger(__name__)


def _fqn(table: str) -> str:
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _merge_sql(target: str) -> str:
    """Idempotent single-row MERGE keyed on ``workspace_id``; named parameters only."""
    return (
        f"MERGE INTO {target} AS t USING (SELECT :workspace_id AS workspace_id) AS s "
        "ON t.workspace_id = s.workspace_id "
        "WHEN MATCHED THEN UPDATE SET metastore_id = :metastore_id, "
        "settings_json = :settings_json, updated_by = :updated_by, "
        "updated_at = current_timestamp() "
        "WHEN NOT MATCHED THEN INSERT (workspace_id, metastore_id, settings_json, "
        "updated_by, updated_at) VALUES (s.workspace_id, :metastore_id, :settings_json, "
        ":updated_by, current_timestamp())"
    )


def _execute(client, warehouse_id: str, statement: str, params=None) -> None:
    """Run one statement on the warehouse and poll to a terminal state (decisions.py loop)."""
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, parameters=params, wait_timeout="30s",
    )
    statement_id = resp.statement_id if resp else None
    deadline = time.monotonic() + 40
    while resp and resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline or not statement_id:
            raise RuntimeError("settings mirror write timed out")
        time.sleep(1.0)
        resp = client.statement_execution.get_statement(statement_id=statement_id)
    if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
        detail = getattr(getattr(resp, "status", None), "error", None)
        raise RuntimeError(f"settings mirror write failed: {detail}")


def write_mirror(
    settings: OntologySettings, *, workspace_id: str, metastore_id: str, updated_by: str
) -> None:
    """Mirror the saved Settings to Delta as the SP. Creates the table first (the app
    may save before the job ever ran). Raises on failure; the caller decides."""
    from databricks.sdk.service.sql import StatementParameterListItem
    from genie_space_optimizer.ontology import ddl

    from backend.services.auth import get_service_principal_client

    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        raise RuntimeError("SQL_WAREHOUSE_ID is not configured; cannot mirror settings")
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    if not catalog:
        raise RuntimeError("GSO_CATALOG is not configured; cannot mirror settings")

    client = get_service_principal_client()
    create = ddl.all_ddl(catalog, schema)[ddl.TABLE_ONT_SETTINGS_MIRROR]
    _execute(client, warehouse_id, create)

    params = [
        StatementParameterListItem(name="workspace_id", value=workspace_id),
        StatementParameterListItem(name="metastore_id", value=metastore_id),
        StatementParameterListItem(
            name="settings_json", value=json.dumps(settings.model_dump(mode="json"), sort_keys=True)
        ),
        StatementParameterListItem(name="updated_by", value=updated_by),
    ]
    _execute(client, warehouse_id, _merge_sql(_fqn(ddl.TABLE_ONT_SETTINGS_MIRROR)), params)
    logger.info("ontology settings mirrored for workspace %s by %s", workspace_id, updated_by)
