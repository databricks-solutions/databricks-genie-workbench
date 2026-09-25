#!/usr/bin/env python3
"""Provision the observe-only VC control plane for a single-SP nonprod deploy.

The full ``provision_sandbox.py`` path stands up six least-privilege service
principals and a governed ``vc-provision`` job for the complete governed-write
model. The observe-only surface is far smaller: it needs just the four control
tables + the snapshots Volume, all readable/writable by the ONE app service
principal (the app SP is the ambient identity on Databricks Apps).

This script renders the reviewed owner DDL in ``backend/version_control_ddl/``
(it authors no DDL itself) and the least-privilege grants, then executes them
against a SQL warehouse. Re-running is safe: every CREATE is ``IF NOT EXISTS``,
constraints are DROP-then-ADD, and grants are idempotent.

Prerequisite (catalog-owner authority, NOT done here): the catalog + control
schema must already exist and the app SP must hold ``USE CATALOG`` / ``USE
SCHEMA`` on them. Those are admin/owner grants; this script only creates the
objects and grants object-level privileges on them.

Usage:
    python scripts/version_control/provision_observe_nonprod.py \
        --profile <cli-profile> \
        --warehouse-id <sql-warehouse-id> \
        --catalog <catalog> \
        --control-schema <schema> \
        --app-sp <app-service-principal-application-id> \
        [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_DDL_ROOT = Path(__file__).resolve().parents[2] / "backend" / "version_control_ddl"
# The observe surface only touches these objects (versions ledger, registry,
# coordination, operation facts, snapshot Volume). Approval/promotion Volumes are
# not part of the reads-only observe scope and are intentionally skipped.
_OBSERVE_DDL = ("01-versions.sql", "02-registry.sql", "03-snapshots-volume.sql",
                "05-coordination.sql", "06-operations.sql", "09-version-tags.sql")
_OBSERVE_TABLES = ("genie_space_versions", "genie_space_registry",
                   "genie_ops_coordination", "genie_space_operations",
                   "genie_space_version_tags")
_OBSERVE_VOLUME = "vc_snapshots"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PRINCIPAL = re.compile(r"[A-Za-z0-9._@-]+")


def _split_statements(text: str) -> list[str]:
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return [part.strip() for part in body.split(";") if part.strip()]


def _render(statement: str, catalog: str, control_schema: str) -> str:
    rendered = (statement.replace("${catalog}", f"`{catalog}`")
                         .replace("${control_schema}", f"`{control_schema}`"))
    if "${" in rendered:
        raise ValueError("Unresolved template placeholder in owner migration")
    return rendered


def build_statements(catalog: str, control_schema: str, app_sp: str) -> list[str]:
    """Return the rendered DDL + grant statements for the observe control plane."""
    if not (_IDENTIFIER.fullmatch(catalog) and _IDENTIFIER.fullmatch(control_schema)):
        raise ValueError("Catalog and control schema must be simple identifiers")
    if not _PRINCIPAL.fullmatch(app_sp):
        raise ValueError(f"Unsafe or empty principal identifier: {app_sp!r}")

    statements: list[str] = []
    for filename in _OBSERVE_DDL:
        path = _DDL_ROOT / filename
        for statement in _split_statements(path.read_text()):
            statements.append(_render(statement, catalog, control_schema))

    table = f"`{catalog}`.`{control_schema}`"
    principal = f"`{app_sp}`"
    # Single app SP holds SELECT+MODIFY on every control table (append-only tables
    # + non-owner => append-only in practice; coordination is CAS-guarded in code)
    # and READ/WRITE on the snapshots Volume.
    for name in _OBSERVE_TABLES:
        statements.append(f"GRANT SELECT, MODIFY ON TABLE {table}.{name} TO {principal}")
    statements.append(
        f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {table}.{_OBSERVE_VOLUME} TO {principal}")
    return statements


def _execute_all(statements: list[str], *, profile: str | None, warehouse_id: str) -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    for statement in statements:
        response = client.statement_execution.execute_statement(
            statement=statement, warehouse_id=warehouse_id, wait_timeout="50s")
        state = response.status.state if response.status else None
        if state != StatementState.SUCCEEDED:
            message = response.status.error.message if response.status and response.status.error else state
            raise RuntimeError(f"Statement failed ({state}): {message}\n  SQL: {statement}")
        print(f"  ok: {statement[:90]}{'…' if len(statement) > 90 else ''}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=None, help="Databricks CLI profile (default: env/ambient)")
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--control-schema", required=True)
    parser.add_argument("--app-sp", required=True, help="App service principal applicationId")
    parser.add_argument("--dry-run", action="store_true", help="Print statements, do not execute")
    args = parser.parse_args(argv)

    statements = build_statements(args.catalog, args.control_schema, args.app_sp)
    print(f"Observe control plane: {len(statements)} statements for "
          f"{args.catalog}.{args.control_schema} (app SP {args.app_sp})\n")
    if args.dry_run:
        for statement in statements:
            print(statement + ";")
        return 0
    _execute_all(statements, profile=args.profile, warehouse_id=args.warehouse_id)
    print("\nObserve control plane provisioned.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
