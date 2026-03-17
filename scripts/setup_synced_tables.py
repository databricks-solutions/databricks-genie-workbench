"""
Setup Synced Tables for GSO Delta → Lakebase replication.

Runnable as a standalone script (CLI) or as a Databricks notebook.
No imports from the GSO package required.

Usage (CLI):
  python scripts/setup_synced_tables.py \
    --source-catalog my_catalog \
    --lakebase-catalog my_lakebase_catalog

Usage (Databricks notebook):
  Set widget parameters source_catalog, source_schema, lakebase_catalog, lakebase_schema
  then Run All.
"""

import argparse
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    NewPipelineSpec,
    SyncedTableSchedulingPolicy,
)

# ---------------------------------------------------------------------------
# Table definitions: (table_name, primary_key_columns)
# ---------------------------------------------------------------------------
TABLES = [
    ("genie_opt_runs",               ["run_id"]),
    ("genie_opt_stages",             ["run_id", "stage", "started_at"]),
    ("genie_opt_iterations",         ["run_id", "iteration", "eval_scope"]),
    ("genie_opt_patches",            ["run_id", "iteration", "lever", "patch_index"]),
    ("genie_eval_asi_results",       ["run_id", "iteration", "question_id", "judge"]),
    ("genie_opt_provenance",         ["run_id", "iteration", "lever", "question_id", "judge"]),
    ("genie_opt_suggestions",        ["suggestion_id"]),
    ("genie_opt_data_access_grants", ["grant_id"]),
]


def get_params() -> dict:
    """Get parameters from CLI args or Databricks notebook widgets."""
    try:
        # Databricks notebook mode
        dbutils = globals().get("dbutils") or __import__("IPython").get_ipython().user_ns.get("dbutils")
        if dbutils:
            dbutils.widgets.text("source_catalog", "", "Source Catalog")
            dbutils.widgets.text("source_schema", "genie_space_optimizer", "Source Schema")
            dbutils.widgets.text("lakebase_catalog", "", "Lakebase Catalog")
            dbutils.widgets.text("lakebase_schema", "gso", "Lakebase Schema")
            return {
                "source_catalog": dbutils.widgets.get("source_catalog"),
                "source_schema": dbutils.widgets.get("source_schema"),
                "lakebase_catalog": dbutils.widgets.get("lakebase_catalog"),
                "lakebase_schema": dbutils.widgets.get("lakebase_schema"),
            }
    except Exception:
        pass

    # CLI mode
    parser = argparse.ArgumentParser(description="Setup GSO Synced Tables (Delta → Lakebase)")
    parser.add_argument("--source-catalog", required=True, help="Unity Catalog name for GSO Delta tables")
    parser.add_argument("--source-schema", default="genie_space_optimizer", help="Schema containing GSO tables")
    parser.add_argument("--lakebase-catalog", required=True, help="Lakebase catalog name")
    parser.add_argument("--lakebase-schema", default="gso", help="Target schema in Lakebase")
    args = parser.parse_args()
    return {
        "source_catalog": args.source_catalog,
        "source_schema": args.source_schema,
        "lakebase_catalog": args.lakebase_catalog,
        "lakebase_schema": args.lakebase_schema,
    }


def enable_cdf(w: WorkspaceClient, source_catalog: str, source_schema: str, warehouse_id: str | None) -> None:
    """Enable Change Data Feed on all source tables (idempotent)."""
    print("\n=== Step 1: Enable Change Data Feed ===")
    for table_name, _ in TABLES:
        full_name = f"{source_catalog}.{source_schema}.{table_name}"
        sql = f"ALTER TABLE {full_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        print(f"  Enabling CDF on {full_name}...")
        try:
            w.statement_execution.execute_statement(
                statement=sql,
                warehouse_id=warehouse_id,
                wait_timeout="30s",
            )
        except Exception as e:
            # Table may not exist yet or CDF already enabled — continue
            print(f"    Warning: {e}")
    print("  CDF enabled on all tables.")


def create_lakebase_schema(w: WorkspaceClient, lakebase_catalog: str, lakebase_schema: str, warehouse_id: str | None) -> None:
    """Create the target schema in Lakebase if it doesn't exist."""
    print(f"\n=== Step 2: Create Lakebase schema '{lakebase_schema}' ===")
    sql = f"CREATE SCHEMA IF NOT EXISTS {lakebase_catalog}.{lakebase_schema}"
    try:
        w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        print(f"  Schema {lakebase_catalog}.{lakebase_schema} ready.")
    except Exception as e:
        print(f"  Warning: {e}")


def create_synced_tables(
    w: WorkspaceClient,
    source_catalog: str,
    source_schema: str,
    lakebase_catalog: str,
    lakebase_schema: str,
) -> list[str]:
    """Create synced tables for all GSO Delta tables. Returns list of synced table names."""
    print("\n=== Step 3: Create Synced Tables ===")
    created = []
    for table_name, pk_cols in TABLES:
        full_target = f"{lakebase_catalog}.{lakebase_schema}.{table_name}"
        full_source = f"{source_catalog}.{source_schema}.{table_name}"
        print(f"  Creating synced table {full_target} ← {full_source}...")
        try:
            w.database.create_synced_database_table(
                SyncedDatabaseTable(
                    name=full_target,
                    spec=SyncedTableSpec(
                        source_table_full_name=full_source,
                        primary_key_columns=pk_cols,
                        scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
                        new_pipeline_spec=NewPipelineSpec(
                            storage_catalog=source_catalog,
                            storage_schema=source_schema,
                        ),
                    ),
                )
            )
            created.append(full_target)
            print(f"    Created: {full_target}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"    Already exists: {full_target}")
                created.append(full_target)
            else:
                print(f"    Error: {e}")
    return created


def wait_for_sync(w: WorkspaceClient, synced_tables: list[str], timeout_seconds: int = 600) -> None:
    """Poll synced table status until all are synced or timeout."""
    print(f"\n=== Step 4: Waiting for initial sync (timeout {timeout_seconds}s) ===")
    start = time.time()
    pending = set(synced_tables)

    while pending and (time.time() - start) < timeout_seconds:
        for table_name in list(pending):
            try:
                status = w.database.get_synced_database_table(name=table_name)
                sync_state = status.data_synchronization_status
                if sync_state and sync_state.detailed_state:
                    state = str(sync_state.detailed_state)
                    if "ACTIVE" in state.upper() or "SUCCEEDED" in state.upper():
                        print(f"  Synced: {table_name}")
                        pending.discard(table_name)
                    elif "FAILED" in state.upper():
                        print(f"  FAILED: {table_name} — {state}")
                        pending.discard(table_name)
            except Exception as e:
                print(f"  Error checking {table_name}: {e}")

        if pending:
            elapsed = int(time.time() - start)
            print(f"  Waiting... ({len(pending)} remaining, {elapsed}s elapsed)")
            time.sleep(30)

    if pending:
        print(f"  WARNING: {len(pending)} tables did not sync within timeout: {pending}")
    else:
        print("  All tables synced successfully.")


def verify_row_counts(
    w: WorkspaceClient,
    source_catalog: str,
    source_schema: str,
    lakebase_catalog: str,
    lakebase_schema: str,
    warehouse_id: str | None,
) -> None:
    """Compare row counts between source Delta tables and Lakebase synced tables."""
    print("\n=== Step 5: Verify row counts ===")
    for table_name, _ in TABLES:
        source_full = f"{source_catalog}.{source_schema}.{table_name}"
        lakebase_full = f"{lakebase_catalog}.{lakebase_schema}.{table_name}"
        try:
            src_result = w.statement_execution.execute_statement(
                statement=f"SELECT count(*) AS cnt FROM {source_full}",
                warehouse_id=warehouse_id,
                wait_timeout="30s",
            )
            lb_result = w.statement_execution.execute_statement(
                statement=f"SELECT count(*) AS cnt FROM {lakebase_full}",
                warehouse_id=warehouse_id,
                wait_timeout="30s",
            )
            src_count = src_result.result.data_array[0][0] if src_result.result else "?"
            lb_count = lb_result.result.data_array[0][0] if lb_result.result else "?"
            match = "✓" if src_count == lb_count else "✗ MISMATCH"
            print(f"  {table_name}: source={src_count}, lakebase={lb_count} {match}")
        except Exception as e:
            print(f"  {table_name}: Error — {e}")


def main():
    params = get_params()
    source_catalog = params["source_catalog"]
    source_schema = params["source_schema"]
    lakebase_catalog = params["lakebase_catalog"]
    lakebase_schema = params["lakebase_schema"]

    if not source_catalog or not lakebase_catalog:
        print("ERROR: source_catalog and lakebase_catalog are required.")
        sys.exit(1)

    print(f"Source:   {source_catalog}.{source_schema}")
    print(f"Lakebase: {lakebase_catalog}.{lakebase_schema}")

    w = WorkspaceClient()

    # Auto-detect a warehouse for SQL operations
    warehouses = list(w.warehouses.list())
    warehouse_id = warehouses[0].id if warehouses else None
    if warehouse_id:
        print(f"Using warehouse: {warehouse_id}")
    else:
        print("WARNING: No SQL warehouse found. CDF and verification steps may fail.")

    enable_cdf(w, source_catalog, source_schema, warehouse_id)
    create_lakebase_schema(w, lakebase_catalog, lakebase_schema, warehouse_id)
    synced = create_synced_tables(w, source_catalog, source_schema, lakebase_catalog, lakebase_schema)
    if synced:
        wait_for_sync(w, synced)
        verify_row_counts(w, source_catalog, source_schema, lakebase_catalog, lakebase_schema, warehouse_id)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
