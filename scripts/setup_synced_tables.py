"""
Setup Synced Tables for GSO Delta → Lakebase replication.

Synced tables are created in the **same catalog/schema** as the source Delta
tables, with a `_synced` suffix.  Because the Databricks SDK does not yet
support Lakebase Autoscaling project/branch fields, the synced tables must be
created manually via the Catalog Explorer UI.  This script handles:

  Step 1 — Enable Change Data Feed (CDF) on all source tables (idempotent)
  Step 2 — Print instructions for UI-based synced table creation
  Step 3 — Wait for sync (poll synced table status)
  Step 4 — Verify row counts between source and synced tables

Usage (CLI):
  # Enable CDF + print UI instructions:
  python scripts/setup_synced_tables.py \\
    --source-catalog my_catalog \\
    --warehouse-id abc123

  # Verify only (after creating synced tables via UI):
  python scripts/setup_synced_tables.py \\
    --source-catalog my_catalog \\
    --warehouse-id abc123 \\
    --verify-only

Usage (Databricks notebook):
  Set widget parameters source_catalog, source_schema then Run All.
"""

import argparse
import sys
import time

from databricks.sdk import WorkspaceClient

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

SYNCED_SUFFIX = "_synced"


def get_params() -> dict:
    """Get parameters from CLI args or Databricks notebook widgets."""
    try:
        # Databricks notebook mode
        dbutils = globals().get("dbutils") or __import__("IPython").get_ipython().user_ns.get("dbutils")
        if dbutils:
            dbutils.widgets.text("source_catalog", "", "Source Catalog")
            dbutils.widgets.text("source_schema", "genie_space_optimizer", "Source Schema")
            dbutils.widgets.dropdown("verify_only", "false", ["true", "false"], "Verify Only")
            return {
                "source_catalog": dbutils.widgets.get("source_catalog"),
                "source_schema": dbutils.widgets.get("source_schema"),
                "verify_only": dbutils.widgets.get("verify_only") == "true",
                "warehouse_id": None,
            }
    except Exception:
        pass

    # CLI mode
    parser = argparse.ArgumentParser(description="Setup GSO Synced Tables (Delta → Lakebase)")
    parser.add_argument("--source-catalog", required=True, help="Unity Catalog name for GSO Delta tables")
    parser.add_argument("--source-schema", default="genie_space_optimizer", help="Schema containing GSO tables")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile (sets DATABRICKS_CONFIG_PROFILE)")
    parser.add_argument("--warehouse-id", default=None, help="SQL Warehouse ID (skips auto-detection)")
    parser.add_argument("--verify-only", action="store_true",
                        help="Skip CDF enablement — only check sync status and row counts")
    args = parser.parse_args()

    # Set profile env var before WorkspaceClient is constructed
    if args.profile:
        import os
        os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile

    return {
        "source_catalog": args.source_catalog,
        "source_schema": args.source_schema,
        "verify_only": args.verify_only,
        "warehouse_id": args.warehouse_id,
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


def print_ui_instructions(source_catalog: str, source_schema: str) -> None:
    """Print instructions for creating synced tables via Catalog Explorer UI."""
    print("\n=== Step 2: Create Synced Tables (Manual — Catalog Explorer UI) ===")
    print()
    print("  The Databricks SDK does not yet support creating synced tables for")
    print("  Lakebase Autoscaling projects. Create them manually via the UI.")
    print()
    print("  For each table below, in Catalog Explorer:")
    print("    1. Navigate to the source table")
    print("    2. Click 'Create' → 'Synced table'")
    print("    3. Set the name (with _synced suffix, same schema)")
    print("    4. Database type: Lakebase Serverless (Autoscaling)")
    print("    5. Project: genie-workbench-db, Branch: production")
    print("    6. Sync mode: Triggered")
    print("    7. Verify primary key detection, then create")
    print()
    print(f"  Source schema: {source_catalog}.{source_schema}")
    print()
    print(f"  {'Source Table':<40} {'Synced Table Name':<45} {'Primary Keys'}")
    print(f"  {'─' * 40} {'─' * 45} {'─' * 40}")
    for table_name, pk_cols in TABLES:
        synced_name = f"{table_name}{SYNCED_SUFFIX}"
        full_synced = f"{source_catalog}.{source_schema}.{synced_name}"
        print(f"  {table_name:<40} {full_synced:<45} {', '.join(pk_cols)}")
    print()
    print("  Docs: https://docs.databricks.com/aws/en/oltp/projects/sync-tables")
    print()


def wait_for_sync(
    w: WorkspaceClient,
    source_catalog: str,
    source_schema: str,
    timeout_seconds: int = 600,
) -> None:
    """Poll synced table status until all are synced or timeout."""
    print(f"\n=== Step 3: Waiting for initial sync (timeout {timeout_seconds}s) ===")
    synced_tables = [
        f"{source_catalog}.{source_schema}.{name}{SYNCED_SUFFIX}"
        for name, _ in TABLES
    ]
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
    warehouse_id: str | None,
) -> None:
    """Compare row counts between source Delta tables and synced tables."""
    print("\n=== Step 4: Verify row counts ===")
    for table_name, _ in TABLES:
        source_full = f"{source_catalog}.{source_schema}.{table_name}"
        synced_full = f"{source_catalog}.{source_schema}.{table_name}{SYNCED_SUFFIX}"
        try:
            src_result = w.statement_execution.execute_statement(
                statement=f"SELECT count(*) AS cnt FROM {source_full}",
                warehouse_id=warehouse_id,
                wait_timeout="30s",
            )
            lb_result = w.statement_execution.execute_statement(
                statement=f"SELECT count(*) AS cnt FROM {synced_full}",
                warehouse_id=warehouse_id,
                wait_timeout="30s",
            )
            src_count = src_result.result.data_array[0][0] if src_result.result else "?"
            lb_count = lb_result.result.data_array[0][0] if lb_result.result else "?"
            match = "✓" if src_count == lb_count else "✗ MISMATCH"
            print(f"  {table_name}: source={src_count}, synced={lb_count} {match}")
        except Exception as e:
            print(f"  {table_name}: Error — {e}")


def main():
    params = get_params()
    source_catalog = params["source_catalog"]
    source_schema = params["source_schema"]
    verify_only = params["verify_only"]
    explicit_warehouse_id = params.get("warehouse_id")

    if not source_catalog:
        print("ERROR: source_catalog is required.")
        sys.exit(1)

    print(f"Source: {source_catalog}.{source_schema}")
    print(f"Synced tables: {source_catalog}.{source_schema}.*{SYNCED_SUFFIX}")

    from backend._telemetry import PRODUCT_NAME, PRODUCT_VERSION
    w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

    # Use explicit warehouse ID if provided, otherwise auto-detect
    if explicit_warehouse_id:
        warehouse_id = explicit_warehouse_id
        print(f"Using warehouse: {warehouse_id} (explicit)")
    else:
        warehouses = list(w.warehouses.list())
        warehouse_id = warehouses[0].id if warehouses else None
        if warehouse_id:
            print(f"Using warehouse: {warehouse_id} (auto-detected)")
        else:
            print("WARNING: No SQL warehouse found. CDF and verification steps may fail.")

    if not verify_only:
        enable_cdf(w, source_catalog, source_schema, warehouse_id)
        print_ui_instructions(source_catalog, source_schema)
    else:
        print("\n  --verify-only: skipping CDF enablement and UI instructions")

    wait_for_sync(w, source_catalog, source_schema)
    verify_row_counts(w, source_catalog, source_schema, warehouse_id)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
