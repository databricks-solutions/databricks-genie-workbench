"""Add join specifications to a Genie Space by inferring relationships.

Inspects Unity Catalog table schemas to find foreign key relationships
and common column naming patterns (_id suffixes, shared column names),
then adds join_specs to the space config.

Usage:
    python add-joins.py <space_id>

Requires: databricks-sdk, requests
"""

import json
import sys
from itertools import combinations

import requests
from databricks.sdk import WorkspaceClient


def get_serialized_space(host: str, token: str, space_id: str) -> dict:
    """Retrieve full space config via empty PATCH."""
    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
    )
    resp.raise_for_status()
    return json.loads(resp.json()["serialized_space"])


def update_serialized_space(host: str, token: str, space_id: str, space_data: dict) -> None:
    """Write updated space config back via PATCH. Tables must be sorted."""
    space_data["data_sources"]["tables"] = sorted(
        space_data["data_sources"]["tables"],
        key=lambda t: t["identifier"],
    )
    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"serialized_space": json.dumps(space_data)},
    )
    resp.raise_for_status()
    print(f"Space {space_id} updated successfully.")


def get_table_columns(w: WorkspaceClient, identifier: str) -> list[str]:
    """Get column names from Unity Catalog."""
    parts = identifier.split(".")
    if len(parts) != 3:
        return []
    try:
        table = w.tables.get(identifier)
        return [c.name for c in (table.columns or [])]
    except Exception:
        return []


def infer_joins(
    w: WorkspaceClient,
    tables: list[dict],
) -> list[dict]:
    """Infer join relationships between tables.

    Strategies:
    1. Foreign key pattern: table_a has column "table_b_id" and table_b has "id"
    2. Shared ID columns: both tables have a column like "customer_id"
    3. Explicit FK constraints from Unity Catalog (if available)
    """
    # Build column map: identifier -> set of column names
    col_map: dict[str, set[str]] = {}
    for table in tables:
        ident = table.get("identifier", "")
        cols = get_table_columns(w, ident)
        if cols:
            col_map[ident] = set(cols)

    joins = []
    seen = set()

    for (id_a, cols_a), (id_b, cols_b) in combinations(col_map.items(), 2):
        table_name_a = id_a.split(".")[-1]
        table_name_b = id_b.split(".")[-1]

        # Strategy 1: FK naming pattern (e.g., orders.customer_id -> customers.id)
        for col in cols_a:
            if col.endswith("_id"):
                ref_name = col[:-3]  # "customer_id" -> "customer"
                # Check if ref matches the other table name (singular or plural)
                if (ref_name == table_name_b or
                    ref_name + "s" == table_name_b or
                    ref_name == table_name_b.rstrip("s")):
                    if "id" in cols_b:
                        key = tuple(sorted([id_a, id_b]))
                        if key not in seen:
                            seen.add(key)
                            joins.append({
                                "left_table": id_a,
                                "right_table": id_b,
                                "join_type": "INNER",
                                "conditions": [{"left_column": col, "right_column": "id"}],
                            })

        # Check reverse direction
        for col in cols_b:
            if col.endswith("_id"):
                ref_name = col[:-3]
                if (ref_name == table_name_a or
                    ref_name + "s" == table_name_a or
                    ref_name == table_name_a.rstrip("s")):
                    if "id" in cols_a:
                        key = tuple(sorted([id_a, id_b]))
                        if key not in seen:
                            seen.add(key)
                            joins.append({
                                "left_table": id_b,
                                "right_table": id_a,
                                "join_type": "INNER",
                                "conditions": [{"left_column": col, "right_column": "id"}],
                            })

        # Strategy 2: Shared ID columns (e.g., both have "customer_id")
        shared_ids = {c for c in cols_a & cols_b if c.endswith("_id")}
        for shared_col in shared_ids:
            key = tuple(sorted([id_a, id_b]))
            if key not in seen:
                seen.add(key)
                joins.append({
                    "left_table": id_a,
                    "right_table": id_b,
                    "join_type": "INNER",
                    "conditions": [{"left_column": shared_col, "right_column": shared_col}],
                })

    return joins


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python add-joins.py <space_id>")
        sys.exit(1)

    import os

    space_id = sys.argv[1]
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    if not host or not token:
        print("Error: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
        sys.exit(1)

    w = WorkspaceClient()
    space_data = get_serialized_space(host, token, space_id)

    tables = space_data.get("data_sources", {}).get("tables", [])
    print(f"Space has {len(tables)} tables.")

    if len(tables) < 2:
        print("Need at least 2 tables to infer joins.")
        sys.exit(0)

    # Check existing joins
    existing = space_data.get("instructions", {}).get("join_specs", [])
    if existing:
        print(f"Space already has {len(existing)} join specs. Adding new ones only.")

    # Infer joins
    inferred = infer_joins(w, tables)

    if not inferred:
        print("No join relationships could be inferred from column patterns.")
        print("You may need to add join specs manually based on your domain knowledge.")
        sys.exit(0)

    print(f"\nInferred {len(inferred)} join(s):")
    for j in inferred:
        conds = ", ".join(
            f"{c['left_column']} = {c['right_column']}" for c in j["conditions"]
        )
        left_short = j["left_table"].split(".")[-1]
        right_short = j["right_table"].split(".")[-1]
        print(f"  {left_short} -> {right_short} ON {conds}")

    # Apply
    space_data.setdefault("instructions", {})["join_specs"] = existing + inferred
    update_serialized_space(host, token, space_id, space_data)
    print(f"\nAdded {len(inferred)} join specs to the space.")
