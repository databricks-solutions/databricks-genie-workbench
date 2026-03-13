"""Add text instructions to a Genie Space from table metadata.

Generates context-rich instructions by inspecting Unity Catalog table schemas
and applying them to the space's serialized_space config.

Usage:
    python add-instructions.py <space_id>

Requires: databricks-sdk, requests
"""

import json
import sys
import uuid

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


def build_instructions_from_tables(w: WorkspaceClient, tables: list[dict]) -> str:
    """Generate instruction text from Unity Catalog table metadata."""
    lines = ["This space contains the following tables:\n\n"]

    for table_entry in tables:
        identifier = table_entry.get("identifier", "")
        parts = identifier.split(".")
        if len(parts) != 3:
            lines.append(f"- {identifier}\n")
            continue

        catalog, schema, table_name = parts
        lines.append(f"### {table_name}\n")

        try:
            uc_table = w.tables.get(f"{catalog}.{schema}.{table_name}")
            if uc_table.comment:
                lines.append(f"{uc_table.comment}\n\n")

            # List key columns with descriptions
            if uc_table.columns:
                described = [c for c in uc_table.columns if c.comment]
                if described:
                    lines.append("Key columns:\n")
                    for col in described[:10]:  # Cap at 10 to keep instructions readable
                        lines.append(f"- **{col.name}**: {col.comment}\n")
                    lines.append("\n")
        except Exception as e:
            lines.append(f"(Could not fetch metadata: {e})\n\n")

    return "".join(lines)


def apply_column_descriptions(w: WorkspaceClient, space_data: dict) -> int:
    """Pull column descriptions from UC and apply to space config. Returns count added."""
    count = 0
    for table_entry in space_data.get("data_sources", {}).get("tables", []):
        parts = table_entry.get("identifier", "").split(".")
        if len(parts) != 3:
            continue

        catalog, schema, table_name = parts
        try:
            uc_table = w.tables.get(f"{catalog}.{schema}.{table_name}")
            uc_col_map = {c.name: c.comment for c in (uc_table.columns or []) if c.comment}

            for col in table_entry.get("columns", []):
                if not col.get("description") and col.get("name") in uc_col_map:
                    col["description"] = uc_col_map[col["name"]]
                    count += 1
        except Exception:
            pass

    return count


def apply_table_descriptions(w: WorkspaceClient, space_data: dict) -> int:
    """Pull table descriptions from UC and apply to space config. Returns count added."""
    count = 0
    for table_entry in space_data.get("data_sources", {}).get("tables", []):
        if table_entry.get("description"):
            continue

        parts = table_entry.get("identifier", "").split(".")
        if len(parts) != 3:
            continue

        catalog, schema, table_name = parts
        try:
            uc_table = w.tables.get(f"{catalog}.{schema}.{table_name}")
            if uc_table.comment:
                table_entry["description"] = uc_table.comment
                count += 1
        except Exception:
            pass

    return count


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python add-instructions.py <space_id>")
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

    # 1. Generate and add text instructions
    instruction_text = build_instructions_from_tables(w, tables)
    instruction = {
        "id": str(uuid.uuid4()),
        "content": instruction_text.split("\n"),
    }
    space_data.setdefault("instructions", {}).setdefault("text_instructions", []).append(instruction)
    print(f"Added text instruction ({len(instruction_text)} chars).")

    # 2. Pull table descriptions from UC
    table_desc_count = apply_table_descriptions(w, space_data)
    print(f"Added {table_desc_count} table descriptions from UC.")

    # 3. Pull column descriptions from UC
    col_desc_count = apply_column_descriptions(w, space_data)
    print(f"Added {col_desc_count} column descriptions from UC.")

    # 4. Write back
    update_serialized_space(host, token, space_id, space_data)
