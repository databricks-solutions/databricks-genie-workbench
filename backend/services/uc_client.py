"""Unity Catalog browser for the Create Wizard."""
import logging
from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)


def list_catalogs() -> list[dict]:
    try:
        client = get_workspace_client()
        return [{"name": c.name, "comment": c.comment} for c in client.catalogs.list()]
    except Exception as e:
        logger.error(f"list_catalogs failed: {e}")
        return []


def list_schemas(catalog: str) -> list[dict]:
    try:
        client = get_workspace_client()
        return [{"name": s.name, "catalog_name": s.catalog_name, "comment": s.comment}
                for s in client.schemas.list(catalog_name=catalog)]
    except Exception as e:
        logger.error(f"list_schemas({catalog}) failed: {e}")
        return []


def list_tables(catalog: str, schema: str) -> list[dict]:
    try:
        client = get_workspace_client()
        return [
            {
                "name": t.name,
                "full_name": t.full_name,
                "catalog_name": t.catalog_name,
                "schema_name": t.schema_name,
                "comment": t.comment,
                "table_type": str(t.table_type) if t.table_type else None,
            }
            for t in client.tables.list(catalog_name=catalog, schema_name=schema)
        ]
    except Exception as e:
        logger.error(f"list_tables({catalog}.{schema}) failed: {e}")
        return []


def get_table_columns(catalog: str, schema: str, table: str) -> list[dict]:
    try:
        client = get_workspace_client()
        t = client.tables.get(f"{catalog}.{schema}.{table}")
        return [
            {
                "name": col.name,
                "type": str(col.type_text or col.type_name or ""),
                "comment": col.comment,
            }
            for col in (t.columns or [])
        ]
    except Exception as e:
        logger.error(f"get_table_columns({catalog}.{schema}.{table}) failed: {e}")
        return []
