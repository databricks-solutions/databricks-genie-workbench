"""
Genie Space creation utilities.

Creates new Genie Spaces from optimized configurations via the Databricks API.
"""

import json
import logging
import os

from backend.services.auth import get_workspace_client, get_databricks_host
from backend.sql_executor import get_sql_warehouse_id

logger = logging.getLogger(__name__)


# Fields that must be arrays of strings per the schema
_STRING_ARRAY_FIELDS = {
    "description", "content", "question", "sql", "instruction",
    "synonyms", "usage_guidance", "comment"
}

# Fields that must be arrays of objects per the schema
_OBJECT_ARRAY_FIELDS = {
    "sample_questions", "tables", "metric_views", "column_configs",
    "text_instructions", "example_question_sqls", "sql_functions",
    "join_specs", "filters", "expressions", "measures", "questions",
    "answer", "parameters"
}

# Sorting requirements per API documentation
# Maps field name -> sort key(s)
_SORT_REQUIREMENTS = {
    # Sort by 'id'
    "sample_questions": ("id",),
    "text_instructions": ("id",),
    "example_question_sqls": ("id",),
    "join_specs": ("id",),
    "filters": ("id",),
    "expressions": ("id",),
    "measures": ("id",),
    "questions": ("id",),
    # Sort by 'identifier'
    "tables": ("identifier",),
    "metric_views": ("identifier",),
    # Sort by 'column_name'
    "column_configs": ("column_name",),
    # Sort by (id, identifier) tuple
    "sql_functions": ("id", "identifier"),
}


def _enforce_constraints(config: dict) -> dict:
    """Enforce API constraints on the config.

    Fixes:
    - Text instructions: At most 1 allowed (keep first only)
    - Empty SQL snippets: Remove filters/expressions/measures with empty sql
    """
    import copy
    config = copy.deepcopy(config)

    # Limit text_instructions to 1
    instructions = config.get("instructions", {})
    text_instructions = instructions.get("text_instructions", [])
    if isinstance(text_instructions, list) and len(text_instructions) > 1:
        logger.warning(f"Truncating text_instructions from {len(text_instructions)} to 1")
        instructions["text_instructions"] = text_instructions[:1]

    # Remove sql_snippets with empty sql
    sql_snippets = instructions.get("sql_snippets", {})
    for snippet_type in ["filters", "expressions", "measures"]:
        items = sql_snippets.get(snippet_type, [])
        if isinstance(items, list):
            # Filter out items with empty sql
            filtered = []
            for item in items:
                if isinstance(item, dict):
                    sql_field = item.get("sql", [])
                    # Check if sql is non-empty
                    if sql_field and (
                        (isinstance(sql_field, list) and any(s.strip() for s in sql_field if isinstance(s, str))) or
                        (isinstance(sql_field, str) and sql_field.strip())
                    ):
                        filtered.append(item)
                    else:
                        logger.warning(f"Removing {snippet_type} item with empty sql: {item.get('id', 'unknown')}")
                else:
                    filtered.append(item)
            sql_snippets[snippet_type] = filtered

    return config


def _sort_array(items: list, sort_keys: tuple) -> list:
    """Sort an array of dicts by the specified key(s)."""
    if not items:
        return items

    # Check if all items have the required sort keys
    if not all(isinstance(item, dict) for item in items):
        return items

    def sort_key(item):
        # Build a tuple of values for multi-key sorting
        return tuple(item.get(k, "") for k in sort_keys)

    return sorted(items, key=sort_key)


def _clean_config(obj: any, key: str | None = None) -> any:
    """Recursively clean a config for API compatibility.

    Fixes:
    - Removes null values from arrays (API rejects them in repeated fields)
    - Converts string values to arrays for fields that require string arrays
    - Wraps single objects in arrays for fields that require object arrays
    - Sorts arrays by required keys (id, identifier, column_name, etc.)
    """
    if isinstance(obj, dict):
        # Check if this dict should be wrapped in an array
        if key in _OBJECT_ARRAY_FIELDS:
            # This object should be inside an array, wrap it
            return [_clean_config(obj, None)]
        return {k: _clean_config(v, k) for k, v in obj.items()}
    elif isinstance(obj, list):
        # Filter out None values and recursively clean remaining items
        cleaned = [_clean_config(item, None) for item in obj if item is not None]
        # Sort if this field has sorting requirements
        if key in _SORT_REQUIREMENTS and cleaned:
            sort_keys = _SORT_REQUIREMENTS[key]
            cleaned = _sort_array(cleaned, sort_keys)
        return cleaned
    elif isinstance(obj, str) and key in _STRING_ARRAY_FIELDS:
        # API expects these fields to be arrays of strings
        return [obj]
    else:
        return obj


def get_target_directory() -> str:
    """Get the target directory for new Genie Spaces.

    Returns:
        The workspace path from GENIE_TARGET_DIRECTORY env var

    Raises:
        ValueError: If GENIE_TARGET_DIRECTORY is not configured
    """
    target_dir = os.environ.get("GENIE_TARGET_DIRECTORY", "").strip()
    if not target_dir:
        raise ValueError(
            "GENIE_TARGET_DIRECTORY must be configured. "
            "Set it to a workspace path like /Workspace/Users/you@company.com/"
        )
    return target_dir


def create_genie_space(
    display_name: str,
    merged_config: dict,
    parent_path: str | None = None,
) -> dict:
    """Create a new Genie Space with the given configuration.

    Args:
        display_name: The display name for the new Genie Space
        merged_config: The merged configuration dict (from optimization)
        parent_path: Optional workspace path for the parent directory.
                    If not provided, uses GENIE_TARGET_DIRECTORY env var.

    Returns:
        dict with:
            - genie_space_id: ID of the created space
            - display_name: Display name of the space
            - space_url: URL to access the space in Databricks

    Raises:
        ValueError: If configuration is invalid or parent_path not provided
        PermissionError: If the app doesn't have write permission
    """
    # Resolve parent path
    if parent_path:
        target_path = parent_path.strip()
    else:
        target_path = get_target_directory()

    # Ensure path ends with /
    if not target_path.endswith("/"):
        target_path += "/"

    # Validate display name
    if not display_name or not display_name.strip():
        raise ValueError("Display name is required")

    display_name = display_name.strip()

    # Get warehouse ID (required by API)
    warehouse_id = get_sql_warehouse_id()
    if not warehouse_id:
        raise ValueError(
            "SQL_WAREHOUSE_ID must be configured to create Genie Spaces. "
            "Set it to your SQL Warehouse ID."
        )

    # Enforce API constraints (text_instructions limit, empty sql removal, etc.)
    constrained_config = _enforce_constraints(merged_config)

    # Clean up config for API compatibility (type fixes, sorting)
    cleaned_config = _clean_config(constrained_config)

    # Serialize the config to JSON string (API expects serialized_space as string)
    serialized_space = json.dumps(cleaned_config)

    client = get_workspace_client()
    host = get_databricks_host()

    logger.info(f"Creating Genie Space with title: {display_name}")
    logger.info(f"Parent path: {target_path}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    logger.info(f"Workspace host: {host}")
    logger.info(f"Serialized space length: {len(serialized_space)} chars")

    try:
        response = client.api_client.do(
            method="POST",
            path="/api/2.0/genie/spaces",
            body={
                "title": display_name,
                "description": f"Optimized Genie Space created from GenieRx",
                "parent_path": target_path,
                "warehouse_id": warehouse_id,
                "serialized_space": serialized_space,
            },
        )

        logger.info(f"API response keys: {list(response.keys()) if isinstance(response, dict) else response}")

        # Extract the space ID from response (API returns space_id)
        genie_space_id = response.get("space_id")
        if not genie_space_id:
            logger.error(f"No space_id in response. Full response: {response}")
            raise ValueError(f"API did not return a space_id. Response: {response}")

        # Build the URL to the new space
        space_url = f"{host}/genie/rooms/{genie_space_id}"

        logger.info(f"Created Genie Space: {genie_space_id}")
        logger.info(f"Space URL: {space_url}")

        return {
            "genie_space_id": genie_space_id,
            "display_name": display_name,
            "space_url": space_url,
        }

    except Exception as e:
        error_str = str(e).lower()
        logger.error(f"Failed to create Genie Space: {e}")

        # Map common errors to user-friendly messages
        if "403" in error_str or "permission" in error_str or "forbidden" in error_str:
            raise PermissionError(
                "Cannot create Genie Space. Ensure the app has write permission "
                "to the target directory."
            )
        elif "400" in error_str or "invalid" in error_str:
            raise ValueError(f"The configuration is invalid: {e}")
        elif "timeout" in error_str:
            raise TimeoutError("Request timed out. Please try again.")
        else:
            raise
