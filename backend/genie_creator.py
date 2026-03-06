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


_FALLBACK_DIR = "/Shared/"


def get_target_directory() -> str:
    """Get the configured target directory for new Genie Spaces.

    Returns GENIE_TARGET_DIRECTORY if set, otherwise ``/Shared/``.
    """
    target_dir = os.environ.get("GENIE_TARGET_DIRECTORY", "").strip()
    return target_dir if target_dir else _FALLBACK_DIR


def _build_path_candidates(explicit_path: str | None) -> list[str]:
    """Build an ordered list of parent paths to try.

    Priority:
    1. Explicitly provided path (from the agent/user)
    2. GENIE_TARGET_DIRECTORY env var (if different from #1)
    3. /Shared/ as a last-resort fallback

    Duplicates are removed while preserving order.
    """
    candidates: list[str] = []
    seen: set[str] = set()

    def _add(p: str) -> None:
        normalized = p.rstrip("/") + "/"
        if normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)

    if explicit_path and explicit_path.strip():
        _add(explicit_path.strip())

    env_dir = os.environ.get("GENIE_TARGET_DIRECTORY", "").strip()
    if env_dir:
        _add(env_dir)

    _add(_FALLBACK_DIR)
    return candidates


def _is_permission_error(e: Exception) -> bool:
    s = str(e).lower()
    return "403" in s or "permission" in s or "forbidden" in s


def create_genie_space(
    display_name: str,
    merged_config: dict,
    parent_path: str | None = None,
) -> dict:
    """Create a new Genie Space with the given configuration.

    Attempts creation with a fallback chain of parent paths.  If the
    primary path returns a permission error, the next candidate is
    tried automatically (configured directory -> /Shared/).

    Args:
        display_name: The display name for the new Genie Space
        merged_config: The merged configuration dict (from optimization)
        parent_path: Optional workspace path for the parent directory.
                    If not provided, uses GENIE_TARGET_DIRECTORY or /Shared/.

    Returns:
        dict with genie_space_id, display_name, space_url, parent_path

    Raises:
        ValueError: If configuration is invalid
        PermissionError: If none of the candidate paths are writable
        TimeoutError: If the request timed out
    """
    if not display_name or not display_name.strip():
        raise ValueError("Display name is required")
    display_name = display_name.strip()

    warehouse_id = get_sql_warehouse_id()
    if not warehouse_id:
        raise ValueError(
            "SQL_WAREHOUSE_ID must be configured to create Genie Spaces. "
            "Set it to your SQL Warehouse ID."
        )

    constrained_config = _enforce_constraints(merged_config)
    cleaned_config = _clean_config(constrained_config)
    serialized_space = json.dumps(cleaned_config)

    client = get_workspace_client()
    host = get_databricks_host()

    candidates = _build_path_candidates(parent_path)
    last_error: Exception | None = None

    for target_path in candidates:
        logger.info(f"Attempting to create Genie Space '{display_name}' in {target_path}")

        try:
            response = client.api_client.do(
                method="POST",
                path="/api/2.0/genie/spaces",
                body={
                    "title": display_name,
                    "description": "Optimized Genie Space created from GenieRx",
                    "parent_path": target_path,
                    "warehouse_id": warehouse_id,
                    "serialized_space": serialized_space,
                },
            )

            genie_space_id = response.get("space_id")
            if not genie_space_id:
                logger.error(f"No space_id in response: {response}")
                raise ValueError(f"API did not return a space_id. Response: {response}")

            space_url = f"{host}/genie/rooms/{genie_space_id}"
            logger.info(f"Created Genie Space {genie_space_id} in {target_path}")

            return {
                "genie_space_id": genie_space_id,
                "display_name": display_name,
                "space_url": space_url,
                "parent_path": target_path,
            }

        except Exception as e:
            last_error = e
            if _is_permission_error(e) and target_path != candidates[-1]:
                logger.warning(
                    f"Permission denied for {target_path}, trying next candidate"
                )
                continue

            error_str = str(e).lower()
            if "400" in error_str or "invalid" in error_str:
                raise ValueError(f"The configuration is invalid: {e}")
            if "timeout" in error_str:
                raise TimeoutError("Request timed out. Please try again.")
            if _is_permission_error(e):
                break
            raise

    raise PermissionError(
        f"Cannot create Genie Space — no writable directory found. "
        f"Tried: {', '.join(candidates)}. "
        f"Grant the app's service principal 'Can Manage' on a workspace folder."
    )
