"""
Genie Space creation utilities.

Creates new Genie Spaces from optimized configurations via the Databricks API.
"""

import json
import logging
import os
import re

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
    # Sort by 'name'
    "parameters": ("name",),
}


_MAX_STRING_CHARS = 25_000        # per-string character limit (API enforced)
_MAX_SERIALIZED_BYTES = 3_500_000 # 3.5 MB total
_MAX_TABLES = 30
_SIZE_CHECKED_FIELDS = frozenset({
    "description", "content", "question", "sql",
    "instruction", "synonyms", "usage_guidance", "comment",
})


def _enforce_constraints(config: dict) -> dict:
    """Enforce API constraints on the config.

    Fixes:
    - Text instructions: At most 1 allowed (keep first only)
    - Empty SQL snippets: Remove filters/expressions/measures with empty sql
    - String values in size-checked fields truncated to <= 25,000 chars
    - Tables capped at 30
    - Column configs: Remove empty column_name entries and deduplicate
    - Instruction IDs: Deduplicate across all instruction array scopes
    - Question IDs: Deduplicate across sample_questions and benchmarks
    - Serialized output capped at 3.5 MB (warning only)
    """
    import copy
    config = copy.deepcopy(config)

    # Limit text_instructions to 1
    instructions = config.get("instructions", {})
    text_instructions = instructions.get("text_instructions", [])
    if isinstance(text_instructions, list) and len(text_instructions) > 1:
        logger.warning(f"Truncating text_instructions from {len(text_instructions)} to 1")
        instructions["text_instructions"] = text_instructions[:1]

    # Cap tables at 30
    tables = config.get("data_sources", {}).get("tables", [])
    if isinstance(tables, list) and len(tables) > _MAX_TABLES:
        logger.warning("Truncating tables from %d to %d", len(tables), _MAX_TABLES)
        config["data_sources"]["tables"] = tables[:_MAX_TABLES]
        tables = config["data_sources"]["tables"]

    # Remove sql_snippets with empty sql
    sql_snippets = instructions.get("sql_snippets", {})
    for snippet_type in ["filters", "expressions", "measures"]:
        items = sql_snippets.get(snippet_type, [])
        if isinstance(items, list):
            filtered = []
            for item in items:
                if isinstance(item, dict):
                    sql_field = item.get("sql", [])
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

    # Deduplicate column_configs and remove entries with empty column_name
    for tbl in tables:
        ccs = tbl.get("column_configs", [])
        if not isinstance(ccs, list):
            continue
        seen_cols: set[str] = set()
        deduped = []
        for cc in ccs:
            if not isinstance(cc, dict):
                continue
            col_name = cc.get("column_name", "")
            if not col_name:
                logger.warning("Removing column_config with empty column_name in table %s",
                               tbl.get("identifier", "?"))
                continue
            if col_name in seen_cols:
                logger.warning("Removing duplicate column_config '%s' in table %s",
                               col_name, tbl.get("identifier", "?"))
                continue
            seen_cols.add(col_name)
            deduped.append(cc)
        tbl["column_configs"] = deduped

    # Deduplicate instruction IDs across all instruction scopes
    seen_iids: set[str] = set()
    for arr_name in ("text_instructions", "example_question_sqls", "sql_functions", "join_specs"):
        items = instructions.get(arr_name, [])
        if isinstance(items, list):
            deduped = []
            for item in items:
                iid = item.get("id", "") if isinstance(item, dict) else ""
                if iid and iid in seen_iids:
                    logger.warning("Removing duplicate instruction id '%s' in %s", iid, arr_name)
                    continue
                if iid:
                    seen_iids.add(iid)
                deduped.append(item)
            instructions[arr_name] = deduped
    for stype in ("filters", "expressions", "measures"):
        items = sql_snippets.get(stype, [])
        if isinstance(items, list):
            deduped = []
            for item in items:
                iid = item.get("id", "") if isinstance(item, dict) else ""
                if iid and iid in seen_iids:
                    logger.warning("Removing duplicate instruction id '%s' in %s", iid, stype)
                    continue
                if iid:
                    seen_iids.add(iid)
                deduped.append(item)
            sql_snippets[stype] = deduped

    # Deduplicate question IDs across sample_questions and benchmarks.questions
    seen_qids: set[str] = set()
    config_block = config.get("config", {})
    sqs = config_block.get("sample_questions", [])
    if isinstance(sqs, list):
        deduped = []
        for sq in sqs:
            qid = sq.get("id", "") if isinstance(sq, dict) else ""
            if qid and qid in seen_qids:
                logger.warning("Removing duplicate question id '%s' in sample_questions", qid)
                continue
            if qid:
                seen_qids.add(qid)
            deduped.append(sq)
        config_block["sample_questions"] = deduped
    bench = config.get("benchmarks", {})
    bqs = bench.get("questions", [])
    if isinstance(bqs, list):
        deduped = []
        for bq in bqs:
            qid = bq.get("id", "") if isinstance(bq, dict) else ""
            if qid and qid in seen_qids:
                logger.warning("Removing duplicate question id '%s' in benchmarks.questions", qid)
                continue
            if qid:
                seen_qids.add(qid)
            deduped.append(bq)
        bench["questions"] = deduped

    # Normalize join relationship types to uppercase underscores
    _normalize_join_relationships(config)

    # Truncate oversized strings in size-checked fields
    _truncate_oversized_strings(config)

    # Warn if total serialized size is close to / over limit
    serialized_size = len(json.dumps(config).encode("utf-8"))
    if serialized_size > _MAX_SERIALIZED_BYTES:
        logger.error(
            "Serialized config is %s bytes — exceeds 3.5 MB limit. "
            "The API may reject this request.", f"{serialized_size:,}"
        )

    return config


def _truncate_oversized_strings(obj: any, field_name: str = "") -> None:
    """Walk the config tree and truncate strings exceeding the character limit in-place."""
    if isinstance(obj, dict):
        for k in obj:
            v = obj[k]
            if isinstance(v, str) and k in _SIZE_CHECKED_FIELDS:
                if len(v) > _MAX_STRING_CHARS:
                    logger.warning(
                        "Truncating %s from %d to %d chars", k, len(v), _MAX_STRING_CHARS,
                    )
                    obj[k] = v[:_MAX_STRING_CHARS]
            else:
                _truncate_oversized_strings(v, k)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, str) and field_name in _SIZE_CHECKED_FIELDS:
                if len(item) > _MAX_STRING_CHARS:
                    logger.warning(
                        "Truncating %s[%d] from %d to %d chars",
                        field_name, i, len(item), _MAX_STRING_CHARS,
                    )
                    obj[i] = item[:_MAX_STRING_CHARS]
            else:
                _truncate_oversized_strings(item, field_name)


_RT_PATTERN = re.compile(r"--rt=FROM_RELATIONSHIP_TYPE_([^-]+)--")


def _normalize_join_relationships(config: dict) -> None:
    """Fix join_spec relationship tags in-place.

    The Genie API requires FROM_RELATIONSHIP_TYPE_MANY_TO_ONE (uppercase
    underscores), but the LLM may produce many-to-one (lowercase hyphens).
    """
    join_specs = (
        config.get("instructions", {}).get("join_specs", [])
    )
    if not isinstance(join_specs, list):
        return
    for js in join_specs:
        sql_lines = js.get("sql", [])
        if not isinstance(sql_lines, list):
            continue
        for i, line in enumerate(sql_lines):
            if not isinstance(line, str) or "--rt=" not in line:
                continue
            m = _RT_PATTERN.search(line)
            if m:
                original = m.group(1)
                normalized = original.upper().replace("-", "_")
                if normalized != original:
                    sql_lines[i] = line.replace(original, normalized)
                    logger.info("Normalized join relationship: %s -> %s", original, normalized)


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
    import time as _time

    if not display_name or not display_name.strip():
        raise ValueError("Display name is required")
    display_name = display_name.strip()

    warehouse_id = get_sql_warehouse_id()
    if not warehouse_id:
        raise ValueError(
            "No SQL warehouse available. Ensure you have access to at least "
            "one running Pro or Serverless SQL warehouse."
        )

    t0 = _time.monotonic()
    constrained_config = _enforce_constraints(merged_config)
    cleaned_config = _clean_config(constrained_config)
    serialized_space = json.dumps(cleaned_config)
    t_prep = _time.monotonic() - t0
    logger.info("Config prep took %.2fs (serialized %d bytes)", t_prep, len(serialized_space))

    client = get_workspace_client()
    host = get_databricks_host()

    candidates = _build_path_candidates(parent_path)
    last_error: Exception | None = None

    for target_path in candidates:
        logger.info(f"Attempting to create Genie Space '{display_name}' in {target_path}")

        try:
            t_api = _time.monotonic()
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
            t_api_done = _time.monotonic() - t_api

            genie_space_id = response.get("space_id")
            if not genie_space_id:
                logger.error(f"No space_id in response: {response}")
                raise ValueError(f"API did not return a space_id. Response: {response}")

            space_url = f"{host}/genie/rooms/{genie_space_id}"
            logger.info("Created Genie Space %s in %s (API call %.2fs)", genie_space_id, target_path, t_api_done)

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
