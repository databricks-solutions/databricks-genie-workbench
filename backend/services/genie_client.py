"""
Genie Space data ingestion utilities.

Fetches and parses Genie Space configurations from the Databricks API.
Supports both local development (PAT) and Databricks Apps (OBO) authentication.
"""

import json
import logging
import os
import time
from typing import Any

from dotenv import load_dotenv

from backend.services.auth import get_workspace_client, get_service_principal_client, is_running_on_databricks_apps

load_dotenv()

logger = logging.getLogger(__name__)


def _enum_value_upper(value: Any) -> str:
    """Normalize SDK enum-like values for stable comparisons."""
    raw = getattr(value, "value", value)
    return str(raw or "").upper()


def _identifier_leaf(identifier: str) -> str:
    return identifier.replace("`", "").split(".")[-1].lower()


def _entry_declares_metric_view(entry: dict) -> bool:
    for key in ("table_type", "type", "object_type"):
        if "METRIC_VIEW" in _enum_value_upper(entry.get(key)):
            return True
    return False


def _uc_metric_view_status(client, identifier: str) -> bool | None:
    """Return whether UC says *identifier* is a metric view, or None if unknown."""
    if not client or not identifier:
        return None
    try:
        info = client.tables.get(full_name=identifier)
    except Exception as exc:
        logger.debug("Unable to fetch UC table type for %s: %s", identifier, exc)
        return None

    table_type = _enum_value_upper(getattr(info, "table_type", None))
    if not table_type:
        return None
    return "METRIC_VIEW" in table_type


def normalize_metric_view_sources(space_data: dict, client=None) -> dict:
    """Move metric-view entries returned under data_sources.tables into metric_views.

    Some Genie API responses flatten metric views into ``data_sources.tables`` even
    when the submitted serialized_space used ``data_sources.metric_views``. The
    Workbench UI and scorer expect the schema shape documented for serialized_space,
    so normalize fetched configs back to that shape.
    """
    data_sources = space_data.get("data_sources")
    if not isinstance(data_sources, dict):
        return space_data

    tables = data_sources.get("tables", [])
    metric_views = data_sources.get("metric_views", [])
    if not isinstance(tables, list):
        return space_data
    if not isinstance(metric_views, list):
        metric_views = []

    normalized_tables: list[Any] = []
    normalized_metric_views = [
        mv for mv in metric_views if isinstance(mv, dict)
    ]
    metric_view_ids = {
        mv.get("identifier")
        for mv in normalized_metric_views
        if isinstance(mv.get("identifier"), str)
    }
    moved = 0

    for table in tables:
        if not isinstance(table, dict):
            normalized_tables.append(table)
            continue

        identifier = table.get("identifier")
        if not isinstance(identifier, str) or not identifier:
            normalized_tables.append(table)
            continue

        if identifier in metric_view_ids:
            moved += 1
            continue

        uc_status = _uc_metric_view_status(client, identifier)
        is_metric_view = (
            _entry_declares_metric_view(table)
            or uc_status is True
            or (uc_status is None and _identifier_leaf(identifier).startswith("mv_"))
        )

        if is_metric_view:
            normalized_metric_views.append(table)
            metric_view_ids.add(identifier)
            moved += 1
        else:
            normalized_tables.append(table)

    if moved:
        normalized_tables.sort(key=lambda x: x.get("identifier", "") if isinstance(x, dict) else "")
        normalized_metric_views.sort(key=lambda x: x.get("identifier", "") if isinstance(x, dict) else "")
        data_sources["tables"] = normalized_tables
        data_sources["metric_views"] = normalized_metric_views
        logger.info("Normalized %d metric view(s) from data_sources.tables", moved)

    return space_data


def _is_scope_error(e: Exception) -> bool:
    """Check if exception is a missing OAuth scope error."""
    msg = str(e).lower()
    return "scope" in msg or "insufficient_scope" in msg


def get_genie_space(
    genie_space_id: str | None = None,
) -> dict:
    """Fetch and parse a Genie space's serialized configuration.

    Uses the Databricks SDK's API client which automatically handles
    OBO authentication when running on Databricks Apps, ensuring that
    the user's permissions are checked. Users without access to the Genie
    Space will receive a 403/404 error.

    Args:
        genie_space_id: The Genie space ID (defaults to GENIE_SPACE_ID env var)

    Returns:
        Parsed serialized space configuration as a dictionary

    Raises:
        Exception: If the API request fails (e.g., 403 for no access)
    """
    genie_space_id = genie_space_id or os.environ.get("GENIE_SPACE_ID")
    if not genie_space_id:
        raise ValueError("genie_space_id is required")

    # Use SDK's API client - handles OBO auth automatically
    client = get_workspace_client()

    # Log diagnostic info for debugging
    logger.info(f"Fetching Genie Space: {genie_space_id}")
    logger.info(f"Running on Databricks Apps: {is_running_on_databricks_apps()}")
    logger.info(f"Workspace host: {client.config.host}")
    logger.info(f"Auth type: {client.config.auth_type}")

    try:
        return _get_space_with_client(client, genie_space_id)
    except Exception as e:
        if _is_scope_error(e):
            logger.info("OBO token lacks genie scope, retrying with service principal")
            sp_client = get_service_principal_client()
            if sp_client is not client:
                return _get_space_with_client(sp_client, genie_space_id)
        logger.error(f"Failed to fetch Genie Space {genie_space_id}: {e}")
        raise ValueError(f"Unable to get space [{genie_space_id}]. {e}")


def _get_space_with_client(client, genie_space_id: str) -> dict:
    """Fetch a single Genie space using the given client."""
    response = client.api_client.do(
        method="GET",
        path=f"/api/2.0/genie/spaces/{genie_space_id}",
        query={"include_serialized_space": "true"},
    )
    return response


def list_genie_spaces() -> list[dict]:
    """Fetch all Genie Spaces from the Databricks API with cursor pagination.

    Returns list of dicts with: id, display_name, description, create_time, update_time
    Raises an Exception on failure (callers should handle as appropriate).
    """
    client = get_workspace_client()
    try:
        return _list_spaces_with_client(client)
    except Exception as e:
        if _is_scope_error(e):
            logger.info("OBO token lacks genie scope, retrying with service principal")
            sp_client = get_service_principal_client()
            if sp_client is not client:
                return _list_spaces_with_client(sp_client)
        raise


def _list_spaces_with_client(client) -> list[dict]:
    """Paginate through all Genie Spaces using the given client."""
    spaces = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token

        response = client.api_client.do(
            method="GET",
            path="/api/2.0/genie/spaces",
            query=params,
        )

        items = response.get("spaces", [])
        spaces.extend(items)

        page_token = response.get("next_page_token")
        if not page_token or not items:
            break

    return spaces


def get_serialized_space(genie_space_id: str | None = None) -> dict:
    """Fetch a Genie space and return the parsed serialized space.

    Args:
        genie_space_id: The Genie space ID (defaults to GENIE_SPACE_ID env var)

    Returns:
        Parsed serialized space configuration as a dictionary
    """
    data = get_genie_space(genie_space_id=genie_space_id)
    space_data = json.loads(data["serialized_space"])
    try:
        client = get_workspace_client()
    except Exception:
        client = None
    return normalize_metric_view_sources(space_data, client=client)


def query_genie_for_sql(
    genie_space_id: str,
    question: str,
    timeout_seconds: int = 120,
    poll_interval_seconds: float = 2.0,
) -> dict:
    """Query a Genie Space with a natural language question and retrieve generated SQL.

    Uses the Databricks Genie conversation API to start a conversation, poll for
    completion, and extract any generated SQL from the response.

    Args:
        genie_space_id: The Genie space ID
        question: Natural language question to ask Genie
        timeout_seconds: Maximum time to wait for response (default 120s)
        poll_interval_seconds: Time between status polls (default 2s)

    Returns:
        dict with keys:
            - sql: Generated SQL string (or None if no SQL generated)
            - status: Final status ("COMPLETED", "FAILED", etc.)
            - error: Error message if failed
            - conversation_id: ID of the conversation
            - message_id: ID of the message

    Raises:
        ValueError: If parameters are invalid
        TimeoutError: If response not received within timeout
    """
    if not genie_space_id:
        raise ValueError("genie_space_id is required")
    if not question:
        raise ValueError("question is required")

    client = get_workspace_client()

    # Step 1: Start conversation
    logger.info(f"Starting Genie conversation for space {genie_space_id}")
    logger.info(f"Question: {question[:100]}...")

    start_response = client.api_client.do(
        method="POST",
        path=f"/api/2.0/genie/spaces/{genie_space_id}/start-conversation",
        body={"content": question},
    )

    # Response contains nested conversation and message objects
    conversation_id = start_response["conversation"]["id"]
    message_id = start_response["message"]["id"]

    logger.info(f"Started conversation {conversation_id}, message {message_id}")

    # Step 2: Poll for completion
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(f"Genie query timed out after {timeout_seconds}s")

        message_response = client.api_client.do(
            method="GET",
            path=f"/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages/{message_id}",
        )

        status = message_response.get("status")
        logger.debug(f"Poll status: {status} (elapsed: {elapsed:.1f}s)")

        if status == "COMPLETED":
            # Extract SQL from attachments
            # Each attachment has: text, query (the SQL string), attachment_id
            attachments = message_response.get("attachments", [])
            sql = None
            for attachment in attachments:
                # The query field contains the SQL statement directly
                if "query" in attachment:
                    query_value = attachment["query"]
                    # Handle both cases: query as string or as nested object
                    if isinstance(query_value, str):
                        sql = query_value
                    elif isinstance(query_value, dict):
                        sql = query_value.get("query")
                    if sql:
                        break

            logger.info(f"Genie query completed, SQL found: {sql is not None}")

            return {
                "sql": sql,
                "status": status,
                "error": None,
                "conversation_id": conversation_id,
                "message_id": message_id,
            }

        elif status in ("FAILED", "CANCELLED"):
            error_msg = message_response.get("error", "Unknown error")
            logger.warning(f"Genie query failed: {error_msg}")
            return {
                "sql": None,
                "status": status,
                "error": error_msg,
                "conversation_id": conversation_id,
                "message_id": message_id,
            }

        # Still in progress (IN_PROGRESS, EXECUTING_QUERY, etc.), wait and poll again
        time.sleep(poll_interval_seconds)
