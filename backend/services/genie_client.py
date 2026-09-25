"""
Genie Agent data ingestion utilities.

Fetches and parses Genie Agent configurations from the Databricks API.
Supports both local development (PAT) and Databricks Apps (OBO) authentication.
"""

import json
import logging
import os
import time
from copy import deepcopy
from typing import Any
from urllib.parse import quote, urlsplit

import requests

from dotenv import load_dotenv

from backend.services.auth import get_workspace_client, get_service_principal_client, is_running_on_databricks_apps
from backend.services.version_control import contracts as vc

load_dotenv()

logger = logging.getLogger(__name__)


class GenieTransport:
    """Explicit-executor VC transport; legacy read helpers remain separate."""

    def __init__(self, *, executor, authenticate, coordination, registry, flags=None,
                 warehouse_id=None, parent_path=None):
        self.executor = executor
        self.authenticate = authenticate
        self.coordination = coordination
        self.registry = registry
        self.flags = flags
        self.warehouse_id = warehouse_id
        self.parent_path = parent_path
        parsed = urlsplit(executor.host)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment or parsed.port):
            raise ValueError("Explicit HTTPS workspace origin required")
        self.host = executor.host.rstrip("/")

    def get(self, binding, executor):
        # Value-equality (not object identity): every caller re-derives a freshly
        # *verified* executor per operation (identity.executor runs a live SCIM Me
        # check each time), so the same verified principal is a new object each call.
        # The transport always authenticates as `self.executor` (its own pinned,
        # verified credential) in `_request`, so requiring the exact instance would
        # make the promotion observer path impossible while adding no security --
        # a value match on (workspace/host/principal/execution_ref) is the real pin.
        if executor != self.executor:
            raise PermissionError("Transport is pinned to its verified executor")
        self._binding(binding)
        response = self._request("GET", self._path(binding), query={"include_serialized_space": "true"})
        if "serialized_space" not in response:
            raise ValueError("Full serialized state is required")
        return response

    def create_once(self, payload, claim):
        binding = self.registry.resolve(claim.binding_id)
        if binding.space_id is not None or not self.warehouse_id:
            raise PermissionError("Create requires provisional identity and explicit warehouse")
        body = {"title": payload.title, "description": payload.description,
                "serialized_space": json.dumps(vc.to_wire(payload.serialized_space)),
                "warehouse_id": self.warehouse_id}
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        response = self._write("POST", "/api/2.0/genie/spaces", binding, body, claim)
        if not isinstance(response.get("space_id"), str) or not response["space_id"]:
            raise RuntimeError("Possible create orphan: missing physical identity")
        return vc.CreateResponse(response["space_id"], response)

    def patch_config_once(self, binding, serialized_space, claim):
        self._write("PATCH", self._path(binding), binding,
                    {"serialized_space": json.dumps(serialized_space)}, claim)

    def patch_description_once(self, binding, description, claim):
        self._write("PATCH", self._path(binding), binding, {"description": description}, claim)

    @staticmethod
    def _path(binding):
        if not binding.space_id:
            raise PermissionError("Physical binding required before GET or PATCH")
        return f"/api/2.0/genie/spaces/{quote(binding.space_id, safe='')}"

    def _binding(self, binding):
        if (binding.workspace_id != self.executor.workspace_id
                or self.registry.resolve(binding.binding_id) != binding):
            raise PermissionError("Binding or explicit target executor mismatch")

    def _write(self, method, path, binding, body, claim):
        if self.flags is None or self.flags.enabled("vc_writes_enabled") is not True:
            raise PermissionError("VC transport writes disabled")
        self._binding(binding)
        if (not isinstance(claim, vc.AdmissionClaim) or claim.binding_id != binding.binding_id
                or claim.binding_revision != binding.binding_revision):
            raise PermissionError("Bound admission claim required")
        return self._request(method, path, body=body, claim=claim)

    def _request(self, method, path, *, body=None, query=None, claim=None):
        headers = self.authenticate(self.executor)
        if not isinstance(headers, dict) or not headers.get("Authorization"):
            raise PermissionError("Explicit executor authentication unavailable")
        with requests.Session() as session:
            session.trust_env = False
            session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            if claim is not None:
                self.coordination.assert_owner(claim)
            response = session.request(method, self.host + path, headers=headers, json=body,
                params=query, timeout=(10, 60), allow_redirects=False)
            if 300 <= response.status_code < 400:
                raise RuntimeError("Redirect refused; mutation outcome requires verification")
            response.raise_for_status()
            return response.json() if response.content else {}


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


def is_scope_error(e: Exception) -> bool:
    """Check if exception is a missing OAuth scope error."""
    msg = str(e).lower()
    return "scope" in msg or "insufficient_scope" in msg


def call_with_sp_fallback(fn, *, what: str = "genie API call"):
    """Run ``fn(client)`` with the OBO/workspace client; on an OAuth scope error,
    retry once with the service principal client.

    Some Genie Conversation API endpoints (e.g. permissions, message comments)
    are not covered by the app's ``dashboards.genie`` user_api_scope, so a
    correctly-deployed app can still get ``insufficient_scope`` on those calls.
    The SP fallback keeps those reads working. We log a WARNING when it fires so
    a genuine scope/deploy misconfiguration stays visible (the request then runs
    under the SP identity rather than the user's own permissions).
    """
    client = get_workspace_client()
    try:
        return fn(client)
    except Exception as e:
        if is_scope_error(e):
            sp_client = get_service_principal_client()
            if sp_client is not client:
                logger.warning(
                    "%s: OBO token lacks required scope; falling back to the "
                    "service principal (runs under SP identity, not the user's). "
                    "Verify the app's user_api_scopes if this is unexpected.",
                    what,
                )
                return fn(sp_client)
        raise


def get_genie_space(
    genie_space_id: str | None = None,
) -> dict:
    """Fetch and parse a Genie Agent's serialized configuration.

    Uses the Databricks SDK's API client which automatically handles
    OBO authentication when running on Databricks Apps, ensuring that
    the user's permissions are checked. Users without access to the Genie
    Space will receive a 403/404 error.

    Args:
        genie_space_id: The Genie Agent ID (defaults to GENIE_SPACE_ID env var)

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
    logger.info(f"Fetching Genie Agent: {genie_space_id}")
    logger.info(f"Running on Databricks Apps: {is_running_on_databricks_apps()}")
    logger.info(f"Workspace host: {client.config.host}")
    logger.info(f"Auth type: {client.config.auth_type}")

    try:
        return _get_space_with_client(client, genie_space_id)
    except Exception as e:
        if is_scope_error(e):
            logger.info("OBO token lacks genie scope, retrying with service principal")
            sp_client = get_service_principal_client()
            if sp_client is not client:
                return _get_space_with_client(sp_client, genie_space_id)
        logger.error(f"Failed to fetch Genie Agent {genie_space_id}: {e}")
        raise ValueError(f"Unable to get agent [{genie_space_id}]. {e}")


def _get_space_with_client(client, genie_space_id: str) -> dict:
    """Fetch a single Genie Agent using the given client."""
    response = client.api_client.do(
        method="GET",
        path=f"/api/2.0/genie/spaces/{genie_space_id}",
        query={"include_serialized_space": "true"},
    )
    return response


def list_genie_spaces() -> list[dict]:
    """Fetch all Genie Agents from the Databricks API with cursor pagination.

    Returns list of dicts with: id, display_name, description, create_time, update_time
    Raises an Exception on failure (callers should handle as appropriate).
    """
    return call_with_sp_fallback(_list_spaces_with_client, what="list_genie_spaces")


def _list_spaces_with_client(client) -> list[dict]:
    """Paginate through all Genie Agents using the given client."""
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


def _parse_serialized_space_value(serialized_space: Any) -> dict:
    """Parse the API ``serialized_space`` value into a mutable dict."""
    if isinstance(serialized_space, dict):
        return deepcopy(serialized_space)
    if isinstance(serialized_space, str):
        parsed = json.loads(serialized_space)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Genie API returned an invalid serialized_space payload")


def get_serialized_space(
    genie_space_id: str | None = None,
    *,
    include_top_level_description: bool = False,
) -> dict:
    """Fetch a Genie Agent and return the parsed serialized space.

    Args:
        genie_space_id: The Genie Agent ID (defaults to GENIE_SPACE_ID env var)
        include_top_level_description: Copy the API-level agent description
            into the returned dict for read-only scoring paths. Genie stores
            the agent description outside ``serialized_space``.

    Returns:
        Parsed serialized space configuration as a dictionary
    """
    data = get_genie_space(genie_space_id=genie_space_id)
    space_data = _parse_serialized_space_value(data["serialized_space"])
    if include_top_level_description and "description" in data:
        space_data["description"] = data.get("description") or ""
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
    """Query a Genie Agent with a natural language question and retrieve generated SQL.

    Uses the Databricks Genie conversation API to start a conversation, poll for
    completion, and extract any generated SQL from the response.

    Args:
        genie_space_id: The Genie Agent ID
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
    logger.info(f"Starting Genie conversation for agent {genie_space_id}")
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
