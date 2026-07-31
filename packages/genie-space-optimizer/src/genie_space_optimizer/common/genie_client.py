"""
Genie Agent API wrapper.

All Genie Agent API interactions. Every function takes ``WorkspaceClient``
as its first argument (APX pattern: dependency injection, no global state).
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, cast

from databricks.sdk import WorkspaceClient

from databricks.sdk.errors.platform import ResourceExhausted

from .config import (
    BENCHMARK_WINDOW_MAX,
    BENCHMARK_WINDOW_MIN,
    GENIE_MAX_WAIT,
    GENIE_POLL_INITIAL,
    GENIE_POLL_MAX,
    GENIE_RATE_LIMIT_BASE_DELAY,
    GENIE_RATE_LIMIT_RETRIES,
    KNOWN_INTERNAL_RUNTIME_KEYS,
    NON_EXPORTABLE_FIELDS,
    is_runtime_key,
    scoring_v2_is_legacy,
)

logger = logging.getLogger(__name__)


# ── Space Discovery & Config ───────────────────────────────────────────


class MissingSerializedSpaceError(RuntimeError):
    """Raised when Genie returns a space without an exportable config."""


_MISSING = object()


def _space_from_config(config: dict | None) -> dict:
    """Return the parsed serialized space from a fetch result or snapshot."""
    if not isinstance(config, dict):
        return {}

    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return parsed

    ss = config.get("serialized_space")
    if isinstance(ss, str):
        try:
            loaded = json.loads(ss)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    if isinstance(ss, dict):
        return ss

    if any(key in config for key in ("data_sources", "instructions", "config")):
        return config
    return {}


def drop_empty_text_instruction_placeholders(config: dict | None) -> int:
    """Remove exported text-instruction rows that contain no content.

    Genie can export an empty UI placeholder as an ID-only row even though
    ``content`` is required when ``serialized_space`` is validated or patched.
    The row carries no instruction semantics, so normalize it to the canonical
    representation for "no text instructions": an empty list. Non-empty rows
    are left untouched and remain subject to normal schema validation.

    Returns the number of placeholders removed.
    """
    parsed = _space_from_config(config)
    instructions = parsed.get("instructions") if isinstance(parsed, dict) else None
    if not isinstance(instructions, dict):
        return 0
    text_instructions = instructions.get("text_instructions")
    if not isinstance(text_instructions, list):
        return 0

    kept: list[Any] = []
    removed = 0
    for instruction in text_instructions:
        if isinstance(instruction, dict) and not instruction.get("content"):
            removed += 1
        else:
            kept.append(instruction)
    if removed:
        instructions["text_instructions"] = kept
    return removed


def _serialized_space_for_patch(config: dict) -> dict:
    """Return the parsed ``serialized_space`` object expected by PATCH.

    Most callers already pass the parsed config directly, but history and
    snapshot paths can hand around the raw Genie Agent API response, where the
    exportable config is nested under ``serialized_space`` / ``_parsed_space``.
    The PATCH body must contain the parsed serialized-space object itself.
    """
    parsed = _space_from_config(config)
    return parsed if parsed else config


def space_config_data_source_counts(config: dict | None) -> dict[str, int]:
    """Return counts for exported Genie data-source collections."""
    parsed = _space_from_config(config)
    ds = parsed.get("data_sources") if isinstance(parsed, dict) else None
    if not isinstance(ds, dict):
        return {"tables": 0, "metric_views": 0, "functions": 0}
    counts: dict[str, int] = {}
    for key in ("tables", "metric_views", "functions"):
        values = ds.get(key)
        counts[key] = len(values) if isinstance(values, list) else 0
    return counts


def space_config_has_data_sources(config: dict | None) -> bool:
    """Return True when a config has at least one table, MV, or TVF."""
    return any(space_config_data_source_counts(config).values())


def space_config_has_tables(config: dict | None) -> bool:
    """Return True when a config snapshot has at least one table entry."""
    return space_config_data_source_counts(config).get("tables", 0) > 0


def list_spaces(w: WorkspaceClient) -> list[dict[str, str]]:
    """List available Genie Agents via SDK, paginating through all pages.

    Returns a list of ``{"id": ..., "title": ...}`` dicts.
    """
    all_spaces: list[dict[str, str]] = []
    page_token: str | None = None
    while True:
        resp = w.genie.list_spaces(page_size=100, page_token=page_token)
        for s in resp.spaces or []:
            all_spaces.append({"id": s.space_id, "title": s.title})
        page_token = resp.next_page_token
        if not page_token:
            break
    return all_spaces


EDITABLE_PERMISSIONS = {"CAN_MANAGE", "CAN_EDIT"}


# ── REST-based permission helpers ───────────────────────────────────────


def get_space_permissions_rest(w: WorkspaceClient, space_id: str) -> dict | None:
    """Fetch Genie Agent ACL via REST API.

    Returns the raw JSON response dict, or ``None`` on failure.
    Prefer this over ``permissions.get()`` SDK which requires specific
    OAuth scopes that OBO tokens may lack.
    """
    try:
        resp = w.api_client.do("GET", f"/api/2.0/permissions/genie/{space_id}")
        return resp if isinstance(resp, dict) else None
    except Exception:
        return None


def _check_user_edit_from_rest_acl(
    acl_response: dict, user_email: str, user_groups: set[str],
) -> bool:
    """Return True if *user_email* has CAN_MANAGE or CAN_EDIT in a REST ACL response."""
    for entry in acl_response.get("access_control_list", []):
        principal = (
            entry.get("user_name") or entry.get("group_name") or ""
        ).lower()
        is_me = (
            principal == user_email
            or principal in user_groups
            or entry.get("group_name") == "admins"
        )
        if not is_me:
            continue
        for p in entry.get("all_permissions", []):
            level = str(p.get("permission_level", ""))
            if level in EDITABLE_PERMISSIONS:
                return True
    return False


def _check_sp_manage_from_rest_acl(
    acl_response: dict, sp_aliases: set[str],
) -> bool:
    """Return True if any SP alias has CAN_MANAGE in a REST ACL response."""
    sp_aliases_lower = {a.lower() for a in sp_aliases}
    acl_principals = []
    for entry in acl_response.get("access_control_list", []):
        principal = (
            entry.get("user_name") or entry.get("group_name")
            or entry.get("service_principal_name") or ""
        ).lower()
        acl_principals.append(principal)
        if principal not in sp_aliases_lower:
            continue
        for p in entry.get("all_permissions", []):
            if str(p.get("permission_level", "")) == "CAN_MANAGE":
                logger.debug(
                    "SP alias %r matched ACL principal %r with CAN_MANAGE",
                    principal, principal,
                )
                return True
    logger.debug(
        "No SP alias matched CAN_MANAGE. SP aliases: %s, ACL principals: %s",
        sp_aliases_lower, acl_principals,
    )
    return False


def _check_user_manage_from_rest_acl(
    acl_response: dict, user_email: str, user_groups: set[str],
) -> bool:
    """Return True if *user_email* has CAN_MANAGE (not just CAN_EDIT) in a REST ACL."""
    for entry in acl_response.get("access_control_list", []):
        principal = (
            entry.get("user_name") or entry.get("group_name") or ""
        ).lower()
        is_me = (
            principal == user_email
            or principal in user_groups
            or entry.get("group_name") == "admins"
        )
        if not is_me:
            continue
        for p in entry.get("all_permissions", []):
            if str(p.get("permission_level", "")) == "CAN_MANAGE":
                return True
    return False


def _check_user_edit_from_perms(
    perms, user_email: str, user_groups: set[str],
) -> bool:
    """Return True if *user_email* has CAN_MANAGE or CAN_EDIT in SDK *perms*."""
    for acl in getattr(perms, "access_control_list", None) or []:
        principal = (acl.user_name or acl.group_name or "").lower()
        is_me = (
            principal == user_email
            or principal in user_groups
            or acl.group_name == "admins"
        )
        if not is_me:
            continue
        for p in acl.all_permissions or []:
            if str(p.permission_level).replace("PermissionLevel.", "") in EDITABLE_PERMISSIONS:
                return True
    return False


_PERMISSION_RANK = {"CAN_MANAGE": 3, "CAN_EDIT": 2, "CAN_VIEW": 1, "CAN_RUN": 1}


def _get_user_access_level_from_rest_acl(
    acl_response: dict, user_email: str, user_groups: set[str],
) -> str | None:
    """Return the user's highest permission level from a REST ACL response."""
    best: str | None = None
    best_rank = 0
    for entry in acl_response.get("access_control_list", []):
        principal = (
            entry.get("user_name") or entry.get("group_name") or ""
        ).lower()
        is_me = (
            principal == user_email
            or principal in user_groups
            or entry.get("group_name") == "admins"
        )
        if not is_me:
            continue
        for p in entry.get("all_permissions", []):
            level = str(p.get("permission_level", ""))
            rank = _PERMISSION_RANK.get(level, 0)
            if rank > best_rank:
                best_rank = rank
                best = level
    if best and best == "CAN_RUN":
        best = "CAN_VIEW"
    return best


def get_user_access_level(
    w: WorkspaceClient,
    space_id: str,
    *,
    user_email: str | None = None,
    user_groups: set[str] | None = None,
    acl_client: WorkspaceClient | None = None,
) -> str | None:
    """Return the user's highest permission on a Genie Agent.

    Returns ``"CAN_MANAGE"``, ``"CAN_EDIT"``, ``"CAN_VIEW"``, or ``None``.
    """
    try:
        if not user_email:
            me = w.current_user.me()
            user_email = (me.user_name or "").lower()
            if user_groups is None and me.groups:
                user_groups = {g.display.lower() for g in me.groups if g.display}
        else:
            user_email = user_email.lower()
        user_groups = user_groups or set()

        for client in [w, acl_client] if acl_client else [w]:
            acl_resp = get_space_permissions_rest(client, space_id)
            if acl_resp is not None:
                return _get_user_access_level_from_rest_acl(acl_resp, user_email, user_groups)

        return None
    except Exception:
        logger.warning("Could not determine access level for space %s", space_id)
        return None


def user_can_edit_space(
    w: WorkspaceClient,
    space_id: str,
    *,
    user_email: str | None = None,
    user_groups: set[str] | None = None,
    acl_client: WorkspaceClient | None = None,
    cached_perms: dict | object | None = None,
) -> bool:
    """Check whether a user has CAN_MANAGE or CAN_EDIT on a Genie Agent.

    Uses REST API ``GET /api/2.0/permissions/genie/{id}`` via the OBO
    client first, then falls back to the SP client.  The ``cached_perms``
    parameter accepts either a raw REST dict or an SDK ``ObjectPermissions``.
    """
    try:
        if not user_email:
            me = w.current_user.me()
            user_email = (me.user_name or "").lower()
            if user_groups is None and me.groups:
                user_groups = {g.display.lower() for g in me.groups if g.display}
        else:
            user_email = user_email.lower()
        user_groups = user_groups or set()

        if cached_perms is not None:
            if isinstance(cached_perms, dict):
                return _check_user_edit_from_rest_acl(cached_perms, user_email, user_groups)
            return _check_user_edit_from_perms(cached_perms, user_email, user_groups)

        # OBO REST first, SP REST fallback
        for client in [w, acl_client] if acl_client else [w]:
            acl_resp = get_space_permissions_rest(client, space_id)
            if acl_resp is not None:
                return _check_user_edit_from_rest_acl(acl_resp, user_email, user_groups)

        return False
    except Exception:
        logger.warning("Could not check permissions for space %s — hiding", space_id)
        return False


def sp_can_manage_space(
    w: WorkspaceClient, space_id: str, sp_aliases: set[str],
    cached_perms: dict | None = None,
    sp_client: WorkspaceClient | None = None,
) -> bool:
    """Check whether a service principal has CAN_MANAGE on a Genie Agent.

    Uses REST API ``GET /api/2.0/permissions/genie/{id}``.
    Accepts a pre-fetched REST dict via ``cached_perms``.

    Checks the primary client (typically OBO) first, then falls back to
    *sp_client*.  The SP client is tried even when the primary client
    returns a valid ACL, because OBO tokens may return a filtered view
    that omits the service principal's own ACL entry.
    """
    acl_resp = cached_perms or get_space_permissions_rest(w, space_id)
    if acl_resp is not None and _check_sp_manage_from_rest_acl(acl_resp, sp_aliases):
        return True
    # OBO ACL was empty or didn't show SP with CAN_MANAGE — try SP client
    if sp_client is not None:
        sp_acl = get_space_permissions_rest(sp_client, space_id)
        if sp_acl is not None:
            return _check_sp_manage_from_rest_acl(sp_acl, sp_aliases)
    return False


def fetch_space_config(w: WorkspaceClient, space_id: str) -> dict:
    """GET Genie Agent config with full serialized_space content.

    Returns the raw API response augmented with convenience keys:
    ``_parsed_space``, ``_tables``, ``_metric_views``, ``_functions``,
    ``_instructions``.

    Raises:
        MissingSerializedSpaceError: if Genie returns a 200 response without
            the requested ``serialized_space`` export, or with an empty export.
    """
    raw_config = w.api_client.do(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        query={"include_serialized_space": "true"},
    )
    if not isinstance(raw_config, dict):
        raise RuntimeError(
            f"Unexpected Genie Agent response type: {type(raw_config).__name__}"
        )
    config = cast(dict[str, Any], raw_config)

    ss = config.get("serialized_space", _MISSING)
    if ss is _MISSING or ss is None or ss == "":
        logger.error(
            "Genie Agent %s response omitted serialized_space despite "
            "include_serialized_space=true; response keys=%s",
            space_id,
            sorted(config.keys()),
        )
        raise MissingSerializedSpaceError(
            f"Genie Agent {space_id} response omitted serialized_space; "
            "the caller must retry with a client that can export the space config."
        )
    if isinstance(ss, str):
        if not ss.strip():
            logger.error(
                "Genie Agent %s response returned empty serialized_space string",
                space_id,
            )
            raise MissingSerializedSpaceError(
                f"Genie Agent {space_id} response returned empty serialized_space"
            )
        ss = json.loads(ss)
    if not isinstance(ss, dict) or not ss:
        logger.error(
            "Genie Agent %s response had empty/invalid serialized_space "
            "despite include_serialized_space=true; type=%s",
            space_id,
            type(ss).__name__,
        )
        raise MissingSerializedSpaceError(
            f"Genie Agent {space_id} response had empty serialized_space; "
            "the caller must reject this snapshot."
        )
    config["_parsed_space"] = ss

    removed_placeholders = drop_empty_text_instruction_placeholders(config)
    if removed_placeholders:
        logger.warning(
            "Normalized %d empty text-instruction placeholder(s) exported by "
            "Genie Agent %s",
            removed_placeholders,
            space_id,
        )

    ds = ss.get("data_sources", {})
    if isinstance(ds, dict):
        tables_list = ds.get("tables", [])
        mvs_list = ds.get("metric_views", [])
        funcs_list = ds.get("functions", [])
    else:
        tables_list, mvs_list, funcs_list = [], [], []

    from genie_space_optimizer.common.genie_schema import normalize_join_spec_sql

    for _js_source in (ds, ss.get("instructions", {})):
        if not isinstance(_js_source, dict):
            continue
        for _js in _js_source.get("join_specs", []):
            if isinstance(_js, dict):
                normalize_join_spec_sql(_js)

    instr = ss.get("instructions", {})
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    has_instructions = bool(text_instr) or bool(config.get("description", ""))

    config["_tables"] = [t.get("identifier", "") for t in tables_list if isinstance(t, dict)]
    config["_metric_views"] = [m.get("identifier", "") for m in mvs_list if isinstance(m, dict)]
    config["_functions"] = [f.get("identifier", "") for f in funcs_list if isinstance(f, dict)]
    config["_instructions"] = text_instr

    logger.info(
        "Space state: tables=%d, metric_views=%d, tvfs=%d, instructions=%s",
        len(tables_list),
        len(mvs_list),
        len(funcs_list),
        "present" if has_instructions else "absent",
    )
    return config


# ── Genie Query ────────────────────────────────────────────────────────


def run_genie_query(
    w: WorkspaceClient,
    space_id: str,
    question: str,
    max_wait: int = GENIE_MAX_WAIT,
) -> dict:
    """Send a question to Genie and return generated SQL + result metadata.

    Uses adaptive polling (``GENIE_POLL_INITIAL`` → ``GENIE_POLL_MAX``).
    Returns ``{"status", "sql", "conversation_id", "message_id",
    "attachment_id", "statement_id"}``.

    ``statement_id`` can be used with ``fetch_genie_result_df`` to retrieve
    the query results that Genie already computed (avoiding re-execution).

    Retries with exponential backoff on ``ResourceExhausted`` (HTTP 429)
    and ``TimeoutError`` from the SDK's own retry layer.
    """
    for rate_attempt in range(GENIE_RATE_LIMIT_RETRIES + 1):
        try:
            resp = w.genie.start_conversation(space_id=space_id, content=question)
            conversation_id = resp.conversation_id
            message_id = resp.message_id

            poll_interval = GENIE_POLL_INITIAL
            start = time.time()
            msg = None
            status = "UNKNOWN"

            while time.time() - start < max_wait:
                time.sleep(poll_interval)
                msg = w.genie.get_message(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    message_id=message_id,
                )
                status = str(msg.status) if hasattr(msg, "status") else "UNKNOWN"
                if any(s in status for s in ["COMPLETED", "FAILED", "CANCELLED"]):
                    break
                poll_interval = min(poll_interval + 1, GENIE_POLL_MAX)

            elapsed = time.time() - start
            if not any(s in status for s in ["COMPLETED", "FAILED", "CANCELLED"]):
                logger.warning(
                    "Genie query timed out after %.1fs for space %s conversation=%s message=%s",
                    elapsed,
                    space_id,
                    conversation_id,
                    message_id,
                )
                return {
                    "status": "TIMEOUT",
                    "sql": None,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                    "attachment_id": None,
                    "statement_id": None,
                    "analysis_text": None,
                    "error": f"Genie query timed out after {elapsed:.1f}s",
                }

            sql = None
            attachment_id = None
            statement_id = None
            analysis_text = None

            if msg and hasattr(msg, "attachments") and msg.attachments:
                for att in msg.attachments:
                    if hasattr(att, "query") and att.query:
                        sql = att.query.query if hasattr(att.query, "query") else str(att.query)
                        attachment_id = getattr(att, "id", None) or getattr(att, "attachment_id", None)
                    if hasattr(att, "text") and att.text:
                        text_content = getattr(att.text, "content", None)
                        if text_content and text_content.strip():
                            analysis_text = text_content.strip()

            if msg and hasattr(msg, "query_result") and msg.query_result:
                statement_id = getattr(msg.query_result, "statement_id", None)

            if not statement_id and attachment_id:
                try:
                    qr = w.genie.get_message_attachment_query_result(
                        space_id=space_id,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        attachment_id=attachment_id,
                    )
                    statement_id = getattr(qr, "statement_id", None)
                except Exception:
                    logger.debug("Could not fetch attachment query result for statement_id", exc_info=True)

            return {
                "status": status,
                "sql": sql,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "attachment_id": attachment_id,
                "statement_id": statement_id,
                "analysis_text": analysis_text,
            }
        except (ResourceExhausted, TimeoutError) as e:
            if rate_attempt < GENIE_RATE_LIMIT_RETRIES:
                delay = GENIE_RATE_LIMIT_BASE_DELAY * (2 ** rate_attempt)
                logger.warning(
                    "Genie rate-limited (attempt %d/%d), retrying in %ds: %s",
                    rate_attempt + 1,
                    GENIE_RATE_LIMIT_RETRIES,
                    delay,
                    e,
                )
                time.sleep(delay)
                continue
            logger.exception("Genie query failed after %d rate-limit retries for space %s", GENIE_RATE_LIMIT_RETRIES, space_id)
            return {"status": "ERROR", "sql": None, "error": str(e)}
        except Exception as e:
            logger.exception("Genie query failed for space %s", space_id)
            return {"status": "ERROR", "sql": None, "error": str(e)}
    return {"status": "ERROR", "sql": None, "error": "exhausted rate-limit retries"}


def fetch_genie_result_df(
    w: WorkspaceClient,
    statement_id: str,
    max_retries: int = 3,
    initial_delay: float = 2.0,
):
    """Fetch Genie's query result as a pandas DataFrame using the Statement Execution API.

    Retries up to *max_retries* times with linear backoff when the statement is
    still ``PENDING``/``RUNNING`` or when results are transiently unavailable.
    Returns ``None`` if the result cannot be retrieved after all attempts.
    """
    import pandas as pd

    for attempt in range(max_retries):
        try:
            stmt = w.statement_execution.get_statement(statement_id)
            if stmt.status and str(stmt.status.state) in ("PENDING", "RUNNING"):
                time.sleep(initial_delay * (attempt + 1))
                continue
            if stmt.result and stmt.result.data_array and stmt.manifest and stmt.manifest.schema:
                cols = stmt.manifest.schema.columns
                if cols:
                    col_names = pd.Index([str(c.name) for c in cols])
                    rows = [
                        [str(v) if v is not None else None for v in row]
                        for row in stmt.result.data_array
                    ]
                    return pd.DataFrame(rows, columns=col_names)
            if attempt < max_retries - 1:
                time.sleep(initial_delay * (attempt + 1))
                continue
            return None
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(initial_delay * (attempt + 1))
                continue
            logger.debug(
                "Could not fetch statement %s results after %d attempts",
                statement_id,
                max_retries,
                exc_info=True,
            )
            return None
    return None


# ── Asset Detection ────────────────────────────────────────────────────


def detect_asset_type(
    sql: str,
    mv_names: list[str] | None = None,
) -> str:
    """Detect asset type (MV, TVF, TABLE, NONE) from a SQL string.

    Parameters
    ----------
    sql : str
        SQL query text to inspect.
    mv_names : list[str] | None
        Optional metric-view table names.  When a known MV name appears
        in the SQL, the query is classified as ``MV`` even without a
        ``MEASURE()`` call.

    Notes
    -----
    Under the default scoring policy (``GSO_SCORING_V2`` != ``off``), the
    bare ``"mv_"`` substring rule is **dropped**. Customer tables named
    ``mv_something`` are legitimately regular ``TABLE``s; the old rule
    caused systematic ``Expected TABLE, got MV`` false negatives in
    ``asset_routing`` scoring. The authoritative MV signals are
    ``MEASURE(...)`` and an explicit ``mv_names`` list from the Genie
    space config.

    When ``GSO_SCORING_V2=off`` we fall back to the legacy behavior
    (any SQL containing ``"mv_"`` is classified as MV) so the kill-switch
    reproduces the pre-fix output byte-for-byte.
    """
    if not sql:
        return "NONE"
    sql_lower = sql.lower()
    if "measure(" in sql_lower:
        return "MV"
    if mv_names and any(name.lower() in sql_lower for name in mv_names):
        return "MV"
    if scoring_v2_is_legacy() and "mv_" in sql_lower:
        return "MV"
    if re.search(r"\bget_\w+\s*\(", sql_lower):
        return "TVF"
    return "TABLE"


# ── SQL Helpers ────────────────────────────────────────────────────────


def resolve_sql(sql: str, catalog: str, gold_schema: str) -> str:
    """Substitute ``${catalog}`` and ``${gold_schema}`` template variables."""
    if not sql:
        return sql
    return sql.replace("${catalog}", catalog).replace("${gold_schema}", gold_schema)


def sanitize_sql(sql: str) -> str:
    """Extract the first SQL statement, strip comments and trailing semicolons.

    Genie may return multi-statement SQL for compound questions.
    """
    if not sql:
        return sql
    sql = sql.strip().rstrip(";").strip()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if not statements:
        return sql
    first = statements[0]
    lines = [line for line in first.split("\n") if not line.strip().startswith("--")]
    return "\n".join(lines).strip()


# ── Config Mutation ────────────────────────────────────────────────────


def _migrate_column_configs_v1_to_v2(config: dict) -> dict:
    """Migrate v1 column config fields to v2 and strip non-exportable column fields.

    The Genie Agent export API v2 renamed:
      - ``get_example_values``    -> ``enable_format_assistance``
      - ``build_value_dictionary`` -> ``enable_entity_matching``

    Also removes ``data_type`` which is not part of the ColumnConfig proto.
    """
    _V1_TO_V2 = {
        "get_example_values": "enable_format_assistance",
        "build_value_dictionary": "enable_entity_matching",
    }
    _STRIP_FIELDS = {"data_type"}

    ds = config.get("data_sources", {})
    for key in ("tables", "metric_views"):
        for tbl in ds.get(key, []):
            for cc in tbl.get("column_configs", []):
                for old_key, new_key in _V1_TO_V2.items():
                    if old_key in cc:
                        if new_key not in cc:
                            cc[new_key] = cc[old_key]
                        del cc[old_key]
                for field in _STRIP_FIELDS:
                    cc.pop(field, None)
    return config


_NON_API_COLUMN_CONFIG_KEYS = {"uc_comment", "data_type_source"}


SERIALIZED_SPACE_TOP_LEVEL_KEYS = frozenset({
    "version",
    "config",
    "data_sources",
    "instructions",
    "benchmarks",
})
"""Allowed top-level keys on the ``serialized_space`` PATCH payload.

Anything else (including runtime annotations written onto
``metadata_snapshot`` by the lever loop, e.g. ``_failure_clusters``,
``_space_id``, or legacy non-exportable metadata such as ``title`` /
``creator``) is stripped before PATCH. The Genie API rejects unknown
top-level fields with ``Invalid serialized_space: Cannot find field``,
so this allowlist is the last line of defense before hitting the API."""


def strip_non_exportable_fields(config: dict) -> dict:
    """Remove non-exportable top-level keys before PATCH requests.

    Uses an allowlist of the five top-level ``serialized_space`` fields
    documented by the Genie API (see :data:`SERIALIZED_SPACE_TOP_LEVEL_KEYS`).
    Any other top-level key is dropped with a warning so we notice future
    pollution (e.g. runtime annotations on ``metadata_snapshot``) without
    breaking the run. Also strips internal-only keys from nested
    ``column_configs``.
    """
    cleaned: dict = {}
    dropped: list[str] = []
    for k, v in config.items():
        if k in SERIALIZED_SPACE_TOP_LEVEL_KEYS:
            cleaned[k] = v
        else:
            dropped.append(k)
    if dropped:
        _known_meta = [k for k in dropped if k in NON_EXPORTABLE_FIELDS]
        _unknown = [
            k for k in dropped
            if k not in NON_EXPORTABLE_FIELDS and not is_runtime_key(k)
        ]
        _unknown_runtime = [
            k for k in dropped
            if is_runtime_key(k) and k not in KNOWN_INTERNAL_RUNTIME_KEYS
        ]
        if _unknown:
            logger.warning(
                "strip_non_exportable_fields dropped unknown top-level keys "
                "from PATCH payload: %s. Known metadata dropped: %s. If any "
                "of the unknown keys are intentional runtime state, prefix "
                "them with '_' so they stay local to the snapshot.",
                _unknown, _known_meta,
            )
        if _unknown_runtime:
            logger.info(
                "strip_non_exportable_fields dropped undocumented runtime "
                "keys: %s. Add them to KNOWN_INTERNAL_RUNTIME_KEYS in "
                "common/config.py if they are intentional.",
                _unknown_runtime,
            )

    ds = cleaned.get("data_sources")
    if isinstance(ds, dict):
        for key in ("tables", "metric_views"):
            for tbl in ds.get(key, []):
                if not isinstance(tbl, dict):
                    continue
                for cc in tbl.get("column_configs", []):
                    if not isinstance(cc, dict):
                        continue
                    for bad_key in _NON_API_COLUMN_CONFIG_KEYS:
                        cc.pop(bad_key, None)

        # join_specs belongs under instructions, not data_sources
        misplaced_js = ds.pop("join_specs", None)
        if misplaced_js:
            inst_block = cleaned.setdefault("instructions", {})
            existing = inst_block.get("join_specs", [])
            inst_block["join_specs"] = existing + misplaced_js

    inst = cleaned.get("instructions")
    if isinstance(inst, dict):
        ti_list = inst.get("text_instructions")
        if isinstance(ti_list, list):
            inst["text_instructions"] = [
                ti for ti in ti_list
                if isinstance(ti, dict) and ti.get("content")
            ]

    return _migrate_column_configs_v1_to_v2(cleaned)


def sort_genie_config(config: dict) -> dict:
    """Sort all arrays in a Genie config to satisfy API sort requirements.

    The Genie API rejects unsorted data. Each collection must be sorted
    by the key documented at:
    https://docs.databricks.com/aws/en/genie/conversation-api#sorting-requirements
    """
    # ── data_sources.tables / metric_views  (by identifier) ──────
    if "data_sources" in config:
        for key in ["tables", "metric_views"]:
            if key in config["data_sources"]:
                config["data_sources"][key] = sorted(
                    config["data_sources"][key],
                    key=lambda x: x.get("identifier", ""),
                )
                for tbl in config["data_sources"][key]:
                    if "column_configs" in tbl and tbl["column_configs"]:
                        tbl["column_configs"] = sorted(
                            tbl["column_configs"],
                            key=lambda x: x.get("column_name", ""),
                        )

    # ── config.sample_questions  (by id) ─────────────────────────
    if "config" in config:
        sqs = config["config"].get("sample_questions")
        if sqs:
            config["config"]["sample_questions"] = sorted(
                sqs, key=lambda x: x.get("id", "")
            )

    # ── instructions ─────────────────────────────────────────────
    if "instructions" in config:
        inst = config["instructions"]

        if "sql_functions" in inst:
            inst["sql_functions"] = sorted(
                inst["sql_functions"],
                key=lambda x: (x.get("id", ""), x.get("identifier", "")),
            )
        for key in ["text_instructions", "example_question_sqls", "join_specs"]:
            if key in inst:
                inst[key] = sorted(inst[key], key=lambda x: x.get("id", ""))

        # sql_snippets sub-arrays (by id)
        snippets = inst.get("sql_snippets")
        if isinstance(snippets, dict):
            for snippet_key in ["filters", "expressions", "measures"]:
                if snippet_key in snippets and snippets[snippet_key]:
                    snippets[snippet_key] = sorted(
                        snippets[snippet_key],
                        key=lambda x: x.get("id", ""),
                    )

    # ── benchmarks.questions  (by id) ────────────────────────────
    if "benchmarks" in config:
        questions = config["benchmarks"].get("questions", [])
        if questions:
            config["benchmarks"]["questions"] = sorted(
                questions,
                key=lambda x: x.get("id", ""),
            )

    return config


def patch_space_config(
    w: WorkspaceClient,
    space_id: str,
    config: dict,
    *,
    max_retries: int = 2,
    retry_delay: float = 5.0,
) -> dict:
    """PATCH a Genie Agent with updated serialized_space config.

    Strips non-exportable fields, sorts arrays, and validates the payload
    structure before sending.  Retries on transient HTTP errors (429, 5xx).
    Returns the raw API response.
    """
    from .genie_schema import normalize_array_fields, validate_serialized_space

    clean = strip_non_exportable_fields(_serialized_space_for_patch(config))
    clean = sort_genie_config(clean)
    # Coerce every array-typed leaf field (description / synonyms / content /
    # …) to ``list[str]`` IN PLACE before validation AND serialization. The
    # Genie API rejects a bare string here with "Expected an array for
    # <field>"; the schema's model-level coercion never reaches this dict
    # (see normalize_array_fields). This is the single choke point every
    # PATCH flows through, so it also neutralizes a bare string left on a
    # shared metadata_snapshot by an upstream enrichment step.
    clean = normalize_array_fields(clean)

    ok, errors = validate_serialized_space(clean, strict=True)
    if not ok:
        logger.error(
            "Config validation failed before PATCH for space %s: %s",
            space_id,
            errors,
        )
        raise ValueError(f"Genie config validation failed: {errors}")

    payload = {"serialized_space": json.dumps(clean)}
    payload_size = len(payload["serialized_space"])
    logger.info(
        "PATCHing Genie Agent %s (payload: %d chars)", space_id, payload_size,
    )

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 2):
        try:
            raw_resp = w.api_client.do(
                "PATCH", f"/api/2.0/genie/spaces/{space_id}", body=payload,
            )
            logger.info("PATCH succeeded for space %s on attempt %d", space_id, attempt)
            if isinstance(raw_resp, dict):
                return raw_resp
            return {}
        except Exception as exc:
            last_exc = exc
            _err_body = ""
            if hasattr(exc, "response"):
                resp = getattr(exc, "response", None)
                if resp is not None:
                    _err_body = f" | HTTP {getattr(resp, 'status_code', '?')}: {getattr(resp, 'text', '')[:500]}"
            logger.warning(
                "PATCH attempt %d/%d failed for space %s: %s%s",
                attempt,
                max_retries + 1,
                space_id,
                exc,
                _err_body,
            )
            if attempt <= max_retries:
                time.sleep(retry_delay * attempt)

    raise last_exc  # type: ignore[misc]


def update_space_description(
    w: WorkspaceClient,
    space_id: str,
    description: str,
    *,
    max_retries: int = 2,
    retry_delay: float = 5.0,
) -> dict:
    """PATCH only the top-level ``description`` field of a Genie Agent.

    ``description`` is a top-level metadata field on the Space object, NOT
    inside ``serialized_space``.  This sends a minimal PATCH with just
    ``{"description": "..."}`` to avoid coupling with config updates.
    """
    payload = {"description": description}
    logger.info(
        "PATCHing Genie Agent %s description (%d chars)", space_id, len(description),
    )

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 2):
        try:
            raw_resp = w.api_client.do(
                "PATCH", f"/api/2.0/genie/spaces/{space_id}", body=payload,
            )
            logger.info(
                "Description PATCH succeeded for space %s on attempt %d",
                space_id, attempt,
            )
            if isinstance(raw_resp, dict):
                return raw_resp
            return {}
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Description PATCH attempt %d/%d failed for space %s: %s",
                attempt, max_retries + 1, space_id, exc,
            )
            if attempt <= max_retries:
                time.sleep(retry_delay * attempt)

    raise last_exc  # type: ignore[misc]


# ── Benchmark Publishing ──────────────────────────────────────────────

GENIE_MAX_BENCHMARK_QUESTIONS = 500

AUTO_OPTIMIZE_TAG_PREFIX = "[auto-optimize] "
"""Visible prefix on questions published by the optimizer. End users can
distinguish optimizer-authored benchmarks from their own curated ones."""


def _normalize_question_text(text: str) -> str:
    """Lower-case + whitespace-collapse + strip the ``[auto-optimize]`` tag
    prefix. Used as the dedup key so a tagged question does not double up
    against its untagged counterpart in the existing space."""
    if not isinstance(text, str):
        return ""
    if text.startswith(AUTO_OPTIMIZE_TAG_PREFIX):
        text = text[len(AUTO_OPTIMIZE_TAG_PREFIX):]
    return re.sub(r"\s+", " ", text.strip().lower())


def _ngram_similarity_for_dedup(a: str, b: str, n: int = 3) -> float:
    """Local n-gram Jaccard similarity used for benchmark dedup.

    Duplicated (not imported) to keep this module dependency-free — the
    original lives in ``optimization/optimizer.py`` and we want this module
    usable even when the optimization sub-package is not loaded (e.g. from
    lightweight apply paths).
    """
    if not a or not b:
        return 0.0
    a_lower, b_lower = a.lower(), b.lower()
    if len(a_lower) < n or len(b_lower) < n:
        return 0.0
    a_ngrams = {a_lower[i : i + n] for i in range(len(a_lower) - n + 1)}
    b_ngrams = {b_lower[i : i + n] for i in range(len(b_lower) - n + 1)}
    if not a_ngrams or not b_ngrams:
        return 0.0
    return len(a_ngrams & b_ngrams) / len(a_ngrams | b_ngrams)


_DEDUP_SIMILARITY_THRESHOLD = 0.90


def _extract_existing_question_text(entry: Any) -> str:
    """Pull the first human-readable question string out of an existing
    ``benchmarks.questions`` entry. The Genie format stores ``question`` as
    a list of strings; sometimes a single string slips through."""
    if not isinstance(entry, dict):
        return ""
    q = entry.get("question")
    if isinstance(q, list) and q:
        return str(q[0])
    if isinstance(q, str):
        return q
    return ""


def _benchmarks_to_genie_format(
    benchmarks: list[dict],
    *,
    tag_as_optimizer: bool = False,
    run_id: str | None = None,
) -> list[dict]:
    """Convert optimizer benchmark dicts to Genie-native ``benchmarks.questions`` format.

    Published rows are plain Genie benchmark questions. The optimizer keeps
    source/provenance metadata in the UC Delta benchmark table, not in the Genie
    Space payload. Prioritises curated/P0 benchmarks first, then fills with
    synthetic. The ``tag_as_optimizer`` parameter is retained only for
    backward-compatible callers that still want the legacy
    ``[auto-optimize]`` prefix + structured metadata; it must remain ``False``
    for ``publish_benchmarks_to_genie_space``.
    """
    curated: list[dict] = []
    synthetic: list[dict] = []
    for b in benchmarks:
        source = b.get("source", "")
        priority = b.get("priority", "")
        if source == "genie_space" or priority == "P0":
            curated.append(b)
        else:
            synthetic.append(b)

    ordered = curated + synthetic

    genie_questions: list[dict] = []
    seen: set[str] = set()
    skipped_no_answer = 0
    for b in ordered:
        question = str(b.get("question", "")).strip()
        if not question:
            continue

        dedup_key = _normalize_question_text(question)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        expected_sql = str(b.get("expected_sql", "")).strip()
        if not expected_sql:
            skipped_no_answer += 1
            continue

        display_question = (
            f"{AUTO_OPTIMIZE_TAG_PREFIX}{question}"
            if tag_as_optimizer
            else question
        )

        # A curated native benchmark keeps its Genie question ID so a SQL
        # repair can update that exact row. Other rows receive fresh IDs;
        # notably, sample-question IDs cannot be reused because Genie requires
        # uniqueness across sample questions and benchmark questions.
        native_question_id = (
            str(b.get("space_question_id") or "").strip()
            if source == "genie_benchmark"
            else ""
        )
        entry: dict[str, Any] = {
            "id": native_question_id or uuid.uuid4().hex,
            "question": [display_question],
            "answer": [{"format": "SQL", "content": [expected_sql]}],
        }
        if tag_as_optimizer:
            entry["metadata"] = {
                "source": "gso_optimizer",
                "run_id": run_id or "",
                "original_question": question,
                "benchmark_id": b.get("id", ""),
                "category": b.get("category", ""),
            }
        genie_questions.append(entry)

    if skipped_no_answer:
        logger.warning(
            "Skipped %d benchmark(s) without expected_sql "
            "(Genie requires exactly 1 answer per question)",
            skipped_no_answer,
        )

    return genie_questions


def _extract_benchmark_sql_answer(answer: Any) -> str:
    """Reconstruct the complete SQL answer from Genie content fragments.

    Genie serializes answer ``content`` as an array. Existing spaces can carry
    one SQL statement split across multiple array elements, while GSO writes a
    newly repaired statement as one element. The elements are ordered content
    fragments, so concatenate all of them exactly as the benchmark extractor
    does instead of treating ``content[0]`` as the whole statement.
    """
    if not isinstance(answer, list):
        return ""
    for item in answer:
        if not isinstance(item, dict):
            continue
        if str(item.get("format") or "").upper() != "SQL":
            continue
        content = item.get("content")
        if isinstance(content, list):
            return "".join(
                str(fragment) for fragment in content if fragment is not None
            ).strip()
        return str(content or "").strip()
    return ""


def _dedupe_and_merge_benchmarks(
    existing: list[dict],
    additions: list[dict],
    *,
    question_update_ids: set[str] | None = None,
) -> tuple[list[dict], int, int, list[dict]]:
    """Merge ``additions`` into ``existing`` preserving user-authored rows.

    An addition whose stable ID matches an existing row may update only that
    row's SQL answer by default. Question text may change only when the caller
    explicitly includes the stable native ID in ``question_update_ids`` after
    a bounded quality-warning repair. All other existing rows remain
    byte-for-byte intact.

    Returns ``(merged, added_count, skipped_count, updated)``. A new row is
    skipped only when its normalized question text exactly matches an existing
    row. Similar-but-distinct phrasings must remain separate live benchmarks:
    the native evaluator identifies rows by live question ID, and mapping a
    fuzzy match to an existing row could silently evaluate different wording
    or ground-truth SQL. Near-duplicate pruning remains an advisory window
    recommendation. ``updated`` contains before/after SQL records for the
    provenance ledger.
    """
    merged: list[dict] = list(existing) if isinstance(existing, list) else []
    question_update_ids = question_update_ids or set()

    existing_index_by_id = {
        str(entry.get("id") or "").strip(): index
        for index, entry in enumerate(merged)
        if isinstance(entry, dict) and str(entry.get("id") or "").strip()
    }

    existing_norms: list[str] = [
        _normalize_question_text(_extract_existing_question_text(e))
        for e in merged
    ]
    existing_norms = [n for n in existing_norms if n]

    added = 0
    skipped = 0
    updated: list[dict] = []
    for add in additions:
        add_text = _extract_existing_question_text(add)
        add_norm = _normalize_question_text(add_text)
        if not add_norm:
            skipped += 1
            continue

        add_id = str(add.get("id") or "").strip() if isinstance(add, dict) else ""
        existing_index = existing_index_by_id.get(add_id) if add_id else None
        if existing_index is not None:
            current = merged[existing_index]
            current_text = _extract_existing_question_text(current)
            current_norm = _normalize_question_text(current_text)
            question_changed = current_norm != add_norm
            if question_changed and add_id not in question_update_ids:
                logger.warning(
                    "Skipped benchmark update for stable id %s because question text changed",
                    add_id,
                )
                skipped += 1
                continue
            if question_changed:
                collides_with_other = any(
                    index != existing_index
                    and _normalize_question_text(_extract_existing_question_text(entry))
                    == add_norm
                    for index, entry in enumerate(merged)
                )
                if collides_with_other:
                    logger.warning(
                        "Skipped authorized benchmark question update for stable id %s "
                        "because the proposed wording duplicates another live row",
                        add_id,
                    )
                    skipped += 1
                    continue
            before_answer = current.get("answer") if isinstance(current, dict) else None
            after_answer = add.get("answer") if isinstance(add, dict) else None
            before_sql = _extract_benchmark_sql_answer(before_answer)
            after_sql = _extract_benchmark_sql_answer(after_answer)
            # Do not report or apply a repair when the only difference is the
            # serialized content shape (many fragments versus one fragment).
            # This preserves the user's original row byte-for-byte and keeps
            # the benchmark-change count semantic rather than representational.
            if not question_changed and before_sql == after_sql:
                skipped += 1
                continue
            repaired = dict(current)
            if question_changed:
                repaired["question"] = add.get("question")
            repaired["answer"] = after_answer
            merged[existing_index] = repaired

            updated.append({
                "id": add_id,
                "question": add_text,
                "before_question": current_text,
                "after_question": add_text,
                "before_sql": before_sql,
                "after_sql": after_sql,
            })
            continue

        if add_norm in existing_norms:
            skipped += 1
            continue

        merged.append(add)
        if add_id:
            existing_index_by_id[add_id] = len(merged) - 1
        existing_norms.append(add_norm)
        added += 1

    return merged, added, skipped, updated


def _extract_example_sql_questions(parsed: dict) -> set[str]:
    """Collect normalized question texts already mirrored in the space's
    ``example_question_sqls``. The space.benchmarks publisher uses this to
    suppress any question that is already a live example SQL — mirroring
    the same question into both slots would double-count as training data
    and re-introduce the Bug #4 leak."""
    result: set[str] = set()

    def _walk_example_sqls(container: dict) -> None:
        eqs = container.get("example_question_sqls")
        if isinstance(eqs, list):
            for e in eqs:
                if not isinstance(e, dict):
                    continue
                q = e.get("question")
                if isinstance(q, list) and q:
                    result.add(_normalize_question_text(str(q[0])))
                elif isinstance(q, str):
                    result.add(_normalize_question_text(q))

    _walk_example_sqls(parsed)
    inst = parsed.get("instructions")
    if isinstance(inst, dict):
        _walk_example_sqls(inst)
    tables = parsed.get("tables")
    if isinstance(tables, list):
        for t in tables:
            if isinstance(t, dict):
                _walk_example_sqls(t)
    return {r for r in result if r}


def compute_benchmark_window_recommendation(
    benchmarks: list[dict],
    *,
    window_min: int = BENCHMARK_WINDOW_MIN,
    window_max: int = BENCHMARK_WINDOW_MAX,
) -> dict:
    """Recommend how to bring a benchmark set into the 30–40 window (D8).

    Pure function — produces a RECOMMENDATION only; it never mutates or
    drops anything (prune is never a silent auto-delete). Returns a dict:

    * ``status`` — ``within_window`` | ``over_window`` | ``under_window``
    * ``count`` — current set count
    * ``window`` — ``[window_min, window_max]``
    * ``recommended_prune`` — for ``over_window``, the list of question ids
      recommended for removal (near-duplicates first, then lowest
      priority), trimmed down to ``window_max``.
    * ``recommended_topup`` — for ``under_window``, how many synthetic
      questions to generate to reach ``window_min``.

    The caller passes the POST-MERGE candidate set — existing live
    questions + net-new additions, after dedupe — so the resulting
    ``status``/``count`` reflect the real live set, not the net-new
    slice alone. Near-duplicate detection reuses the same normalized
    n-gram Jaccard (>= 0.90) the publisher uses for merge dedup, so the
    recommendation is consistent with how rows actually merge.
    """
    count = len(benchmarks)
    rec: dict[str, Any] = {
        "count": count,
        "window": [window_min, window_max],
        "recommended_prune": [],
        "recommended_topup": 0,
    }

    def _qid(b: dict) -> str:
        return str(b.get("id", b.get("question_id", "")) or "")

    if count < window_min:
        rec["status"] = "under_window"
        rec["recommended_topup"] = window_min - count
        return rec

    if count <= window_max:
        rec["status"] = "within_window"
        return rec

    # over_window — recommend trimming to window_max. Order of removal:
    # near-duplicates first (keep the higher-priority member of each
    # near-dup pair), then lowest priority. NEVER applied automatically.
    rec["status"] = "over_window"
    norms = [(_qid(b), _normalize_question_text(str(b.get("question", "")))) for b in benchmarks]
    prio = {_qid(b): str(b.get("priority", "")) for b in benchmarks}
    # P0 ranks highest (kept); blank/other lowest.
    prio_rank = {"P0": 0, "P1": 1, "P2": 2}

    near_dup_ids: list[str] = []
    kept: list[tuple[str, str]] = []
    for qid, norm in norms:
        if not norm:
            continue
        is_dup = any(
            _ngram_similarity_for_dedup(norm, kept_norm) >= _DEDUP_SIMILARITY_THRESHOLD
            for _kid, kept_norm in kept
        )
        if is_dup:
            near_dup_ids.append(qid)
        else:
            kept.append((qid, norm))

    over_by = count - window_max
    prune: list[str] = list(near_dup_ids[:over_by])
    if len(prune) < over_by:
        # Still over — recommend lowest-priority survivors (stable order).
        remaining = [
            qid for qid, _ in norms
            if qid not in prune
        ]
        remaining.sort(key=lambda q: prio_rank.get(prio.get(q, ""), 3))
        for qid in reversed(remaining):
            if len(prune) >= over_by:
                break
            if qid not in prune:
                prune.append(qid)
    rec["recommended_prune"] = prune[:over_by]
    return rec


@dataclass
class BenchmarkPushReport:
    """Structured outcome of a merge-only benchmark push to a Genie Agent.

    Surfaces enough detail for the v2 provenance ledger (§3.5) to record
    the added/removed/changed diff without re-deriving it. ``added`` rows
    are the net-new questions actually written into the space (each
    ``{id, question, sql}``); ``merged`` is the WHOLE post-merge set
    (existing live + net-new) so callers can compute the 30–40 window
    recommendation over the real resulting set; ``merged_total`` is its
    length. The push is additive except for SQL repair of an identical
    question selected by stable native ID, plus explicitly authorized wording
    repairs produced by the bounded benchmark-quality loop. User-authored rows
    are NEVER removed or truncated here, and cannot be reworded without that
    per-ID authorization.

    ``window`` is the post-merge 30–40 window recommendation (recommendation
    only — never auto-applied). ``over_cap`` is set when the merged set would
    exceed the genuine Genie API cap (``hard_cap``); in that case the push is
    NOT applied (``patched`` stays False) — the publisher fails closed rather
    than silently dropping rows.
    """

    added_count: int = 0
    updated_count: int = 0
    dedup_skipped: int = 0
    mirror_skipped: int = 0
    merged_total: int = 0
    existing_count: int = 0
    added: list[dict] = field(default_factory=list)
    updated: list[dict] = field(default_factory=list)
    merged: list[dict] = field(default_factory=list)
    window: dict | None = None
    over_cap: bool = False
    hard_cap: int = GENIE_MAX_BENCHMARK_QUESTIONS
    patched: bool = False


def publish_benchmarks_to_genie_space_with_report(
    w: WorkspaceClient,
    space_id: str,
    benchmarks: list[dict],
    max_questions: int = GENIE_MAX_BENCHMARK_QUESTIONS,
    *,
    run_id: str | None = None,
    question_update_ids: set[str] | None = None,
) -> BenchmarkPushReport:
    """Merge optimizer benchmarks into the space's native benchmarks section.

    Fetches the current space config, converts benchmarks to Genie-native
    format, MERGES them into existing ``serialized_space.benchmarks.questions``
    (preserving any user-authored rows), and PATCHes the space via
    ``updateSpace``. Published rows are plain benchmark questions: no
    ``[auto-optimize]`` prefix and no GSO ``metadata`` payload. Provenance
    stays in the UC Delta benchmark table, where the optimizer needs it.

    Questions that are already mirrored in the space's ``example_question_sqls``
    are excluded — keeping the same question in both slots would restore the
    exact leak Bug #4 guards against.

    The push is additive except for bounded stable-ID updates. SQL-only repair
    is allowed when question text is identical. A wording repair additionally
    requires the native ID in ``question_update_ids``; the QC task supplies
    that authorization only for revalidated warning proposals. The merged set
    is NEVER sliced or truncated,
    so pushed rows can't be silently dropped and pre-existing user-authored
    rows can't be deleted or reworded. ``max_questions`` is the genuine Genie
    API hard cap (``GENIE_MAX_BENCHMARK_QUESTIONS``), NOT a train/held-out
    target. If the post-merge set would exceed that hard cap, the publisher
    FAILS CLOSED — it does not patch and returns a non-mutating report with
    ``over_cap=True`` (the caller turns that into a hard failure). The 30–40
    *working window* is surfaced as a RECOMMENDATION only (``window``)
    computed over the post-merge set; it is never auto-applied here.

    Returns a :class:`BenchmarkPushReport` describing the merge.
    ``publish_benchmarks_to_genie_space`` is the thin int-returning wrapper
    kept for backward compatibility.
    """
    config = fetch_space_config(w, space_id)
    parsed = config.get("_parsed_space", {})
    if not isinstance(parsed, dict):
        parsed = {}

    existing_benchmarks_container = parsed.get("benchmarks")
    if not isinstance(existing_benchmarks_container, dict):
        existing_benchmarks_container = {}
    existing_questions = existing_benchmarks_container.get("questions")
    if not isinstance(existing_questions, list):
        existing_questions = []

    example_sql_questions = _extract_example_sql_questions(parsed)
    pre_filtered = [
        b for b in benchmarks
        if _normalize_question_text(str(b.get("question", "")))
        not in example_sql_questions
    ]
    skipped_mirror = len(benchmarks) - len(pre_filtered)
    if skipped_mirror:
        logger.info(
            "Skipped %d benchmark(s) already mirrored in example_question_sqls "
            "(Bug #4 leakage guard)",
            skipped_mirror,
        )

    new_genie_questions = _benchmarks_to_genie_format(
        pre_filtered, tag_as_optimizer=False, run_id=run_id,
    )

    existing_count = len(existing_questions)
    (
        merged_questions,
        _added_count,
        dedup_skipped,
        updated_detail,
    ) = _dedupe_and_merge_benchmarks(
        existing_questions,
        new_genie_questions,
        question_update_ids=question_update_ids,
    )
    # The merge appends net-new rows after the existing ones, so the
    # tail [existing_count:] is exactly what GSO added this push.
    added_rows = merged_questions[existing_count:]

    def _first(v: Any) -> str:
        if isinstance(v, list) and v:
            return str(v[0])
        return str(v) if v is not None else ""

    def _to_record(r: dict) -> dict:
        return {
            "id": str(r.get("id", "")),
            "question": _first(r.get("question")),
            "sql": _extract_benchmark_sql_answer(r.get("answer")),
        }

    merged_detail = [_to_record(r) for r in merged_questions if isinstance(r, dict)]
    added_detail = [_to_record(r) for r in added_rows if isinstance(r, dict)]

    # 30–40 working-window recommendation over the POST-MERGE set (existing
    # live rows + net-new additions, after dedupe). Recommendation only —
    # the publisher never prunes or truncates.
    window = compute_benchmark_window_recommendation(merged_detail)

    # Genuine Genie API hard cap — additive/merge-only NEVER truncates. If
    # the merged set would exceed the API cap we FAIL CLOSED: do not patch,
    # return a non-mutating "would exceed cap" report. This protects both
    # pushed rows (never silently dropped) and existing user-authored rows
    # (never deleted to make room).
    if len(merged_questions) > max_questions:
        logger.error(
            "Refusing to push benchmarks to Genie Agent %s: merged set of %d "
            "exceeds the Genie API hard cap of %d. Publisher is merge-only and "
            "will not truncate — pruning the live set is an explicit operator "
            "decision. No mutation performed.",
            space_id, len(merged_questions), max_questions,
        )
        return BenchmarkPushReport(
            added_count=0,
            updated_count=0,
            dedup_skipped=dedup_skipped,
            mirror_skipped=skipped_mirror,
            merged_total=len(merged_questions),
            existing_count=existing_count,
            added=[],
            updated=[],
            merged=merged_detail,
            window=window,
            over_cap=True,
            hard_cap=max_questions,
            patched=False,
        )

    parsed["benchmarks"] = dict(existing_benchmarks_container)
    parsed["benchmarks"]["questions"] = merged_questions

    patch_space_config(w, space_id, parsed)

    logger.info(
        "Published %d new and updated %d existing benchmark question(s) to Genie Agent %s "
        "(dedup-skipped: %d, example-sql-mirror-skipped: %d, total after merge: %d, "
        "window: %s)",
        len(added_detail), len(updated_detail), space_id, dedup_skipped, skipped_mirror,
        len(merged_questions), window.get("status"),
    )

    return BenchmarkPushReport(
        added_count=len(added_detail),
        updated_count=len(updated_detail),
        dedup_skipped=dedup_skipped,
        mirror_skipped=skipped_mirror,
        merged_total=len(merged_questions),
        existing_count=existing_count,
        added=added_detail,
        updated=updated_detail,
        merged=merged_detail,
        window=window,
        over_cap=False,
        hard_cap=max_questions,
        patched=True,
    )


def publish_benchmarks_to_genie_space(
    w: WorkspaceClient,
    space_id: str,
    benchmarks: list[dict],
    max_questions: int = GENIE_MAX_BENCHMARK_QUESTIONS,
    *,
    run_id: str | None = None,
) -> int:
    """Backward-compatible wrapper: returns the net-new benchmark count.

    See :func:`publish_benchmarks_to_genie_space_with_report` for the full
    merge semantics and the structured report consumed by the v2 preflight
    push / provenance ledger.
    """
    report = publish_benchmarks_to_genie_space_with_report(
        w, space_id, benchmarks, max_questions, run_id=run_id,
    )
    return report.added_count


def configure_connection_pool(w: WorkspaceClient, pool_size: int = 20) -> None:
    """Increase urllib3 connection pool size on the client's HTTP session.

    The default ``maxsize=1`` causes ``Connection pool is full, discarding
    connection`` warnings under the concurrent evaluation load typical of
    optimization runs.
    """
    try:
        from requests.adapters import HTTPAdapter

        session = getattr(w.api_client, "_session", None) or getattr(w, "_session", None)
        if session is None:
            session = getattr(w.config, "_session", None)
        if session is not None:
            adapter = HTTPAdapter(
                pool_connections=pool_size,
                pool_maxsize=pool_size,
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            logger.debug("Configured connection pool size=%d", pool_size)
    except Exception:
        logger.debug("Could not configure connection pool size", exc_info=True)


def configure_mlflow_connection_pool(pool_size: int = 20) -> None:
    """Patch the global requests default pool size so MLflow's internal HTTP
    sessions also use a larger connection pool.

    MLflow creates its own ``requests.Session`` objects with the default
    urllib3 pool of 10 connections.  Under concurrent evaluation load this
    triggers ``Connection pool is full, discarding connection`` warnings.
    """
    try:
        import requests.adapters as _ra
        if getattr(_ra, "DEFAULT_POOLSIZE", 10) < pool_size:
            setattr(_ra, "DEFAULT_POOLSIZE", pool_size)
            setattr(_ra, "DEFAULT_POOLCONNECTIONS", pool_size)
            logger.debug("Patched requests.adapters.DEFAULT_POOLSIZE=%d", pool_size)
    except Exception:
        logger.debug("Could not configure MLflow connection pool size", exc_info=True)
