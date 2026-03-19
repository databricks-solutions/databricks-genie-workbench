"""
Genie Space API wrapper.

All Genie Space API interactions. Every function takes ``WorkspaceClient``
as its first argument (APX pattern: dependency injection, no global state).
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from typing import Any, cast

from databricks.sdk import WorkspaceClient

from databricks.sdk.errors.platform import ResourceExhausted

from .config import (
    GENIE_MAX_WAIT,
    GENIE_POLL_INITIAL,
    GENIE_POLL_MAX,
    GENIE_RATE_LIMIT_BASE_DELAY,
    GENIE_RATE_LIMIT_RETRIES,
    NON_EXPORTABLE_FIELDS,
)

logger = logging.getLogger(__name__)


# ── Space Discovery & Config ───────────────────────────────────────────


def list_spaces(w: WorkspaceClient) -> list[dict[str, str]]:
    """List available Genie Spaces via SDK, paginating through all pages.

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
    """Fetch Genie Space ACL via REST API.

    Returns the raw JSON response dict, or ``None`` on failure.
    Prefer this over ``permissions.get()`` SDK which requires specific
    OAuth scopes that OBO tokens may lack.
    """
    try:
        resp = w.api_client.do("GET", f"/api/2.0/permissions/genie/{space_id}")
        return resp if isinstance(resp, dict) else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            "get_space_permissions_rest failed for %s (auth=%s): %s",
            space_id, getattr(w.config, 'auth_type', '?'), e,
        )
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
    """Return True if any SP alias has CAN_EDIT or CAN_MANAGE in a REST ACL response."""
    sp_aliases_lower = {a.lower() for a in sp_aliases}
    for entry in acl_response.get("access_control_list", []):
        principal = (
            entry.get("user_name") or entry.get("group_name")
            or entry.get("service_principal_name") or ""
        ).lower()
        if principal not in sp_aliases_lower:
            continue
        for p in entry.get("all_permissions", []):
            if str(p.get("permission_level", "")) in EDITABLE_PERMISSIONS:
                return True
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
    """Return the user's highest permission on a Genie space.

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
    """Check whether a user has CAN_MANAGE or CAN_EDIT on a Genie space.

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
    """Check whether a service principal has CAN_EDIT or CAN_MANAGE on a Genie space.

    Tries the REST ACL endpoint ``GET /api/2.0/permissions/genie/{id}`` with
    ``w`` first (OBO client), then ``sp_client`` if provided.  Returns False
    when neither client can retrieve the ACL — the previous serialized-space
    probe was unreliable (CAN_VIEW is sufficient to fetch the space config,
    producing false positives).
    """
    # Try OBO client first, then SP client for the ACL fetch
    acl_resp = cached_perms
    if acl_resp is None:
        for client in ([w] if sp_client is None else [w, sp_client]):
            acl_resp = get_space_permissions_rest(client, space_id)
            if acl_resp is not None:
                break

    if acl_resp is not None:
        return _check_sp_manage_from_rest_acl(acl_resp, sp_aliases)

    return False


def fetch_space_config(w: WorkspaceClient, space_id: str) -> dict:
    """GET Genie Space config with full serialized_space content.

    Returns the raw API response augmented with convenience keys:
    ``_parsed_space``, ``_tables``, ``_metric_views``, ``_functions``,
    ``_instructions``.
    """
    raw_config = w.api_client.do(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        query={"include_serialized_space": "true"},
    )
    if not isinstance(raw_config, dict):
        raise RuntimeError(
            f"Unexpected Genie space response type: {type(raw_config).__name__}"
        )
    config = cast(dict[str, Any], raw_config)

    ss = config.get("serialized_space", {})
    if isinstance(ss, str):
        ss = json.loads(ss)
    config["_parsed_space"] = ss

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
    """
    if not sql:
        return "NONE"
    sql_lower = sql.lower()
    if "measure(" in sql_lower:
        return "MV"
    if mv_names and any(name.lower() in sql_lower for name in mv_names):
        return "MV"
    if "mv_" in sql_lower:
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

    The Genie Space export API v2 renamed:
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


def strip_non_exportable_fields(config: dict) -> dict:
    """Remove read-only server-managed fields before PATCH requests.

    The GET ``/api/2.0/genie/spaces/{id}`` response includes top-level
    metadata fields that are NOT part of the ``GenieSpaceExport`` protobuf.
    Including them in the PATCH payload causes ``InvalidParameterValue``.
    Also strips internal-only keys from nested ``column_configs``.
    """
    cleaned = {
        k: v for k, v in config.items()
        if k not in NON_EXPORTABLE_FIELDS and not k.startswith("_")
    }

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
    """PATCH a Genie Space with updated serialized_space config.

    Strips non-exportable fields, sorts arrays, and validates the payload
    structure before sending.  Retries on transient HTTP errors (429, 5xx).
    Returns the raw API response.
    """
    from .genie_schema import validate_serialized_space

    clean = strip_non_exportable_fields(config)
    clean = sort_genie_config(clean)

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
        "PATCHing Genie Space %s (payload: %d chars)", space_id, payload_size,
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
    """PATCH only the top-level ``description`` field of a Genie Space.

    ``description`` is a top-level metadata field on the Space object, NOT
    inside ``serialized_space``.  This sends a minimal PATCH with just
    ``{"description": "..."}`` to avoid coupling with config updates.
    """
    payload = {"description": description}
    logger.info(
        "PATCHing Genie Space %s description (%d chars)", space_id, len(description),
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


def _benchmarks_to_genie_format(benchmarks: list[dict]) -> list[dict]:
    """Convert optimizer benchmark dicts to Genie-native ``benchmarks.questions`` format.

    Prioritises curated/P0 benchmarks first, then fills with synthetic.
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
        q_lower = question.lower()
        if q_lower in seen:
            continue
        seen.add(q_lower)

        expected_sql = str(b.get("expected_sql", "")).strip()
        if not expected_sql:
            skipped_no_answer += 1
            continue

        entry: dict[str, Any] = {
            "id": uuid.uuid4().hex,
            "question": [question],
            "answer": [{"format": "SQL", "content": [expected_sql]}],
        }
        genie_questions.append(entry)

    if skipped_no_answer:
        logger.warning(
            "Skipped %d benchmark(s) without expected_sql "
            "(Genie requires exactly 1 answer per question)",
            skipped_no_answer,
        )

    return genie_questions


def publish_benchmarks_to_genie_space(
    w: WorkspaceClient,
    space_id: str,
    benchmarks: list[dict],
    max_questions: int = GENIE_MAX_BENCHMARK_QUESTIONS,
) -> int:
    """Write optimizer benchmarks into the Genie Space's native benchmarks section.

    Fetches the current space config, converts benchmarks to Genie-native
    format, merges them into ``serialized_space.benchmarks.questions``, and
    PATCHes the space via the existing ``updateSpace`` API.

    Returns the number of benchmark questions published.
    """
    config = fetch_space_config(w, space_id)
    parsed = config.get("_parsed_space", {})
    if not isinstance(parsed, dict):
        parsed = {}

    genie_questions = _benchmarks_to_genie_format(benchmarks)
    if len(genie_questions) > max_questions:
        logger.warning(
            "Truncating benchmarks from %d to %d (Genie space limit)",
            len(genie_questions),
            max_questions,
        )
        genie_questions = genie_questions[:max_questions]

    parsed["benchmarks"] = {"questions": genie_questions}

    patch_space_config(w, space_id, parsed)

    logger.info(
        "Published %d benchmark questions to Genie space %s",
        len(genie_questions),
        space_id,
    )
    return len(genie_questions)


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
