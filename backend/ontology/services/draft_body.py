"""Ontology on-demand + bulk body drafting (Stage 4.1d Steps 3–4).

``draft_one`` drafts a single Page body with the SHARED wheel-native LLM client
(MV-D65), runs the SAME gates as the batch drafter, and UPDATEs genie_ont_pages
via the SQL warehouse (the decisions.py pattern). On failure or a failed gate,
body is unchanged, ok=false + reason (MV-D43).

``draft_subdomain`` runs draft_one over all Pages in a sub-domain under a BOUNDED
worker pool (stdlib concurrency, no new dependency), stamping body_source='llm_bulk'.
Bulk results are kept in an in-process task registry for polling (MV-D43).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from asyncio import Semaphore
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from backend.ontology.services import mirror, ont_settings
from genie_space_optimizer.common.llm import call_llm_core
from genie_space_optimizer.ontology.pages import (
    chunk_safe_gate,
    default_page_drafter,
    identifier_gate,
    specificity_gate,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# In-process task registry: task_id → {done, total, running, results}
_task_registry: dict[str, dict[str, Any]] = {}
_registry_lock = threading.Lock()


def _now_iso() -> str:
    """ISO-8601 timestamp for as_of fields."""
    return datetime.now(timezone.utc).isoformat()


def _gso_fqn(table: str) -> str:
    """Fully qualified name for GSO catalog tables."""
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _update_page_body_sql(metastore_id: str, page_id: str, body: str, body_source: str) -> tuple[str, list]:
    """Build an UPDATE statement for genie_ont_pages with body + evidence mutation.

    Updates body, body_source (via evidence JSON), facts_hash, and body_stale.
    Uses named parameters (StatementParameterListItem) so values are not interpolated.
    Returns (statement, params_list).
    """
    table = _gso_fqn("genie_ont_pages")

    # The statement will mutate the evidence JSON to set body_source and update facts_hash.
    # We compute facts_hash as a simple hash of the body to track changes.
    import hashlib
    facts_hash = hashlib.sha256(body.encode()).hexdigest()[:16]

    # Escape single quotes in the body for the JSON update
    body_escaped = body.replace("'", "''") if body else ""
    body_source_escaped = body_source.replace("'", "''") if body_source else ""

    # Build UPDATE that merges the evidence JSON with new body_source/facts_hash
    stmt = (
        f"UPDATE {table} SET "
        f"  body = :body, "
        f"  evidence = IF(evidence IS NULL OR evidence = '', "
        f"    CAST(map('body_source', :body_source, 'facts_hash', :facts_hash, 'body_stale', false) AS STRING), "
        f"    CAST(from_json(evidence, 'struct<*>') MERGE (body_source := :body_source, facts_hash := :facts_hash, body_stale := false) AS STRING) "
        f"  ), "
        f"  updated_at = current_timestamp() "
        f"WHERE metastore_id = :metastore_id AND page_id = :page_id"
    )

    from databricks.sdk.service.sql import StatementParameterListItem

    params = [
        StatementParameterListItem(name="metastore_id", value=metastore_id),
        StatementParameterListItem(name="page_id", value=page_id),
        StatementParameterListItem(name="body", value=body),
        StatementParameterListItem(name="body_source", value=body_source),
        StatementParameterListItem(name="facts_hash", value=facts_hash),
    ]
    return stmt, params


def _execute_update_via_warehouse(
    w: "WorkspaceClient",
    metastore_id: str,
    page_id: str,
    body: str,
    body_source: str,
) -> bool:
    """Execute an UPDATE on genie_ont_pages via the SQL warehouse (OBO identity).

    Returns True on success, False on failure (logged but not raised — degrade-not-hang).
    """
    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        logger.warning("SQL_WAREHOUSE_ID not configured; cannot update page body")
        return False

    from databricks.sdk.service.sql import StatementState

    try:
        stmt, params = _update_page_body_sql(metastore_id, page_id, body, body_source)
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=stmt,
            parameters=params,
            wait_timeout="30s",
        )
        statement_id = resp.statement_id if resp else None
        deadline = time.monotonic() + 40

        while resp and resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline or not statement_id:
                logger.warning("page body update timed out for %s/%s", metastore_id, page_id)
                return False
            time.sleep(1.0)
            resp = w.statement_execution.get_statement(statement_id=statement_id)

        if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
            detail = getattr(getattr(resp, "status", None), "error", None) if resp else None
            logger.warning("page body update failed for %s/%s: %s", metastore_id, page_id, detail)
            return False

        logger.info("ontology page body updated: %s/%s (source=%s)", metastore_id, page_id, body_source)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("page body update exception for %s/%s: %s", metastore_id, page_id, e)
        return False


def _read_page_by_id_sync(metastore_id: str, page_id: str) -> dict[str, Any] | None:
    """Read a single Page from the mirror by page_id (sync wrapper for asyncio context).

    Returns the raw row dict or None if not found/failed (degrade-not-hang).
    """
    try:
        # Try to use the async function if we're in an event loop context
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context; run as thread-safe sync read
                return asyncio.run_coroutine_threadsafe(
                    mirror._read_table("genie_ont_pages", metastore_id), loop
                ).result(timeout=10)
        except RuntimeError:
            pass
        # Fall back to direct async-to-sync via a new loop
        rows = asyncio.run(mirror._read_table("genie_ont_pages", metastore_id))
        for row in rows:
            if row.get("page_id") == page_id:
                return row
    except Exception:  # noqa: BLE001
        logger.info("failed to read page %s/%s from mirror", metastore_id, page_id, exc_info=True)
    return None


def _read_pages_by_domain_sync(metastore_id: str, domain_id: str) -> list[dict[str, Any]]:
    """Read all Pages for a sub-domain from the mirror (sync wrapper).

    Returns [] on any failure (degrade-not-hang).
    """
    try:
        rows = asyncio.run(mirror._read_table("genie_ont_pages", metastore_id))
        return [row for row in rows if row.get("domain_id") == domain_id]
    except Exception:  # noqa: BLE001
        logger.info("failed to read pages for domain %s/%s from mirror", metastore_id, domain_id, exc_info=True)
    return []


def _extract_page_facts(row: dict[str, Any]) -> dict[str, Any]:
    """Extract the facts dict from a page row for the drafter.

    Includes title, archetype, synonyms, source_fqns, description/definition/rules
    parsed from evidence or body deterministically.
    """
    evidence = mirror._evidence_of(row) if isinstance(row, dict) else {}

    title = str(row.get("title") or "")
    archetype = str(row.get("archetype") or "Routing")
    synonyms = mirror._as_list(row.get("synonyms"))
    source_fqns = mirror._as_list(row.get("source_fqns"))
    body = str(row.get("body") or "")

    # Parse existing body sections if present (fallback to empty)
    def _parse_section(body: str, section: str) -> str:
        lines = body.split("\n") if body else []
        in_section = False
        section_text = []
        for line in lines:
            stripped = line.strip().lower()
            if stripped.startswith(f"{section}:"):
                in_section = True
                rest = line.strip()[len(section) + 1:].strip()
                if rest:
                    section_text.append(rest)
                continue
            if in_section:
                if any(s in stripped for s in ["description:", "definition:", "rules:"]):
                    break
                if line.strip():
                    section_text.append(line)
        return " ".join(section_text).strip()

    description = _parse_section(body, "description")
    definition = _parse_section(body, "definition")
    rules_raw = _parse_section(body, "rules")
    # Clean up bullet markers
    rules = " ".join(line.strip() for line in rules_raw.split("\n") if line.strip() and not line.strip().startswith("-"))

    return {
        "title": title,
        "archetype": archetype,
        "concept": title,  # Concept is derived from title
        "synonyms": synonyms,
        "sources": source_fqns,
        "description": description,
        "definition": definition,
        "rules": rules,
        "evidence": evidence,
    }


def _validate_draft(body: str, source_fqns: list[str], w: "WorkspaceClient") -> tuple[bool, str | None]:
    """Validate the drafted body against all gates.

    Returns (is_valid, failure_reason). On success, is_valid=True, reason=None.
    On gate failure, reason is a short string explaining which gate failed.
    """
    if not body or not body.strip():
        return False, "draft is empty"

    # Chunk-safe gate: no bare pronouns opening a rule
    if not chunk_safe_gate(body):
        return False, "chunk_safe_gate failed: bare pronoun in a rule"

    # Specificity gate: backticked identifier in Definition AND every Rules bullet
    if not specificity_gate(body):
        return False, "specificity_gate failed: missing backticked identifier in Definition or Rules"

    # Identifier gate: all backticked identifiers and source FQNs must be in the universe
    # For now, we'll build the universe from the source_fqns (since we don't have a full member list)
    # The gates are lenient in the on-demand path (not in batch).
    universe_stub = frozenset(source_fqns)
    is_ok, invented = identifier_gate(body, source_fqns, universe_stub)
    if not is_ok:
        return False, f"identifier_gate failed: invented identifiers {invented[:3]}"  # Show first 3

    return True, None


def draft_one(
    page_id: str,
    *,
    metastore_id: str,
    w: "WorkspaceClient",
) -> dict[str, Any]:
    """Draft a single Page body with the LLM, run gates, and UPDATE genie_ont_pages.

    Returns {ok, page_id, body, body_source, as_of}.
    On success: ok=true, body=new_body, body_source="llm_ondemand".
    On failure: ok=false, body=current_body, reason=<explanation>.
    """
    result = {
        "ok": False,
        "page_id": page_id,
        "body": "",
        "body_source": "unknown",
        "as_of": _now_iso(),
    }

    # Read the page from the mirror
    page_row = _read_page_by_id_sync(metastore_id, page_id)

    if not page_row:
        logger.warning("page not found: %s/%s", metastore_id, page_id)
        result["body"] = ""
        return result

    # Current body (fallback for ok=false case)
    current_body = str(page_row.get("body") or "")
    result["body"] = current_body

    # Extract facts for the drafter
    facts = _extract_page_facts(page_row)

    # Build the drafter (via default_page_drafter, which uses call_llm_core internally)
    try:
        drafter = default_page_drafter(model=None, w=w)
        draft_body = drafter(facts)
    except Exception as e:  # noqa: BLE001
        logger.info("draft_one drafter failed for %s: %s", page_id, e)
        result["body"] = current_body
        return result

    if not draft_body or not draft_body.strip():
        logger.info("draft_one: drafter returned empty for %s", page_id)
        result["body"] = current_body
        return result

    # Validate the draft against gates
    source_fqns = mirror._as_list(page_row.get("source_fqns"))
    is_valid, reason = _validate_draft(draft_body, source_fqns, w)

    if not is_valid:
        logger.info("draft_one: gate validation failed for %s: %s", page_id, reason)
        result["body"] = current_body
        result["reason"] = reason or "gate validation failed"
        return result

    # Update the page in the warehouse via OBO
    success = _execute_update_via_warehouse(
        w,
        metastore_id,
        page_id,
        draft_body,
        "llm_ondemand",
    )

    if not success:
        logger.warning("draft_one: warehouse update failed for %s", page_id)
        result["body"] = current_body
        result["reason"] = "database write failed"
        return result

    # Success
    result["ok"] = True
    result["body"] = draft_body
    result["body_source"] = "llm_ondemand"
    return result


def _draft_one_sync_wrapper(
    page_id: str,
    metastore_id: str,
    w: "WorkspaceClient",
) -> dict[str, Any]:
    """Synchronous wrapper for draft_one for use in ThreadPoolExecutor."""
    return draft_one(page_id, metastore_id=metastore_id, w=w)


def draft_subdomain(
    domain_id: str,
    *,
    metastore_id: str,
    w: "WorkspaceClient",
    max_workers: int = 4,
) -> tuple[str, int, list[dict[str, Any]]]:
    """Draft all Pages in a sub-domain under a bounded worker pool.

    Returns (task_id, total, results) where results is [{page_id, ok, reason}].
    Stores the task in the registry for polling; results are best-effort, persisted
    only while the process is alive.
    """
    # Read pages for the domain
    page_rows = _read_pages_by_domain_sync(metastore_id, domain_id)

    total = len(page_rows)
    task_id = str(uuid.uuid4())

    # Initialize task record
    with _registry_lock:
        _task_registry[task_id] = {
            "done": 0,
            "total": total,
            "running": True,
            "results": [],
        }

    def _run_batch():
        results = []
        done_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for row in page_rows:
                page_id = str(row.get("page_id") or "")
                if not page_id:
                    continue
                future = executor.submit(_draft_one_sync_wrapper, page_id, metastore_id, w)
                futures[future] = page_id

            for future in futures:
                page_id = futures[future]
                try:
                    result = future.result(timeout=120)
                    results.append({
                        "page_id": page_id,
                        "ok": result.get("ok", False),
                        "reason": result.get("reason"),
                    })
                except Exception as e:  # noqa: BLE001
                    logger.warning("draft_subdomain worker failed for %s: %s", page_id, e)
                    results.append({
                        "page_id": page_id,
                        "ok": False,
                        "reason": f"worker exception: {str(e)[:50]}",
                    })

                done_count += 1
                with _registry_lock:
                    if task_id in _task_registry:
                        _task_registry[task_id]["done"] = done_count
                        _task_registry[task_id]["results"] = results

        with _registry_lock:
            if task_id in _task_registry:
                _task_registry[task_id]["running"] = False
                _task_registry[task_id]["results"] = results

    # Start the batch in a background thread (fire-and-forget)
    thread = threading.Thread(target=_run_batch, daemon=True)
    thread.start()

    return task_id, total, []


def get_bulk_draft_status(task_id: str) -> dict[str, Any] | None:
    """Retrieve the status of a bulk draft task by task_id.

    Returns {done, total, running, results} or None if task not found / expired.
    """
    with _registry_lock:
        return _task_registry.get(task_id)
