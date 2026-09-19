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
    _backticked,
    _canonical_body,
    _compute_facts_hash,
    _DraftSpec,
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


def _update_page_body_sql(
    metastore_id: str, page_id: str, body: str, evidence_json: str
) -> tuple[str, list]:
    """Build an UPDATE statement for genie_ont_pages (body + evidence).

    The evidence JSON is assembled in Python (``draft_one`` merges body_source /
    facts_hash / body_stale over the row's existing evidence) and bound WHOLE as a
    parameter. This avoids SQL-side JSON manipulation: ``genie_ont_pages.evidence`` is a
    JSON *text* column read by ``mirror._evidence_of`` via ``json.loads``, and Spark's
    ``CAST(map/struct AS STRING)`` emits the ``{k -> v}`` display form (NOT JSON), so any
    in-SQL merge would corrupt the column. Every value is a named parameter
    (``StatementParameterListItem``) — never interpolated. ``genie_ont_pages`` has no
    ``updated_at`` column, so none is set. Returns (statement, params_list).
    """
    table = _gso_fqn("genie_ont_pages")

    stmt = (
        f"UPDATE {table} SET body = :body, evidence = :evidence "
        f"WHERE metastore_id = :metastore_id AND page_id = :page_id"
    )

    from databricks.sdk.service.sql import StatementParameterListItem

    params = [
        StatementParameterListItem(name="metastore_id", value=metastore_id),
        StatementParameterListItem(name="page_id", value=page_id),
        StatementParameterListItem(name="body", value=body),
        StatementParameterListItem(name="evidence", value=evidence_json),
    ]
    return stmt, params


def _execute_update_via_warehouse(
    w: "WorkspaceClient",
    metastore_id: str,
    page_id: str,
    body: str,
    evidence_json: str,
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
        stmt, params = _update_page_body_sql(metastore_id, page_id, body, evidence_json)
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

        logger.info("ontology page body updated: %s/%s", metastore_id, page_id)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("page body update exception for %s/%s: %s", metastore_id, page_id, e)
        return False


def _all_pages_sync(metastore_id: str) -> list[dict[str, Any]]:
    """Read every genie_ont_pages row for a metastore (sync wrapper — draft_one/
    draft_subdomain always run in a worker thread with no running loop, via
    ``asyncio.to_thread`` from the routes or the bulk ThreadPoolExecutor, so
    ``asyncio.run`` is safe here). Returns [] on any failure (degrade-not-hang)."""
    try:
        return asyncio.run(mirror._read_table("genie_ont_pages", metastore_id))
    except Exception:  # noqa: BLE001
        logger.info("failed to read pages from mirror for %s", metastore_id, exc_info=True)
        return []


def _read_page_by_id_sync(metastore_id: str, page_id: str) -> dict[str, Any] | None:
    """Read a single Page from the mirror BY page_id. Returns the matching row dict or
    None if not found/failed (degrade-not-hang)."""
    for row in _all_pages_sync(metastore_id):
        if row.get("page_id") == page_id:
            return row
    return None


def _read_pages_by_domain_sync(metastore_id: str, domain_id: str) -> list[dict[str, Any]]:
    """Read all Pages for a sub-domain from the mirror. Returns [] on any failure."""
    return [row for row in _all_pages_sync(metastore_id) if row.get("domain_id") == domain_id]


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


def _spec_from_row(row: dict[str, Any]) -> "_DraftSpec":
    """Reconstruct the wheel ``_DraftSpec`` from a persisted page row so the on-demand
    path drives the SAME drafter prompt (``spec.facts()``) and the SAME canonical
    reassembly (``_canonical_body``) the batch's ``_autodraft`` uses — reuse, no fork
    (MV-D65/MV-D70). Prose fields (description/definition/rules) are parsed back from the
    stored body; structural fields come from the row/evidence. ``key_ids``/``nl_question``
    are only used for page_id derivation / optional routing validation, neither of which
    the on-demand path performs, so an empty default is byte-safe."""
    facts = _extract_page_facts(row)
    evidence = facts["evidence"] if isinstance(facts.get("evidence"), dict) else {}
    rules_str = str(facts.get("rules") or "").strip()
    try:
        corroboration = int(evidence.get("corroboration") or row.get("corroboration") or 0)
    except (TypeError, ValueError):
        corroboration = 0
    try:
        confidence = float(row.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return _DraftSpec(
        archetype=str(facts.get("archetype") or "Routing"),
        canonical_id=str(row.get("canonical_id") or evidence.get("canonical_id") or ""),
        domain_id=str(row.get("domain_id") or ""),
        concept_name=str(facts.get("concept") or facts.get("title") or ""),
        title=str(facts.get("title") or ""),
        description=str(facts.get("description") or ""),
        definition=str(facts.get("definition") or ""),
        rules=(rules_str,) if rules_str else (),
        key_ids=(),
        synonyms=tuple(facts.get("synonyms") or ()),
        synonym_classes=frozenset(str(c) for c in (evidence.get("synonym_classes") or [])),
        related_fqns=tuple(mirror._as_list(row.get("related_fqns"))),
        source_fqns=tuple(facts.get("sources") or ()),
        corroboration=corroboration,
        certify_shape=bool(row.get("certify")),
        confidence=confidence,
        evidence=evidence,
        nl_question=str(evidence.get("nl_question") or ""),
    )


def _grounded_universe(spec: "_DraftSpec", current_body: str) -> frozenset[str]:
    """The identifier allowlist for the on-demand gate. The batch runs against
    ``build_universe`` (estate-wide: member assets + metric-view / measure names +
    coded columns + serving Agents), which the app can't reconstruct from a single row.
    Instead we admit only PROVEN-grounded identifiers: the row's Sources + Related plus
    every backtick ALREADY in the persisted stub body — the stub was emitted by
    ``_stub_body`` from the same spec and passed ``identifier_gate`` against the full
    universe at materialize, so its identifiers (e.g. the metric-view measure name a
    Routing page legitimately cites, which is NOT a Source FQN) are genuine. This stops
    the on-demand gate from falsely rejecting a draft that names the measure while staying
    honest — nothing outside the already-gate-proven set is admitted."""
    return frozenset(
        [*spec.source_fqns, *spec.related_fqns, *_backticked(current_body or "")]
    )


def _validate_draft(body: str, source_fqns: list[str], universe: frozenset[str]) -> tuple[bool, str | None]:
    """Validate the drafted body against all gates (the SAME gates as the batch).

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

    # Identifier gate: every backticked identifier + Source FQN must be in the grounded
    # universe (Sources + Related + identifiers already proven in the persisted body).
    is_ok, invented = identifier_gate(body, source_fqns, universe)
    if not is_ok:
        return False, f"identifier_gate failed: invented identifiers {invented[:3]}"  # Show first 3

    return True, None


def draft_one(
    page_id: str,
    *,
    metastore_id: str,
    w: "WorkspaceClient",
    body_source: str = "llm_ondemand",
) -> dict[str, Any]:
    """Draft a single Page body with the LLM, run gates, and UPDATE genie_ont_pages.

    Mirrors the batch's ``_autodraft`` (MV-D66/MV-D70): draft prose from ``spec.facts()``,
    reassemble into the canonical skeleton (``_canonical_body`` — guarantees the Definition
    carries a real in-universe backticked identifier, falling back to the evidence-derived
    definition when the model paraphrased it away), THEN run the identifier/chunk/specificity
    gates on the reassembled body. ``body_source`` is ``"llm_ondemand"`` for a single draft and
    ``"llm_bulk"`` when invoked by :func:`draft_subdomain`.

    Returns {ok, page_id, body, body_source, as_of, reason}. On any failure or a failed gate:
    ok=false, body=current_body, reason=<explanation> (degrade-not-hang, MV-D43).
    """
    result = {
        "ok": False,
        "page_id": page_id,
        "body": "",
        "body_source": "unknown",
        "as_of": _now_iso(),
    }

    # Read THIS page from the mirror (by page_id).
    page_row = _read_page_by_id_sync(metastore_id, page_id)

    if not page_row:
        logger.warning("page not found: %s/%s", metastore_id, page_id)
        result["reason"] = "page not found"
        return result

    # Current body (fallback for ok=false case)
    current_body = str(page_row.get("body") or "")
    result["body"] = current_body

    # Reconstruct the wheel spec so the drafter prompt + canonical reassembly match batch.
    try:
        spec = _spec_from_row(page_row)
    except Exception as e:  # noqa: BLE001
        logger.info("draft_one: spec reconstruction failed for %s: %s", page_id, e)
        result["reason"] = "spec reconstruction failed"
        return result

    # Draft prose from the SAME facts the batch feeds (spec.facts()).
    try:
        drafter = default_page_drafter(model=None, w=w)
        raw = drafter(spec.facts())
    except Exception as e:  # noqa: BLE001
        logger.info("draft_one drafter failed for %s: %s", page_id, e)
        result["reason"] = "drafter failed"
        return result

    if not raw or not raw.strip():
        logger.info("draft_one: drafter returned empty for %s", page_id)
        result["reason"] = "drafter returned empty"
        return result

    # MV-D70: reassemble into the canonical plain-text skeleton BEFORE the gates, exactly
    # as _autodraft does — so a draft that paraphrased the identifier away is salvaged via
    # the deterministic definition rather than needlessly rejected.
    try:
        reassembled, _def_source = _canonical_body(raw, spec)
    except Exception as e:  # noqa: BLE001
        logger.info("draft_one: canonical reassembly failed for %s: %s", page_id, e)
        result["reason"] = "canonical reassembly failed"
        return result

    # Validate the reassembled body against the same gates the batch runs, using a
    # universe grounded in the row's Sources/Related + the persisted body's proven backticks
    # (so a legitimately-cited metric-view measure name is not falsely "invented").
    universe = _grounded_universe(spec, current_body)
    is_valid, reason = _validate_draft(reassembled, list(spec.source_fqns), universe)
    if not is_valid:
        logger.info("draft_one: gate validation failed for %s: %s", page_id, reason)
        result["reason"] = reason or "gate validation failed"
        return result

    # Step 2 (MV-D66): PRESERVE the batch-computed facts_hash — the curator re-drafts the
    # prose, not the concept, so the facts hash must not change (else the next re-materialize
    # would falsely flag the body stale). Fall back to recomputing the batch way only when the
    # row carries none (pre-Step-2 rows).
    existing_evidence = spec.evidence if isinstance(spec.evidence, dict) else {}
    facts_hash = str(existing_evidence.get("facts_hash") or "")
    if not facts_hash:
        try:
            facts_hash = _compute_facts_hash(spec)
        except Exception:  # noqa: BLE001
            facts_hash = ""

    # Merge body_source / facts_hash / body_stale OVER the row's existing evidence in
    # Python and bind the whole JSON as a parameter (evidence is a JSON text column;
    # an in-SQL CAST(map/struct AS STRING) would emit non-JSON and corrupt it).
    merged_evidence = {
        **existing_evidence,
        "body_source": body_source,
        "facts_hash": facts_hash,
        "body_stale": False,
    }
    try:
        evidence_json = json.dumps(merged_evidence)
    except (TypeError, ValueError):
        evidence_json = json.dumps(
            {"body_source": body_source, "facts_hash": facts_hash, "body_stale": False}
        )

    # Update the page in the warehouse via OBO.
    success = _execute_update_via_warehouse(
        w,
        metastore_id,
        page_id,
        reassembled,
        evidence_json,
    )

    if not success:
        logger.warning("draft_one: warehouse update failed for %s", page_id)
        result["reason"] = "database write failed"
        return result

    # Success
    result["ok"] = True
    result["body"] = reassembled
    result["body_source"] = body_source
    return result


def _draft_one_sync_wrapper(
    page_id: str,
    metastore_id: str,
    w: "WorkspaceClient",
    body_source: str,
) -> dict[str, Any]:
    """Synchronous wrapper for draft_one for use in ThreadPoolExecutor."""
    return draft_one(page_id, metastore_id=metastore_id, w=w, body_source=body_source)


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
                future = executor.submit(_draft_one_sync_wrapper, page_id, metastore_id, w, "llm_bulk")
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
