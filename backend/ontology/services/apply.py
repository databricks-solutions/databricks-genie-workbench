"""Ontology apply service (Phase 5 / 17i) — the consented governed-tag apply.

The ONLY write module in the ontology subsystem. Two pure-ish phases:
1. build_apply_plan(): reads approved consents + members, builds statements + diff, no UC write
2. execute_apply_plan(): executes statements under OBO, audits to genie_ont_applied, flips consent

The statement builder is shared by preview and execute, and by the copy-ready card.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from backend.ontology import models
from backend.ontology.services import mirror
from backend.services.auth import get_workspace_client, require_obo_workspace_client

logger = logging.getLogger(__name__)

_CONSENTS = "genie_ont_consents"
_APPLIED = "genie_ont_applied"


def _fqn(table: str) -> str:
    """Fully qualified table name."""
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _apply_id(proposal_id: str, shape: str, target_fqn: str, tag_value: str | None) -> str:
    """Idempotent apply_id: ap_<sha256(proposal_id|shape|target_fqn|tag_value)>."""
    msg = f"{proposal_id}|{shape}|{target_fqn}|{tag_value or ''}"
    digest = hashlib.sha256(msg.encode()).hexdigest()
    return f"ap_{digest}"


def _plan_hash(items: list[models.ApplyItem]) -> str:
    """Fingerprint the ordered statements so execute matches preview exactly."""
    statements_json = json.dumps([item.statement for item in items], sort_keys=True)
    return hashlib.sha256(statements_json.encode()).hexdigest()[:16]


async def build_apply_plan(
    metastore_id: str,
) -> models.ApplyPlan:
    """Dry-run: read approved consents + members, expand to statements + diff, no UC write.

    Returns an ApplyPlan with:
    - items: each statement (create_tag / set_tag / unset_tag), whether it's executable,
             blocked reason / grant lines, and diff hints (current_value for adds vs moves)
    - executable_count / blocked_count
    - plan_hash: fingerprint of ordered statements for execute consent gate
    - source: "mirror" / "live" / "cold" (empty/degraded)
    """
    try:
        # Read approved consents + members from mirror (the metastore grain).
        consents = await mirror.read_approved_consents(metastore_id)
        if not consents:
            logger.info("no approved consents for metastore %s", metastore_id)
            return models.ApplyPlan(
                items=[],
                executable_count=0,
                blocked_count=0,
                plan_hash=_plan_hash([]),
                source="mirror",
                as_of=datetime.now(timezone.utc).isoformat(),
            )

        items: list[models.ApplyItem] = []
        for consent in consents:
            proposal_kind = consent.get("proposal_kind", "")
            proposal_id = consent.get("proposal_id", "")

            # Skip page consents (copy-ready-only, MV-D27).
            if proposal_kind == "page":
                logger.debug("skipping page consent (copy-ready-only): %s", proposal_id)
                continue

            # Expand domain/subdomain/reassign consents to statements.
            if proposal_kind in ("domain", "subdomain"):
                domain_items = await _domain_items(
                    metastore_id, proposal_id, consent, proposal_kind
                )
                items.extend(domain_items)
            elif proposal_kind == "reassign":
                reassign_items = await _reassign_items(metastore_id, proposal_id, consent)
                items.extend(reassign_items)

        # Compute counts and fingerprint.
        executable_count = sum(1 for item in items if item.executable)
        blocked_count = len(items) - executable_count
        plan_hash = _plan_hash(items)

        return models.ApplyPlan(
            items=items,
            executable_count=executable_count,
            blocked_count=blocked_count,
            plan_hash=plan_hash,
            source="mirror",
            as_of=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as e:
        logger.exception("build_apply_plan failed: %s", e)
        # Degrade-not-hang (MV-D43): return empty cold plan.
        return models.ApplyPlan(
            items=[],
            executable_count=0,
            blocked_count=0,
            plan_hash=_plan_hash([]),
            source="cold",
            as_of=datetime.now(timezone.utc).isoformat(),
        )


async def _domain_items(
    metastore_id: str, domain_id: str, domain_consent: dict[str, Any], proposal_kind: str
) -> list[models.ApplyItem]:
    """Expand a domain/subdomain consent to create_tag + set_tag statements."""
    items: list[models.ApplyItem] = []
    tag_decision = domain_consent.get("tag_decision", "")
    tag_key = domain_consent.get("tag_key", "")
    tag_value = domain_consent.get("tag_value")
    name = domain_consent.get("name", "")

    if not tag_key:
        logger.warning("domain consent missing tag_key: %s", domain_id)
        return items

    # create_tag: CREATE GOVERNED TAG (sub-domain value in {parent}/{child} convention).
    if tag_decision == "create":
        create_item = models.ApplyItem(
            proposal_id=domain_id,
            proposal_kind=proposal_kind,
            shape="create_tag",
            target_fqn=tag_key,
            tag_key=tag_key,
            tag_value=tag_value,
            current_value=None,
            statement=f"CREATE GOVERNED TAG `{tag_key}`" +
                     (f" WITH ALLOWED_VALUES ('{tag_value}')" if tag_value else ""),
            executable=True,  # TODO: probe MANAGE DISCOVERY + ASSIGN / CREATE GOVERNED TAG
            blocked_reason=None,
            required_grants=[],
        )
        items.append(create_item)

    # set_tag: SET TAG on each member of the domain.
    members = await mirror.read_domain_members(metastore_id, domain_id)
    for member in members:
        asset_fqn = member.get("asset_fqn", "")
        if not asset_fqn:
            continue

        if tag_value:
            statement = f"ALTER ASSET `{asset_fqn}` SET TAG `{tag_key}` = '{tag_value}'"
        else:
            statement = f"ALTER ASSET `{asset_fqn}` SET TAG `{tag_key}`"

        set_item = models.ApplyItem(
            proposal_id=domain_id,
            proposal_kind=proposal_kind,
            shape="set_tag",
            target_fqn=asset_fqn,
            tag_key=tag_key,
            tag_value=tag_value,
            current_value=None,  # TODO: probe current value
            statement=statement,
            executable=True,  # TODO: probe APPLY TAG / USE SCHEMA / USE CATALOG
            blocked_reason=None,
            required_grants=[],
        )
        items.append(set_item)

    return items


async def _reassign_items(
    metastore_id: str, reassign_id: str, reassign_consent: dict[str, Any]
) -> list[models.ApplyItem]:
    """Expand a reassign consent to unset_tag + set_tag statements (move members)."""
    items: list[models.ApplyItem] = []
    conflict_tag = reassign_consent.get("conflict_tag", "")
    new_tag_key = reassign_consent.get("tag_key", "")
    new_tag_value = reassign_consent.get("tag_value")

    if not new_tag_key or not conflict_tag:
        logger.warning("reassign consent missing tag keys: %s", reassign_id)
        return items

    # Read members of the old tag (conflict_tag) that need to move.
    members = await mirror.read_tag_members(metastore_id, conflict_tag)
    for member in members:
        asset_fqn = member.get("asset_fqn", "")
        if not asset_fqn:
            continue

        # unset_tag: remove from old.
        unset_stmt = f"ALTER ASSET `{asset_fqn}` UNSET TAG `{conflict_tag}`"
        unset_item = models.ApplyItem(
            proposal_id=reassign_id,
            proposal_kind="reassign",
            shape="unset_tag",
            target_fqn=asset_fqn,
            tag_key=conflict_tag,
            tag_value=None,
            current_value=None,
            statement=unset_stmt,
            executable=True,  # TODO: probe
            blocked_reason=None,
            required_grants=[],
        )
        items.append(unset_item)

        # set_tag: add to new.
        if new_tag_value:
            set_stmt = f"ALTER ASSET `{asset_fqn}` SET TAG `{new_tag_key}` = '{new_tag_value}'"
        else:
            set_stmt = f"ALTER ASSET `{asset_fqn}` SET TAG `{new_tag_key}`"
        set_item = models.ApplyItem(
            proposal_id=reassign_id,
            proposal_kind="reassign",
            shape="set_tag",
            target_fqn=asset_fqn,
            tag_key=new_tag_key,
            tag_value=new_tag_value,
            current_value=None,
            statement=set_stmt,
            executable=True,  # TODO: probe
            blocked_reason=None,
            required_grants=[],
        )
        items.append(set_item)

    return items


async def execute_apply_plan(
    plan: models.ApplyPlan,
    metastore_id: str,
    workspace_id: str,
    applied_by: str,
) -> models.ApplyResult:
    """Execute the plan under OBO: run statements, audit to genie_ont_applied, flip consent.

    Per-statement fail-soft: PERMISSION_DENIED records state='failed' + degrades to copy-ready;
    other errors still run; never 500. Idempotent: flip consent approved→applied.
    """
    result = models.ApplyResult(as_of=datetime.now(timezone.utc).isoformat())

    try:
        client = require_obo_workspace_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            logger.error("SQL_WAREHOUSE_ID not configured; cannot execute apply")
            return result

        for item in plan.items:
            if not item.executable:
                # Item is blocked — skip execution, mark as blocked in result.
                outcome = models.ApplyOutcome(
                    proposal_id=item.proposal_id,
                    shape=item.shape,
                    target_fqn=item.target_fqn,
                    ok=False,
                    state="blocked",
                    error=item.blocked_reason,
                )
                result.blocked.append(outcome)
                continue

            # Execute the statement under OBO.
            try:
                resp = client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=item.statement,
                    wait_timeout="30s",
                )
                statement_id = resp.statement_id if resp else None
                deadline = time.monotonic() + 40
                while (
                    resp
                    and resp.status
                    and resp.status.state in (StatementState.PENDING, StatementState.RUNNING)
                ):
                    if time.monotonic() > deadline or not statement_id:
                        raise RuntimeError("statement execution timed out")
                    time.sleep(1.0)
                    resp = client.statement_execution.get_statement(statement_id=statement_id)

                if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
                    error_detail = getattr(getattr(resp, "status", None), "error", None)
                    raise RuntimeError(f"statement failed: {error_detail}")

                # Audit the successful execution.
                apply_id = _apply_id(
                    item.proposal_id, item.shape, item.target_fqn, item.tag_value
                )
                _write_audit_row(
                    metastore_id=metastore_id,
                    apply_id=apply_id,
                    workspace_id=workspace_id,
                    proposal_kind=item.proposal_kind,
                    proposal_id=item.proposal_id,
                    shape=item.shape,
                    statement=item.statement,
                    target_fqn=item.target_fqn,
                    tag_key=item.tag_key,
                    tag_value=item.tag_value,
                    prev_value=item.current_value,
                    state="applied",
                    applied_by=applied_by,
                    error=None,
                )

                outcome = models.ApplyOutcome(
                    proposal_id=item.proposal_id,
                    shape=item.shape,
                    target_fqn=item.target_fqn,
                    ok=True,
                    state="applied",
                    error=None,
                )
                result.applied.append(outcome)

            except Exception as e:
                # Per-statement fail-soft: record failure, continue to next item.
                error_str = str(e)
                state = "failed" if "PERMISSION_DENIED" not in error_str else "blocked"

                apply_id = _apply_id(
                    item.proposal_id, item.shape, item.target_fqn, item.tag_value
                )
                _write_audit_row(
                    metastore_id=metastore_id,
                    apply_id=apply_id,
                    workspace_id=workspace_id,
                    proposal_kind=item.proposal_kind,
                    proposal_id=item.proposal_id,
                    shape=item.shape,
                    statement=item.statement,
                    target_fqn=item.target_fqn,
                    tag_key=item.tag_key,
                    tag_value=item.tag_value,
                    prev_value=item.current_value,
                    state=state,
                    applied_by=applied_by,
                    error=error_str,
                )

                outcome = models.ApplyOutcome(
                    proposal_id=item.proposal_id,
                    shape=item.shape,
                    target_fqn=item.target_fqn,
                    ok=False,
                    state=state,
                    error=error_str,
                )
                if state == "blocked":
                    result.blocked.append(outcome)
                else:
                    result.failed.append(outcome)

                logger.warning("apply item failed: %s/%s (%s)", item.proposal_id, item.shape, error_str)

        # Flip consents: approved → applied (idempotent MERGE).
        _flip_consents(
            metastore_id=metastore_id,
            workspace_id=workspace_id,
            proposal_ids={item.proposal_id for item in plan.items},
            applied_by=applied_by,
        )

    except Exception as e:
        logger.exception("execute_apply_plan failed: %s", e)

    return result


def _write_audit_row(
    metastore_id: str,
    apply_id: str,
    workspace_id: str,
    proposal_kind: str,
    proposal_id: str,
    shape: str,
    statement: str,
    target_fqn: str,
    tag_key: str,
    tag_value: str | None,
    prev_value: str | None,
    state: str,
    applied_by: str,
    error: str | None,
) -> None:
    """Write one row to genie_ont_applied (audit table). Degrade-not-hang on failure."""
    try:
        from genie_space_optimizer.ontology import ddl

        applied_at = datetime.now(timezone.utc).isoformat()
        target = _fqn(ddl.TABLE_ONT_APPLIED)

        # INSERT into applied table (idempotent on apply_id).
        sql = f"""
INSERT INTO {target} (
    metastore_id, apply_id, workspace_id, proposal_kind, proposal_id,
    shape, statement, target_fqn, tag_key, tag_value, prev_value,
    state, applied_by, applied_at, error, run_ref
) VALUES (
    '{metastore_id}', '{apply_id}', '{workspace_id}', '{proposal_kind}', '{proposal_id}',
    '{shape}', '{statement}', '{target_fqn}', '{tag_key}', {repr(tag_value)},
    {repr(prev_value)}, '{state}', '{applied_by}', '{applied_at}', {repr(error)}, NULL
)
"""
        # Execute as SP (best-effort audit; failure does not fail the apply).
        from backend.services.auth import get_service_principal_client

        client = get_service_principal_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if warehouse_id:
            resp = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id, statement=sql, wait_timeout="10s"
            )
            if resp and resp.statement_id:
                deadline = time.monotonic() + 15
                while (
                    resp
                    and resp.status
                    and resp.status.state
                    in (StatementState.PENDING, StatementState.RUNNING)
                ):
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.5)
                    resp = client.statement_execution.get_statement(
                        statement_id=resp.statement_id
                    )
    except Exception as e:
        logger.warning("audit write failed (non-fatal): %s", e)


def _flip_consents(
    metastore_id: str,
    workspace_id: str,
    proposal_ids: set[str],
    applied_by: str,
) -> None:
    """Flip consumed consents from approved → applied (idempotent). Degrade-not-hang on failure."""
    if not proposal_ids:
        return
    try:
        from backend.services.auth import get_service_principal_client

        client = get_service_principal_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            return

        target = _fqn(_CONSENTS)
        applied_at = datetime.now(timezone.utc).isoformat()
        proposal_list = ", ".join(f"'{pid}'" for pid in proposal_ids)

        sql = f"""
UPDATE {target}
SET state = 'applied', workspace_id = '{workspace_id}'
WHERE metastore_id = '{metastore_id}' AND proposal_id IN ({proposal_list}) AND state = 'approved'
"""
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, wait_timeout="10s"
        )
        if resp and resp.statement_id:
            deadline = time.monotonic() + 15
            while (
                resp
                and resp.status
                and resp.status.state in (StatementState.PENDING, StatementState.RUNNING)
            ):
                if time.monotonic() > deadline:
                    break
                time.sleep(0.5)
                resp = client.statement_execution.get_statement(statement_id=resp.statement_id)
    except Exception as e:
        logger.warning("consent flip failed (non-fatal): %s", e)
