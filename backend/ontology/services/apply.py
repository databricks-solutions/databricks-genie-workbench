"""Ontology apply service (Phase 5 / 17i) — the consented governed-tag apply.

The ONLY write module in the ontology subsystem. Two pure-ish phases:
1. build_apply_plan(): reads approved consents + members, builds statements + diff, no UC write
2. execute_apply_plan(): executes statements under OBO, audits to genie_ont_applied, flips consent

The statement builder is shared by preview and execute, and by the copy-ready card.

──────────────────────────────────────────────────────────────────────────────
VERIFIED GOVERNED-TAG SYNTAX (STEP 0 — reconciled against the Databricks docs
on 2026-09-17; supersedes the Stage-1 ``ALTER ASSET … SET TAG`` / ``WITH
ALLOWED_VALUES`` drafts, which diverged):

  - CREATE GOVERNED TAG <tag_key> [ VALUES ( '<v>' [, …] ) ]
      docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-governed-tag
      · tag_key is an identifier (we backtick-quote it to escape).
      · allowed values are SINGLE-QUOTED STRING LITERALS (we double any ``'``).

  - SET TAG ON { CATALOG | SCHEMA | TABLE | VIEW | VOLUME | FUNCTION | COLUMN }
        <name> <tag_key> [ = <tag_value> ]
      docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-set-tag
      · the securable TYPE is a required keyword (from the member's asset_type).
      · <name> is a dotted identifier; each segment is backtick-quoted.
      · BOTH tag_key AND tag_value are IDENTIFIERS (e.g. ``= hr`` / ``= `hr` ``),
        NOT string literals — so we backtick-quote the value too. A sub-domain
        value follows the verified {parent}/{child} convention (e.g. ``Finance/Tax``,
        docs.databricks.com/aws/en/uc-semantics/domains); the ``/`` REQUIRES the
        backtick-quoting we already apply.

  - UNSET TAG ON <securable_type> <name> <tag_key>

SQL SAFETY (BUILD A): the governed-tag statement is fully self-contained — every
piece that lands in it is an IDENTIFIER position (asset FQN, tag_key, and — per the
verified syntax — the SET TAG value), so it is made injection-safe by backtick
ESCAPING (doubling internal backticks), NOT by parameter binding: the SQL Statement
Execution API's ``:name`` markers bind VALUE literals, and there is no value literal
in a SET/UNSET TAG statement. (BUILD A's "bind tag values" premise assumed a literal
position; STEP 0 corrected that.) The CREATE GOVERNED TAG allowed-value IS a literal,
so it is single-quote escaped. Where genuine value literals DO exist — the
``genie_ont_applied`` audit INSERT and the consent-flip UPDATE — every column is bound
via ``StatementParameterListItem`` (see ``_write_audit_row`` / ``_flip_consents``).
``plan_hash`` fingerprints the ordered, fully-inlined statements; because the write
carries no bound params, the statement string IS the template+values, so preview and
execute (which rebuilds the plan) still match exactly.

IDENTITY SPLIT (BUILD D · MV-D50): the governed-tag WRITE runs under OBO
(``require_obo_workspace_client`` — attributed to the consenting human, gated on THEIR
grants). The bookkeeping — the ``genie_ont_applied`` audit INSERT and the consent
``approved → applied`` flip — runs as the SERVICE PRINCIPAL (``get_service_principal_client``),
because the SP owns those app-state tables. This split is deliberate: DO NOT move the
audit / consent-flip to OBO.
──────────────────────────────────────────────────────────────────────────────
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
from backend.ontology.services import grants, mirror
from backend.services.auth import get_workspace_client, require_obo_workspace_client

logger = logging.getLogger(__name__)

_CONSENTS = "genie_ont_consents"
_APPLIED = "genie_ont_applied"

# asset_type → the SET/UNSET TAG securable-type keyword (verified syntax above).
_SECURABLE_KW = {
    "table": "TABLE",
    "view": "VIEW",
    "materialized_view": "TABLE",
    "streaming_table": "TABLE",
    "schema": "SCHEMA",
    "catalog": "CATALOG",
    "volume": "VOLUME",
    "function": "FUNCTION",
    "column": "COLUMN",
}

_BLOCKED_REASON = "You need a permission to apply this change."


def _fqn(table: str) -> str:
    """Fully qualified table name."""
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _securable_kw(asset_type: str) -> str:
    return _SECURABLE_KW.get((asset_type or "table").lower(), "TABLE")


def _bt(name: str) -> str:
    """Backtick-quote a single identifier, doubling any internal backtick (injection-safe)."""
    return "`" + str(name).replace("`", "``") + "`"


def _bt_fqn(fqn: str) -> str:
    """Backtick-quote each dotted segment of an asset FQN (``a.b.c`` → `` `a`.`b`.`c` ``)."""
    parts = [p for p in str(fqn).split(".") if p != ""]
    return ".".join(_bt(p) for p in parts) if parts else _bt(fqn)


def _sq(literal: str) -> str:
    """Single-quote a string literal, doubling any internal single quote (injection-safe)."""
    return "'" + str(literal).replace("'", "''") + "'"


# ── statement builders (the single source shared by preview + execute) ──────────


def _create_tag_statement(tag_key: str, tag_value: str | None) -> str:
    """CREATE GOVERNED TAG `<key>` [ VALUES ('<value>') ] — value is a STRING LITERAL."""
    stmt = f"CREATE GOVERNED TAG {_bt(tag_key)}"
    if tag_value:
        stmt += f" VALUES ({_sq(tag_value)})"
    return stmt


def _set_tag_statement(asset_fqn: str, asset_type: str, tag_key: str, tag_value: str | None) -> str:
    """SET TAG ON <type> `<fqn>` `<key>` [ = `<value>` ] — key AND value are IDENTIFIERS."""
    stmt = f"SET TAG ON {_securable_kw(asset_type)} {_bt_fqn(asset_fqn)} {_bt(tag_key)}"
    if tag_value:
        stmt += f" = {_bt(tag_value)}"
    return stmt


def _unset_tag_statement(asset_fqn: str, asset_type: str, tag_key: str) -> str:
    """UNSET TAG ON <type> `<fqn>` `<key>`."""
    return f"UNSET TAG ON {_securable_kw(asset_type)} {_bt_fqn(asset_fqn)} {_bt(tag_key)}"


def _apply_id(proposal_id: str, shape: str, target_fqn: str, tag_value: str | None) -> str:
    """Idempotent apply_id: ap_<sha256(proposal_id|shape|target_fqn|tag_value)>."""
    msg = f"{proposal_id}|{shape}|{target_fqn}|{tag_value or ''}"
    digest = hashlib.sha256(msg.encode()).hexdigest()
    return f"ap_{digest}"


def _plan_hash(items: list[models.ApplyItem]) -> str:
    """Fingerprint the ordered statements so execute matches preview exactly.

    The write statements are fully inlined (no bound params — every substituted piece is
    an identifier position, escaped in place), so the statement string already captures
    the "template + values" BUILD A calls for; hashing the ordered statements is the whole
    fingerprint. Execute rebuilds the plan and rejects a hash it does not echo (409)."""
    statements_json = json.dumps([item.statement for item in items], sort_keys=True)
    return hashlib.sha256(statements_json.encode()).hexdigest()[:16]


def _probe_client():
    """The OBO client used ONLY for the read-only preview probes (write grants +
    current tag value). Resolved on-platform (``DATABRICKS_HOST`` set); ``None`` off-platform
    or on any failure, so the offline suite never touches the network and the probes
    fail-soft (MV-D43). This client is NOT the write client — the write resolves its own
    ``require_obo_workspace_client`` inside ``execute_apply_plan``."""
    if not os.environ.get("DATABRICKS_HOST", "").strip():
        return None
    try:
        return get_workspace_client()
    except Exception as e:  # noqa: BLE001 — a probe never blocks (MV-D43)
        logger.info("probe client unavailable: %s", e)
        return None


def _probe_principal(client) -> str | None:
    """The OBO viewer's user name (for copy-ready GRANT lines). None on any failure."""
    if client is None:
        return None
    try:
        return (client.current_user.me().user_name or "").strip() or None
    except Exception:  # noqa: BLE001
        return None


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

        # Read-only probe client (write grants + current value), resolved once. None
        # off-platform → both probes fail-soft, so the plan is byte-identical to Stage 1.
        client = _probe_client()
        principal = _probe_principal(client)

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
                    metastore_id, proposal_id, consent, proposal_kind, client, principal
                )
                items.extend(domain_items)
            elif proposal_kind == "reassign":
                reassign_items = await _reassign_items(
                    metastore_id, proposal_id, consent, client, principal
                )
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


def _probe_write(
    client, target_fqn: str, tag_key: str, asset_type: str, principal: str | None
) -> tuple[bool, str | None, list[str]]:
    """Grant probe for one write target → (executable, blocked_reason, required_grants).
    Probe failure/indeterminate ⇒ executable=True (MV-D43)."""
    ok, missing = grants.membership_write_probe(
        client, target_fqn, tag_key, asset_type=asset_type, principal=principal
    )
    if ok:
        return True, None, []
    return False, _BLOCKED_REASON, missing


async def _domain_items(
    metastore_id: str,
    domain_id: str,
    domain_consent: dict[str, Any],
    proposal_kind: str,
    client=None,
    principal: str | None = None,
) -> list[models.ApplyItem]:
    """Expand a domain/subdomain consent to create_tag + set_tag statements."""
    items: list[models.ApplyItem] = []
    tag_decision = domain_consent.get("tag_decision", "")
    tag_key = domain_consent.get("tag_key", "")
    tag_value = domain_consent.get("tag_value")

    if not tag_key:
        logger.warning("domain consent missing tag_key: %s", domain_id)
        return items

    # create_tag: CREATE GOVERNED TAG (sub-domain value in {parent}/{child} convention).
    # The account-level create privilege is not probeable per-asset; leave it executable and
    # let the OBO execute degrade to copy-ready on a real denial (MV-D43).
    if tag_decision == "create":
        create_item = models.ApplyItem(
            proposal_id=domain_id,
            proposal_kind=proposal_kind,
            shape="create_tag",
            target_fqn=tag_key,
            tag_key=tag_key,
            tag_value=tag_value,
            current_value=None,
            statement=_create_tag_statement(tag_key, tag_value),
            executable=True,
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
        asset_type = member.get("asset_type", "table")

        executable, blocked_reason, required_grants = _probe_write(
            client, asset_fqn, tag_key, asset_type, principal
        )
        current_value = grants.current_tag_value(client, asset_fqn, tag_key, asset_type=asset_type)

        set_item = models.ApplyItem(
            proposal_id=domain_id,
            proposal_kind=proposal_kind,
            shape="set_tag",
            target_fqn=asset_fqn,
            tag_key=tag_key,
            tag_value=tag_value,
            current_value=current_value,
            statement=_set_tag_statement(asset_fqn, asset_type, tag_key, tag_value),
            executable=executable,
            blocked_reason=blocked_reason,
            required_grants=required_grants,
        )
        items.append(set_item)

    return items


async def _reassign_items(
    metastore_id: str,
    reassign_id: str,
    reassign_consent: dict[str, Any],
    client=None,
    principal: str | None = None,
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
        asset_type = member.get("asset_type", "table")

        # unset_tag: remove from old. Capture the CURRENT value of the tag we remove as
        # the item's current_value — exactly as the sibling set_tag below probes it — so the
        # audit row's prev_value is persisted and the unset becomes undoable (17j BUILD A).
        # Absent / indeterminate ⇒ None (byte-identical: that unset simply stays non-undoable).
        unset_ok, unset_reason, unset_grants = _probe_write(
            client, asset_fqn, conflict_tag, asset_type, principal
        )
        unset_current = grants.current_tag_value(
            client, asset_fqn, conflict_tag, asset_type=asset_type
        )
        unset_item = models.ApplyItem(
            proposal_id=reassign_id,
            proposal_kind="reassign",
            shape="unset_tag",
            target_fqn=asset_fqn,
            tag_key=conflict_tag,
            tag_value=None,
            current_value=unset_current,
            statement=_unset_tag_statement(asset_fqn, asset_type, conflict_tag),
            executable=unset_ok,
            blocked_reason=unset_reason,
            required_grants=unset_grants,
        )
        items.append(unset_item)

        # set_tag: add to new.
        set_ok, set_reason, set_grants = _probe_write(
            client, asset_fqn, new_tag_key, asset_type, principal
        )
        current_value = grants.current_tag_value(client, asset_fqn, new_tag_key, asset_type=asset_type)
        set_item = models.ApplyItem(
            proposal_id=reassign_id,
            proposal_kind="reassign",
            shape="set_tag",
            target_fqn=asset_fqn,
            tag_key=new_tag_key,
            tag_value=new_tag_value,
            current_value=current_value,
            statement=_set_tag_statement(asset_fqn, asset_type, new_tag_key, new_tag_value),
            executable=set_ok,
            blocked_reason=set_reason,
            required_grants=set_grants,
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

    The governed-tag WRITE runs under OBO (MV-D50) — attributed to ``applied_by``. Per-statement
    fail-soft: PERMISSION_DENIED records state='failed' + degrades to copy-ready; other errors
    still run; never 500. The audit + consent-flip bookkeeping is SP (BUILD D). Idempotent: the
    consent flips approved→applied so a re-run is a no-op.
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

        # Flip consents: approved → applied (idempotent).
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
    """Write one row to genie_ont_applied (audit table). Degrade-not-hang on failure.

    BUILD A: every VALUE is bound via ``StatementParameterListItem`` — never string-
    interpolated — so a statement / error / email carrying a quote can never break out.
    BUILD D: this bookkeeping write runs as the SERVICE PRINCIPAL (the SP owns the table);
    only the governed-tag write itself is OBO."""
    try:
        from genie_space_optimizer.ontology import ddl

        applied_at = datetime.now(timezone.utc).isoformat()
        target = _fqn(ddl.TABLE_ONT_APPLIED)

        # INSERT into applied table — all VALUES bound; applied_at cast to TIMESTAMP.
        sql = f"""
INSERT INTO {target} (
    metastore_id, apply_id, workspace_id, proposal_kind, proposal_id,
    shape, statement, target_fqn, tag_key, tag_value, prev_value,
    state, applied_by, applied_at, error, run_ref
) VALUES (
    :metastore_id, :apply_id, :workspace_id, :proposal_kind, :proposal_id,
    :shape, :statement, :target_fqn, :tag_key, :tag_value, :prev_value,
    :state, :applied_by, CAST(:applied_at AS TIMESTAMP), :error, :run_ref
)
"""
        params = [
            StatementParameterListItem(name="metastore_id", value=metastore_id),
            StatementParameterListItem(name="apply_id", value=apply_id),
            StatementParameterListItem(name="workspace_id", value=workspace_id),
            StatementParameterListItem(name="proposal_kind", value=proposal_kind),
            StatementParameterListItem(name="proposal_id", value=proposal_id),
            StatementParameterListItem(name="shape", value=shape),
            StatementParameterListItem(name="statement", value=statement),
            StatementParameterListItem(name="target_fqn", value=target_fqn),
            StatementParameterListItem(name="tag_key", value=tag_key),
            StatementParameterListItem(name="tag_value", value=tag_value),
            StatementParameterListItem(name="prev_value", value=prev_value),
            StatementParameterListItem(name="state", value=state),
            StatementParameterListItem(name="applied_by", value=applied_by),
            StatementParameterListItem(name="applied_at", value=applied_at),
            StatementParameterListItem(name="error", value=error),
            StatementParameterListItem(name="run_ref", value=None),
        ]

        # Execute as SP (best-effort audit; failure does not fail the apply — BUILD D).
        from backend.services.auth import get_service_principal_client

        client = get_service_principal_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if warehouse_id:
            resp = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id, statement=sql, parameters=params, wait_timeout="10s"
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
    """Flip consumed consents from approved → applied (idempotent). Degrade-not-hang on failure.

    BUILD A: the metastore_id / workspace_id / proposal-id list are all bound via
    ``StatementParameterListItem`` (the IN list gets one named marker per id). BUILD D: this
    consent-flip is a SERVICE-PRINCIPAL write (the SP owns the ledger); only the governed-tag
    write itself is OBO."""
    if not proposal_ids:
        return
    try:
        from backend.services.auth import get_service_principal_client

        client = get_service_principal_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            return

        target = _fqn(_CONSENTS)
        ordered_ids = sorted(proposal_ids)
        placeholders = ", ".join(f":pid{i}" for i in range(len(ordered_ids)))
        params = [
            StatementParameterListItem(name="workspace_id", value=workspace_id),
            StatementParameterListItem(name="metastore_id", value=metastore_id),
        ]
        params += [
            StatementParameterListItem(name=f"pid{i}", value=pid)
            for i, pid in enumerate(ordered_ids)
        ]

        sql = f"""
UPDATE {target}
SET state = 'applied', workspace_id = :workspace_id
WHERE metastore_id = :metastore_id AND proposal_id IN ({placeholders}) AND state = 'approved'
"""
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, parameters=params, wait_timeout="10s"
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


# ──────────────────────────────────────────────────────────────────────────────
# Phase 5 (17j): UNDO — the inverse of a recorded genie_ont_applied membership row.
#
# An applied membership can be undone by the same consenting human, under OBO, from
# the audit trail alone (17j §2). The inverse reuses the SAME allowed tokens (SET TAG
# / UNSET TAG) in THIS file only — it NEVER drops or alters a governed tag (§3): the
# create_tag is not undoable, so undoing a domain reverts its member set_tags and
# leaves the empty governed tag in place, surfaced as an informational note. Same
# guardrails as apply: grant probe → copy-ready, per-statement fail-soft, plan_hash +
# confirm gate, and the OBO-write / SP-bookkeeping identity split (MV-D50).
#
#   | applied row            | prev_value | inverse           |
#   | set_tag  (an add)      | NULL       | UNSET TAG         |
#   | set_tag  (a move)      | <old>      | SET TAG = `<old>` |
#   | unset_tag              | <old>      | SET TAG = `<old>` |
#   | create_tag             | —          | none (§3, noted)  |
# ──────────────────────────────────────────────────────────────────────────────

# Reverse of _SECURABLE_KW: the applied row stores the securable keyword inside its
# `statement`, not asset_type, so the inverse recovers a representative asset_type from
# the parsed keyword (any asset_type mapping to the same keyword yields an identical
# statement, so a representative is exact for the inverse write).
_ASSET_TYPE_FROM_KW = {
    "TABLE": "table",
    "VIEW": "view",
    "SCHEMA": "schema",
    "CATALOG": "catalog",
    "VOLUME": "volume",
    "FUNCTION": "function",
    "COLUMN": "column",
}


def _asset_type_from_statement(statement: str) -> str:
    """Recover the securable asset_type from an applied statement's keyword — the token
    after ``ON`` (``… TAG ON TABLE `c`.`s`.`t` …``). Defaults to ``table`` when unparseable,
    so the inverse targets the SAME securable type the original write did."""
    toks = str(statement).split()
    for i, tok in enumerate(toks):
        if tok.upper() == "ON" and i + 1 < len(toks):
            return _ASSET_TYPE_FROM_KW.get(toks[i + 1].upper(), "table")
    return "table"


def _norm(value: str | None) -> str | None:
    """Normalize a tag value for the no-op comparison: ``""`` ⇒ ``None`` (absent)."""
    if value is None:
        return None
    s = str(value)
    return s or None


def _invert_row(shape: str, prev_value: str | None) -> tuple[str, str | None] | None:
    """The inverse (shape, tag_value) of a recorded membership row, or None if not
    reversible (17j §2). ``create_tag`` is handled by the caller (excluded + noted)."""
    prev = _norm(prev_value)
    if shape == "set_tag":
        # add (prev NULL) → UNSET; move (prev <old>) → SET = <old>.
        return ("unset_tag", None) if prev is None else ("set_tag", prev)
    if shape == "unset_tag":
        # re-add the removed value — only when the pre-value was captured (17j BUILD A).
        return ("set_tag", prev) if prev is not None else None
    return None


async def build_undo_plan(
    metastore_id: str,
    proposal_ids: list[str],
) -> models.ApplyPlan:
    """Dry-run: read the ``state='applied'`` audit rows for ``proposal_ids`` and compute the
    inverse ApplyPlan (17j §2). ``create_tag`` rows are excluded and counted into
    ``notes`` (a governed tag is never auto-dropped, §3). Fingerprinted with the SAME
    ``_plan_hash`` so execute must echo it. Writes nothing. Degrade-not-hang (cold plan)."""
    try:
        rows = await mirror.read_applied_memberships(metastore_id, proposal_ids)
        if not rows:
            return models.ApplyPlan(
                items=[],
                executable_count=0,
                blocked_count=0,
                plan_hash=_plan_hash([]),
                source="mirror",
                as_of=datetime.now(timezone.utc).isoformat(),
            )

        # Read-only probe client (write grants), resolved once; None off-platform → fail-soft.
        client = _probe_client()
        principal = _probe_principal(client)

        items: list[models.ApplyItem] = []
        create_left = 0
        for row in rows:
            shape = str(row.get("shape") or "")
            if shape == "create_tag":
                create_left += 1  # never auto-dropped (§3) — counted for the note.
                continue

            target_fqn = str(row.get("target_fqn") or "")
            tag_key = str(row.get("tag_key") or "")
            if not target_fqn or not tag_key:
                continue

            inverse = _invert_row(shape, row.get("prev_value"))
            if inverse is None:
                continue  # not reversible (e.g. an unset with no captured pre-value)
            inv_shape, inv_value = inverse

            asset_type = _asset_type_from_statement(str(row.get("statement") or ""))
            kind = str(row.get("proposal_kind") or "domain")
            if kind not in ("domain", "subdomain", "reassign"):
                kind = "domain"

            if inv_shape == "unset_tag":
                inv_statement = _unset_tag_statement(target_fqn, asset_type, tag_key)
            else:
                inv_statement = _set_tag_statement(target_fqn, asset_type, tag_key, inv_value)

            executable, blocked_reason, required_grants = _probe_write(
                client, target_fqn, tag_key, asset_type, principal
            )
            # current_value carries the value being reverted FROM (the value the original
            # applied row put on the asset) — for the diff and the new audit row's prev_value.
            reverted_from = str(row["tag_value"]) if row.get("tag_value") else None
            items.append(
                models.ApplyItem(
                    proposal_id=str(row.get("proposal_id") or ""),
                    proposal_kind=kind,  # type: ignore[arg-type]
                    shape=inv_shape,  # type: ignore[arg-type]
                    target_fqn=target_fqn,
                    tag_key=tag_key,
                    tag_value=inv_value,
                    current_value=reverted_from,
                    statement=inv_statement,
                    executable=executable,
                    blocked_reason=blocked_reason,
                    required_grants=required_grants,
                )
            )

        notes: list[str] = []
        if create_left:
            plural = "s" if create_left != 1 else ""
            notes.append(
                f"{create_left} grouping{plural} left in place — undo never removes a grouping."
            )

        executable_count = sum(1 for item in items if item.executable)
        return models.ApplyPlan(
            items=items,
            executable_count=executable_count,
            blocked_count=len(items) - executable_count,
            plan_hash=_plan_hash(items),
            source="mirror",
            as_of=datetime.now(timezone.utc).isoformat(),
            notes=notes,
        )
    except Exception as e:
        logger.exception("build_undo_plan failed: %s", e)
        return models.ApplyPlan(
            items=[],
            executable_count=0,
            blocked_count=0,
            plan_hash=_plan_hash([]),
            source="cold",
            as_of=datetime.now(timezone.utc).isoformat(),
        )


async def execute_undo_plan(
    plan: models.ApplyPlan,
    metastore_id: str,
    workspace_id: str,
    applied_by: str,
) -> models.ApplyResult:
    """Execute the inverse plan under OBO — the mirror of ``execute_apply_plan``. Per-statement
    fail-soft; a current-value no-op guard (already at the post-undo value ⇒ ``state='noop'``,
    so a double-undo is safe); each action appends a NEW ``genie_ont_applied`` row for the
    inverse shape (the original row is never mutated), with the value being reverted FROM as
    its ``prev_value``; then the consumed consents flip ``applied → approved`` (SP). Idempotent:
    the inverse ``apply_id`` collides on a repeat, and the consent re-flip is a no-op re-run."""
    result = models.ApplyResult(as_of=datetime.now(timezone.utc).isoformat())

    try:
        client = require_obo_workspace_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            logger.error("SQL_WAREHOUSE_ID not configured; cannot execute undo")
            return result

        for item in plan.items:
            if not item.executable:
                result.blocked.append(
                    models.ApplyOutcome(
                        proposal_id=item.proposal_id,
                        shape=item.shape,
                        target_fqn=item.target_fqn,
                        ok=False,
                        state="blocked",
                        error=item.blocked_reason,
                    )
                )
                continue

            asset_type = _asset_type_from_statement(item.statement)
            apply_id = _apply_id(item.proposal_id, item.shape, item.target_fqn, item.tag_value)

            # Current-value no-op guard (17j §2): if the asset is already at the post-undo
            # value (item.tag_value; None ⇒ tag absent), the undo is a no-op — record it and
            # skip the write, so a double-undo never re-writes or errors.
            current = grants.current_tag_value(
                client, item.target_fqn, item.tag_key, asset_type=asset_type
            )
            if _norm(current) == _norm(item.tag_value):
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
                    prev_value=_norm(current),
                    state="noop",
                    applied_by=applied_by,
                    error=None,
                )
                result.applied.append(
                    models.ApplyOutcome(
                        proposal_id=item.proposal_id,
                        shape=item.shape,
                        target_fqn=item.target_fqn,
                        ok=True,
                        state="applied",
                        error=None,
                    )
                )
                continue

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

                # Append a NEW audit row for the inverse action (never mutate the original).
                # prev_value = the value being reverted FROM (the value the asset carried).
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

                result.applied.append(
                    models.ApplyOutcome(
                        proposal_id=item.proposal_id,
                        shape=item.shape,
                        target_fqn=item.target_fqn,
                        ok=True,
                        state="applied",
                        error=None,
                    )
                )

            except Exception as e:
                error_str = str(e)
                state = "failed" if "PERMISSION_DENIED" not in error_str else "blocked"
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
                logger.warning("undo item failed: %s/%s (%s)", item.proposal_id, item.shape, error_str)

        # Re-surface the undone proposals: flip their consents applied → approved (SP).
        _reflip_consents(
            metastore_id=metastore_id,
            workspace_id=workspace_id,
            proposal_ids={item.proposal_id for item in plan.items},
            applied_by=applied_by,
        )

    except Exception as e:
        logger.exception("execute_undo_plan failed: %s", e)

    return result


def _reflip_consents(
    metastore_id: str,
    workspace_id: str,
    proposal_ids: set[str],
    applied_by: str,
) -> None:
    """Flip the undone consents ``applied → approved`` (the inverse of ``_flip_consents``) so
    an undone proposal re-surfaces as actionable. Idempotent; degrade-not-hang. Every value —
    including the IN list — is bound via ``StatementParameterListItem``. BUILD D identity split:
    this consent re-flip is a SERVICE-PRINCIPAL write (the SP owns the ledger); only the
    governed-tag write itself is OBO."""
    if not proposal_ids:
        return
    try:
        from backend.services.auth import get_service_principal_client

        client = get_service_principal_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            return

        target = _fqn(_CONSENTS)
        ordered_ids = sorted(proposal_ids)
        placeholders = ", ".join(f":pid{i}" for i in range(len(ordered_ids)))
        params = [
            StatementParameterListItem(name="workspace_id", value=workspace_id),
            StatementParameterListItem(name="metastore_id", value=metastore_id),
        ]
        params += [
            StatementParameterListItem(name=f"pid{i}", value=pid)
            for i, pid in enumerate(ordered_ids)
        ]

        sql = f"""
UPDATE {target}
SET state = 'approved', workspace_id = :workspace_id
WHERE metastore_id = :metastore_id AND proposal_id IN ({placeholders}) AND state = 'applied'
"""
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, parameters=params, wait_timeout="10s"
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
        logger.warning("consent re-flip failed (non-fatal): %s", e)
