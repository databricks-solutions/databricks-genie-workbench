"""Ontology decision ledger writes (Phase 3d, MV-D26) — the ONLY writer of the
consent / suppression ledger, and it writes under **OBO** (attributed to the
deciding human).

A human adjudicates a served draft (17.0d/17.0e): ``approve`` and an accepted
``reassign`` record a **consent** (state ``approved`` — 17i's apply will consume it;
**no UC write now**); ``dismiss`` and a **rejected** ``reassign`` record a
**suppression** so a re-run never resurfaces the resolved proposal (the wheel reads
this ledger and marks the proposal ``surfaced=false``). Idempotent on
``(metastore_id, proposal_kind, proposal_id)`` — re-deciding the same proposal
updates the row in place; a rejected reassign stays suppressed on re-run.

These are **app-state Delta rows**, written via the existing SQL warehouse under the
OBO identity — **not** a Unity Catalog governance mutation. No tag apply, no apply.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Literal

from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)

_CONSENTS = "genie_ont_consents"
_SUPPRESSIONS = "genie_ont_suppressions"

# action → (ledger table, recorded kind). approve / reassign_accept pin the human's
# "yes" as a consent; dismiss / reassign_reject record a suppression (MV-D26).
_ACTION_TARGET: dict[str, tuple[str, Literal["consent", "suppression"]]] = {
    "approve": (_CONSENTS, "consent"),
    "reassign_accept": (_CONSENTS, "consent"),
    "dismiss": (_SUPPRESSIONS, "suppression"),
    "reassign_reject": (_SUPPRESSIONS, "suppression"),
}


def _fqn(table: str) -> str:
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _merge_sql(table: str) -> str:
    """Idempotent single-row MERGE keyed on (metastore_id, proposal_kind, proposal_id)
    — matched updates in place (so re-deciding is idempotent), unmatched inserts. Uses
    named parameters (``:name``) so no value is interpolated into the SQL text."""
    target = _fqn(table)
    key_on = (
        "t.metastore_id = s.metastore_id AND t.proposal_kind = s.proposal_kind "
        "AND t.proposal_id = s.proposal_id"
    )
    src = (
        "SELECT :metastore_id AS metastore_id, :workspace_id AS workspace_id, "
        ":proposal_kind AS proposal_kind, :proposal_id AS proposal_id"
    )
    if table == _CONSENTS:
        return (
            f"MERGE INTO {target} AS t USING ({src}) AS s ON {key_on} "
            "WHEN MATCHED THEN UPDATE SET state = :state, decided_by = :decided_by, "
            "decided_at = current_timestamp(), workspace_id = s.workspace_id "
            "WHEN NOT MATCHED THEN INSERT (metastore_id, workspace_id, proposal_kind, "
            "proposal_id, state, decided_by, decided_at) VALUES (s.metastore_id, "
            "s.workspace_id, s.proposal_kind, s.proposal_id, :state, :decided_by, current_timestamp())"
        )
    return (
        f"MERGE INTO {target} AS t USING ({src}) AS s ON {key_on} "
        "WHEN MATCHED THEN UPDATE SET reason = :reason, dismissed_by = :decided_by, "
        "dismissed_at = current_timestamp(), workspace_id = s.workspace_id "
        "WHEN NOT MATCHED THEN INSERT (metastore_id, workspace_id, proposal_kind, "
        "proposal_id, reason, dismissed_by, dismissed_at) VALUES (s.metastore_id, "
        "s.workspace_id, s.proposal_kind, s.proposal_id, :reason, :decided_by, current_timestamp())"
    )


def record_decision(
    *,
    kind: str,
    proposal_id: str,
    action: str,
    metastore_id: str,
    workspace_id: str,
    decided_by: str,
) -> Literal["consent", "suppression"]:
    """Record one human decision in the ledger under OBO, idempotently. Returns the
    ledger it wrote (``"consent"`` / ``"suppression"``). Raises ``ValueError`` on an
    unknown action; raises on a warehouse failure so the route surfaces it (the ledger
    write is the whole point of the call — a silent failure would drop the decision).

    ``decided_by`` is the OBO email (attribution, MV-D50). ``proposal_kind`` = the
    ``DecisionKind`` (``domain``/``subdomain``/``page``/``reassign``), which the wheel
    matches when it reads the suppression ledger. **No governed-tag write; no apply.**
    """
    target = _ACTION_TARGET.get(action)
    if target is None:
        raise ValueError(f"unknown decision action: {action!r}")
    table, recorded = target

    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        raise RuntimeError("SQL_WAREHOUSE_ID is not configured; cannot record the decision")

    params = [
        StatementParameterListItem(name="metastore_id", value=metastore_id),
        StatementParameterListItem(name="workspace_id", value=workspace_id),
        StatementParameterListItem(name="proposal_kind", value=kind),
        StatementParameterListItem(name="proposal_id", value=proposal_id),
        StatementParameterListItem(name="decided_by", value=decided_by),
    ]
    if table == _CONSENTS:
        params.append(StatementParameterListItem(name="state", value="approved"))
    else:
        params.append(StatementParameterListItem(name="reason", value=f"{action} by curator"))

    # OBO: the write is attributed to the deciding human (their token backs the client).
    client = get_workspace_client()
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=_merge_sql(table), parameters=params, wait_timeout="30s",
    )
    statement_id = resp.statement_id if resp else None
    deadline = time.monotonic() + 40
    while resp and resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline or not statement_id:
            raise RuntimeError("decision ledger write timed out")
        time.sleep(1.0)
        resp = client.statement_execution.get_statement(statement_id=statement_id)
    if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
        detail = getattr(getattr(resp, "status", None), "error", None)
        raise RuntimeError(f"decision ledger write failed: {detail}")
    logger.info("ontology decision recorded: %s %s/%s by %s", recorded, kind, proposal_id, decided_by)
    return recorded


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
