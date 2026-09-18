"""BROWSE grant surfacing for the app service principal (MV-D42).

The ontology tag-graph reader runs as the *service principal* (system tables are
not OBO-readable). But governed-tag **assignments** come from
``system.information_schema.*_tags``, which is *privilege-filtered* — a principal
only sees rows for objects it has a privilege on. With just ``USE CATALOG`` the SP
sees zero tables, so every domain renders with **0 members** even though the tags
exist. The minimal, governance-appropriate fix is ``BROWSE`` on each allowlisted
catalog: it exposes object metadata **and tags** to the SP while granting **no
access to the underlying data**.

**Why we surface the grant instead of applying it in-app.** The app's OBO token is
scoped read-only for Unity Catalog — the deployed ``user_api_scopes`` are
``catalog.catalogs:read`` / ``catalog.schemas:read`` / ``catalog.tables:read`` plus
``sql`` and ``iam.access-control:read``, with **no UC write/manage scope**.
Databricks blocks any functionality outside the approved scopes *even if the user
has the underlying privilege*, so ``grants.update`` (the REST permissions API) is
rejected under OBO regardless of authority. The app SP itself only holds
``USE CATALOG`` and cannot grant to itself either. So the reliable path is the one
the MV advisor already uses for its remediation SQL (``mv_entitlement._remediation_sql``):
render a **copy-ready ``GRANT`` statement** for a human with ``MANAGE``/ownership to
run. The preflight banner shows it whenever member counts read 0 because the SP is
blind (see ``routers/preflight``).
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def app_service_principal() -> str | None:
    """The app SP's client id — the grant *target*.

    Databricks Apps inject ``DATABRICKS_CLIENT_ID`` (the app's own service
    principal). Returns ``None`` off-platform so callers fall back to the
    ``<app-service-principal>`` placeholder in copy-ready grant lines.
    """
    sp = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    return sp or None


def browse_grant_line(catalog: str, sp: str | None) -> str:
    """A single copy-paste-ready ``GRANT BROWSE`` statement for the banner."""
    target = f"`{sp}`" if sp else "`<app-service-principal>`"
    return f"GRANT BROWSE ON CATALOG `{catalog}` TO {target}"


def browse_needed(*, tag_ok: bool, allowlist: list[str], sp_seen: int, obo_seen: int) -> bool:
    """Decide whether BROWSE is the missing link (pure — unit-tested directly).

    True only when the governed-tag catalog is readable (so the tree renders) yet
    the SP sees **no** assignments in scope while the admin (OBO) sees some — the
    exact signature of "privilege-filtered information_schema hid the members".
    Both-zero means there genuinely are no assignments, so we never nag.
    """
    return bool(tag_ok and allowlist and sp_seen == 0 and obo_seen > 0)


# ── Phase 5 (17i) Stage 2: read-only write-grant + current-value probes ─────────
#
# The apply plan (``services/apply.py``) needs two READ-ONLY facts per pending
# governed-tag write, resolved as the OBO viewer (MV-D50) WITHOUT touching UC:
#   1. Does the viewer hold the write grants (APPLY TAG / USE SCHEMA / USE CATALOG on
#      the asset; ASSIGN on the governed tag)?  → ``membership_write_probe``.
#   2. What value (if any) does the tag already carry on the asset?  → ``current_tag_value``
#      (drives the "add" vs "move" diff, MV-D23).
# Both are DELIBERATELY fail-soft (MV-D43): a probe that cannot determine the answer
# returns the permissive default (executable / no current value) so the apply still
# offers the change and lets the OBO execute degrade to copy-ready on a real denial —
# a probe is never allowed to block a change it merely could not verify. Neither
# function writes anything. The caller (``build_apply_plan``) resolves the client only
# on-platform, so the offline suite passes ``client=None`` and both short-circuit.

# UC securable keyword for the grants API + the information_schema tag view suffix.
_SECURABLE_TYPE = {
    "table": "table",
    "view": "table",
    "materialized_view": "table",
    "streaming_table": "table",
    "schema": "schema",
    "catalog": "catalog",
    "volume": "volume",
    "function": "function",
    "column": "table",
}
_TAG_VIEW = {"table": "table_tags", "schema": "schema_tags", "catalog": "catalog_tags", "volume": "volume_tags"}


def _bt(name: str) -> str:
    """Backtick-quote a single identifier, doubling any internal backtick (injection-safe)."""
    return "`" + str(name).replace("`", "``") + "`"


def _bt_fqn(fqn: str) -> str:
    """Backtick-quote each dotted segment of an asset FQN (``a.b.c`` → `` `a`.`b`.`c` ``)."""
    parts = [p for p in str(fqn).split(".") if p != ""]
    return ".".join(_bt(p) for p in parts) if parts else _bt(fqn)


def write_grant_lines(target_fqn: str, tag_key: str, *, asset_type: str = "table", principal: str | None = None) -> list[str]:
    """Copy-ready GRANT lines the applying human needs to write this membership (MV-D43).

    Rendered when a probe positively finds a missing privilege; the curator hands them to
    an account admin. Identifiers are backtick-escaped; the principal defaults to a
    ``<the applying user>`` placeholder when unresolved (mirrors ``browse_grant_line``)."""
    who = _bt(principal) if principal else "`<the applying user>`"
    sec = _SECURABLE_TYPE.get((asset_type or "table").lower(), "table").upper()
    parts = [p for p in str(target_fqn).split(".") if p != ""]
    lines = [f"GRANT APPLY TAG ON {sec} {_bt_fqn(target_fqn)} TO {who}"]
    if len(parts) >= 2:
        lines.append(f"GRANT USE SCHEMA ON SCHEMA {_bt_fqn('.'.join(parts[:2]))} TO {who}")
    if parts:
        lines.append(f"GRANT USE CATALOG ON CATALOG {_bt(parts[0])} TO {who}")
    if tag_key:
        # ASSIGN on the governed tag itself (not a create/alter/drop — safe for the firewall).
        lines.append(f"GRANT ASSIGN ON GOVERNED TAG {_bt(tag_key)} TO {who}")
    return lines


def membership_write_probe(
    client, target_fqn: str, tag_key: str, *, asset_type: str = "table", principal: str | None = None
) -> tuple[bool, list[str]]:
    """Read the OBO viewer's effective write privileges on ``target_fqn`` WITHOUT writing.

    Returns ``(ok, missing_grant_lines)``:
      - ``(True, [])``  — the viewer holds APPLY TAG (or ALL PRIVILEGES / ownership), OR the
        probe is INDETERMINATE (no client, unreadable, or SDK failure). Fail-soft (MV-D43):
        an unverifiable probe never blocks — the OBO execute still degrades to copy-ready on a
        real ``PERMISSION_DENIED``.
      - ``(False, [grant lines])`` — the viewer's privileges ARE readable and APPLY TAG is
        positively absent; the item is blocked → copy-ready with the exact grants.
    """
    if client is None:
        return True, []
    try:
        sec = _SECURABLE_TYPE.get((asset_type or "table").lower(), "table")
        eff = client.grants.get_effective(securable_type=sec, full_name=target_fqn)
        assignments = getattr(eff, "privilege_assignments", None) or []
        privs: set[str] = set()
        for pa in assignments:
            for p in (getattr(pa, "privileges", None) or []):
                privs.add(str(getattr(p, "privilege", p)).upper())
        if not privs:
            # Nothing readable → indeterminate, not a positive "missing" → fail-soft.
            return True, []
        if any("APPLY_TAG" in x or "ALL_PRIVILEGES" in x for x in privs):
            return True, []
        # Privileges ARE readable and APPLY TAG is not among them → a real gap.
        return False, write_grant_lines(target_fqn, tag_key, asset_type=asset_type, principal=principal)
    except Exception as e:  # noqa: BLE001 — a probe never raises (MV-D43); indeterminate → fail-soft
        logger.info("membership_write_probe indeterminate for %s: %s", target_fqn, e)
        return True, []


def current_tag_value(client, target_fqn: str, tag_key: str, *, asset_type: str = "table") -> str | None:
    """The current value of ``tag_key`` on ``target_fqn`` (read-only), for the add-vs-move
    diff (MV-D23). Reads ``system.information_schema.<securable>_tags`` as the OBO viewer via
    the SQL warehouse, VALUES bound as parameters. Returns the value, or ``None`` when the tag
    is absent OR the read is unavailable/unreadable — ``None`` is byte-identical to the pre-Stage-2
    behaviour (a plain "add"), so a degraded read never invents a "move"."""
    if client is None or not tag_key:
        return None
    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        return None
    parts = [p for p in str(target_fqn).split(".") if p != ""]
    if len(parts) < 3:
        return None  # column/other shapes: no clean information_schema key → degrade to "add"
    try:
        import time

        from databricks.sdk.service.sql import StatementParameterListItem, StatementState

        sec = _SECURABLE_TYPE.get((asset_type or "table").lower(), "table")
        view = _TAG_VIEW.get(sec, "table_tags")
        catalog, schema, table = parts[0], parts[1], parts[2]
        sql = (
            f"SELECT tag_value FROM system.information_schema.{view} "
            "WHERE catalog_name = :c AND schema_name = :s AND table_name = :t AND tag_name = :k LIMIT 1"
        )
        params = [
            StatementParameterListItem(name="c", value=catalog),
            StatementParameterListItem(name="s", value=schema),
            StatementParameterListItem(name="t", value=table),
            StatementParameterListItem(name="k", value=tag_key),
        ]
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, parameters=params, wait_timeout="15s"
        )
        deadline = time.monotonic() + 20
        while resp and resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline or not resp.statement_id:
                return None
            time.sleep(0.5)
            resp = client.statement_execution.get_statement(statement_id=resp.statement_id)
        if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
            return None
        if not resp.result or not resp.result.data_array:
            return None
        row = resp.result.data_array[0]
        return str(row[0]) if row and row[0] is not None else None
    except Exception as e:  # noqa: BLE001 — a read never raises; degrade to "add"
        logger.info("current_tag_value indeterminate for %s/%s: %s", target_fqn, tag_key, e)
        return None


def membership_write_status(
    client, catalogs: list[str], sp: str | None, *, principal: str | None = None
) -> tuple[str, list[str], str | None]:
    """The preflight ``membership_write`` tier (BUILD B) — a REAL read-only view of the OBO
    viewer's apply capability (mirrors the enrichment tier's ok/blocked shape). Returns
    ``(status, grant_lines, reason)``:

    - No catalogs in scope → ``("not_exercised", …, "select catalogs")`` — nothing to probe yet.
    - Viewer holds APPLY TAG on a representative catalog → ``("ok", informational lines, None)``.
    - Viewer positively lacks it → ``("blocked", copy-ready lines, reason)``.
    - Indeterminate (no client off-platform, or unreadable) → ``("ok", informational lines, note)``
      — the exact per-change grants are re-checked at preview time (MV-D43 fail-soft).
    """
    informational = [
        "GRANT APPLY TAG / USE SCHEMA / USE CATALOG on the target assets (to the applying user)",
        "GRANT ASSIGN on each governed tag (to the applying user)",
    ]
    if not catalogs:
        return "not_exercised", informational, (
            "No catalogs selected yet — choose catalogs in Settings, then the apply "
            "capability is checked against them."
        )
    if client is None:
        return "ok", informational, (
            "Membership apply runs under your account (the signed-in admin); the exact "
            "grants for each change are confirmed when you preview it."
        )
    catalog = catalogs[0]
    ok, missing = membership_write_probe(client, catalog, "", asset_type="catalog", principal=principal)
    if ok:
        return "ok", informational, None
    return "blocked", missing, (
        "You can preview the changes, but applying them needs a grant first — copy the "
        "steps below for an account admin, then preview again."
    )
