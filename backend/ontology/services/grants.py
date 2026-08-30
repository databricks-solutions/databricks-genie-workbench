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

import os


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
