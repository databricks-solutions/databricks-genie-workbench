"""BROWSE grant surfacing + detection (MV-D42).

The tag-graph reader runs as the SP, but governed-tag assignments come from
privilege-filtered ``information_schema`` — without BROWSE the SP sees 0 rows and
every domain renders with 0 members. The app's OBO token is scoped read-only for
UC (no UC-write scope), so we surface a copy-ready GRANT for an admin rather than
applying it in-app. These tests pin the helper contracts:
- ``browse_needed`` fires only on the exact "SP blind, OBO sees tags" signature;
- the copy-ready grant line targets the resolved SP (placeholder off-platform).
"""

from __future__ import annotations

from backend.ontology.services import grants


def test_browse_grant_line_targets_resolved_sp():
    line = grants.browse_grant_line("finance", "abc-123")
    assert line == "GRANT BROWSE ON CATALOG `finance` TO `abc-123`"


def test_browse_grant_line_falls_back_to_placeholder():
    line = grants.browse_grant_line("finance", None)
    assert line == "GRANT BROWSE ON CATALOG `finance` TO `<app-service-principal>`"


def test_app_service_principal_reads_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-xyz")
    assert grants.app_service_principal() == "sp-xyz"
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    assert grants.app_service_principal() is None


def test_browse_needed_only_when_sp_blind_but_obo_sees_tags():
    # The bug's signature: tree renders (tag_ok), SP sees 0, OBO sees some.
    assert grants.browse_needed(tag_ok=True, allowlist=["c"], sp_seen=0, obo_seen=5) is True
    # SP already sees assignments → grant is present.
    assert grants.browse_needed(tag_ok=True, allowlist=["c"], sp_seen=3, obo_seen=5) is False
    # Genuinely no tags anywhere → don't nag.
    assert grants.browse_needed(tag_ok=True, allowlist=["c"], sp_seen=0, obo_seen=0) is False
    # No scope / catalog unreadable → not a BROWSE problem.
    assert grants.browse_needed(tag_ok=True, allowlist=[], sp_seen=0, obo_seen=5) is False
    assert grants.browse_needed(tag_ok=False, allowlist=["c"], sp_seen=0, obo_seen=5) is False


def test_grants_module_has_no_uc_write_path():
    """We surface copy-ready GRANTs, never execute them: the OBO token is scoped
    read-only for UC. Pin that the module is pure — no SDK client or permissions
    types in its namespace and no apply function."""
    assert not hasattr(grants, "grant_browse_to_sp")
    # A pure copy-SQL surface imports no SDK write machinery.
    assert "WorkspaceClient" not in vars(grants)
    assert "PermissionsChange" not in vars(grants)
    assert "grants" not in vars(grants)  # no `from databricks.sdk... import grants`
