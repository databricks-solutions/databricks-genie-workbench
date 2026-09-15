"""Phase 4 Stage C (17h) — the external-context user surface (backend slices).

Covers the two backend-side pieces of Stage C:

1. ``mirror._assemble_domain_draft`` surfaces a labeled/dated ``sources`` list ONLY when
   the name came from an APPLIED external Context Pack prior (MV-D23/D35). A curated name
   that the prior merely corroborated (``applied=false``) — or no prior at all — surfaces
   ``[]`` so no chip renders and the estate-only path is byte-identical.
2. ``grant_permissions.context_source_grant_statements`` emits a copy-ready
   ``GRANT EXECUTE`` for an enabled UC-securable source, skips workspace-path MCPs, and is
   a no-op when nothing is enabled (DEFAULT OFF, MV-D44).
"""

from __future__ import annotations

import json

from backend.ontology.services import mirror
from scripts.grant_permissions import (
    CONTEXT_MCP_OAUTH_SCOPES,
    context_source_grant_statements,
)


def _row(evidence: dict) -> dict:
    return {
        "domain_id": "d1",
        "name": "Merchandising",
        "description": "d",
        "tag_decision": "create",
        "evidence": json.dumps(evidence),
    }


def test_applied_naming_prior_surfaces_a_labeled_dated_source():
    row = _row({
        "surfaced": True,
        "rank": {
            "tier": "high",
            "naming_prior": {
                "value": "Merchandising",
                "tier": "T2",
                "source_url": "https://example.com/model",
                "source_kind": "industry_model",
                "as_of": "2025-06-01",
                "applied": True,
                "outranked_by": None,
            },
        },
    })
    draft = mirror._assemble_domain_draft(row, [], [], "high")
    assert draft["sources"] == [
        {"label": "Industry reference", "url": "https://example.com/model", "as_of": "2025-06-01"}
    ]
    # Zero-burden: the chip never leaks the pack / tier / provenance machinery (MV-D23).
    blob = json.dumps(draft["sources"]).lower()
    for token in ("t2", "naming_prior", "provenance", "pack", "outranked"):
        assert token not in blob


def test_unapplied_prior_surfaces_no_source():
    # A curated (T0) name that the prior only corroborated — applied=false ⇒ no chip.
    row = _row({
        "surfaced": True,
        "rank": {
            "tier": "high",
            "naming_prior": {
                "value": "Finance",
                "tier": "T2",
                "source_url": "https://example.com/model",
                "source_kind": "industry_model",
                "as_of": "2025-06-01",
                "applied": False,
                "outranked_by": "curated",
            },
        },
    })
    assert mirror._assemble_domain_draft(row, [], [], "high")["sources"] == []


def test_no_prior_surfaces_no_source():
    # Estate-only run (external context off) — no naming_prior at all ⇒ [] ⇒ no chip.
    row = _row({"surfaced": True, "rank": {"tier": "high"}})
    assert mirror._assemble_domain_draft(row, [], [], "high")["sources"] == []


def test_grant_statements_for_enabled_uc_source():
    stmts = context_source_grant_statements(["web_search"], "app-sp-123")
    assert stmts == ["GRANT EXECUTE ON `system.ai.web_search` TO `app-sp-123`"]


def test_grant_statements_skip_workspace_path_mcp():
    # Genie One is a workspace-path MCP (/api/2.0/mcp/genie) — not a grantable securable.
    assert context_source_grant_statements(["genie_one"], "app-sp-123") == []


def test_grant_statements_no_op_when_nothing_enabled():
    assert context_source_grant_statements([], "app-sp-123") == []


def test_oauth_scopes_cover_the_managed_mcp_path():
    # §1 Stage C: the OBO managed-MCP path needs these scopes.
    assert set(CONTEXT_MCP_OAUTH_SCOPES) == {"genie", "sql", "unity-catalog", "ai-search"}
