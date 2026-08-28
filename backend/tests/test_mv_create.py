"""Tests for the metric view create-and-attach service and its routes (Prompt 9).

Two things matter and are tested at the seam, not end to end (no Databricks):

- The **MV-D22 abort guard**: when a fresh probe would force the stored YAML to a
  lower ladder rung than it was rendered for, the create path drops that
  suggestion instead of installing the wrong artifact. This passes vacuously
  today (MV-D13 pins both compute paths to the same floor), so it is asserted
  against a *synthetically* stricter revalidation to prove the guard is wired,
  not merely dormant.
- The **lifecycle routes**: proposals list, DDL artifact, approve/reject
  decision, and the OBO-gated drop that refuses a non-owner or a non-DETACHED
  object.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import mv_create
from genie_space_optimizer.common import warehouse
from genie_space_optimizer.optimization import mv_yaml


# ── Service: create_and_attach_for_run ─────────────────────────────────────


def _verification(effective_mode="create_and_attach", downgrade_reason=None, verdict="SUFFICIENT"):
    fresh = SimpleNamespace(capabilities=[], checked_as="analyst@example.com")
    return SimpleNamespace(
        effective_mode=effective_mode,
        downgrade_reason=downgrade_reason,
        verdict=verdict,
        fresh_probe=fresh,
    )


_CONSENT = {"target_catalog": "finance", "target_schema": "sales", "probe_id": "p1"}
_ARTIFACT = {
    "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
    "join_strategy": "nested",
    "proposed_object": "warehouse.raw.revenue_metrics",
}


@pytest.fixture
def create_env(monkeypatch):
    """Wire the create path down to captured warehouse calls."""
    executed: list[str] = []
    upserts: list[dict] = []

    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (_verification(), dict(_CONSENT)),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": "fp1"}],
    )
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact", lambda *a, **k: dict(_ARTIFACT),
    )
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: False)
    monkeypatch.setattr(mv_create, "_confirm_metric_view", lambda *a, **k: True)
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: executed.append(sql),
    )
    monkeypatch.setattr(
        warehouse, "wh_upsert_mv_created_object",
        lambda ws, warehouse_id, **kw: (upserts.append(kw) or kw["full_name"]),
    )
    return executed, upserts


def _run_create():
    return mv_create.create_and_attach_for_run(
        "run-1",
        space_id="space-1",
        probe_id="p1",
        approved_suggestion_ids=["sug1"],
        catalog="main",
        schema="gso",
        warehouse_id="wh1",
    )


def test_happy_path_creates_and_attaches_at_the_consented_name(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert handoff.action_mode == "create_and_attach"
    # Re-targeted to the CONSENTED catalog/schema, not the render-time
    # proposed_object (warehouse.raw.*). Base name is preserved.
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"
    assert upserts[0]["created_by"] == "analyst@example.com"


def test_candidate_yaml_text_is_the_fallback_when_no_artifact(create_env, monkeypatch):
    """MV-D23 severs coupling 3: a standalone advice candidate carries the
    replay body on the row, so an absent run-partitioned artifact is not a skip."""
    executed, upserts = create_env
    # No run-keyed artifact (the standalone advice path never wrote one) — the
    # candidate row carries yaml_text + evidence.join_strategy instead.
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1",
            "dedup_fingerprint": "fp1",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "evidence": {"join_strategy": "nested"},
        }],
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"


def test_no_artifact_and_no_candidate_body_skips(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    # Candidate row has no yaml_text either — nothing replayable, so it skips.
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": "fp1"}],
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


def test_revalidation_downgrade_aborts_the_create(create_env, monkeypatch):
    """MV-D22: a rung below the stored one drops the suggestion, never creates."""
    executed, upserts = create_env
    # Stored strategy is "nested"; a stricter probe forces "subquery_source".
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(
            ok=True, downgrade_to="subquery_source",
        ),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.attach_views == []
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


def test_revalidation_failure_drops_the_suggestion(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=False, errors=("bad yaml",)),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)


def test_no_consent_downgrades_without_probing(monkeypatch):
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "verify_consent", lambda **kw: (None, None))

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.attach_views == []


def test_verify_downgrade_returns_suggest_only(monkeypatch):
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (_verification(effective_mode="suggest_only",
                                    downgrade_reason="revoked"), dict(_CONSENT)),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == "revoked"


def test_downgrade_stamps_the_consent_with_run_and_reason(create_env, monkeypatch):
    """Prompt 15.5 / Scenario B: an auto-downgraded run stamps ``run_id`` +
    ``downgrade_reason`` (and the re-verified verdict) onto the consent, so
    ``/mv-created`` — which reads the consent BY run — stops surfacing NULL. The
    stamp is the missing warehouse twin of ``mark_mv_consent_reverified``."""
    _executed, _upserts = create_env
    stamps: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append(kw),
    )
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (
            _verification(
                effective_mode="suggest_only",
                downgrade_reason="grant revoked before trigger",
                verdict="INSUFFICIENT",
            ),
            dict(_CONSENT),
        ),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == "grant revoked before trigger"
    assert len(stamps) == 1
    assert stamps[0]["probe_id"] == "p1"
    assert stamps[0]["run_id"] == "run-1"
    assert stamps[0]["verdict"] == "INSUFFICIENT"
    assert stamps[0]["downgrade_reason"] == "grant revoked before trigger"


def test_success_stamps_the_consent_run_without_a_downgrade_reason(create_env, monkeypatch):
    """The success path binds the run to the consent (so ``/mv-created`` can find
    it) and records the re-verified verdict, but leaves ``downgrade_reason``
    unset — a create-and-attach is not a downgrade."""
    _executed, _upserts = create_env
    stamps: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append(kw),
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert handoff.action_mode == "create_and_attach"
    assert len(stamps) == 1
    assert stamps[0]["run_id"] == "run-1"
    assert stamps[0]["verdict"] == "SUFFICIENT"
    assert stamps[0].get("downgrade_reason") is None


def test_existing_object_is_not_clobbered(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)


@pytest.mark.parametrize(
    "downgrade_to,stored,expected",
    [
        ("subquery_source", "nested", True),   # forced below where it was rendered
        (None, "nested", False),               # no downgrade demanded
        ("subquery_source", "subquery_source", False),  # already at that rung
    ],
)
def test_rung_below(downgrade_to, stored, expected):
    assert mv_create._rung_below(downgrade_to, stored) is expected


# ── Routes ─────────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_list_mv_proposals_returns_the_rows(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
            "confidence_score": 82.0, "approved_for_rerun": True,
        }],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["proposals"][0]["suggestion_id"] == "sug1"
    assert data["proposals"][0]["approved_for_rerun"] is True


def test_list_space_mv_proposals_filters_by_space_and_approved(client, monkeypatch):
    captured: dict = {}

    def fake_load(*args, **kwargs):
        captured.update(kwargs)
        return [{
            "suggestion_id": "sug_space", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
            "confidence_score": 91.0, "approved_for_rerun": True,
        }]

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", fake_load)
    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true")
    assert resp.status_code == 200
    data = resp.json()
    assert data["space_id"] == "space-1"
    assert data["proposals"][0]["suggestion_id"] == "sug_space"
    # The gate is space-scoped; run_id must NOT stand in for it (MV-D23).
    assert captured.get("target_space_id") == "space-1"
    assert captured.get("approved_for_rerun") is True
    assert captured.get("run_id") is None


def test_list_space_mv_proposals_defaults_approved_to_none(client, monkeypatch):
    captured: dict = {}

    def fake_load(*args, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", fake_load)
    # MV-D31: the unfiltered panel load also reads the last-scan summary; stub it
    # so this test pins the candidate read + response shape (last_scan hydrates on
    # its own, and is None here — this space has no advice run in the stub).
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", lambda *a, **k: None)
    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")
    assert resp.status_code == 200
    assert resp.json() == {"space_id": "space-1", "proposals": [], "last_scan": None}
    assert captured.get("approved_for_rerun") is None


def test_list_space_mv_proposals_hydrates_last_scan(client, monkeypatch):
    """MV-D31 hydrate-on-mount: the unfiltered panel load returns the last scan's
    timestamp, real duration, and empty/skip state, so the surface opens showing
    "last scanned … — N proposals" instead of a bare button."""
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    monkeypatch.setattr(
        warehouse, "wh_load_latest_advice_scan",
        lambda *a, **k: {
            "run_id": "run-adv-9",
            "scanned_at": "2026-08-25T05:00:00Z",
            "status": "SKIPPED",
            "duration_seconds": 252.0,
            "skip_reason": "NO_CANDIDATES",
            "measures_found": 4,
        },
    )
    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")
    assert resp.status_code == 200
    scan = resp.json()["last_scan"]
    assert scan["status"] == "SKIPPED"
    assert scan["skip_reason"] == "NO_CANDIDATES"
    assert scan["measures_found"] == 4
    assert scan["duration_seconds"] == 252.0
    assert scan["proposal_count"] == 0


def test_list_space_mv_proposals_rerun_gate_skips_last_scan(client, monkeypatch):
    """The re-run gate query (approved_for_rerun=true) asks a different question
    ("what has this Agent had approved?") and never wants the last-scan framing,
    so the summary read is not issued on that path."""
    read = {"called": False}

    def _scan(*a, **k):
        read["called"] = True
        return None

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", _scan)
    resp = client.get(
        "/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true"
    )
    assert resp.status_code == 200
    assert resp.json()["last_scan"] is None
    assert read["called"] is False


def test_get_mv_ddl_surfaces_yaml_and_grant(client, monkeypatch):
    # Deployed-review fix: the card GRANT names the GSO service principal (the one
    # grant that matters functionally — the optimizer must read the view on a
    # create-and-attach run), not the broad space audience. Never a `<grantee>`.
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e",
    )
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "join_strategy": "subquery_source",
            "yaml_text": "version: 0.1\n",
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    data = resp.json()
    assert data["yaml_text"] == "version: 0.1\n"
    assert (
        "GRANT SELECT ON VIEW finance.sales.revenue_metrics TO "
        "`a803ebc5-232f-44c0-9ed6-fb17d7c77f9e`;" in data["grant_sql"]
    )
    assert "<grantee>" not in data["grant_sql"]
    # Exactly one GRANT statement now — the "why so many grants?" noise is gone.
    assert data["grant_sql"].count("GRANT SELECT") == 1


def test_get_mv_ddl_parses_source_tables_from_yaml(client, monkeypatch):
    """Deployed review #2/#3: the card's Source column is fed serve-time from the
    rendered YAML's ``source:`` (base + each join), so an existing candidate shows
    its sources with no re-scan and no extra column on the row."""
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "join_strategy": "denormalized",
            "yaml_text": (
                "version: '1.1'\n"
                "source: finance.sales.fact_orders\n"
                "joins:\n"
                "  - name: customer\n"
                "    source: `finance`.`sales`.`dim_customer`\n"
                "    on: fact_orders.customer_id = dim_customer.id\n"
            ),
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    assert resp.json()["source_tables"] == [
        "finance.sales.fact_orders",
        "finance.sales.dim_customer",
    ]


def test_get_mv_ddl_grant_says_so_in_words_when_no_sp_resolves(client, monkeypatch):
    """No resolvable optimizer SP → the GRANT says so in words rather than emitting
    a placeholder that cannot run (a commented instruction, no runnable statement
    with a fake principal)."""
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "yaml_text": "version: 0.1\n",
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    grant = resp.json()["grant_sql"]
    assert "<grantee>" not in grant
    assert "Could not resolve the optimizer service principal" in grant
    # Nothing executable with an invented principal — every SQL line is commented.
    assert not any(
        line.strip() and not line.strip().startswith("--") for line in grant.splitlines()
    )


def test_get_mv_ddl_404_when_absent(client, monkeypatch):
    # No artifact AND no candidate fallback (an advice run with nothing rendered):
    # the route still 404s. The fallback is stubbed to None so this pins the
    # empty case, not the warehouse read.
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda run_id, kind: None)
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_fallback", lambda run_id, suggestion_id: None
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 404


def test_get_mv_ddl_falls_back_to_candidate_yaml_text(client, monkeypatch):
    """MV-D23 / Prompt 15.1: with no run-partitioned artifact (an advice run),
    route 7 renders the DDL from the candidate row's yaml_text — best-wins on the
    wh_load_mv_candidates ordering — so a never-optimized space serves copy-ready
    DDL instead of 404ing. validation is None on this preview path (documented)."""
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda run_id, kind: None)
    # Deployed-review fix: the advice-run fallback GRANT also names the GSO SP.
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e",
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sugA", "dedup_fingerprint": "fpA",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "yaml_text": "version: 0.1\n",
            "evidence": {"join_strategy": "subquery_source"},
        }],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    data = resp.json()
    assert data["suggestion_id"] == "sugA"
    assert data["yaml_text"] == "version: 0.1\n"
    assert data["join_strategy"] == "subquery_source"
    assert "CREATE VIEW finance.sales.revenue_metrics" in data["ddl"]
    assert (
        "GRANT SELECT ON VIEW finance.sales.revenue_metrics TO "
        "`a803ebc5-232f-44c0-9ed6-fb17d7c77f9e`;" in data["grant_sql"]
    )
    assert data["validation"] is None


def test_decision_records_and_flips_rerun(client, monkeypatch):
    recorded: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": "fp1"}],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: recorded.append(kw),
    )
    resp = client.post(
        "/api/auto-optimize/mv/proposals/sug1/decision",
        json={"space_id": "space-1", "decision": "approved"},
        headers={"x-forwarded-email": "analyst@example.com"},
    )
    assert resp.status_code == 200
    assert resp.json()["approved_for_rerun"] is True
    assert recorded[0]["dedup_fingerprint"] == "fp1"
    assert recorded[0]["decided_by"] == "analyst@example.com"


def test_reject_fans_out_to_per_measure_suppression(client, monkeypatch):
    """MV-D30 as-implemented (Prompt 15.3): rejecting a view-grained bundle must
    suppress each member measure, not just the bundle key. The bundle's
    dedup_fingerprint is membership-sensitive, so recording the bundle decision
    alone lets a rejected measure resurface inside a differently-membered bundle.
    The router fans the rejection out to the per-measure suppression ledger,
    reading member fingerprints from the row's evidence.measures[]."""
    recorded: list[dict] = []
    suppressed: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "bundle1",
            "dedup_fingerprint": "bundle-fp",
            "evidence": {"measures": [
                {"dedup_fingerprint": "m-fp-1"},
                {"dedup_fingerprint": "m-fp-2"},
            ]},
        }],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: recorded.append(kw),
    )
    monkeypatch.setattr(
        warehouse, "wh_suppress_mv_measures",
        lambda ws, warehouse_id, **kw: suppressed.append(kw),
    )
    resp = client.post(
        "/api/auto-optimize/mv/proposals/bundle1/decision",
        json={"space_id": "space-1", "decision": "rejected"},
        headers={"x-forwarded-email": "analyst@example.com"},
    )
    assert resp.status_code == 200
    assert resp.json()["approved_for_rerun"] is False
    # The bundle-row decision is still recorded, keyed on the view-grained key.
    assert recorded[0]["dedup_fingerprint"] == "bundle-fp"
    # AND every member fingerprint is written to the suppression ledger,
    # tagged with the originating bundle so the fan-out is auditable.
    assert len(suppressed) == 1
    assert set(suppressed[0]["measure_fingerprints"]) == {"m-fp-1", "m-fp-2"}
    assert suppressed[0]["originating_suggestion_id"] == "bundle1"
    assert suppressed[0]["target_space_id"] == "space-1"


def test_approve_does_not_suppress_members(client, monkeypatch):
    """Approval stays bundle-grained: approving a bundle records the decision but
    never writes to the suppression ledger (per-measure partial approval is out
    of scope — MV-D30 future note)."""
    suppressed: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "bundle1",
            "dedup_fingerprint": "bundle-fp",
            "evidence": {"measures": [{"dedup_fingerprint": "m-fp-1"}]},
        }],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: None,
    )
    monkeypatch.setattr(
        warehouse, "wh_suppress_mv_measures",
        lambda ws, warehouse_id, **kw: suppressed.append(kw),
    )
    resp = client.post(
        "/api/auto-optimize/mv/proposals/bundle1/decision",
        json={"space_id": "space-1", "decision": "approved"},
    )
    assert resp.status_code == 200
    assert suppressed == []


def test_decision_404_when_suggestion_not_in_space(client, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    resp = client.post(
        "/api/auto-optimize/mv/proposals/sug1/decision",
        json={"space_id": "space-1", "decision": "rejected"},
    )
    assert resp.status_code == 404


def _created_row(status="DETACHED", created_by="analyst@example.com"):
    return {
        "run_id": "r1", "suggestion_id": "sug1",
        "full_name": "finance.sales.revenue_metrics",
        "created_by": created_by, "status": status,
    }


def _obo_as(email):
    ws = MagicMock()
    ws.current_user.me.return_value = SimpleNamespace(user_name=email)
    return ws


def test_drop_requires_confirm(client):
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": "r1", "confirm": False},
    )
    assert resp.status_code == 400


def test_drop_happy_path(client, monkeypatch):
    executed: list[str] = []
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("analyst@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: _created_row())
    monkeypatch.setattr(warehouse, "sql_warehouse_execute",
                        lambda ws, warehouse_id, sql: executed.append(sql))
    monkeypatch.setattr(warehouse, "wh_update_mv_created_object_status",
                        lambda *a, **k: None)
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": "r1", "confirm": True},
    )
    assert resp.status_code == 200
    assert resp.json()["dropped"] is True
    assert any("DROP VIEW IF EXISTS finance.sales.revenue_metrics" in s for s in executed)


def test_drop_forbidden_for_non_owner(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("someone-else@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: _created_row(created_by="owner@example.com"))
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": "r1", "confirm": True},
    )
    assert resp.status_code == 403


def test_drop_refuses_a_non_detached_object(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("analyst@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: _created_row(status="ATTACHED"))
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": "r1", "confirm": True},
    )
    assert resp.status_code == 409


# ── Created-object results read (Prompt 13 step 0) ─────────────────────────

_RUN_UUID = "22222222-2222-4222-8222-222222222222"

_LIFT = {
    "delta_affected": -0.07, "delta_suite": -0.03,
    "regressed_question_ids": ["bq_0007"], "needs_review_count": 3,
    "pre_eval_run_id": "eval_a1", "post_eval_run_id": "eval_b2",
    "question_subset": ["bq_0007", "bq_0019"],
    "pre_accuracy_affected": 0.78, "post_accuracy_affected": 0.71,
    "pre_accuracy_suite": 0.80, "post_accuracy_suite": 0.77,
    "needs_review_question_ids": ["bq_0022", "bq_0033", "bq_0041"],
    "graded_affected_count": 12, "graded_suite_count": 40,
}


def test_list_mv_created_returns_objects_with_lift(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_objects",
        lambda *a, **k: [{
            "run_id": "r1", "suggestion_id": "sug1",
            "full_name": "finance.sales.order_revenue",
            "created_by": "analyst@example.com", "status": "DETACHED",
            "baseline_eval_run_id": "eval_a1", "post_attach_eval_run_id": "eval_b2",
            "on_regression_action": "DETACH_ONLY_NEVER_DROP",
            "lift_report": _LIFT,
        }],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    data = resp.json()
    obj = data["created"][0]
    assert obj["full_name"] == "finance.sales.order_revenue"
    assert obj["status"] == "DETACHED"
    # The 14-key lift shape is mirrored verbatim, not reshaped.
    assert obj["lift_report"]["pre_accuracy_affected"] == 0.78
    assert obj["lift_report"]["post_accuracy_affected"] == 0.71
    assert obj["lift_report"]["needs_review_count"] == 3
    assert obj["lift_report"]["pre_eval_run_id"] == "eval_a1"
    assert data["downgrade_reason"] is None


def test_list_mv_created_returns_provenance(client, monkeypatch):
    # Prompt 14.1 (exposure-matrix GAP 1): route 10 surfaces provenance so a
    # reloaded UI can hide the Drop affordance on USER_CREATED views. A row with
    # no provenance column reads as the legacy OBO_CREATED (NULL convention).
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_objects",
        lambda *a, **k: [
            {
                "run_id": "r1", "suggestion_id": "byo1",
                "full_name": "finance.sales.net_revenue",
                "created_by": "prashanth@example.com", "status": "CREATED",
                "provenance": "USER_CREATED",
            },
            {
                "run_id": "r1", "suggestion_id": "obo1",
                "full_name": "finance.sales.order_revenue",
                "created_by": "analyst@example.com", "status": "ATTACHED",
            },
        ],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    by_id = {o["suggestion_id"]: o for o in resp.json()["created"]}
    assert by_id["byo1"]["provenance"] == "USER_CREATED"
    assert by_id["obo1"]["provenance"] == "OBO_CREATED"


def test_list_mv_created_surfaces_downgrade_reason(client, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_created_objects", lambda *a, **k: [])
    monkeypatch.setattr(
        warehouse, "wh_load_mv_consent_by_run",
        lambda *a, **k: {"run_id": "r1", "downgrade_reason": "grant revoked before trigger"},
    )
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    data = resp.json()
    assert data["created"] == []
    assert data["downgrade_reason"] == "grant revoked before trigger"


def test_list_mv_created_tolerates_a_missing_lift_report(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_objects",
        lambda *a, **k: [{
            "run_id": "r1", "suggestion_id": "sug1",
            "full_name": "finance.sales.order_revenue",
            "status": "ATTACHED",
        }],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    obj = resp.json()["created"][0]
    assert obj["status"] == "ATTACHED"
    assert obj["lift_report"] is None


def test_list_mv_created_returns_empty_on_read_failure(client, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("warehouse asleep")

    monkeypatch.setattr(warehouse, "wh_load_mv_created_objects", _boom)
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    assert resp.json() == {"run_id": _RUN_UUID, "created": [], "downgrade_reason": None}


# ── Trigger threading ──────────────────────────────────────────────────────


@pytest.fixture
def trigger_client(monkeypatch):
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: MagicMock())

    captured: dict = {}

    def _fake_trigger(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            run_id="run-xyz", job_run_id="42", job_url=None, status="IN_PROGRESS",
        )

    monkeypatch.setattr(auto_optimize, "trigger_optimization", _fake_trigger)
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app), captured


def test_trigger_threads_mv_params_and_builds_hook(trigger_client):
    client, captured = trigger_client
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={
            "space_id": "space-1",
            "enable_metric_view_suggestions": True,
            "mv_action_mode": "create_and_attach",
            "mv_min_confidence": 80,
            "mv_approved_suggestion_ids": ["sug1"],
            "mv_consent": {
                "granted_by": "analyst@example.com",
                "granted_at": "2026-08-24T00:00:00+00:00",
                "probe_id": "p1",
            },
        },
    )
    assert resp.status_code == 200
    assert captured["enable_metric_view_suggestions"] is True
    assert captured["mv_action_mode"] == "create_and_attach"
    assert captured["mv_min_confidence"] == 80
    assert callable(captured["mv_attach_hook"])


def test_trigger_omits_hook_when_suggest_only(trigger_client):
    client, captured = trigger_client
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={"space_id": "space-1", "enable_metric_view_suggestions": True},
    )
    assert resp.status_code == 200
    assert captured["mv_attach_hook"] is None
    assert captured["mv_action_mode"] == "suggest_only"
