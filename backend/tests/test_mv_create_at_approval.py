"""Create-at-approval (MV-D34) + the MV-D35 served fields — Prompt 15.8.

Tested at the seam (no Databricks). What matters:

- **The acceptance journey ends in a real view (MV-D34):** a SUFFICIENT fresh
  probe → OBO ``CREATE`` through the SAME ``mv_create`` seam → an ``OBO_CREATED``
  ledger row on a sentinel advice run (the BYO-register rails), so
  attach-on-next-run picks it up. Never the SP, never a fork.
- **Never a dead end (MV-D34.c):** an INSUFFICIENT fresh probe (or a create-time
  degrade) returns ``degraded`` with the remediation GRANT — nothing created —
  so the card can fall back to [Approve for later].
- **The MV-D22 guards travel:** a revalidation failure / rung-below / collision
  refuses the create with a reason, never installs the wrong artifact.
- **The facts row (MV-D35) is gated on real proof:** ``_mv_checks_from_row``
  emits a check ONLY when the row proves its gate ran.
- **The GRANT grantee is ACL-derived (fix #3):** ``_space_audience_grantees``
  returns the space's CAN RUN/VIEW/MANAGE principals, deduped.
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


# ── Service: create_at_approval ─────────────────────────────────────────────


def _verification(effective_mode="create_and_attach", downgrade_reason=None, verdict="SUFFICIENT"):
    fresh = SimpleNamespace(
        capabilities=[],
        checked_as="analyst@example.com",
        remediation_sql="GRANT ALL PRIVILEGES ON SCHEMA finance.sales TO `analyst@example.com`",
    )
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
def approval_env(monkeypatch):
    executed: list[str] = []
    upserts: list[dict] = []
    advice_runs: list[dict] = []

    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(
        mv_create, "verify_consent", lambda **kw: (_verification(), dict(_CONSENT))
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "proposed_object": "warehouse.raw.revenue_metrics",
        }],
    )
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: dict(_ARTIFACT))
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: False)
    monkeypatch.setattr(mv_create, "_confirm_metric_view", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: executed.append(sql),
    )
    monkeypatch.setattr(
        warehouse, "wh_ensure_optimization_tables",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        warehouse, "wh_create_advice_run",
        lambda ws, warehouse_id, **kw: advice_runs.append(kw),
    )
    monkeypatch.setattr(
        warehouse, "wh_upsert_mv_created_object",
        lambda ws, warehouse_id, **kw: (upserts.append(kw) or kw["full_name"]),
    )
    # MV-D34 attach-at-approval: default the attach to success so the happy path
    # exercises the create-and-attach outcome. Tests that want the created-not-
    # attached seam override this with a False stub.
    monkeypatch.setattr(
        mv_create, "_attach_metric_view_to_space", lambda *a, **k: True
    )
    return executed, upserts, advice_runs


def _create():
    return mv_create.create_at_approval(
        space_id="space-1", suggestion_id="sug1", probe_id="p1",
        catalog="main", schema="gso", warehouse_id="wh1",
    )


def test_happy_path_creates_and_attaches_at_the_consented_name(approval_env):
    executed, upserts, advice_runs = approval_env

    result = _create()

    assert result.created is True
    assert result.degraded is False
    # MV-D34 attach-at-approval: the create ALSO shelved the view on the Agent, so
    # the result reports attached and the ledger row records ATTACHED.
    assert result.attached is True
    # Re-targeted to the CONSENTED catalog/schema, base name preserved.
    assert result.full_name == "finance.sales.revenue_metrics"
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)
    assert len(advice_runs) == 1 and advice_runs[0]["space_id"] == "space-1"
    assert upserts and upserts[0]["status"] == "ATTACHED"
    assert upserts[0]["provenance"] == mv_create.MV_PROVENANCE_OBO_CREATED
    assert upserts[0]["created_by"] == "analyst@example.com"
    assert result.run_id == advice_runs[0]["run_id"]


def test_attach_failure_records_created_not_attached(approval_env, monkeypatch):
    """The config PATCH failing (e.g. no CAN EDIT) must not fail the create: the
    UC view still exists, so the result is created-not-attached and the ledger row
    records CREATED, never a silent success that claims a config change."""
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(mv_create, "_attach_metric_view_to_space", lambda *a, **k: False)

    result = _create()

    assert result.created is True
    assert result.attached is False
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"


def test_attach_helper_is_idempotent_and_never_raises(monkeypatch):
    """``_attach_metric_view_to_space`` shelves the identifier once (under
    ``tables``), is a no-op when it is already present under EITHER data-source
    list, and returns False (never raises) on any error."""
    from types import SimpleNamespace

    # A legacy config: one base table + one view left in the metric_views bucket.
    space = {
        "data_sources": {
            "tables": [{"identifier": "a.b.base_table"}],
            "metric_views": [{"identifier": "a.b.legacy_mv"}],
        }
    }
    patched: list = []

    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        lambda ws, sid: {"_parsed_space": space},
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        lambda ws, sid, cfg: patched.append(cfg),
    )

    # New identifier → appended to TABLES (matching Genie's collapse) + PATCH.
    assert mv_create._attach_metric_view_to_space(
        SimpleNamespace(), space_id="s1", full_name="a.b.new_view"
    ) is True
    table_idents = {t["identifier"] for t in space["data_sources"]["tables"]}
    assert table_idents == {"a.b.base_table", "a.b.new_view"}
    # metric_views bucket is left untouched (we never write to it).
    assert [m["identifier"] for m in space["data_sources"]["metric_views"]] == [
        "a.b.legacy_mv"
    ]
    assert len(patched) == 1

    # Already present under tables → no-op, no second PATCH.
    assert mv_create._attach_metric_view_to_space(
        SimpleNamespace(), space_id="s1", full_name="A.B.NEW_VIEW"
    ) is True
    assert len(patched) == 1

    # Present under the LEGACY metric_views bucket → also a no-op (cross-bucket).
    assert mv_create._attach_metric_view_to_space(
        SimpleNamespace(), space_id="s1", full_name="A.B.LEGACY_MV"
    ) is True
    assert len(patched) == 1

    # A read failure returns False rather than raising.
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        lambda ws, sid: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert mv_create._attach_metric_view_to_space(
        SimpleNamespace(), space_id="s1", full_name="a.b.z"
    ) is False


def test_attach_writes_into_tables_not_metric_views(monkeypatch):
    """Genie serialized_space v2 collapses ``metric_views[]`` into ``tables[]`` on
    write (confirmed by round-trip; see playbook round 10), so the attach shelves
    the view directly under ``data_sources.tables`` and never creates a
    ``metric_views`` bucket. Pins the write target so a well-meaning revert to
    ``metric_views`` — which reads back empty and re-offers the view — fails."""
    from types import SimpleNamespace

    space: dict = {"data_sources": {}}
    patched: list = []
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        lambda ws, sid: {"_parsed_space": space},
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        lambda ws, sid, cfg: patched.append(cfg),
    )

    assert mv_create._attach_metric_view_to_space(
        SimpleNamespace(), space_id="s1", full_name="cat.gold.rev_metrics"
    ) is True
    assert [t["identifier"] for t in space["data_sources"]["tables"]] == [
        "cat.gold.rev_metrics"
    ]
    assert "metric_views" not in space["data_sources"]
    assert len(patched) == 1


def test_candidate_yaml_text_is_the_fallback_when_no_artifact(approval_env, monkeypatch):
    executed, upserts, _ = approval_env
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "evidence": {"join_strategy": "nested"},
        }],
    )
    result = _create()
    assert result.created is True
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)


def test_insufficient_fresh_probe_degrades_with_remediation_and_creates_nothing(approval_env, monkeypatch):
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (
            _verification(effective_mode="suggest_only",
                          downgrade_reason="grant revoked", verdict="INSUFFICIENT"),
            dict(_CONSENT),
        ),
    )
    result = _create()
    assert result.created is False
    assert result.degraded is True
    assert result.verdict == "INSUFFICIENT"
    assert result.remediation_sql and "GRANT" in result.remediation_sql
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []


def test_no_consent_degrades(approval_env, monkeypatch):
    monkeypatch.setattr(mv_create, "verify_consent", lambda **kw: (None, None))
    result = _create()
    assert result.created is False
    assert result.degraded is True
    assert "consent" in (result.reason or "")


def test_revalidation_failure_refuses_and_creates_nothing(approval_env, monkeypatch):
    executed, upserts, _ = approval_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=False, errors=("bad yaml",)),
    )
    result = _create()
    assert result.created is False
    assert result.degraded is False
    assert "re-validation" in (result.reason or "")
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


def test_rung_below_refuses(approval_env, monkeypatch):
    executed, _, _ = approval_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to="subquery_source"),
    )
    result = _create()
    assert result.created is False
    assert not any("CREATE VIEW" in s for s in executed)


def test_existing_metric_view_is_attached_not_clobbered(approval_env, monkeypatch):
    """MV-D34 idempotent re-approval: a view that ALREADY exists as a metric view
    (a prior round, or a create-not-attached left by a failed PATCH) is no longer
    a "refusing to clobber" dead end. Approving skips the CREATE and (re)attaches
    it — the config is the source of truth, so attaching is what clears it from
    the list. The ledger records ATTACHED and the result flags already_existed."""
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    # _confirm_metric_view defaults True in the fixture → it IS a metric view.

    result = _create()

    assert result.created is True
    assert result.attached is True
    assert result.already_existed is True
    assert result.full_name == "finance.sales.revenue_metrics"
    # No CREATE VIEW: the existing object is reused, never clobbered.
    assert not any("CREATE VIEW" in s for s in executed)
    assert not any("DROP VIEW" in s for s in executed)
    assert upserts and upserts[0]["status"] == "ATTACHED"
    assert len(advice_runs) == 1


def test_existing_non_metric_object_is_refused(approval_env, monkeypatch):
    """A same-named object that is NOT a metric view is a genuine collision: the
    create is refused with a reason and nothing is created, attached, or dropped
    (it is not ours to touch)."""
    executed, upserts, _ = approval_env
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    monkeypatch.setattr(mv_create, "_confirm_metric_view", lambda *a, **k: False)

    result = _create()

    assert result.created is False
    assert "not a metric view" in (result.reason or "")
    assert not any("CREATE VIEW" in s for s in executed)
    assert not any("DROP VIEW" in s for s in executed)
    assert upserts == []


def test_missing_candidate_returns_a_reason(approval_env, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    result = _create()
    assert result.created is False
    assert result.degraded is False
    assert "no proposal" in (result.reason or "")


# ── The facts row (MV-D35): _mv_checks_from_row is gated on real proof ─────


def test_checks_all_pass_for_a_servable_non_overlapping_row():
    checks = auto_optimize._mv_checks_from_row(
        {"proposed_object": "finance.sales.revenue_metrics", "conflicts": []}
    )
    assert checks == {"validated": "PASS", "executable": "PASS", "no_overlap": "PASS"}


def test_checks_omit_no_overlap_when_conflicts_present():
    checks = auto_optimize._mv_checks_from_row(
        {"proposed_object": "finance.sales.revenue_metrics", "conflicts": [{"x": 1}]}
    )
    # Validated/executable still prove out; no_overlap is NOT claimed.
    assert checks == {"validated": "PASS", "executable": "PASS"}


def test_checks_none_for_a_blank_row_never_lies():
    # A blank identifier never claims validated/executable (no servable body).
    # It still (harmlessly) reports the dedup gate finding no overlap — the row
    # is dropped before surfacing regardless. But a blank row that ALSO carries a
    # conflict claims NOTHING at all: no key is invented, so checks is None.
    assert auto_optimize._mv_checks_from_row({"proposed_object": "  ", "conflicts": []}) == {
        "no_overlap": "PASS"
    }
    assert auto_optimize._mv_checks_from_row(
        {"proposed_object": None, "conflicts": [{"overlap": "x"}]}
    ) is None


# ── The masking-bug root cause (Prompt 15.9, item a): honest bool coercion ───
#
# Warehouse rows arrive stringified (Statement Execution JSON_ARRAY), so a raw
# ``bool("false")`` is True for EVERY row — which forced ``approved_for_rerun``
# true and opened the accept flow on its "approved" terminal, hiding
# [Create this metric view] (MV-D34 shipped invisible). The mapping must coerce
# the stringified boolean honestly via ``_safe_bool``.


def test_proposal_mapping_coerces_stringified_false_boolean():
    """A row whose ``approved_for_rerun`` arrives as the STRING "false" (the
    warehouse's on-the-wire shape) must map to Python ``False`` — the pin on the
    masking bug. The sibling ``tier_capped_by_coverage`` gets the same coercion,
    while a true row and a NULL row still read true / None."""
    unacted = auto_optimize._mv_proposal_from_row(
        {
            "suggestion_id": "s1",
            "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "candidate_type": "NEW_METRIC_VIEW",
            "approved_for_rerun": "false",
            "tier_capped_by_coverage": "false",
        }
    )
    assert unacted.approved_for_rerun is False
    assert unacted.tier_capped_by_coverage is False

    approved = auto_optimize._mv_proposal_from_row(
        {
            "suggestion_id": "s2",
            "dedup_fingerprint": "fp2",
            "target_space_id": "space-1",
            "candidate_type": "NEW_METRIC_VIEW",
            "approved_for_rerun": "true",
            "tier_capped_by_coverage": None,
        }
    )
    assert approved.approved_for_rerun is True
    # A legacy NULL stays None (the panel falls back to the tier-only split).
    assert approved.tier_capped_by_coverage is None


# ── The attached marker (MV-D34): _mv_mark_attached reads the config ─────────


def _proposal(obj: str):
    return auto_optimize._mv_proposal_from_row(
        {
            "suggestion_id": "s-" + obj,
            "dedup_fingerprint": "fp-" + obj,
            "target_space_id": "space-1",
            "candidate_type": "NEW_METRIC_VIEW",
            "proposed_object": obj,
        }
    )


def test_mark_attached_flags_only_proposals_on_the_config():
    """A proposal whose proposed_object is on ``data_sources.metric_views`` (via
    the ``_metric_views`` identifiers fetch_space_config extracts) is marked
    attached, case-insensitively; others are left false. The config is the source
    of truth, so no ledger cross-reference is needed."""
    proposals = [
        _proposal("finance.sales.revenue_metrics"),
        _proposal("finance.sales.other_view"),
    ]
    space_config = {"_metric_views": ["FINANCE.SALES.REVENUE_METRICS"]}
    auto_optimize._mv_mark_attached(proposals, space_config)
    assert proposals[0].attached is True
    assert proposals[1].attached is False


def test_mark_attached_is_a_no_op_without_a_usable_config():
    proposals = [_proposal("finance.sales.revenue_metrics")]
    auto_optimize._mv_mark_attached(proposals, None)
    assert proposals[0].attached is False
    auto_optimize._mv_mark_attached(proposals, {"_metric_views": []})
    assert proposals[0].attached is False


def test_mark_attached_normalizes_backticks():
    """Genie can export identifiers backticked (`` `cat`.`sch`.`view` ``); the
    warehouse ``proposed_object`` is bare. Both sides are backtick-stripped so a
    truly-attached view is recognized and stops being re-offered."""
    proposals = [_proposal("finance.sales.revenue_metrics")]
    space_config = {"_metric_views": ["`finance`.`sales`.`revenue_metrics`"]}
    auto_optimize._mv_mark_attached(proposals, space_config)
    assert proposals[0].attached is True


def test_mark_attached_matches_a_metric_view_filed_under_tables():
    """Field reality (deployed): a Genie space files an added metric view under
    ``data_sources.tables`` (``_tables``), leaving ``_metric_views`` empty. The
    marker matches BOTH data-source lists, so such a view is recognized as present
    and drops out of the suggestions instead of being re-offered as "create"."""
    proposals = [
        _proposal("cat.gold.fact_booking_daily_metrics"),  # present as a table
        _proposal("cat.gold.dim_property_metrics"),        # not present anywhere
    ]
    space_config = {
        "_metric_views": [],
        "_tables": [
            "cat.gold.fact_booking_daily",             # base table — must NOT match
            "cat.gold.fact_booking_daily_metrics",     # the MV, filed under tables
        ],
    }
    auto_optimize._mv_mark_attached(proposals, space_config)
    assert proposals[0].attached is True
    assert proposals[1].attached is False


# ── ACL-derived grantees (fix #3): _space_audience_grantees ──────────────────


def _acl_ws(acl: dict):
    ws = MagicMock()
    ws.api_client.do.return_value = acl
    return ws


def test_grantees_are_the_audience_principals_deduped(monkeypatch):
    acl = {
        "access_control_list": [
            {"user_name": "a@x.com", "all_permissions": [{"permission_level": "CAN_RUN"}]},
            {"group_name": "analysts", "all_permissions": [{"permission_level": "CAN_VIEW"}]},
            {"user_name": "owner@x.com", "all_permissions": [{"permission_level": "CAN_MANAGE"}]},
            # A principal with no audience-level permission is excluded.
            {"user_name": "nobody@x.com", "all_permissions": [{"permission_level": "SOMETHING_ELSE"}]},
            # A duplicate principal is not repeated.
            {"user_name": "a@x.com", "all_permissions": [{"permission_level": "CAN_VIEW"}]},
        ]
    }
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: _acl_ws(acl))
    grantees = auto_optimize._space_audience_grantees("space-1")
    assert grantees == ["a@x.com", "analysts", "owner@x.com"]


def test_grantees_empty_when_acl_unreadable(monkeypatch):
    ws = MagicMock()
    ws.api_client.do.side_effect = RuntimeError("403")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: ws)
    assert auto_optimize._space_audience_grantees("space-1") == []


# ── Route: POST /spaces/{space_id}/mv/create ────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(
        auto_optimize, "require_obo_workspace_client",
        lambda: SimpleNamespace(current_user=MagicMock()),
    )
    # #2 post-create link: the route resolves the workspace URL from the client
    # config so the created terminal can deep-link Catalog Explorer. Stub a clean
    # host so the resolved value is deterministic (not a MagicMock coerced to str).
    monkeypatch.setattr(
        auto_optimize, "get_workspace_client",
        lambda: SimpleNamespace(
            config=SimpleNamespace(host="https://example.databricks.com")
        ),
    )
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_create_route_returns_created_attached_and_grant(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=True, attached=True, full_name="finance.sales.revenue_metrics",
            run_id="run-obo-1", suggestion_id="sug1", verdict="SUFFICIENT",
        ),
    )
    # A resolvable SP id so the route emits an executable GRANT (not the worded
    # fallback), proving grant_sql rides the create response now (MV-D34).
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "abcdef01-2345-6789-abcd-ef0123456789",
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] is True
    assert body["attached"] is True
    assert body["full_name"] == "finance.sales.revenue_metrics"
    assert body["provenance"] == "OBO_CREATED"
    assert body["run_id"] == "run-obo-1"
    assert body["already_existed"] is False
    assert body["grant_sql"] and "GRANT SELECT ON VIEW finance.sales.revenue_metrics" in body["grant_sql"]
    # #2: the workspace host rides the create response so the terminal can link
    # the new view in Catalog Explorer without threading a host prop down.
    assert body["workspace_host"] == "https://example.databricks.com"


def test_create_route_reports_already_existed_and_still_grants(client, monkeypatch):
    """Idempotent re-approval rides the same response shape: created + attached
    with already_existed true, and grant_sql still resolves so the SP SELECT the
    optimizer needs is one copy away even when the view was made in a prior round."""
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=True, attached=True, already_existed=True,
            full_name="finance.sales.revenue_metrics",
            run_id="run-obo-2", suggestion_id="sug1", verdict="SUFFICIENT",
        ),
    )
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "abcdef01-2345-6789-abcd-ef0123456789",
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] is True
    assert body["attached"] is True
    assert body["already_existed"] is True
    assert body["grant_sql"] and "GRANT SELECT ON VIEW finance.sales.revenue_metrics" in body["grant_sql"]


def test_create_route_returns_degraded(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=False, degraded=True, suggestion_id="sug1",
            verdict="INSUFFICIENT", remediation_sql="GRANT ...",
            reason="not sufficient",
        ),
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] is False
    assert body["degraded"] is True
    assert body["remediation_sql"] == "GRANT ..."


def test_create_route_requires_obo(client, monkeypatch):
    def _no_obo():
        raise RuntimeError("This operation requires user authorization")

    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", _no_obo)
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 401
