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

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import mv_create
from genie_space_optimizer.common import warehouse
from genie_space_optimizer.common.config import MV_RENDER_VERSION
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


_CONSENT = {
    "target_catalog": "finance", "target_schema": "sales", "probe_id": "p1",
    "probe_results": {"privileges": [{"privilege": "SELECT", "securable": "finance.sales.orders"}]},
}
_ARTIFACT = {
    "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
    "join_strategy": "direct",
    "proposed_object": "warehouse.raw.revenue_metrics",
    "render_version": MV_RENDER_VERSION,
}


_OBO_WS = MagicMock(name="obo_ws")
_SP_WS = MagicMock(name="sp_ws")


class _Calls(list):
    """Captured calls, with ``on`` holding the workspace client each ran on."""

    def __init__(self):
        super().__init__()
        self.on: list = []


@pytest.fixture
def approval_env(monkeypatch):
    executed = _Calls()
    upserts = _Calls()
    advice_runs: list[dict] = []

    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: _SP_WS)
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: _OBO_WS)
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
        lambda ws, warehouse_id, sql: (executed.on.append(ws), executed.append(sql)),
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
        lambda ws, warehouse_id, **kw: (upserts.on.append(ws), upserts.append(kw))
        and kw["full_name"],
    )
    # MV-D34 attach-at-approval: default the attach to success so the happy path
    # exercises the create-and-attach outcome. Tests that want the created-not-
    # attached seam override this with a False stub.
    monkeypatch.setattr(
        mv_create, "_attach_metric_view_to_space", lambda *a, **k: True
    )
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda *a, **k: (True, True, "analyst@example.com", None),
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
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
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
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
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
            "evidence": {"join_strategy": "direct", "render_version": MV_RENDER_VERSION},
        }],
    )
    result = _create()
    assert result.created is True
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)


def test_approval_refuses_an_unstamped_body_with_rescan_reason(approval_env, monkeypatch):
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
            "join_strategy": "direct",
            "proposed_object": "warehouse.raw.revenue_metrics",
        },
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
            "evidence": {"join_strategy": "nested", "render_version": 0},
        }],
    )

    result = _create()

    assert result.created is False
    assert result.degraded is False
    assert result.reason == mv_create.STALE_BODY_REASON
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []


def test_approval_refuses_a_stamped_row_whose_body_the_in_job_writer_cleared(
    approval_env, monkeypatch,
):
    """The in-job writer overwrites ``yaml_text`` with None when its render did
    not produce a body, so a stamp never re-arms an older body."""
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "yaml_text": None,
            "evidence": {"join_strategy": "direct", "render_version": MV_RENDER_VERSION},
        }],
    )

    result = _create()

    assert result.created is False
    assert result.reason == mv_create._NO_BODY_REASON
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []


def test_approval_uses_a_stamped_candidate_when_the_artifact_is_stale(
    approval_env, monkeypatch,
):
    executed, upserts, _ = approval_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            "yaml_text": "version: 0.1\nsource: stale.artifact.body\n",
            "join_strategy": "direct",
            "proposed_object": "warehouse.raw.revenue_metrics",
        },
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n# candidate\n",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "evidence": {"join_strategy": "direct", "render_version": MV_RENDER_VERSION},
        }],
    )
    seen_bodies: list[str] = []

    def validate(text, **kw):
        seen_bodies.append(text)
        return mv_yaml.ValidationReport(ok=True, downgrade_to=None)

    monkeypatch.setattr(mv_yaml, "validate", validate)

    result = _create()

    assert result.created is True
    assert any("# candidate" in t for t in seen_bodies)
    assert not any("stale.artifact.body" in t for t in seen_bodies)
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
    assert upserts and upserts[0]["status"] == "ATTACHED"


def test_approval_refuses_a_non_plain_target_name(approval_env, monkeypatch):
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            **_ARTIFACT,
            "proposed_object": "main.sales.revenue metrics",
        },
    )

    result = _create()

    assert result.created is False
    assert result.degraded is False
    assert "not a plain Unity Catalog name" in (result.reason or "")
    assert not any("DESCRIBE" in s or "CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []


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


@pytest.mark.parametrize("strategy", ["nested", "subquery_source", "denormalized"])
def test_a_body_needing_an_unproven_join_is_not_created(approval_env, monkeypatch, strategy):
    """MV-D117 (C-8): only a ``direct`` body is created until the join rungs are
    proven in Unity Catalog."""
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: dict(_ARTIFACT, join_strategy=strategy),
    )

    result = _create()

    assert result.created is False
    assert result.degraded is False
    assert result.reason == mv_create.UNPROVEN_RUNG_REASON
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []


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
    assert upserts[0]["provenance"] == mv_create.MV_PROVENANCE_OBO_CREATED
    assert result.provenance == mv_create.MV_PROVENANCE_OBO_CREATED


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


def test_a_squatted_name_with_a_different_definition_is_refused(approval_env, monkeypatch):
    """MV-D112: a metric view already at the consented name that is not this
    proposal is refused. Nothing is created, attached, recorded or dropped."""
    executed, upserts, advice_runs = approval_env
    attaches: list[dict] = []
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda *a, **k: (False, False, "", "finance.sales.revenue_metrics already exists with a different definition than this proposal; refusing to attach it."),
    )
    monkeypatch.setattr(
        mv_create, "_attach_metric_view_to_space",
        lambda *a, **k: (attaches.append(k) or True),
    )

    result = _create()

    assert result.created is False
    assert result.degraded is False
    assert "different definition" in (result.reason or "")
    assert not any("CREATE VIEW" in s or "DROP VIEW" in s for s in executed)
    assert attaches == []
    assert upserts == []
    assert advice_runs == []


def test_a_matching_view_the_caller_does_not_own_is_attached_as_user_created(approval_env, monkeypatch):
    executed, upserts, _ = approval_env
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda *a, **k: (True, False, "other@example.com", None),
    )

    result = _create()

    assert result.created is True
    assert result.already_existed is True
    assert result.provenance == mv_create.MV_PROVENANCE_USER_CREATED
    assert result.owner == "other@example.com"
    assert upserts[0]["provenance"] == mv_create.MV_PROVENANCE_USER_CREATED
    assert upserts.on == [_SP_WS]
    assert not any("CREATE VIEW" in s for s in executed)


def test_the_existing_view_is_checked_against_the_replayed_body_as_the_caller(approval_env, monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda ws, warehouse_id, **k: (
            calls.append({"ws": ws, **k}) or (True, True, "analyst@example.com", None)
        ),
    )

    _create()

    assert calls == [{
        "ws": _OBO_WS,
        "full_name": "finance.sales.revenue_metrics",
        "yaml_text": _ARTIFACT["yaml_text"],
        "caller": "analyst@example.com",
    }]


def test_a_fresh_create_does_not_run_the_existing_view_check(approval_env, monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda *a, **k: (calls.append(k) or (True, True, "analyst@example.com", None)),
    )

    executed, upserts, _ = approval_env

    result = _create()

    assert result.created is True and result.already_existed is False
    assert calls == []
    assert [ws for ws, sql in zip(executed.on, executed) if "CREATE VIEW" in sql] == [_OBO_WS]
    assert upserts.on == [_SP_WS]


# ── MV-D120: a CREATE that failed, and a record that could not be written ───


def _create_raises(monkeypatch, executed):
    def _execute(ws, warehouse_id, sql):
        executed.on.append(ws)
        executed.append(sql)
        if "CREATE VIEW" in sql:
            raise RuntimeError("zq_secret")

    monkeypatch.setattr(warehouse, "sql_warehouse_execute", _execute)


def _stub_adopt(monkeypatch, outcome):
    calls: list[dict] = []

    def _adopt(ws, warehouse_id, **kw):
        calls.append({"ws": ws, **kw})
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(mv_create, "_adopt_existing_view", _adopt)
    return calls


def _assert_no_secret(caplog):
    for record in caplog.records:
        assert "zq_secret" not in record.getMessage()
        assert record.exc_info is None


@pytest.mark.parametrize(
    "existing, provenance, owner",
    [
        (
            mv_create.ExistingView(True, True, "analyst@example.com", None),
            mv_create.MV_PROVENANCE_OBO_CREATED, "analyst@example.com",
        ),
        (
            mv_create.ExistingView(True, False, "other@example.com", None),
            mv_create.MV_PROVENANCE_USER_CREATED, "other@example.com",
        ),
    ],
    ids=["owned", "someone_elses"],
)
def test_a_failed_create_whose_view_exists_is_attached_at_approval(
    approval_env, monkeypatch, caplog, existing, provenance, owner
):
    caplog.set_level("DEBUG")
    executed, upserts, advice_runs = approval_env
    _create_raises(monkeypatch, executed)
    lookups = _stub_adopt(monkeypatch, existing)

    result = _create()

    assert result.created is True
    assert result.attached is True
    assert result.already_existed is True
    assert result.provenance == provenance
    assert result.owner == owner
    assert lookups == [{
        "ws": _OBO_WS, "full_name": "finance.sales.revenue_metrics",
        "yaml_text": _ARTIFACT["yaml_text"], "caller": "analyst@example.com",
    }]
    assert upserts[0]["provenance"] == provenance
    assert upserts.on == [_SP_WS]
    assert len(advice_runs) == 1
    assert not any("DROP VIEW" in s for s in executed)
    assert any("RuntimeError" in r.getMessage() for r in caplog.records)
    _assert_no_secret(caplog)


def _refused_after_a_failed_create(approval_env, monkeypatch, caplog, lookup):
    caplog.set_level("DEBUG")
    executed, upserts, advice_runs = approval_env
    _create_raises(monkeypatch, executed)
    _stub_adopt(monkeypatch, lookup)
    attaches: list[dict] = []
    monkeypatch.setattr(
        mv_create, "_attach_metric_view_to_space",
        lambda *a, **k: (attaches.append(k) or True),
    )

    result = _create()

    assert result.created is False
    assert result.degraded is False
    assert attaches == []
    assert upserts == [] and advice_runs == []
    assert not any("DROP VIEW" in s for s in executed)
    assert "zq_secret" not in repr(result)
    _assert_no_secret(caplog)
    return result


@pytest.mark.parametrize(
    "lookup",
    [
        mv_create.ExistingView(
            False, False, "", "finance.sales.revenue_metrics was not found", exists=False,
        ),
        RuntimeError("zq_secret"),
    ],
    ids=["absent", "lookup_raises"],
)
def test_a_failed_create_whose_view_is_not_found_returns_both_causes(
    approval_env, monkeypatch, caplog, lookup
):
    """MV-D120: not found covers a slow warehouse and a create that failed, and
    an unknown lookup reads as not found; the reason promises no attach."""
    result = _refused_after_a_failed_create(approval_env, monkeypatch, caplog, lookup)

    assert result.reason == (
        "The create of finance.sales.revenue_metrics didn't complete and the view "
        "wasn't found. If the warehouse was slow, approving again will attach it; "
        "if this repeats, approve it for the next run instead."
    )


@pytest.mark.parametrize(
    "reason",
    [
        "finance.sales.revenue_metrics already exists and is not a metric view; refusing to clobber it",
        "finance.sales.revenue_metrics already exists with a different definition",
    ],
    ids=["not_a_metric_view", "different"],
)
def test_a_failed_create_whose_view_exists_but_is_refused_returns_its_reason(
    approval_env, monkeypatch, caplog, reason
):
    """MV-D120: a view that is there but isn't this proposal says why, rather than
    asking for an approval that would refuse it again."""
    lookup = mv_create.ExistingView(False, False, "", reason)

    result = _refused_after_a_failed_create(approval_env, monkeypatch, caplog, lookup)

    assert result.reason == reason


@pytest.mark.parametrize(
    "failing", ["wh_ensure_optimization_tables", "wh_create_advice_run", "wh_upsert_mv_created_object"]
)
@pytest.mark.parametrize("attached", [True, False], ids=["attached", "not_attached"])
def test_a_ledger_failure_after_create_is_a_reason_and_keeps_the_view(
    approval_env, monkeypatch, caplog, failing, attached
):
    caplog.set_level("DEBUG")
    executed, _, _ = approval_env

    def _raise(*a, **k):
        raise RuntimeError("zq_secret")

    monkeypatch.setattr(warehouse, failing, _raise)
    monkeypatch.setattr(mv_create, "_attach_metric_view_to_space", lambda *a, **k: attached)

    result = _create()

    assert result.created is False
    assert result.degraded is False
    done = "created and attached" if attached else "created"
    assert result.reason == (
        f"finance.sales.revenue_metrics was {done} but couldn't be recorded. "
        "Approve again to record it."
    )
    assert any("CREATE VIEW" in s for s in executed)
    assert not any("DROP VIEW" in s for s in executed)
    assert "zq_secret" not in repr(result)
    assert any(
        "could not record it" in r.getMessage() and "RuntimeError" in r.getMessage()
        for r in caplog.records
    )
    _assert_no_secret(caplog)


@pytest.mark.parametrize("attached", [True, False], ids=["attached", "not_attached"])
def test_a_ledger_failure_after_finding_the_view_says_found(
    approval_env, monkeypatch, caplog, attached
):
    """MV-D120: a view this call found, not created, is reported as found."""
    caplog.set_level("DEBUG")
    executed, _, _ = approval_env
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)

    def _raise(*a, **k):
        raise RuntimeError("zq_secret")

    monkeypatch.setattr(warehouse, "wh_upsert_mv_created_object", _raise)
    monkeypatch.setattr(mv_create, "_attach_metric_view_to_space", lambda *a, **k: attached)

    result = _create()

    assert result.created is False
    done = "found and attached" if attached else "found"
    assert result.reason == (
        f"finance.sales.revenue_metrics was {done} but couldn't be recorded. "
        "Approve again to record it."
    )
    assert not any(s.startswith(("CREATE VIEW", "DROP VIEW")) for s in executed)
    assert any(
        "could not record it" in r.getMessage() and "RuntimeError" in r.getMessage()
        for r in caplog.records
    )
    _assert_no_secret(caplog)


def test_a_fresh_create_has_no_owner(approval_env):
    result = _create()

    assert result.created is True and result.already_existed is False
    assert result.owner is None


def test_missing_candidate_returns_a_reason(approval_env, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    result = _create()
    assert result.created is False
    assert result.degraded is False
    assert "no proposal" in (result.reason or "")


# ── The facts row (MV-D35): _mv_checks_from_row is gated on real proof ─────


_CURRENT_EVIDENCE = {"render_version": MV_RENDER_VERSION}


def test_checks_all_pass_for_a_servable_non_overlapping_row():
    checks = auto_optimize._mv_checks_from_row(
        {
            "proposed_object": "finance.sales.revenue_metrics", "conflicts": [],
            "evidence": _CURRENT_EVIDENCE,
        }
    )
    assert checks == {"validated": "PASS", "executable": "PASS", "no_overlap": "PASS"}


def test_checks_omit_no_overlap_when_conflicts_present():
    checks = auto_optimize._mv_checks_from_row(
        {
            "proposed_object": "finance.sales.revenue_metrics", "conflicts": [{"x": 1}],
            "evidence": _CURRENT_EVIDENCE,
        }
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


# ── Existing-view definition check (MV-D112) ────────────────────────────────

# What the app sends: the MV-D22 replay body.
_PROPOSAL_YAML = (
    'version: "1.1"\n'
    "comment: |\n"
    "  PURPOSE: Paid order revenue by day.\n"
    "source: finance.sales.orders\n"
    "dimensions: []\n"
    "measures:\n"
    "  - name: paid_revenue\n"
    "    expr: SUM(CASE WHEN status = 'paid' THEN amount END)\n"
    "    synonyms: ['2', revenue]\n"
)

# The same view as Unity Catalog stores it (check V4): quotes dropped, the empty
# list dropped, the block scalar chomped, and keys reordered.
_UC_VIEW_TEXT = (
    "measures:\n"
    "- name: paid_revenue\n"
    "  expr: SUM(CASE WHEN status = 'paid' THEN amount END)\n"
    "  synonyms:\n"
    "  - 2\n"
    "  - revenue\n"
    "version: 1.1\n"
    "source: finance.sales.orders\n"
    "comment: |-\n"
    "  PURPOSE: Paid order revenue by day.\n"
)


def _envelope(view_text=_UC_VIEW_TEXT, owner="analyst@example.com", type_="METRIC_VIEW"):
    return {
        "catalog_name": "finance", "schema_name": "sales", "table_name": "revenue_metrics",
        "type": type_, "owner": owner, "view_text": view_text,
    }


def _describe_returns(monkeypatch, envelope):
    seen = _Calls()

    def query(ws, warehouse_id, sql):
        seen.on.append(ws)
        seen.append(sql)
        return pd.DataFrame({"json_metadata": [json.dumps(envelope)]})

    monkeypatch.setattr(warehouse, "sql_warehouse_query", query)
    return seen


def _match(yaml_text=_PROPOSAL_YAML, caller="analyst@example.com"):
    return mv_create._existing_view_matches(
        _OBO_WS, "wh1",
        full_name="finance.sales.revenue_metrics", yaml_text=yaml_text, caller=caller,
    )


def test_uc_rewritten_definition_matches_its_proposal(monkeypatch):
    seen = _describe_returns(monkeypatch, _envelope())

    assert _match() == (True, True, "analyst@example.com", None)
    assert seen == ["DESCRIBE TABLE EXTENDED `finance`.`sales`.`revenue_metrics` AS JSON"]
    assert seen.on == [_OBO_WS]


def test_a_changed_literal_is_a_different_definition(monkeypatch):
    squatted = _UC_VIEW_TEXT.replace("'paid'", "'refunded'")
    _describe_returns(monkeypatch, _envelope(view_text=squatted))

    matches, owned, owner, reason = _match()

    assert (matches, owned, owner) == (False, False, "")
    assert "different definition" in reason


@pytest.mark.parametrize(
    "owner,expected",
    [("other@example.com", False), ("ANALYST@example.com", True), ("", False)],
)
def test_ownership_is_the_describe_owner_against_the_caller(monkeypatch, owner, expected):
    _describe_returns(monkeypatch, _envelope(owner=owner))

    assert _match() == (True, expected, owner.lower(), None)


def test_existing_view_match_returns_the_uc_owner(monkeypatch):
    """M7c: the UC owner comes back, lowercased and stripped, so approval can name it."""
    _describe_returns(monkeypatch, _envelope(owner=" Owner@Example.com "))
    assert _match(caller="analyst@example.com") == (True, False, "owner@example.com", None)

    _describe_returns(monkeypatch, _envelope(owner="Analyst@Example.COM"))
    assert _match(caller="analyst@example.com") == (True, True, "analyst@example.com", None)


def test_existing_view_match_with_no_owner_is_not_the_callers(monkeypatch):
    envelope = _envelope()
    del envelope["owner"]
    _describe_returns(monkeypatch, envelope)

    assert _match() == (True, False, "", None)


@pytest.mark.parametrize("scalar", ["true", "007", "0.10", "1:30"])
def test_a_quoted_scalar_matches_its_unquoted_uc_form(monkeypatch, scalar):
    proposal = _PROPOSAL_YAML.replace("['2', revenue]", f"['{scalar}', revenue]")
    _describe_returns(monkeypatch, _envelope(view_text=_UC_VIEW_TEXT.replace("  - 2\n", f"  - {scalar}\n")))

    assert _match(yaml_text=proposal) == (True, True, "analyst@example.com", None)


def test_a_bare_yaml_boolean_is_not_its_quoted_word(monkeypatch):
    proposal = _PROPOSAL_YAML.replace("['2', revenue]", "['True', revenue]")
    _describe_returns(monkeypatch, _envelope(view_text=_UC_VIEW_TEXT.replace("  - 2\n", "  - on\n")))

    matches, _, owner, reason = _match(yaml_text=proposal)

    assert matches is False and owner == ""
    assert "different definition" in reason


def test_an_unparsable_definition_is_refused(monkeypatch):
    _describe_returns(monkeypatch, _envelope(view_text="measures: [unclosed\n"))

    matches, owned, owner, reason = _match()

    assert (matches, owned, owner) == (False, False, "")
    assert "could not be read" in reason


def test_a_self_referencing_alias_is_refused_not_raised(monkeypatch):
    _describe_returns(monkeypatch, _envelope(view_text="measures: &loop\n- *loop\n"))

    matches, owned, owner, reason = _match()

    assert (matches, owned, owner) == (False, False, "")
    assert "could not be read" in reason


def test_a_hidden_definition_is_refused(monkeypatch):
    _describe_returns(monkeypatch, _envelope(view_text=""))

    matches, _, owner, reason = _match()

    assert matches is False and owner == ""
    assert "not visible to you" in reason


def test_an_object_that_is_not_a_metric_view_is_refused(monkeypatch):
    _describe_returns(monkeypatch, _envelope(type_="VIEW"))

    matches, _, owner, reason = _match()

    assert matches is False and owner == ""
    assert "could not be checked" in reason


def _adopt():
    return mv_create._adopt_existing_view(
        _OBO_WS, "wh1",
        full_name="finance.sales.revenue_metrics", yaml_text=_PROPOSAL_YAML,
        caller="analyst@example.com",
    )


def _not_a_confirmed_metric_view(monkeypatch, *, found: bool):
    """``_confirm_metric_view`` says no; ``_object_exists`` answers ``found``.
    Returns the captured confirm calls, existence calls and matcher calls."""
    confirms: list = []
    lookups: list = []
    matches: list = []
    monkeypatch.setattr(
        mv_create, "_confirm_metric_view",
        lambda ws, warehouse_id, full_name: (confirms.append((ws, warehouse_id, full_name)) or False),
    )
    monkeypatch.setattr(
        mv_create, "_object_exists",
        lambda ws, warehouse_id, full_name: (lookups.append((ws, warehouse_id, full_name)) or found),
    )
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda *a, **k: (matches.append(k) or (True, True, "analyst@example.com", None)),
    )
    return confirms, lookups, matches


def test_adopt_existing_view_refuses_a_non_metric_view(monkeypatch):
    confirms, lookups, matches = _not_a_confirmed_metric_view(monkeypatch, found=True)

    existing = _adopt()

    assert existing == mv_create.ExistingView(
        matches=False, owned_by_caller=False, owner="",
        reason="finance.sales.revenue_metrics already exists and is not a metric view; "
        "refusing to clobber it",
        exists=True,
    )
    assert confirms == [(_OBO_WS, "wh1", "finance.sales.revenue_metrics")]
    assert lookups == [(_OBO_WS, "wh1", "finance.sales.revenue_metrics")]
    assert matches == []


def test_adopt_existing_view_reports_an_object_it_cannot_find(monkeypatch):
    """MV-D120: after a failed CREATE the name may be empty; the check says it found
    nothing rather than claiming an object that is not a metric view."""
    _, lookups, matches = _not_a_confirmed_metric_view(monkeypatch, found=False)

    existing = _adopt()

    assert existing.exists is False
    assert existing.matches is False and existing.owned_by_caller is False
    assert "already exists" not in (existing.reason or "")
    assert lookups == [(_OBO_WS, "wh1", "finance.sales.revenue_metrics")]
    assert matches == []


@pytest.mark.parametrize(
    "stub",
    [
        (True, True, "analyst@example.com", None),
        (True, False, "other@example.com", None),
        (False, False, "", "finance.sales.revenue_metrics already exists with a different definition"),
    ],
)
def test_adopt_existing_view_passes_the_match_through(monkeypatch, stub):
    calls: list = []
    monkeypatch.setattr(mv_create, "_confirm_metric_view", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_create, "_existing_view_matches",
        lambda ws, warehouse_id, **k: (calls.append({"ws": ws, "wh": warehouse_id, **k}) or stub),
    )

    existing = _adopt()

    assert existing == mv_create.ExistingView(*stub)
    assert calls == [{
        "ws": _OBO_WS, "wh": "wh1",
        "full_name": "finance.sales.revenue_metrics",
        "yaml_text": _PROPOSAL_YAML,
        "caller": "analyst@example.com",
    }]


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
    assert body["grant_sql"] and "GRANT SELECT ON VIEW `finance`.`sales`.`revenue_metrics`" in body["grant_sql"]
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
    assert body["grant_sql"] and "GRANT SELECT ON VIEW `finance`.`sales`.`revenue_metrics`" in body["grant_sql"]


def test_create_route_reports_user_created_provenance(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=True, attached=True, already_existed=True,
            full_name="finance.sales.revenue_metrics", run_id="run-obo-2",
            suggestion_id="sug1", verdict="SUFFICIENT",
            provenance=mv_create.MV_PROVENANCE_USER_CREATED,
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
    assert resp.json()["provenance"] == "USER_CREATED"
    assert resp.json()["already_existed"] is True


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


def test_a_warehouse_runtime_error_is_a_500_not_a_401(client, monkeypatch, caplog):
    caplog.set_level("DEBUG")

    def _raise(**k):
        raise RuntimeError("zq_secret")

    monkeypatch.setattr(mv_create, "create_at_approval", _raise)
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 500
    assert resp.json() == {
        "detail": "Create failed; please retry or approve for the next run."
    }
    assert "zq_secret" not in resp.text
    assert any(
        "mv/create (at approval) failed for space space-1 (RuntimeError)" == r.getMessage()
        for r in caplog.records
    )
    for record in caplog.records:
        assert "zq_secret" not in record.getMessage()
        assert record.exc_info is None


@pytest.mark.parametrize(
    "provenance, owner, grant_expected",
    [
        ("USER_CREATED", "other@example.com", False),
        ("OBO_CREATED", None, True),
    ],
)
def test_grant_sql_only_for_the_owner(client, monkeypatch, provenance, owner, grant_expected):
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=True, attached=True, already_existed=owner is not None,
            full_name="finance.sales.revenue_metrics", run_id="run-obo-3",
            suggestion_id="sug1", verdict="SUFFICIENT",
            provenance=provenance, owner=owner,
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
    assert body["provenance"] == provenance
    assert body["owner"] == owner
    if grant_expected:
        assert "GRANT SELECT ON VIEW `finance`.`sales`.`revenue_metrics`" in body["grant_sql"]
    else:
        assert body["grant_sql"] is None
    assert body["workspace_host"] == "https://example.databricks.com"


def test_approval_refuses_a_body_reading_a_table_the_consent_did_not_cover(approval_env, monkeypatch):
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {**_ARTIFACT, "yaml_text": "version: 0.1\nsource: finance.hr.salaries\n"},
    )
    result = _create()
    assert result.created is False
    assert result.degraded is False
    assert result.reason == mv_create.UNCOVERED_TABLES_REASON
    assert not result.remediation_sql
    assert not any("DESCRIBE" in s or "CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []
