"""Nightly settings mirror, app side (MV-D109 P5).

Pins: (1) the drift guard — the mirror's widget map equals exactly what an
on-demand ``_launch`` forwards (minus infra keys), so nightly == on-demand and P4
cannot forward ``external_context_*`` on demand without also teaching the mirror;
(2) the Settings PUT mirrors as the OBO admin's email and a mirror failure never
fails the save; (3) ``write_mirror`` binds every value (no interpolation) and runs as
the service principal; (4) the job never writes the mirror table.
"""

from __future__ import annotations

import inspect
import json
import pathlib
import re
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from genie_space_optimizer.ontology import ddl
from genie_space_optimizer.ontology.settings_mirror import settings_to_widgets

from backend.ontology.models import IndustryAlignment, OntologySettings
from backend.ontology.routers.settings import router as settings_router
from backend.ontology.services import mirror, ont_settings, refresh, settings_mirror
from backend.tests._admin import admin_client

_REPO = pathlib.Path(__file__).resolve().parents[2]
_WHEEL = _REPO / "packages/genie-space-optimizer/src/genie_space_optimizer"
_INFRA_KEYS = {"metastore_id", "workspace_id", "trigger", "catalog", "schema"}

_NON_DEFAULT = OntologySettings(
    company_name="Acme Air",
    catalog_allowlist=["finance", "ops"],
    read_identity="sp",
    domain_facet_denylist=["pii"],
    domain_min_tables=5,
    domain_min_schemas=3,
    domain_require_connection=False,
    domain_schema_denylist=["information_schema", "scratch"],
    domain_join_col_suffixes=["_id"],
    domain_join_col_max_schemas=4,
    domain_join_col_denylist=["id"],
    domain_max_diffuse_schemas=9,
    domain_min_home_concentration=0.75,
    page_autodraft_min_corroboration=2,
    page_autodraft_max_pages=7,
    industry_alignment=IndustryAlignment(enabled=True, reference_model="airline"),
)


def _capture_launch_params(monkeypatch, settings: OntologySettings) -> dict[str, str]:
    """Run the real on-demand path (trigger → _launch) with a fake jobs.run_now."""
    from backend.services import auth

    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    monkeypatch.setattr(ont_settings, "_metastore_id", lambda: "ms1")
    monkeypatch.setenv("GSO_ONT_JOB_ID", "12345")
    monkeypatch.setenv("GSO_CATALOG", "cat")

    async def _none(ws):
        return None

    async def _settings():
        return settings

    captured: dict = {}

    def _run_now(*, job_id, job_parameters):
        captured.update(job_parameters)
        return SimpleNamespace(run_id=99)

    fake = SimpleNamespace(jobs=SimpleNamespace(run_now=_run_now))
    monkeypatch.setattr(mirror, "latest_run", _none)
    monkeypatch.setattr(mirror, "latest_succeeded_run", _none)
    monkeypatch.setattr(ont_settings, "get_settings", _settings)
    monkeypatch.setattr(auth, "get_service_principal_client", lambda: fake)

    import asyncio

    asyncio.run(refresh.trigger())
    assert captured, "trigger() did not reach jobs.run_now"
    return captured


@pytest.mark.parametrize("settings", [_NON_DEFAULT, OntologySettings()], ids=["custom", "default"])
def test_mirror_widgets_equal_on_demand_launch_params(monkeypatch, settings):
    """The drift guard: nightly (mirror) == on-demand (_launch), key for key."""
    launched = _capture_launch_params(monkeypatch, settings)
    on_demand = {k: v for k, v in launched.items() if k not in _INFRA_KEYS}
    assert settings_to_widgets(settings.model_dump(mode="json")) == on_demand
    assert len(on_demand) == 16
    assert not any(k.startswith("external_context") for k in on_demand)


def _put_client(monkeypatch, write_mirror):
    async def _save(payload):
        return payload

    monkeypatch.setattr(ont_settings, "save_settings", _save)
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    monkeypatch.setattr(ont_settings, "_metastore_id", lambda: "ms1")
    monkeypatch.setattr(settings_mirror, "write_mirror", write_mirror)
    app = FastAPI()
    app.include_router(settings_router)
    return admin_client(app)


def test_put_mirrors_with_the_obo_email(monkeypatch):
    calls = []
    client = _put_client(monkeypatch, lambda s, **kw: calls.append((s, kw)))
    resp = client.put(
        "/api/ontology/settings",
        json={"catalog_allowlist": ["finance"]},
        headers={"x-forwarded-email": "admin@example.com"},
    )
    assert resp.status_code == 200
    assert len(calls) == 1
    saved, kw = calls[0]
    assert saved.catalog_allowlist == ["finance"]
    assert kw == {"workspace_id": "ws1", "metastore_id": "ms1", "updated_by": "admin@example.com"}


def test_mirror_failure_never_fails_the_save(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("warehouse down")

    client = _put_client(monkeypatch, _boom)
    resp = client.put(
        "/api/ontology/settings",
        json={"company_name": "Beta", "catalog_allowlist": ["ops"]},
        headers={"x-forwarded-email": "admin@example.com"},
    )
    assert resp.status_code == 200
    assert resp.json()["company_name"] == "Beta" and resp.json()["catalog_allowlist"] == ["ops"]


class _FakeExec:
    def __init__(self):
        self.calls = []

    def execute_statement(self, *, warehouse_id, statement, parameters, wait_timeout):
        from databricks.sdk.service.sql import StatementState

        self.calls.append((statement, parameters))
        return SimpleNamespace(statement_id="s1", status=SimpleNamespace(state=StatementState.SUCCEEDED))


def test_write_mirror_binds_every_value_and_runs_as_sp(monkeypatch):
    from backend.services import auth

    fake_exec = _FakeExec()
    monkeypatch.setattr(
        auth, "get_service_principal_client", lambda: SimpleNamespace(statement_execution=fake_exec)
    )
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")
    monkeypatch.setenv("GSO_CATALOG", "cat")
    monkeypatch.setenv("GSO_SCHEMA", "sch")

    evil = OntologySettings(company_name="O'Brien'; DROP TABLE x --", catalog_allowlist=["fin'ance"])
    settings_mirror.write_mirror(
        evil, workspace_id="ws'1", metastore_id="ms'1", updated_by="a'b@example.com"
    )

    (create_sql, create_params), (merge_sql, merge_params) = fake_exec.calls
    assert create_sql == ddl.all_ddl("cat", "sch")[ddl.TABLE_ONT_SETTINGS_MIRROR]
    assert create_params is None
    assert merge_sql.startswith("MERGE INTO cat.sch.genie_ont_settings_mirror ")
    # No value is interpolated: every value is a bound :name parameter.
    for needle in ("O'Brien", "fin'ance", "ws'1", "ms'1", "a'b@"):
        assert needle not in merge_sql
    assert set(re.findall(r":(\w+)", merge_sql)) == {
        "workspace_id", "metastore_id", "settings_json", "updated_by",
    }
    bound = {p.name: p.value for p in merge_params}
    assert bound["workspace_id"] == "ws'1" and bound["updated_by"] == "a'b@example.com"
    assert json.loads(bound["settings_json"]) == evil.model_dump(mode="json")

    src = inspect.getsource(settings_mirror.write_mirror)
    assert "get_service_principal_client" in src
    assert "require_obo_workspace_client" not in src and "get_workspace_client()" not in src


def test_write_mirror_raises_without_a_warehouse(monkeypatch):
    monkeypatch.delenv("SQL_WAREHOUSE_ID", raising=False)
    with pytest.raises(RuntimeError, match="SQL_WAREHOUSE_ID"):
        settings_mirror.write_mirror(
            OntologySettings(), workspace_id="ws1", metastore_id="ms1", updated_by="a@b.c"
        )


def test_save_settings_stays_lakebase_only():
    """Lakebase is the source of truth; save_settings never touches the mirror."""
    src = inspect.getsource(ont_settings)
    assert "settings_mirror" not in src and "genie_ont_settings_mirror" not in src


def test_the_job_never_writes_the_mirror_table():
    notebook = (_WHEEL / "jobs/run_ontology_materialize.py").read_text().lower()
    mat = (_WHEEL / "ontology/materialize.py").read_text().lower()
    for src in (notebook, mat):
        assert not re.search(
            r"(merge\s+into|insert\s+into|update|delete\s+from)\s+\S*genie_ont_settings_mirror", src
        )
    # The notebook reaches the table only through the wheel's SELECT builder.
    assert "genie_ont_settings_mirror" not in notebook
    assert "mirror_read_sql" in notebook
