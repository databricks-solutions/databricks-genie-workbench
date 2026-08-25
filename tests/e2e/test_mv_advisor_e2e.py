"""MV Advisor end-to-end scenarios against a real Databricks workspace (Prompt 15).

Four scenarios (A-D, including D's BYO leg), tiered cheapest-first so the manual
checkpoint can proceed as config is assembled:

  Tier 1 (no job, no eval budget) : Scenario D — suggest COMPLETE, suggest EMPTY,
                                    and the cheap BYO asserts (register ->
                                    USER_CREATED, drop refused, route-10 provenance).
  Tier 2 (adds GSO_JOB_ID + low-priv token) : Scenario A (suggest_only run) and
                                    Scenario B (denied-permission downgrade).
  Tier 3 (full consent chain, the eval spender) : Scenario C, and the BYO
                                    attach-and-measure leg.

Every test is env-gated: a missing variable skips naming the variable AND the
scenario. Every test is ``slow`` and runs under the process-wide serialization
lock in conftest, so the ~20 questions/min native-eval ceiling is respected.
The real route code runs in-process (decision A) with OBO injected by the real
middleware; see ``conftest.py``.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from .conftest import require

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


# ── shared helpers ───────────────────────────────────────────────────────────


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _ddl_parses_without_error(ddl: str) -> None:
    """Scenario A: the rendered CREATE VIEW parses under the pinned sqlglot."""
    import sqlglot

    sqlglot.parse_one(ddl, read="databricks")  # raises on malformed DDL


def _yaml_is_structurally_valid(yaml_text: str) -> None:
    """Scenario A/C: MV-D8 static checks (no transitive joins, structured
    comment, format/synonym bounds). Without a benchmark oracle the BEST FOR
    echo check reports NOT_COMPARED rather than failing — that half is pinned by
    the firewall unit tests, not observable here."""
    from genie_space_optimizer.optimization import mv_yaml

    report = mv_yaml.validate(yaml_text)
    assert report.ok, f"mv_yaml.validate rejected the shipped YAML: {report.errors}"


def _load_candidates(ws, gso, *, run_id=None, target_space_id=None):
    from genie_space_optimizer.common.warehouse import wh_load_mv_candidates

    return wh_load_mv_candidates(
        ws, gso.warehouse_id, gso.catalog, gso.schema,
        run_id=run_id, target_space_id=target_space_id,
    )


def _load_created(ws, gso, run_id):
    from genie_space_optimizer.common.warehouse import wh_load_mv_created_objects

    return wh_load_mv_created_objects(
        ws, gso.warehouse_id, catalog=gso.catalog, schema=gso.schema, run_id=run_id
    )


def _load_consent_by_run(ws, gso, run_id):
    from genie_space_optimizer.common.warehouse import wh_load_mv_consent_by_run

    return wh_load_mv_consent_by_run(
        ws, gso.warehouse_id, catalog=gso.catalog, schema=gso.schema, run_id=run_id
    )


def _describe_is_metric_view(run_sql, full_name: str) -> bool:
    """DESCRIBE EXTENDED must show ``Type: METRIC_VIEW`` (MV post-create pin)."""
    df = run_sql(f"DESCRIBE EXTENDED {full_name}")
    rows = df.to_dict("records") if hasattr(df, "to_dict") else list(df)
    for row in rows:
        values = [str(v) for v in row.values()]
        if any("METRIC_VIEW" in v for v in values):
            return True
    return False


# ── Scenario D — suggest with no run at all (Tier 1; MV-D23/MV-D24) ──────────


def test_scenario_d_suggest_with_curated_sql(api_primary, ws, gso):
    """D-curated: a never-optimized space with curated SQL yields proposals from
    the space-scoped suggest route, with no run present, each citing evidence."""
    require("GSO_JOB_ID", "Scenario D (config gate only; suggest never triggers it)")
    space_id = require("MV_E2E_SUGGEST_SPACE_ID", "Scenario D-curated")

    resp = api_primary.post(f"/api/auto-optimize/spaces/{space_id}/mv/suggest")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["space_id"] == space_id
    assert body["status"] == "COMPLETE", (
        f"expected COMPLETE for a curated space; got {body['status']} "
        f"(skip_reason={body.get('skip_reason')}). Pick a space with curated "
        f"example_question_sqls / sql_snippets.measures for MV_E2E_SUGGEST_SPACE_ID."
    )
    assert body["proposals"], "curated space produced no proposals"
    for proposal in body["proposals"]:
        # Evidence must be present and cite curated provenance (POV Part 4).
        assert proposal.get("evidence"), f"proposal {proposal['suggestion_id']} has no evidence"


def test_scenario_d_suggest_empty_with_reason(api_primary, gso):
    """D-empty: a bare space (no curated SQL, no history) returns an EMPTY result
    WITH a reason — not an error, not a 500, not silence. The demo's first hit."""
    require("GSO_JOB_ID", "Scenario D (config gate only)")
    space_id = require("MV_E2E_EMPTY_SPACE_ID", "Scenario D-empty")

    resp = api_primary.post(f"/api/auto-optimize/spaces/{space_id}/mv/suggest")
    assert resp.status_code == 200, resp.text  # not a 500, not an error
    body = resp.json()
    assert body["status"] == "SKIPPED", (
        f"a bare space must SKIP with a reason, not {body['status']}"
    )
    assert body["skip_reason"], "EMPTY result carried no skip_reason (silent empty)"
    assert body["proposals"] == []
    assert body["error"] is None


def test_scenario_d_byo_register_refuse_and_provenance(api_primary, ws, gso, scratch, run_sql, cleanup, primary_email):
    """D-BYO (cheap half): a user-created view registers as USER_CREATED, the app
    REFUSES to drop it (409), and route 10 reports provenance so the UI hides the
    Drop affordance. The attach-and-measure half is Tier 3 (see below)."""
    require("GSO_JOB_ID", "Scenario D-BYO (config gate)")
    space_id = require("MV_E2E_SUGGEST_SPACE_ID", "Scenario D-BYO")
    scratch_catalog, scratch_schema = scratch

    # Obtain a real, renderable metric-view YAML by asking the advisor for the
    # curated space, then copy its DDL into the scratch schema by hand — exactly
    # the "created a metric view manually from the copied DDL" the scenario means.
    suggest = api_primary.post(f"/api/auto-optimize/spaces/{space_id}/mv/suggest").json()
    if suggest.get("status") != "COMPLETE" or not suggest.get("proposals"):
        pytest.skip(
            "Scenario D-BYO needs a curated MV_E2E_SUGGEST_SPACE_ID that yields a "
            f"proposal to copy; suggest returned status={suggest.get('status')}"
        )
    suggest_run = suggest["run_id"]
    ddl_resp = api_primary.get(f"/api/auto-optimize/runs/{suggest_run}/mv-ddl")
    assert ddl_resp.status_code == 200, ddl_resp.text
    yaml_text = ddl_resp.json().get("yaml_text")
    assert yaml_text, "suggest run produced no DDL artifact to copy"

    from genie_space_optimizer.optimization.mv_yaml import create_ddl

    byo_name = f"{scratch_catalog}.{scratch_schema}.mv_e2e_byo"
    cleanup.append(lambda: run_sql(f"DROP VIEW IF EXISTS {byo_name}"))
    run_sql(f"DROP VIEW IF EXISTS {byo_name}")
    run_sql(create_ddl(byo_name, yaml_text))

    # Register it. Verified and refused are both 200; here we expect verified.
    reg = api_primary.post(
        f"/api/auto-optimize/spaces/{space_id}/mv/register",
        json={"full_name": byo_name},
    )
    assert reg.status_code == 200, reg.text
    reg_body = reg.json()
    assert reg_body["registered"] is True, f"register refused: {reg_body.get('reason')}"
    assert reg_body["provenance"] == "USER_CREATED"
    byo_run = reg_body["run_id"]
    byo_suggestion_id = reg_body["suggestion_id"]
    assert byo_run, "register did not return the sentinel advice run_id"

    # Route 10 surfaces provenance (Prompt 14.1 fix) and the registering user.
    created = api_primary.get(f"/api/auto-optimize/runs/{byo_run}/mv-created").json()
    byo_rows = [o for o in created["created"] if o["full_name"] == byo_name]
    assert byo_rows, f"USER_CREATED ledger row for {byo_name} not found on route 10"
    assert byo_rows[0]["provenance"] == "USER_CREATED"
    assert byo_rows[0]["created_by"] == primary_email

    # The app must REFUSE to drop a USER_CREATED view (409). Teardown drops it
    # manually (registered above) — the app never does.
    drop = api_primary.post(
        f"/api/auto-optimize/mv/created/{byo_suggestion_id}/drop",
        json={"run_id": byo_run, "confirm": True},
    )
    assert drop.status_code == 409, (
        f"drop must refuse USER_CREATED with 409; got {drop.status_code}: {drop.text}"
    )


# ── Scenario A — suggest_only run (Tier 2) ───────────────────────────────────


def test_scenario_a_suggest_only(api_primary, ws, gso, poll_job_run):
    """A: a run with the advisor on in suggest_only completes, persists >=1
    candidate, emits a DDL artifact that parses and is structurally valid, the
    UI endpoints return DDL + GRANT, and NO metric view is created."""
    require("GSO_JOB_ID", "Scenario A")
    space_id = require("MV_E2E_SPACE_ID", "Scenario A")

    resp = api_primary.post(
        "/api/auto-optimize/trigger",
        json={
            "space_id": space_id,
            "enable_metric_view_suggestions": True,
            "mv_action_mode": "suggest_only",
            "max_attempts": 1,
        },
    )
    assert resp.status_code == 200, resp.text
    trig = resp.json()
    run_id, job_run_id = trig["runId"], trig["jobRunId"]

    result = poll_job_run(job_run_id)
    assert result.result_state == "SUCCESS", (
        f"optimization run did not succeed: {result}"
    )

    candidates = _load_candidates(ws, gso, run_id=run_id)
    assert candidates, "suggest_only run persisted no MV candidates"

    ddl_resp = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-ddl")
    assert ddl_resp.status_code == 200, ddl_resp.text
    ddl = ddl_resp.json()
    assert ddl["ddl"], "no DDL in the artifact"
    assert ddl["grant_sql"], "no GRANT in the artifact"
    assert "GRANT SELECT ON VIEW" in ddl["grant_sql"]
    _ddl_parses_without_error(ddl["ddl"])
    if ddl.get("yaml_text"):
        _yaml_is_structurally_valid(ddl["yaml_text"])

    created = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-created").json()
    assert created["created"] == [], "suggest_only must create no metric views"


# ── Scenario B — denied permission downgrade (Tier 2) ────────────────────────


def test_scenario_b_probe_insufficient(api_primary, gso, denied):
    """B (probe half): the PRIMARY identity's probe for create_and_attach on a
    schema it cannot write returns a non-SUFFICIENT verdict — either INSUFFICIENT
    (grants resolve as not-granted) or UNKNOWN (grants resolve as unreadable).

    Both are acceptable and asserted as a set: at the UC boundary denied and
    unreadable are indistinguishable (the same reasoning MV-D13 recorded for
    NOT_FOUND being treated as DENIED), and ``_verdict`` returns INSUFFICIENT only
    on DENIED while UNKNOWN short-circuits to UNKNOWN (mv_entitlement.py:400-402).
    Either downgrades a create_and_attach run, so the scenario's substance holds;
    the strict downgrade is pinned by the run-half test below. (This is the
    simplified Scenario B — no second low-priv identity; the SP-fallback detection
    it would have exercised is pinned offline instead, per the runbook tradeoff.)
    """
    require("GSO_JOB_ID", "Scenario B")
    denied_catalog, denied_schema = denied

    resp = api_primary.post(
        "/api/auto-optimize/mv/probe",
        json={
            "catalog": denied_catalog,
            "schema": denied_schema,
            "space_id": require("MV_E2E_SPACE_ID", "Scenario B"),
            "source_tables": [],
            "materialize_consented": False,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    verdict = body["verdict"]
    assert verdict in {"INSUFFICIENT", "UNKNOWN"}, (
        f"primary identity on the denied schema {denied_catalog}.{denied_schema} "
        f"should be INSUFFICIENT (not-granted) or UNKNOWN (unreadable) — both "
        f"downgrade a create_and_attach run and are indistinguishable at the UC "
        f"boundary (MV-D13). Got {verdict}. A SUFFICIENT verdict means the denied "
        f"schema is unexpectedly writable by this identity; set "
        f"MV_E2E_DENIED_CATALOG/SCHEMA to one it cannot create in."
    )
    # missing is named on the DENIED path; UNKNOWN short-circuits before it.
    if verdict == "INSUFFICIENT":
        assert body["missing"], "INSUFFICIENT verdict named no missing privilege"


def test_scenario_b_run_auto_downgrades(api_primary, ws, gso, denied, poll_job_run, primary_email):
    """B (run half): a create_and_attach run whose consent re-verifies to a
    non-SUFFICIENT verdict on the denied schema auto-downgrades to suggest_only,
    creates NO UC object, records a downgrade_reason, and still completes the
    optimization. This is the assertion that carries the scenario — verify()
    treats anything short of SUFFICIENT as a downgrade, so it holds identically
    for INSUFFICIENT and UNKNOWN and stays strict regardless of which the probe
    half observed."""
    require("GSO_JOB_ID", "Scenario B")
    space_id = require("MV_E2E_SPACE_ID", "Scenario B")
    denied_catalog, denied_schema = denied

    probe = api_primary.post(
        "/api/auto-optimize/mv/probe",
        json={
            "catalog": denied_catalog,
            "schema": denied_schema,
            "space_id": space_id,
            "source_tables": [],
            "materialize_consented": False,
        },
    ).json()
    probe_id = probe["probe_id"]

    resp = api_primary.post(
        "/api/auto-optimize/trigger",
        json={
            "space_id": space_id,
            "enable_metric_view_suggestions": True,
            "mv_action_mode": "create_and_attach",
            "max_attempts": 1,
            "mv_consent": {
                "granted_by": primary_email,
                "granted_at": _now_iso(),
                "probe_id": probe_id,
            },
        },
    )
    assert resp.status_code == 200, resp.text
    run_id, job_run_id = resp.json()["runId"], resp.json()["jobRunId"]

    result = poll_job_run(job_run_id)
    assert result.result_state == "SUCCESS", f"downgraded run must still finish: {result}"

    created = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-created").json()
    assert created["created"] == [], "a downgraded run must create NO metric view"
    assert created["downgrade_reason"], "downgrade left no downgrade_reason on the run"


# ── Scenario C — approve, re-run, create_and_attach with lift (Tier 3) ───────


def test_scenario_c_create_attach_and_lift(api_primary, ws, gso, scratch, run_sql, poll_job_run, cleanup, primary_email):
    """C: two runs per MV-D1. Run 1 (suggest_only) proposes; approve one; Run 2
    (create_and_attach) creates the MV under the USER's identity in the consented
    schema, attaches it via the mv_attach patch, measures lift, and records the
    audit trail. Teardown drops the scratch MV — the app never does outside sandbox.
    """
    require("GSO_JOB_ID", "Scenario C")
    space_id = require("MV_E2E_SPACE_ID", "Scenario C")
    scratch_catalog, scratch_schema = scratch

    # ── Run 1: suggest_only proposes ────────────────────────────────────────
    r1 = api_primary.post(
        "/api/auto-optimize/trigger",
        json={
            "space_id": space_id,
            "enable_metric_view_suggestions": True,
            "mv_action_mode": "suggest_only",
            "max_attempts": 1,
        },
    )
    assert r1.status_code == 200, r1.text
    run1_id, run1_job = r1.json()["runId"], r1.json()["jobRunId"]
    assert poll_job_run(run1_job).result_state == "SUCCESS"

    candidates = _load_candidates(ws, gso, run_id=run1_id)
    if not candidates:
        pytest.skip("Run 1 proposed no candidates; pick an MV_E2E_SPACE_ID that does")
    suggestion_id = candidates[0]["suggestion_id"]

    # ── Approve one proposal via the decision endpoint ──────────────────────
    decision = api_primary.post(
        f"/api/auto-optimize/mv/proposals/{suggestion_id}/decision",
        json={"space_id": space_id, "run_id": run1_id, "decision": "approved"},
    )
    assert decision.status_code == 200, decision.text
    assert decision.json()["approved_for_rerun"] is True

    # ── Probe (primary identity, expected SUFFICIENT) + consent ─────────────
    probe = api_primary.post(
        "/api/auto-optimize/mv/probe",
        json={
            "catalog": scratch_catalog,
            "schema": scratch_schema,
            "space_id": space_id,
            "source_tables": [],
            "materialize_consented": False,
        },
    ).json()
    if probe["verdict"] != "SUFFICIENT":
        pytest.skip(
            f"primary identity is {probe['verdict']} on {scratch_catalog}."
            f"{scratch_schema} (missing={probe.get('missing')}); grant it "
            "USE SCHEMA + CREATE TABLE to run Scenario C"
        )
    probe_id = probe["probe_id"]

    # ── Run 2: create_and_attach with consent + the approved suggestion ─────
    r2 = api_primary.post(
        "/api/auto-optimize/trigger",
        json={
            "space_id": space_id,
            "enable_metric_view_suggestions": True,
            "mv_action_mode": "create_and_attach",
            "max_attempts": 1,
            "mv_approved_suggestion_ids": [suggestion_id],
            "mv_consent": {
                "granted_by": primary_email,
                "granted_at": _now_iso(),
                "probe_id": probe_id,
            },
        },
    )
    assert r2.status_code == 200, r2.text
    run2_id, run2_job = r2.json()["runId"], r2.json()["jobRunId"]

    # The backend creates the MV under OBO BEFORE the job starts (MV-D20). Read
    # the ledger for the created object; register teardown as soon as we know it.
    created_rows = _load_created(ws, gso, run2_id)
    if not created_rows:
        # Give the pre-submit create a moment if the read raced the write, then
        # rely on the terminal poll below to surface a real failure.
        pass
    for row in created_rows:
        fq = row.get("full_name")
        if fq:
            cleanup.append(lambda name=fq: run_sql(f"DROP VIEW IF EXISTS {name}"))

    assert poll_job_run(run2_job).result_state == "SUCCESS", "create_and_attach run failed"

    # ── Assertions on the created object + audit trail ──────────────────────
    created_rows = _load_created(ws, gso, run2_id)
    assert created_rows, "create_and_attach recorded no created object"
    obj = created_rows[0]
    full_name = obj["full_name"]
    cleanup.append(lambda: run_sql(f"DROP VIEW IF EXISTS {full_name}"))

    # Created by the USER, in the consented schema ONLY.
    assert obj["created_by"] == primary_email, "MV not created under the user's identity"
    assert obj.get("provenance", "OBO_CREATED") == "OBO_CREATED"
    assert full_name.startswith(f"{scratch_catalog}.{scratch_schema}."), (
        f"MV created outside the consented schema: {full_name}"
    )

    # DESCRIBE EXTENDED shows Type: METRIC_VIEW.
    assert _describe_is_metric_view(run_sql, full_name), (
        f"{full_name} is not a METRIC_VIEW per DESCRIBE EXTENDED"
    )

    # YAML passes mv_yaml.validate (structured comment, no transitive joins, ...).
    ddl = api_primary.get(f"/api/auto-optimize/runs/{run2_id}/mv-ddl").json()
    if ddl.get("yaml_text"):
        _yaml_is_structurally_valid(ddl["yaml_text"])

    # The attach patch was created; lift eval ran to DONE with both run ids.
    assert obj.get("attach_patch_id"), "no mv_attach patch id recorded"
    assert obj.get("baseline_eval_run_id"), "no baseline eval run id"
    assert obj.get("post_attach_eval_run_id"), "no post-attach eval run id"
    assert obj.get("lift_report"), "lift_report not stored on the created object"
    lift = obj["lift_report"]
    assert lift.get("pre_eval_run_id") and lift.get("post_eval_run_id"), (
        "lift_report missing its eval_run_id pair"
    )

    # Audit rows answer "who created this and why".
    consent = _load_consent_by_run(ws, gso, run2_id)
    assert consent, "no consent row for the create_and_attach run"
    assert consent.get("granted_by") == primary_email

    # serialized_space reflects the attach when the view stayed ATTACHED; a
    # DETACHED verdict (regression) is a valid outcome and reverts the attach.
    if obj["status"] == "ATTACHED":
        from genie_space_optimizer.common.genie_client import fetch_space_config

        cfg = fetch_space_config(ws, space_id).get("_parsed_space", {})
        identifiers = [
            (mv.get("identifier") or "")
            for mv in cfg.get("data_sources", {}).get("metric_views", [])
        ]
        assert any(full_name in ident for ident in identifiers), (
            "ATTACHED object not present in serialized_space.data_sources.metric_views"
        )
