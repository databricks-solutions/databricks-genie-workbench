"""In-process dry-run harness for the MV Delta-by-run_id handoff (Prompt 14).

The unit suites each pin one phase. This harness verifies the *handoff between*
them end to end with fakes, because the defects this branch actually hit lived in
the seams: a write under one key that the next phase reads under another. It runs
the real writers and readers over the in-memory ``FakeDeltaSpark`` (no mocked
return values), so a key or column drift between a producer and its consumer
fails here.

Two legs:

* **In-job (Spark).** trigger-created object row -> attach phase (which runs the
  ``mv_lift`` sub-step) -> advisor proposes the next candidate -> the row the
  *next* trigger reads (``approved_for_rerun`` + ``target_space_id``). This is the
  exact chain Prompt 13's create path walks: ``create_and_attach_for_run`` reads
  ``wh_load_mv_candidates(approved_for_rerun=True)``; here its Spark twin
  ``mv_state.load_mv_candidates`` reads the same table under the same keys.
* **Standalone advice (MV-D23/D24, warehouse).** suggest's born-terminal sentinel
  run -> candidate row carrying ``yaml_text`` -> a ``USER_CREATED`` register write
  -> the attach phase accepting that verified bring-your-own row. The write side
  runs the real ``wh_*`` writers through the captured ``sql_warehouse_execute``
  seam; the acceptance runs the real attach phase.

Harness helpers are reused from ``test_mv_attach`` (which reuses
``FakeDeltaSpark`` from ``test_mv_state``) — the same cross-module import the
attach suite already relies on.
"""

from __future__ import annotations

import base64
import json
import re

import pytest

from genie_space_optimizer.common import warehouse
from genie_space_optimizer.common.config import (
    MV_ADVICE_RUN_EXCLUSION,
    MV_ADVICE_RUN_STATUS,
    MV_RUN_KIND_ADVICE,
)
from genie_space_optimizer.optimization import mv_attach, mv_state
from test_mv_attach import (
    AFFECTED,
    CATALOG,
    MV_NAME,
    RUN_ID,
    SCHEMA,
    SPACE_ID,
    USER,
    FakeDeltaSpark,
    _FakeRunner,
    _baseline_output,
    _config,
    _created_row,
    _row,
    _run_phase,
    _seed,
)


def _decoded_literals(sql: str) -> list[str]:
    """Every ``unbase64('..')`` string literal in a merge/insert, decoded."""
    return [
        base64.b64decode(blob).decode("utf-8")
        for blob in re.findall(r"unbase64\('([^']*)'\)", sql)
    ]


@pytest.fixture
def executed(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: statements.append(sql),
    )
    return statements


# ── Leg 1: in-job Delta-by-run_id handoff ────────────────────────────────


def test_attach_lift_then_the_next_trigger_reads_the_approved_candidate() -> None:
    """created row -> attach+lift -> advisor proposal -> next-trigger read."""
    spark = FakeDeltaSpark()
    # Trigger-created object row (CREATED) + consent + the fingerprinted candidate,
    # all under RUN_ID — exactly what the backend create hook writes before the job.
    _seed(spark)

    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    # Attach + lift handoff: the phase wrote the transition and the lift report
    # onto the created row keyed by RUN_ID, and it is readable back by run_id.
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED
    # FakeDeltaSpark is a single in-memory table, so a run_id read returns the
    # consent/candidate rows too; the created-object rows are the ones carrying
    # full_name, exactly as the attach suite disambiguates them.
    created = [
        r for r in mv_state.load_mv_created_objects(spark, RUN_ID, CATALOG, SCHEMA)
        if r.get("full_name")
    ]
    assert len(created) == 1
    assert created[0]["status"] == "ATTACHED"
    assert created[0]["post_attach_eval_run_id"] == "eval-lift"
    lift = json.loads(created[0]["lift_report_json"])
    assert lift["delta_affected"] == pytest.approx(0.5)

    # Advisor proposes the next candidate for this space (a fresh fingerprint).
    mv_state.upsert_mv_candidate(
        spark,
        catalog=CATALOG,
        schema=SCHEMA,
        run_id=RUN_ID,
        target_space_id=SPACE_ID,
        suggestion_id="sug-2",
        dedup_fingerprint="fp-2",
        candidate_type="NEW_METRIC_VIEW",
        tier="HIGH",
    )
    # A human approves it; MV-D1 gates the next run's create on approved_for_rerun.
    mv_state.record_mv_candidate_decision(
        spark,
        catalog=CATALOG,
        schema=SCHEMA,
        target_space_id=SPACE_ID,
        dedup_fingerprint="fp-2",
        decision="approved",
        decided_by=USER,
    )

    # The next trigger's read: space-scoped, approved only — the same query
    # create_and_attach_for_run runs. It sees the approved fingerprint and NOT
    # the seeded, undecided one (approved_for_rerun defaults false).
    approved = mv_state.load_mv_candidates(
        spark, CATALOG, SCHEMA, target_space_id=SPACE_ID, approved_for_rerun=True,
    )
    fingerprints = {row["dedup_fingerprint"] for row in approved}
    assert fingerprints == {"fp-2"}
    assert approved[0]["approved_for_rerun"] is True


def test_a_regression_detaches_but_the_created_row_survives_for_the_next_read() -> None:
    """Detach-never-drop: the run_id-keyed ledger row stays readable after detach."""
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.verdict == mv_attach.VERDICT_DETACHED
    created = [
        r for r in mv_state.load_mv_created_objects(spark, RUN_ID, CATALOG, SCHEMA)
        if r.get("full_name")
    ]
    assert len(created) == 1
    assert created[0]["status"] == "DETACHED"
    assert created[0]["full_name"] == MV_NAME


# ── Leg 2: standalone advice flow (MV-D23/D24) ───────────────────────────


def test_the_sentinel_advice_run_is_born_terminal_and_excluded(executed) -> None:
    warehouse.wh_create_advice_run(
        object(), "wh",
        run_id="advice-1", space_id=SPACE_ID, domain="",
        catalog=CATALOG, schema=SCHEMA, triggered_by=USER,
    )
    assert len(executed) == 1
    sql = executed[0]
    assert sql.startswith(f"INSERT INTO {CATALOG}.{SCHEMA}.genie_opt_runs")
    # Born terminal: written with the advice status + a completion timestamp, so
    # active-run reconciliation ({QUEUED, IN_PROGRESS}) can never adopt it.
    assert f"'{MV_ADVICE_RUN_STATUS}'" in sql
    assert "completed_at" in sql and "current_timestamp()" in sql
    # run_kind ties the row to the pinned exclusion predicate applied at every
    # listing site — the value the predicate filters out is exactly this one.
    assert f"'{MV_RUN_KIND_ADVICE}'" in sql
    assert MV_RUN_KIND_ADVICE in MV_ADVICE_RUN_EXCLUSION


def test_the_advice_candidate_carries_its_yaml_text_replay_body(executed) -> None:
    warehouse.wh_upsert_mv_candidate(
        object(), "wh",
        catalog=CATALOG, schema=SCHEMA,
        run_id="advice-1", target_space_id=SPACE_ID,
        suggestion_id="sug-a", dedup_fingerprint="fp-a",
        candidate_type="NEW_METRIC_VIEW",
        yaml_text="version: '1.1'\nsource: main.sales.fact_orders\n",
    )
    assert len(executed) == 1
    sql = executed[0]
    assert f"MERGE INTO {CATALOG}.{SCHEMA}.genie_opt_mv_candidates" in sql
    # MV-D23: the rendered replay body rides on the candidate row, so the create
    # path no longer needs the run-partitioned artifact.
    assert "yaml_text" in sql
    assert any("version: '1.1'" in literal for literal in _decoded_literals(sql))


def test_register_writes_a_user_created_ledger_row(executed) -> None:
    warehouse.wh_upsert_mv_created_object(
        object(), "wh",
        catalog=CATALOG, schema=SCHEMA,
        run_id="advice-2", suggestion_id="sug-a",
        full_name=MV_NAME, created_by=USER, status="CREATED",
        provenance="USER_CREATED",
    )
    assert len(executed) == 1
    sql = executed[0]
    assert f"MERGE INTO {CATALOG}.{SCHEMA}.genie_opt_mv_created_objects" in sql
    assert "provenance" in sql
    # MV-D24: the create-path discriminator is USER_CREATED, and the app must
    # never drop it — the value, not just the column, must be written. This
    # writer renders short enum-like strings as plain SQL literals.
    assert "'USER_CREATED'" in sql


def test_a_verified_user_created_row_is_accepted_and_attached() -> None:
    """The register -> attach handoff: attach accepts the bring-your-own row.

    A USER_CREATED row's ``created_by`` need not match the consent's
    ``granted_by`` — the registration verification is the coverage the creator
    guard exists to require. The same row that would skip as OBO_CREATED attaches
    once it is USER_CREATED, and a positive lift keeps it.
    """
    spark = FakeDeltaSpark()
    _seed(spark, created_by="someone.else@example.com", provenance="USER_CREATED")
    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.skip_reason != mv_attach.SKIP_CREATOR_MISMATCH
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED
    created = _created_row(spark)
    assert created["status"] == "ATTACHED"
