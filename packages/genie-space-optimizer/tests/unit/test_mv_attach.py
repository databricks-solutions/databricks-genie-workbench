"""The metric view attach + lift phase (MV-D16), and the applier action it needs.

Three things here are load-bearing beyond ordinary coverage:

* **The consent chain.** ``mv_attach_data_source`` is registered in
  ``PATCH_TYPES`` and deliberately absent from the unified loop's
  ``_ALLOWED_PATCH_TYPES``. Two tests defend that: one reads the frozenset, and
  one drives an LLM response that proposes the type and asserts it is dropped.
  The second is the one that matters — the absence of a line in a frozenset is
  not self-documenting, and a future contributor adding it will have a reason
  that looks good at the time.
* **The ordering.** Iteration-0 must measure the space *before* the attach, because
  that baseline corpus is what the advisor fingerprints when proposing the next
  metric view.
* **Verbatim lift reports.** ``lift_report_json`` stores ``LiftReport.to_dict()``
  as handed over, round-tripped through the real MERGE builder rather than a mock.
"""

from __future__ import annotations

import base64
import copy
import json
import re
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from genie_space_optimizer.common.config import (
    HIGH_RISK_PATCHES,
    PATCH_TYPES,
    TABLE_MV_CREATED_OBJECTS,
)
from genie_space_optimizer.optimization import applier, mv_attach, mv_state, unified_loop
from genie_space_optimizer.optimization.champion import BaselineReset
from genie_space_optimizer.optimization.eval_runner import lift_report
from genie_space_optimizer.optimization.mv_yaml import (
    ColumnFacts,
    MeasureRequest,
    MvProfiling,
    generate,
)
from genie_space_optimizer.optimization.mv_scoring import MetricViewCandidate
from test_mv_state import FakeDeltaSpark

CATALOG = "main"
SCHEMA = "gso"
RUN_ID = "run-mv-1"
SPACE_ID = "space-abc"
PROBE_ID = "probe-1"
USER = "analyst@example.com"
MV_NAME = "main.sales.mv_revenue"
SUGGESTION_ID = "sug-1"
AFFECTED = ["rev_001", "rev_002"]


# ── Fixtures and fakes ───────────────────────────────────────────────────


def _config(metric_views: list[dict] | None = None) -> dict[str, Any]:
    return {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": "main.sales.fact_orders"}],
            "metric_views": list(metric_views or []),
            "functions": [],
        },
        "instructions": {"text_instructions": [], "example_question_sqls": []},
    }


def _row(
    question_id: str,
    assessment: str,
    *,
    sql: str | None = None,
    expected: str | None = None,
) -> dict[str, Any]:
    row = {
        "question_id": question_id,
        "assessment": assessment,
        "needs_review": assessment == "NEEDS_REVIEW",
    }
    if sql is not None:
        row["generated_sql"] = sql
    if expected is not None:
        row["expected_sql"] = expected
    return row


def _baseline_output(rows: list[dict[str, Any]], *, failed: bool = False) -> dict[str, Any]:
    """The loop's iteration-0 eval-output dict, in the shape the loop holds it."""
    return {
        "eval_run_id": "eval-baseline",
        "eval_run_status": "DONE",
        "eval_run_failed": failed,
        "total_questions": len(rows),
        "correct_count": sum(1 for r in rows if r["assessment"] == "GOOD"),
        "num_done": len(rows),
        "num_needs_review": 0,
        "rows": rows,
        "overall_accuracy": 0.0,
    }


class _FakeRunner:
    """The post-attach full-suite eval (MV-D114 d3): records calls, answers rows."""

    def __init__(
        self, rows: list[dict[str, Any]], *, status: str = "DONE",
        raises: BaseException | None = None,
    ) -> None:
        self.rows = rows
        self.status = status
        self.raises = raises
        self.calls: list[str] = []

    def __call__(self) -> dict[str, Any]:
        self.calls.append("full")
        if self.raises is not None:
            raise self.raises
        out = _baseline_output(self.rows, failed=self.status != "DONE")
        good = sum(1 for r in self.rows if r["assessment"] == "GOOD")
        out.update(
            eval_run_id="eval-lift",
            eval_run_status=self.status,
            overall_accuracy=(100.0 * good / len(self.rows)) if self.rows else 0.0,
        )
        return out


def _seed(
    spark: FakeDeltaSpark,
    *,
    verdict: str = "SUFFICIENT",
    reverified: bool = True,
    created_by: str = USER,
    status: str = "CREATED",
    full_name: str = MV_NAME,
    benchmark_questions: list[str] | None = AFFECTED,
    provenance: str | None = None,
    dedup_fingerprint: str = "fp-1",
    evidence: dict[str, Any] | None = None,
) -> None:
    """Seed consent, created-object and candidate rows through the real writers."""
    mv_state.upsert_mv_consent(
        spark,
        catalog=CATALOG,
        schema=SCHEMA,
        probe_id=PROBE_ID,
        granted_by=USER,
        target_catalog="main",
        target_schema="sales",
        verdict=verdict,
        run_id=RUN_ID,
    )
    if reverified:
        mv_state.mark_mv_consent_reverified(
            spark, catalog=CATALOG, schema=SCHEMA, probe_id=PROBE_ID, run_id=RUN_ID,
        )
    mv_state.upsert_mv_created_object(
        spark,
        catalog=CATALOG,
        schema=SCHEMA,
        run_id=RUN_ID,
        suggestion_id=SUGGESTION_ID,
        full_name=full_name,
        created_by=created_by,
        status=status,
        provenance=provenance,
    )
    if evidence is not None or benchmark_questions is not None:
        mv_state.upsert_mv_candidate(
            spark,
            catalog=CATALOG,
            schema=SCHEMA,
            run_id=RUN_ID,
            target_space_id=SPACE_ID,
            suggestion_id=SUGGESTION_ID,
            dedup_fingerprint=dedup_fingerprint,
            candidate_type="NEW_METRIC_VIEW",
            evidence=(
                evidence
                if evidence is not None
                else {"benchmark_questions": list(benchmark_questions or [])}
            ),
        )


def _created_row(spark: FakeDeltaSpark) -> dict[str, Any]:
    return next(
        row for row in spark.rows if row.get("full_name") is not None
    )


def _run_phase(
    spark: FakeDeltaSpark,
    *,
    config: dict[str, Any],
    baseline: dict[str, Any],
    runner: Any,
    attach_views: Any = f'["{MV_NAME}"]',
    consent_probe_id: str = PROBE_ID,
) -> mv_attach.AttachOutcome:
    return mv_attach.run_mv_attach_phase(
        spark,
        run_id=RUN_ID,
        space_id=SPACE_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        attach_views=attach_views,
        consent_probe_id=consent_probe_id,
        config=config,
        baseline_eval=baseline,
        w=None,
        post_attach_eval=runner,
    )


# ── The consent chain: attach is not an LLM lever (MV-D16(a)) ────────────


def test_the_attach_type_is_registered_but_not_an_llm_lever() -> None:
    assert "mv_attach_data_source" in PATCH_TYPES
    assert "mv_attach_data_source" in HIGH_RISK_PATCHES
    assert applier.classify_risk("mv_attach_data_source") == "high"
    # The frozenset is the LLM-PROPOSAL surface, not the lever surface.
    # apply_patch_set never reads it, and the attach phase calls that directly.
    assert "mv_attach_data_source" not in unified_loop._ALLOWED_PATCH_TYPES


def test_an_llm_proposed_attach_patch_is_dropped() -> None:
    """The assertion protecting the consent chain.

    An LLM that proposes an attach has invented a UC identifier: there is no
    consent row for it and no ``genie_opt_mv_created_objects`` entry. If someone
    adds the type to the allowlist, this fails.
    """
    lever, _rationale, patches = unified_loop._normalize_llm_patches(
        {
            "lever": 2,
            "rationale": "attach a metric view",
            "patches": [
                {
                    "type": "mv_attach_data_source",
                    "target": "main.sales.mv_invented",
                    "new_text": "main.sales.mv_invented",
                },
                {
                    "type": "update_description",
                    "target": "main.sales.fact_orders",
                    "new_text": "Order facts.",
                },
            ],
        },
        allowed_levers=[1, 2],
    )
    assert lever == 2
    assert [p["type"] for p in patches] == ["update_description"]


# ── The applier action ───────────────────────────────────────────────────


def test_apply_puts_the_identifier_on_the_space_config() -> None:
    config = _config()
    apply_log = applier.apply_patch_set(
        None,
        SPACE_ID,
        mv_attach._attach_patches([MV_NAME]),
        config,
        force_apply=True,
    )
    assert apply_log["patch_deployed"] is True
    identifiers = [
        mv["identifier"]
        for mv in apply_log["post_snapshot"]["data_sources"]["metric_views"]
    ]
    assert identifiers == [MV_NAME]
    assert apply_log["pre_snapshot"]["data_sources"]["metric_views"] == []


def test_snapshot_revert_removes_the_identifier() -> None:
    """Detach is a whole-snapshot revert through the applier, not revert.py."""
    config = _config()
    apply_log = applier.apply_patch_set(
        None, SPACE_ID, mv_attach._attach_patches([MV_NAME]), config, force_apply=True,
    )
    result = applier.rollback(apply_log, None, SPACE_ID)
    assert result["restored_config"]["data_sources"]["metric_views"] == []


def test_render_patch_output_is_reviewable() -> None:
    rendered = applier.render_patch(
        mv_attach._attach_patches([MV_NAME])[0], SPACE_ID, _config(),
    )
    command = json.loads(rendered["command"])
    rollback_command = json.loads(rendered["rollback_command"])
    assert command == {
        "op": "add",
        "section": "metric_views",
        "asset": {"identifier": MV_NAME},
    }
    assert rollback_command == {
        "op": "remove",
        "section": "metric_views",
        "identifier": MV_NAME,
    }
    assert rendered["risk_level"] == "high"


def test_an_attach_without_an_identifier_is_refused() -> None:
    with pytest.raises(RuntimeError, match="identifier"):
        applier.render_patch(
            {"type": "mv_attach_data_source", "lever": 2}, SPACE_ID, _config(),
        )


def test_high_risk_means_the_attach_is_queued_unless_forced() -> None:
    """Documents why the phase passes force_apply: the consent row is the approval."""
    apply_log = applier.apply_patch_set(
        None, SPACE_ID, mv_attach._attach_patches([MV_NAME]), _config(),
    )
    assert apply_log["applied"] == []
    assert [entry["patch"]["type"] for entry in apply_log["queued_high"]] == [
        "mv_attach_data_source"
    ]


def test_attaching_an_already_attached_view_is_a_no_op() -> None:
    config = _config([{"identifier": MV_NAME}])
    apply_log = applier.apply_patch_set(
        None, SPACE_ID, mv_attach._attach_patches([MV_NAME]), config, force_apply=True,
    )
    assert apply_log["applied"] == []
    assert len(config["data_sources"]["metric_views"]) == 1


def test_the_attach_applies_to_the_config_whatever_the_apply_mode() -> None:
    """A uc_artifact apply_mode must not route an attach away from the config.

    There is no UC-side expression of "this space may query this view", so
    resolving the scope by lever would silently apply nothing.
    """
    config = _config()
    apply_log = applier.apply_patch_set(
        None,
        SPACE_ID,
        mv_attach._attach_patches([MV_NAME]),
        config,
        apply_mode="uc_artifact",
        force_apply=True,
    )
    assert [
        mv["identifier"]
        for mv in apply_log["post_snapshot"]["data_sources"]["metric_views"]
    ] == [MV_NAME]


# ── The update_mv_yaml validation gate (#331) ────────────────────────────


def _valid_mv_yaml() -> str:
    profiling = MvProfiling(
        source_table="main.sales.fact_orders",
        table_columns={
            "main.sales.fact_orders": (
                ColumnFacts(name="order_id"),
                ColumnFacts(name="net_revenue"),
            ),
        },
        measures=(
            MeasureRequest(
                name="total_revenue",
                expr="SUM(net_revenue)",
                comment="Net revenue after discounts.",
            ),
        ),
        domain="sales",
    )
    candidate = MetricViewCandidate(
        space_id=SPACE_ID,
        concept="revenue",
        measure_expr="SUM(net_revenue)",
        source_tables=("main.sales.fact_orders",),
        benchmark_question_ids=("rev_001",),
    )
    generated = generate(candidate, profiling)
    assert generated.yaml_text, "generation must produce YAML for this fixture"
    return generated.yaml_text


def test_update_mv_yaml_refuses_yaml_that_fails_validation() -> None:
    with pytest.raises(RuntimeError, match="failed validation"):
        applier.render_patch(
            {
                "type": "update_mv_yaml",
                "target": MV_NAME,
                "new_text": "version: '0.0'\nnot_a_metric_view: true\n",
            },
            SPACE_ID,
            _config(),
        )


def test_update_mv_yaml_accepts_engine_valid_yaml() -> None:
    rendered = applier.render_patch(
        {"type": "update_mv_yaml", "target": MV_NAME, "new_text": _valid_mv_yaml()},
        SPACE_ID,
        _config(),
    )
    assert json.loads(rendered["command"])["section"] == "mv_yaml"


def test_a_failing_update_mv_yaml_is_dropped_without_aborting_the_patch_set() -> None:
    config = _config()
    apply_log = applier.apply_patch_set(
        None,
        SPACE_ID,
        [
            {"type": "update_mv_yaml", "target": MV_NAME, "new_text": "not: yaml: at all: ["},
            {
                "type": "update_description",
                "target": "main.sales.fact_orders",
                "new_text": "Order facts.",
                "lever": 1,
            },
        ],
        config,
    )
    assert [entry["patch"]["type"] for entry in apply_log["applied"]] == [
        "update_description"
    ]
    dropped = apply_log["dropped_patches"]
    assert [p["type"] for p in dropped] == ["update_mv_yaml"]
    assert dropped[0]["drop_reason"] == "validation_missing"


# ── The phase: happy path ────────────────────────────────────────────────


def test_a_positive_lift_keeps_the_attach_and_persists_the_report() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.status == mv_attach.STATUS_COMPLETE
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED
    assert outcome.attached == (MV_NAME,)
    assert outcome.delta_affected == pytest.approx(0.5)
    assert outcome.config["data_sources"]["metric_views"] == [{"identifier": MV_NAME}]

    row = _created_row(spark)
    assert row["status"] == "ATTACHED"
    assert row["baseline_eval_run_id"] == "eval-baseline"
    assert row["post_attach_eval_run_id"] == "eval-lift"
    assert row["attach_patch_id"] == f"{RUN_ID}:0:2:0"


def test_the_verdict_scores_the_affected_subset_of_a_full_suite_eval() -> None:
    """One full post-attach eval, the verdict on the affected subset (MV-D114 d3/d4)."""
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=["rev_002"])
    baseline = _baseline_output(
        [_row("rev_001", "GOOD"), _row("rev_002", "BAD"), _row("rev_003", "GOOD")]
    )
    runner = _FakeRunner(
        [_row("rev_001", "BAD"), _row("rev_002", "GOOD"), _row("rev_003", "GOOD")]
    )

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert runner.calls == ["full"]
    assert outcome.delta_affected == pytest.approx(1.0)
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED
    report = json.loads(_created_row(spark)["lift_report_json"])
    assert report["graded_suite_count"] == 3


def test_a_wash_with_a_regression_outside_the_subset_detaches_on_the_suite_loss() -> None:
    """d4: the regression outside the subset is not a subset regression, but it is
    a net suite loss, and MV-D118 (net_suite) detaches on that."""
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=["rev_001"])
    baseline = _baseline_output([_row("rev_001", "GOOD"), _row("rev_009", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_009", "BAD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.delta_affected == pytest.approx(0.0)
    assert outcome.delta_suite < 0
    assert outcome.verdict == mv_attach.VERDICT_DETACHED
    assert outcome.regressed_question_count == 0


def test_zero_graded_affected_rows_reverts_and_leaves_the_object_created() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)  # AFFECTED = rev_001, rev_002
    baseline = _baseline_output(
        [_row("rev_001", "BAD"), _row("rev_002", "GOOD"), _row("rev_003", "GOOD")]
    )
    runner = _FakeRunner([
        _row("rev_001", "NEEDS_REVIEW"),
        _row("rev_002", "NEEDS_REVIEW"),
        _row("rev_003", "GOOD"),
    ])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.status == mv_attach.STATUS_SKIPPED
    assert outcome.skip_reason == mv_attach.SKIP_LIFT_NOT_GRADED
    assert outcome.graded_affected_count == 0
    assert outcome.verdict is None
    assert outcome.config["data_sources"]["metric_views"] == []
    row = _created_row(spark)
    assert row["status"] == "CREATED"
    assert json.loads(row["lift_report_json"])["graded_affected_count"] == 0


def test_a_kept_attach_hands_back_the_post_attach_eval_as_the_new_baseline() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.verdict == mv_attach.VERDICT_ATTACHED
    assert outcome.post_attach_eval["eval_run_id"] == "eval-lift"
    assert outcome.post_attach_accuracy == pytest.approx(100.0)
    assert outcome.graded_affected_count == 2
    assert "post_attach_eval" not in outcome.detail()  # no rows in a stage row
    assert "post_attach_eval" not in repr(outcome)
    assert outcome.detail()["post_attach_accuracy"] == pytest.approx(100.0)
    assert outcome.detail()["graded_affected_count"] == 2


@pytest.mark.parametrize("post_rows", [
    [_row("rev_001", "BAD"), _row("rev_002", "GOOD")],  # regression → detached
    [_row("rev_001", "NEEDS_REVIEW"), _row("rev_002", "NEEDS_REVIEW")],  # zero graded
])
def test_only_a_kept_attach_resets_the_baseline(post_rows) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(
        spark, config=_config(), baseline=baseline, runner=_FakeRunner(post_rows),
    )
    assert outcome.post_attach_eval is None


def test_the_phase_no_longer_calls_run_subset() -> None:
    import inspect

    assert "run_subset" not in inspect.getsource(mv_attach)
    assert not hasattr(mv_attach, "LIFT_EVAL_LABEL")


def test_lift_report_json_round_trips_to_dict_byte_identically() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    pre_rows = [_row("rev_001", "BAD"), _row("rev_002", "GOOD")]
    post_rows = [_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]
    baseline = _baseline_output(pre_rows)
    runner = _FakeRunner(post_rows)

    _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    expected = lift_report(
        mv_attach._eval_result_from_output(baseline),
        mv_attach._eval_result_from_output(_FakeRunner(post_rows)()),
        AFFECTED,
    ).to_dict()
    stored = _created_row(spark)["lift_report_json"]
    assert stored == json.dumps(expected, default=str)
    assert json.loads(stored) == expected


def test_lift_report_json_is_a_declared_column() -> None:
    """MV-D7 deferred this column; the migration and the DDL must agree."""
    from genie_space_optimizer.optimization.ddl import (
        ADDITIVE_COLUMN_MIGRATIONS,
        _ALL_DDL,
    )

    assert "lift_report_json" in _ALL_DDL[TABLE_MV_CREATED_OBJECTS]
    assert (TABLE_MV_CREATED_OBJECTS, "lift_report_json") in {
        (table, column) for table, column, _ddl in ADDITIVE_COLUMN_MIGRATIONS
    }


# ── The phase: regression detaches, never drops ──────────────────────────


def test_a_regression_detaches_and_never_drops_the_object() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.verdict == mv_attach.VERDICT_DETACHED
    assert outcome.attached == ()
    assert outcome.detached == (MV_NAME,)
    assert outcome.config["data_sources"]["metric_views"] == []

    row = _created_row(spark)
    assert row["status"] == "DETACHED"
    assert row["full_name"] == MV_NAME  # the UC object is untouched
    assert json.loads(row["lift_report_json"])["delta_affected"] < 0
    assert row["on_regression_action"] == "DETACH_ONLY_NEVER_DROP"


def test_a_wash_with_broken_questions_is_treated_as_a_regression() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "GOOD"), _row("rev_002", "BAD")])
    runner = _FakeRunner([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.delta_affected == pytest.approx(0.0)
    assert outcome.verdict == mv_attach.VERDICT_DETACHED


def test_an_ungradeable_lift_eval_reverts_and_leaves_the_object_created() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([], status="EVALUATION_FAILED")

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.skip_reason == mv_attach.SKIP_LIFT_EVAL_UNUSABLE
    assert outcome.config["data_sources"]["metric_views"] == []
    assert _created_row(spark)["status"] == "CREATED"


# ── The phase: a failure after the attach deployed reverts (MV-D114 d6) ──


def _stage_statements(spark: FakeDeltaSpark, stage: str) -> list[str]:
    """The exact INSERT ``state.write_stage`` emits, not its duration lookup."""
    return [
        s for s in spark.statements
        if s.startswith(f"INSERT INTO {CATALOG}.{SCHEMA}.genie_opt_stages ")
        and f"'{stage}'" in s
    ]


def _spy_rollback(monkeypatch, *, fail_times: int = 0) -> list[str]:
    real = mv_attach.rollback
    calls: list[str] = []

    def spy(apply_log, w, space_id):
        calls.append(space_id)
        if len(calls) <= fail_times:
            return {
                "status": "error",
                "errors": ["Failed to apply rollback via API"],
                "restored_config": apply_log.get("pre_snapshot"),
            }
        return real(apply_log, w, space_id)

    monkeypatch.setattr(mv_attach, "rollback", spy)
    return calls


def test_an_exception_after_the_attach_is_rolled_back_and_recorded(monkeypatch) -> None:
    """M4 exit criterion: raise inside the lift after deploy."""
    spark = FakeDeltaSpark()
    _seed(spark)
    calls = _spy_rollback(monkeypatch)
    runner = _FakeRunner([], raises=RuntimeError("eval service exploded"))

    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=runner,
    )

    assert calls == [SPACE_ID]
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.error == "RuntimeError"
    assert outcome.rollback_status == mv_attach.ROLLBACK_REVERTED
    assert outcome.attached == ()
    assert outcome.config["data_sources"]["metric_views"] == []
    assert _created_row(spark)["status"] == "CREATED"
    stage = _stage_statements(spark, "MV_ATTACH")
    assert stage and "'FAILED'" in stage[-1]
    assert '"rollback_status": "reverted"' in stage[-1]
    assert "eval service exploded" not in stage[-1]


@pytest.mark.parametrize("where", ["lift_report", "update_iteration_observed_config"])
def test_any_failure_after_deploy_reverts(monkeypatch, where: str) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    calls = _spy_rollback(monkeypatch)

    def boom(*_a, **_k):
        raise RuntimeError(f"{where} failed")

    monkeypatch.setattr(mv_attach, where, boom)
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
    )

    assert calls == [SPACE_ID]
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.config["data_sources"]["metric_views"] == []
    # Even after the kept path wrote ATTACHED, the revert writes CREATED back.
    assert _created_row(spark)["status"] == "CREATED"
    assert outcome.post_attach_eval is None


def test_an_interrupt_after_deploy_still_reverts_then_propagates(monkeypatch) -> None:
    """The finally clause, not only the except: a BaseException also reverts."""
    spark = FakeDeltaSpark()
    _seed(spark)
    calls = _spy_rollback(monkeypatch)

    with pytest.raises(KeyboardInterrupt):
        _run_phase(
            spark, config=_config(),
            baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
            runner=_FakeRunner([], raises=KeyboardInterrupt()),
        )
    assert calls == [SPACE_ID]


def test_an_interrupt_after_the_attached_write_reverts_and_demotes_the_row(
    monkeypatch,
) -> None:
    """An interrupt after the kept path wrote ATTACHED must not leave that claim."""
    spark = FakeDeltaSpark()
    _seed(spark)
    calls = _spy_rollback(monkeypatch)

    def interrupt(*_a, **_k):
        raise KeyboardInterrupt()

    monkeypatch.setattr(mv_attach, "update_iteration_observed_config", interrupt)
    with pytest.raises(KeyboardInterrupt):
        _run_phase(
            spark, config=_config(),
            baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
            runner=_FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
        )
    assert calls == [SPACE_ID]
    assert _created_row(spark)["status"] == "CREATED"


def test_a_revert_that_fails_once_is_retried(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    calls = _spy_rollback(monkeypatch, fail_times=1)
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([], raises=RuntimeError("boom")),
    )
    assert calls == [SPACE_ID, SPACE_ID]
    assert outcome.rollback_status == mv_attach.ROLLBACK_REVERTED


def test_a_revert_that_fails_twice_is_reported_never_recorded_detached(monkeypatch) -> None:
    """d6: say the view may still be live; carry pre-attach; never write DETACHED."""
    spark = FakeDeltaSpark()
    _seed(spark)
    calls = _spy_rollback(monkeypatch, fail_times=2)
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),  # regression
    )
    assert calls == [SPACE_ID, SPACE_ID]
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.verdict == mv_attach.VERDICT_DETACHED  # the lift's decision stands
    assert outcome.detached == ()  # but nothing is claimed detached
    assert outcome.rollback_status == mv_attach.ROLLBACK_FAILED
    assert "Failed to apply rollback via API" in (outcome.rollback_error or "")
    assert (outcome.error or "").startswith("ROLLBACK_FAILED")
    assert outcome.config["data_sources"]["metric_views"] == []
    assert _created_row(spark)["status"] == "CREATED"


def test_a_failure_before_deploy_does_not_revert(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    calls = _spy_rollback(monkeypatch)
    monkeypatch.setattr(
        mv_attach, "load_mv_consent",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("consent unreadable")),
    )
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert calls == []
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.rollback_status is None


# ── MV-D118: no exception text leaves the phase; a late failure keeps its lift ──


_SENTINEL = "secret_literal"


def test_a_post_deploy_exception_persists_its_type_only(monkeypatch, caplog) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    _spy_rollback(monkeypatch)
    with caplog.at_level("DEBUG"):
        outcome = _run_phase(
            spark, config=_config(),
            baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
            runner=_FakeRunner([], raises=RuntimeError(f"SELECT {_SENTINEL} FROM t")),
        )
    assert outcome.error == "RuntimeError"
    assert _SENTINEL not in caplog.text
    assert not [s for s in spark.statements if _SENTINEL in s]


def test_a_raised_revert_is_recorded_by_type(monkeypatch, caplog) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)

    def raising(*_a, **_k):
        raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(mv_attach, "rollback", raising)
    with caplog.at_level("DEBUG"):
        outcome = _run_phase(
            spark, config=_config(),
            baseline=_baseline_output([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
            runner=_FakeRunner([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        )
    assert outcome.rollback_status == mv_attach.ROLLBACK_FAILED
    assert outcome.rollback_error == "RuntimeError"
    assert _SENTINEL not in (outcome.error or "")
    assert _SENTINEL not in caplog.text
    assert not [s for s in spark.statements if _SENTINEL in s]


def test_a_pre_deploy_exception_persists_its_type_only(monkeypatch, caplog) -> None:
    spark = FakeDeltaSpark()
    monkeypatch.setattr(
        mv_attach, "load_mv_consent",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError(_SENTINEL)),
    )
    with caplog.at_level("DEBUG"):
        outcome = _run_phase(
            spark, config=_config(),
            baseline=_baseline_output([_row("rev_001", "BAD")]),
            runner=_FakeRunner([]),
        )
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.error == "RuntimeError"
    assert _SENTINEL not in caplog.text


def test_a_failure_after_the_measurement_keeps_the_measured_fields(monkeypatch) -> None:
    """m-2: the lift was measured before the failure, so the outcome says so."""
    spark = FakeDeltaSpark()
    _seed(spark)
    _spy_rollback(monkeypatch)
    monkeypatch.setattr(
        mv_attach, "update_iteration_observed_config",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("late")),
    )
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
    )
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.error == "RuntimeError"
    assert outcome.verdict is None
    assert outcome.rollback_status == mv_attach.ROLLBACK_REVERTED
    assert outcome.lift_eval_run_id == "eval-lift"
    assert outcome.graded_affected_count == 2
    assert outcome.delta_affected is not None and outcome.delta_affected > 0
    assert outcome.delta_suite is not None
    assert json.loads(_created_row(spark)["lift_report_json"])["question_subset"] == AFFECTED


# ── The phase: every verify-before-attach mismatch is a recorded skip ────


def test_a_missing_consent_row_is_a_recorded_skip() -> None:
    spark = FakeDeltaSpark()
    baseline = _baseline_output([_row("rev_001", "BAD")])
    runner = _FakeRunner([_row("rev_001", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.status == mv_attach.STATUS_SKIPPED
    assert outcome.skip_reason == mv_attach.SKIP_NO_CONSENT_ROW
    assert outcome.config["data_sources"]["metric_views"] == []
    assert runner.calls == []


def test_an_insufficient_consent_is_a_recorded_skip() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, verdict="INSUFFICIENT")
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_CONSENT_NOT_SUFFICIENT


def test_a_consent_never_reverified_at_trigger_is_a_recorded_skip() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, reverified=False)
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_CONSENT_NOT_REVERIFIED


def test_an_identifier_with_no_created_row_is_a_recorded_skip() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, full_name="main.sales.mv_something_else")
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_NO_CREATED_OBJECT


def test_a_creator_who_is_not_the_consenting_user_is_a_recorded_skip() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, created_by="someone.else@example.com")
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_CREATOR_MISMATCH


def test_a_user_created_row_bypasses_the_creator_mismatch_guard() -> None:
    # MV-D24: a USER_CREATED (bring-your-own) row is a verified registration, so
    # its created_by need not match the consent's granted_by — that verification
    # IS the consent coverage the guard exists to require. The same row that
    # skips as OBO_CREATED must NOT skip once it is USER_CREATED.
    spark = FakeDeltaSpark()
    _seed(
        spark,
        created_by="someone.else@example.com",
        provenance="USER_CREATED",
    )
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason != mv_attach.SKIP_CREATOR_MISMATCH


def test_an_object_not_in_created_status_is_a_recorded_skip() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_NO_CREATED_OBJECT


def test_a_failed_baseline_eval_blocks_the_attach() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")], failed=True),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_BASELINE_UNUSABLE


def test_a_proposal_with_no_recorded_questions_blocks_the_attach() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=[])
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_NO_AFFECTED_QUESTIONS


def test_no_parameters_skips_the_phase_at_zero_cost() -> None:
    spark = FakeDeltaSpark()
    runner = _FakeRunner([])
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]),
        runner=runner,
        attach_views="",
        consent_probe_id="",
    )
    assert outcome.skip_reason == mv_attach.SKIP_NOT_REQUESTED
    assert runner.calls == []


def test_a_phase_exception_is_swallowed_and_recorded() -> None:
    spark = FakeDeltaSpark()

    def boom(*_args, **_kwargs):
        raise RuntimeError("consent table unreadable")

    original = mv_attach.load_mv_consent
    mv_attach.load_mv_consent = boom  # type: ignore[assignment]
    try:
        config = _config()
        outcome = _run_phase(
            spark,
            config=config,
            baseline=_baseline_output([_row("rev_001", "BAD")]),
            runner=_FakeRunner([]),
        )
    finally:
        mv_attach.load_mv_consent = original  # type: ignore[assignment]

    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.error == "RuntimeError"
    assert outcome.config is config


def test_parse_attach_views_ignores_a_malformed_parameter() -> None:
    assert mv_attach.parse_attach_views('{"not": "a list"}') == []
    assert mv_attach.parse_attach_views(None) == []
    assert mv_attach.parse_attach_views([MV_NAME, MV_NAME]) == [MV_NAME]


# ── MV-D114 d1/d2: the lift measures live benchmark questions the view affects ──

REVENUE_SQL = "SELECT SUM(amount) FROM main.sales.fact_orders"
OTHER_SQL = "SELECT COUNT(DISTINCT customer_id) FROM main.sales.dim_customer"


def _revenue_fingerprint() -> str:
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures

    measures = extract_measures(REVENUE_SQL)
    # Positive control: the fixture must actually extract a resolved measure,
    # or every SQL-derived assertion below is vacuous.
    assert measures and measures[0].canonical_expr
    assert measures[0].source_tables == ("main.sales.fact_orders",)
    return mv_state.mv_candidate_fingerprint(
        SPACE_ID, measures[0].canonical_expr, measures[0].source_tables,
    )


def test_a_bundle_measures_every_member_not_just_the_anchor() -> None:
    spark = FakeDeltaSpark()
    _seed(
        spark,
        evidence={
            "bundle": True,
            "benchmark_questions": ["rev_001"],  # the anchor's, inherited
            "benchmark_question_ids": ["rev_001", "rev_002"],  # the member union
            "measures": [
                {"dedup_fingerprint": "fp-a", "benchmark_question_ids": ["rev_001"]},
                {"dedup_fingerprint": "fp-b", "benchmark_question_ids": ["rev_002"]},
            ],
        },
    )
    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "BAD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.affected_question_count == 2


def test_recorded_ids_that_are_not_live_benchmark_questions_are_dropped() -> None:
    spark = FakeDeltaSpark()
    _seed(
        spark,
        benchmark_questions=[
            "rev_001",
            "trusted_asset:ta-1",
            "sql_snippet:measures:s-1",
            "gso_patch:0:2:0",
            "rev_999",  # a benchmark question retired since the proposing run
        ],
    )
    baseline = _baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.affected_question_count == 1
    report = json.loads(_created_row(spark)["lift_report_json"])
    assert report["question_subset"] == ["rev_001"]


def test_a_curated_candidate_is_measured_on_baseline_rows_using_its_measure() -> None:
    """An IQ-scan approval carries only curated provenance (mv_suggest.py:176)."""
    spark = FakeDeltaSpark()
    _seed(
        spark,
        dedup_fingerprint=_revenue_fingerprint(),
        benchmark_questions=["sql_snippet:measures:s-1"],
    )
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql=REVENUE_SQL),
        _row("rev_002", "BAD", sql=OTHER_SQL),
    ])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "BAD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.affected_question_count == 1
    assert json.loads(_created_row(spark)["lift_report_json"])["question_subset"] == ["rev_001"]


def test_expected_sql_selects_a_question_genie_answered_without_the_measure() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint=_revenue_fingerprint(), benchmark_questions=[])
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql=OTHER_SQL, expected=REVENUE_SQL),
        _row("rev_002", "GOOD", sql=OTHER_SQL, expected=OTHER_SQL),
    ])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.affected_question_count == 1
    assert json.loads(_created_row(spark)["lift_report_json"])["question_subset"] == ["rev_001"]


def test_a_bundle_member_fingerprint_selects_rows_too() -> None:
    spark = FakeDeltaSpark()
    _seed(
        spark,
        dedup_fingerprint="bundle-fp",
        evidence={
            "bundle": True,
            "benchmark_question_ids": [],
            "measures": [{"dedup_fingerprint": _revenue_fingerprint(), "benchmark_question_ids": []}],
        },
    )
    baseline = _baseline_output([_row("rev_001", "BAD", sql=REVENUE_SQL), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.affected_question_count == 1


UNQUALIFIED_REVENUE_SQL = "SELECT SUM(amount) FROM fact_orders"


def _revenue_bundle_evidence() -> dict[str, Any]:
    return {
        "bundle": True,
        "benchmark_question_ids": [],
        "source_tables": ["main.sales.fact_orders"],
        "measures": [{"dedup_fingerprint": _revenue_fingerprint(), "benchmark_question_ids": []}],
    }


def test_unqualified_sql_over_the_candidates_table_selects_the_row() -> None:
    """m-4: the unqualified table resolves through the space to the full name the key is over."""
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=_revenue_bundle_evidence())
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql=UNQUALIFIED_REVENUE_SQL), _row("rev_002", "GOOD"),
    ])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.affected_question_count == 1


def test_the_same_expression_over_another_table_does_not_select() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=_revenue_bundle_evidence())
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql="SELECT SUM(amount) FROM main.sales.fact_returns"),
    ])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=_FakeRunner([]))
    assert outcome.skip_reason == mv_attach.SKIP_NO_AFFECTED_QUESTIONS


def test_without_candidate_tables_an_unqualified_statement_still_selects() -> None:
    """A legacy row records no ``source_tables``; the statement's table resolves through the space (MV-D123)."""
    spark = FakeDeltaSpark()
    evidence = _revenue_bundle_evidence()
    del evidence["source_tables"]
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=evidence)
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql=UNQUALIFIED_REVENUE_SQL), _row("rev_002", "GOOD"),
    ])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.affected_question_count == 1


# ── MV-D123: the statement's tables resolve against the space's own identifiers ──

FACT_ORDERS = "main.sales.fact_orders"
FACT_RETURNS = "main.sales.fact_returns"


def _space_config(*tables: str) -> dict[str, Any]:
    config = _config()
    config["data_sources"]["tables"] = [{"identifier": t} for t in tables]
    return config


def _amount_fingerprint(table: str) -> str:
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures

    (measure,) = extract_measures(f"SELECT SUM(amount) FROM {table}")
    assert measure.source_tables == (table,)
    return mv_state.mv_candidate_fingerprint(SPACE_ID, measure.canonical_expr, (table,))


def _member_evidence(member_fp: str) -> dict[str, Any]:
    """A bundle row as a legacy writer left it: member keys, no ``source_tables``."""
    return {
        "bundle": True,
        "benchmark_question_ids": [],
        "measures": [{"dedup_fingerprint": member_fp, "benchmark_question_ids": []}],
    }


def test_an_unqualified_statement_selects_a_conflict_row() -> None:
    """MV-D118's reach residual: a CONFLICT row records no ``source_tables``.

    Its key is MV-D7 over the full name, and ``FROM fact_orders`` resolves to that
    name through the space, so the recomputed key is the row's own.
    """
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=None)
    mv_state.upsert_mv_candidate(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID, target_space_id=SPACE_ID,
        suggestion_id=SUGGESTION_ID, dedup_fingerprint=_amount_fingerprint(FACT_ORDERS),
        candidate_type="CONFLICT",
        evidence={"benchmark_questions": [], "lineage_source_tables": [FACT_ORDERS]},
    )
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql=UNQUALIFIED_REVENUE_SQL), _row("rev_002", "GOOD"),
    ])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.affected_question_count == 1
    assert json.loads(_created_row(spark)["lift_report_json"])["question_subset"] == ["rev_001"]


def test_a_same_leaf_table_in_another_catalog_does_not_select() -> None:
    """Two catalogs are two tables, and a leaf both catalogs carry resolves to neither."""
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=_revenue_bundle_evidence())
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql="SELECT SUM(amount) FROM other.sales.fact_orders"),
        _row("rev_002", "BAD", sql=UNQUALIFIED_REVENUE_SQL),
    ])
    outcome = _run_phase(
        spark, config=_space_config(FACT_ORDERS, "other.sales.fact_orders"),
        baseline=baseline, runner=_FakeRunner([]),
    )
    assert outcome.skip_reason == mv_attach.SKIP_NO_AFFECTED_QUESTIONS


def test_a_bare_no_from_expression_selects_a_single_table_candidate() -> None:
    """MV-D118 Ruling 6, kept by MV-D123 Ruling 2: a table-less statement joins its calculation's one table."""
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=_member_evidence(_revenue_fingerprint()))
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql="SELECT SUM(amount)"), _row("rev_002", "GOOD"),
    ])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(
        spark, config=_space_config(FACT_ORDERS, FACT_RETURNS), baseline=baseline, runner=runner,
    )
    assert outcome.affected_question_count == 1


def test_a_bare_no_from_expression_does_not_select_when_the_calculation_has_two_tables() -> None:
    """Ruling 2: with the calculation on two of the views' tables, a table-less statement joins neither."""
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=_revenue_bundle_evidence())
    mv_state.upsert_mv_created_object(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID,
        suggestion_id=SUGGESTION_ORDERS, full_name=MV_ORDERS, created_by=USER,
        status="CREATED",
    )
    mv_state.upsert_mv_candidate(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID, target_space_id=SPACE_ID,
        suggestion_id=SUGGESTION_ORDERS, dedup_fingerprint="bundle-fp-2",
        candidate_type="NEW_METRIC_VIEW",
        evidence={
            **_member_evidence(_amount_fingerprint(FACT_RETURNS)),
            "source_tables": [FACT_RETURNS],
        },
    )
    baseline = _baseline_output([
        _row("rev_001", "BAD", sql="SELECT SUM(amount)"),
        # Positive control: each view's qualified statement still selects on its own.
        _row("rev_002", "BAD", sql=REVENUE_SQL),
        _row("rev_003", "BAD", sql=f"SELECT SUM(amount) FROM {FACT_RETURNS}"),
    ])
    runner = _FakeRunner([_row(q, "GOOD") for q in ("rev_001", "rev_002", "rev_003")])
    _run_phase(
        spark, config=_space_config(FACT_ORDERS, FACT_RETURNS), baseline=baseline,
        runner=runner, attach_views=json.dumps([MV_NAME, MV_ORDERS]),
    )
    assert json.loads(_created_row(spark)["lift_report_json"])["question_subset"] == [
        "rev_002", "rev_003",
    ]


@pytest.mark.parametrize("sql", [
    REVENUE_SQL,
    "SELECT SUM(amount) FROM `Main`.`Sales`.`Fact_Orders`",
    "SELECT SUM(amount) FROM sales.fact_orders",
])
def test_v1_keyed_member_fingerprints_still_match_qualified_statements(sql: str) -> None:
    """Ruling 1: a v1 key over one three-part table is its v2 key, so an approved row keeps matching."""
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures
    from genie_space_optimizer.optimization.mv_identity_v1 import v1_member_keys

    v1_key = v1_member_keys(SPACE_ID, {0: extract_measures(REVENUE_SQL)[0]})[0]
    assert v1_key == _amount_fingerprint(FACT_ORDERS)
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint="bundle-fp", evidence=_member_evidence(v1_key))
    baseline = _baseline_output([_row("rev_001", "BAD", sql=sql), _row("rev_002", "GOOD")])
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.affected_question_count == 1


# ── MV-D123 Ruling 9: an approved v1-keyed bundle beside its v2 successor ──

_ORDERS_VIEW = "main.sales.fact_orders_metrics"


def _orders_member_key(aggregate: str) -> str:
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures

    (measure,) = extract_measures(f"SELECT {aggregate} FROM {FACT_ORDERS}")
    return mv_state.mv_candidate_fingerprint(SPACE_ID, measure.canonical_expr, (FACT_ORDERS,))


def _seed_orders_bundle(spark: FakeDeltaSpark, member_keys: list[str]) -> str:
    from genie_space_optimizer.optimization.mv_scoring import suggestion_id_for

    bundle_key = mv_state.mv_bundle_fingerprint(SPACE_ID, member_keys, [FACT_ORDERS])
    suggestion_id = suggestion_id_for(bundle_key)
    mv_state.upsert_mv_candidate(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID, target_space_id=SPACE_ID,
        suggestion_id=suggestion_id, dedup_fingerprint=bundle_key,
        candidate_type="NEW_METRIC_VIEW", proposed_object=_ORDERS_VIEW,
        evidence={
            "bundle": True,
            "benchmark_question_ids": [],
            "source_tables": [FACT_ORDERS],
            "measures": [{"dedup_fingerprint": k, "benchmark_question_ids": []} for k in member_keys],
        },
    )
    return suggestion_id


def test_an_approved_v1_bundle_is_measured_on_its_own_members_beside_its_successor() -> None:
    """The approval is found by its stored ``suggestion_id`` and measured on its
    stored member keys; the reshaped successor and an older pending row of the
    same view are not read in its place."""
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures
    from genie_space_optimizer.optimization.mv_identity_v1 import v1_member_keys

    (v1_sum,) = v1_member_keys(SPACE_ID, {0: extract_measures(REVENUE_SQL)[0]}).values()
    total, rows, biggest = (_orders_member_key(a) for a in ("SUM(amount)", "COUNT(*)", "MAX(amount)"))
    assert v1_sum == total
    spark = FakeDeltaSpark()
    approved = _seed_orders_bundle(spark, [v1_sum])
    mv_state.record_mv_candidate_decision(
        spark, catalog=CATALOG, schema=SCHEMA, target_space_id=SPACE_ID,
        dedup_fingerprint=mv_state.mv_bundle_fingerprint(SPACE_ID, [v1_sum], [FACT_ORDERS]),
        decision="approved", decided_by=USER,
    )
    pending = _seed_orders_bundle(spark, [total, rows])
    successor = _seed_orders_bundle(spark, [total, rows, biggest])
    baseline = _baseline_output([
        _row("q_sum", "BAD", sql=UNQUALIFIED_REVENUE_SQL),
        _row("q_rows", "BAD", sql="SELECT COUNT(*) FROM fact_orders"),
        _row("q_max", "BAD", sql="SELECT MAX(amount) FROM sales.fact_orders"),
    ])

    def affected(suggestion_id: str) -> list[str]:
        return mv_attach._affected_question_ids(
            spark, space_id=SPACE_ID, catalog=CATALOG, schema=SCHEMA,
            suggestion_ids=[suggestion_id], baseline_eval=baseline, config=_config(),
        )

    loaded = {r["suggestion_id"]: r for r in mv_state.load_mv_candidates(
        spark, CATALOG, SCHEMA, target_space_id=SPACE_ID,
    )}
    assert loaded[approved]["decision"] == "approved"
    assert len({approved, pending, successor}) == 3
    assert affected(approved) == ["q_sum"]
    assert affected(pending) == ["q_sum", "q_rows"]
    assert affected(successor) == ["q_sum", "q_rows", "q_max"]


# ── MV-D123: statements the matcher must not select ──


def _affected_by_orders_member(aggregate: str, rows: list[dict[str, Any]]) -> list[str]:
    spark = FakeDeltaSpark()
    suggestion_id = _seed_orders_bundle(spark, [_orders_member_key(aggregate)])
    return mv_attach._affected_question_ids(
        spark, space_id=SPACE_ID, catalog=CATALOG, schema=SCHEMA,
        suggestion_ids=[suggestion_id], baseline_eval=_baseline_output(rows), config=_config(),
    )


@pytest.mark.parametrize("sql", [
    f"WITH x AS (SELECT * FROM {FACT_ORDERS}) SELECT COUNT(*) FROM x",
    f"SELECT COUNT(*) FROM (SELECT * FROM {FACT_ORDERS}) s",
], ids=["cte", "derived"])
def test_a_row_count_over_a_cte_or_derived_table_does_not_select(sql: str) -> None:
    """Ruling 27: the row count names no table and is unresolved, not table-less.

    Read as table-less, it would key over the space's one table and match the
    orders row count, so only the matcher's unresolved-source guard keeps it out.
    """
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures

    (measure,) = extract_measures(sql)
    assert measure.has_unresolved_source and measure.source_tables == ()
    assert _affected_by_orders_member("COUNT(*)", [
        _row("q_derived", "BAD", sql=sql),
        _row("q_rows", "BAD", sql="SELECT COUNT(*) FROM fact_orders"),
    ]) == ["q_rows"]


def test_an_unqualified_statement_over_a_table_the_space_does_not_list_does_not_select() -> None:
    """Ruling 38: ``fact_returns`` is not a space table, so it is unresolved; read as
    table-less, it would key over the space's one table and match the orders member."""
    assert _affected_by_orders_member("SUM(amount)", [
        _row("q_unlisted", "BAD", sql="SELECT SUM(amount) FROM fact_returns"),
        _row("q_listed", "BAD", sql=UNQUALIFIED_REVENUE_SQL),
    ]) == ["q_listed"]


@pytest.mark.parametrize("key", ["expected_sql", "inputs/expected_response"])
def test_each_expected_sql_key_selects_alone(key: str) -> None:
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint=_revenue_fingerprint(), benchmark_questions=[])
    row = {"question_id": "rev_001", "assessment": "BAD", "needs_review": False, key: REVENUE_SQL}
    outcome = _run_phase(
        spark, config=_config(), baseline=_baseline_output([row]),
        runner=_FakeRunner([_row("rev_001", "GOOD")]),
    )
    assert outcome.affected_question_count == 1


@pytest.mark.parametrize("key", ["generated_sql", "outputs/response"])
def test_each_generated_sql_key_selects_alone(key: str) -> None:
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint=_revenue_fingerprint(), benchmark_questions=[])
    row = {"question_id": "rev_001", "assessment": "BAD", "needs_review": False, key: REVENUE_SQL}
    outcome = _run_phase(
        spark, config=_config(), baseline=_baseline_output([row]),
        runner=_FakeRunner([_row("rev_001", "GOOD")]),
    )
    assert outcome.affected_question_count == 1


@pytest.mark.parametrize("key", ["benchmark_question_ids", "benchmark_questions"])
def test_each_recorded_id_key_selects_alone(key: str) -> None:
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=None, evidence={key: ["rev_001"]})
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
    )
    assert outcome.affected_question_count == 1


def test_no_live_or_matching_question_skips_before_anything_is_applied() -> None:
    spark = FakeDeltaSpark()
    _seed(
        spark,
        dedup_fingerprint=_revenue_fingerprint(),
        benchmark_questions=["trusted_asset:ta-1"],
    )
    baseline = _baseline_output([_row("rev_002", "BAD", sql=OTHER_SQL)])
    runner = _FakeRunner([])

    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert outcome.skip_reason == mv_attach.SKIP_NO_AFFECTED_QUESTIONS
    assert runner.calls == []
    assert outcome.config["data_sources"]["metric_views"] == []


def test_the_stage_row_carries_ids_and_counts_never_sql() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, dedup_fingerprint=_revenue_fingerprint(), benchmark_questions=[])
    baseline = _baseline_output([_row("rev_001", "BAD", sql=OTHER_SQL, expected=REVENUE_SQL)])
    _run_phase(
        spark, config=_config(), baseline=baseline,
        runner=_FakeRunner([_row("rev_001", "GOOD")]),
    )
    # The exact INSERT ``state.write_stage`` emits; its duration-lookup SELECT
    # also names the table and the stage, and is not the row.
    stage = [
        s for s in spark.statements
        if s.startswith(f"INSERT INTO {CATALOG}.{SCHEMA}.genie_opt_stages ")
        and "'MV_ATTACH'" in s
    ]
    assert stage
    assert not [s for s in stage if "fact_orders" in s or "SUM(" in s.upper()]


# ── Ordering: iteration-0 is pre-attach (MV-D16(b)) ──────────────────────


def test_the_attach_phase_runs_after_iteration_zero_and_before_the_first_patch(
    monkeypatch,
) -> None:
    """The baseline corpus must be pre-attach SQL.

    Attaching before iteration-0 would make the advisor fingerprint SQL that
    already uses a metric view, so each view would bias the case for its own
    successor.
    """
    events: list[str] = []
    baseline_config = _config()
    seen: dict[str, Any] = {}
    eval_kwargs: list[dict[str, Any]] = []

    def evaluate(*_args, **kwargs):
        events.append("baseline_eval")
        eval_kwargs.append(kwargs)
        return {
            "overall_accuracy": 10.0,
            "total_questions": 2,
            "correct_count": 0,
            "scores": {},
            "failures": [],
            "remaining_failures": [],
            "thresholds_met": False,
            "rows": [_row("rev_001", "BAD"), _row("rev_002", "BAD")],
            "eval_run_id": "eval-baseline",
            "eval_run_status": "DONE",
        }

    def attach_phase(_spark, **kwargs):
        events.append("mv_attach")
        seen.update(kwargs)
        return mv_attach.AttachOutcome(
            status=mv_attach.STATUS_SKIPPED,
            skip_reason=mv_attach.SKIP_NOT_REQUESTED,
            config=kwargs["config"],
        )

    def propose(*_args, **_kwargs):
        events.append("propose_patches")
        return None, "no patches", [], "{}"

    monkeypatch.setattr(
        unified_loop,
        "fetch_space_config",
        lambda *_a, **_k: {"_parsed_space": copy.deepcopy(baseline_config)},
    )
    monkeypatch.setattr(
        unified_loop,
        "run_space_quality_enrichment",
        lambda *_a, **_k: SimpleNamespace(current_config=copy.deepcopy(baseline_config)),
    )
    monkeypatch.setattr(unified_loop, "_native_eval", evaluate)
    monkeypatch.setattr(unified_loop, "run_mv_attach_phase", attach_phase)
    monkeypatch.setattr(unified_loop, "propose_patches", propose)
    monkeypatch.setattr(unified_loop, "write_iteration", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "_stamp_terminal", lambda *_a, **_k: None)

    unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id=RUN_ID,
        space_id=SPACE_ID,
        benchmarks=[],
        catalog=CATALOG,
        schema=SCHEMA,
        levers=[1],
        max_attempts=1,
        target_accuracy=0.9,
        mv_attach_views=f'["{MV_NAME}"]',
        mv_consent_id=PROBE_ID,
    )

    assert events.index("baseline_eval") < events.index("mv_attach")
    assert events.index("mv_attach") < events.index("propose_patches")
    # The phase is handed iteration-0's own eval and a config with no metric view.
    assert seen["baseline_eval"]["eval_run_id"] == "eval-baseline"
    assert seen["config"]["data_sources"]["metric_views"] == []
    assert seen["attach_views"] == f'["{MV_NAME}"]'
    assert seen["consent_probe_id"] == PROBE_ID
    # MV-D114 d3: the phase gets a full-suite eval callable, not a runner. Invoking
    # it runs the loop's own ``_native_eval`` (the OfficialBenchmarkRunner seam)
    # at iteration 0, for this space.
    assert "eval_runner" not in seen
    post = seen["post_attach_eval"]
    assert callable(post)
    assert events.count("baseline_eval") == 1
    post()
    assert events.count("baseline_eval") == 2
    assert eval_kwargs[-1]["iteration"] == 0
    assert eval_kwargs[-1]["space_id"] == SPACE_ID


def test_the_loop_carries_forward_the_config_the_phase_returned(monkeypatch) -> None:
    """A detached attach must not leave the metric view in the loop's config."""
    attached = _config([{"identifier": MV_NAME}])
    reverted = _config()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        unified_loop,
        "fetch_space_config",
        lambda *_a, **_k: {"_parsed_space": copy.deepcopy(attached)},
    )
    monkeypatch.setattr(
        unified_loop,
        "run_space_quality_enrichment",
        lambda *_a, **_k: SimpleNamespace(current_config=copy.deepcopy(attached)),
    )
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        lambda *_a, **_k: {
            "overall_accuracy": 95.0,
            "total_questions": 1,
            "correct_count": 1,
            "scores": {},
            "failures": [],
            "remaining_failures": [],
            "thresholds_met": True,
            "rows": [],
        },
    )
    monkeypatch.setattr(
        unified_loop,
        "run_mv_attach_phase",
        lambda *_a, **_k: mv_attach.AttachOutcome(
            status=mv_attach.STATUS_COMPLETE,
            verdict=mv_attach.VERDICT_DETACHED,
            detached=(MV_NAME,),
            config=copy.deepcopy(reverted),
        ),
    )
    monkeypatch.setattr(unified_loop, "write_iteration", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_a, **_k: None)

    def stamp(*_args, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(unified_loop, "_stamp_terminal", stamp)

    unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id=RUN_ID,
        space_id=SPACE_ID,
        benchmarks=[],
        catalog=CATALOG,
        schema=SCHEMA,
        levers=[1],
        max_attempts=0,
        target_accuracy=0.9,
    )

    assert captured["config"]["data_sources"]["metric_views"] == []


# ── Does the attach survive finalize? (MV-D18) ───────────────────────────
#
# Publish never re-deploys a config (``publish.py`` promotes the champion in
# Delta only), so "what is deployed" is whatever the optimize task left live.
# Four trajectories, one test each, plus the reconciliation that keeps
# ``genie_opt_mv_created_objects.status`` true of the config the run ends on.


def _observed_config_updates(spark: FakeDeltaSpark) -> list[dict[str, Any]]:
    """Decode every ``observed_config_json`` UPDATE the phase issued.

    The fake applies MERGEs and serves SELECTs but ignores UPDATEs, so the
    assertion reads the statement log. Decoding the base64 payload rather than
    grepping the SQL is the point: it proves what would land in the column.
    """
    out: list[dict[str, Any]] = []
    for statement in spark.statements:
        if "observed_config_json" not in statement or not statement.startswith("UPDATE"):
            continue
        encoded = re.search(r"unbase64\('([^']+)'\)", statement)
        iteration = re.search(r"iteration = (\d+)", statement)
        assert encoded and iteration, statement
        out.append(
            {
                "iteration": int(iteration.group(1)),
                "config": json.loads(base64.b64decode(encoded.group(1)).decode("utf-8")),
                "statement": statement,
            }
        )
    return out


def _loop_eval(accuracy: float) -> dict[str, Any]:
    """One full-suite eval output at ``accuracy``, in the shape the loop holds it.

    Six questions, all moving the same way: enough for the loop's paired sign
    test to reach significance, so an improvement is actually accepted rather
    than rejected on insufficient evidence.
    """
    verdict = "GOOD" if accuracy > 50 else "BAD"
    rows = [_row(f"rev_{n:03d}", verdict) for n in range(1, 7)]
    return {
        "overall_accuracy": accuracy,
        "total_questions": len(rows),
        "correct_count": sum(1 for r in rows if r["assessment"] == "GOOD"),
        "scores": {},
        "failures": [],
        "remaining_failures": [],
        "thresholds_met": False,
        "rows": rows,
        "eval_run_id": f"eval-{accuracy}",
        "eval_run_status": "DONE",
    }


def _loop(
    monkeypatch, *, attach, accuracies, spark=None, raise_at=None, observed="live",
    status_calls: list | None = None, proposed: list | None = None,
    record: list | None = None, w: Any = None, phase_calls: list | None = None,
    **loop_kwargs,
):
    """Drive the loop with a fixed attach outcome and accuracy trajectory.

    ``accuracies[0]`` is the baseline; the rest are candidate attempts. The
    applier and rollback are the real ones — the whole question in cases 1 and 2
    is what the real ``pre_snapshot`` contract does to a post-attach config.

    The optional lists capture what the loop did: ``status_calls`` the kwargs of
    every ``update_run_status``, ``proposed`` the kwargs of every
    ``propose_patches``, ``record`` one ``eval:<accuracy>`` entry per
    ``_native_eval`` the loop ran, and ``phase_calls`` the kwargs the loop handed
    ``run_mv_attach_phase``. ``w`` is the workspace client the loop is given.

    ``observed`` models the per-iteration authoritative read-back at
    ``unified_loop.py:3382``, which the loop prefers over its own submitted
    config. Both settings are production paths and they reach the attach by
    different routes, so the cases are asserted under each:

    * ``"live"`` — the GET answers with the space as the attach left it, which is
      what really happens because the attach PATCHed it.
    * ``None`` — the read-back was unavailable, so the loop falls back to its own
      submitted config. This is the stricter case: it proves the attach survives
      through the loop's *own* config lineage and not merely because the live
      space happens to hold it.
    """
    pre_attach = _config()
    attached_config = copy.deepcopy(attach.config) if attach.config else pre_attach
    evals = iter(accuracies)
    stamped: dict[str, Any] = {}
    calls: list[str] = record if record is not None else []

    def evaluate(*_args, **_kwargs):
        accuracy = next(evals)
        calls.append(f"eval:{accuracy}")
        if raise_at is not None and len(calls) >= raise_at:
            raise RuntimeError("optimize blew up mid-loop")
        return _loop_eval(accuracy)

    def propose(*_args, **kwargs):
        if proposed is not None:
            proposed.append(kwargs)
        return (
            1,
            "add a description",
            [
                {
                    "type": "update_description",
                    "target": "main.sales.fact_orders",
                    "new_text": "Order facts.",
                    "lever": 1,
                }
            ],
            "{}",
        )

    def status(*_args, **kwargs):
        if status_calls is not None:
            status_calls.append(kwargs)

    def phase(*_args, **kwargs):
        if phase_calls is not None:
            phase_calls.append(kwargs)
        return attach

    monkeypatch.setattr(
        unified_loop,
        "fetch_space_config",
        lambda *_a, **_k: {"_parsed_space": copy.deepcopy(pre_attach)},
    )
    monkeypatch.setattr(
        unified_loop,
        "run_space_quality_enrichment",
        lambda *_a, **_k: SimpleNamespace(current_config=copy.deepcopy(pre_attach)),
    )
    monkeypatch.setattr(unified_loop, "_native_eval", evaluate)
    monkeypatch.setattr(unified_loop, "run_mv_attach_phase", phase)
    monkeypatch.setattr(
        unified_loop,
        "_read_observed_config_after_evaluation",
        lambda *_a, **_k: (copy.deepcopy(attached_config) if observed == "live" else None),
    )
    monkeypatch.setattr(unified_loop, "propose_patches", propose)
    monkeypatch.setattr(unified_loop, "write_iteration", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "update_run_status", status)
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "mark_patches_rolled_back", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "mark_iteration_rolled_back", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "write_patch", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "_stamp_terminal", lambda *_a, **kw: stamped.update(kw))

    out = unified_loop.run_unified_optimization_loop(
        w,
        spark if spark is not None else MagicMock(),
        run_id=RUN_ID,
        space_id=SPACE_ID,
        benchmarks=[],
        catalog=CATALOG,
        schema=SCHEMA,
        levers=[1],
        max_attempts=max(0, len(accuracies) - 1),
        target_accuracy=99.0,
        mv_attach_views=f'["{MV_NAME}"]',
        mv_consent_id=PROBE_ID,
        **loop_kwargs,
    )
    return out, stamped


def _kept_attach(post_accuracy: float | None = None) -> mv_attach.AttachOutcome:
    return mv_attach.AttachOutcome(
        status=mv_attach.STATUS_COMPLETE,
        verdict=mv_attach.VERDICT_ATTACHED,
        attached=(MV_NAME,),
        config=_config([{"identifier": MV_NAME}]),
        post_attach_eval=_loop_eval(post_accuracy) if post_accuracy is not None else None,
        post_attach_accuracy=post_accuracy,
    )


@pytest.mark.parametrize("observed", ["live", None])
def test_case_1_a_late_champion_still_carries_the_metric_view(
    monkeypatch, observed,
) -> None:
    """Lift passes, the loop improves, the champion is a late iteration.

    Asserted under both read-back paths, because they reach the attach
    differently: with a live read-back the champion has the view because the
    attach PATCHed the space, and with the read-back unavailable it has the view
    because the loop's own submitted config descends from the post-attach config.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    _out, stamped = _loop(
        monkeypatch,
        attach=_kept_attach(),
        accuracies=[10.0, 80.0],
        spark=spark,
        observed=observed,
    )

    assert stamped["iteration"] == 1
    assert stamped["config"]["data_sources"]["metric_views"] == [{"identifier": MV_NAME}]
    assert _created_row(spark)["status"] == "ATTACHED"


def test_case_2_a_regressing_loop_keeps_a_view_that_passed_its_own_lift(
    monkeypatch,
) -> None:
    """Lift passes, every lever attempt is rejected, the champion is iteration 0.

    This is the case worth defending. A rejected attempt reverts to that
    attempt's ``pre_snapshot``, which is the post-attach config the loop was
    handed — so unrelated levers regressing must not cost a metric view that
    measurably helped.

    Read-back unavailable is the deliberate setting: it removes the live space
    from the answer, so what survives is what the loop's own rollback restored.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    _out, stamped = _loop(
        monkeypatch,
        attach=_kept_attach(),
        accuracies=[80.0, 10.0],
        spark=spark,
        observed=None,
    )

    assert stamped["iteration"] == 0
    assert stamped["config"]["data_sources"]["metric_views"] == [{"identifier": MV_NAME}]
    # The rejected lever patch is gone; the consent-backed attach is not.
    assert stamped["config"]["data_sources"]["tables"] == [
        {"identifier": "main.sales.fact_orders"}
    ]
    assert _created_row(spark)["status"] == "ATTACHED"


def test_case_2_the_champion_record_is_repointed_at_the_attached_config() -> None:
    """The half a live-space check cannot see.

    Iteration 0's row is written before the attach, and it becomes the champion
    whenever no attempt is accepted. ``revert_optimization(target="champion")``
    resolves to ``observed_config_json``, so without this the button a user
    presses to KEEP the optimized config would strip the view.
    """
    spark = FakeDeltaSpark()
    _seed(spark)
    outcome = _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
    )
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED

    updates = _observed_config_updates(spark)
    assert len(updates) == 1
    assert updates[0]["iteration"] == 0
    assert updates[0]["config"]["data_sources"]["metric_views"] == [
        {"identifier": MV_NAME}
    ]
    # Scoped to the full-scope row, so a same-iteration slice row is untouched.
    assert "eval_scope = 'full'" in updates[0]["statement"]


def test_case_2_the_submitted_baseline_config_is_left_pre_attach() -> None:
    """``config_json`` records what was scored, and the baseline WAS pre-attach.

    Correcting the observed column is a fidelity fix; rewriting the submitted one
    would be a misattribution — iteration 0's accuracy was measured without the
    view.
    """
    spark = FakeDeltaSpark()
    _seed(spark)
    _run_phase(
        spark,
        config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=_FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")]),
    )
    assert not [s for s in spark.statements if "SET config_json" in s]


def test_case_3_a_mid_loop_failure_leaves_the_status_alone(monkeypatch) -> None:
    """Lift passes, the loop raises. The view is still live, so the row stands.

    ``run_optimize`` writes a failure stage and re-raises without reverting, and
    reconciliation never runs — which is the right outcome, because demoting a
    row here would deny an attachment that is genuinely still on the space.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    with pytest.raises(RuntimeError, match="blew up mid-loop"):
        _loop(
            monkeypatch,
            attach=_kept_attach(),
            accuracies=[10.0, 20.0],
            spark=spark,
            raise_at=2,
        )

    assert _created_row(spark)["status"] == "ATTACHED"


def test_case_4_a_failed_lift_leaves_no_view_and_no_claim(monkeypatch) -> None:
    """Lift fails, the attach is already reverted, the loop runs on normally."""
    spark = FakeDeltaSpark()
    _seed(spark, status="DETACHED")
    detached = mv_attach.AttachOutcome(
        status=mv_attach.STATUS_COMPLETE,
        verdict=mv_attach.VERDICT_DETACHED,
        detached=(MV_NAME,),
        config=_config(),
    )
    _out, stamped = _loop(
        monkeypatch, attach=detached, accuracies=[10.0, 80.0], spark=spark,
    )

    assert stamped["config"]["data_sources"]["metric_views"] == []
    assert _created_row(spark)["status"] == "DETACHED"


# ── A kept attach re-baselines the loop (MV-D114 d7) ─────────────────────


def test_a_lever_is_not_credited_with_the_views_gain(monkeypatch) -> None:
    """M4 exit criterion (baseline-reset pin).

    Baseline 10, post-attach 70, the lever lands at 60. Against the stale baseline
    the lever "improves" by 50 and is accepted; against the post-attach baseline
    it regresses and is rejected.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    _out, stamped = _loop(
        monkeypatch,
        attach=_kept_attach(post_accuracy=70.0),
        accuracies=[10.0, 60.0],
        spark=spark,
    )
    assert stamped["iteration"] == 0
    assert stamped["best_accuracy"] == pytest.approx(70.0)
    assert stamped["config"]["data_sources"]["metric_views"] == [{"identifier": MV_NAME}]


def test_levers_are_proposed_from_the_post_attach_failures(monkeypatch) -> None:
    proposed: list[dict] = []
    _loop(
        monkeypatch, attach=_kept_attach(post_accuracy=70.0),
        accuracies=[10.0, 60.0], proposed=proposed,
    )
    assert proposed and proposed[0]["eval_result"]["eval_run_id"] == "eval-70.0"


def test_the_run_status_carries_the_post_attach_baseline(monkeypatch) -> None:
    status_calls: list[dict] = []
    _loop(
        monkeypatch, attach=_kept_attach(post_accuracy=70.0),
        accuracies=[10.0, 60.0], status_calls=status_calls,
    )
    resets = [c for c in status_calls if c.get("best_accuracy") == pytest.approx(70.0)]
    assert resets and resets[0].get("best_iteration") == 0


def test_a_post_attach_baseline_at_target_ends_the_run(monkeypatch) -> None:
    record: list[str] = []
    _out, stamped = _loop(
        monkeypatch, attach=_kept_attach(post_accuracy=100.0),
        accuracies=[10.0, 60.0], record=record,
    )
    assert stamped["reason"] == "TARGET_REACHED"
    assert stamped["iteration"] == 0
    assert stamped["best_accuracy"] == pytest.approx(100.0)
    assert record == ["eval:10.0"]  # the baseline only: no lever eval ran


def test_a_detached_attach_leaves_the_baseline_alone(monkeypatch) -> None:
    """Control: without a kept attach the same lever IS accepted."""
    detached = mv_attach.AttachOutcome(
        status=mv_attach.STATUS_COMPLETE,
        verdict=mv_attach.VERDICT_DETACHED,
        detached=(MV_NAME,),
        config=_config(),
    )
    _out, stamped = _loop(monkeypatch, attach=detached, accuracies=[10.0, 60.0])
    assert stamped["iteration"] == 1
    assert stamped["best_accuracy"] == pytest.approx(60.0)


# ── MV-D118 (wide_schema = rerun): wide schema re-plans after a kept attach ──


def test_a_kept_attach_re_plans_wide_schema_on_its_own_failures(monkeypatch) -> None:
    """The plan the post-attach re-plan returns is the one the loop carries on.

    ``_adapt_wide_schema_for_failures`` is a closure inside the loop, so the
    post-attach call is steered to the marker plan through the module seams it
    calls: its failure SQL names an omitted column, and ``revise_plan_for_column``
    activates it into the marker. Every other call runs the real code. The loop's
    next reader of the plan is the re-plan after an accepted lever, so the run
    carries one lever that wins, against post-attach rows that fail.
    """
    marker = {
        "profiling_budget": {}, "marker": "post-attach", "plan_hash": "plan-post-attach", "revision": 2,
    }
    marker_sql = "SELECT marker_col FROM main.sales.fact_orders"
    marker_key = ("main", "sales", "fact_orders", "marker_col")
    seen: list[str] = []
    plans_seen: list[tuple[str, Any]] = []
    real_failure_rows = unified_loop._failure_rows
    real_evidence = unified_loop.sql_column_evidence
    real_revise = unified_loop.revise_plan_for_column

    def spy(eval_result, *a, **k):
        seen.append(str(eval_result.get("eval_run_id")))
        return real_failure_rows(eval_result, *a, **k)

    def evidence(sql, inventory):
        if sql == marker_sql:
            return [{"column_key": list(marker_key)}]
        return real_evidence(sql, inventory)

    def revise(plan, inventory, column_key, **kwargs):
        if tuple(column_key) == marker_key:
            return marker
        return real_revise(plan, inventory, column_key, **kwargs)

    def active(plan):
        plans_seen.append((seen[-1], plan))
        return set()

    monkeypatch.setenv("GSO_WIDE_SCHEMA_PLAN_HASH", "")
    monkeypatch.setattr(unified_loop, "_failure_rows", spy)
    monkeypatch.setattr(unified_loop, "sql_column_evidence", evidence)
    monkeypatch.setattr(unified_loop, "revise_plan_for_column", revise)
    monkeypatch.setattr(unified_loop, "active_column_keys", active)
    monkeypatch.setattr(
        unified_loop, "write_required_artifact", lambda *_a, **_k: {"artifact_id": "art-post-attach"},
    )
    monkeypatch.setattr(unified_loop, "write_artifact", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "project_active_inventory", lambda *_a, **_k: [])
    monkeypatch.setattr(unified_loop, "validate_inventory", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "validate_selection_plan", lambda *_a, **_k: None)
    failing_rows = [_row(f"rev_{n:03d}", "BAD", expected=marker_sql) for n in range(1, 7)]
    kept = replace(
        _kept_attach(90.0), post_attach_eval={**_loop_eval(90.0), "rows": failing_rows},
    )
    _out, stamped = _loop(
        monkeypatch, attach=kept, accuracies=[10.0, 95.0],
        wide_schema_inventory={"inventory_hash": "h"},
        wide_schema_plan={"profiling_budget": {}},
        wide_schema_profile_budget={},
    )
    assert "eval-10.0" in seen and "eval-90.0" in seen
    assert seen.index("eval-10.0") < seen.index("eval-90.0")
    assert stamped["iteration"] == 1
    assert [eval_id for eval_id, _plan in plans_seen] == ["eval-10.0", "eval-90.0", "eval-95.0"]
    assert plans_seen[-1] == ("eval-95.0", marker)


def test_a_detached_attach_does_not_re_plan(monkeypatch) -> None:
    seen: list[str] = []
    monkeypatch.setattr(
        unified_loop, "_failure_rows",
        lambda eval_result, *a, **k: seen.append(str(eval_result.get("eval_run_id"))) or [],
    )
    monkeypatch.setattr(unified_loop, "validate_inventory", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "validate_selection_plan", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "active_column_keys", lambda *_a, **_k: set())
    detached = mv_attach.AttachOutcome(
        status=mv_attach.STATUS_COMPLETE, verdict=mv_attach.VERDICT_DETACHED, config=_config(),
    )
    _loop(
        monkeypatch, attach=detached, accuracies=[10.0],
        wide_schema_inventory={"inventory_hash": "h"},
        wide_schema_plan={"profiling_budget": {}},
        wide_schema_profile_budget={},
    )
    assert seen.count("eval-10.0") == 1 and "eval-90.0" not in seen


# ── MV-D118 (d4 = net_suite): a suite loss is a regression ───────────────


def test_a_net_suite_loss_detaches_even_when_the_subset_improves(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    reverts = _spy_rollback(monkeypatch)
    baseline = _baseline_output([
        _row("rev_001", "BAD"), _row("rev_002", "GOOD"),
        _row("rev_003", "GOOD"), _row("rev_004", "GOOD"),
    ])
    runner = _FakeRunner([
        _row("rev_001", "GOOD"), _row("rev_002", "GOOD"),
        _row("rev_003", "BAD"), _row("rev_004", "BAD"),
    ])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.delta_affected > 0
    assert outcome.delta_suite < 0
    assert outcome.verdict == mv_attach.VERDICT_DETACHED
    assert reverts == [SPACE_ID]
    assert outcome.config["data_sources"]["metric_views"] == []


def test_an_even_suite_keeps_a_subset_gain(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    baseline = _baseline_output([
        _row("rev_001", "BAD"), _row("rev_002", "GOOD"), _row("rev_003", "GOOD"),
    ])
    runner = _FakeRunner([
        _row("rev_001", "GOOD"), _row("rev_002", "GOOD"), _row("rev_003", "BAD"),
    ])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.delta_suite == 0
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED


def test_an_even_suite_keeps_a_wash_whose_only_regression_is_outside_the_subset() -> None:
    """d4 with net_suite: an outside regression offset by an outside gain is noise."""
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=["rev_001"])
    baseline = _baseline_output([
        _row("rev_001", "GOOD"), _row("rev_008", "GOOD"), _row("rev_009", "BAD"),
    ])
    runner = _FakeRunner([
        _row("rev_001", "GOOD"), _row("rev_008", "BAD"), _row("rev_009", "GOOD"),
    ])
    outcome = _run_phase(spark, config=_config(), baseline=baseline, runner=runner)
    assert outcome.delta_affected == pytest.approx(0.0)
    assert outcome.delta_suite == 0
    assert outcome.regressed_question_count == 0
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED


def test_the_attach_diagnostic_carries_delta_suite(monkeypatch) -> None:
    events: list[tuple[str, dict]] = []
    attach = replace(_kept_attach(90.0), delta_affected=0.5, delta_suite=0.25)
    _loop(
        monkeypatch, attach=attach, accuracies=[10.0],
        diagnostic_callback=lambda event, **payload: events.append((event, payload)),
    )
    measured = [p for e, p in events if e == "Metric view attach measured"]
    assert measured and measured[0]["delta_suite"] == 0.25


# ── Status truthfulness: end-of-run reconciliation ───────────────────────


def test_reconciliation_demotes_a_claim_the_final_config_does_not_carry() -> None:
    """An ATTACHED row pointing at a space without the view is worse than no row.

    Prompt 9's re-run flow and Prompt 13's UI both read this column, so the run
    does not end on an unverified status.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")

    result = mv_attach.reconcile_attached_objects(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
    )

    assert result == {
        "checked": 1,
        "verified": 0,
        "demoted": 1,
        "identifiers": [MV_NAME],
    }
    assert _created_row(spark)["status"] == "DETACHED"


def test_reconciliation_leaves_a_verified_claim_untouched() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    before = len(spark.statements)

    result = mv_attach.reconcile_attached_objects(
        spark,
        run_id=RUN_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        config=_config([{"identifier": MV_NAME}]),
    )

    assert (result["verified"], result["demoted"]) == (1, 0)
    assert _created_row(spark)["status"] == "ATTACHED"
    # No status write at all on the verified path, so it is safe to call at every
    # loop exit.
    assert not [s for s in spark.statements[before:] if s.startswith("MERGE INTO")]


def test_reconciliation_matches_identifiers_case_insensitively() -> None:
    """UC identifiers are case-insensitive; a case difference is not a detach."""
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")

    result = mv_attach.reconcile_attached_objects(
        spark,
        run_id=RUN_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        config=_config([{"identifier": MV_NAME.upper()}]),
    )

    assert (result["verified"], result["demoted"]) == (1, 0)


def test_reconciliation_never_drops_the_uc_object() -> None:
    """Demotion is a status correction. Drop stays an explicit backend endpoint."""
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")

    mv_attach.reconcile_attached_objects(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
    )

    # The rollback_policy literal DETACH_ONLY_NEVER_DROP contains the word, so
    # match a statement that would actually drop something.
    assert not [
        s for s in spark.statements if re.search(r"\bDROP\s+(VIEW|TABLE)\b", s, re.I)
    ]


def test_reconciliation_is_idempotent() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")
    kwargs = {"run_id": RUN_ID, "catalog": CATALOG, "schema": SCHEMA, "config": _config()}

    first = mv_attach.reconcile_attached_objects(spark, **kwargs)
    second = mv_attach.reconcile_attached_objects(spark, **kwargs)

    assert first["demoted"] == 1
    # Already DETACHED, so the second pass has nothing to check or correct.
    assert second == {"checked": 0, "verified": 0, "demoted": 0, "identifiers": []}


@pytest.mark.parametrize("seeded", ["CREATED", "DETACHED"])
def test_reconciliation_never_promotes_a_row_the_config_happens_to_carry(
    seeded: str,
) -> None:
    """Demote-only: presence in the config is not evidence of a consented attach.

    The identifier IS on the final config here, which is the shape that would
    tempt a promotion. It must not happen: an identifier can reach
    ``data_sources.metric_views`` by any route, and only the attach phase has
    checked the consent row and the created object. Promoting on config presence
    alone would let an attach that bypassed MV-D1's gate acquire a
    legitimate-looking ATTACHED status, which Prompt 9 and Prompt 13 would then
    read as consented truth.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status=seeded)
    before = len(spark.statements)

    result = mv_attach.reconcile_attached_objects(
        spark,
        run_id=RUN_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        config=_config([{"identifier": MV_NAME}]),
    )

    assert result == {"checked": 0, "verified": 0, "demoted": 0, "identifiers": []}
    assert _created_row(spark)["status"] == seeded
    assert not [s for s in spark.statements[before:] if s.startswith("MERGE INTO")]


def test_reconciliation_writes_no_status_other_than_detached() -> None:
    """The property stated as a property: DETACHED is the only status it writes.

    Pins it against the whole status vocabulary rather than the two cases above,
    so a status added to MV_CREATED_OBJECT_STATUSES later cannot quietly become
    something reconciliation is willing to write.
    """
    for seeded in mv_state.MV_CREATED_OBJECT_STATUSES:
        spark = FakeDeltaSpark()
        _seed(spark, status=seeded)
        before = len(spark.statements)

        mv_attach.reconcile_attached_objects(
            spark,
            run_id=RUN_ID,
            catalog=CATALOG,
            schema=SCHEMA,
            config=_config([{"identifier": MV_NAME}]),
        )

        written = {
            m.group(1).upper()
            for s in spark.statements[before:]
            if s.startswith("MERGE INTO")
            for m in re.finditer(r"status\s*=\s*'([^']*)'", s)
        }
        assert written <= {"DETACHED"}, f"seeded={seeded} wrote {written}"


def test_reconciliation_ignores_a_non_attached_row_the_read_lets_through() -> None:
    """The per-row check, exercised independently of the read's status filter.

    If a future edit widens or drops ``status=ATTACHED`` on the load, the loop
    itself still refuses every row that does not already claim ATTACHED — so the
    demote-only property does not rest on one argument at one call site.
    """
    spark = FakeDeltaSpark()
    _seed(spark, status="CREATED")

    with patch.object(
        mv_attach,
        "load_mv_created_objects",
        return_value=[
            {"full_name": MV_NAME, "suggestion_id": SUGGESTION_ID, "status": "CREATED"},
        ],
    ):
        result = mv_attach.reconcile_attached_objects(
            spark,
            run_id=RUN_ID,
            catalog=CATALOG,
            schema=SCHEMA,
            config=_config([{"identifier": MV_NAME}]),
        )

    assert result == {"checked": 0, "verified": 0, "demoted": 0, "identifiers": []}
    assert _created_row(spark)["status"] == "CREATED"


def test_reconciliation_survives_an_unreadable_table() -> None:
    """A read failure must not cost the run its terminal stamp."""
    result = mv_attach.reconcile_attached_objects(
        MagicMock(), run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
    )
    assert result["checked"] == 0


def test_attached_identifiers_tolerates_a_shapeless_config() -> None:
    assert mv_attach.attached_identifiers(None) == set()
    assert mv_attach.attached_identifiers({}) == set()
    assert mv_attach.attached_identifiers({"data_sources": {"metric_views": ["x"]}}) == set()


# ── Genie moves an attached view to data_sources.tables (M4 live run) ────
#
# The applier writes metric_views; Genie relocates the entry to tables on write and
# exports it there (backend/services/mv_create.py records the same round trip). A
# config read back from the space therefore carries the view under tables.


def _config_with_view_under_tables() -> dict[str, Any]:
    config = _config()
    config["data_sources"]["tables"].append({"identifier": MV_NAME})
    return config


def test_attached_identifiers_counts_a_view_genie_moved_to_tables() -> None:
    assert MV_NAME in mv_attach.attached_identifiers(_config_with_view_under_tables())


def test_a_failed_revert_is_reported_when_the_live_space_lists_the_view_under_tables() -> None:
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark)

    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(),
        live_config=_config_with_view_under_tables,
    )

    assert live == [MV_NAME]
    stage = _stage_statements(spark, mv_attach.UNMEASURED_PHASE_NAME.upper())
    assert stage and MV_NAME in stage[-1]


def test_reconciliation_does_not_demote_a_view_listed_under_tables() -> None:
    """A restarted loop starts from the space's export, where the view is a table entry."""
    spark = FakeDeltaSpark()
    _seed(spark, status="ATTACHED")

    result = mv_attach.reconcile_attached_objects(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config_with_view_under_tables(),
    )

    assert (result["verified"], result["demoted"]) == (1, 0)
    assert _created_row(spark)["status"] == "ATTACHED"


# ── The kept attach's re-baseline, read back for publish (MV-D114 d7) ────


def _stages(*details: dict[str, Any], stage: str | None = None) -> pd.DataFrame:
    name = stage or mv_attach.MV_ATTACH_PHASE_NAME.upper()
    return pd.DataFrame([{"stage": name, "detail_json": json.dumps(d)} for d in details])


BASELINE_EVAL_ID = "eval-baseline-1"


def _kept_detail(accuracy: float | None = 90.0) -> dict[str, Any]:
    return mv_attach.AttachOutcome(
        status=mv_attach.STATUS_COMPLETE, verdict=mv_attach.VERDICT_ATTACHED,
        baseline_eval_run_id=BASELINE_EVAL_ID, post_attach_accuracy=accuracy,
    ).detail()


def test_the_kept_attach_reset_is_read_from_the_stage_the_phase_writes() -> None:
    with patch.object(mv_attach, "load_stages", return_value=_stages(_kept_detail())):
        assert mv_attach.kept_attach_baseline_reset(
            MagicMock(), RUN_ID, CATALOG, SCHEMA,
        ) == BaselineReset(BASELINE_EVAL_ID, 90.0)


_DETACHED = mv_attach.AttachOutcome(
    status=mv_attach.STATUS_COMPLETE, verdict=mv_attach.VERDICT_DETACHED,
    post_attach_accuracy=80.0,
).detail()
_SKIPPED = mv_attach.AttachOutcome(
    status=mv_attach.STATUS_SKIPPED, skip_reason="NO_CREATED_OBJECT",
).detail()


@pytest.mark.parametrize("stages", [
    _stages(_DETACHED),
    _stages(_SKIPPED),
    _stages(),
    _stages(_kept_detail(), _SKIPPED),  # a restarted task whose phase skipped
    _stages(_kept_detail(), stage="MV_ATTACH_RECONCILE"),
    _stages(_kept_detail(None)),
])
def test_no_kept_attach_means_no_reset(stages) -> None:
    with patch.object(mv_attach, "load_stages", return_value=stages):
        assert mv_attach.kept_attach_baseline_reset(
            MagicMock(), RUN_ID, CATALOG, SCHEMA,
        ) is None


def test_an_unreadable_stage_table_means_no_reset() -> None:
    with patch.object(mv_attach, "load_stages", side_effect=RuntimeError("table gone")):
        assert mv_attach.kept_attach_baseline_reset(
            MagicMock(), RUN_ID, CATALOG, SCHEMA,
        ) is None
    with patch.object(mv_attach, "load_stages", return_value=None):
        assert mv_attach.kept_attach_baseline_reset(
            MagicMock(), RUN_ID, CATALOG, SCHEMA,
        ) is None


# ── End-of-run report of an unmeasured live view (MV-D114 d6) ────────────


def _seed_created_with_patch(spark: FakeDeltaSpark) -> None:
    _seed(spark)
    mv_state.update_mv_created_object_status(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID,
        suggestion_id=SUGGESTION_ID, status="CREATED",
        attach_patch_id=f"{RUN_ID}:0:2:0",
    )


def test_a_created_view_still_on_the_final_config_is_reported() -> None:
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark)

    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config([{"identifier": MV_NAME}]),
    )

    assert live == [MV_NAME]
    stage = _stage_statements(spark, mv_attach.UNMEASURED_PHASE_NAME.upper())
    assert stage and MV_NAME in stage[-1]


def test_the_unmeasured_report_never_writes_a_status() -> None:
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark)
    before = len(spark.statements)
    mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config([{"identifier": MV_NAME}]),
    )
    assert not [s for s in spark.statements[before:] if s.startswith("MERGE INTO")]
    assert _created_row(spark)["status"] == "CREATED"


@pytest.mark.parametrize("patched,config_views", [
    (False, [{"identifier": MV_NAME}]),  # never attached by this run: not ours to flag
    (True, []),  # reverted: nothing live
])
def test_the_unmeasured_report_is_silent_otherwise(patched, config_views) -> None:
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark) if patched else _seed(spark)
    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(config_views),
    )
    assert live == []
    assert not _stage_statements(spark, mv_attach.UNMEASURED_PHASE_NAME.upper())


def test_the_unmeasured_report_survives_an_unreadable_table() -> None:
    assert mv_attach.report_unmeasured_attachments(
        MagicMock(), run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
    ) == []


def test_the_unmeasured_report_survives_a_raising_read() -> None:
    with patch.object(
        mv_attach, "load_mv_created_objects", side_effect=RuntimeError("table gone"),
    ):
        assert mv_attach.report_unmeasured_attachments(
            MagicMock(), run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
            config=_config([{"identifier": MV_NAME}]),
        ) == []


def test_loop_exit_reconciliation_runs_the_unmeasured_report(monkeypatch) -> None:
    seen: list[str] = []
    monkeypatch.setattr(
        unified_loop, "report_unmeasured_attachments",
        lambda *_a, **_k: seen.append("reported") or [],
    )
    unified_loop._reconcile_mv_attachment(
        MagicMock(), run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(), emit=lambda *_a, **_k: None,
    )
    assert seen == ["reported"]


# ── The report reads the live space, not the in-memory config (MV-D114 d6) ─
#
# A revert that fails twice hands the loop the PRE-attach config, so the
# in-memory config never carries the view that may still be live. The report
# therefore asks the space itself, and falls back to memory only when it cannot.


def test_a_failed_revert_is_reported_from_the_live_config() -> None:
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark)

    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(),
        live_config=lambda: _config([{"identifier": MV_NAME}]),
    )

    assert live == [MV_NAME]
    stage = _stage_statements(spark, mv_attach.UNMEASURED_PHASE_NAME.upper())
    assert stage and MV_NAME in stage[-1]


@pytest.mark.parametrize("memory_views", [[], [{"identifier": MV_NAME}]])
def test_a_view_a_later_patch_dropped_is_not_reported(memory_views) -> None:
    """The live read wins over memory in both directions."""
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark)

    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(memory_views),
        live_config=lambda: _config(),
    )

    assert live == []
    assert not _stage_statements(spark, mv_attach.UNMEASURED_PHASE_NAME.upper())


def _raise_live_read() -> dict[str, Any]:
    raise RuntimeError("space read failed")


@pytest.mark.parametrize("reader", [_raise_live_read, lambda: None])
@pytest.mark.parametrize("memory_views,expected", [
    ([{"identifier": MV_NAME}], [MV_NAME]),
    ([], []),
])
def test_an_unreadable_live_config_falls_back_to_memory(
    reader, memory_views, expected,
) -> None:
    spark = FakeDeltaSpark()
    _seed_created_with_patch(spark)

    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(memory_views), live_config=reader,
    )

    assert live == expected


@pytest.mark.parametrize("seed", ["unpatched", "empty"])
def test_the_live_config_is_not_read_without_a_patched_created_row(seed) -> None:
    spark = FakeDeltaSpark()
    if seed == "unpatched":
        _seed(spark)
    calls: list[str] = []

    live = mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config([{"identifier": MV_NAME}]),
        live_config=lambda: calls.append("read") or _config([{"identifier": MV_NAME}]),
    )

    assert live == []
    assert calls == []


def test_loop_exit_passes_a_live_reader_and_emits_what_it_reports(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_report(*_a, **kwargs):
        captured.update(kwargs)
        return [MV_NAME]

    monkeypatch.setattr(unified_loop, "report_unmeasured_attachments", fake_report)
    fetched: list[tuple[Any, str]] = []
    workspace = object()
    monkeypatch.setattr(
        unified_loop,
        "fetch_space_config",
        lambda w, sid: fetched.append((w, sid)) or {
            "_parsed_space": _config([{"identifier": MV_NAME}]),
        },
    )
    emitted: list[tuple[str, dict[str, Any]]] = []

    unified_loop._reconcile_mv_attachment(
        MagicMock(), run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(), emit=lambda event, **kw: emitted.append((event, kw)),
        w=workspace, space_id="space-1",
    )

    reader = captured.get("live_config")
    assert callable(reader)
    assert reader() == _config([{"identifier": MV_NAME}])
    assert fetched == [(workspace, "space-1")]
    assert ("Metric view may still be attached", {"identifiers": [MV_NAME]}) in emitted


def test_loop_exit_without_a_workspace_passes_no_live_reader(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        unified_loop, "report_unmeasured_attachments",
        lambda *_a, **kwargs: captured.update(kwargs) or [],
    )
    unified_loop._reconcile_mv_attachment(
        MagicMock(), run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA,
        config=_config(), emit=lambda *_a, **_k: None,
    )
    assert captured.get("live_config") is None


def test_the_loop_live_read_is_best_effort(monkeypatch) -> None:
    def fail(_w, _sid):
        raise RuntimeError("genie unavailable")

    monkeypatch.setattr(unified_loop, "fetch_space_config", fail)
    assert unified_loop._read_live_space_config(object(), "space-1", run_id=RUN_ID) is None


# ── MV-D118: a view attached before the run is not this run's to report ──


def test_the_stage_row_records_views_already_on_the_config(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    runner = _FakeRunner([])
    outcome = _run_phase(
        spark, config=_config([{"identifier": MV_NAME.upper()}]),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=runner,
    )
    assert outcome.skip_reason == mv_attach.SKIP_ATTACH_NOT_APPLIED
    assert runner.calls == []
    stage = _stage_statements(spark, "MV_ATTACH")
    assert f'"pre_attached": ["{MV_NAME}"]' in stage[-1]


def test_the_unmeasured_report_skips_a_pre_attached_view() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    mv_state.update_mv_created_object_status(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID,
        suggestion_id=SUGGESTION_ID, status="CREATED", attach_patch_id="ref",
    )
    live = _config([{"identifier": MV_NAME}])
    with patch.object(mv_attach, "load_stages", return_value=[]):
        assert mv_attach.report_unmeasured_attachments(
            spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
            live_config=lambda: live, exclude=[MV_NAME],
        ) == []
        assert mv_attach.report_unmeasured_attachments(
            spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
            live_config=lambda: live,
        ) == [MV_NAME]


def test_the_earliest_stage_rows_pre_attached_survives_a_restart() -> None:
    """A restart's config may carry a view a failed revert left: the first row wins."""
    spark = FakeDeltaSpark()
    _seed(spark)
    mv_state.update_mv_created_object_status(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID,
        suggestion_id=SUGGESTION_ID, status="CREATED", attach_patch_id="ref",
    )
    # FakeDeltaSpark does not serve write_stage's INSERTs back, so the two rows
    # _record would write are served the way the reset tests serve them.
    stages = _stages(
        mv_attach.AttachOutcome(status="COMPLETE", pre_attached=()).detail(),
        mv_attach.AttachOutcome(status="SKIPPED", pre_attached=(MV_NAME,)).detail(),
    )
    live = _config([{"identifier": MV_NAME}])
    with patch.object(mv_attach, "load_stages", return_value=stages):
        assert mv_attach.report_unmeasured_attachments(
            spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
            live_config=lambda: live, exclude=[MV_NAME],
        ) == [MV_NAME]


def test_loop_exit_passes_the_phases_pre_attached(monkeypatch) -> None:
    seen: list[dict] = []
    monkeypatch.setattr(
        unified_loop, "report_unmeasured_attachments",
        lambda *_a, **kw: seen.append(kw) or [],
    )
    attach = replace(_kept_attach(90.0), pre_attached=(MV_NAME,))
    _loop(monkeypatch, attach=attach, accuracies=[10.0])
    assert seen and list(seen[-1]["exclude"]) == [MV_NAME]


MV_ORDERS = "main.sales.mv_orders"
SUGGESTION_ORDERS = "sug-2"


def _seed_mixed(spark: FakeDeltaSpark) -> None:
    """MV_NAME is already on the space before the run; MV_ORDERS is new.

    rev_001 is MV_NAME's alone, rev_002 MV_ORDERS' alone, rev_003 both views'.
    """
    _seed(spark, benchmark_questions=["rev_001", "rev_003"])
    mv_state.upsert_mv_created_object(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID,
        suggestion_id=SUGGESTION_ORDERS, full_name=MV_ORDERS, created_by=USER,
        status="CREATED",
    )
    mv_state.upsert_mv_candidate(
        spark, catalog=CATALOG, schema=SCHEMA, run_id=RUN_ID, target_space_id=SPACE_ID,
        suggestion_id=SUGGESTION_ORDERS, dedup_fingerprint="fp-2",
        candidate_type="NEW_METRIC_VIEW",
        evidence={"benchmark_questions": ["rev_002", "rev_003"]},
    )


def _created_row_for(spark: FakeDeltaSpark, suggestion_id: str) -> dict[str, Any]:
    return next(
        row for row in spark.rows
        if row.get("full_name") is not None and row.get("suggestion_id") == suggestion_id
    )


def _run_mixed(monkeypatch, spark: FakeDeltaSpark, *, baseline_rows, post_rows):
    updated: list[str] = []
    real_update = mv_attach.update_mv_created_object_status

    def spy(*args, **kwargs):
        updated.append(kwargs["suggestion_id"])
        return real_update(*args, **kwargs)

    monkeypatch.setattr(mv_attach, "update_mv_created_object_status", spy)
    outcome = _run_phase(
        spark, config=_config([{"identifier": MV_NAME}]),
        baseline=_baseline_output(baseline_rows), runner=_FakeRunner(post_rows),
        attach_views=json.dumps([MV_NAME, MV_ORDERS]),
    )
    return outcome, updated


def test_a_mixed_kept_attach_records_only_the_view_it_applied(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed_mixed(spark)
    before = dict(_created_row_for(spark, SUGGESTION_ID))
    outcome, updated = _run_mixed(
        monkeypatch, spark,
        baseline_rows=[_row("rev_001", "GOOD"), _row("rev_002", "BAD"), _row("rev_003", "BAD")],
        post_rows=[_row("rev_001", "GOOD"), _row("rev_002", "GOOD"), _row("rev_003", "GOOD")],
    )
    assert outcome.verdict == mv_attach.VERDICT_ATTACHED
    assert outcome.pre_attached == (MV_NAME,)
    assert outcome.attached == (MV_ORDERS,)
    assert outcome.suggestion_ids == (SUGGESTION_ORDERS,)
    assert _created_row_for(spark, SUGGESTION_ORDERS)["status"] == "ATTACHED"
    assert SUGGESTION_ID not in updated
    assert _created_row_for(spark, SUGGESTION_ID) == before


def test_a_mixed_detach_leaves_the_pre_attached_view_and_its_row(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed_mixed(spark)
    before = dict(_created_row_for(spark, SUGGESTION_ID))
    outcome, updated = _run_mixed(
        monkeypatch, spark,
        baseline_rows=[_row("rev_001", "GOOD"), _row("rev_002", "GOOD"), _row("rev_003", "GOOD")],
        post_rows=[_row("rev_001", "GOOD"), _row("rev_002", "BAD"), _row("rev_003", "GOOD")],
    )
    assert outcome.verdict == mv_attach.VERDICT_DETACHED
    assert outcome.detached == (MV_ORDERS,)
    assert outcome.config["data_sources"]["metric_views"] == [{"identifier": MV_NAME}]
    assert _created_row_for(spark, SUGGESTION_ORDERS)["status"] == "DETACHED"
    assert SUGGESTION_ID not in updated
    assert _created_row_for(spark, SUGGESTION_ID) == before


def test_a_mixed_request_measures_only_the_applied_views_questions(monkeypatch) -> None:
    spark = FakeDeltaSpark()
    _seed_mixed(spark)
    outcome, _ = _run_mixed(
        monkeypatch, spark,
        baseline_rows=[_row("rev_001", "GOOD"), _row("rev_002", "BAD"), _row("rev_003", "BAD")],
        post_rows=[_row("rev_001", "GOOD"), _row("rev_002", "GOOD"), _row("rev_003", "GOOD")],
    )
    assert outcome.affected_question_count == 2
    report = json.loads(_created_row_for(spark, SUGGESTION_ORDERS)["lift_report_json"])
    assert report["question_subset"] == ["rev_002", "rev_003"]


def test_a_bring_your_own_view_without_a_candidate_is_not_measured() -> None:
    """m-6: USER_CREATED waives the creator check, but a view needs questions."""
    spark = FakeDeltaSpark()
    _seed(
        spark, provenance="USER_CREATED", created_by="someone-else@example.com",
        benchmark_questions=None, evidence=None,
    )
    runner = _FakeRunner([])
    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD")]), runner=runner,
    )
    assert outcome.skip_reason == mv_attach.SKIP_NO_AFFECTED_QUESTIONS
    assert runner.calls == []
    assert outcome.config["data_sources"]["metric_views"] == []


# ── MV-D118 (patch = read_then_revert): a PATCH that raised may have landed ──


def _unconfirmed_apply_log(**extra: Any) -> dict[str, Any]:
    return {
        "applied": [{"action": {"target": MV_NAME}, "patch": {"type": "mv_attach_data_source"}}],
        "patch_deployed": False,
        "patch_error": f"timed out {_SENTINEL}",
        "patch_error_type": "TimeoutError",
        "pre_snapshot": _config(),
        "post_snapshot": _config([{"identifier": MV_NAME}]),
        **extra,
    }


def _run_unconfirmed(monkeypatch, *, live, apply_log=None, rollback_ok=True, w=None):
    spark = FakeDeltaSpark()
    _seed(spark)
    reverts: list[str] = []
    reads: list[str] = []
    monkeypatch.setattr(
        mv_attach, "apply_patch_set", lambda *_a, **_k: apply_log or _unconfirmed_apply_log(),
    )

    def rollback(_log, _w, space_id):
        reverts.append(space_id)
        return {"status": "ok"} if rollback_ok else {"status": "error", "errors": ["Failed to apply rollback via API"]}

    monkeypatch.setattr(mv_attach, "rollback", rollback)

    def read():
        reads.append("read")
        if isinstance(live, BaseException):
            raise live
        return live

    outcome = mv_attach.run_mv_attach_phase(
        spark, run_id=RUN_ID, space_id=SPACE_ID, catalog=CATALOG, schema=SCHEMA,
        attach_views=f'["{MV_NAME}"]', consent_probe_id=PROBE_ID, config=_config(),
        baseline_eval=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        w=w if w is not None else MagicMock(), post_attach_eval=_FakeRunner([]),
        live_config=read,
    )
    return spark, outcome, reverts, reads


def _patch_statements(spark: FakeDeltaSpark) -> list[str]:
    """The ``genie_opt_patches`` INSERTs ``state.write_patch`` emits."""
    return [
        s for s in spark.statements
        if s.startswith(f"INSERT INTO {CATALOG}.{SCHEMA}.genie_opt_patches ")
    ]


@pytest.mark.parametrize("shelf", ["metric_views", "tables"])
def test_a_landed_unconfirmed_patch_is_reverted(monkeypatch, shelf) -> None:
    live = _config()
    live["data_sources"][shelf].append({"identifier": MV_NAME})
    spark, outcome, reverts, _ = _run_unconfirmed(monkeypatch, live=live)
    assert reverts == [SPACE_ID]
    assert outcome.status == mv_attach.STATUS_SKIPPED
    assert outcome.skip_reason == mv_attach.SKIP_PATCH_UNCONFIRMED
    assert outcome.rollback_status == mv_attach.ROLLBACK_REVERTED
    assert outcome.error == "TimeoutError"
    assert outcome.config["data_sources"]["metric_views"] == []
    assert not [s for s in spark.statements if _SENTINEL in s]
    assert _patch_statements(spark) == []


def test_an_unlanded_unconfirmed_patch_is_not_reverted(monkeypatch) -> None:
    _, outcome, reverts, reads = _run_unconfirmed(monkeypatch, live=_config())
    assert reads == ["read"] and reverts == []
    assert outcome.skip_reason == mv_attach.SKIP_ATTACH_NOT_APPLIED
    assert outcome.error == "TimeoutError"


def test_an_unreadable_space_is_treated_as_landed(monkeypatch) -> None:
    _, outcome, reverts, _ = _run_unconfirmed(monkeypatch, live=RuntimeError(_SENTINEL))
    assert reverts == [SPACE_ID]
    assert outcome.skip_reason == mv_attach.SKIP_PATCH_UNCONFIRMED


def test_a_failed_revert_of_an_unconfirmed_patch_is_reported(monkeypatch) -> None:
    live = _config([{"identifier": MV_NAME}])
    spark, outcome, reverts, _ = _run_unconfirmed(monkeypatch, live=live, rollback_ok=False)
    assert reverts == [SPACE_ID, SPACE_ID]
    assert outcome.status == mv_attach.STATUS_FAILED
    assert outcome.rollback_status == mv_attach.ROLLBACK_FAILED
    assert (outcome.error or "").startswith("ROLLBACK_FAILED")
    assert "PATCH_UNCONFIRMED: TimeoutError" in outcome.error
    patch_rows = _patch_statements(spark)
    assert len(patch_rows) == 1
    assert f"VALUES ('{RUN_ID}', 0, 2, 0, " in patch_rows[0]
    assert f"'{MV_NAME}'" in patch_rows[0]
    assert _created_row(spark)["status"] == "CREATED"
    assert _created_row(spark)["attach_patch_id"] == f"{RUN_ID}:0:2:0"
    assert mv_attach.report_unmeasured_attachments(
        spark, run_id=RUN_ID, catalog=CATALOG, schema=SCHEMA, config=_config(),
        live_config=lambda: live,
    ) == [MV_NAME]


def test_the_failed_revert_log_names_only_the_applied_view(monkeypatch, caplog) -> None:
    """MV_NAME was on the space before the run; only MV_ORDERS rode the PATCH."""
    spark = FakeDeltaSpark()
    _seed_mixed(spark)
    log = _unconfirmed_apply_log(
        applied=[{"action": {"target": MV_ORDERS}, "patch": {"type": "mv_attach_data_source"}}],
        pre_snapshot=_config([{"identifier": MV_NAME}]),
    )
    monkeypatch.setattr(mv_attach, "apply_patch_set", lambda *_a, **_k: log)
    monkeypatch.setattr(
        mv_attach, "rollback",
        lambda *_a, **_k: {"status": "error", "errors": ["Failed to apply rollback via API"]},
    )
    live = _config([{"identifier": MV_NAME}, {"identifier": MV_ORDERS}])
    with caplog.at_level("DEBUG"):
        outcome = mv_attach.run_mv_attach_phase(
            spark, run_id=RUN_ID, space_id=SPACE_ID, catalog=CATALOG, schema=SCHEMA,
            attach_views=json.dumps([MV_NAME, MV_ORDERS]), consent_probe_id=PROBE_ID,
            config=_config([{"identifier": MV_NAME}]),
            baseline_eval=_baseline_output(
                [_row("rev_001", "GOOD"), _row("rev_002", "BAD"), _row("rev_003", "BAD")],
            ),
            w=MagicMock(), post_attach_eval=_FakeRunner([]), live_config=lambda: live,
        )
    assert outcome.rollback_status == mv_attach.ROLLBACK_FAILED
    errors = [
        r.getMessage() for r in caplog.records
        if r.levelname == "ERROR" and "revert failed" in r.getMessage()
    ]
    assert len(errors) == 1
    assert MV_ORDERS in errors[0] and MV_NAME not in errors[0]
    patch_rows = _patch_statements(spark)
    assert len(patch_rows) == 1
    assert f"'{MV_ORDERS}'" in patch_rows[0] and MV_NAME not in patch_rows[0]
    assert _SENTINEL not in caplog.text
    assert not [r for r in caplog.records if r.exc_info]


def test_a_raised_patch_with_no_message_is_read_back(monkeypatch) -> None:
    log = _unconfirmed_apply_log(patch_error="", patch_error_type="TimeoutError")
    _, outcome, _, reads = _run_unconfirmed(
        monkeypatch, live=_config([{"identifier": MV_NAME}]), apply_log=log,
    )
    assert reads == ["read"]
    assert outcome.skip_reason == mv_attach.SKIP_PATCH_UNCONFIRMED


def test_a_log_with_no_error_type_is_not_unconfirmed(monkeypatch) -> None:
    log = _unconfirmed_apply_log(patch_error="boom", patch_error_type="", validation_errors=[])
    _, outcome, reverts, reads = _run_unconfirmed(
        monkeypatch, live=_config([{"identifier": MV_NAME}]), apply_log=log,
    )
    assert reads == [] and reverts == []
    assert outcome.skip_reason == mv_attach.SKIP_ATTACH_NOT_APPLIED


def test_a_validation_failure_is_not_an_unconfirmed_patch(monkeypatch) -> None:
    log = _unconfirmed_apply_log(
        validation_errors=["bad"], patch_error="Validation failed: ['bad']", patch_error_type="",
    )
    _, outcome, reverts, reads = _run_unconfirmed(monkeypatch, live=_config(), apply_log=log)
    assert reads == [] and reverts == []
    assert outcome.skip_reason == mv_attach.SKIP_ATTACH_NOT_APPLIED


def test_a_patch_the_real_applier_saw_raise_is_read_back(monkeypatch) -> None:
    """The applier's PATCH-raised log carries ``validation_errors: []`` and the type."""

    def raise_timeout(*_a, **_k):
        raise TimeoutError(_SENTINEL)

    monkeypatch.setattr(applier, "patch_space_config", raise_timeout)
    spark = FakeDeltaSpark()
    _seed(spark)
    logs: list[dict[str, Any]] = []
    reverts: list[str] = []
    real_apply = mv_attach.apply_patch_set
    monkeypatch.setattr(
        mv_attach, "apply_patch_set",
        lambda *a, **k: logs.append(real_apply(*a, **k)) or logs[-1],
    )
    monkeypatch.setattr(
        mv_attach, "rollback",
        lambda _log, _w, space_id: reverts.append(space_id) or {"status": "ok"},
    )
    outcome = mv_attach.run_mv_attach_phase(
        spark, run_id=RUN_ID, space_id=SPACE_ID, catalog=CATALOG, schema=SCHEMA,
        attach_views=f'["{MV_NAME}"]', consent_probe_id=PROBE_ID, config=_config(),
        baseline_eval=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        w=MagicMock(), post_attach_eval=_FakeRunner([]),
        live_config=lambda: _config([{"identifier": MV_NAME}]),
    )
    assert logs[0]["validation_errors"] == [] and logs[0]["patch_error_type"] == "TimeoutError"
    assert reverts == [SPACE_ID]
    assert outcome.skip_reason == mv_attach.SKIP_PATCH_UNCONFIRMED
    assert outcome.error == "TimeoutError"
    assert not [s for s in spark.statements if _SENTINEL in s]


def test_the_loop_hands_the_phase_a_live_reader(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    skipped = mv_attach.AttachOutcome(status=mv_attach.STATUS_SKIPPED, config=_config())
    _loop(monkeypatch, attach=skipped, accuracies=[95.0], w=MagicMock(), phase_calls=calls)
    live = _config([{"identifier": MV_NAME}])
    monkeypatch.setattr(
        unified_loop, "fetch_space_config", lambda *_a, **_k: {"_parsed_space": copy.deepcopy(live)},
    )
    reader = calls[-1]["live_config"]
    assert callable(reader)
    assert reader()["data_sources"]["metric_views"] == [{"identifier": MV_NAME}]

    _loop(monkeypatch, attach=skipped, accuracies=[95.0], phase_calls=calls)
    assert calls[-1]["live_config"] is None


def test_a_kept_attach_records_its_remaining_failures() -> None:
    spark = FakeDeltaSpark()
    _seed(spark)
    runner = _FakeRunner([_row("rev_001", "GOOD"), _row("rev_002", "GOOD")])
    real_call = runner.__call__

    def with_failures():
        out = real_call()
        out["remaining_failures"] = ["rev_009"]
        return out

    outcome = _run_phase(
        spark, config=_config(),
        baseline=_baseline_output([_row("rev_001", "BAD"), _row("rev_002", "GOOD")]),
        runner=with_failures,
    )
    assert outcome.post_attach_remaining_failures == 1
    assert '"post_attach_remaining_failures": 1' in _stage_statements(spark, "MV_ATTACH")[-1]


def test_the_restart_lookup_scores_iteration_zero_at_the_reset(monkeypatch) -> None:
    rows = [
        {"iteration": 0, "eval_scope": "full", "eval_run_id": "eval-b", "overall_accuracy": 86.67},
        {"iteration": 1, "eval_scope": "full", "eval_run_id": "eval-1", "overall_accuracy": 88.0},
    ]
    monkeypatch.setattr(unified_loop, "load_all_scored_iterations", lambda *_a, **_k: rows)
    monkeypatch.setattr(
        unified_loop, "kept_attach_baseline_reset", lambda *_a, **_k: BaselineReset("eval-b", 90.0),
    )
    assert unified_loop._best_persisted_iteration(MagicMock(), RUN_ID, catalog=CATALOG, schema=SCHEMA) == (0, 90.0)
    monkeypatch.setattr(unified_loop, "kept_attach_baseline_reset", lambda *_a, **_k: None)
    assert unified_loop._best_persisted_iteration(MagicMock(), RUN_ID, catalog=CATALOG, schema=SCHEMA) == (1, 88.0)
