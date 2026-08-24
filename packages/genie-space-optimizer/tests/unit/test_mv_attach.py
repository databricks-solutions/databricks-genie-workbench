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
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.common.config import (
    HIGH_RISK_PATCHES,
    PATCH_TYPES,
    TABLE_MV_CREATED_OBJECTS,
)
from genie_space_optimizer.optimization import applier, mv_attach, mv_state, unified_loop
from genie_space_optimizer.optimization.eval_runner import EvalRunResult, lift_report
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


def _row(question_id: str, assessment: str) -> dict[str, Any]:
    return {
        "question_id": question_id,
        "assessment": assessment,
        "needs_review": assessment == "NEEDS_REVIEW",
    }


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
    """Records what the lift eval was asked to score, and answers with rows."""

    def __init__(self, rows: list[dict[str, Any]], *, status: str = "DONE") -> None:
        self.rows = rows
        self.status = status
        self.calls: list[tuple[str, list[str], str]] = []

    def run_subset(self, space_id: str, question_ids: Any, label: str) -> EvalRunResult:
        ids = list(question_ids)
        self.calls.append((space_id, ids, label))
        return EvalRunResult(
            eval_run_id="eval-lift",
            status=self.status,
            num_correct=sum(1 for r in self.rows if r["assessment"] == "GOOD"),
            num_done=len(self.rows),
            num_needs_review=0,
            num_questions=len(self.rows),
            rows=self.rows,
            wall_clock_seconds=1.0,
            eval_scope=label,
            requested_question_ids=tuple(ids),
        )


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
    if benchmark_questions is not None:
        mv_state.upsert_mv_candidate(
            spark,
            catalog=CATALOG,
            schema=SCHEMA,
            run_id=RUN_ID,
            target_space_id=SPACE_ID,
            suggestion_id=SUGGESTION_ID,
            dedup_fingerprint="fp-1",
            candidate_type="NEW_METRIC_VIEW",
            evidence={"benchmark_questions": list(benchmark_questions)},
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
        eval_runner=runner,
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


def test_the_lift_eval_scores_the_affected_questions_not_the_suite() -> None:
    spark = FakeDeltaSpark()
    _seed(spark, benchmark_questions=["rev_002"])
    baseline = _baseline_output(
        [_row("rev_001", "GOOD"), _row("rev_002", "BAD"), _row("rev_003", "GOOD")]
    )
    runner = _FakeRunner([_row("rev_002", "GOOD")])

    _run_phase(spark, config=_config(), baseline=baseline, runner=runner)

    assert runner.calls == [(SPACE_ID, ["rev_002"], mv_attach.LIFT_EVAL_LABEL)]


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
        runner.run_subset(SPACE_ID, AFFECTED, mv_attach.LIFT_EVAL_LABEL),
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
    assert "consent table unreadable" in (outcome.error or "")
    assert outcome.config is config


def test_parse_attach_views_ignores_a_malformed_parameter() -> None:
    assert mv_attach.parse_attach_views('{"not": "a list"}') == []
    assert mv_attach.parse_attach_views(None) == []
    assert mv_attach.parse_attach_views([MV_NAME, MV_NAME]) == [MV_NAME]


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

    def evaluate(*_args, **_kwargs):
        events.append("baseline_eval")
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


def _loop(
    monkeypatch, *, attach, accuracies, spark=None, raise_at=None, observed="live",
):
    """Drive the loop with a fixed attach outcome and accuracy trajectory.

    ``accuracies[0]`` is the baseline; the rest are candidate attempts. The
    applier and rollback are the real ones — the whole question in cases 1 and 2
    is what the real ``pre_snapshot`` contract does to a post-attach config.

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
    calls: list[str] = []

    def evaluate(*_args, **_kwargs):
        accuracy = next(evals)
        calls.append(f"eval:{accuracy}")
        if raise_at is not None and len(calls) >= raise_at:
            raise RuntimeError("optimize blew up mid-loop")
        # Six questions, all moving the same way: enough for the loop's paired
        # sign test to reach significance, so an improvement is actually accepted
        # rather than rejected on insufficient evidence.
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
    monkeypatch.setattr(unified_loop, "run_mv_attach_phase", lambda *_a, **_k: attach)
    monkeypatch.setattr(
        unified_loop,
        "_read_observed_config_after_evaluation",
        lambda *_a, **_k: (copy.deepcopy(attached_config) if observed == "live" else None),
    )
    monkeypatch.setattr(
        unified_loop,
        "propose_patches",
        lambda *_a, **_k: (
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
        ),
    )
    monkeypatch.setattr(unified_loop, "write_iteration", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "mark_patches_rolled_back", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "mark_iteration_rolled_back", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "write_patch", lambda *_a, **_k: None)
    monkeypatch.setattr(unified_loop, "_stamp_terminal", lambda *_a, **kw: stamped.update(kw))

    out = unified_loop.run_unified_optimization_loop(
        None,
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
    )
    return out, stamped


def _kept_attach() -> mv_attach.AttachOutcome:
    return mv_attach.AttachOutcome(
        status=mv_attach.STATUS_COMPLETE,
        verdict=mv_attach.VERDICT_ATTACHED,
        attached=(MV_NAME,),
        config=_config([{"identifier": MV_NAME}]),
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
