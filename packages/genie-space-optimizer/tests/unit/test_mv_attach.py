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

import copy
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

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
