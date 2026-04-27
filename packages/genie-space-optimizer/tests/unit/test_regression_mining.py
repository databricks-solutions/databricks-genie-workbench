"""Tests for the regression-mining module.

The mining module turns failed candidate eval rows into structured
``RegressionInsight`` values without touching acceptance, rollback,
or state loaders. The first insight type is ``column_confusion``,
emitted when a previously-passing question regressed and the
expected vs generated SQL pair carries column-confusion evidence
from :func:`detect_column_confusion`.

Mining is pure: no I/O, no LLM, no Genie SDK.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.regression_mining import (
    DECISION_AUDIT_DECISION,
    DECISION_AUDIT_GATE_NAME,
    DECISION_AUDIT_STAGE_LETTER,
    STRATEGIST_HINT_HEADER,
    RegressionInsight,
    build_decision_audit_rows,
    collect_insights_from_reflection_buffer,
    insight_to_audit_metrics,
    mine_regression_insights,
    recommended_patch_types_for_insight,
    render_strategist_hint_block,
    select_strategist_visible_insights,
    summarize_insights_for_reflection,
)


def _row(
    qid: str,
    expected_sql: str,
    generated_sql: str,
    *,
    response_text: str = "",
) -> dict:
    """Build an MLflow-flattened candidate eval row matching the live
    schema (``outputs.predictions.sql`` etc.).
    """
    return {
        "inputs.question_id": qid,
        "outputs.predictions.sql": generated_sql,
        "inputs.expected_sql": expected_sql,
        "outputs.predictions.response_text": response_text,
    }


# ── Happy path: column_confusion insight ─────────────────────────────


def test_emits_column_confusion_for_regressed_qid_with_swap():
    """Q-mtd regressed (was passing, now failing). Generated SQL used
    `use_mtdate_flag` while expected used `is_month_to_date` — that's
    a textbook abbreviation column confusion the optimizer should
    learn from."""

    failed_rows = [
        _row(
            "q-mtd",
            expected_sql=(
                "SELECT full_date FROM dim_date "
                "WHERE is_month_to_date = 'Y'"
            ),
            generated_sql=(
                "SELECT full_date FROM dim_date "
                "WHERE use_mtdate_flag = 'Y'"
            ),
        )
    ]

    insights = mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-mtd"],
    )

    assert len(insights) == 1
    insight = insights[0]
    assert isinstance(insight, RegressionInsight)
    assert insight.insight_type == "column_confusion"
    assert insight.question_id == "q-mtd"
    assert insight.intended_column == "is_month_to_date"
    assert insight.confused_column == "use_mtdate_flag"
    assert insight.source == "regression_mining"
    assert insight.recommended_patch_types  # non-empty


def test_recommends_lever1_metadata_patches_for_column_confusion():
    """Column confusion fixes are paired Lever 1 metadata changes:
    strengthen the intended column, clarify the confused column."""

    failed_rows = [
        _row(
            "q-mtd",
            expected_sql="SELECT a FROM t WHERE is_month_to_date = 'Y'",
            generated_sql="SELECT a FROM t WHERE use_mtdate_flag = 'Y'",
        )
    ]

    insights = mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-mtd"],
    )

    assert insights
    rec = set(insights[0].recommended_patch_types)
    assert "update_column_description" in rec
    assert "add_column_synonym" in rec


# ── Filtering: only regressed qids are mined ─────────────────────────


def test_skips_qids_not_in_regressed_set():
    failed_rows = [
        _row(
            "q-other",
            expected_sql="SELECT a FROM t WHERE is_month_to_date = 'Y'",
            generated_sql="SELECT a FROM t WHERE use_mtdate_flag = 'Y'",
        ),
        _row(
            "q-mtd",
            expected_sql="SELECT a FROM t WHERE is_month_to_date = 'Y'",
            generated_sql="SELECT a FROM t WHERE use_mtdate_flag = 'Y'",
        ),
    ]

    insights = mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-mtd"],
    )

    assert {i.question_id for i in insights} == {"q-mtd"}


def test_returns_empty_when_no_regressed_qids():
    failed_rows = [
        _row(
            "q-mtd",
            expected_sql="SELECT a FROM t WHERE is_month_to_date = 'Y'",
            generated_sql="SELECT a FROM t WHERE use_mtdate_flag = 'Y'",
        ),
    ]

    assert mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=[],
    ) == []


def test_returns_empty_when_no_failed_rows():
    assert mine_regression_insights(
        failed_eval_rows=[],
        regressed_qids=["q-mtd"],
    ) == []


# ── No insight when the failure surface lacks column-confusion ───────


def test_returns_empty_when_no_column_confusion_in_pair():
    """A regressed qid whose SQL pair has no column overlap pattern
    must produce no insight — mining must not invent evidence."""

    failed_rows = [
        _row(
            "q-other",
            expected_sql="SELECT customer_id FROM fact_orders",
            generated_sql="SELECT order_total FROM fact_orders",
        ),
    ]

    assert mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-other"],
    ) == []


def test_handles_missing_sql_gracefully():
    failed_rows = [
        {"inputs.question_id": "q-x"},  # no SQL keys at all
    ]

    assert mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-x"],
    ) == []


# ── Multiple insights per row are deduped ────────────────────────────


def test_multiple_clauses_for_same_pair_collapse_to_unique_insights():
    """If the same column pair is detected in multiple clauses, mining
    must not emit duplicate ``column_confusion`` insights for the same
    (qid, intended, confused). The clause with highest confidence wins.
    """

    failed_rows = [
        _row(
            "q-mtd",
            expected_sql=(
                "SELECT is_month_to_date FROM t "
                "WHERE is_month_to_date = 'Y'"
            ),
            generated_sql=(
                "SELECT use_mtdate_flag FROM t "
                "WHERE use_mtdate_flag = 'Y'"
            ),
        ),
    ]

    insights = mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-mtd"],
    )

    keys = {(i.question_id, i.intended_column, i.confused_column) for i in insights}
    assert len(keys) == len(insights), insights


# ── Confidence is a stable float in [0, 1] ───────────────────────────


def test_insight_confidence_is_normalized_float():
    failed_rows = [
        _row(
            "q-mtd",
            expected_sql="SELECT a FROM t WHERE is_month_to_date = 'Y'",
            generated_sql="SELECT a FROM t WHERE use_mtdate_flag = 'Y'",
        ),
    ]

    insights = mine_regression_insights(
        failed_eval_rows=failed_rows,
        regressed_qids=["q-mtd"],
    )

    assert insights
    for i in insights:
        assert isinstance(i.confidence, float)
        assert 0.0 < i.confidence <= 1.0


# ── Persistence projections ──────────────────────────────────────────


def _sample_insight(qid: str = "q-mtd") -> RegressionInsight:
    return RegressionInsight(
        insight_type="column_confusion",
        question_id=qid,
        intended_column="is_month_to_date",
        confused_column="use_mtdate_flag",
        table="dim_date",
        sql_clause="WHERE",
        confidence=0.82,
        rationale="Generated SQL substituted use_mtdate_flag for is_month_to_date in WHERE.",
        recommended_patch_types=("update_column_description", "add_column_synonym"),
    )


def test_insight_to_audit_metrics_round_trips_columns_and_clause():
    """The audit-metrics projection must preserve enough evidence for a
    SQL query to reconstruct the lesson without re-running the miner."""

    metrics = insight_to_audit_metrics(_sample_insight())

    assert metrics["insight_type"] == "column_confusion"
    assert metrics["source"] == "regression_mining"
    assert metrics["question_id"] == "q-mtd"
    assert metrics["intended_column"] == "is_month_to_date"
    assert metrics["confused_column"] == "use_mtdate_flag"
    assert metrics["table"] == "dim_date"
    assert metrics["sql_clause"] == "WHERE"
    assert metrics["confidence"] == 0.82
    assert "recommended_patch_types" in metrics
    assert isinstance(metrics["recommended_patch_types"], list)
    assert "update_column_description" in metrics["recommended_patch_types"]


def test_summarize_insights_for_reflection_truncates_and_keeps_total():
    """The reflection summary must cap items but always report the full
    count so run history shows mining produced N insights even when the
    JSON only embeds the top few."""

    insights = [_sample_insight(qid=f"q-{i}") for i in range(10)]

    summary = summarize_insights_for_reflection(insights, max_items=3)

    assert summary["total"] == 10
    assert len(summary["items"]) == 3
    first = summary["items"][0]
    assert first["insight_type"] == "column_confusion"
    assert first["intended_column"] == "is_month_to_date"
    assert first["confused_column"] == "use_mtdate_flag"
    assert "rationale" not in first  # summary is intentionally compact
    # ``confidence`` is rounded for compactness and stays JSON-safe.
    assert isinstance(first["confidence"], float)


def test_build_decision_audit_rows_matches_writer_schema():
    """``build_decision_audit_rows`` must produce dicts the existing
    ``write_lever_loop_decisions`` writer accepts: stable gate_name,
    decision, stage_letter; ag_id and run_id pass through; one row per
    insight with monotonic ``decision_order``; reason_code carries the
    insight type so SQL filters work without parsing metrics_json."""

    insights = [_sample_insight("q-1"), _sample_insight("q-2")]

    rows = build_decision_audit_rows(
        insights, run_id="run-x", iteration=7, ag_id="ag-3",
    )

    assert len(rows) == 2

    for idx, row in enumerate(rows, start=1):
        assert row["run_id"] == "run-x"
        assert row["iteration"] == 7
        assert row["ag_id"] == "ag-3"
        assert row["decision_order"] == idx
        assert row["stage_letter"] == DECISION_AUDIT_STAGE_LETTER
        assert row["gate_name"] == DECISION_AUDIT_GATE_NAME
        assert row["decision"] == DECISION_AUDIT_DECISION
        assert row["reason_code"] == "column_confusion"
        assert row["reason_detail"]
        # Affected qids are list-typed so the writer can JSON-serialize
        # them without an extra round-trip.
        assert isinstance(row["affected_qids"], list)
        assert isinstance(row["metrics"], dict)
        assert row["metrics"]["intended_column"] == "is_month_to_date"

    # Per-row qid identity is preserved.
    assert rows[0]["affected_qids"] == ["q-1"]
    assert rows[1]["affected_qids"] == ["q-2"]


def test_build_decision_audit_rows_caps_long_rationale():
    """Decision audit's ``reason_detail`` is bounded; the helper must
    enforce a 1000-char ceiling so the writer never silently drops a
    row that exceeds the column's safe size."""

    long_rationale = "x" * 5000
    insight = RegressionInsight(
        insight_type="column_confusion",
        question_id="q-long",
        intended_column="a",
        confused_column="b",
        rationale=long_rationale,
    )

    rows = build_decision_audit_rows(
        [insight], run_id="run-y", iteration=1, ag_id=None,
    )

    assert len(rows[0]["reason_detail"]) == 1000


def test_build_decision_audit_rows_empty_input_is_noop():
    assert build_decision_audit_rows(
        [], run_id="run-z", iteration=0, ag_id="ag-1",
    ) == []


# ── Strategist input path (feature-flagged) ──────────────────────────


def test_select_strategist_visible_returns_empty_when_disabled():
    """The flag is the kill switch — flag-off means no insights flow
    to the strategist regardless of confidence."""
    insights = [_sample_insight()]
    assert select_strategist_visible_insights(
        insights, min_confidence=0.5, enabled=False,
    ) == []


def test_select_strategist_visible_filters_low_confidence():
    """Insights below the strategist threshold are excluded even with
    the flag on. The audit lane keeps everything; the strategist sees
    only high-evidence lessons."""
    high = _sample_insight("q-high")
    low = RegressionInsight(
        insight_type="column_confusion",
        question_id="q-low",
        intended_column="a",
        confused_column="b",
        confidence=0.3,
    )
    visible = select_strategist_visible_insights(
        [high, low], min_confidence=0.7, enabled=True,
    )
    assert len(visible) == 1
    assert visible[0].question_id == "q-high"


def test_select_strategist_visible_drops_blank_columns():
    """A column_confusion insight with empty intended/confused is
    actionable for the strategist only as noise — skip it."""
    blank = RegressionInsight(
        insight_type="column_confusion",
        question_id="q-blank",
        intended_column="",
        confused_column="",
        confidence=0.99,
    )
    assert select_strategist_visible_insights(
        [blank], min_confidence=0.7, enabled=True,
    ) == []


def test_collect_insights_from_reflection_buffer_round_trips_summary():
    """The harness writes ``reflection["regression_mining"]`` via
    ``summarize_insights_for_reflection``; the strategist input path
    must be able to reconstruct insights from that summary alone (so
    it doesn't have to re-query the audit table)."""

    insights = [_sample_insight("q-1"), _sample_insight("q-2")]
    summary = summarize_insights_for_reflection(insights, max_items=10)

    buffer = [
        {"iteration": 1, "regression_mining": summary},
        {"iteration": 2},  # no mining on this iter
        {"iteration": 3, "regression_mining": {"total": 0, "items": []}},
    ]

    rebuilt = collect_insights_from_reflection_buffer(buffer)

    assert len(rebuilt) == 2
    qids = {i.question_id for i in rebuilt}
    assert qids == {"q-1", "q-2"}
    for r in rebuilt:
        assert r.intended_column == "is_month_to_date"
        assert r.confused_column == "use_mtdate_flag"


def test_collect_insights_handles_empty_or_malformed_buffer():
    """Malformed reflection entries (missing keys, wrong types) must
    never raise — the harness logs and continues."""

    assert collect_insights_from_reflection_buffer([]) == []
    assert collect_insights_from_reflection_buffer(
        [{"regression_mining": "not-a-dict"}, "garbage"],  # type: ignore[list-item]
    ) == []
    # Shape-correct but value-broken items: skip silently.
    assert collect_insights_from_reflection_buffer([
        {"regression_mining": {"items": [{"confidence": "nope"}]}},
    ]) == []


def test_render_strategist_hint_block_formats_column_confusion():
    """The rendered block must include the contrastive intended/confused
    column pair and the recommended patch types so the strategist can
    parse intent without ambiguity."""

    insights = [_sample_insight()]
    block = render_strategist_hint_block(insights)

    assert STRATEGIST_HINT_HEADER in block
    assert "is_month_to_date" in block
    assert "use_mtdate_flag" in block
    assert "update_column_description" in block
    assert "add_column_synonym" in block
    # Clause label is preserved verbatim from the insight.
    assert "WHERE" in block


def test_render_strategist_hint_block_does_not_leak_question_text_or_sql():
    """Leak safety: rendered text must contain only column identifiers,
    clause names, and patch-type names. Question-id values are mining-
    internal and must not be rendered into the strategist prompt."""

    ins = RegressionInsight(
        insight_type="column_confusion",
        question_id="q-secret-benchmark-id",
        intended_column="customer_segment",
        confused_column="cust_seg_code",
        sql_clause="WHERE",
        confidence=0.9,
        rationale="should-not-appear in prompt",
        recommended_patch_types=("update_column_description",),
    )
    block = render_strategist_hint_block([ins])

    assert "q-secret-benchmark-id" not in block
    assert "should-not-appear" not in block
    # The columns themselves are space metadata and DO appear.
    assert "customer_segment" in block
    assert "cust_seg_code" in block


def test_render_strategist_hint_block_dedups_repeating_pairs():
    """Same column pair mined across multiple qids renders once so the
    prompt stays compact and the lesson isn't over-weighted."""

    a = _sample_insight("q-1")
    b = _sample_insight("q-2")
    c = _sample_insight("q-3")
    block = render_strategist_hint_block([a, b, c])

    assert block.count("is_month_to_date") == 1


def test_render_strategist_hint_block_truncates_to_max_items():
    insights = [
        RegressionInsight(
            insight_type="column_confusion",
            question_id=f"q-{i}",
            intended_column=f"col_{i}_a",
            confused_column=f"col_{i}_b",
            sql_clause="WHERE",
            confidence=0.95,
            recommended_patch_types=("update_column_description",),
        )
        for i in range(20)
    ]
    block = render_strategist_hint_block(insights, max_items=3)
    # 3 hint lines under the header; each begins with ``\n- `` because
    # the header sits on the line above the first hint.
    assert block.count("\n- ") == 3


def test_render_strategist_hint_block_empty_input_returns_empty_string():
    """Empty insights → empty block (no dangling header), so callers
    can append unconditionally with no formatting hazards."""

    assert render_strategist_hint_block([]) == ""


# ── Patch-type routing (contrastive intent, no example-SQL leakage) ──


def test_routing_recommends_lever1_only_for_column_confusion():
    """Column-confusion insights must route to safer Lever 1 metadata
    fixes (column description + synonym) — never directly to Lever 5
    example_sql, which has higher leakage risk and a separate AFS-driven
    proposal path."""

    insight = _sample_insight()
    rec = recommended_patch_types_for_insight(insight)

    assert "update_column_description" in rec
    assert "add_column_synonym" in rec
    # Explicit guard against future regressions that would push mining
    # toward example-SQL recommendations from this routing helper.
    assert "add_example_sql" not in rec
    assert "add_example_question_sql" not in rec


def test_routing_unknown_insight_types_yield_empty_tuple():
    """Unknown insight types must produce no recommendations so
    additions to the analyzer don't accidentally trigger patches the
    routing helper hasn't been taught about."""
    novel = RegressionInsight(
        insight_type="hypothetical_new_signal",
        question_id="q-x",
        confidence=0.9,
    )
    assert recommended_patch_types_for_insight(novel) == ()


def test_mined_insights_all_carry_routing_recommendations():
    """End-to-end: when the miner emits a column-confusion insight,
    the recommendation list must already be populated via the routing
    helper. The strategist hint block and decision-audit projection
    both rely on this."""

    failed_rows = [
        _row(
            "q-mtd",
            expected_sql="SELECT a FROM t WHERE is_month_to_date = 'Y'",
            generated_sql="SELECT a FROM t WHERE use_mtdate_flag = 'Y'",
        )
    ]
    insights = mine_regression_insights(
        failed_eval_rows=failed_rows, regressed_qids=["q-mtd"],
    )
    assert insights
    for ins in insights:
        rec = ins.recommended_patch_types
        assert rec, "miner must populate recommended_patch_types"
        assert set(rec) == set(recommended_patch_types_for_insight(ins))
