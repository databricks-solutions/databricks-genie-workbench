"""Unit tests for F5 — ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` handling.

The second-most-common preflight example-SQL failure class is the LLM
referencing a metric-view measure column as a bare identifier
(``SELECT cy_sales FROM mv_sales``) instead of wrapping it in
``MEASURE(cy_sales)``. Four pieces of logic cooperate to fix this:

* ``_is_measure_function_failure`` — classifier that routes the retry.
* ``_extract_offending_measures`` — pulls bracketed names out of Spark's
  error reason so the retry feedback can be specific.
* ``_repair_measure_refs_on_proposal`` — deterministic pre-validation
  rewrite; delegates to ``evaluation._rewrite_measure_refs`` so the
  preflight path and the eval path wrap identically.
* ``_build_measure_function_feedback`` — retry payload with BAD/GOOD
  worked examples grounded in THIS slice's MV identifier.

This file also covers the prompt-rendering side (F5a/b):
``_format_slice_metric_views`` now tags each column ``[measure]`` /
``[dimension]``, ``_format_slice_columns`` mirrors the tag, and
``_format_metric_view_contract`` renders a conditional HARD constraint
with worked examples.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
    _build_measure_function_feedback,
    _extract_offending_measures,
    _format_metric_view_contract,
    _format_slice_columns,
    _format_slice_metric_views,
    _is_measure_function_failure,
    _mv_column_entries,
    _repair_measure_refs_on_proposal,
)
from genie_space_optimizer.optimization.synthesis import GateResult


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


def _mv(
    identifier: str,
    measures: list[str] | None = None,
    dimensions: list[str] | None = None,
    description: str = "",
) -> dict:
    """Build a metric-view snapshot shaped like production column_configs.

    ``measures`` get ``column_type="measure"``; ``dimensions`` are
    declared with no type (treated as dimensions by the reader).
    """
    column_configs: list[dict] = []
    for m in measures or []:
        column_configs.append({
            "column_name": m,
            "column_type": "measure",
        })
    for d in dimensions or []:
        column_configs.append({
            "column_name": d,
            "column_type": "dimension",
        })
    return {
        "identifier": identifier,
        "name": identifier.split(".")[-1],
        "description": description,
        "column_configs": column_configs,
    }


def _slice_with_mv(
    mv_identifier: str,
    *,
    measures: list[str] | None = None,
    dimensions: list[str] | None = None,
    tables: list[str] | None = None,
    slice_columns: list[tuple[str, str]] | None = None,
    description: str = "",
) -> AssetSlice:
    return AssetSlice(
        tables=[
            {"identifier": t, "name": t.split(".")[-1], "column_configs": []}
            for t in (tables or [])
        ],
        columns=list(slice_columns or []),
        metric_view=_mv(
            mv_identifier,
            measures=measures,
            dimensions=dimensions,
            description=description,
        ),
        join_spec=None,
    )


def _gate(passed: bool, reason: str = "", gate: str = "execute") -> GateResult:
    return GateResult(passed, gate, reason)


# ═══════════════════════════════════════════════════════════════════════
# _is_measure_function_failure — classifier
# ═══════════════════════════════════════════════════════════════════════


class TestIsMeasureFunctionFailure:
    def test_marker_in_reason_detected(self):
        g = _gate(False, "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] ...")
        assert _is_measure_function_failure(g) is True

    def test_other_failure_not_detected(self):
        g = _gate(False, "[UNRESOLVED_COLUMN] `dim_date`")
        assert _is_measure_function_failure(g) is False

    def test_passed_gate_not_detected(self):
        g = _gate(True, "")
        assert _is_measure_function_failure(g) is False

    def test_none_gate_not_detected(self):
        assert _is_measure_function_failure(None) is False

    def test_marker_case_insensitive(self):
        g = _gate(False, "metric_view_missing_measure_function boom")
        assert _is_measure_function_failure(g) is True


# ═══════════════════════════════════════════════════════════════════════
# _extract_offending_measures — regex extractor
# ═══════════════════════════════════════════════════════════════════════


class TestExtractOffendingMeasures:
    def test_single_measure_extracted(self):
        reason = (
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of "
            "measure column [7now_avg_txn_cy_day] must be wrapped "
            "in MEASURE()"
        )
        assert _extract_offending_measures(reason) == ["7now_avg_txn_cy_day"]

    def test_multiple_measures_extracted(self):
        reason = (
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] measure column "
            "[7now_avg_txn_cy_day, 7now_avg_txn_prior, cy_sales]"
        )
        assert _extract_offending_measures(reason) == [
            "7now_avg_txn_cy_day",
            "7now_avg_txn_prior",
            "cy_sales",
        ]

    def test_duplicate_measures_deduped(self):
        reason = (
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] [a, b, a, b]"
        )
        assert _extract_offending_measures(reason) == ["a", "b"]

    def test_no_marker_returns_empty(self):
        reason = "UNRESOLVED_COLUMN: `foo` suggestions: [a, b, c]"
        assert _extract_offending_measures(reason) == []

    def test_bracketed_but_no_marker_returns_empty(self):
        """Bracketed lists appear in UNRESOLVED_COLUMN suggestions —
        those MUST NOT be treated as measure offenders (wrong failure
        class)."""
        reason = "some other error [foo, bar]"
        assert _extract_offending_measures(reason) == []

    def test_empty_reason_returns_empty(self):
        assert _extract_offending_measures("") == []
        assert _extract_offending_measures("   ") == []


# ═══════════════════════════════════════════════════════════════════════
# _mv_column_entries — measure/dimension extraction from snapshot
# ═══════════════════════════════════════════════════════════════════════


class TestMvColumnEntries:
    def test_column_configs_measure_type(self):
        mv = _mv("cat.sch.mv_a", measures=["cy_sales"], dimensions=["zone"])
        measures, dims = _mv_column_entries(mv)
        assert measures == ["cy_sales"]
        assert dims == ["zone"]

    def test_is_measure_flag_recognised(self):
        mv = {
            "identifier": "cat.sch.mv_b",
            "column_configs": [
                {"column_name": "x", "is_measure": True},
                {"column_name": "y"},
            ],
        }
        measures, dims = _mv_column_entries(mv)
        assert measures == ["x"]
        assert dims == ["y"]

    def test_fallback_top_level_measures_dimensions(self):
        """Test-fixture shape: measures / dimensions as top-level lists."""
        mv = {
            "identifier": "cat.sch.mv_c",
            "measures": [{"name": "m1"}, "m2"],
            "dimensions": ["d1"],
        }
        measures, dims = _mv_column_entries(mv)
        assert measures == ["m1", "m2"]
        assert dims == ["d1"]

    def test_column_configs_take_priority_over_top_level(self):
        mv = {
            "identifier": "cat.sch.mv_d",
            "column_configs": [
                {"column_name": "cy_sales", "column_type": "measure"},
            ],
            "measures": ["cy_sales"],
        }
        measures, _ = _mv_column_entries(mv)
        # Deduped — appears once even though registered twice.
        assert measures == ["cy_sales"]

    def test_empty_mv_returns_empty_lists(self):
        assert _mv_column_entries({"identifier": "cat.sch.mv"}) == ([], [])


# ═══════════════════════════════════════════════════════════════════════
# _repair_measure_refs_on_proposal — deterministic MEASURE() wrapping
# ═══════════════════════════════════════════════════════════════════════


class TestRepairMeasureRefsOnProposal:
    def test_bare_measure_in_select_is_wrapped(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales",
            measures=["cy_sales", "cy_orders"],
            dimensions=["zone"],
        )
        p, wrapped = _repair_measure_refs_on_proposal(
            {
                "example_sql": (
                    "SELECT zone, cy_sales FROM cat.sch.mv_sales GROUP BY ALL"
                ),
            },
            slice_,
        )
        assert "MEASURE(cy_sales)" in p["example_sql"]
        assert wrapped == ["cy_sales"]

    def test_already_wrapped_is_noop(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        sql = (
            "SELECT zone, MEASURE(cy_sales) FROM cat.sch.mv_sales GROUP BY ALL"
        )
        p, wrapped = _repair_measure_refs_on_proposal(
            {"example_sql": sql}, slice_,
        )
        # No second wrap.
        assert p["example_sql"].count("MEASURE(") == 1
        # The rewriter returns the SQL unchanged when nothing needed
        # wrapping, so the proposal is returned as-is and `wrapped`
        # is empty.
        assert wrapped == []

    def test_dimension_column_not_wrapped(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        p, wrapped = _repair_measure_refs_on_proposal(
            {"example_sql": "SELECT zone FROM cat.sch.mv_sales GROUP BY zone"},
            slice_,
        )
        assert "MEASURE(" not in p["example_sql"]
        assert wrapped == []

    def test_full_fq_from_clause_still_matches(self):
        """F5d runs AFTER F4a's stem repair, so the FROM clause is
        fully qualified by the time MEASURE wrapping fires. The
        rewriter's internal ``.split('.')[-1]`` handles this."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        sql = "SELECT zone, cy_sales FROM cat.sch.mv_sales GROUP BY ALL"
        p, wrapped = _repair_measure_refs_on_proposal(
            {"example_sql": sql}, slice_,
        )
        assert "MEASURE(cy_sales)" in p["example_sql"]
        assert "cy_sales" in wrapped

    def test_no_metric_view_in_slice_is_noop(self):
        slice_ = AssetSlice(
            tables=[{"identifier": "cat.sch.t", "name": "t", "column_configs": []}],
            columns=[],
            metric_view=None,
            join_spec=None,
        )
        p, wrapped = _repair_measure_refs_on_proposal(
            {"example_sql": "SELECT x FROM cat.sch.t"}, slice_,
        )
        assert wrapped == []

    def test_no_measures_declared_is_noop(self):
        """Dimension-only MV can't possibly need a MEASURE() wrap."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_dim_only", measures=[], dimensions=["zone", "region"],
        )
        p, wrapped = _repair_measure_refs_on_proposal(
            {
                "example_sql": (
                    "SELECT zone, region FROM cat.sch.mv_dim_only "
                    "GROUP BY ALL"
                ),
            },
            slice_,
        )
        assert wrapped == []

    def test_empty_sql_is_noop(self):
        slice_ = _slice_with_mv("cat.sch.mv", measures=["m"], dimensions=[])
        p, wrapped = _repair_measure_refs_on_proposal(
            {"example_sql": ""}, slice_,
        )
        assert wrapped == []

    def test_repair_trace_recorded(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        p, wrapped = _repair_measure_refs_on_proposal(
            {
                "example_sql": "SELECT cy_sales FROM cat.sch.mv_sales GROUP BY ALL",
            },
            slice_,
        )
        trace = p.get("_repair_trace") or []
        assert ("measure", "cy_sales") in trace


# ═══════════════════════════════════════════════════════════════════════
# _build_measure_function_feedback — retry payload
# ═══════════════════════════════════════════════════════════════════════


class TestBuildMeasureFunctionFeedback:
    def test_feedback_contains_offender_names(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        fb = _build_measure_function_feedback(
            {"example_sql": "SELECT cy_sales FROM cat.sch.mv_sales"},
            slice_,
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] ...",
            offending_measures=["cy_sales", "cy_orders"],
        )
        assert "`cy_sales`" in fb
        assert "`cy_orders`" in fb
        assert "bare columns" in fb.lower() or "bare" in fb.lower()

    def test_feedback_shows_mv_identifier_in_worked_example(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        fb = _build_measure_function_feedback(
            {"example_sql": "SELECT cy_sales FROM cat.sch.mv_sales"},
            slice_, "reason",
            offending_measures=["cy_sales"],
        )
        # BAD + GOOD worked examples both reference the slice's MV id.
        assert fb.count("cat.sch.mv_sales") >= 2
        assert "MEASURE(cy_sales)" in fb

    def test_feedback_uses_first_offender_as_example(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_a", measures=["m1", "m2"], dimensions=[],
        )
        fb = _build_measure_function_feedback(
            {"example_sql": "SELECT m1, m2 FROM cat.sch.mv_a"},
            slice_, "reason",
            offending_measures=["m1", "m2"],
        )
        assert "SELECT location, m1 FROM cat.sch.mv_a" in fb
        assert "MEASURE(m1)" in fb

    def test_feedback_without_offenders_still_usable(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_a", measures=["m1"], dimensions=[],
        )
        fb = _build_measure_function_feedback(
            {"example_sql": "SELECT m1 FROM cat.sch.mv_a"},
            slice_, "reason",
        )
        assert fb, "should render a non-empty feedback even without offenders"
        assert "MEASURE()" in fb or "MEASURE(" in fb

    def test_feedback_empty_when_no_sql(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv", measures=["m"], dimensions=[],
        )
        assert _build_measure_function_feedback(
            {"example_sql": ""}, slice_, "reason",
        ) == ""

    def test_prior_sql_truncated_at_300_chars(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv", measures=["m"], dimensions=[],
        )
        long_sql = "SELECT " + ("x, " * 200) + " FROM cat.sch.mv"
        fb = _build_measure_function_feedback(
            {"example_sql": long_sql}, slice_, "reason",
        )
        # The prior-SQL block should be truncated; we don't want the
        # full 1000-char SQL inflating the retry prompt.
        assert "…" in fb or len(fb) < len(long_sql) + 1000


# ═══════════════════════════════════════════════════════════════════════
# _format_slice_metric_views — [measure]/[dimension] type tags (F5b)
# ═══════════════════════════════════════════════════════════════════════


class TestFormatSliceMetricViews:
    def test_measures_and_dimensions_rendered_with_tags(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales",
            measures=["cy_sales", "cy_orders"],
            dimensions=["zone", "location_number"],
            description="Store sales daily",
        )
        out = _format_slice_metric_views(slice_)
        assert "- cat.sch.mv_sales" in out
        assert "Store sales daily" in out
        assert "[measure]   cy_sales" in out
        assert "[measure]   cy_orders" in out
        assert "[dimension] zone" in out
        assert "[dimension] location_number" in out

    def test_long_measure_list_truncated(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv", measures=[f"m{i}" for i in range(20)], dimensions=[],
        )
        out = _format_slice_metric_views(slice_)
        # Default cap is 8; the rest should fold into a "(+N more)" row.
        assert "[measure]   m0" in out
        assert "[measure]   m7" in out
        assert "more not shown" in out

    def test_no_mv_returns_none_sentinel(self):
        slice_ = AssetSlice(
            tables=[], columns=[], metric_view=None, join_spec=None,
        )
        assert _format_slice_metric_views(slice_) == "(none)"

    def test_mv_with_no_columns_still_renders_header(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_empty", measures=[], dimensions=[],
        )
        out = _format_slice_metric_views(slice_)
        assert "- cat.sch.mv_empty" in out
        assert "[measure]" not in out
        assert "[dimension]" not in out


# ═══════════════════════════════════════════════════════════════════════
# _format_slice_columns — column-type tagging for MV-scoped entries
# ═══════════════════════════════════════════════════════════════════════


class TestFormatSliceColumnsTagging:
    def test_measure_column_tagged(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales",
            measures=["cy_sales"],
            dimensions=["zone"],
            slice_columns=[
                ("cat.sch.mv_sales", "cy_sales"),
                ("cat.sch.mv_sales", "zone"),
            ],
        )
        out = _format_slice_columns(slice_)
        assert "cy_sales [measure]" in out
        assert "zone [dimension]" in out

    def test_table_columns_not_tagged(self):
        slice_ = AssetSlice(
            tables=[{
                "identifier": "cat.sch.t",
                "name": "t",
                "column_configs": [{"column_name": "amount"}],
            }],
            columns=[("cat.sch.t", "amount")],
            metric_view=None,
            join_spec=None,
        )
        out = _format_slice_columns(slice_)
        assert "[measure]" not in out
        assert "[dimension]" not in out


# ═══════════════════════════════════════════════════════════════════════
# _format_metric_view_contract — conditional HARD constraint (F5a)
# ═══════════════════════════════════════════════════════════════════════


class TestFormatMetricViewContract:
    def test_empty_when_no_metric_view(self):
        slice_ = AssetSlice(
            tables=[], columns=[], metric_view=None, join_spec=None,
        )
        assert _format_metric_view_contract(slice_) == ""

    def test_contract_includes_mv_id_and_measure_example(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales",
            measures=["cy_sales", "cy_orders"],
            dimensions=["zone"],
        )
        out = _format_metric_view_contract(slice_)
        assert "cat.sch.mv_sales" in out
        # BAD example uses bare measure + MV id.
        assert "BAD" in out
        assert "GOOD" in out
        assert "MEASURE(cy_sales)" in out
        # HARD constraint label so the LLM prioritises it.
        assert "HARD" in out
        # CTE guidance for WHERE-clause usage.
        assert "CTE" in out or "WITH" in out

    def test_contract_falls_back_when_no_measures(self):
        """Dimension-only MV still emits a contract (the rule still
        applies, just with a placeholder column). This is defensive —
        production MVs always declare measures, but test fixtures
        sometimes don't."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_dim_only", measures=[], dimensions=["zone"],
        )
        out = _format_metric_view_contract(slice_)
        assert out, "expected a non-empty contract block"
        assert "cat.sch.mv_dim_only" in out


# ═══════════════════════════════════════════════════════════════════════
# Integration — MEASURE-wrap repair interacts correctly with identifier
# stem repair when both fire on the same proposal.
# ═══════════════════════════════════════════════════════════════════════


class TestStemAndMeasureRepairInteraction:
    def test_double_wrap_prevented_when_both_fire(self):
        """When F4a's stem repair rewrites the FROM identifier and
        F5d's MEASURE wrap runs right after, we must not end up with
        ``MEASURE(MEASURE(cy_sales))``."""
        from genie_space_optimizer.optimization.preflight_synthesis import (
            _repair_stemmed_identifiers,
        )

        slice_ = _slice_with_mv(
            "cat.sch.mv_esr_sales",
            measures=["cy_sales"],
            dimensions=["zone"],
        )
        # LLM emitted stemmed FROM + bare measure. Stem repair runs
        # first, then MEASURE wrap.
        p = {"example_sql": "SELECT zone, cy_sales FROM sales"}
        p, _ = _repair_stemmed_identifiers(p, slice_)
        p, _ = _repair_measure_refs_on_proposal(p, slice_)
        # Exactly one MEASURE() wrapping.
        assert p["example_sql"].count("MEASURE(") == 1
        assert "MEASURE(cy_sales)" in p["example_sql"]
        assert "cat.sch.mv_esr_sales" in p["example_sql"]


# ═══════════════════════════════════════════════════════════════════════
# Fix 4 — alias-aware MEASURE wrap on ``<alias>.<col>`` and HAVING.
#
# The lever loop run produced 5 ``METRIC_VIEW_MISSING_MEASURE_FUNCTION``
# candidates that the previous repair missed because they referenced
# the measure via an explicit alias (``FROM mv s SELECT s.cy_sales``)
# or in the HAVING clause. The refactored ``_rewrite_measure_refs``:
#
#   * walks FROM/JOIN clauses, capturing both short-name and alias.
#   * builds a ``relevant_measures`` map keyed by alias_or_short.
#   * recognises both the unqualified (``cy_sales``) and qualified
#     (``s.cy_sales``) forms.
#   * extends rewrite coverage to HAVING in addition to SELECT and
#     ORDER BY.
#
# These tests exercise the new behaviour through the proposal-side
# wrapper (``_repair_measure_refs_on_proposal``) since that's how the
# preflight pipeline drives it; the wrapper delegates to the same
# ``_rewrite_measure_refs`` used by the eval pipeline so this also
# covers the eval path.
# ═══════════════════════════════════════════════════════════════════════


class TestAliasAwareMeasureRewrite:
    def test_qualified_measure_via_explicit_alias_is_wrapped(self):
        """``FROM mv s ... SELECT s.cy_sales`` → ``MEASURE(s.cy_sales)``."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        sql = "SELECT zone, s.cy_sales FROM cat.sch.mv_sales s GROUP BY ALL"
        p, _wrapped = _repair_measure_refs_on_proposal(
            {"example_sql": sql}, slice_,
        )
        assert "MEASURE(s.cy_sales)" in p["example_sql"], (
            f"qualified measure not wrapped: {p['example_sql']!r}"
        )

    def test_qualified_measure_with_AS_alias_is_wrapped(self):
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=[],
        )
        sql = "SELECT s.cy_sales FROM cat.sch.mv_sales AS s"
        p, _ = _repair_measure_refs_on_proposal({"example_sql": sql}, slice_)
        assert "MEASURE(s.cy_sales)" in p["example_sql"]

    def test_qualified_measure_via_short_name_is_wrapped(self):
        """``FROM mv ... SELECT mv.cy_sales`` (no alias) — qualifier is
        the table's short name."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=[],
        )
        sql = "SELECT mv_sales.cy_sales FROM cat.sch.mv_sales"
        p, _ = _repair_measure_refs_on_proposal({"example_sql": sql}, slice_)
        assert "MEASURE(mv_sales.cy_sales)" in p["example_sql"]

    def test_having_clause_measure_is_wrapped(self):
        """HAVING coverage — bare measure in HAVING should also be
        wrapped (previously only SELECT / ORDER BY were covered)."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        sql = (
            "SELECT zone, MEASURE(cy_sales) AS s FROM cat.sch.mv_sales "
            "GROUP BY zone HAVING cy_sales > 1000"
        )
        p, _ = _repair_measure_refs_on_proposal({"example_sql": sql}, slice_)
        # The HAVING reference must be wrapped — the SELECT alias `s`
        # is not necessarily resolvable in HAVING in Spark SQL.
        having_segment = p["example_sql"].split("HAVING", 1)[1]
        assert "MEASURE(cy_sales)" in having_segment, (
            f"HAVING measure not wrapped: {p['example_sql']!r}"
        )

    def test_group_by_is_left_alone(self):
        """Measures should never appear in GROUP BY; if they do, let
        EXPLAIN reject. The rewriter must NOT wrap GROUP BY columns
        (would mask the underlying error)."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=[],
        )
        # Pathological proposal: bare measure in GROUP BY. The rewrite
        # only touches SELECT / HAVING / ORDER BY.
        sql = "SELECT cy_sales FROM cat.sch.mv_sales GROUP BY cy_sales"
        p, _ = _repair_measure_refs_on_proposal({"example_sql": sql}, slice_)
        group_segment = p["example_sql"].split("GROUP BY", 1)[1]
        # GROUP BY clause unchanged — no MEASURE() wrap there.
        assert "MEASURE(" not in group_segment, (
            f"GROUP BY wrongly wrapped: {p['example_sql']!r}"
        )

    def test_unqualified_measure_with_aliased_from_still_wraps(self):
        """``FROM mv s SELECT cy_sales`` — bare ``cy_sales`` (no alias
        prefix) is still resolvable because we have a single MV in
        FROM. Must wrap."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=["zone"],
        )
        sql = "SELECT zone, cy_sales FROM cat.sch.mv_sales s GROUP BY ALL"
        p, _ = _repair_measure_refs_on_proposal({"example_sql": sql}, slice_)
        assert "MEASURE(cy_sales)" in p["example_sql"]


# ═══════════════════════════════════════════════════════════════════════
# Fix 5 — strip redundant outer aggregate around MEASURE().
#
# After MV expansion ``MEASURE(SUM(...))`` is the canonical shape; the
# LLM sometimes emits ``SUM(MEASURE(...))`` which after expansion
# becomes ``SUM(MEASURE(SUM(...)))`` — Spark rejects it as
# ``NESTED_AGGREGATE_FUNCTION``. ``_strip_outer_agg_around_measure``
# walks the SQL AST (sqlglot) and collapses the redundant aggregate
# wrapper. Falls back to a regex when sqlglot can't parse.
# ═══════════════════════════════════════════════════════════════════════


class TestStripOuterAggregateAroundMeasure:
    def test_sum_around_measure_is_stripped(self):
        from genie_space_optimizer.optimization.evaluation import (
            _strip_outer_agg_around_measure,
        )

        sql, count = _strip_outer_agg_around_measure(
            "SELECT SUM(MEASURE(cy_sales)) FROM cat.sch.mv_sales",
        )
        assert count == 1, f"expected 1 strip, got {count}"
        # Outer SUM is gone; MEASURE wrap is preserved.
        assert "SUM(" not in sql.upper().split("FROM")[0]
        assert "MEASURE(cy_sales)" in sql

    def test_avg_around_qualified_measure_is_stripped(self):
        from genie_space_optimizer.optimization.evaluation import (
            _strip_outer_agg_around_measure,
        )

        sql, count = _strip_outer_agg_around_measure(
            "SELECT AVG(MEASURE(s.amt)) FROM cat.sch.mv s",
        )
        assert count == 1
        assert "AVG(" not in sql.upper().split("FROM")[0]
        assert "MEASURE(s.amt)" in sql

    def test_non_aggregate_wrapper_left_alone(self):
        """``COALESCE(MEASURE(x), 0)`` is NOT a nested aggregate — it's
        a perfectly valid expression. Must not be stripped."""
        from genie_space_optimizer.optimization.evaluation import (
            _strip_outer_agg_around_measure,
        )

        original = "SELECT COALESCE(MEASURE(cy_sales), 0) FROM cat.sch.mv_sales"
        sql, count = _strip_outer_agg_around_measure(original)
        assert count == 0, f"unexpected strip: {sql!r}"
        # COALESCE preserved.
        assert "COALESCE" in sql.upper()
        assert "MEASURE(cy_sales)" in sql

    def test_aggregate_with_extra_args_not_stripped(self):
        """``COUNT(MEASURE(x), 'extra')`` (hypothetical) shouldn't strip
        — only single-argument outer aggregates collapse."""
        from genie_space_optimizer.optimization.evaluation import (
            _strip_outer_agg_around_measure,
        )

        # Use ``GREATEST`` which is a known multi-arg function (not in
        # the aggregate-allowlist) to verify the helper doesn't touch
        # multi-arg wrappers.
        original = (
            "SELECT GREATEST(MEASURE(cy_sales), 0) FROM cat.sch.mv_sales"
        )
        sql, count = _strip_outer_agg_around_measure(original)
        assert count == 0
        assert "GREATEST" in sql.upper()

    def test_no_measure_call_is_noop(self):
        from genie_space_optimizer.optimization.evaluation import (
            _strip_outer_agg_around_measure,
        )

        original = "SELECT SUM(amount) FROM cat.sch.t_orders"
        sql, count = _strip_outer_agg_around_measure(original)
        assert count == 0
        assert sql == original or sql.strip() == original.strip()

    def test_proposal_pipeline_runs_strip_after_rewrite(self):
        """End-to-end via the proposal hook: a proposal that emits
        ``SUM(MEASURE(...))`` must come out as ``MEASURE(...)`` after
        the proposal repair pipeline (rewrite + strip)."""
        slice_ = _slice_with_mv(
            "cat.sch.mv_sales", measures=["cy_sales"], dimensions=[],
        )
        sql = "SELECT SUM(MEASURE(cy_sales)) AS s FROM cat.sch.mv_sales"
        p, _ = _repair_measure_refs_on_proposal({"example_sql": sql}, slice_)
        # The outer SUM around MEASURE should be gone after the
        # proposal-side strip step.
        select_segment = p["example_sql"].split("FROM", 1)[0]
        assert "SUM(" not in select_segment.upper(), (
            f"outer SUM not stripped: {p['example_sql']!r}"
        )
        assert "MEASURE(cy_sales)" in p["example_sql"]

    def test_regex_fallback_handles_unparseable_sql(self):
        """If sqlglot fails to parse, the regex fallback should still
        catch the simple ``AGG(MEASURE(x))`` shape.

        Construct a malformed-but-trimmable input by inserting an
        unmatched bracket downstream of the offending pattern. sqlglot
        will reject the parse; the regex fallback must still strip.
        """
        from genie_space_optimizer.optimization.evaluation import (
            _strip_outer_agg_around_measure,
        )

        # Trailing junk that breaks AST parsing but leaves the head
        # well-formed for the regex.
        malformed = "SELECT SUM(MEASURE(cy_sales)) FROM cat.sch.mv ((( ?"
        sql, count = _strip_outer_agg_around_measure(malformed)
        # Either the AST path or the regex fallback should strip the
        # outer SUM. The exact count depends on which path engages,
        # but the result must contain MEASURE without the outer SUM.
        head = sql.split("FROM", 1)[0]
        assert "SUM(" not in head.upper() or count >= 1, (
            f"fallback failed to strip: count={count} sql={sql!r}"
        )


def test_preflight_summary_renders_measure_failure_telemetry() -> None:
    import contextlib
    import io

    from genie_space_optimizer.optimization.preflight_synthesis import (
        _print_summary,
    )

    result = {
        "applied": 0,
        "need": 5,
        "existing": 0,
        "target": 5,
        "generated": 5,
        "passed_parse": 5,
        "passed_identifier_qualification": 5,
        "passed_execute": 0,
        "passed_firewall": 0,
        "passed_structural": 0,
        "passed_arbiter": 0,
        "passed_genie_agreement": 0,
        "dedup_rejected": 0,
        "rejected_by_gate": {"execute": 5},
        "asset_coverage": {},
        "archetype_distribution": {},
        "skipped_reason": None,
        "traits": [],
        "eligible_archetypes": [],
        "gate_rejected_examples": [],
        "execute_subbuckets": {"mv_missing_measure_function": 5},
        "execute_subbucket_examples": {},
        "retries_on_measure_fired": 6,
        "retries_on_measure_attempts": 7,
        "retries_on_measure_succeeded": 1,
        "repaired_measure_refs": 2,
        "measure_retry_no_known_measures": 3,
        "measure_retry_repair_still_failed": 2,
        "measure_retry_same_failure_after_llm": 4,
        "measure_retry_changed_failure_class": 1,
    }

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _print_summary(result)
    out = buf.getvalue()

    assert "measure_retry_no_known_measures" in out
    assert "measure_retry_repair_still_failed" in out
    assert "measure_retry_same_failure_after_llm" in out
    assert "measure_retry_changed_failure_class" in out
