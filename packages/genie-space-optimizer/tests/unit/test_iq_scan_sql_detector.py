"""Tests for the scanner v2 SQL-in-prose detector.

The new detector (``looks_like_sql_in_prose`` + ``sql_in_text_findings``)
replaces the naïve ``_SQL_IN_TEXT_RE`` keyword regex in scanner check #4
and GSO's strict prose validator. Goal: eliminate false positives on
natural-language prose like "Do not join X to Y" while keeping real SQL
fragments flagged.

Coverage matrix:

- Anchor patterns (Tier 1): each clause shape flagged.
- Prose imperatives (false-positive regressions): natural English passes.
- Density fallback (Tier 2): 2+ keywords + structural signal flagged;
  single-keyword English passes.
- Multi-line spans: ``sql_in_text_findings`` catches SQL on ANY line.
- Edge cases: empty, whitespace, unicode.
- Scanner integration: check #4 reports the offending line in its detail.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.iq_scan.scoring import (
    _SQL_IN_TEXT_RE,
    calculate_score,
    looks_like_sql_in_prose,
    sql_in_text_findings,
)


# ── Tier 1: anchor patterns ─────────────────────────────────────────


class TestAnchorPatterns:
    """Anchor = keyword + structural neighbour. Any one match flags the line."""

    @pytest.mark.parametrize("line", [
        "SELECT SUM(amount) FROM orders",
        "select col from t",  # lowercase
        "Here's the query: SELECT * FROM orders WHERE x=1",
        "- Use this: SELECT sales_amount FROM esr_fact_sales",
    ])
    def test_select_from_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "FROM orders WHERE status = 'active'",
        "FROM fact_sales JOIN dim_date ON ...",
        "FROM orders AS o WHERE ...",
        "FROM orders GROUP BY region",
        "FROM orders ORDER BY date",
        "FROM t LIMIT 10",
    ])
    def test_from_plus_clause_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "JOIN dim_date ON fact.dk = dim.dk",
        "join t ON a.k = b.k",
        "- Always JOIN orders ON orders.customer_id = customers.id",
    ])
    def test_join_on_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "WHERE status = 'active'",
        "WHERE amount > 1000",
        "WHERE region IN ('NA', 'EU')",
        "where col LIKE '%foo%'",
        "- Filter with WHERE date >= '2024-01-01'",
        "WHERE col != 0",
        "WHERE col <> 0",
    ])
    def test_where_comparator_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "GROUP BY region HAVING COUNT(*) > 10",
        "GROUP BY a, b, c ORDER BY a",
        "GROUP BY region LIMIT 10",
    ])
    def test_group_by_plus_clause_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "ORDER BY total DESC",
        "ORDER BY created ASC LIMIT 10",
        "ORDER BY a, b DESC",
        "order by rank DESC LIMIT 5",
    ])
    def test_order_by_direction_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "HAVING COUNT(*) > 5",
        "HAVING SUM(amount) > 1000",
        "having avg(revenue) >= 100",
    ])
    def test_having_aggregate_anchor_flags(self, line: str) -> None:
        assert looks_like_sql_in_prose(line)


# ── False-positive regressions (the bugs scanner v2 fixes) ──────────


class TestProseImperativesPassClean:
    """Natural-language prose that scanner v1 false-flagged must pass now."""

    @pytest.mark.parametrize("line", [
        "- Do not join `mv_esr_fact_sales` directly to `mv_7now_fact_sales`",
        "- do not join fact_a to fact_b — they represent disjoint channels",
        "Never join two MVs directly; use a CTE first",
        "Don't join tables with different grains",
        "- Always join orders through the customer dimension",
    ])
    def test_do_not_join_prose_passes(self, line: str) -> None:
        """The bug from the failing run — natural English imperative using 'join'."""
        assert not looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "- Where applicable, use the default region",
        "Where necessary, include the join spec",
        "where possible, prefer MV over raw table",
    ])
    def test_where_applicable_prose_passes(self, line: str) -> None:
        assert not looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "Having determined the schema, proceed",
        "- Having reviewed the data, note the caveats",
    ])
    def test_having_participle_prose_passes(self, line: str) -> None:
        assert not looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "- Order by priority, not by date",
        "order by urgency, not chronologically",
    ])
    def test_order_by_without_direction_passes(self, line: str) -> None:
        assert not looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "- Combine tables with care",
        "- Link the dim tables through their surrogate keys",
        "- Pair fact with dimension for full context",
        "- Associate the customer with the account",
        "- Avoid cross-joining large fact tables",
        "- Prefer MEASURE() for aggregations",
        "- Consider using a CTE for readability",
        "- Note: data is refreshed nightly at 2am UTC",
    ])
    def test_other_prose_imperatives_pass(self, line: str) -> None:
        assert not looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "Use `orders.amount` for totals — prefer aggregating via MEASURE()",
        "The `status` column has mixed casing; normalize before filtering",
    ])
    def test_backticked_identifiers_in_prose_pass(self, line: str) -> None:
        """Backticks appear in both SQL and prose — they alone don't indicate SQL."""
        assert not looks_like_sql_in_prose(line)

    @pytest.mark.parametrize("line", [
        "For joining two fact tables, use a CTE first",
        "joining is tricky with metric views",
        "The ordering of columns in the result matters",
    ])
    def test_english_gerunds_pass(self, line: str) -> None:
        """'joining', 'ordering' are not keyword matches (\\b requires exact word)."""
        assert not looks_like_sql_in_prose(line)


# ── Tier 2: density fallback ────────────────────────────────────────


class TestDensityFallback:
    """2+ distinct SQL keywords AND 1+ structural signal, not prose-imperative."""

    def test_two_keywords_plus_signal_flags(self) -> None:
        # SELECT + FROM is already an anchor hit — but also fires density.
        line = "Query: SELECT col FROM t WHERE x = y"
        assert looks_like_sql_in_prose(line)

    def test_one_keyword_no_signal_passes(self) -> None:
        line = "- Where the data is ambiguous, prefer the explicit filter"
        assert not looks_like_sql_in_prose(line)

    def test_two_keywords_no_signal_passes(self) -> None:
        """2 keywords but no structural signal → density doesn't fire."""
        line = "the WHERE and GROUP BY considerations are separate concerns"
        # "WHERE" + "GROUP BY" are 2 distinct keywords; line has no dotted
        # ident, no comparator, no aggregate call → density doesn't trigger.
        assert not looks_like_sql_in_prose(line)

    def test_keywords_with_signal_but_prose_imperative_passes(self) -> None:
        line = "Do not JOIN on `a.k = b.k` patterns that cross grains"
        # Line starts with "Do not" → imperative → density skipped.
        # "JOIN on" without the `ON ident.col = ident.col` shape doesn't
        # match the anchor either (anchor requires JOIN ident ON).
        assert not looks_like_sql_in_prose(line)


# ── Multi-line spans ────────────────────────────────────────────────


class TestSqlInTextFindings:
    """The multi-line wrapper used by the miner for source_span validation."""

    def test_empty_returns_empty(self) -> None:
        assert sql_in_text_findings("") == []
        assert sql_in_text_findings("   ") == []
        assert sql_in_text_findings("\n\n\n") == []

    def test_prose_only_returns_empty(self) -> None:
        text = (
            "- Do not join A to B\n"
            "- Where applicable, use default\n"
            "- Having considered the data, proceed"
        )
        assert sql_in_text_findings(text) == []

    def test_sql_on_any_line_is_flagged(self) -> None:
        text = (
            "- This is a prose bullet\n"
            "- Another prose bullet\n"
            "- SELECT SUM(amount) FROM orders WHERE status = 'x'"
        )
        findings = sql_in_text_findings(text)
        assert len(findings) == 1
        assert "SELECT" in findings[0]

    def test_multiple_sql_lines_all_returned(self) -> None:
        text = (
            "SELECT * FROM t1\n"
            "- prose\n"
            "WHERE col = 1\n"
        )
        findings = sql_in_text_findings(text)
        assert len(findings) == 2


# ── Edge cases ──────────────────────────────────────────────────────


class TestEdgeCases:
    @pytest.mark.parametrize("line", ["", "   ", "\t", "\n"])
    def test_empty_and_whitespace_pass(self, line: str) -> None:
        assert not looks_like_sql_in_prose(line)

    def test_unicode_prose_passes(self) -> None:
        line = "• Ne pas joindre A à B — différentes sources"
        assert not looks_like_sql_in_prose(line)

    def test_unicode_bullet_with_do_not_passes(self) -> None:
        line = "\u2022 Do not join fact tables from separate channels"
        assert not looks_like_sql_in_prose(line)


# ── Parity with scanner v1 on true positives ────────────────────────


class TestParity:
    """v2 must flag everything v1 flagged on *real* SQL fragments.

    v2 is allowed to pass things v1 flagged (that's the whole point —
    reducing false positives). This test just guards against v2 missing
    obviously-SQL fragments that v1 correctly caught.
    """

    @pytest.mark.parametrize("line", [
        "SELECT * FROM t WHERE x = 1",
        "JOIN t2 ON a.k = b.k",
        "WHERE status = 'active'",
        "GROUP BY region HAVING COUNT(*) > 10",
        "ORDER BY date DESC LIMIT 100",
    ])
    def test_v1_hits_also_hit_in_v2(self, line: str) -> None:
        assert _SQL_IN_TEXT_RE.search(line)  # baseline: v1 catches this
        assert looks_like_sql_in_prose(line)  # v2 must also catch


# ── Scanner integration: check #4 uses sql_in_text_findings ─────────


class TestScannerCheck4Integration:
    """End-to-end: calculate_score wires scanner v2 into check #4."""

    def _base_space(self, text_instruction: str) -> dict:
        # Minimal space_data shape that gets past the earlier checks.
        return {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.t",
                    "description": ["test"],
                    "column_configs": [{"column_name": "id", "description": ["pk"]}],
                }],
                "metric_views": [],
            },
            "instructions": {
                "text_instructions": [
                    {"id": "i1", "content": [text_instruction]}
                ],
                "join_specs": [],
                "sql_snippets": {},
                "example_question_sqls": [],
            },
        }

    def test_check4_no_warning_on_prose_imperative(self) -> None:
        """'Do not join' no longer triggers the SQL-in-text warning."""
        prose = "- Do not join fact_a to fact_b — they represent disjoint channels. " * 3
        result = calculate_score(self._base_space(prose))
        ti_check = next(c for c in result["checks"] if "Text instructions" in c["label"])
        # May still warn on other things (length), but not SQL-in-text.
        assert "SQL patterns" not in (ti_check.get("detail") or "")
        # More importantly: no SQL-in-text warning string anywhere.
        for w in result.get("warnings", []):
            assert "SQL patterns" not in w

    def test_check4_warning_surfaces_offending_line(self) -> None:
        prose = (
            "- A reasonable prose bullet that explains something.\n"
            "- Another reasonable bullet.\n"
            "- SELECT SUM(amount) FROM orders WHERE status = 'x' -- oops"
        ) + (" padding " * 20)  # pad to pass the 50-char check
        result = calculate_score(self._base_space(prose))
        sql_warnings = [w for w in result.get("warnings", []) if "SQL patterns" in w]
        assert len(sql_warnings) == 1
        # Warning should include the offender sample for UX.
        assert "SELECT" in sql_warnings[0]
