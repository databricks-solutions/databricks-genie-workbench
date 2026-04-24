"""Unit tests for F4 — deterministic identifier-qualification repair.

The most common preflight example-SQL qualification failure in prod is
the LLM emitting a stemmed identifier (``FROM dim_date``) when the
allowlist has exactly one matching canonical name
(``cat.sch.mv_esr_dim_date``). ``_repair_stemmed_identifiers`` rewrites
those stems deterministically before the validator runs, saving an LLM
retry round-trip for the common case. Ambiguous stems are left alone.

``_extract_offending_identifiers`` parses Spark EXPLAIN error reasons
so the qualification retry feedback can call out the exact offending
tokens verbatim, instead of just handing the model the allowlist.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
    _build_qualification_feedback,
    _extract_offending_identifiers,
    _repair_stemmed_identifiers,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


def _slice(identifiers: list[str], metric_view: str | None = None) -> AssetSlice:
    """Build a minimal AssetSlice with the given table identifiers.

    The repair helper only looks at each asset's ``identifier`` /
    ``name`` field, so we pass minimal dicts with no column_configs.
    """
    return AssetSlice(
        tables=[
            {"identifier": i, "name": i.split(".")[-1], "column_configs": []}
            for i in identifiers
        ],
        columns=[],
        metric_view=(
            {
                "identifier": metric_view,
                "name": metric_view.split(".")[-1],
                "columns": [],
            }
            if metric_view
            else None
        ),
        join_spec=None,
    )


# ═══════════════════════════════════════════════════════════════════════
# _repair_stemmed_identifiers — positive cases (unique-stem rewrite)
# ═══════════════════════════════════════════════════════════════════════


class TestRepairStemmedIdentifiersPositive:
    def test_mv_esr_prefix_stem_repaired(self):
        """The production scenario: LLM writes `FROM dim_date` when the
        allowlist has exactly ``cat.sch.mv_esr_dim_date``."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM dim_date"},
            _slice(["cat.sch.mv_esr_dim_date"]),
        )
        assert p["example_sql"] == "SELECT * FROM cat.sch.mv_esr_dim_date"
        assert len(subs) == 1
        assert subs[0][1] == "cat.sch.mv_esr_dim_date"

    def test_mv_7now_two_segment_prefix_repaired(self):
        """Two-segment leaf prefix (``mv_7now_``) is stripped to a
        unique soft stem (``fact_sales``) and rewritten."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM fact_sales"},
            _slice(["cat.sch.mv_7now_fact_sales"]),
        )
        assert "cat.sch.mv_7now_fact_sales" in p["example_sql"]
        assert subs

    def test_hard_stem_unqualified_leaf_repaired(self):
        """A bare leaf name (``mv_esr_dim_date`` without the catalog
        prefix) is unique across the allowlist → rewritten to the FQ."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM mv_esr_dim_date"},
            _slice(["cat.sch.mv_esr_dim_date"]),
        )
        assert p["example_sql"] == "SELECT * FROM cat.sch.mv_esr_dim_date"
        assert subs[0] == ("mv_esr_dim_date", "cat.sch.mv_esr_dim_date")

    def test_join_clause_is_repaired(self):
        sql = (
            "SELECT * FROM cat.sch.mv_esr_fact_sales f "
            "JOIN dim_date d ON f.k=d.k"
        )
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": sql},
            _slice([
                "cat.sch.mv_esr_dim_date",
                "cat.sch.mv_esr_fact_sales",
            ]),
        )
        assert "cat.sch.mv_esr_dim_date d" in p["example_sql"]
        # Only the `dim_date` table was stemmed; fact_sales was already
        # fully qualified.
        assert all(orig == "dim_date" for orig, _ in subs)

    def test_case_insensitive_match_canonical_case_preserved(self):
        """LLM may emit ``FROM DIM_DATE`` in upper-case; rewrite matches
        and uses the canonical-case identifier in the replacement."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM DIM_DATE"},
            _slice(["cat.sch.mv_esr_dim_date"]),
        )
        assert p["example_sql"] == "SELECT * FROM cat.sch.mv_esr_dim_date"
        assert subs[0][0] == "DIM_DATE"
        assert subs[0][1] == "cat.sch.mv_esr_dim_date"

    def test_metric_view_identifier_is_part_of_map(self):
        """Metric view assets contribute to the unique-stem map too."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM store_sales"},
            _slice([], metric_view="cat.sch.mv_esr_store_sales"),
        )
        assert "cat.sch.mv_esr_store_sales" in p["example_sql"]
        assert subs

    def test_repair_trace_recorded_on_proposal(self):
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM dim_date"},
            _slice(["cat.sch.mv_esr_dim_date"]),
        )
        assert p.get("_repair_trace") == subs


# ═══════════════════════════════════════════════════════════════════════
# _repair_stemmed_identifiers — negative cases (no rewrite)
# ═══════════════════════════════════════════════════════════════════════


class TestRepairStemmedIdentifiersNegative:
    def test_ambiguous_stem_is_not_rewritten(self):
        """Two tables share the same stemmed leaf (``dim_date``) after
        prefix-stripping — rewriting either one might be wrong, so the
        helper leaves the SQL alone and hands off to the LLM retry."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM dim_date"},
            _slice([
                "cat.a.mv_esr_dim_date",
                "cat.b.other_dim_date",
            ]),
        )
        assert p["example_sql"] == "SELECT * FROM dim_date"
        assert subs == []

    def test_fully_qualified_is_noop(self):
        sql = "SELECT * FROM cat.sch.mv_esr_dim_date"
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": sql}, _slice(["cat.sch.mv_esr_dim_date"]),
        )
        assert p["example_sql"] == sql
        assert subs == []

    def test_column_qualifier_is_not_mis_matched(self):
        """``t.dim_date`` is a column qualifier, not a table reference —
        the token-boundary regex must NOT treat it as a stem."""
        sql = (
            "SELECT t.dim_date FROM cat.sch.mv_esr_fact_sales t "
            "WHERE t.dim_date > 0"
        )
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": sql},
            _slice([
                "cat.sch.mv_esr_dim_date",
                "cat.sch.mv_esr_fact_sales",
            ]),
        )
        assert p["example_sql"] == sql
        assert all(orig != "dim_date" for orig, _ in subs)

    def test_substring_inside_larger_identifier_is_not_matched(self):
        """The regex uses ``(?![\\w.])`` so ``dim_date_extra`` isn't
        matched as ``dim_date``."""
        sql = "SELECT * FROM dim_date_extra"
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": sql}, _slice(["cat.sch.mv_esr_dim_date"]),
        )
        assert p["example_sql"] == sql
        assert subs == []

    def test_empty_sql_is_noop(self):
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": ""}, _slice(["cat.sch.t"]),
        )
        assert subs == []

    def test_empty_slice_is_noop(self):
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM dim_date"}, _slice([]),
        )
        assert subs == []

    def test_no_stems_when_canonical_is_leaf_itself(self):
        """If the canonical identifier IS the stem (no prefix to strip,
        no dotted qualifier), the helper has nothing to rewrite."""
        p, subs = _repair_stemmed_identifiers(
            {"example_sql": "SELECT * FROM dim_date"},
            _slice(["dim_date"]),
        )
        assert p["example_sql"] == "SELECT * FROM dim_date"
        assert subs == []


# ═══════════════════════════════════════════════════════════════════════
# _extract_offending_identifiers
# ═══════════════════════════════════════════════════════════════════════


class TestOffendingIdentifierExtraction:
    def test_unresolved_column_backtick(self):
        reason = (
            "UNRESOLVED_COLUMN: `dim_date` — suggestion: "
            "`dim_date`.`day_of_week`, `dim_date`.`full_date`"
        )
        out = _extract_offending_identifiers(reason)
        assert "dim_date" in out

    def test_unresolved_table_extracted(self):
        reason = "UNRESOLVED_TABLE: `foo_bar` at position 10"
        assert _extract_offending_identifiers(reason) == ["foo_bar"]

    def test_table_or_view_not_found(self):
        reason = "TABLE_OR_VIEW_NOT_FOUND: `missing_table`"
        assert _extract_offending_identifiers(reason) == ["missing_table"]

    def test_multiple_identifiers_order_preserved(self):
        reason = (
            "UNRESOLVED_TABLE: `alpha` ... "
            "UNRESOLVED_COLUMN: `beta` at ..."
        )
        assert _extract_offending_identifiers(reason) == ["alpha", "beta"]

    def test_duplicate_identifiers_deduped(self):
        reason = "UNRESOLVED_COLUMN: `dim_date` and UNRESOLVED_TABLE: `dim_date`"
        assert _extract_offending_identifiers(reason) == ["dim_date"]

    def test_no_match_returns_empty(self):
        assert _extract_offending_identifiers("") == []
        assert _extract_offending_identifiers("something else") == []
        # Not a recognized error code — returns empty.
        assert _extract_offending_identifiers("SYNTAX_ERROR at `foo`") == []


# ═══════════════════════════════════════════════════════════════════════
# _build_qualification_feedback — offender-hint plumbing (F4b)
# ═══════════════════════════════════════════════════════════════════════


class TestQualificationFeedbackWithOffenders:
    def test_feedback_calls_out_offenders_explicitly(self):
        proposal = {"example_sql": "SELECT * FROM dim_date"}
        slice_ = _slice(["cat.sch.mv_esr_dim_date"])
        feedback = _build_qualification_feedback(
            proposal, slice_,
            "UNRESOLVED_TABLE: `dim_date`",
            offending_identifiers=["dim_date"],
        )
        assert "You wrote `dim_date`" in feedback
        assert "NOT in the allowlist" in feedback
        # The allowlist block is still present.
        assert "cat.sch.mv_esr_dim_date" in feedback

    def test_feedback_with_no_offenders_omits_block(self):
        """Calling without the new kwarg preserves the pre-F4b message
        shape — backward compat for any external callers."""
        proposal = {"example_sql": "SELECT * FROM dim_date"}
        slice_ = _slice(["cat.sch.mv_esr_dim_date"])
        feedback = _build_qualification_feedback(
            proposal, slice_, "some reason",
        )
        assert "You wrote" not in feedback
        assert "cat.sch.mv_esr_dim_date" in feedback

    def test_offenders_sorted_and_deduped(self):
        proposal = {"example_sql": "SELECT * FROM x"}
        slice_ = _slice(["cat.sch.t"])
        feedback = _build_qualification_feedback(
            proposal, slice_, "reason",
            offending_identifiers=["b", "a", "b", "a"],
        )
        # Sorted: a comes before b. Dedup: each appears once.
        assert feedback.count("`a`") == 1
        assert feedback.count("`b`") == 1
        idx_a = feedback.index("`a`")
        idx_b = feedback.index("`b`")
        assert idx_a < idx_b
