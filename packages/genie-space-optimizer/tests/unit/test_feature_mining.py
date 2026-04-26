"""Tests for Task 6: reactive SQL feature mining.

Covers two layers:

1. Pure SQL diff (``mine_sql_features`` + ``compute_diff``) over the
   Q011 missing-GROUP BY-YEAR shape and the Q009 measure-swap shape
   from the decoded retail run. These are the failures the loop kept
   missing because the typed AST diff was dead code.

2. The dedup contract (Semantic Consistency Rule 6): mining must not
   duplicate existing space artifacts. A measure already in a metric
   view becomes an enrichment of the metric view's measure
   description, not a new ``add_sql_snippet_expression``. Existing
   snippets, joins, descriptions, and example SQLs cannot be
   re-emitted by mined patches.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.feature_mining import (
    CorpusProfile,
    DiffKind,
    DiffSet,
    SqlDiff,
    aggregate_corpus_profile,
    apply_dedup_contract,
    compute_diff,
    is_eligible_passing_row,
    mine_sql_features,
    reactive_patches_from_diff,
    run_proactive_feature_mining,
    synthesize_proactive_patches,
)


# ── mine_sql_features ─────────────────────────────────────────


def test_extracts_select_columns_and_group_by():
    feat = mine_sql_features(
        "SELECT YEAR(date_key_2) AS yr, MONTH(date_key_2) AS mo, "
        "AVG(exchange_rate) AS avg_x FROM mv_esr_fact_sales "
        "GROUP BY YEAR(date_key_2), MONTH(date_key_2)"
    )

    # Group-by has both YEAR and MONTH applied to date_key_2
    assert any("YEAR" in g.upper() for g in feat.group_by_cols)
    assert any("MONTH" in g.upper() for g in feat.group_by_cols)
    # Aggregation function is AVG
    assert any("AVG" in a.upper() for a in feat.aggregation_funcs)


def test_extracts_measures_from_aggregate_args():
    feat = mine_sql_features(
        "SELECT location_number, MEASURE(total_txn_py_mtdate) FROM "
        "mv_esr_store_sales GROUP BY location_number"
    )

    # ``measures`` carries the column inside the aggregate
    assert any("total_txn_py_mtdate" in m.lower() for m in feat.measures)


def test_returns_empty_features_on_unparseable_sql():
    # Should not raise; pure helpers return empty containers instead.
    feat = mine_sql_features("not valid SQL at all")

    assert feat.group_by_cols == ()
    assert feat.aggregation_funcs == ()


# ── compute_diff: Q011 shape ────────────────────────────────


def test_q011_missing_groupby_year_classifies_correctly():
    """The retail Q011 case: GT groups by YEAR + MONTH; Genie groups by
    MONTH only. The missing column should drive a typed
    MISSING_GROUPBY_COL diff."""
    genie = mine_sql_features(
        "SELECT MONTH(date_key_2) AS mo, AVG(exchange_rate) "
        "FROM mv_esr_fact_sales GROUP BY MONTH(date_key_2)"
    )
    gt = mine_sql_features(
        "SELECT YEAR(date_key_2) AS yr, MONTH(date_key_2) AS mo, "
        "AVG(exchange_rate) FROM mv_esr_fact_sales "
        "GROUP BY YEAR(date_key_2), MONTH(date_key_2)"
    )

    diff = compute_diff(genie=genie, ground_truth=gt)

    assert diff.primary_kind == DiffKind.MISSING_GROUPBY_COL
    # The missing GROUP BY entry mentions YEAR(date_key_2)
    missing = " ".join(diff.missing_in_genie.group_by_cols).upper()
    assert "YEAR" in missing


# ── compute_diff: Q009 measure swap ─────────────────────────


def test_q009_measure_swap_classifies_and_routes_to_lever_1():
    """The retail Q009 case: GT uses total_txn_py_mtdate (count); Genie
    uses apsd_customer_py_mtdate (per-store-day average). Both are
    measures on the same MV; the diff is a swap."""
    genie = mine_sql_features(
        "SELECT MEASURE(apsd_customer_py_mtdate) FROM mv_esr_store_sales "
        "GROUP BY location_number"
    )
    gt = mine_sql_features(
        "SELECT MEASURE(total_txn_py_mtdate) FROM mv_esr_store_sales "
        "GROUP BY location_number"
    )

    diff = compute_diff(genie=genie, ground_truth=gt)

    assert diff.primary_kind == DiffKind.MEASURE_SWAP
    assert any("total_txn_py_mtdate" in m.lower() for m in diff.missing_in_genie.measures)
    assert any("apsd_customer_py_mtdate" in m.lower() for m in diff.extra_in_genie.measures)
    assert diff.candidate_levers == (1,)


# ── compute_diff: speculative-filter (Q019 shape) ────────────


def test_speculative_extra_filter_classifies_as_extra_filter():
    """Q019 in the retail run: Genie adds an IS NOT NULL filter the GT
    didn't have."""
    genie = mine_sql_features(
        "SELECT area_leader_name FROM mv_7now_store_sales "
        "WHERE area_leader_name IS NOT NULL"
    )
    gt = mine_sql_features("SELECT area_leader_name FROM mv_7now_store_sales")

    diff = compute_diff(genie=genie, ground_truth=gt)

    assert diff.primary_kind == DiffKind.EXTRA_FILTER
    assert any("area_leader_name" in f.lower() for f in diff.extra_in_genie.filter_cols)


def test_unknown_diff_when_features_are_identical():
    # Compute diff of a SQL against itself → no differences.
    feat = mine_sql_features("SELECT col FROM tbl GROUP BY col")

    diff = compute_diff(genie=feat, ground_truth=feat)

    assert diff.primary_kind == DiffKind.UNKNOWN


# ── reactive_patches_from_diff ──────────────────────────────


def test_measure_swap_routes_to_column_metadata():
    diff = SqlDiff(
        primary_kind=DiffKind.MEASURE_SWAP,
        missing_in_genie=DiffSet(measures=("total_txn_py_mtdate",)),
        candidate_levers=(1,),
    )

    out = reactive_patches_from_diff(
        diff,
        table_id="mv_esr_store_sales",
        metadata_snapshot={},
        leakage_oracle=None,
    )

    assert len(out) == 1
    assert out[0]["type"] == "update_column_description"
    assert out[0]["column"] == "total_txn_py_mtdate"


def test_missing_groupby_emits_example_and_description_candidates():
    diff = SqlDiff(
        primary_kind=DiffKind.MISSING_GROUPBY_COL,
        missing_in_genie=DiffSet(group_by_cols=("YEAR(date_key_2)",)),
        candidate_levers=(4, 1),
    )

    out = reactive_patches_from_diff(
        diff,
        table_id="mv_esr_fact_sales",
        metadata_snapshot={},
        leakage_oracle=None,
    )

    types = {p["type"] for p in out}
    # Both candidate types are emitted; strategist picks among them.
    assert "add_example_sql" in types
    assert "update_column_description" in types


# ── Dedup contract (Semantic Consistency Rule 6) ────────────


def test_measure_already_in_metric_view_routes_to_enrichment():
    """Rule 6.1: a measure that already lives in a metric view becomes
    a description enrichment, not a free-floating SQL snippet."""
    diff = SqlDiff(
        primary_kind=DiffKind.MEASURE_SWAP,
        missing_in_genie=DiffSet(measures=("total_txn_py_mtdate",)),
        candidate_levers=(1,),
    )
    snap = {
        "metric_views": [{
            "name": "mv_esr_store_sales",
            "measures": [{"name": "total_txn_py_mtdate"}],
        }],
    }

    out = reactive_patches_from_diff(
        diff,
        table_id="mv_esr_store_sales",
        metadata_snapshot=snap,
        leakage_oracle=None,
    )

    assert len(out) == 1
    assert out[0]["type"] == "update_column_description"
    assert out[0].get("dedup_route") == "metric_view_measure_enrich"


def test_existing_snippet_name_collision_drops_candidate():
    candidate = {
        "type": "add_sql_snippet_expression",
        "snippet_name": "month_year",
    }
    snap = {"instructions": {"sql_snippets": [{"name": "month_year"}]}}

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert out == []
    assert candidate.get("dedup_dropped_reason") == "snippet_already_exists"


def test_snippet_for_metric_view_measure_rewrites_to_description_enrichment():
    """Rule 6.1 path through the snippet generator: a snippet whose
    ``metric`` field names an existing metric view measure is rewritten
    rather than dropped."""
    candidate = {
        "type": "add_sql_snippet_expression",
        "snippet_name": "py_mtd_customers",
        "metric": "total_txn_py_mtdate",
    }
    snap = {
        "metric_views": [{
            "name": "mv_esr_store_sales",
            "measures": [{"name": "total_txn_py_mtdate"}],
        }],
    }

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert len(out) == 1
    assert out[0]["type"] == "update_column_description"
    assert out[0]["column"] == "total_txn_py_mtdate"
    assert out[0]["dedup_route"] == "metric_view_measure_enrich"


def test_existing_join_spec_drops_candidate():
    candidate = {
        "type": "add_join_spec",
        "left_table": "mv_esr_store_sales",
        "right_table": "mv_esr_dim_location",
        "on_columns": ["location_number"],
    }
    snap = {"instructions": {"join_specs": [{
        "left": {"identifier": "mv_esr_store_sales"},
        "right": {"identifier": "mv_esr_dim_location"},
        "on": ["location_number"],
    }]}}

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert out == []
    assert candidate.get("dedup_dropped_reason") == "join_spec_already_exists"


def test_example_sql_collision_drops_via_leakage_oracle():
    class _FakeOracle:
        def contains_sql(self, sql, *, w=None):
            return "month_year" in sql

        def contains_question(self, q, threshold=0.85, *, w=None):
            return False

    candidate = {
        "type": "add_example_sql",
        "sql": "SELECT month_year FROM t",
        "question": "Show monthly sales",
    }

    out = apply_dedup_contract([candidate], {}, leakage_oracle=_FakeOracle())

    assert out == []
    assert candidate["dedup_dropped_reason"] == "example_sql_already_exists"


def test_example_question_phrasing_collision_drops_via_oracle():
    class _FakeOracle:
        def contains_sql(self, sql, *, w=None):
            return False

        def contains_question(self, q, threshold=0.85, *, w=None):
            return "monthly" in q.lower()

    candidate = {
        "type": "add_example_sql",
        "sql": "SELECT 1",
        "question": "Show me the monthly trend",
    }

    out = apply_dedup_contract([candidate], {}, leakage_oracle=_FakeOracle())

    assert out == []
    assert candidate["dedup_dropped_reason"] == "example_question_already_exists"


def test_description_already_present_drops_candidate():
    candidate = {
        "type": "update_column_description",
        "column": "date_key_2",
        "proposed_description": "Use YEAR + MONTH grouping for multi-year ranges.",
    }
    snap = {"tables": [{
        "name": "mv_esr_fact_sales",
        "columns": [{
            "name": "date_key_2",
            "description": "Date key. Use YEAR + MONTH grouping for multi-year ranges.",
        }],
    }]}

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert out == []
    assert candidate["dedup_dropped_reason"] == "description_already_present"


def test_description_with_no_overlap_passes_through():
    candidate = {
        "type": "update_column_description",
        "column": "date_key_2",
        "proposed_description": "Always include YEAR alongside MONTH on multi-year date ranges.",
    }
    snap = {"tables": [{
        "name": "mv_esr_fact_sales",
        "columns": [{
            "name": "date_key_2",
            "description": "Calendar surrogate key.",  # short, unrelated
        }],
    }]}

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert out == [candidate]
    assert "dedup_dropped_reason" not in candidate


def test_apply_dedup_contract_handles_empty_input():
    assert apply_dedup_contract([], {}, leakage_oracle=None) == []


# ── Task 9: proactive mining ────────────────────────────────────


def _passing_row(qid: str, sql: str, *, arbiter: str = "both_correct") -> dict:
    return {
        "inputs.question_id": qid,
        "inputs.expected_sql": sql,
        "outputs.predictions.sql": sql,
        "feedback/result_correctness/value": "yes",
        "feedback/arbiter/value": arbiter,
    }


def _failing_row(qid: str, sql: str) -> dict:
    return {
        "inputs.question_id": qid,
        "inputs.expected_sql": sql,
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "ground_truth_correct",
    }


# ── is_eligible_passing_row ────────────────────────────────


def test_eligibility_accepts_rc_yes():
    assert is_eligible_passing_row(_passing_row("q1", "SELECT 1")) is True


def test_eligibility_accepts_arbiter_both_correct():
    row = _passing_row("q1", "SELECT 1", arbiter="both_correct")
    row["feedback/result_correctness/value"] = "no"

    assert is_eligible_passing_row(row) is True


def test_eligibility_rejects_genuine_failure():
    assert is_eligible_passing_row(_failing_row("q1", "SELECT 1")) is False


def test_eligibility_accepts_genie_correct_only_after_corpus_fix():
    row = {
        "inputs.question_id": "q1",
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "genie_correct",
    }

    # Pending review → not eligible.
    assert is_eligible_passing_row(row, gt_corrections={"q1": "pending_review"}) is False
    # Reviewer marked the corpus fix accepted → eligible.
    assert is_eligible_passing_row(row, gt_corrections={"q1": "accepted_corpus_fix"}) is True


# ── aggregate_corpus_profile ────────────────────────────────


def test_corpus_profile_aggregates_measure_dim_pairs_from_passing_rows():
    rows = [
        _passing_row(
            "q1",
            "SELECT MEASURE(total_txn) FROM t GROUP BY region",
        ),
        _passing_row(
            "q2",
            "SELECT MEASURE(total_txn) FROM t GROUP BY region",
        ),
    ]

    profile = aggregate_corpus_profile(rows)

    # The pair appears in both rows → count = 2 (the exact key
    # depends on how sqlglot stringifies the GROUP BY expression; we
    # just assert the total count matches).
    assert sum(profile.measure_dim_pairs.values()) >= 2


def test_corpus_profile_skips_failing_rows():
    rows = [
        _passing_row("q1", "SELECT MEASURE(total_txn) FROM t GROUP BY region"),
        _failing_row("q2", "SELECT MEASURE(total_txn) FROM t GROUP BY region"),
    ]

    profile = aggregate_corpus_profile(rows)

    # Only the passing row contributes.
    assert sum(profile.measure_dim_pairs.values()) >= 1
    # And the failing row's contribution is excluded — the count is
    # exactly what the single passing row produced.
    profile_passing_only = aggregate_corpus_profile([rows[0]])
    assert profile.measure_dim_pairs == profile_passing_only.measure_dim_pairs


def test_corpus_profile_handles_empty_input():
    profile = aggregate_corpus_profile([])

    assert isinstance(profile, CorpusProfile)
    assert profile.measure_dim_pairs == {}


# ── synthesize_proactive_patches: dedup contract integration ─


def test_proactive_measure_in_metric_view_routes_to_metric_view_table():
    """Plan dedup test #1: a measure that already lives in a metric
    view becomes a description on the metric view's measure column,
    not a column-description on the base table."""
    profile = CorpusProfile(
        measure_dim_pairs={("total_txn_py_mtdate", "location_number"): 5},
    )
    snap = {
        "metric_views": [{
            "name": "mv_esr_store_sales",
            "measures": [{"name": "total_txn_py_mtdate"}],
        }],
    }

    patches = synthesize_proactive_patches(
        profile,
        table_id="mv_esr_fact_sales",
        metadata_snapshot=snap,
    )

    assert len(patches) == 1
    assert patches[0]["type"] == "update_column_description"
    assert patches[0]["column"] == "total_txn_py_mtdate"
    assert patches[0]["table_id"] == "mv_esr_store_sales"  # routed to MV


def test_proactive_tvf_wrapping_metric_view_measure_routes_to_enrichment():
    """Plan dedup test #2: a TVF whose definition references a
    metric-view measure becomes a description enrichment on that
    measure, NOT a duplicate sql_snippet."""
    profile = CorpusProfile(tvf_calls={"fn_mtd_or_mtday": 7})
    snap = {
        "metric_views": [{
            "name": "mv_esr_store_sales",
            "measures": [{
                "name": "py_mtd_customers",
                "expression": (
                    "fn_mtd_or_mtday(MEASURE(total_txn_py_mtdate), "
                    "MEASURE(total_txn_py_mtday))"
                ),
            }],
        }],
    }

    patches = synthesize_proactive_patches(
        profile,
        table_id="mv_esr_store_sales",
        metadata_snapshot=snap,
    )

    enrichments = [
        p for p in patches if p.get("dedup_route") == "metric_view_measure_enrich"
    ]
    snippets = [p for p in patches if p["type"] == "add_sql_snippet_expression"]
    assert len(enrichments) == 1
    assert snippets == []
    # Enrichment targets the measure that wraps the TVF.
    assert enrichments[0]["column"] == "py_mtd_customers"


def test_proactive_existing_join_spec_dropped():
    """Plan dedup test #3: when the snapshot already declares an
    equivalent join_spec, the proactive miner does not re-emit it."""
    profile = CorpusProfile(
        join_clauses={
            ("mv_esr_store_sales", "INNER", "mv_esr_dim_location"): 8,
        },
    )
    snap = {"instructions": {"join_specs": [{
        "left": {"identifier": "mv_esr_store_sales"},
        "right": {"identifier": "mv_esr_dim_location"},
    }]}}

    patches = synthesize_proactive_patches(
        profile,
        table_id="mv_esr_store_sales",
        metadata_snapshot=snap,
    )

    assert all(p["type"] != "add_join_spec" for p in patches)


def test_proactive_unrelated_tvf_emits_sql_snippet():
    """When the TVF does NOT wrap a metric-view measure, the
    proactive miner emits a normal sql_snippet candidate."""
    profile = CorpusProfile(tvf_calls={"fn_quarter_bucket": 4})
    snap = {"metric_views": []}

    patches = synthesize_proactive_patches(
        profile,
        table_id="mv_esr_fact_sales",
        metadata_snapshot=snap,
    )

    snippets = [p for p in patches if p["type"] == "add_sql_snippet_expression"]
    assert len(snippets) == 1
    assert snippets[0]["snippet_name"] == "fn_quarter_bucket"


def test_proactive_respects_per_class_budget():
    profile = CorpusProfile(
        measure_dim_pairs={
            ("m{}".format(i), "d"): 10 for i in range(20)
        },
    )

    patches = synthesize_proactive_patches(
        profile, table_id="t", metadata_snapshot={}, budget={"column_description": 2},
    )

    # 2 column_description patches, no joins / snippets without input.
    descs = [p for p in patches if p["type"] == "update_column_description"]
    assert len(descs) == 2


# ── run_proactive_feature_mining (entry point) ──────────────


def test_run_proactive_returns_profile_blob_and_patches():
    rows = [
        _passing_row(
            "q1", "SELECT MEASURE(total_txn) FROM mv GROUP BY region",
        ),
        _passing_row(
            "q2", "SELECT MEASURE(total_txn) FROM mv GROUP BY region",
        ),
    ]

    out = run_proactive_feature_mining(
        eval_rows=rows,
        metadata_snapshot={},
        table_ids=["mv"],
    )

    assert "profile" in out
    assert "eligible_row_count" in out
    assert "patches" in out
    assert out["eligible_row_count"] == 2
    assert all(p.get("phase") == "proactive" for p in out["patches"])


def test_run_proactive_handles_empty_input():
    out = run_proactive_feature_mining(eval_rows=[], metadata_snapshot={})

    assert out["eligible_row_count"] == 0
    assert out["patches"] == []
    # Profile blob is present but empty.
    assert out["profile"]["measure_dim_pairs"] == []


def test_run_proactive_skips_genie_correct_without_accepted_fix():
    """A row with arbiter=genie_correct is excluded from mining
    until the GT-correction queue marks it accepted."""
    rows = [
        {
            "inputs.question_id": "q1",
            "inputs.expected_sql": "SELECT MEASURE(m) FROM t GROUP BY d",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "genie_correct",
        },
    ]

    out = run_proactive_feature_mining(
        eval_rows=rows,
        metadata_snapshot={},
        gt_corrections={"q1": "pending_review"},  # not accepted yet
    )

    assert out["eligible_row_count"] == 0


def test_proactive_default_config_flag_is_off():
    """Pin the rollout default — proactive mining ships behind its
    own release flag per plan §Rollout step 7."""
    from genie_space_optimizer.common.config import (
        ENABLE_PROACTIVE_FEATURE_MINING,
    )

    assert ENABLE_PROACTIVE_FEATURE_MINING is False
