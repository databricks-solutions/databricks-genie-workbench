"""Tests for Task 5: proposal grounding + small-AG selection.

The retail run shipped 8-patch bundles whose targets did not always
appear in the failing question's SQL or NL. AG2 in particular pushed
``update_column_description`` patches on ``zone_combination`` /
``market_combination`` even though the actual failures (Q011 missing
GROUP BY YEAR, Q009 wrong customer-count measure) had nothing to do
with those columns. These tests pin the contract that:

* A patch whose targets do not appear anywhere in the failing
  question's surface scores 0.0 and gets dropped.
* The bundle selector keeps at most ``MAX_AG_PATCHES`` and orders by
  relevance score after risk_level (the existing diversity sort still
  drives intra-relevance ordering elsewhere; here we just enforce the
  upper bound).
* When a proposal already carries a ``relevance_score`` field, the
  selector trusts it (avoids re-parsing).
* The grounding check is purely local — it never sends benchmark text
  to an LLM (the plan's Bug #4 contract). All comparisons run on
  schema identifiers + tokenized NL.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.proposal_grounding import (
    explain_relevance,
    extract_failure_surface,
    extract_patch_targets,
    relevance_score,
    select_patch_bundle,
)


# ── extract_patch_targets ─────────────────────────────────────────


def test_extracts_column_and_target_keys():
    patch = {"type": "update_column_description", "column": "zone_combination"}

    targets = extract_patch_targets(patch)

    assert "zone_combination" in targets


def test_normalizes_to_lowercase():
    patch = {"type": "add_instruction", "target": "TIME GROUPING"}

    targets = extract_patch_targets(patch)

    # Comparison is case-insensitive — surface normalizes too.
    assert "time grouping" in targets


def test_collects_multiple_target_fields():
    patch = {
        "type": "add_sql_snippet_expression",
        "target": "month_year",
        "metric": "AVG_EXCHANGE_RATE",
        "instruction_section": "TIME_GROUPING",
    }

    targets = extract_patch_targets(patch)

    assert {"month_year", "avg_exchange_rate", "time_grouping"}.issubset(targets)


def test_collects_rca_target_fields_and_dotted_identifier_parts():
    patch = {
        "type": "update_measure_description",
        "target_object": "sales_mv.avg_txn_day",
        "target_table": "sales_mv",
    }

    targets = extract_patch_targets(patch)

    assert "sales_mv.avg_txn_day" in targets
    assert "sales_mv" in targets
    assert "avg_txn_day" in targets


def test_returns_empty_set_when_no_target_keys():
    patch = {"type": "rewrite_instruction"}

    assert extract_patch_targets(patch) == set()


# ── extract_failure_surface ────────────────────────────────────


def test_extracts_columns_and_functions_from_sql():
    row = {
        "generated_sql": "SELECT MONTH(date_key_2), AVG(exchange_rate) FROM mv_esr_fact_sales GROUP BY MONTH(date_key_2)",
        "expected_sql": "SELECT YEAR(date_key_2), MONTH(date_key_2), AVG(exchange_rate) FROM mv_esr_fact_sales GROUP BY YEAR(date_key_2), MONTH(date_key_2)",
        "nl_response": "Average exchange rate by month.",
    }

    surface = extract_failure_surface(row)

    # Columns
    assert "date_key_2" in surface
    assert "exchange_rate" in surface
    # Function tokens (regex fallback picks them up regardless of sqlglot).
    assert "month" in surface
    assert "year" in surface
    assert "avg" in surface
    # Table identifier
    assert "mv_esr_fact_sales" in surface


def test_extracts_nl_tokens_too():
    row = {"nl_response": "Show me the average exchange rate per month."}

    surface = extract_failure_surface(row)

    assert "exchange" in surface
    assert "rate" in surface
    assert "month" in surface


def test_handles_missing_sql_keys():
    surface = extract_failure_surface({"nl_response": "anything"})

    assert "anything" in surface


def test_handles_unparseable_sql_via_regex_fallback():
    """Even if sqlglot can't parse, identifiers should still be picked
    up by the regex fallback."""
    row = {"generated_sql": "this is not valid SQL but has identifier zone_combination"}

    surface = extract_failure_surface(row)

    assert "zone_combination" in surface


# ── relevance_score ────────────────────────────────────────────


def test_drops_patch_with_no_overlap_with_failed_question_surface():
    """The exact AG2 failure mode from the retail run: patch on
    zone_combination, but failing question (Q011) is about exchange
    rate by month — no overlap."""
    patch = {"type": "update_column_description", "column": "zone_combination"}
    failing = {
        "generated_sql": "SELECT MONTH(date_key_2), AVG(exchange_rate) FROM mv_esr_fact_sales GROUP BY MONTH(date_key_2)",
        "expected_sql": "SELECT YEAR(date_key_2), MONTH(date_key_2), AVG(exchange_rate) FROM mv_esr_fact_sales GROUP BY YEAR(date_key_2), MONTH(date_key_2)",
        "nl_response": "Average exchange rate by month.",
    }

    assert relevance_score(patch, [failing]) == 0.0


def test_full_overlap_scores_one():
    patch = {"type": "update_column_description", "column": "date_key_2"}
    failing = {
        "generated_sql": "SELECT MONTH(date_key_2) FROM t",
        "expected_sql": "SELECT YEAR(date_key_2) FROM t",
        "nl_response": "monthly trend",
    }

    assert relevance_score(patch, [failing]) == 1.0


def test_partial_overlap_scores_fraction():
    patch = {
        "type": "add_instruction",
        "target": "month_year",        # not in surface
        "instruction_section": "date_key_2",  # in surface
    }
    failing = {
        "generated_sql": "SELECT MONTH(date_key_2) FROM t",
        "expected_sql": "SELECT YEAR(date_key_2) FROM t",
    }

    score = relevance_score(patch, [failing])

    # 1 of 2 targets overlaps → 0.5
    assert score == 0.5


def test_zero_when_no_targets():
    patch = {"type": "rewrite_instruction"}

    assert relevance_score(patch, [{"generated_sql": "SELECT 1"}]) == 0.0


def test_zero_when_no_failing_rows():
    patch = {"type": "update_column_description", "column": "anything"}

    assert relevance_score(patch, []) == 0.0


def test_relevance_score_with_mlflow_flattened_keys():
    """Persisted MLflow eval rows in ``iterations.rows_json`` use dotted
    keys (``outputs.predictions.sql`` / ``inputs.expected_sql`` /
    ``outputs.predictions.response_text``). Without dotted-key support,
    every patch in the AG1 retail run scored 0.0 because the surface
    set was empty for every row.

    This regression test pins the dotted-key contract using a row shape
    that contains *only* dotted keys — no flat fallbacks — to ensure
    the MLflow path is exercised independently of the legacy flat
    fixtures elsewhere in this file.
    """
    row = {
        "outputs.predictions.sql":
            "SELECT region_name, SUM(sales) FROM mv_sales GROUP BY region_name",
        "inputs.expected_sql":
            "SELECT region_combination, SUM(sales) FROM mv_sales "
            "GROUP BY region_combination",
        "outputs.predictions.response_text": "Sales by region.",
    }
    patch = {
        "type": "update_column_description",
        "column": "region_combination",
    }

    assert relevance_score(patch, [row]) == 1.0


def test_extract_failure_surface_with_mlflow_flattened_keys():
    """Companion lower-level test: the surface extractor must resolve
    SQL identifiers and NL tokens from the dotted-key shape directly.
    """
    row = {
        "outputs.predictions.sql": "SELECT MONTH(date_key_2) FROM mv_t",
        "inputs.expected_sql": "SELECT YEAR(date_key_2) FROM mv_t",
        "outputs.predictions.response_text": "yearly trend",
    }

    surface = extract_failure_surface(row)

    assert "date_key_2" in surface
    assert "year" in surface
    assert "month" in surface
    assert "mv_t" in surface
    assert "yearly" in surface


def test_metric_view_measure_expression_keeps_measure_name_grounded():
    row = {
        "inputs.expected_sql": (
            "SELECT MEASURE(avg_txn_day) FROM sales_mv GROUP BY region"
        ),
    }
    patch = {
        "type": "update_measure_description",
        "target_object": "sales_mv.avg_txn_day",
        "target_table": "sales_mv",
    }

    surface = extract_failure_surface(row)

    assert "avg_txn_day" in surface
    assert relevance_score(patch, [row]) > 0.0


def test_explain_relevance_reports_overlap_and_missing_targets():
    row = {"expected_sql": "SELECT MEASURE(avg_txn_day) FROM sales_mv"}
    patch = {
        "type": "update_measure_description",
        "target_object": "sales_mv.avg_txn_day",
        "target_table": "sales_mv",
        "column": "unrelated_column",
    }

    explanation = explain_relevance(patch, [row])

    assert explanation["score"] > 0.0
    assert "avg_txn_day" in explanation["overlap"]
    assert "unrelated_column" in explanation["missing_targets"]


# ── select_patch_bundle ────────────────────────────────────────


def test_drops_below_min_relevance():
    proposals = [
        {"id": "p_grounded", "type": "update_column_description", "column": "date_key_2", "relevance_score": 1.0, "risk_level": "low"},
        {"id": "p_unrelated", "type": "update_column_description", "column": "zone_combination", "relevance_score": 0.0, "risk_level": "low"},
    ]

    selected = select_patch_bundle(proposals, max_patches=8, min_relevance=0.1)

    ids = [p["id"] for p in selected]
    assert "p_grounded" in ids
    assert "p_unrelated" not in ids


def test_caps_to_max_patches_after_grounding():
    proposals = [
        {"id": f"p{i}", "type": "update_column_description", "column": "date_key_2", "relevance_score": 1.0, "risk_level": "low"}
        for i in range(10)
    ]

    selected = select_patch_bundle(proposals, max_patches=3)

    assert len(selected) == 3


def test_preserves_input_order_within_relevance_ties():
    """Stable sort: equal-relevance proposals keep their incoming
    order. Diversity selection happens upstream of grounding in the
    harness."""
    proposals = [
        {"id": "p1", "type": "x", "column": "date_key_2", "relevance_score": 1.0, "risk_level": "low"},
        {"id": "p2", "type": "x", "column": "date_key_2", "relevance_score": 1.0, "risk_level": "high"},
        {"id": "p3", "type": "x", "column": "date_key_2", "relevance_score": 1.0, "risk_level": "low"},
    ]

    selected = select_patch_bundle(proposals, max_patches=3)

    assert [p["id"] for p in selected] == ["p1", "p2", "p3"]


def test_higher_relevance_beats_lower():
    proposals = [
        {"id": "low_rel", "type": "x", "column": "zone_combination", "relevance_score": 0.2, "risk_level": "low"},
        {"id": "high_rel", "type": "x", "column": "date_key_2", "relevance_score": 1.0, "risk_level": "low"},
    ]

    selected = select_patch_bundle(proposals, max_patches=1)

    assert [p["id"] for p in selected] == ["high_rel"]


def test_computes_relevance_when_field_missing(monkeypatch=None):
    """If a proposal does not carry a precomputed relevance_score,
    the selector computes one from failing_rows_by_proposal."""
    proposals = [
        {"id": "p_grounded", "type": "x", "column": "date_key_2", "risk_level": "low"},
        {"id": "p_unrelated", "type": "x", "column": "zone_combination", "risk_level": "low"},
    ]
    failing_rows_by_proposal = {
        "p_grounded": [
            {"generated_sql": "SELECT MONTH(date_key_2) FROM t",
             "expected_sql": "SELECT YEAR(date_key_2), MONTH(date_key_2) FROM t"},
        ],
        "p_unrelated": [
            {"generated_sql": "SELECT MONTH(date_key_2) FROM t",
             "expected_sql": "SELECT YEAR(date_key_2), MONTH(date_key_2) FROM t"},
        ],
    }

    selected = select_patch_bundle(
        proposals,
        max_patches=8,
        min_relevance=0.1,
        failing_rows_by_proposal=failing_rows_by_proposal,
    )

    ids = [p["id"] for p in selected]
    assert "p_grounded" in ids
    assert "p_unrelated" not in ids


def test_empty_input_returns_empty():
    assert select_patch_bundle([], max_patches=3) == []


def test_extract_failure_surface_reads_nested_inputs_outputs() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        extract_failure_surface,
    )

    row = {
        "inputs": {
            "expected_sql": (
                "SELECT region_combination FROM mv_esr_store_sales "
                "GROUP BY region_combination"
            ),
            "question_id": "q_region",
        },
        "outputs": {
            "predictions": {
                "sql": (
                    "SELECT region_name FROM mv_esr_store_sales "
                    "GROUP BY region_name"
                ),
                "response_text": "Sales by region",
            }
        },
    }

    surface = extract_failure_surface(row)

    assert "region_combination" in surface
    assert "region_name" in surface
    assert "mv_esr_store_sales" in surface


def test_extract_failure_surface_reads_asi_metadata_except_response_quality() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        extract_failure_surface,
    )

    row = {
        "question_id": "q_region",
        "schema_accuracy/metadata": {
            "failure_type": "wrong_column",
            "blame_set": ["region_name", "region_combination"],
            "wrong_clause": "GROUP BY region_name",
            "counterfactual_fix": "Use region_combination for region grouping.",
        },
        "response_quality/metadata": {
            "failure_type": "other",
            "blame_set": ["friendly wording"],
            "counterfactual_fix": "Write a nicer sentence.",
        },
    }

    surface = extract_failure_surface(row)

    assert "region_combination" in surface
    assert "region_name" in surface
    assert "friendly" not in surface
    assert "wording" not in surface


def test_causal_relevance_uses_target_qids_and_rca_metadata() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        causal_relevance_score,
        explain_causal_relevance,
    )

    patch = {
        "type": "update_column_description",
        "column": "region_combination",
        "target_qids": ["q_region"],
        "rca_id": "rca_region",
    }
    rows = [
        {
            "question_id": "q_region",
            "schema_accuracy/metadata": {
                "failure_type": "wrong_column",
                "blame_set": ["region_name", "region_combination"],
                "counterfactual_fix": "Use region_combination for region grouping.",
            },
        },
        {
            "question_id": "q_other",
            "inputs.expected_sql": "SELECT sales_amount FROM mv_sales",
        },
    ]

    score = causal_relevance_score(patch, rows, target_qids=("q_region",))
    details = explain_causal_relevance(patch, rows, target_qids=("q_region",))

    assert score == 1.0
    assert details["target_qids"] == ["q_region"]
    assert "region_combination" in details["overlap"]


def test_proposal_grounding_ignored_metadata_matches_config() -> None:
    """``proposal_grounding._IGNORED_METADATA_PREFIXES`` must mirror
    ``common.config.IGNORED_OPTIMIZATION_JUDGES`` so the optimizer
    engine has a single ignored-judge policy.
    """
    from genie_space_optimizer.common.config import (
        IGNORED_OPTIMIZATION_JUDGES as CONFIG_IGNORED,
    )
    from genie_space_optimizer.optimization.proposal_grounding import (
        _IGNORED_METADATA_PREFIXES,
    )

    assert isinstance(_IGNORED_METADATA_PREFIXES, frozenset)
    assert _IGNORED_METADATA_PREFIXES == frozenset(CONFIG_IGNORED)


def test_instruction_patch_grounds_on_body_text_and_rca_terms() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        causal_relevance_score,
        explain_causal_relevance,
    )

    patch = {
        "type": "add_instruction",
        "section_name": "FUNCTION ROUTING",
        "new_text": (
            "When users ask for fn_mtd_or_mtday, use the registered TVF "
            "instead of inlining CASE logic."
        ),
        "rca_id": "rca_fn",
        "patch_family": "function_routing_guidance",
        "target_qids": ["q_fn"],
        "_rca_grounding_terms": ["fn_mtd_or_mtday", "tvf"],
    }
    rows = [
        {
            "question_id": "q_fn",
            "schema_accuracy/metadata": {
                "failure_type": "wrong_column",
                "blame_set": ["fn_mtd_or_mtday"],
                "counterfactual_fix": "Use fn_mtd_or_mtday rather than inlining CASE logic.",
            },
        }
    ]

    assert causal_relevance_score(patch, rows, target_qids=("q_fn",)) == 1.0
    details = explain_causal_relevance(patch, rows, target_qids=("q_fn",))
    assert "fn_mtd_or_mtday" in details["overlap"]
    assert details["target_qids"] == ["q_fn"]


def test_instruction_patch_does_not_ground_when_body_lacks_rca_terms() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import causal_relevance_score

    patch = {
        "type": "add_instruction",
        "section_name": "FUNCTION ROUTING",
        "new_text": "Use clear instructions for functions.",
        "target_qids": ["q_fn"],
    }
    rows = [
        {
            "question_id": "q_fn",
            "schema_accuracy/metadata": {
                "failure_type": "wrong_column",
                "blame_set": ["fn_mtd_or_mtday"],
            },
        }
    ]

    assert causal_relevance_score(patch, rows, target_qids=("q_fn",)) == 0.0



def test_causal_relevance_supports_slash_style_qids_and_surfaces() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        causal_relevance_score,
        explain_causal_relevance,
    )

    rows = [
        {
            "inputs/question_id": "7now_delivery_analytics_space_gs_025",
            "inputs/question": "Which zone VPs stores have the highest total CY sales?",
            "inputs/expected_response": (
                "SELECT zone_vp_name, SUM(cy_sales) AS total_cy_sales "
                "FROM mv_7now_fact_sales GROUP BY zone_vp_name "
                "ORDER BY total_cy_sales DESC"
            ),
            "outputs/response": (
                "SELECT zone_vp_name, total_cy_sales FROM ranked "
                "WHERE rank = 1"
            ),
            "schema_accuracy/metadata": {
                "failure_type": "wrong_column",
                "blame_set": ["RANK()", "rank_filter"],
                "counterfactual_fix": (
                    "Remove WHERE rank = 1 and return all zone VPs ordered "
                    "by total_cy_sales DESC."
                ),
            },
        }
    ]
    patch = {
        "type": "update_instruction_section",
        "section_name": "QUERY PATTERNS",
        "new_text": (
            "- For plural highest/lowest questions, return all grouped "
            "entities ordered by the metric; do not add WHERE rank = 1."
        ),
        "target_qids": ["7now_delivery_analytics_space_gs_025"],
    }

    score = causal_relevance_score(
        patch,
        rows,
        target_qids=("7now_delivery_analytics_space_gs_025",),
    )
    details = explain_causal_relevance(
        patch,
        rows,
        target_qids=("7now_delivery_analytics_space_gs_025",),
    )

    assert score > 0.0
    assert details["scoped_row_count"] == 1
    assert "rank" in details["overlap"]


def test_rca_grounding_terms_are_sufficient_when_they_overlap_surface() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        causal_relevance_score,
        explain_causal_relevance,
    )

    row = {
        "inputs/question_id": "q_topn",
        "inputs/question": "Which zone VPs stores have the highest total CY sales?",
        "schema_accuracy/metadata": {
            "failure_type": "wrong_column",
            "blame_set": ["rank_filter"],
            "counterfactual_fix": "Remove WHERE rank = 1.",
        },
    }
    patch = {
        "type": "update_instruction_section",
        "section_name": "QUERY PATTERNS",
        "new_text": "- Preserve plural cardinality for ordered ranking questions.",
        "_rca_grounding_terms": ["rank_filter"],
    }

    assert causal_relevance_score(patch, [row], target_qids=("q_topn",)) == 1.0
    details = explain_causal_relevance(patch, [row], target_qids=("q_topn",))
    assert details["rca_overlap"] == ["rank_filter"]
    assert details["failure_category"] == "grounded"


def test_grounding_explanation_distinguishes_no_scoped_rows_and_empty_surface() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import (
        explain_causal_relevance,
    )

    patch = {"type": "update_column_description", "column": "zone_vp_name"}

    no_rows = explain_causal_relevance(patch, [], target_qids=("q_missing",))
    assert no_rows["failure_category"] == "no_scoped_rows"

    empty_surface = explain_causal_relevance(
        patch,
        [{"inputs/question_id": "q_empty"}],
        target_qids=("q_empty",),
    )
    assert empty_surface["failure_category"] == "empty_surface"
