from __future__ import annotations

import json

from genie_space_optimizer.optimization import benchmark_quality as quality


def _patch_deterministic_checks(monkeypatch, *, valid: bool = True) -> None:
    monkeypatch.setattr(
        quality,
        "validate_benchmarks",
        lambda benchmarks, *_args, **_kwargs: [
            {"valid": valid, "error": "syntax error" if not valid else ""}
            for _ in benchmarks
        ],
    )
    monkeypatch.setattr(
        quality,
        "validate_predicate_values",
        lambda benchmarks, *_args, **_kwargs: [
            {"valid": True, "mismatches": []} for _ in benchmarks
        ],
    )
    monkeypatch.setattr(
        quality,
        "validate_gt_returns_results",
        lambda benchmarks, *_args, **_kwargs: [
            {"has_results": True, "row_count": 1, "error": None}
            for _ in benchmarks
        ],
    )


def test_alignment_error_excludes_benchmark(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    monkeypatch.setattr(
        quality,
        "_llm_review",
        lambda *_args, **_kwargs: (
            [
                quality._finding(
                    question_id="q1",
                    question="Revenue by region",
                    source="genie_space",
                    category=quality.QUESTION_SQL_ALIGNMENT,
                    code="EXTRA_FILTER",
                    severity="error",
                    explanation="SQL adds an active-only filter.",
                    confidence=0.96,
                )
            ],
            set(),
        ),
    )

    result = quality.review_benchmark_quality(
        [{"id": "q1", "question": "Revenue by region", "expected_sql": "SELECT 1"}],
        object(),
        config={},
    )

    assert result["accepted"] == []
    assert len(result["excluded"]) == 1
    assert result["counts"]["excluded"] == 1
    assert result["benchmark_results"][0]["disposition"] == "excluded"


def test_weak_but_answerable_remains_eligible(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    monkeypatch.setattr(
        quality,
        "_llm_review",
        lambda *_args, **_kwargs: (
            [
                quality._finding(
                    question_id="q1",
                    question="Show revenue",
                    source="synthetic",
                    category=quality.QUESTION_QUALITY,
                    code="WEAK_BUT_ANSWERABLE",
                    severity="warning",
                    explanation="The canonical revenue instruction makes this answerable.",
                    confidence=0.88,
                )
            ],
            set(),
        ),
    )

    result = quality.review_benchmark_quality(
        [{"id": "q1", "question": "Show revenue", "expected_sql": "SELECT 1"}],
        object(),
        config={},
    )

    assert len(result["accepted"]) == 1
    assert result["excluded"] == []
    assert result["counts"]["warnings"] == 1


def test_review_failure_is_explicit_and_does_not_silently_pass(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    monkeypatch.setattr(
        quality,
        "_llm_review",
        lambda *_args, **_kwargs: ([], {"q1"}),
    )

    result = quality.review_benchmark_quality(
        [{"id": "q1", "question": "Show revenue", "expected_sql": "SELECT 1"}],
        object(),
        config={},
    )

    assert result["review_status"] == "degraded"
    assert result["semantic_review_coverage"] == 0.0
    assert result["counts"]["review_not_run"] == 1
    assert result["findings"][0]["code"] == "REVIEW_NOT_RUN"
    assert result["benchmark_results"][0]["disposition"] == "warning"


def test_llm_review_requires_complete_stable_question_ids(monkeypatch) -> None:
    from genie_space_optimizer.optimization import benchmarking

    monkeypatch.setattr(
        benchmarking,
        "_build_schema_contexts",
        lambda *_args, **_kwargs: {
            "valid_assets_context": "assets",
            "tables_context": "tables",
            "metric_views_context": "metric views",
            "tvfs_context": "functions",
            "join_specs_context": "joins",
            "instructions_context": "instructions",
            "sample_questions_context": "samples",
            "column_allowlist": "columns",
            "data_profile_context": "profile",
        },
    )
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps(
            [
                {
                    "question_id": "space_q_1",
                    "confidence": 0.9,
                    "issues": [],
                    "proposed_question": None,
                    "proposed_sql": None,
                }
            ]
        ),
    )

    findings, incomplete = quality._llm_review(
        [
            {"id": "space_q_1", "question": "Q1", "expected_sql": "SELECT 1"},
            {"id": "space_q_2", "question": "Q2", "expected_sql": "SELECT 2"},
        ],
        config={},
        uc_columns=[],
        uc_routines=[],
        batch_size=10,
    )

    assert findings == []
    assert incomplete == {"space_q_2"}


def test_low_confidence_llm_error_is_advisory(monkeypatch) -> None:
    from genie_space_optimizer.optimization import benchmarking

    monkeypatch.setattr(
        benchmarking,
        "_build_schema_contexts",
        lambda *_args, **_kwargs: {
            "valid_assets_context": "assets",
            "tables_context": "tables",
            "metric_views_context": "metric views",
            "tvfs_context": "functions",
            "join_specs_context": "joins",
            "instructions_context": "instructions",
        },
    )
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps([{
            "question_id": "q1",
            "confidence": 0.4,
            "issues": [{
                "category": "question_quality",
                "code": "AMBIGUOUS_METRIC",
                "severity": "error",
                "explanation": "Metric may be ambiguous.",
            }],
        }]),
    )

    findings, incomplete = quality._llm_review(
        [{"id": "q1", "question": "Show performance", "expected_sql": "SELECT 1"}],
        config={}, uc_columns=[], uc_routines=[], batch_size=10,
    )

    assert incomplete == set()
    assert findings[0]["severity"] == "warning"


def test_non_finite_confidence_cannot_exclude_or_leak_malformed_repair(monkeypatch) -> None:
    from genie_space_optimizer.optimization import benchmarking

    monkeypatch.setattr(
        benchmarking,
        "_build_schema_contexts",
        lambda *_args, **_kwargs: {
            "valid_assets_context": "assets",
            "tables_context": "tables",
            "metric_views_context": "metric views",
            "tvfs_context": "functions",
            "join_specs_context": "joins",
            "instructions_context": "instructions",
        },
    )
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps([{
            "question_id": "q1",
            "confidence": float("nan"),
            "issues": [{
                "category": "question_sql_alignment",
                "code": "WRONG_METRIC",
                "severity": "error",
                "explanation": "Metric may be wrong.",
            }],
            "proposed_question": {"not": "text"},
            "proposed_sql": ["SELECT 1"],
        }]),
    )

    findings, incomplete = quality._llm_review(
        [{"id": "q1", "question": "Show performance", "expected_sql": "SELECT 1"}],
        config={}, uc_columns=[], uc_routines=[], batch_size=10,
    )

    assert incomplete == set()
    assert findings[0]["confidence"] == 0.0
    assert findings[0]["severity"] == "warning"
    assert findings[0]["proposed_question"] is None
    assert findings[0]["proposed_sql"] is None


def test_compile_failure_is_classified_without_semantic_review(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch, valid=False)
    semantic_called = False

    def _semantic(*_args, **_kwargs):
        nonlocal semantic_called
        semantic_called = True
        return [], set()

    monkeypatch.setattr(quality, "_llm_review", _semantic)
    result = quality.review_benchmark_quality(
        [{"id": "q1", "question": "Question", "expected_sql": "SELEC 1"}],
        object(),
        config={},
    )

    assert semantic_called is False
    assert result["findings"][0]["category"] == quality.SQL_VALIDITY
    assert result["findings"][0]["code"] == "SYNTAX_ERROR"


def _patch_quality_context(monkeypatch) -> None:
    from genie_space_optimizer.optimization import benchmarking

    monkeypatch.setattr(
        benchmarking,
        "_build_schema_contexts",
        lambda *_args, **_kwargs: {
            "valid_assets_context": "assets",
            "tables_context": "tables",
            "metric_views_context": "metric views",
            "tvfs_context": "functions",
            "join_specs_context": "joins",
            "instructions_context": "instructions",
        },
    )


def test_high_confidence_generated_implementation_hint_is_excluded(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    _patch_quality_context(monkeypatch)
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps([{
            "question_id": "q1",
            "confidence": 0.96,
            "issues": [{
                "category": "question_quality",
                "code": "IMPLEMENTATION_HINT",
                "severity": "warning",
                "explanation": "The question tells the evaluator which join to use.",
            }],
            "proposed_question": "How many tickets were created per account segment?",
        }]),
    )

    result = quality.review_benchmark_quality(
        [{
            "id": "q1",
            "question": "Count tickets per segment. Join tickets to accounts.",
            "expected_sql": "SELECT 1",
            "source": "llm_generated",
            "provenance": "synthetic",
        }],
        object(),
        config={},
    )

    assert result["accepted"] == []
    assert len(result["excluded"]) == 1
    assert result["findings"][0]["code"] == "IMPLEMENTATION_HINT"
    assert result["findings"][0]["severity"] == "error"


def test_low_confidence_implementation_hint_is_warning(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    _patch_quality_context(monkeypatch)
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps([{
            "question_id": "q1",
            "confidence": 0.45,
            "issues": [{
                "category": "question_quality",
                "code": "IMPLEMENTATION_HINT",
                "severity": "error",
                "explanation": "This might be an implementation hint.",
            }],
        }]),
    )

    result = quality.review_benchmark_quality(
        [{
            "id": "q1",
            "question": "Show renewal revenue from accounts",
            "expected_sql": "SELECT 1",
            "source": "llm_generated",
            "provenance": "synthetic",
        }],
        object(),
        config={},
    )

    assert len(result["accepted"]) == 1
    assert result["counts"]["warnings"] == 1
    assert result["findings"][0]["severity"] == "warning"


def test_user_authored_implementation_hint_is_advisory_and_not_rewritten(
    monkeypatch,
) -> None:
    _patch_deterministic_checks(monkeypatch)
    _patch_quality_context(monkeypatch)
    original = "Count tickets per segment. Join tickets to accounts."
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps([{
            "question_id": "q1",
            "confidence": 0.99,
            "issues": [{
                "category": "question_quality",
                "code": "IMPLEMENTATION_HINT",
                "severity": "error",
                "explanation": "The question includes a join instruction.",
            }],
            "proposed_question": "How many tickets were created per account segment?",
        }]),
    )

    result = quality.review_benchmark_quality(
        [{
            "id": "q1",
            "question": original,
            "expected_sql": "SELECT 1",
            "source": "genie_benchmark",
            "provenance": "curated",
        }],
        object(),
        config={},
    )

    assert result["excluded"] == []
    assert result["accepted"][0]["question"] == original
    assert result["findings"][0]["severity"] == "warning"
    assert result["findings"][0]["proposed_question"] != original


def test_actionable_warning_proposal_builds_repair_without_mutating_input() -> None:
    benchmark = {
        "id": "q1",
        "space_question_id": "native-q1",
        "question": "Show revenue",
        "expected_sql": "SELECT old_metric FROM revenue",
        "source": "genie_benchmark",
    }
    result = {
        "question_id": "q1",
        "disposition": "warning",
        "findings": [
            quality._finding(
                question_id="q1",
                question="Show revenue",
                source="genie_benchmark",
                category=quality.QUESTION_SQL_ALIGNMENT,
                code="WRONG_METRIC",
                severity="warning",
                confidence=0.65,
                explanation="Use recognized revenue and make the wording precise.",
                expected_sql="SELECT old_metric FROM revenue",
                proposed_question="Show recognized revenue",
                proposed_sql="SELECT recognized_revenue FROM revenue",
            ),
        ],
    }

    candidate, change = quality.build_actionable_warning_repair(benchmark, result)

    assert candidate is not None
    assert candidate["question"] == "Show recognized revenue"
    assert candidate["expected_sql"] == "SELECT recognized_revenue FROM revenue"
    assert benchmark["question"] == "Show revenue"
    assert change is not None
    assert change["question_id"] == "native-q1"
    assert change["reason"] == "benchmark_quality_warning_repair"


def test_actionable_warning_repair_ignores_system_and_conflicting_proposals() -> None:
    benchmark = {
        "id": "q1",
        "question": "Show revenue",
        "expected_sql": "SELECT 1",
    }
    system_result = {
        "question_id": "q1",
        "disposition": "warning",
        "findings": [
            quality._finding(
                question_id="q1",
                question="Show revenue",
                source="genie_benchmark",
                category=quality.REVIEW_SYSTEM,
                code="REVIEW_NOT_RUN",
                severity="warning",
                explanation="Review unavailable.",
                proposed_question="Unsafe suggestion",
            ),
        ],
    }
    assert quality.build_actionable_warning_repair(benchmark, system_result) == (
        None,
        None,
    )

    conflicting_result = {
        "question_id": "q1",
        "disposition": "warning",
        "findings": [
            quality._finding(
                question_id="q1",
                question="Show revenue",
                source="genie_benchmark",
                category=quality.QUESTION_QUALITY,
                code="AMBIGUOUS_METRIC",
                severity="warning",
                explanation="Ambiguous.",
                proposed_question="Show gross revenue",
            ),
            quality._finding(
                question_id="q1",
                question="Show revenue",
                source="genie_benchmark",
                category=quality.QUESTION_SQL_ALIGNMENT,
                code="WRONG_METRIC",
                severity="warning",
                explanation="Wrong metric.",
                proposed_question="Show net revenue",
            ),
        ],
    }
    assert quality.build_actionable_warning_repair(benchmark, conflicting_result) == (
        None,
        None,
    )


def test_legitimate_where_and_from_are_not_deterministically_rejected(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    _patch_quality_context(monkeypatch)
    monkeypatch.setattr(
        quality,
        "_call_quality_llm",
        lambda _prompt: json.dumps([{
            "question_id": "q1",
            "confidence": 0.95,
            "issues": [],
        }]),
    )

    question = "Where did revenue from renewals increase last quarter?"
    result = quality.review_benchmark_quality(
        [{
            "id": "q1",
            "question": question,
            "expected_sql": "SELECT 1",
            "source": "llm_generated",
            "provenance": "synthetic",
        }],
        object(),
        config={},
    )

    assert result["excluded"] == []
    assert result["accepted"][0]["question"] == question
    assert result["counts"]["trusted"] == 1


def test_ground_truth_stability_checks_detect_unsafe_sql() -> None:
    def codes(sql: str) -> set[str]:
        return {
            finding["code"]
            for finding in quality._deterministic_sql_findings(
                {"question": "Question", "expected_sql": sql},
                question_id="q1",
            )
        }

    assert "UNORDERED_LIMIT" in codes("SELECT id FROM orders LIMIT 10")
    assert "UNORDERED_LIMIT" in codes(
        "SELECT * FROM (SELECT id FROM orders LIMIT 10) limited"
    )
    assert "UNORDERED_LIMIT" not in codes(
        "SELECT id FROM orders ORDER BY revenue DESC, id LIMIT 10"
    )
    assert "NONDETERMINISTIC_SQL" in codes("SELECT id FROM orders ORDER BY RAND()")
    assert "NONDETERMINISTIC_SQL" in codes("SELECT * FROM orders TABLESAMPLE (10 PERCENT)")
    assert "VOLATILE_TIME_REFERENCE" in codes(
        "SELECT * FROM orders WHERE order_date >= CURRENT_DATE() - INTERVAL 7 DAYS"
    )


def test_ground_truth_stability_checks_ignore_identifier_names() -> None:
    def codes(sql: str) -> set[str]:
        return {
            finding["code"]
            for finding in quality._deterministic_sql_findings(
                {"question": "Question", "expected_sql": sql},
                question_id="q1",
            )
        }

    assert "NONDETERMINISTIC_SQL" not in codes(
        "SELECT sample FROM sample_orders"
    )
    assert "NONDETERMINISTIC_SQL" not in codes(
        "SELECT `sample` FROM `sample`"
    )
    assert "UNORDERED_LIMIT" not in codes(
        "SELECT `limit` FROM account_limits"
    )


def test_volatile_time_reference_is_advisory(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    monkeypatch.setattr(quality, "_llm_review", lambda *_args, **_kwargs: ([], set()))

    result = quality.review_benchmark_quality(
        [{
            "id": "q1",
            "question": "Show orders from the last seven days",
            "expected_sql": (
                "SELECT order_id FROM orders "
                "WHERE order_date >= CURRENT_DATE() - INTERVAL 7 DAYS"
            ),
        }],
        object(),
        config={},
    )

    assert result["benchmark_results"][0]["disposition"] == "warning"
    assert result["findings"][0]["code"] == "VOLATILE_TIME_REFERENCE"
    assert result["findings"][0]["recommended_action"] == "review_temporal_ground_truth"


def test_unordered_limit_is_excluded_before_semantic_review(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    semantic_called = False

    def semantic_review(*_args, **_kwargs):
        nonlocal semantic_called
        semantic_called = True
        return [], set()

    monkeypatch.setattr(quality, "_llm_review", semantic_review)
    result = quality.review_benchmark_quality(
        [{
            "id": "q1",
            "question": "Show ten customers",
            "expected_sql": "SELECT customer_id FROM customers LIMIT 10",
        }],
        object(),
        config={},
    )

    assert semantic_called is False
    assert result["benchmark_results"][0]["disposition"] == "excluded"
    assert result["findings"][0]["code"] == "UNORDERED_LIMIT"


def test_safe_data_value_correction_is_actionable_warning(monkeypatch) -> None:
    _patch_deterministic_checks(monkeypatch)
    monkeypatch.setattr(
        quality,
        "validate_predicate_values",
        lambda *_args, **_kwargs: [{
            "valid": False,
            "mismatches": [{
                "table": "cat.sch.orders",
                "column": "state_code",
                "literal": "California",
                "profiled_values": ["CA"],
                "suggestion": "CA",
            }],
        }],
    )
    monkeypatch.setattr(quality, "_llm_review", lambda *_args, **_kwargs: ([], set()))
    benchmark = {
        "id": "q1",
        "question": "Show California orders",
        "expected_sql": "SELECT order_id FROM orders WHERE state_code = 'California'",
    }
    result = quality.review_benchmark_quality(
        [benchmark],
        object(),
        config={"_data_profile": {"orders": {"columns": {}}}},
    )

    assert result["benchmark_results"][0]["disposition"] == "warning"
    finding = result["findings"][0]
    assert finding["code"] == "DATA_VALUE_MISMATCH"
    assert finding["proposed_sql"].endswith("state_code = 'CA'")
    repaired, _change = quality.build_actionable_warning_repair(
        benchmark,
        result["benchmark_results"][0],
    )
    assert repaired is not None
    assert repaired["expected_sql"].endswith("state_code = 'CA'")


def test_value_access_gap_distinguishes_agent_metadata_from_bad_sql() -> None:
    benchmark = {
        "id": "q1",
        "question": "Show customers in California",
        "expected_sql": (
            "SELECT customer_id FROM cat.sch.customers c WHERE c.state_code = 'CA'"
        ),
    }
    config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.customers",
                    "column_configs": [{
                        "column_name": "state_code",
                        "enable_format_assistance": False,
                        "enable_entity_matching": False,
                    }],
                }],
            },
        },
    }

    findings = quality._value_access_findings(
        benchmark,
        question_id="q1",
        config=config,
    )
    assert len(findings) == 1
    assert findings[0]["code"] == "VALUE_ACCESS_GAP"
    assert findings[0]["recommended_action"] == "enable_value_matching"
    assert findings[0]["evidence"]["implicit_sql_values"] == ["CA"]

    config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"][0][
        "enable_entity_matching"
    ] = True
    assert quality._value_access_findings(
        benchmark,
        question_id="q1",
        config=config,
    ) == []


def test_value_access_gap_detects_configured_asset_without_column_configs() -> None:
    benchmark = {
        "id": "q1",
        "question": "Show customers in California",
        "expected_sql": (
            "SELECT customer_id FROM cat.sch.customers c WHERE c.state_code = 'CA'"
        ),
    }
    config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [{"identifier": "cat.sch.customers"}],
            },
        },
    }

    findings = quality._value_access_findings(
        benchmark,
        question_id="q1",
        config=config,
    )

    assert [finding["code"] for finding in findings] == ["VALUE_ACCESS_GAP"]
    assert findings[0]["evidence"]["column"] == "state_code"


def test_format_coverage_reports_missing_and_complete_corpora() -> None:
    complete = [
        {"category": "aggregation", "question": "Total sales", "expected_sql": "SELECT SUM(x)"},
        {"category": "detail", "question": "What is customer A's status?", "expected_sql": "SELECT status"},
        {"category": "comparison", "question": "Compare A versus B", "expected_sql": "SELECT 1"},
        {"category": "time-series", "question": "Sales last month", "expected_sql": "SELECT 1"},
        {"category": "ranking", "question": "Top customers", "expected_sql": "SELECT 1 ORDER BY x LIMIT 5"},
    ]
    covered = quality.review_benchmark_format_coverage(complete)
    assert covered["missing"] == []
    assert covered["findings"] == []

    incomplete = quality.review_benchmark_format_coverage(complete[:2])
    assert set(incomplete["missing"]) == {"comparison", "time_filter", "top_n"}
    assert incomplete["findings"][0]["code"] == "MISSING_QUESTION_FORMAT"
