from genie_space_optimizer.optimization.evaluation import (
    _benchmark_space_routine_violations,
    _extract_fully_qualified_routine_calls,
)


def test_extract_fully_qualified_routine_calls_ignores_tables_and_builtins() -> None:
    sql = """
    SELECT
      zone_combination,
      cat.sch.fn_mtd_or_mtday(MEASURE(`_7now_cy_sales_mtd`), MEASURE(`_7now_cy_sales_day`)) AS value,
      coalesce(country_code, 'US') AS country
    FROM cat.sch.mv_7now_store_sales
    WHERE zone_combination IS NOT NULL
    """

    assert _extract_fully_qualified_routine_calls(sql) == {
        "cat.sch.fn_mtd_or_mtday",
    }


def test_extract_fully_qualified_routine_calls_handles_backticks() -> None:
    sql = "SELECT `cat`.`sch`.`fn_mtd_or_mtday`(MEASURE(metric)) FROM `cat`.`sch`.`mv`"

    assert _extract_fully_qualified_routine_calls(sql) == {
        "cat.sch.fn_mtd_or_mtday",
    }


def test_benchmark_space_routine_violations_rejects_function_not_in_space() -> None:
    config = {
        "_functions": [],
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.mv_7now_store_sales",
                        "column_configs": [
                            {"column_name": "_7now_cy_sales_mtd", "column_type": "measure"},
                            {"column_name": "_7now_cy_sales_day", "column_type": "measure"},
                        ],
                    }
                ],
                "functions": [],
            }
        },
    }
    sql = """
    SELECT cat.sch.fn_mtd_or_mtday(MEASURE(`_7now_cy_sales_mtd`), MEASURE(`_7now_cy_sales_day`))
    FROM cat.sch.mv_7now_store_sales
    """

    violations = _benchmark_space_routine_violations(sql, config)

    assert violations == ["cat.sch.fn_mtd_or_mtday"]


def test_benchmark_space_routine_violations_allows_registered_function() -> None:
    config = {
        "_functions": ["cat.sch.fn_mtd_or_mtday"],
        "_parsed_space": {
            "data_sources": {
                "functions": [{"identifier": "cat.sch.fn_mtd_or_mtday"}],
            }
        },
    }
    sql = "SELECT cat.sch.fn_mtd_or_mtday(MEASURE(a), MEASURE(b)) FROM cat.sch.mv_7now_store_sales"

    assert _benchmark_space_routine_violations(sql, config) == []


def test_mark_function_not_in_space_marks_candidate_invalid() -> None:
    from genie_space_optimizer.optimization.evaluation import (
        _mark_function_not_in_space_if_needed,
    )

    config = {"_functions": []}
    candidate = {
        "question": "Use fn_mtd_or_mtday by zone",
        "expected_sql": "SELECT cat.sch.fn_mtd_or_mtday(MEASURE(a), MEASURE(b)) FROM cat.sch.mv",
        "validation_status": "valid",
        "validation_reason_code": "ok",
        "validation_error": None,
    }

    assert _mark_function_not_in_space_if_needed(candidate, config) is True

    assert candidate["validation_status"] == "invalid"
    assert candidate["validation_reason_code"] == "function_not_in_space"
    assert candidate["quarantine_reason_code"] == "function_not_in_space"
    assert "cat.sch.fn_mtd_or_mtday" in candidate["validation_error"]


def test_mark_function_not_in_space_leaves_registered_candidate_valid() -> None:
    from genie_space_optimizer.optimization.evaluation import (
        _mark_function_not_in_space_if_needed,
    )

    config = {"_functions": ["cat.sch.fn_mtd_or_mtday"]}
    candidate = {
        "question": "Use fn_mtd_or_mtday by zone",
        "expected_sql": "SELECT cat.sch.fn_mtd_or_mtday(MEASURE(a), MEASURE(b)) FROM cat.sch.mv",
        "validation_status": "valid",
        "validation_reason_code": "ok",
        "validation_error": None,
    }

    assert _mark_function_not_in_space_if_needed(candidate, config) is False
    assert candidate["validation_status"] == "valid"
    assert candidate["validation_reason_code"] == "ok"


def test_benchmark_llm_calls_are_wrapped_in_named_chain_spans() -> None:
    import inspect

    from genie_space_optimizer.optimization import evaluation

    source = inspect.getsource(evaluation.generate_benchmarks)
    correction_source = inspect.getsource(evaluation._attempt_sql_correction)

    assert 'name="benchmark_generation"' in source
    assert 'name="benchmark_correction"' in correction_source
    assert "SpanType.CHAIN" in source
    assert "SpanType.CHAIN" in correction_source
