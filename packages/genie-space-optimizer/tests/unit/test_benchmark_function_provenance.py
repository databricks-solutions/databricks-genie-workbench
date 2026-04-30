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
