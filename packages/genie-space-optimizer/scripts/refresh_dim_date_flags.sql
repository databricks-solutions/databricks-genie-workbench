-- refresh_dim_date_flags.sql — C3, baseline-eval-fix plan.
--
-- Recomputes calendar-grounding flags on the DIM_DATE dimension table
-- against ``CURRENT_DATE()``. Intended to run as part of the benchmark
-- prep notebook (NOT the optimizer runtime) before each evaluation so
-- ``is_current_year`` / ``is_last_12_months`` stay accurate.
--
-- Parameters (set before running):
--   :catalog — UC catalog that owns DIM_DATE
--   :schema  — UC schema that owns DIM_DATE
--   :table   — table name (defaults to ``DIM_DATE``)
--
-- Example usage in Databricks SQL:
--   SET var.catalog = my_cat;
--   SET var.schema  = my_schema;
--   SET var.table   = DIM_DATE;
--   -- then run this script.

-- Safety: fail fast if the table is not a Delta table we can update.
DESCRIBE DETAIL ${var.catalog}.${var.schema}.${var.table};

-- Core refresh. All flags are derived deterministically from
-- ``date_key`` vs ``CURRENT_DATE()``. Extending this block with
-- additional flags is safe — the applier's instructions only reference
-- ``is_current_year``, ``is_last_12_months``, and ``is_last_30_days``.
UPDATE ${var.catalog}.${var.schema}.${var.table}
SET
    is_current_year   = (YEAR(date_key)  = YEAR(CURRENT_DATE())),
    is_current_month  = (YEAR(date_key)  = YEAR(CURRENT_DATE())
                         AND MONTH(date_key) = MONTH(CURRENT_DATE())),
    is_current_week   = (WEEKOFYEAR(date_key) = WEEKOFYEAR(CURRENT_DATE())
                         AND YEAR(date_key) = YEAR(CURRENT_DATE())),
    is_last_12_months = (date_key >= ADD_MONTHS(CURRENT_DATE(), -12)
                         AND date_key <= CURRENT_DATE()),
    is_last_30_days   = (date_key >= DATE_SUB(CURRENT_DATE(), 30)
                         AND date_key <= CURRENT_DATE()),
    is_last_7_days    = (date_key >= DATE_SUB(CURRENT_DATE(), 7)
                         AND date_key <= CURRENT_DATE()),
    is_today          = (date_key = CURRENT_DATE());

-- Quick post-update sanity check — asserts that at least one row is
-- flagged as today. If this returns 0 your DIM_DATE is missing today's
-- row and the benchmark prep step that generates it must run first.
SELECT
    CURRENT_DATE() AS today,
    COUNT_IF(is_today) AS today_rows,
    COUNT_IF(is_current_year) AS current_year_rows,
    COUNT_IF(is_last_12_months) AS last_12mo_rows,
    MAX(date_key) FILTER (WHERE is_current_year) AS latest_current_year_date
FROM ${var.catalog}.${var.schema}.${var.table};
