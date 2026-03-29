"""Tests for SQL read-only validation (backend/sql_executor.py).

Tests validate_sql_read_only() — a security boundary that blocks destructive
SQL. Pure function, no mocking required.
"""

import pytest

from backend.sql_executor import validate_sql_read_only, SqlValidationError


# ---------------------------------------------------------------------------
# Valid queries (should not raise)
# ---------------------------------------------------------------------------

class TestValidQueries:
    def test_simple_select(self):
        validate_sql_read_only("SELECT * FROM orders")

    def test_select_with_where(self):
        validate_sql_read_only("SELECT id, name FROM users WHERE active = true")

    def test_cte_with_select(self):
        validate_sql_read_only(
            "WITH cte AS (SELECT id FROM orders) SELECT * FROM cte"
        )

    def test_multiline_select(self):
        validate_sql_read_only("""
            SELECT
                o.id,
                c.name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            WHERE o.amount > 100
        """)

    def test_lowercase_select(self):
        validate_sql_read_only("select * from orders")

    def test_select_with_subquery(self):
        validate_sql_read_only(
            "SELECT * FROM orders WHERE id IN (SELECT order_id FROM returns)"
        )


# ---------------------------------------------------------------------------
# Invalid queries (should raise SqlValidationError)
# ---------------------------------------------------------------------------

class TestBlockedQueries:
    def test_drop_table(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DROP TABLE users")

    def test_delete(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DELETE FROM orders WHERE id = 1")

    def test_insert(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("INSERT INTO logs VALUES (1, 'test')")

    def test_update(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("UPDATE users SET active = false")

    def test_alter_table(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("ALTER TABLE users ADD COLUMN email STRING")

    def test_create_table(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("CREATE TABLE evil (id INT)")

    def test_grant(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("GRANT ALL ON orders TO attacker")

    def test_revoke(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("REVOKE SELECT ON orders FROM user")

    def test_exec(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("EXEC dangerous_procedure")

    def test_truncate(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("TRUNCATE TABLE orders")

    def test_statement_chaining(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("SELECT 1; DROP TABLE users")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_must_start_with_select_or_with(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("SHOW TABLES")

    def test_leading_whitespace_ok(self):
        validate_sql_read_only("  SELECT 1")

    def test_case_insensitive_blocking(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("drop table users")

    def test_mixed_case_blocking(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DrOp TaBlE users")
