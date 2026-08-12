from unittest.mock import MagicMock

from genie_space_optimizer.common.uc_metadata import (
    get_tags,
    get_tags_for_tables,
)


def test_get_tags_for_tables_queries_column_and_table_tags() -> None:
    spark = MagicMock()

    get_tags_for_tables(spark, [("main", "sales", "customers")])

    sql = spark.sql.call_args.args[0]
    assert "information_schema.table_tags" in sql
    assert "information_schema.column_tags" in sql
    assert "column_name" in sql


def test_get_tags_queries_column_and_table_tags() -> None:
    spark = MagicMock()

    get_tags(spark, "main", "sales")

    sql = spark.sql.call_args.args[0]
    assert "information_schema.table_tags" in sql
    assert "information_schema.column_tags" in sql
    assert "column_name" in sql
