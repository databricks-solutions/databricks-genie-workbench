from unittest.mock import MagicMock

from genie_space_optimizer.integration import uc_metadata


def test_obo_schema_prefetch_includes_column_tags(monkeypatch) -> None:
    captured = {}

    def capture(_ws, *, warehouse_id, queries):
        captured.update(queries)
        return {}

    monkeypatch.setattr(uc_metadata, "_execute_queries", capture)

    uc_metadata.fetch_uc_metadata_obo(
        MagicMock(),
        warehouse_id="warehouse",
        catalog="main",
        schema_name="sales",
    )

    assert "information_schema.table_tags" in captured["uc_tags"]
    assert "information_schema.column_tags" in captured["uc_tags"]


def test_obo_table_prefetch_includes_column_tags(monkeypatch) -> None:
    captured = {}

    def capture(_ws, *, warehouse_id, queries):
        captured.update(queries)
        return {}

    monkeypatch.setattr(uc_metadata, "_execute_queries", capture)

    uc_metadata.fetch_uc_metadata_obo(
        MagicMock(),
        warehouse_id="warehouse",
        catalog="main",
        schema_name="sales",
        genie_table_refs=[("main", "sales", "customers")],
    )

    assert "information_schema.table_tags" in captured["uc_tags"]
    assert "information_schema.column_tags" in captured["uc_tags"]
