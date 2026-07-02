from __future__ import annotations


def test_prepare_fk_collector_uses_rest_first(monkeypatch):
    from genie_space_optimizer.optimization import harness as h_mod

    calls = []

    def _rest(w, refs):
        calls.append("rest")
        return [
            {
                "child_table": "cat.sch.fact_sales",
                "child_columns": ["location_id"],
                "parent_table": "cat.sch.dim_location",
                "parent_columns": ["location_id"],
                "constraint_name": "fk_location",
            }
        ]

    def _spark(spark, refs):
        calls.append("spark")
        return []

    from genie_space_optimizer.common import uc_metadata as _ucm
    monkeypatch.setattr(_ucm, "get_foreign_keys_for_tables_rest", _rest, raising=False)
    monkeypatch.setattr(_ucm, "get_foreign_keys_for_tables", _spark, raising=False)

    rows = h_mod._collect_uc_foreign_keys_for_enrichment(
        w=object(),
        spark=object(),
        table_refs=[("cat", "sch", "fact_sales")],
    )

    assert calls == ["rest"]
    assert rows[0]["constraint_name"] == "fk_location"
