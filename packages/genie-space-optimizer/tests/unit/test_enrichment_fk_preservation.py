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


def test_refresh_config_preserves_uc_foreign_keys(monkeypatch):
    from genie_space_optimizer.optimization import harness as h_mod

    def _fetch_space_config(w, space_id):
        return {"_parsed_space": {"data_sources": {"tables": []}}}

    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        _fetch_space_config,
    )

    config, metadata = h_mod._refresh_config_preserving_mv_state(
        w=object(),
        space_id="space",
        uc_columns=[],
        data_profile={},
        yaml_cache={},
        table_refs=[],
        uc_foreign_keys=[
            {
                "child_table": "cat.sch.fact_sales",
                "child_columns": ["location_id"],
                "parent_table": "cat.sch.dim_location",
                "parent_columns": ["location_id"],
            }
        ],
    )

    assert config["_uc_foreign_keys"][0]["child_table"] == "cat.sch.fact_sales"
    assert metadata["_uc_foreign_keys"][0]["parent_table"] == "cat.sch.dim_location"
