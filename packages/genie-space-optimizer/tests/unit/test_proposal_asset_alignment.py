from __future__ import annotations

from genie_space_optimizer.optimization.proposal_asset_alignment import (
    cluster_lineage_assets,
    proposal_target_assets,
    proposal_aligns_with_cluster,
)


def test_cluster_lineage_assets_includes_blame_and_reference_assets() -> None:
    cluster = {
        "cluster_id": "C001",
        "blame_assets": ["main.demo.mv_7now_store_sales"],
        "reference_assets": ["main.demo.mv_7now_store_sales", "main.demo.mv_7now_dim_date"],
        "lineage_assets": ["main.demo.mv_7now_store_sales"],
    }
    assets = cluster_lineage_assets(cluster)
    assert "main.demo.mv_7now_store_sales" in assets
    assert "main.demo.mv_7now_dim_date" in assets
    assert "mv_7now_store_sales" in assets
    assert "mv_7now_dim_date" in assets


def test_proposal_target_assets_extracts_from_target_keys() -> None:
    patch = {
        "id": "P010#1",
        "type": "add_sql_snippet_filter",
        "target_object": "main.demo.mv_esr_dim_date",
        "value": "is_month_to_date='Y'",
    }
    assets = proposal_target_assets(patch)
    assert "main.demo.mv_esr_dim_date" in assets
    assert "mv_esr_dim_date" in assets


def test_off_lineage_proposal_does_not_align() -> None:
    cluster = {
        "blame_assets": ["main.demo.mv_7now_store_sales"],
        "reference_assets": [],
        "lineage_assets": [],
    }
    patch = {
        "id": "P010#1",
        "type": "add_sql_snippet_filter",
        "target_object": "main.demo.mv_esr_dim_date",
    }
    decision = proposal_aligns_with_cluster(patch, cluster)
    assert decision["aligned"] is False
    assert decision["reason"] == "asset_not_in_cluster_lineage"
    assert decision["proposal_assets"] == ("main.demo.mv_esr_dim_date", "mv_esr_dim_date")
    assert "main.demo.mv_7now_store_sales" in decision["cluster_assets"]


def test_on_lineage_proposal_aligns() -> None:
    cluster = {
        "blame_assets": ["main.demo.mv_7now_store_sales"],
        "reference_assets": [],
        "lineage_assets": [],
    }
    patch = {
        "id": "P002#3",
        "type": "update_column_description",
        "target_object": "main.demo.mv_7now_store_sales.time_window",
    }
    decision = proposal_aligns_with_cluster(patch, cluster)
    assert decision["aligned"] is True
    assert decision["reason"] == "asset_in_cluster_lineage"


def test_cross_asset_justification_overrides_disjoint_check() -> None:
    cluster = {
        "blame_assets": ["main.demo.mv_7now_store_sales"],
        "reference_assets": [],
        "lineage_assets": [],
    }
    patch = {
        "id": "P011#1",
        "type": "add_join_spec",
        "target_object": "main.demo.mv_esr_dim_date",
        "cross_asset_justification": "join target dim required for the fact table fix",
    }
    decision = proposal_aligns_with_cluster(patch, cluster)
    assert decision["aligned"] is True
    assert decision["reason"] == "cross_asset_justification_present"


def test_proposal_with_no_target_asset_aligns_when_lineage_empty() -> None:
    cluster = {"blame_assets": [], "reference_assets": [], "lineage_assets": []}
    patch = {"id": "P012#1", "type": "rewrite_instruction"}
    decision = proposal_aligns_with_cluster(patch, cluster)
    assert decision["aligned"] is True
    assert decision["reason"] == "no_lineage_constraint"


def test_l6_snippet_must_align_with_cluster_asset_without_cross_asset_justification():
    from genie_space_optimizer.optimization.proposal_asset_alignment import (
        proposal_aligns_with_cluster,
    )

    cluster = {
        "cluster_id": "H002",
        "blame_assets": ["cat.sch.tkt_payment"],
        "reference_assets": [],
        "lineage_assets": ["cat.sch.tkt_payment"],
    }
    patch = {
        "type": "add_sql_snippet_expression",
        "lever": 6,
        "target_table": "cat.sch.tkt_document",
        "column": "BASE_FARE_AMT",
    }

    decision = proposal_aligns_with_cluster(patch, cluster)

    assert decision["aligned"] is False
    assert decision["reason"] == "asset_not_in_cluster_lineage"
