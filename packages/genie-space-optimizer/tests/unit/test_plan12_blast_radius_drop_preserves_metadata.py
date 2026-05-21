"""Plan 12 — when blast radius drops a patch, the drop record must
preserve original_patch_type, original_patch_body, causal_target,
failing_sql_anchor, target_qids, collateral_qids, rca_card_id."""


def _patch_dict():
    return {
        "intent_id": "intent_021",
        "patch_type": "add_sql_snippet_filter",
        "patch_body": {
            "name": "mtd_filter",
            "sql_expression": "order_date >= DATE_TRUNC('month', CURRENT_DATE)",
        },
        "causal_target": "catalog.schema.orders.order_date",
        "failing_sql_anchor": "WHERE order_date >= ...",
        "target_qids": ["gs_021"],
        "rca_card_id": "rca_001",
        "cluster_id": "H001",
        "ag_id": "AG1",
        "original_patch_type": "add_sql_snippet_filter",
    }


def test_blast_radius_drop_builds_drop_record_with_full_metadata():
    from genie_space_optimizer.optimization.optimizer import (
        _build_blast_radius_drop_record,
    )

    rec = _build_blast_radius_drop_record(
        patch=_patch_dict(),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ..."},
    )

    assert rec.intent_id == "intent_021"
    assert rec.original_patch_type == "add_sql_snippet_filter"
    assert rec.original_patch_body["sql_expression"]
    assert rec.causal_target == "catalog.schema.orders.order_date"
    assert rec.target_qids == ("gs_021",)
    assert rec.collateral_qids == ("gs_003",)
    assert rec.protected_sql_by_qid["gs_003"]
    assert rec.rca_card_id == "rca_001"


def test_builder_falls_back_from_original_patch_type_to_patch_type():
    """Plan 11 / pre-Plan-12 patches don't carry ``original_patch_type``
    explicitly. The builder must fall back to ``patch_type`` so the
    record still passes I23's required-field check."""
    from genie_space_optimizer.optimization.optimizer import (
        _build_blast_radius_drop_record,
    )

    patch = _patch_dict()
    del patch["original_patch_type"]
    rec = _build_blast_radius_drop_record(
        patch=patch,
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ..."},
    )
    assert rec.original_patch_type == "add_sql_snippet_filter"
