"""Plan 12 — BlastRadiusDropRecord must carry every field
narrow_replacement_with_llm needs."""


def test_record_roundtrip():
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )

    rec = BlastRadiusDropRecord(
        intent_id="intent_001",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={
            "name": "mtd_filter",
            "sql_expression": "order_date >= DATE_TRUNC('month', CURRENT_DATE)",
        },
        causal_target="catalog.schema.orders.order_date",
        failing_sql_anchor="WHERE order_date >= ...",
        target_qids=("gs_021",),
        collateral_qids=("gs_003", "gs_005"),
        protected_sql_by_qid={
            "gs_003": "SELECT * FROM ... WHERE ...",
            "gs_005": "SELECT * FROM ... WHERE ...",
        },
        rca_card_id="rca_001",
        cluster_id="H001",
        ag_id="AG1",
    )
    rt = BlastRadiusDropRecord.from_json(rec.to_json())
    assert rt == rec
    assert rt.original_patch_type == "add_sql_snippet_filter"
    assert rt.collateral_qids == ("gs_003", "gs_005")
    assert rt.protected_sql_by_qid["gs_003"]


def test_record_required_fields_for_narrow_replacement():
    """The narrow-replacement orchestrator must be able to extract
    all fields it needs from a single BlastRadiusDropRecord."""
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
        narrow_replacement_inputs_from,
    )

    rec = BlastRadiusDropRecord(
        intent_id="intent_001",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={"name": "x", "sql_expression": "..."},
        causal_target="catalog.schema.orders.order_date",
        failing_sql_anchor="WHERE order_date >= ...",
        target_qids=("gs_021",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ..."},
        rca_card_id="rca_001",
        cluster_id="H001",
        ag_id="AG1",
    )
    inputs = narrow_replacement_inputs_from(rec)
    assert inputs["original_patch_type"]
    assert inputs["original_patch_body"]
    assert inputs["causal_target"]
    assert inputs["target_qids"] == ("gs_021",)
    assert inputs["collateral_qids"] == ("gs_003",)
