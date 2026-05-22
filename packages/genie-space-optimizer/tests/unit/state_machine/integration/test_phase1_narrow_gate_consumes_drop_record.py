"""Narrow replacement reads every field from the enriched drop record."""
from genie_space_optimizer.optimization.blast_radius_drop_record import (
    BlastRadiusDropRecord,
)
from genie_space_optimizer.optimization.stages.narrow_replacement import (
    build_narrow_replacement_llm_input,
)


def test_llm_input_includes_protected_sql_for_each_collateral():
    rec = BlastRadiusDropRecord(
        intent_id="i",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={"sql_expression": "time_window = 'mtd'"},
        causal_target="time_window",
        failing_sql_anchor="tkt_payment.time_window",
        target_qids=("gs_024",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ... WHERE PAYMENT_CURRENCY_CD = 'USD'"},
        rca_card_id="r",
        cluster_id="H001",
        ag_id="AG_24",
    )
    llm_input = build_narrow_replacement_llm_input(rec)
    # The gate consumes the first target_qid as the primary target.
    assert llm_input["target_qid"] == "gs_024"
    assert llm_input["original_patch_body"]["sql_expression"] == "time_window = 'mtd'"
    assert "gs_003" in llm_input["protected_sql_by_qid"]
