"""BlastRadiusDropRecord carries every field narrow_replacement_with_llm needs.

The record was enriched in Plan 12 (PR 4) with the metadata superset Phase 1
needs. This test locks the contract: every Phase 1 required field must be
present on the existing record and round-trip cleanly.

Note on naming: the existing record uses ``target_qids`` (tuple) to match
the actual N-to-1 nature of L6 patches (a single patch can target multiple
QIDs). The narrow-replacement gate consumes the first element as the
"primary target QID" when single-QID semantics are required.
"""
from genie_space_optimizer.optimization.blast_radius_drop_record import (
    BlastRadiusDropRecord,
)


def test_drop_record_has_required_fields():
    rec = BlastRadiusDropRecord(
        intent_id="intent_xyz",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={"sql_expression": "time_window = 'mtd'"},
        causal_target="time_window",
        failing_sql_anchor="tkt_payment.time_window",
        target_qids=("gs_024",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ... WHERE PAYMENT_CURRENCY_CD = 'USD'"},
        rca_card_id="rca_abc",
        cluster_id="H001",
        ag_id="AG_24",
    )
    assert rec.original_patch_body["sql_expression"] == "time_window = 'mtd'"
    assert rec.protected_sql_by_qid["gs_003"].startswith("SELECT")
    assert rec.target_qids == ("gs_024",)


def test_drop_record_roundtrip():
    rec = BlastRadiusDropRecord(
        intent_id="i",
        original_patch_type="t",
        original_patch_body={"name": "b"},
        causal_target="c",
        failing_sql_anchor="a",
        target_qids=("gs_024",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT 1"},
        rca_card_id="r",
        cluster_id="H001",
        ag_id="AG_24",
    )
    assert BlastRadiusDropRecord.from_json(rec.to_json()) == rec
