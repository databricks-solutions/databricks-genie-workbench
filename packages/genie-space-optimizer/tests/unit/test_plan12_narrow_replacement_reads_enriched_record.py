"""Plan 12 — narrow_replacement_with_llm accepts a BlastRadiusDropRecord
as the source of truth (rather than scattered kwargs).

The wrapper reconstructs a RepairProposal from the typed record's
original_patch_body + original_patch_type and dispatches to the
existing narrow-replacement LLM loop.
"""
from unittest.mock import MagicMock


def test_narrow_replacement_accepts_drop_record(monkeypatch):
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )
    from genie_space_optimizer.optimization.stages import (
        narrow_replacement as nr,
    )
    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_from_drop_record,
    )
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    rec = BlastRadiusDropRecord(
        intent_id="intent_021",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={"name": "x", "sql_expression": "..."},
        causal_target="catalog.schema.orders.order_date",
        failing_sql_anchor="WHERE ...",
        target_qids=("gs_021",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ..."},
        rca_card_id="rca_001",
        cluster_id="H001",
        ag_id="AG1",
    )
    cluster = FailureCluster(
        cluster_id="H001",
        semantic_theme="mtd filter",
        member_qids=("gs_021",),
        unifying_evidence="...",
        repair_hypothesis="MTD",
        primary_blame_set=("catalog.schema.orders.order_date",),
        confidence="high",
    )

    captured: dict = {}

    def _capture(
        patch,
        *,
        collateral_qids,
        protected_sql,
        cluster,
        w,
        optimization_run_id="",
        iteration=0,
        ag_id="",
        attempt=1,
        max_attempts=None,
    ):
        captured["collateral_qids"] = collateral_qids
        captured["target_qids"] = patch.target_qids
        captured["intent_id"] = patch.intent_id
        captured["patch_type"] = patch.patch_type.value
        captured["ag_id"] = ag_id
        return None

    monkeypatch.setattr(nr, "narrow_replacement_with_llm", _capture)

    narrow_replacement_from_drop_record(
        drop_record=rec,
        cluster=cluster,
        w=None,
        optimization_run_id="run_x",
        iteration=2,
    )

    assert captured["collateral_qids"] == ("gs_003",)
    assert captured["target_qids"] == ("gs_021",)
    assert captured["intent_id"] == "intent_021"
    assert captured["patch_type"] == "add_sql_snippet_filter"
    assert captured["ag_id"] == "AG1"


def test_wrapper_returns_none_on_unknown_patch_type(monkeypatch):
    """Plan 11 / pre-Plan-12 drops with empty or unknown
    original_patch_type cannot reconstruct a RepairProposal — the
    wrapper bails before calling the LLM loop, returning None."""
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )
    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_from_drop_record,
    )
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    rec = BlastRadiusDropRecord(
        intent_id="intent_x",
        original_patch_type="",  # bail-out path
        original_patch_body={},
        causal_target="",
        failing_sql_anchor="",
        target_qids=("gs_021",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={},
        rca_card_id="",
        cluster_id="H001",
        ag_id="AG1",
    )
    cluster = FailureCluster(
        cluster_id="H001",
        semantic_theme="x",
        member_qids=("gs_021",),
        unifying_evidence="x",
        repair_hypothesis="x",
        primary_blame_set=("col.a",),
        confidence="high",
    )

    result = narrow_replacement_from_drop_record(
        drop_record=rec,
        cluster=cluster,
        w=None,
    )
    assert result is None
