"""LLM-driven narrow replacement gate.

When blast_radius rejects a patch as high_collateral_risk_flagged, this
gate calls the LLM with (patch, passing_dependents_outside_target,
their_sql) and asks for one of:
  - accept (the patch is actually safe in this context)
  - narrow_to (return a scoped patch that won't affect dependents)
  - pivot_to_example_sql (return an add_example_sql for just the failing qid)
  - reject_unfixable (no safe scope possible)
"""
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.blast_radius_drop_record import (
    BlastRadiusDropRecord,
)


def _make_drop_record() -> BlastRadiusDropRecord:
    return BlastRadiusDropRecord(
        intent_id="intent_gs_024_v1",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={
            "patch_type": "add_sql_snippet_filter",
            "target_object": "payments.amount",
            "snippet": "WHERE currency = 'USD'",
        },
        causal_target="payments.currency",
        failing_sql_anchor="SUM(amount)",
        target_qids=("gs_024",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT SUM(amount) FROM payments WHERE region='EU'"},
        rca_card_id="rca_gs_024_missing_filter_v1",
        cluster_id="H002",
        ag_id="AG_DECOMPOSED_H002",
    )


def test_narrow_replacement_returns_scoped_patch_when_llm_proposes_narrow():
    from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
        run_narrow_replacement,
    )

    drop = _make_drop_record()
    mock_llm = MagicMock(return_value={
        "decision": "narrow_to",
        "narrowed_patch": {
            "patch_type": "narrow_l6_filter",
            "target_object": "payments.amount",
            "snippet": "WHERE currency = 'USD' AND region <> 'EU'",
            "target_qids": ["gs_024"],
        },
        "rationale": "Excluding region='EU' protects gs_003.",
    })
    verdict = run_narrow_replacement(drop=drop, llm_call=mock_llm)
    assert verdict.decision == "narrow_to"
    assert verdict.scoped_patch["patch_type"] == "narrow_l6_filter"
    assert "gs_024" in verdict.scoped_patch["target_qids"]


def test_narrow_replacement_pivots_to_example_sql_when_llm_says_unfixable_at_l6():
    from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
        run_narrow_replacement,
    )

    drop = _make_drop_record()
    mock_llm = MagicMock(return_value={
        "decision": "pivot_to_example_sql",
        "example_sql": {
            "patch_type": "add_example_sql",
            "example_question": "What is the total USD payment amount?",
            "example_sql": "SELECT SUM(amount) FROM payments WHERE currency = 'USD'",
            "target_qids": ["gs_024"],
        },
        "rationale": "L6 narrowing impossible without breaking gs_003; pivoting to per-question example.",
    })
    verdict = run_narrow_replacement(drop=drop, llm_call=mock_llm)
    assert verdict.decision == "pivot_to_example_sql"
    assert verdict.scoped_patch["patch_type"] == "add_example_sql"


def test_narrow_replacement_rejects_unfixable_when_llm_says_so():
    from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
        run_narrow_replacement,
    )

    drop = _make_drop_record()
    mock_llm = MagicMock(return_value={
        "decision": "reject_unfixable",
        "rationale": "Any filter on payments.currency necessarily affects all currency-sensitive consumers.",
    })
    verdict = run_narrow_replacement(drop=drop, llm_call=mock_llm)
    assert verdict.decision == "reject_unfixable"
    assert verdict.scoped_patch is None
