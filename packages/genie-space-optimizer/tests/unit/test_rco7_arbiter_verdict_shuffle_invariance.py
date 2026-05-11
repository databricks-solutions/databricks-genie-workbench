"""RCO-7 Site 4 — arbiter judge verdict ingestion is reorder-invariant.

The arbiter LLM returns a JSON object with list fields (``blame_set``,
``expected_objects``, ``actual_objects``, ``recommended_levers``).
Downstream consumers — ``build_asi_metadata`` and RCA card construction
— record these lists in order, so any reordering by the LLM bleeds into
deterministic stage outputs. ``canonicalize_arbiter_verdict`` neutralizes
this at the ingestion boundary.
"""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.llm_boundary_sort import (
    canonicalize_arbiter_verdict,
)


def _verdict(blame_order: list[str], lever_order: list[int]) -> dict:
    return {
        "verdict": "ground_truth_correct",
        "failure_type": "wrong_aggregation",
        "blame_set": list(blame_order),
        "rca_kind": "measure_swap",
        "expected_objects": ["metric_revenue", "metric_orders"],
        "actual_objects": ["col_amount", "col_qty"],
        "patch_family": "contrastive_measure_disambiguation",
        "recommended_levers": list(lever_order),
        "rationale": "The genie SQL summed amount instead of revenue.",
    }


def test_arbiter_verdict_canonicalization_is_reorder_invariant() -> None:
    forward = _verdict(
        blame_order=["table_a", "table_b", "metric_z"],
        lever_order=[5, 1, 3],
    )
    reversed_inp = _verdict(
        blame_order=["metric_z", "table_b", "table_a"],
        lever_order=[3, 1, 5],
    )

    out_forward = canonicalize_arbiter_verdict(forward)
    out_reversed = canonicalize_arbiter_verdict(reversed_inp)

    assert json.dumps(out_forward, sort_keys=True) == json.dumps(
        out_reversed, sort_keys=True,
    )
    # Concretely:
    assert out_forward["blame_set"] == ["metric_z", "table_a", "table_b"]
    assert out_forward["expected_objects"] == [
        "metric_orders", "metric_revenue",
    ]
    assert out_forward["actual_objects"] == ["col_amount", "col_qty"]
    assert out_forward["recommended_levers"] == [1, 3, 5]
    # Scalar fields untouched.
    assert out_forward["verdict"] == "ground_truth_correct"
    assert out_forward["failure_type"] == "wrong_aggregation"
    assert out_forward["rca_kind"] == "measure_swap"
    assert out_forward["patch_family"] == "contrastive_measure_disambiguation"


def test_arbiter_verdict_skipped_and_neither_pass_through() -> None:
    """Edge-case verdicts ('skipped', 'neither_correct') with empty
    or absent list fields must canonicalize to an equivalent dict."""
    skipped = {"verdict": "skipped", "rationale": "no_mismatch"}
    neither = {
        "verdict": "neither_correct",
        "blame_set": [],
        "recommended_levers": [],
    }

    assert canonicalize_arbiter_verdict(skipped) == skipped
    out_neither = canonicalize_arbiter_verdict(neither)
    assert out_neither["verdict"] == "neither_correct"
    assert out_neither["blame_set"] == []
    assert out_neither["recommended_levers"] == []
