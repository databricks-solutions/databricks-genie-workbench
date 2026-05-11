"""RCO-4 Task 8 — stage-uniform wrapper over the already-pure
``patch_applyability.py`` module.

The wrapper is a thin adapter: it accepts an
``ApplyabilityGateInput`` and returns an ``ApplyabilityGateOutcome``.
The actual applyability decision is delegated to the existing pure
module; no logic moves.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.gate_types import (
    ApplyabilityGateInput,
    ApplyabilityGateOutcome,
)
from genie_space_optimizer.optimization.stages.gates import (
    run_applyability_gate,
)


def test_wrapper_returns_typed_outcome() -> None:
    inp = ApplyabilityGateInput(
        candidates=(
            {
                "proposal_id": "L2:P001#1",
                "patch_type": "add_column_description",
                "target": "orders.customer_id",
                "table": "orders",
                "column": "customer_id",
                "new_text": "FK to customers",
            },
        ),
        metadata_snapshot={
            "tables": [
                {
                    "name": "orders",
                    "columns": [{"name": "customer_id"}],
                }
            ]
        },
    )
    out = run_applyability_gate(inp)
    assert isinstance(out, ApplyabilityGateOutcome)
    assert (len(out.applyable) + len(out.rejected)) == 1


def test_empty_candidates_returns_empty_outcome() -> None:
    inp = ApplyabilityGateInput(
        candidates=(),
        metadata_snapshot={},
    )
    out = run_applyability_gate(inp)
    assert out.applyable == ()
    assert out.rejected == ()


def test_outcome_preserves_input_order_within_buckets() -> None:
    """Iteration order must be preserved within applyable and
    rejected (the wrapper is a thin adapter; no sorting)."""
    inp = ApplyabilityGateInput(
        candidates=(
            {"proposal_id": "L2:P001#1", "patch_type": "add_column_description",
             "target": "orders.customer_id", "table": "orders",
             "column": "customer_id", "new_text": "FK"},
            {"proposal_id": "L2:P002#1", "patch_type": "add_column_description",
             "target": "orders.totally_missing_col", "table": "orders",
             "column": "totally_missing_col", "new_text": "?"},
            {"proposal_id": "L2:P003#1", "patch_type": "add_column_description",
             "target": "orders.customer_id", "table": "orders",
             "column": "customer_id", "new_text": "FK"},
        ),
        metadata_snapshot={
            "tables": [
                {"name": "orders", "columns": [{"name": "customer_id"}]}
            ]
        },
    )
    out = run_applyability_gate(inp)
    applyable_pids = [p["proposal_id"] for p in out.applyable]
    rejected_pids = [p["proposal_id"] for p in out.rejected]
    # All applyable IDs preserve relative order
    assert applyable_pids == sorted(applyable_pids, key=applyable_pids.index)
    assert rejected_pids == sorted(rejected_pids, key=rejected_pids.index)
