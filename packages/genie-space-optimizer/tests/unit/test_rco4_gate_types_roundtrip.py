"""RCO-4 Task 3 — typed input/output dataclasses for the three
extracted Stage-6 helpers.

Each pair (Input, Outcome) must be JSON-roundtrippable so fixture
pairs in later tasks can serialize cleanly.
"""

from __future__ import annotations

import json


def test_blast_radius_production_input_roundtrip() -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        BlastRadiusProductionInput,
    )

    inp = BlastRadiusProductionInput(
        ag_id="AG_alpha",
        ag_target_qids=("q1", "q2"),
        live_hard_qids=("q1", "q2", "q9"),
        max_outside_target=0,
        patches=(
            {
                "proposal_id": "L6:P001#1",
                "patch_type": "add_sql_snippet_expression",
                "target": "orders",
            },
        ),
    )
    payload = inp.to_json()
    restored = BlastRadiusProductionInput.from_json(json.loads(json.dumps(payload)))
    assert restored == inp


def test_blast_radius_production_outcome_roundtrip() -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        BlastRadiusProductionOutcome,
    )

    out = BlastRadiusProductionOutcome(
        kept=(
            {"proposal_id": "L6:P001#1", "patch_type": "add_sql_snippet_expression"},
        ),
        dropped=(
            {
                "proposal_id": "L6:P002#1",
                "patch_type": "add_sql_snippet_expression",
                "reason": "outside_target_dependents_passing",
                "passing_dependents_outside_target": ["q3"],
                "target": "orders",
                "original_patch": {"proposal_id": "L6:P002#1"},
            },
        ),
    )
    restored = BlastRadiusProductionOutcome.from_json(
        json.loads(json.dumps(out.to_json()))
    )
    assert restored == out


def test_narrow_replacement_input_roundtrip() -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        NarrowReplacementInput,
    )

    inp = NarrowReplacementInput(
        ag_id="AG_alpha",
        ag_rca_id="rca-1",
        ag_target_qids=("q1", "q2"),
        ag_root_cause="missing measure",
        blast_dropped=(
            {
                "proposal_id": "L6:P002#1",
                "patch_type": "add_sql_snippet_expression",
                "reason": "outside_target_dependents_passing",
                "original_patch": {"proposal_id": "L6:P002#1", "rca_id": "rca-1"},
            },
        ),
        qid_to_question_text={"q1": "?", "q2": "?"},
        qid_to_reference_sql={"q1": "select 1", "q2": "select 2"},
    )
    restored = NarrowReplacementInput.from_json(
        json.loads(json.dumps(inp.to_json()))
    )
    assert restored == inp


def test_narrow_replacement_outcome_roundtrip() -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        NarrowReplacementOutcome,
    )

    out = NarrowReplacementOutcome(
        narrow_survivors=({"proposal_id": "L6:P002#1_narrow"},),
        structural_causal_dropped=(),
        halt_no_structural_alternative=False,
    )
    restored = NarrowReplacementOutcome.from_json(
        json.loads(json.dumps(out.to_json()))
    )
    assert restored == out


def test_applyability_gate_input_roundtrip() -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        ApplyabilityGateInput,
    )

    inp = ApplyabilityGateInput(
        candidates=({"proposal_id": "L2:P001#1", "patch_type": "add_column_description"},),
        metadata_snapshot={"tables": []},
    )
    restored = ApplyabilityGateInput.from_json(
        json.loads(json.dumps(inp.to_json()))
    )
    assert restored == inp


def test_applyability_gate_outcome_roundtrip() -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        ApplyabilityGateOutcome,
    )

    out = ApplyabilityGateOutcome(
        applyable=({"proposal_id": "L2:P001#1"},),
        rejected=(
            {
                "proposal_id": "L2:P002#1",
                "expanded_patch_id": "L2:P002#1",
                "patch_type": "add_column_description",
                "target": "orders.customer_id",
                "table": "orders",
                "column": "customer_id",
                "applyable": False,
                "reason": "target_column_missing",
                "error_excerpt": "",
            },
        ),
    )
    restored = ApplyabilityGateOutcome.from_json(
        json.loads(json.dumps(out.to_json()))
    )
    assert restored == out
