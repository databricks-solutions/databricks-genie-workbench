"""RCO-4 Task 5 — pure helper extraction of the inline blast-radius
orchestration at harness.py:20860-20940.

The helper iterates candidate patches, calls
``patch_blast_radius_is_safe`` and ``instruction_patch_scope_is_safe``
per candidate, and accumulates kept/dropped lists with the same
field shape the harness inline code produces (so journey emitters
and decision-record emitters are unchanged).
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.gate_types import (
    BlastRadiusProductionInput,
    BlastRadiusProductionOutcome,
)
from genie_space_optimizer.optimization.stages.gates import (
    run_blast_radius_production_gate,
)


def test_safe_patch_kept() -> None:
    """A patch with no outside-target dependents survives."""
    inp = BlastRadiusProductionInput(
        ag_id="AG_alpha",
        ag_target_qids=("q1", "q2"),
        live_hard_qids=("q1", "q2"),
        max_outside_target=0,
        patches=(
            {
                "proposal_id": "L6:P001#1",
                "patch_type": "add_sql_snippet_expression",
                "target": "orders",
                "target_qids": ("q1", "q2"),
                # ``passing_dependents`` is what patch_blast_radius_is_safe reads.
                # Only q1/q2 are in the target set, so no outside-target collateral.
                "passing_dependents": ["q1", "q2"],
            },
        ),
    )
    out = run_blast_radius_production_gate(inp)
    assert isinstance(out, BlastRadiusProductionOutcome)
    assert len(out.kept) == 1
    assert len(out.dropped) == 0
    assert out.kept[0]["proposal_id"] == "L6:P001#1"


def test_unsafe_patch_dropped_with_required_fields() -> None:
    """A patch with passing dependents outside the AG target set is
    dropped and the drop record carries the production-shape fields
    that journey emitters and DecisionRecord builders depend on."""
    inp = BlastRadiusProductionInput(
        ag_id="AG_alpha",
        ag_target_qids=("q1",),
        live_hard_qids=("q1",),
        max_outside_target=0,
        patches=(
            {
                "proposal_id": "L6:P002#1",
                "patch_type": "add_sql_snippet_expression",
                "target": "orders",
                "target_qids": ("q1",),
                # q3 is outside the target set (q1), triggering the drop.
                # q3 is NOT in live_hard_qids so no shared-cause downgrade.
                "passing_dependents": ["q1", "q3"],
            },
        ),
    )
    out = run_blast_radius_production_gate(inp)
    assert len(out.kept) == 0
    assert len(out.dropped) == 1
    d = out.dropped[0]
    assert d["proposal_id"] == "L6:P002#1"
    assert d["patch_type"] == "add_sql_snippet_expression"
    assert d["reason"]
    assert d["target"] == "orders"
    # The orchestration must echo the source patch under
    # "original_patch" so the narrow-replacement loop can read
    # where_predicate / qid_predicate_column from the full dict.
    assert d["original_patch"]["proposal_id"] == "L6:P002#1"


def test_empty_candidates_returns_empty_outcome() -> None:
    inp = BlastRadiusProductionInput(
        ag_id="AG_alpha",
        ag_target_qids=("q1",),
        live_hard_qids=("q1",),
        max_outside_target=0,
        patches=(),
    )
    out = run_blast_radius_production_gate(inp)
    assert out.kept == ()
    assert out.dropped == ()


def test_ordering_preserved_in_kept_and_dropped() -> None:
    """The helper must preserve the input order of candidates within
    kept/dropped so any caller relying on iteration order (e.g.,
    journey emission ordering) is unaffected."""
    inp = BlastRadiusProductionInput(
        ag_id="AG_alpha",
        ag_target_qids=("q1",),
        live_hard_qids=("q1",),
        max_outside_target=0,
        patches=(
            {
                "proposal_id": "L6:P001#1",
                "patch_type": "add_sql_snippet_expression",
                "target": "orders",
                "target_qids": ("q1",),
                "passing_dependents": ["q1"],  # no outside-target → kept
            },
            {
                "proposal_id": "L6:P002#1",
                "patch_type": "add_sql_snippet_expression",
                "target": "orders",
                "target_qids": ("q1",),
                "passing_dependents": ["q1", "q9"],  # q9 outside target → dropped
            },
            {
                "proposal_id": "L6:P003#1",
                "patch_type": "add_sql_snippet_expression",
                "target": "orders",
                "target_qids": ("q1",),
                "passing_dependents": ["q1"],  # no outside-target → kept
            },
        ),
    )
    out = run_blast_radius_production_gate(inp)
    assert [k["proposal_id"] for k in out.kept] == ["L6:P001#1", "L6:P003#1"]
    assert [d["proposal_id"] for d in out.dropped] == ["L6:P002#1"]
