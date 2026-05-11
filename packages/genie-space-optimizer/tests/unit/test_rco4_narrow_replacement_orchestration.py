"""RCO-4 Task 7 — pure narrow-replacement orchestration extracted from
harness.py:~20947-21041.

The helper takes the inputs and the already-computed
``_run_narrow_l6_replacement_loop`` survivors, then returns:
  * the survivors (passthrough)
  * the structural-causal drops that have NO narrow survivor
    replacement
  * a halt-no-structural-alternative flag the harness reads to wipe
    ``_blast_kept`` and emit the halt records
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.gate_types import (
    NarrowReplacementInput,
    NarrowReplacementOutcome,
)
from genie_space_optimizer.optimization.stages.gates import (
    resolve_narrow_replacement,
)


def test_no_blast_drops_no_halt() -> None:
    """With zero blast-radius drops, the helper short-circuits: no
    survivors, no structural drops, no halt."""
    inp = NarrowReplacementInput(
        ag_id="AG_alpha",
        ag_rca_id="rca-1",
        ag_target_qids=("q1",),
        ag_root_cause="missing measure",
        blast_dropped=(),
        qid_to_question_text={},
        qid_to_reference_sql={},
    )
    out = resolve_narrow_replacement(inp, narrow_survivors=())
    assert isinstance(out, NarrowReplacementOutcome)
    assert out.narrow_survivors == ()
    assert out.structural_causal_dropped == ()
    assert out.halt_no_structural_alternative is False


def test_branch_a_survivor_replaces_no_halt() -> None:
    """A structural-causal drop is replaced by a narrow survivor with
    ``derived_from`` pointing at the dropped proposal — no halt."""
    inp = NarrowReplacementInput(
        ag_id="AG_alpha",
        ag_rca_id="rca-1",
        ag_target_qids=("q1",),
        ag_root_cause="missing measure",
        blast_dropped=(
            {
                "proposal_id": "L6:P002#1",
                "reason": "outside_target_dependents_passing",
                "original_patch": {
                    "proposal_id": "L6:P002#1",
                    "patch_type": "add_sql_snippet_expression",
                    "rca_id": "rca-1",
                    "target": "orders",
                },
            },
        ),
        qid_to_question_text={"q1": "?"},
        qid_to_reference_sql={"q1": "select 1"},
    )
    survivors = (
        {"proposal_id": "L6:P002#1_narrow", "derived_from": "L6:P002#1"},
    )
    out = resolve_narrow_replacement(inp, narrow_survivors=survivors)
    assert out.narrow_survivors == survivors
    assert out.structural_causal_dropped == ()
    assert out.halt_no_structural_alternative is False


def test_structural_drop_with_no_survivor_triggers_halt() -> None:
    """A structural-causal drop with no narrow-survivor replacement
    triggers the halt — the harness will wipe ``_blast_kept`` and
    emit the no_structural_alternative records."""
    inp = NarrowReplacementInput(
        ag_id="AG_alpha",
        ag_rca_id="rca-1",
        ag_target_qids=("q1",),
        ag_root_cause="missing measure",
        blast_dropped=(
            {
                "proposal_id": "L6:P002#1",
                "reason": "outside_target_dependents_passing",
                "original_patch": {
                    "proposal_id": "L6:P002#1",
                    "patch_type": "add_sql_snippet_expression",
                    "rca_id": "rca-1",
                    "target": "orders",
                },
            },
        ),
        qid_to_question_text={"q1": "?"},
        qid_to_reference_sql={"q1": "select 1"},
    )
    out = resolve_narrow_replacement(inp, narrow_survivors=())
    assert out.narrow_survivors == ()
    assert len(out.structural_causal_dropped) == 1
    assert out.halt_no_structural_alternative is True


def test_diagnostic_ag_no_rca_id_no_halt() -> None:
    """A diagnostic AG with no inherited ``ag_rca_id`` is not subject
    to the structural-causal halt — even if it has structural drops."""
    inp = NarrowReplacementInput(
        ag_id="AG_diagnostic",
        ag_rca_id="",
        ag_target_qids=("q1",),
        ag_root_cause="",
        blast_dropped=(
            {
                "proposal_id": "L6:P002#1",
                "reason": "outside_target_dependents_passing",
                "original_patch": {
                    "proposal_id": "L6:P002#1",
                    "patch_type": "add_sql_snippet_expression",
                    "rca_id": "",
                    "target": "orders",
                },
            },
        ),
        qid_to_question_text={},
        qid_to_reference_sql={},
    )
    out = resolve_narrow_replacement(inp, narrow_survivors=())
    assert out.halt_no_structural_alternative is False
    assert out.structural_causal_dropped == ()
