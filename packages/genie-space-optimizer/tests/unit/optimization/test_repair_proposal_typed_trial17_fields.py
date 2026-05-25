"""Trial 17 Step 1 — pin the Lever Selection Contract fields on
``RepairProposal``.

Adds round-trip coverage for the four new fields:
``selected_lever`` / ``expected_behavioral_change`` / ``fallback_lever``
/ ``bundle_id``. Defaults are backward-compatible (empty string), so
pre-Trial-17 serialized rows still parse via ``from_json``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _proposal(**overrides) -> RepairProposal:
    base = dict(
        intent_id="intent_xyz",
        intent_name="top_n_revenue",
        intent_description="adds top-N ORDER BY revenue example sql",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_INSTRUCTION,
        rationale="empirical",
        confidence="high",
        patch_body={"instruction_text": "Use ORDER BY revenue DESC LIMIT 10"},
        blame_set=("cat.sch.t.col",),
    )
    base.update(overrides)
    return RepairProposal(**base)


def test_defaults_are_backward_compatible():
    """Newly-constructed proposals default the Trial 17 fields to empty
    string so pre-Trial-17 call sites compile."""
    p = _proposal()
    assert p.selected_lever == ""
    assert p.expected_behavioral_change == ""
    assert p.fallback_lever == ""
    assert p.bundle_id == ""


def test_round_trip_carries_trial17_fields():
    p = _proposal(
        selected_lever="lever-5",
        expected_behavioral_change=(
            "queries about top N customers will use ORDER BY revenue "
            "DESC LIMIT N instead of MAX(revenue)"
        ),
        fallback_lever="lever-6",
        bundle_id="bundle_top_n_001",
    )
    payload = p.to_json()
    assert payload["selected_lever"] == "lever-5"
    assert payload["expected_behavioral_change"].startswith("queries about top N")
    assert payload["fallback_lever"] == "lever-6"
    assert payload["bundle_id"] == "bundle_top_n_001"

    restored = RepairProposal.from_json(payload)
    assert restored.selected_lever == "lever-5"
    assert restored.expected_behavioral_change == p.expected_behavioral_change
    assert restored.fallback_lever == "lever-6"
    assert restored.bundle_id == "bundle_top_n_001"


def test_from_json_tolerates_missing_trial17_keys():
    """Old Delta rows (pre-Trial-17) do not have these keys; loader
    falls back to empty strings."""
    p = _proposal(selected_lever="lever-5", bundle_id="b1")
    payload = p.to_json()
    for k in (
        "selected_lever",
        "expected_behavioral_change",
        "fallback_lever",
        "bundle_id",
    ):
        payload.pop(k, None)
    restored = RepairProposal.from_json(payload)
    assert restored.selected_lever == ""
    assert restored.expected_behavioral_change == ""
    assert restored.fallback_lever == ""
    assert restored.bundle_id == ""


def test_from_llm_output_reads_trial17_fields_when_present():
    """When the LLM emits Trial 17 fields, ``from_llm_output`` picks
    them up; missing attributes default to empty strings."""

    class _StubLlmOutput:
        intent_name = "top_n_revenue"
        intent_description = "adds top-N ORDER BY revenue example sql"
        repair_shape = RepairShape.OTHER
        patch_type = PatchType.ADD_INSTRUCTION
        rationale = "empirical"
        confidence = "high"
        patch_body = {"instruction_text": "Use ORDER BY revenue DESC LIMIT 10"}
        blame_set = ["cat.sch.t.col"]
        target_objects: list = []
        required_constructs: list = []
        selected_lever = "lever-5"
        expected_behavioral_change = "ORDER BY DESC LIMIT"
        fallback_lever = "lever-6"
        bundle_id = "bundle_42"

    proposal = RepairProposal.from_llm_output(
        _StubLlmOutput(), intent_id="intent_xyz"
    )
    assert proposal.selected_lever == "lever-5"
    assert proposal.expected_behavioral_change == "ORDER BY DESC LIMIT"
    assert proposal.fallback_lever == "lever-6"
    assert proposal.bundle_id == "bundle_42"


def test_from_llm_output_handles_pre_trial17_pydantic_input():
    """If the LLM output lacks the Trial 17 attributes (older prompt
    template), the loader still works and defaults to empty strings."""

    class _OldLlmOutput:
        intent_name = "x"
        intent_description = "y"
        repair_shape = RepairShape.OTHER
        patch_type = PatchType.ADD_INSTRUCTION
        rationale = "r"
        confidence = "low"
        patch_body = {"instruction_text": "instr"}
        blame_set: list = []
        target_objects: list = []
        required_constructs: list = []

    proposal = RepairProposal.from_llm_output(
        _OldLlmOutput(), intent_id="i"
    )
    assert proposal.selected_lever == ""
    assert proposal.bundle_id == ""
