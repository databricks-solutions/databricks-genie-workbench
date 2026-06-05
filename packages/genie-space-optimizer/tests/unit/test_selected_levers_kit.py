"""Phase 2 P2.1 — ``selected_levers`` is the primary lever-kit channel.

Covers the four kit-cardinality regimes that downstream readers care
about:

  * **list + string**           — list authoritative
  * **list only**               — list authoritative; legacy
    ``selected_lever`` derived as ``selected_levers[0]`` for
    back-compat readers
  * **string only**             — single-element kit derived from the
    legacy single-string field
  * **neither**                 — no lever declared; validator owns
    the rejection

And round-trip preservation through ``to_json`` / ``from_json``.
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
    defaults = dict(
        intent_id="intent_H001_AG3_001",
        intent_name="kit_test",
        intent_description="kit cardinality test",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="r",
        confidence="medium",
        patch_body={
            "example_question": "q",
            "example_sql": "SELECT 1",
            "usage_guidance": "g",
            "parameters": [],
        },
        blame_set=(),
    )
    defaults.update(overrides)
    return RepairProposal(**defaults)


def test_effective_kit_prefers_selected_levers_when_both_set() -> None:
    p = _proposal(
        selected_lever="lever-1",
        selected_levers=("lever-1", "lever-6"),
    )
    assert p.effective_selected_levers() == ("lever-1", "lever-6")


def test_effective_kit_falls_back_to_single_string_when_list_empty() -> None:
    p = _proposal(selected_lever="lever-5", selected_levers=())
    assert p.effective_selected_levers() == ("lever-5",)


def test_effective_kit_empty_when_neither_field_set() -> None:
    p = _proposal()
    assert p.effective_selected_levers() == ()


def test_effective_kit_filters_blank_entries() -> None:
    p = _proposal(selected_levers=("lever-1", "", "lever-6"))
    assert p.effective_selected_levers() == ("lever-1", "lever-6")


def test_round_trip_preserves_selected_levers_list() -> None:
    p = _proposal(
        selected_lever="lever-1",
        selected_levers=("lever-1", "lever-6"),
    )
    payload = p.to_json()
    assert payload["selected_levers"] == ["lever-1", "lever-6"]
    assert payload["selected_lever"] == "lever-1"
    revived = RepairProposal.from_json(payload)
    assert revived.selected_levers == ("lever-1", "lever-6")
    assert revived.selected_lever == "lever-1"
    assert revived.effective_selected_levers() == ("lever-1", "lever-6")


def test_round_trip_back_compat_when_only_legacy_field_persisted() -> None:
    payload = {
        "intent_id": "i",
        "intent_name": "n",
        "intent_description": "d",
        "repair_shape": "top_n_by_metric",
        "patch_type": "add_example_sql",
        "rationale": "r",
        "confidence": "medium",
        "patch_body": {},
        "blame_set": [],
        "target_objects": [],
        "required_constructs": [],
        "target_qids": [],
        # Legacy persisted shape: only ``selected_lever``.
        "selected_lever": "lever-2",
        "expected_behavioral_change": "",
        "fallback_lever": "",
        "bundle_id": "",
    }
    revived = RepairProposal.from_json(payload)
    assert revived.selected_levers == ()
    assert revived.selected_lever == "lever-2"
    assert revived.effective_selected_levers() == ("lever-2",)


class _PydanticLike:
    """Stand-in for the Pydantic ``LlmRepairProposalOutput`` shape that
    ``RepairProposal.from_llm_output`` reads via ``getattr``."""

    def __init__(self, **kw) -> None:
        for k, v in kw.items():
            setattr(self, k, v)


def test_from_llm_output_reads_selected_levers_list() -> None:
    raw = _PydanticLike(
        intent_name="n",
        intent_description="d",
        repair_shape="top_n_by_metric",
        patch_type="add_example_sql",
        rationale="r",
        confidence="medium",
        patch_body={},
        blame_set=[],
        target_objects=[],
        required_constructs=[],
        selected_levers=["lever-1", "lever-6"],
    )
    p = RepairProposal.from_llm_output(raw, intent_id="i")
    assert p.selected_levers == ("lever-1", "lever-6")
    # Forward-compat surface: legacy field gets the first kit member so
    # downstream readers that still consult ``selected_lever`` see it.
    assert p.selected_lever == "lever-1"


def test_from_llm_output_back_compat_when_only_single_string() -> None:
    raw = _PydanticLike(
        intent_name="n",
        intent_description="d",
        repair_shape="top_n_by_metric",
        patch_type="add_example_sql",
        rationale="r",
        confidence="medium",
        patch_body={},
        blame_set=[],
        target_objects=[],
        required_constructs=[],
        selected_lever="lever-3",
    )
    p = RepairProposal.from_llm_output(raw, intent_id="i")
    assert p.selected_lever == "lever-3"
    # Back-compat: list derived from the legacy single-string field.
    assert p.selected_levers == ("lever-3",)
