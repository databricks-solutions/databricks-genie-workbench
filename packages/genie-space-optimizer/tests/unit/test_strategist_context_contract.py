"""C15-P2.4: StrategistContextInput / StrategistContextOutput typed contract tests.

This is the user's stated north-star goal for Cycle 15:
'the journey from MLflow Judges Verdicts to Strategist Proposal Generation
should be deterministic.'

StrategistContextInput captures every upstream boundary field the strategist
sees. StrategistContextOutput enforces the Stage 2→4 arrow: only grounded
RCA cards reach the strategist LLM.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.strategist_context import (
    StrategistContextInput,
    StrategistContextOutput,
    build_strategist_context,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_and_output_round_trip() -> None:
    """StrategistContextInput serializes and deserializes cleanly."""
    inp = StrategistContextInput(
        hard_failure_qids=("gs_024",),
        soft_failure_qids=("gs_018",),
        clusters_by_qid={"gs_024": "H004"},
        rca_cards=(
            {
                "rca_id": "rca-h004",
                "cluster_id": "H004",
                "grounding": "grounded",
                "evidence_qids": ["gs_024"],
            },
        ),
        forbidden_ag_ids=(),
        reflection_buffer=(),
        baseline_accuracy=0.833,
    )
    payload = inp.to_json()
    restored = StrategistContextInput.from_json(payload)
    assert restored.hard_failure_qids == ("gs_024",)
    assert restored.baseline_accuracy == pytest.approx(0.833)
    assert len(restored.rca_cards) == 1
    assert restored.rca_cards[0]["grounding"] == "grounded"


def test_build_strategist_context_strips_ungrounded_rcas() -> None:
    """Stage 2 → Stage 4 arrow enforced at the contract level: an
    ungrounded RCA may not appear in the strategist context's
    rca_cards_grounded_only. (Cycle 15 absorbs the original 'RCA
    enforcement' wiring change as a typed-output consumer.)"""
    inp = StrategistContextInput(
        hard_failure_qids=("gs_024",),
        clusters_by_qid={"gs_024": "H004"},
        rca_cards=(
            {
                "rca_id": "g1",
                "cluster_id": "H004",
                "grounding": "grounded",
                "evidence_qids": ["gs_024"],
            },
            {
                "rca_id": "u1",
                "cluster_id": "H005",
                "grounding": "ungrounded",
                "evidence_qids": [],
            },
        ),
    )
    out = build_strategist_context(ctx=None, inp=inp)
    rca_ids = {c["rca_id"] for c in out.rca_cards_grounded_only}
    assert rca_ids == {"g1"}
    assert "u1" not in rca_ids
    assert out.rca_cards_ungrounded_count == 1


def test_output_round_trip_preserves_tuple_fields() -> None:
    """StrategistContextOutput serializes and deserializes correctly."""
    out = StrategistContextOutput(
        iteration=2,
        baseline_accuracy=0.833,
        hard_failure_qids=("gs_024",),
        soft_failure_qids=("gs_018",),
        passing_qids=("gs_001", "gs_007"),
        clusters_by_qid={"gs_024": "H004"},
        rca_cards_grounded_only=(
            {"rca_id": "g1", "cluster_id": "H004"},
        ),
        rca_cards_ungrounded_count=1,
        forbidden_ag_ids=("ag-001",),
        reflection_buffer=(),
    )
    payload = out.to_json()
    restored = StrategistContextOutput.from_json(payload)
    assert restored.iteration == 2
    assert restored.hard_failure_qids == ("gs_024",)
    assert restored.forbidden_ag_ids == ("ag-001",)
    assert len(restored.rca_cards_grounded_only) == 1
    assert restored.rca_cards_grounded_only[0]["rca_id"] == "g1"


def test_input_and_output_mix_jsonroundtrip() -> None:
    assert issubclass(StrategistContextInput, JsonRoundTrip)
    assert issubclass(StrategistContextOutput, JsonRoundTrip)


def test_frozen_input_cannot_be_mutated() -> None:
    from dataclasses import FrozenInstanceError
    inp = StrategistContextInput(hard_failure_qids=("gs_001",))
    with pytest.raises(FrozenInstanceError):
        inp.hard_failure_qids = ()  # type: ignore[misc]


def test_zero_rca_cards_produces_empty_output() -> None:
    """With no RCA cards, both grounded and ungrounded counts are zero."""
    inp = StrategistContextInput(
        hard_failure_qids=("gs_001",),
        rca_cards=(),
    )
    out = build_strategist_context(ctx=None, inp=inp)
    assert out.rca_cards_grounded_only == ()
    assert out.rca_cards_ungrounded_count == 0
