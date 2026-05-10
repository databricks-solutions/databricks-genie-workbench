"""C15 Phase 4.2: GatesInput / GateOutcome JsonRoundTrip contract.

The plan (Task 4.2) refers to GatesOutput but the class-declarations test
pins the natural-noun name GateOutcome. We test GateOutcome.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.gates import (
    GatesInput,
    GateOutcome,
    GateDrop,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_mixes_jsonroundtrip() -> None:
    assert issubclass(GatesInput, JsonRoundTrip)


def test_output_mixes_jsonroundtrip() -> None:
    assert issubclass(GateOutcome, JsonRoundTrip)


def test_input_round_trip() -> None:
    inp = GatesInput(
        proposals_by_ag={"AG1": ({"id": "p001", "patch_text": "SELECT 1"},)},
        ags=({"ag_id": "AG1"},),
    )
    payload = inp.to_json()
    restored = GatesInput.from_json(payload)
    assert "AG1" in restored.proposals_by_ag


def test_output_round_trip() -> None:
    out = GateOutcome(
        survived_by_ag={"AG1": ({"id": "p001"},)},
        dropped=(
            GateDrop(
                proposal_id="p002",
                gate="blast_radius",
                reason="too_many_affected_tables",
            ),
        ),
        new_dead_on_arrival_signatures=("sig-abc",),
    )
    payload = out.to_json()
    restored = GateOutcome.from_json(payload)
    assert "AG1" in restored.survived_by_ag
    assert len(restored.dropped) == 1
    assert restored.dropped[0].gate == "blast_radius"
    assert restored.new_dead_on_arrival_signatures == ("sig-abc",)
