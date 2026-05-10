import pytest
from dataclasses import FrozenInstanceError

from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    AgOutcome,
    AgOutcomeRecord,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_acceptance_input_is_frozen() -> None:
    inp = AcceptanceInput(applied_entries_by_ag={}, ags=())
    with pytest.raises(FrozenInstanceError):
        inp.baseline_accuracy = 0.5  # type: ignore[misc]


def test_acceptance_input_round_trips() -> None:
    inp = AcceptanceInput(
        applied_entries_by_ag={"AG1": ({"patch": {"content_fingerprint": "fp1"}},)},
        ags=({"id": "AG1", "target_qids": ["gs_001"]},),
        baseline_accuracy=0.833,
        candidate_accuracy=0.958,
    )
    payload = inp.to_json()
    restored = AcceptanceInput.from_json(payload)
    assert restored.baseline_accuracy == 0.833
    assert restored.candidate_accuracy == 0.958
    assert restored.ags[0]["id"] == "AG1"


def test_ag_outcome_round_trips() -> None:
    out = AgOutcome(
        outcomes_by_ag={
            "AG1": AgOutcomeRecord(
                ag_id="AG1",
                outcome="accepted",
                reason_code="accepted_with_attribution_drift",
                target_qids=("gs_024",),
                affected_qids=("gs_024",),
                content_fingerprints=("fp1",),
            )
        },
        qid_resolutions={"gs_001": "fail_to_pass"},
        rolled_back_content_fingerprints=frozenset(),
    )
    payload = out.to_json()
    restored = AgOutcome.from_json(payload)
    assert restored.outcomes_by_ag["AG1"].outcome == "accepted"
    assert restored.qid_resolutions == {"gs_001": "fail_to_pass"}


def test_acceptance_input_mixes_jsonroundtrip() -> None:
    assert issubclass(AcceptanceInput, JsonRoundTrip)
    assert issubclass(AgOutcome, JsonRoundTrip)


def test_to_pretty_renders_key_acceptance_fields() -> None:
    inp = AcceptanceInput(
        applied_entries_by_ag={},
        ags=(),
        baseline_accuracy=0.833,
        candidate_accuracy=0.958,
    )
    text = inp.to_pretty()
    assert "baseline_accuracy" in text
    assert "candidate_accuracy" in text
