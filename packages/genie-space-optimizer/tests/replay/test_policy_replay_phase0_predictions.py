"""Phase 0 — pre-registered prediction comparison test.

Loads each ReplayPayload fixture, classifies under the pilot
RegressionDebtPolicy, compares to the pre-registered prediction in
predictions.json, and enforces the spec pass criterion:

    classification matches prediction OR mismatch identifies a
    missing tier.

A structured mismatch (one whose first_failed_gate names a known
policy gate) is treated as PASS but emits a structured diff to the
test report so the engineer can paste it into the Phase-0 results
doc. An unstructured mismatch (first_failed_gate is None for a
rejected candidate, or the prediction shape is unparseable) is a
HARD FAIL and surfaces a missing case in the design.
"""
from __future__ import annotations

import json
import pathlib
import sys

import pytest

from genie_space_optimizer.optimization.acceptance_policy import (
    regression_debt_policy_pilot_default,
)
from genie_space_optimizer.tools.policy_replay import (
    classify_payload,
    load_payload,
)

FIXTURES_DIR = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "policy_replay"
)
PREDICTIONS_PATH = FIXTURES_DIR / "predictions.json"


def _load_predictions() -> dict[str, dict]:
    raw = json.loads(PREDICTIONS_PATH.read_text())
    return {
        str(p["fixture_id"]): p for p in raw["predictions"]
    }


PREDICTION_IDS = sorted(_load_predictions().keys())


def _format_mismatch_block(
    fixture_id: str,
    predicted: dict,
    classification,
) -> str:
    return (
        f"\n--- replay_classifier_decision MISMATCH ---\n"
        f"fixture_id: {fixture_id}\n"
        f"policy: {classification.policy_name}\n"
        f"predicted_accepted: {predicted.get('predicted_accepted')!r}\n"
        f"predicted_reason_code: {predicted.get('predicted_reason_code')!r}\n"
        f"observed_accepted: {classification.accepted!r}\n"
        f"observed_reason_code: {classification.reason_code!r}\n"
        f"first_failed_gate: {classification.first_failed_gate!r}\n"
        f"policy_diagnostics: {classification.policy_diagnostics!r}\n"
        f"-------------------------------------------\n"
    )


@pytest.mark.parametrize("fixture_id", PREDICTION_IDS)
def test_pre_registered_prediction_or_structured_mismatch(
    fixture_id: str, capsys
) -> None:
    fixture_path = FIXTURES_DIR / f"{fixture_id}.json"
    if not fixture_path.exists():
        pytest.skip(
            f"fixture {fixture_id} missing at {fixture_path}; create at Task 2-4"
        )

    payload = load_payload(fixture_path)
    classification = classify_payload(
        payload=payload,
        policy=regression_debt_policy_pilot_default(),
        policy_name="regression_debt_policy_pilot_default",
    )

    predictions = _load_predictions()
    predicted = predictions[fixture_id]
    matched = (
        bool(predicted["predicted_accepted"]) == bool(classification.accepted)
        and predicted["predicted_reason_code"] == classification.reason_code
        if predicted["predicted_accepted"] is not None
        else (
            classification.accepted is None
            and predicted["predicted_reason_code"] == classification.reason_code
        )
    )

    if matched:
        return  # spec pass criterion: classification matches prediction.

    # Pass criterion fallback: mismatch identifies a missing tier.
    if classification.payload_present and classification.accepted is False and (
        classification.first_failed_gate is not None
    ):
        sys.stderr.write(
            _format_mismatch_block(fixture_id, predicted, classification)
        )
        return

    pytest.fail(
        f"Pre-registered prediction for {fixture_id} did not match and the "
        f"mismatch did not identify a missing tier. "
        f"predicted={predicted}; "
        f"observed accepted={classification.accepted!r}, "
        f"reason_code={classification.reason_code!r}, "
        f"first_failed_gate={classification.first_failed_gate!r}. "
        f"This indicates the classifier or fixture has a structural gap "
        f"the design document did not anticipate."
    )
