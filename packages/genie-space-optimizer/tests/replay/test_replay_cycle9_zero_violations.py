"""Cycle 9 replay intake — gate promotion on zero violations (Cycle9 T13).

The cycle 9 raw fixture (verbatim ``PHASE_A_REPLAY_FIXTURE_JSON`` block
from run ``1e855111-b463-4556-9b30-8cd32f78ebcb``) is not yet captured
locally — it requires a deploy + rerun on Databricks to extract from
the job stderr. Until the fixture lands at
``tests/replay/fixtures/airline_real_v1_cycle9_raw.json``, all three
tests skip.

Once a refreshed run captures the post-burndown contract (T6/T7/T10
producers landed, journey_validation populated, decision_records non-
empty per iteration), unskip ``test_every_iteration_has_decision_records``
and ``test_journey_validation_populated_and_no_violations`` to gate
cycle-9 promotion on zero violations.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T13.
"""
from __future__ import annotations

import json
import pathlib

import pytest

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "airline_real_v1_cycle9_raw.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not FIXTURE_PATH.exists():
        pytest.skip(
            "Cycle 9 raw fixture not yet captured — needs a deploy + rerun "
            "to extract the PHASE_A_REPLAY_FIXTURE_JSON block from job stderr."
        )
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_loaded_with_five_iterations(fixture):
    assert fixture["fixture_id"].startswith("airline_real_v1_run_")
    assert len(fixture["iterations"]) == 5


@pytest.mark.skip(
    reason=(
        "Cycle 9 raw fixture — pre-T6/T7/T10. journey_validation and "
        "decision_records are not yet populated. Unskip once a refreshed "
        "run captures the post-burndown contract."
    )
)
def test_every_iteration_has_decision_records(fixture):
    for it in fixture["iterations"]:
        assert it["decision_records"], (
            f"iter {it['iteration']}: decision_records empty — "
            f"producers did not fire"
        )


@pytest.mark.skip(
    reason="Same as above; gated on post-burndown re-run."
)
def test_journey_validation_populated_and_no_violations(fixture):
    for it in fixture["iterations"]:
        jv = it["journey_validation"]
        assert jv is not None, f"iter {it['iteration']}: journey_validation null"
        assert jv.get("violations", []) == [], (
            f"iter {it['iteration']}: journey violations: "
            f"{jv['violations']}"
        )
