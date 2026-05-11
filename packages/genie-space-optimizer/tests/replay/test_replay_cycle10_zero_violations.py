"""Cycle 10 replay intake — gate promotion on zero violations.

The cycle 10 raw fixture is committed at
``tests/replay/fixtures/airline_real_v1_cycle10_raw.json`` (extracted
from optimization_run_id ``407772af-9662-4803-be6b-f00a368c528a``).

Cycle 17 T1 + T2 closes the producer bugs documented in the prior
skip docstring:
  - T1 extension #2 (evaluated → post_eval) — clears the 3 "qids
    reach post_eval directly from evaluated" violations.
  - T2 producer fix under GSO_JOURNEY_PRODUCER_STRICT — clears the
    5 ``clustered → soft_signal`` violations (qid_016 dual-emit).
"""
from __future__ import annotations

import importlib
import json
import pathlib

import pytest

from genie_space_optimizer.optimization.lever_loop_replay import run_replay

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "airline_real_v1_cycle10_raw.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not FIXTURE_PATH.exists():
        pytest.skip(f"cycle 10 raw fixture missing at {FIXTURE_PATH}")
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_loads_with_iterations(fixture):
    assert fixture["fixture_id"].startswith("airline_real_v1_run_")
    assert isinstance(fixture.get("iterations"), list)
    assert len(fixture["iterations"]) >= 1


@pytest.mark.skip(
    reason=(
        "Cycle 10 raw fixture — pre-PR-B2 producers. decision_records are "
        "empty per iteration. Unskip once PR-B2 has shipped and a refreshed "
        "run lands a fixture with populated decision_records."
    )
)
def test_every_iteration_has_decision_records(fixture):
    for it in fixture["iterations"]:
        assert it["decision_records"], (
            f"iter {it['iteration']}: decision_records empty"
        )


def test_replay_yields_zero_violations(fixture, monkeypatch):
    """Cycle 17 T4 — with T1 extensions unconditional + T2 producer
    fix activated via GSO_JOURNEY_PRODUCER_STRICT, the cycle 10
    fixture produces zero journey-contract violations.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    result = run_replay(fixture)
    assert list(result.validation.violations) == [], (
        f"replay produced {len(result.validation.violations)} violations: "
        f"{[(v.question_id, v.kind, v.detail) for v in result.validation.violations[:5]]}"
    )
    assert result.validation.is_valid
