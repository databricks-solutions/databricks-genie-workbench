"""Run-end replay validation: bridge from run_replay(...) to the
{is_valid, violation_count} dict shape consumed by
contract_health.build_contract_health_summary.

These tests pin the recipe (not the harness wiring; that's the
grep-guard's job). The harness wiring inlines the same five lines
inside the Phase A try block.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout


def _validate_fixture_dict(fixture: dict) -> dict:
    """The exact 5-line recipe inlined into harness.py after the
    serialize_replay_fixture call.

    Returned dict shape: {"is_valid": bool, "violation_count": int}.
    Matches contract_health._classify_replay's required keys.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )
    result = run_replay(fixture)
    return {
        "is_valid": bool(result.validation.is_valid),
        "violation_count": int(len(result.validation.violations)),
    }


def test_clean_fixture_produces_is_valid_true_zero_violations() -> None:
    """A fixture whose iterations have no replay-contract violations
    must produce {is_valid: True, violation_count: 0}."""

    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    fixture_json = serialize_replay_fixture(
        fixture_id="test_clean",
        iterations_data=[],
    )
    fixture = json.loads(fixture_json)

    result = _validate_fixture_dict(fixture)

    assert result == {"is_valid": True, "violation_count": 0}, result


def test_violating_fixture_produces_is_valid_false_with_count(monkeypatch) -> None:
    """When the fixture's iterations produce replay-contract violations,
    the bridge must surface the count and is_valid=False.

    We monkey-patch the replay primitive so the test does not need to
    construct a fixture that authentically triggers a journey-contract
    violation. Authenticity is covered by the existing replay tests
    (e.g., the captured-run replay fixtures); here we only verify the
    bridge shape.
    """

    from dataclasses import dataclass

    @dataclass
    class _FakeReport:
        is_valid: bool
        violations: list

    @dataclass
    class _FakeReplayResult:
        validation: _FakeReport

    fake_violations = [
        {"qid": "gs_021", "rule": "clustered_then_soft_signal"},
        {"qid": "gs_021", "rule": "stage_rank_regression"},
        {"qid": "gs_999", "rule": "missing_terminal_state"},
    ]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever_loop_replay.run_replay",
        lambda _fixture: _FakeReplayResult(
            validation=_FakeReport(
                is_valid=False,
                violations=list(fake_violations),
            ),
        ),
    )

    result = _validate_fixture_dict({"fixture_id": "test_dirty", "iterations": []})

    assert result == {"is_valid": False, "violation_count": 3}, result
