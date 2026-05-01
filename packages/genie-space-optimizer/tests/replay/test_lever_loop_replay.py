"""End-to-end replay test: byte-stable canonical ledger + zero violations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "airline_5cluster.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def test_airline_5cluster_replay_validation_is_clean() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    result = run_replay(_load_fixture())
    if not result.validation.is_valid:
        pytest.fail(
            "Replay produced validation violations:\n"
            + "\n".join(
                f"  qid={v.question_id} kind={v.kind} detail={v.detail}"
                for v in result.validation.violations
            )
            + f"\nmissing_qids={list(result.validation.missing_qids)}"
        )


def test_airline_5cluster_replay_canonical_ledger_is_byte_stable() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fixture = _load_fixture()
    expected = fixture.get("expected_canonical_journey")
    if not expected:
        pytest.skip(
            "expected_canonical_journey not yet recorded; run "
            "scripts/record_replay_baseline.py to seed it."
        )
    result = run_replay(fixture)
    assert result.canonical_json == expected, (
        "Canonical journey drift detected. If this drift was intentional, "
        "rerun the baseline recorder script and commit the new fixture."
    )


def test_airline_5cluster_replay_completes_in_under_thirty_seconds() -> None:
    import time
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fixture = _load_fixture()
    started = time.perf_counter()
    run_replay(fixture)
    elapsed = time.perf_counter() - started
    assert elapsed < 30.0, f"Replay took {elapsed:.2f}s (>30s budget)."
