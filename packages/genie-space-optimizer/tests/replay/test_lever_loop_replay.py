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


# -----------------------------------------------------------------------------
# Per-iteration validation tests for `run_replay`.
#
# Pin that ``run_replay`` invokes ``validate_question_journeys`` once per
# iteration (mirroring the harness production contract at
# ``harness.py:16039-16056``) so cross-iteration ``X -> X`` self-transitions on
# the same qid are not reported as illegal_transition violations.
#
# See `docs/2026-05-02-run-replay-per-iteration-fix-plan.md` for the full
# diagnosis (cycle 7's airline_real_v1.json produced 328 violations under the
# old single-call validator, ~320 of which were cross-iteration noise).
# -----------------------------------------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_run_replay_two_iter_clean_fixture_validates_cleanly() -> None:
    """A 2-iter fixture where each iteration is independently legal must report
    zero violations. Reproducer for cycle 7's cross-iteration noise."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_load("synthetic_two_iter_clean.json"))

    assert result.validation.is_valid, (
        "Expected zero violations for a 2-iter fixture where each iteration "
        "is independently legal. Violations:\n"
        + "\n".join(
            f"  qid={v.question_id} kind={v.kind} detail={v.detail}"
            for v in result.validation.violations
        )
    )
    assert result.validation.violations == []
    assert result.validation.missing_qids == ()


def test_run_replay_intra_iter_violation_is_caught_and_attributed() -> None:
    """A qid that goes evaluated -> post_eval (no classification stage) is an
    illegal transition. The fix must keep catching this; it must NOT be silenced
    by per-iteration scoping."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_load("synthetic_two_iter_one_intra_violation.json"))

    assert not result.validation.is_valid
    illegal = [
        v for v in result.validation.violations if v.kind == "illegal_transition"
    ]
    assert len(illegal) == 1, (
        f"Expected 1 illegal_transition (syn_q2 evaluated -> post_eval), got "
        f"{len(illegal)}: {[(v.question_id, v.detail) for v in illegal]}"
    )
    assert illegal[0].question_id == "syn_q2"
    assert illegal[0].detail == "evaluated -> post_eval"


def test_run_replay_single_iter_5cluster_fixture_still_validates_cleanly() -> None:
    """Regression: airline_5cluster.json (1 iteration, the original test
    fixture the validator was designed against) must keep validating cleanly
    after the per-iteration refactor."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_load("airline_5cluster.json"))

    assert result.validation.is_valid, (
        "5cluster regression: validation must remain clean. Violations:\n"
        + "\n".join(
            f"  qid={v.question_id} kind={v.kind} detail={v.detail}"
            for v in result.validation.violations
        )
    )


def test_run_replay_airline_real_v1_within_burndown_budget() -> None:
    """The current canonical airline fixture must validate with no more than
    BURNDOWN_BUDGET violations.

    Tighten the budget in this test each time a real intra-iteration violation
    is fixed in the harness/exporter. When the budget reaches 0, Phase A
    burn-down has closed hard against the airline corpus.

    See `docs/2026-05-02-phase-a-burndown-log.md` for the per-cycle history
    and `docs/2026-05-02-run-replay-per-iteration-fix-plan.md` Phase 5 for
    the per-cycle intake runbook.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    # Updated by Phase 5 Task 14 Step 6 each time a fresh cycle lands.
    # Never increase this number without explicit triage in the burn-down log.
    BURNDOWN_BUDGET = 44

    fx = json.loads((FIXTURES / "airline_real_v1.json").read_text())
    result = run_replay(fx)
    summary = [
        (v.question_id, v.kind, v.detail)
        for v in result.validation.violations[:5]
    ]
    assert len(result.validation.violations) <= BURNDOWN_BUDGET, (
        f"airline_real_v1 produced {len(result.validation.violations)} "
        f"violations (budget={BURNDOWN_BUDGET}). First 5: {summary}"
    )
