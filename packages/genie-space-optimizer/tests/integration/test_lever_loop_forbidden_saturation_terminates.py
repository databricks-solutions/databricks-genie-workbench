"""Risk 1 — integration test: when every iteration hits the AG
collision guard, the lever loop terminates within
``_COLLISION_SATURATION_THRESHOLD`` iterations rather than burning
the full ``max_iterations`` budget.

Fixture strategy: directly drive ``_run_lever_loop`` through the
existing replay-runner harness with a minimal synthetic reflection
buffer pre-seeded with one forbidden AG, plus a stubbed strategist
that re-emits the same AG every iteration. The harness's collision
guard at ``harness.py:19108`` then fires on every iteration.

The test asserts the loop exits inside 3 iterations (threshold=2 +
1-iteration tolerance for the first AG emit that arms the
forbidden set) and the final convergence reason is
``no_more_high_confidence_interventions``.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "risk1_forbidden_saturation"
    / "replay_fixture.json"
)


@pytest.mark.skipif(
    not FIXTURE_PATH.exists(),
    reason=(
        "fixture not yet generated — needs a runid_analysis evidence "
        "snapshot to materialise"
    ),
)
def test_forbidden_saturation_terminates_loop_within_threshold() -> None:
    from genie_space_optimizer.tools.replay_runner import ReplayRunner
    from genie_space_optimizer.common.mlflow_markers import parse_markers

    runner = ReplayRunner(fixture_path=str(FIXTURE_PATH))
    stdout_output = runner.run_replay_and_capture_stdout()
    markers = parse_markers(stdout_output)

    convergence = markers.get("GSO_CONVERGENCE_V1", [])
    assert convergence, (
        "expected at least one GSO_CONVERGENCE_V1 marker in stdout"
    )
    last = convergence[-1]
    assert last.get("reason") == "no_more_high_confidence_interventions", (
        f"expected termination reason "
        f"no_more_high_confidence_interventions; got {last.get('reason')!r}"
    )
    iter_count = int(last.get("iteration_counter") or 0)
    assert iter_count <= 3, (
        f"forbidden-saturation guard should terminate within 3 iterations; "
        f"observed iteration_counter={iter_count}"
    )


def test_collision_saturation_termination_wiring_present_in_harness() -> None:
    """Static check: the ``_run_lever_loop`` body must call
    ``_should_terminate_on_collision_saturation`` so the Risk-1
    guard cannot be silently disabled by a future refactor.
    """
    from genie_space_optimizer import optimization

    harness_path = f"{optimization.__path__[0]}/harness.py"
    text = Path(harness_path).read_text()

    assert "_should_terminate_on_collision_saturation(" in text, (
        "harness.py must call the Risk-1 saturation predicate inside "
        "the ag_identity_skip path"
    )
    assert "_consecutive_collision_skips" in text, (
        "harness.py must maintain the consecutive-collision counter"
    )
    assert "no_more_high_confidence_interventions" in text, (
        "harness.py must emit the canonical termination reason"
    )
