"""Risk 3 — end-to-end: when a run triggers a HIGH-tier invariant
(I12 replay validity) the contract-health summary marker reports
the violation in ``high_tier_violations``.

We force I12 to fire by feeding the harness a replay fixture whose
journey events contain a known ``clustered -> soft_signal`` violation
with ``GSO_JOURNEY_PRODUCER_STRICT=0`` so the legacy producer runs.
That guarantees ``_run_end_replay_validation.is_valid=False`` AND
the per-iteration I12 invariant fires inside the loop.
"""
from __future__ import annotations

from pathlib import Path

import pytest


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "risk3_i12_violation"
    / "replay_fixture.json"
)


@pytest.mark.skipif(
    not FIXTURE_PATH.exists(),
    reason=(
        "fixture not yet generated — see Plan T2 Step 8 (requires "
        "runid_analysis evidence snapshot)"
    ),
)
def test_contract_health_carries_high_tier_invariant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "0")
    monkeypatch.setenv("GSO_LOOP_INVARIANTS_STRICT", "0")
    from genie_space_optimizer.tools.replay_runner import ReplayRunner
    from genie_space_optimizer.common.mlflow_markers import parse_markers

    runner = ReplayRunner(fixture_path=str(FIXTURE_PATH))
    stdout_output = runner.run_replay_and_capture_stdout()
    markers = parse_markers(stdout_output)

    ch = markers.get("GSO_CONTRACT_HEALTH_V1", [])
    assert ch, "expected at least one GSO_CONTRACT_HEALTH_V1 marker"
    summary = ch[-1]
    high = summary.get("high_tier_violations") or ()
    assert high, (
        "expected high_tier_violations to be non-empty when I12 fires; "
        f"got {high!r}"
    )
    assert any(
        str(v.get("invariant_id")) == "I12" for v in high
    ), (
        "expected at least one I12 violation in high_tier_violations; "
        f"got {[v.get('invariant_id') for v in high]!r}"
    )


def test_invariant_violations_accumulator_wired_in_harness() -> None:
    """Static check: harness.py must (1) initialise
    ``_invariant_violations`` in ``_run_lever_loop`` and (2) thread it
    through ``_finalize_iteration_summary`` calls via
    ``run_violations_accumulator=_invariant_violations``. Pins the
    Risk-3 wiring so a future refactor cannot silently un-wire it.
    """
    from genie_space_optimizer import optimization

    harness_path = f"{optimization.__path__[0]}/harness.py"
    text = Path(harness_path).read_text()

    assert "_invariant_violations: list[dict] = []" in text, (
        "harness.py must initialise _invariant_violations as a "
        "run-level accumulator inside _run_lever_loop"
    )
    assert "run_violations_accumulator=_invariant_violations" in text, (
        "harness.py must thread the accumulator into "
        "_finalize_iteration_summary"
    )
    # Consumer at run-end reads _invariant_violations from locals().
    assert 'locals().get("_invariant_violations")' in text, (
        "the contract-health summary emitter must continue to read "
        "_invariant_violations from locals() (Risk-3 invariant)"
    )
