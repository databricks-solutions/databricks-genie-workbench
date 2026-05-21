"""Plan 12 — Phase B end total must reflect the journey ledger, not
its own write counter (which drifted under retry)."""


def test_phase_b_end_total_matches_journey_records():
    from genie_space_optimizer.optimization.harness import (
        derive_phase_b_end_total_from_journey,
    )

    journey_records = [
        {"event": "iteration_started", "iteration": 1},
        {"event": "ag_emitted", "iteration": 1},
        {"event": "proposal_synthesized", "iteration": 1},
        {"event": "patch_applied", "iteration": 1},
        {"event": "iteration_terminated", "iteration": 1},
    ]
    assert derive_phase_b_end_total_from_journey(journey_records) == 5


def test_phase_b_end_total_zero_for_empty_journey():
    from genie_space_optimizer.optimization.harness import (
        derive_phase_b_end_total_from_journey,
    )
    assert derive_phase_b_end_total_from_journey([]) == 0


def test_phase_b_end_total_handles_none():
    from genie_space_optimizer.optimization.harness import (
        derive_phase_b_end_total_from_journey,
    )
    assert derive_phase_b_end_total_from_journey(None) == 0  # type: ignore[arg-type]
