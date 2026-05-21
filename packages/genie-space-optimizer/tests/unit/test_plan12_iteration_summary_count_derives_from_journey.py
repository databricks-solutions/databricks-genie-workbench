"""Plan 12 — iteration_summary_count derives from journey ledger
event stream, not a parallel counter."""


def test_iteration_summary_count_matches_journey():
    from genie_space_optimizer.optimization.harness import (
        derive_iteration_summary_count_from_journey,
    )
    journey = [
        {"event": "iteration_started", "iteration": 1},
        {"event": "iteration_summary_recorded", "iteration": 1},
        {"event": "iteration_started", "iteration": 2},
        {"event": "iteration_summary_recorded", "iteration": 2},
        {"event": "iteration_started", "iteration": 3},
    ]
    assert derive_iteration_summary_count_from_journey(journey) == 2


def test_iteration_summary_count_zero_for_empty_journey():
    from genie_space_optimizer.optimization.harness import (
        derive_iteration_summary_count_from_journey,
    )
    assert derive_iteration_summary_count_from_journey([]) == 0


def test_iteration_summary_count_ignores_other_events():
    from genie_space_optimizer.optimization.harness import (
        derive_iteration_summary_count_from_journey,
    )
    journey = [
        {"event": "ag_emitted", "iteration": 1},
        {"event": "proposal_synthesized", "iteration": 1},
        {"event": "patch_applied", "iteration": 1},
    ]
    assert derive_iteration_summary_count_from_journey(journey) == 0
