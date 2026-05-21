"""Plan 12 PR 7 deferred — verify the harness's ``_finalize_iteration_summary``
stashes the iteration's journey events on
``current_iter_inputs["_journey_events_for_i25"]`` before invoking the
invariant runner.

The stash is the upstream half of the I25 evidence projection wire-in:
without it, the projection has no journey data to feed the derivers
and I25 silently passes on every iteration.
"""


def test_finalize_stashes_journey_events_on_iter_inputs(monkeypatch):
    """``_finalize_iteration_summary`` must copy ``journey_events`` to
    ``current_iter_inputs["_journey_events_for_i25"]`` so the
    projection can read them. The stash happens BEFORE the invariant
    runner is invoked."""
    from genie_space_optimizer.optimization import (
        harness as _harness_mod,
    )

    # Intercept the invariant runner so we can inspect the iter_inputs
    # at the moment the projection would read it.
    captured: dict = {}

    def _spy_runner(*, run_id, iteration, current_iter_inputs, **_kw):
        captured["iter_inputs"] = dict(current_iter_inputs)
        captured["run_id"] = run_id
        captured["iteration"] = iteration

    monkeypatch.setattr(
        _harness_mod,
        "_run_iteration_invariants_and_append_records",
        _spy_runner,
    )

    iter_traces: dict = {}
    iter_summaries: dict = {}
    current_iter_inputs: dict = {"decision_records": []}

    class _FakeJourneyEvent:
        def __init__(self, stage: str):
            self.stage = stage

    journey_events = [
        _FakeJourneyEvent("iteration_started"),
        _FakeJourneyEvent("iteration_summary_recorded"),
    ]

    _harness_mod._finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
        current_iter_inputs=current_iter_inputs,
        journey_events=journey_events,
        journey_report=None,
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=None,
        exit_path="completed",
        run_id="run_test",
    )

    assert "iter_inputs" in captured, "invariant runner spy did not fire"
    stashed = captured["iter_inputs"].get("_journey_events_for_i25")
    assert stashed is not None, "journey events were not stashed"
    assert len(stashed) == 2
    # Stash preserves the underlying objects (or tuple form thereof) —
    # the projection normalizer handles both typed-object + dict shapes.
    assert all(hasattr(e, "stage") for e in stashed)


def test_finalize_handles_empty_journey_without_crashing(monkeypatch):
    """When no journey events were emitted (a legacy iteration body
    or a short-circuit early exit), the stash holds an empty tuple
    and the projection sees 0 journey records — matching the legacy
    zero-counter behavior."""
    from genie_space_optimizer.optimization import (
        harness as _harness_mod,
    )

    captured: dict = {}

    def _spy_runner(*, run_id, iteration, current_iter_inputs, **_kw):
        captured["iter_inputs"] = dict(current_iter_inputs)

    monkeypatch.setattr(
        _harness_mod,
        "_run_iteration_invariants_and_append_records",
        _spy_runner,
    )

    current_iter_inputs: dict = {"decision_records": []}

    _harness_mod._finalize_iteration_summary(
        iter_traces={},
        iter_summaries={},
        iteration=1,
        current_iter_inputs=current_iter_inputs,
        journey_events=None,
        journey_report=None,
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=None,
        exit_path="completed",
        run_id="run_test",
    )

    stashed = captured["iter_inputs"].get("_journey_events_for_i25")
    assert stashed == ()
