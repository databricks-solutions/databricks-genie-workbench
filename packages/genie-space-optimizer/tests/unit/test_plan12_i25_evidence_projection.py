"""Plan 12 PR 7 deferred — I25 evidence projection wire-in tests.

The harness stashes the iteration's journey events on
``current_iter_inputs["_journey_events_for_i25"]`` at finalize time;
``project_iter_evidence`` reads them and populates the 4 pairs of
I25 evidence keys so the invariant can surface drift between legacy
counters and source-of-truth derivers.

The wire-in is observation-actionable: legacy writer paths stay
unchanged. I25 begins firing if/when a future regression introduces
drift.
"""
from genie_space_optimizer.optimization.invariant_projection import (
    project_iter_evidence,
)
from genie_space_optimizer.optimization.patch_survival_emitter import (
    reset_patch_outcome_emitter,
)


def _journey(events: list[str]) -> tuple[dict, ...]:
    return tuple({"event": e} for e in events)


def test_i25_keys_default_zero_when_journey_missing():
    """Pre-Plan-12 fixtures (no _journey_events_for_i25 stash) →
    every I25 key defaults to 0. I25 sees 0 == 0 → green."""
    reset_patch_outcome_emitter()
    evidence = project_iter_evidence(
        current_iter_inputs={"decision_records": []},
        iteration=1,
        run_id="run_x",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    assert evidence["phase_b_end_total_recorded"] == 0
    assert evidence["journey_records_count"] == 0
    assert evidence["iteration_summary_count_recorded"] == 0
    assert evidence["iteration_summary_count_from_journey"] == 0
    assert evidence["proposal_attempts_recorded"] == 0
    assert evidence["proposal_attempts_from_outcomes"] == 0
    assert evidence["run_summary_hard_count_recorded"] == 0
    assert evidence["run_summary_hard_count_from_eval"] == 0


def test_i25_journey_records_count_matches_stashed_journey():
    reset_patch_outcome_emitter()
    evidence = project_iter_evidence(
        current_iter_inputs={
            "decision_records": [
                {"decision_type": "x"},
                {"decision_type": "y"},
                {"decision_type": "z"},
            ],
            "_journey_events_for_i25": _journey([
                "iteration_started",
                "ag_emitted",
                "proposal_synthesized",
            ]),
        },
        iteration=1,
        run_id="run_x",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    assert evidence["phase_b_end_total_recorded"] == 3
    assert evidence["journey_records_count"] == 3


def test_i25_iteration_summary_count_matches_journey():
    """The journey ledger carries ``iteration_summary_recorded`` events
    when an iteration emits its summary. The deriver counts them."""
    reset_patch_outcome_emitter()
    evidence = project_iter_evidence(
        current_iter_inputs={
            "decision_records": [],
            "_journey_events_for_i25": _journey([
                "iteration_started",
                "iteration_summary_recorded",
                "iteration_terminated",
            ]),
        },
        iteration=1,
        run_id="run_x",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    assert evidence["iteration_summary_count_from_journey"] == 1
    assert evidence["iteration_summary_count_recorded"] == 1


def test_i25_proposal_attempts_uses_emitter_intents(monkeypatch):
    """``proposal_attempts_from_outcomes`` counts unique intent_ids
    registered with the patch-outcome emitter for the given
    (run_id, iteration)."""
    reset_patch_outcome_emitter()
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
    )
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        intent_id="intent_001",
        outcome_kind=PatchOutcomeKind.APPLIED,
    )
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        intent_id="intent_002",
        outcome_kind=PatchOutcomeKind.APPLIED,
    )
    # Different iter — must NOT contribute.
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="AG1",
        cluster_id="H001",
        intent_id="intent_999",
        outcome_kind=PatchOutcomeKind.APPLIED,
    )
    evidence = project_iter_evidence(
        current_iter_inputs={
            "proposal_count": 2,
            "decision_records": [],
        },
        iteration=1,
        run_id="run_x",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    assert evidence["proposal_attempts_recorded"] == 2
    assert evidence["proposal_attempts_from_outcomes"] == 2


def test_i25_proposal_attempts_drift_surfaces_through_invariant(monkeypatch):
    """The point of the wire-in: when the legacy counter and the
    source-of-truth deriver diverge, I25 fires. This test forces a
    drift and asserts the invariant produces a violation."""
    reset_patch_outcome_emitter()
    from genie_space_optimizer.optimization.invariants import (
        check_i25_observability_consistency,
    )
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
    )
    # Emitter sees ONE intent; legacy counter claims THREE.
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        intent_id="only_intent",
        outcome_kind=PatchOutcomeKind.APPLIED,
    )
    evidence = project_iter_evidence(
        current_iter_inputs={
            "proposal_count": 3,  # the legacy "recorded" counter
            "decision_records": [],
        },
        iteration=1,
        run_id="run_x",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    violations = check_i25_observability_consistency(evidence)
    assert any(v["field"] == "proposal_attempts" for v in violations), (
        f"expected proposal_attempts violation; got {violations}"
    )
    # The violation carries both sides so a postmortem can pinpoint the
    # drift without re-reading the evidence dict.
    v = next(v for v in violations if v["field"] == "proposal_attempts")
    assert v["recorded"] == 3
    assert v["from_source"] == 1


def test_i25_journey_stash_handles_typed_event_objects():
    """The harness's ``_journey_emit`` produces typed
    ``QuestionJourneyEvent`` instances. The projection's normalizer
    accepts both typed objects (via ``.stage``/``.event`` attribute)
    AND plain dicts so the stash can carry either."""
    reset_patch_outcome_emitter()

    class _FakeJourneyEvent:
        def __init__(self, stage: str):
            self.stage = stage

    evidence = project_iter_evidence(
        current_iter_inputs={
            "decision_records": [],
            "_journey_events_for_i25": (
                _FakeJourneyEvent("iteration_started"),
                _FakeJourneyEvent("iteration_summary_recorded"),
            ),
        },
        iteration=1,
        run_id="run_x",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    assert evidence["journey_records_count"] == 2
    assert evidence["iteration_summary_count_from_journey"] == 1


def test_i25_keys_skip_when_run_id_blank():
    """``project_iter_evidence`` short-circuits when run_id is empty.
    The I25 keys are absent from the no-op evidence dict."""
    reset_patch_outcome_emitter()
    evidence = project_iter_evidence(
        current_iter_inputs={"decision_records": []},
        iteration=1,
        run_id="",  # blank → no-op
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    assert "phase_b_end_total_recorded" not in evidence
    assert "journey_records_count" not in evidence
    assert "proposal_attempts_recorded" not in evidence
