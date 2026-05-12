"""Cycle 17 T2 — producer-side mutual exclusion under
`GSO_JOURNEY_PRODUCER_STRICT`. Tests cover both producer surfaces:
`_replay_iteration` (lever_loop_replay) and
`emit_cluster_membership_events` (question_journey).
"""
from __future__ import annotations

import importlib

import pytest

from genie_space_optimizer.optimization.lever_loop_replay import (
    _replay_iteration,
)
from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
    emit_cluster_membership_events,
)


def _iteration_plan_with_hard_and_soft_overlap() -> dict:
    """Synthetic iteration where qid 'q_overlap' is both row-classified
    soft (default fall-through) AND declared in a hard cluster."""
    return {
        "iteration": 1,
        "eval_rows": [
            {
                "question_id": "q_overlap",
                "result_correctness": "no",
                "arbiter": "genie_correct",  # does not match the explicit
                                              # hard branch → soft fall-through
            },
            {
                "question_id": "q_pure_hard",
                "result_correctness": "no",
                "arbiter": "ground_truth_correct",
            },
        ],
        "clusters": [
            {
                "cluster_id": "H1",
                "root_cause": "rc1",
                "question_ids": ["q_overlap", "q_pure_hard"],
            },
        ],
        "soft_clusters": [],
        "strategist_response": {},
        "ag_outcomes": {},
        "post_eval_passing_qids": [],
    }


def _stages_for_qid(events: list[QuestionJourneyEvent], qid: str) -> list[str]:
    return [ev.stage for ev in events if ev.question_id == qid]


def test_replay_iteration_emits_both_when_flag_off(monkeypatch):
    """Flag-off: legacy behaviour preserved — `soft_signal` and
    `clustered` are both emitted for `q_overlap`. Byte-stability on
    pre-Cycle-17 fixtures depends on this branch.

    Defect Plan 3 (2026-05-12) flipped the default to ON. This test
    pins the legacy-off regime via explicit ``setenv=0``.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "0")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    events: list[QuestionJourneyEvent] = []
    _replay_iteration(
        iteration_plan=_iteration_plan_with_hard_and_soft_overlap(),
        events=events,
    )
    overlap_stages = _stages_for_qid(events, "q_overlap")
    assert "clustered" in overlap_stages
    assert "soft_signal" in overlap_stages, (
        f"flag-off should retain legacy soft_signal emit; got {overlap_stages!r}"
    )


def test_replay_iteration_suppresses_soft_signal_when_flag_on(monkeypatch):
    """Flag-on: `soft_signal` is suppressed for hard-clustered qids.
    The qid stays clustered; the redundant soft fall-through emit
    does not produce an illegal `clustered → soft_signal` trunk
    transition.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    events: list[QuestionJourneyEvent] = []
    _replay_iteration(
        iteration_plan=_iteration_plan_with_hard_and_soft_overlap(),
        events=events,
    )
    overlap_stages = _stages_for_qid(events, "q_overlap")
    assert "clustered" in overlap_stages
    assert "soft_signal" not in overlap_stages, (
        f"flag-on should suppress soft_signal for hard-clustered qid; "
        f"got {overlap_stages!r}"
    )


def test_replay_iteration_keeps_already_passing_under_flag_on(monkeypatch):
    """Flag-on preserves `already_passing` co-emit for hard-clustered
    qids — T1's state-machine extension makes this transition legal,
    so suppression would drop a legitimate signal.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    plan = {
        "iteration": 1,
        "eval_rows": [
            {
                "question_id": "q_passing_in_cluster",
                "result_correctness": "yes",
                "arbiter": "both_correct",  # → row already_passing
            },
        ],
        "clusters": [
            {
                "cluster_id": "H1",
                "root_cause": "rc1",
                "question_ids": ["q_passing_in_cluster"],
            },
        ],
        "soft_clusters": [],
        "strategist_response": {},
        "ag_outcomes": {},
        "post_eval_passing_qids": ["q_passing_in_cluster"],
    }
    events: list[QuestionJourneyEvent] = []
    _replay_iteration(iteration_plan=plan, events=events)
    stages = _stages_for_qid(events, "q_passing_in_cluster")
    assert "clustered" in stages
    assert "already_passing" in stages, (
        f"already_passing must remain co-emitted under flag-on (T1 makes "
        f"clustered → already_passing legal); got {stages!r}"
    )


def test_emit_cluster_membership_events_emits_both_when_flag_off(monkeypatch):
    """Flag-off: legacy behaviour — a qid declared in both
    hard_clusters and soft_clusters receives both events.

    Defect Plan 3 (2026-05-12) flipped the default to ON. This test
    pins the legacy-off regime via explicit ``setenv=0``.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "0")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    captured: list[QuestionJourneyEvent] = []

    def _emit(stage, **fields):
        for q in fields.pop("question_ids", None) or []:
            captured.append(
                QuestionJourneyEvent(question_id=str(q), stage=stage, **fields)
            )

    emit_cluster_membership_events(
        journey_emit=_emit,
        hard_clusters=[{"cluster_id": "H1", "root_cause": "rc1",
                        "question_ids": ["q_dual"]}],
        soft_clusters=[{"cluster_id": "S1", "root_cause": "rc2",
                        "question_ids": ["q_dual"]}],
    )
    stages = _stages_for_qid(captured, "q_dual")
    assert "clustered" in stages
    assert "soft_signal" in stages, (
        f"flag-off should retain legacy soft_signal emit; got {stages!r}"
    )


def test_emit_cluster_membership_events_suppresses_soft_when_flag_on(monkeypatch):
    """Flag-on: when a qid is emitted as `clustered` in the hard
    pass, the soft pass skips it. Hard wins.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    captured: list[QuestionJourneyEvent] = []

    def _emit(stage, **fields):
        for q in fields.pop("question_ids", None) or []:
            captured.append(
                QuestionJourneyEvent(question_id=str(q), stage=stage, **fields)
            )

    emit_cluster_membership_events(
        journey_emit=_emit,
        hard_clusters=[{"cluster_id": "H1", "root_cause": "rc1",
                        "question_ids": ["q_dual"]}],
        soft_clusters=[{"cluster_id": "S1", "root_cause": "rc2",
                        "question_ids": ["q_dual"]}],
    )
    stages = _stages_for_qid(captured, "q_dual")
    assert "clustered" in stages
    assert "soft_signal" not in stages, (
        f"flag-on should suppress soft_signal for hard-clustered qid; "
        f"got {stages!r}"
    )


# ── Defect Plan 3 (2026-05-12) — production producer default-on ─────


def test_emit_cluster_membership_events_default_on_suppresses_soft_for_hard_qid(
    monkeypatch,
):
    """Defect Plan 3 closure for the live producer surface: when
    ``GSO_JOURNEY_PRODUCER_STRICT`` is unset, ``emit_cluster_membership_events``
    must suppress the soft emit for any qid already emitted as
    ``clustered`` in the hard pass.

    This pins the production-side analogue of the replay-side
    ``test_anchor_delenv_uses_default_and_clears_all_violations``
    in tests/replay/test_replay_anchor_ccf1d60d_zero_violations.py.
    """
    monkeypatch.delenv("GSO_JOURNEY_PRODUCER_STRICT", raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    assert config.journey_producer_strict_enabled() is True, (
        "Defect Plan 3 precondition: flag default must be ON"
    )

    captured: list[QuestionJourneyEvent] = []

    def _emit(stage, **fields):
        for q in fields.pop("question_ids", None) or []:
            captured.append(
                QuestionJourneyEvent(question_id=str(q), stage=stage, **fields)
            )

    emit_cluster_membership_events(
        journey_emit=_emit,
        hard_clusters=[
            {"cluster_id": "H1", "root_cause": "rc1",
             "question_ids": ["q_overlap", "q_pure_hard"]},
        ],
        soft_clusters=[
            {"cluster_id": "S1", "root_cause": "rc2",
             "question_ids": ["q_overlap", "q_pure_soft"]},
        ],
    )

    overlap_stages = sorted(_stages_for_qid(captured, "q_overlap"))
    assert overlap_stages == ["clustered"], (
        f"Defect Plan 3: q_overlap must receive exactly one trunk "
        f"event (clustered); got {overlap_stages}"
    )
    assert _stages_for_qid(captured, "q_pure_hard") == ["clustered"]
    assert _stages_for_qid(captured, "q_pure_soft") == ["soft_signal"]


def test_emit_cluster_membership_events_explicit_zero_preserves_legacy_dual_emit(
    monkeypatch,
):
    """Legacy regression: with ``GSO_JOURNEY_PRODUCER_STRICT=0``
    explicitly set, ``q_overlap`` receives BOTH trunk events.
    Used by the legacy-anchor replay tests."""
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "0")
    from genie_space_optimizer.common import config

    importlib.reload(config)
    assert config.journey_producer_strict_enabled() is False

    captured: list[QuestionJourneyEvent] = []

    def _emit(stage, **fields):
        for q in fields.pop("question_ids", None) or []:
            captured.append(
                QuestionJourneyEvent(question_id=str(q), stage=stage, **fields)
            )

    emit_cluster_membership_events(
        journey_emit=_emit,
        hard_clusters=[
            {"cluster_id": "H1", "root_cause": "rc1",
             "question_ids": ["q_overlap"]},
        ],
        soft_clusters=[
            {"cluster_id": "S1", "root_cause": "rc2",
             "question_ids": ["q_overlap"]},
        ],
    )

    overlap_stages = sorted(_stages_for_qid(captured, "q_overlap"))
    assert overlap_stages == ["clustered", "soft_signal"], (
        f"legacy regime should emit both trunk events; got {overlap_stages}"
    )
