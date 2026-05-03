"""Phase D Task 1: ScoreboardSnapshot frozen dataclass.

Covers:
- Constructor accepts every named metric field plus dominant_signal.
- to_dict() returns a sorted dict suitable for JSON / MLflow logging.
- from_dict() round-trips ScoreboardSnapshot.to_dict().
"""
from __future__ import annotations

import pytest


def _make_snapshot(**overrides):
    from genie_space_optimizer.optimization.scoreboard import ScoreboardSnapshot

    base = dict(
        iteration=3,
        run_id="run_demo",
        journey_completeness_pct=0.75,
        hard_cluster_coverage_pct=0.50,
        causal_patch_survival_pct=0.66,
        malformed_proposals_at_cap=2,
        rollback_attribution_complete_pct=1.0,
        terminal_unactionable_qids=4,
        accuracy_delta=0.012,
        trace_id_fallback_rate=0.10,
        decision_trace_completeness_pct=0.90,
        rca_loop_closure_pct=0.80,
        dominant_signal="GATE_OR_CAP_GAP",
    )
    base.update(overrides)
    return ScoreboardSnapshot(**base)


def test_scoreboard_snapshot_is_frozen():
    snap = _make_snapshot()
    with pytest.raises(Exception):
        snap.iteration = 99


def test_scoreboard_snapshot_to_dict_round_trip():
    from genie_space_optimizer.optimization.scoreboard import ScoreboardSnapshot

    snap = _make_snapshot()
    payload = snap.to_dict()
    assert payload["iteration"] == 3
    assert payload["dominant_signal"] == "GATE_OR_CAP_GAP"
    assert payload["decision_trace_completeness_pct"] == pytest.approx(0.90)

    rebuilt = ScoreboardSnapshot.from_dict(payload)
    assert rebuilt == snap


def test_scoreboard_snapshot_to_dict_keys_are_sorted():
    snap = _make_snapshot()
    payload = snap.to_dict()
    assert list(payload.keys()) == sorted(payload.keys())
