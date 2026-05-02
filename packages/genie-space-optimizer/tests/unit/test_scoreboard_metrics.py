"""Track 6 — operator scoreboard. Tests pin the contract of every
metric function and the aggregator's dominant-signal selection.
"""
from __future__ import annotations

import pytest


def _make_snapshot(**overrides):
    """Build a LoopSnapshot with sensible defaults for tests."""
    from genie_space_optimizer.optimization.scoreboard import LoopSnapshot

    base = dict(
        question_ids=["q1", "q2", "q3"],
        hard_cluster_qids={"q1": "H001", "q2": "H001", "q3": "H002"},
        journey_events_per_qid={
            "q1": ["evaluated", "clustered", "ag_assigned", "applied_targeted"],
            "q2": ["evaluated", "clustered", "ag_assigned", "applied_targeted"],
            "q3": ["evaluated", "clustered", "ag_assigned", "applied_targeted"],
        },
        proposed_patches=[],
        applied_patches=[],
        rolled_back_patches=[],
        malformed_proposals_at_cap_count=0,
        rollback_records=[],
        terminal_unactionable_qids=set(),
        baseline_accuracy=0.50,
        candidate_accuracy=0.60,
        trace_id_fallback_recovered=0,
        trace_id_fallback_total=10,
    )
    base.update(overrides)
    return LoopSnapshot(**base)


def test_journey_completeness_pct_returns_one_when_every_qid_reaches_terminal_stage() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        journey_completeness_pct,
    )

    snap = _make_snapshot()
    assert journey_completeness_pct(snap) == pytest.approx(1.0)


def test_journey_completeness_pct_below_one_when_some_qids_drop_early() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        journey_completeness_pct,
    )

    snap = _make_snapshot(
        journey_events_per_qid={
            "q1": ["evaluated", "clustered"],
            "q2": ["evaluated", "clustered", "ag_assigned", "applied_targeted"],
            "q3": ["evaluated", "clustered", "ag_assigned", "applied_targeted"],
        },
    )
    pct = journey_completeness_pct(snap)
    assert 0.6 < pct < 0.7


def test_hard_cluster_coverage_pct_counts_clusters_with_at_least_one_applied_patch() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        hard_cluster_coverage_pct,
    )

    snap = _make_snapshot(
        applied_patches=[
            {"cluster_id": "H001", "proposal_id": "P1"},
        ],
    )
    assert hard_cluster_coverage_pct(snap) == pytest.approx(0.5)


def test_causal_patch_survival_pct_counts_applied_targeted_per_proposed() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        causal_patch_survival_pct,
    )

    snap = _make_snapshot(
        proposed_patches=[
            {"proposal_id": "P1", "target_qids": ["q1"]},
            {"proposal_id": "P2", "target_qids": ["q2"]},
            {"proposal_id": "P3", "target_qids": ["q3"]},
        ],
        applied_patches=[
            {"proposal_id": "P1", "target_qids": ["q1"]},
            {"proposal_id": "P2", "target_qids": ["q2"]},
        ],
    )
    assert causal_patch_survival_pct(snap) == pytest.approx(2 / 3)


def test_malformed_proposals_at_cap_passes_through_count() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        malformed_proposals_at_cap,
    )

    snap = _make_snapshot(malformed_proposals_at_cap_count=3)
    assert malformed_proposals_at_cap(snap) == 3


def test_rollback_attribution_complete_pct_requires_reason_and_class() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        rollback_attribution_complete_pct,
    )

    snap = _make_snapshot(
        rollback_records=[
            {"rollback_reason": "post_arbiter_not_improved",
             "rollback_class": "content_regression"},
            {"rollback_reason": "", "rollback_class": ""},
        ],
    )
    assert rollback_attribution_complete_pct(snap) == pytest.approx(0.5)


def test_terminal_unactionable_qids_returns_count() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        terminal_unactionable_qids,
    )

    snap = _make_snapshot(
        terminal_unactionable_qids={"q1", "q2"},
    )
    assert terminal_unactionable_qids(snap) == 2


def test_accuracy_delta_returns_signed_difference() -> None:
    from genie_space_optimizer.optimization.scoreboard import accuracy_delta

    snap = _make_snapshot(baseline_accuracy=0.50, candidate_accuracy=0.60)
    assert accuracy_delta(snap) == pytest.approx(0.10)

    snap2 = _make_snapshot(baseline_accuracy=0.60, candidate_accuracy=0.50)
    assert accuracy_delta(snap2) == pytest.approx(-0.10)


def test_trace_id_fallback_rate_metric_returns_recovered_over_total() -> None:
    from genie_space_optimizer.optimization.scoreboard import (
        trace_id_fallback_rate_metric,
    )

    snap = _make_snapshot(
        trace_id_fallback_recovered=2,
        trace_id_fallback_total=10,
    )
    assert trace_id_fallback_rate_metric(snap) == pytest.approx(0.2)

    snap2 = _make_snapshot(
        trace_id_fallback_recovered=0,
        trace_id_fallback_total=10,
    )
    assert trace_id_fallback_rate_metric(snap2) == pytest.approx(0.0)


def test_compute_scoreboard_returns_all_eight_metrics_and_dominant_signal() -> None:
    from genie_space_optimizer.optimization.scoreboard import compute_scoreboard

    # 7Now-style state: high journey completeness, hard cluster
    # coverage but causal_patch_survival < 0.5 and malformed proposals
    # at cap > 0 => GATE_OR_CAP_GAP dominates.
    snap = _make_snapshot(
        proposed_patches=[
            {"proposal_id": f"P{i}", "target_qids": [f"q{i}"]}
            for i in range(1, 4)
        ],
        applied_patches=[
            {"proposal_id": "P1", "target_qids": ["q1"]},
        ],
        malformed_proposals_at_cap_count=2,
    )
    sb = compute_scoreboard(snap)

    assert set(sb.keys()) >= {
        "journey_completeness_pct",
        "hard_cluster_coverage_pct",
        "causal_patch_survival_pct",
        "malformed_proposals_at_cap",
        "rollback_attribution_complete_pct",
        "terminal_unactionable_qids",
        "accuracy_delta",
        "trace_id_fallback_rate",
        "dominant_signal",
    }
    assert sb["dominant_signal"] == "GATE_OR_CAP_GAP", (
        f"expected GATE_OR_CAP_GAP for low-survival + malformed-at-cap "
        f"state; got {sb['dominant_signal']}"
    )


def test_compute_scoreboard_dominant_signal_evidence_gap_for_terminal_unactionable() -> None:
    from genie_space_optimizer.optimization.scoreboard import compute_scoreboard

    snap = _make_snapshot(
        terminal_unactionable_qids={"q1", "q2"},
        proposed_patches=[
            {"proposal_id": "P3", "target_qids": ["q3"]},
        ],
        applied_patches=[
            {"proposal_id": "P3", "target_qids": ["q3"]},
        ],
    )
    sb = compute_scoreboard(snap)
    assert sb["dominant_signal"] == "EVIDENCE_GAP"


def test_compute_scoreboard_dominant_signal_proposal_gap_when_no_proposals_for_hard_clusters() -> None:
    from genie_space_optimizer.optimization.scoreboard import compute_scoreboard

    snap = _make_snapshot(
        proposed_patches=[],
        applied_patches=[],
    )
    sb = compute_scoreboard(snap)
    assert sb["dominant_signal"] == "PROPOSAL_GAP"
