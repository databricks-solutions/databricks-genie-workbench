"""Tests for Task 11: soft-cluster damping multiplier removal.

Pre-Task-11, ``cluster_impact`` multiplied soft-signal clusters by
0.5× unless they were re-elevated (T1.12). With Task 6 stamping
typed ``SqlDiff`` on every confirmed failure and Task 7's ranking
tiebreaker preferring SQL_SHAPE > NL_TEXT and ``hard > soft`` at
equal impact, damping has no remaining purpose; it just creates a
class of clusters that can never win the rank without external help.

These tests pin the contract that:

* A soft cluster's raw ``impact_score`` equals an otherwise-identical
  hard cluster's score (no 0.5× multiplier anymore).
* At equal impact, the Task 7 tiebreaker still ranks the hard
  cluster first.
* Re-elevation is a no-op — it does not change the impact score for
  either soft or hard clusters.
* The signal-quality dampen (T1.3, kept) still attenuates clusters
  built from low-trust heuristic attribution.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.optimizer import (
    cluster_impact,
    rank_clusters,
)


def _hard_cluster(**overrides) -> dict:
    base = {
        "cluster_id": "H001",
        "signal_type": "hard",
        "affected_judge": "schema_accuracy",
        "asi_failure_type": "wrong_table",
        "affected_judges": ["schema_accuracy"],
        "question_ids": ["q1", "q2", "q3"],
    }
    base.update(overrides)
    return base


def _soft_cluster(**overrides) -> dict:
    base = {
        "cluster_id": "S001",
        "signal_type": "soft",
        "affected_judge": "schema_accuracy",
        "asi_failure_type": "wrong_table",
        "affected_judges": ["schema_accuracy"],
        "question_ids": ["q1", "q2", "q3"],
    }
    base.update(overrides)
    return base


# ── Damping multiplier removed ────────────────────────────────


def test_soft_cluster_impact_no_longer_dampened():
    """Identical clusters apart from ``signal_type`` produce equal
    impact scores after Task 11."""
    hard = _hard_cluster()
    soft = _soft_cluster()

    assert cluster_impact(hard) == pytest.approx(cluster_impact(soft))


def test_reelevation_is_no_op_for_soft_cluster():
    """Pre-Task-11, ``reelevated=True`` would skip the 0.5× damping.
    After removal, the field has no behavioral effect."""
    soft_plain = _soft_cluster(reelevated=False)
    soft_reelevated = _soft_cluster(reelevated=True)

    assert cluster_impact(soft_plain) == pytest.approx(
        cluster_impact(soft_reelevated),
    )


def test_reelevation_field_is_no_op_for_hard_cluster_too():
    hard_plain = _hard_cluster(reelevated=False)
    hard_reelevated = _hard_cluster(reelevated=True)

    assert cluster_impact(hard_plain) == pytest.approx(
        cluster_impact(hard_reelevated),
    )


def test_question_count_still_drives_impact_linearly():
    small = _hard_cluster(question_ids=["q1"])
    large = _hard_cluster(question_ids=[f"q{i}" for i in range(10)])

    assert cluster_impact(large) == pytest.approx(
        10.0 * cluster_impact(small),
    )


# ── Tiebreaker still favours hard over soft ─────────────────


def test_at_equal_impact_hard_outranks_soft():
    hard = _hard_cluster(question_ids=["q1"])
    soft = _soft_cluster(question_ids=["q2"])

    ranked = rank_clusters([soft, hard])

    assert ranked[0]["cluster_id"] == "H001"
    assert ranked[1]["cluster_id"] == "S001"


def test_large_soft_cluster_can_now_outrank_small_hard_cluster():
    """The whole point of removing damping: a 12-question soft
    cluster (the retail ``response_quality=63%`` example) was being
    suppressed below a 2-question hard cluster. After Task 11, raw
    impact wins."""
    big_soft = _soft_cluster(
        question_ids=[f"q{i}" for i in range(12)],
        cluster_id="S_BIG",
    )
    tiny_hard = _hard_cluster(
        question_ids=["q_only"],
        cluster_id="H_SMALL",
    )

    ranked = rank_clusters([tiny_hard, big_soft])

    assert ranked[0]["cluster_id"] == "S_BIG"


# ── Signal-quality dampen retained (T1.3) ─────────────────


def test_signal_quality_dampen_still_attenuates_low_trust_clusters():
    high_trust = _hard_cluster(signal_quality={"combined": 1.0})
    low_trust = _hard_cluster(signal_quality={"combined": 0.0})

    impact_high = cluster_impact(high_trust)
    impact_low = cluster_impact(low_trust)

    # 1.0 → multiplier 1.0; 0.0 → multiplier 0.6.
    assert impact_high > impact_low
    assert impact_low == pytest.approx(0.6 * impact_high)


# ── Cross-class comparability ───────────────────────────


def test_impact_scores_are_directly_comparable_across_signal_classes():
    """A 10-question soft cluster has the same raw impact as a
    10-question hard cluster (everything else equal). Task 11 makes
    cluster_impact a single comparable scale."""
    big_soft = _soft_cluster(question_ids=[f"q{i}" for i in range(10)])
    big_hard = _hard_cluster(question_ids=[f"q{i}" for i in range(10)])

    assert cluster_impact(big_soft) == pytest.approx(cluster_impact(big_hard))
