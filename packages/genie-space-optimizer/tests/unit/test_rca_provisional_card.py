"""Phase 2.2 — build_provisional_card from soft signals."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca_provisional_card import (
    build_provisional_card,
    ProvisionalCardEligibility,
)


_BASE_SOFT_SIGNAL = {
    "qid": "gs_001",
    "cluster_id": "c1",
    "root_cause_hint": "missing_metric_view",
    "confidence": 0.6,
}


def test_no_soft_signals_returns_none():
    card = build_provisional_card(
        cluster_id="c1",
        soft_signals_for_cluster=[],
    )
    assert card is None


def test_fewer_than_three_soft_signals_returns_none():
    card = build_provisional_card(
        cluster_id="c1",
        soft_signals_for_cluster=[_BASE_SOFT_SIGNAL] * 2,
    )
    assert card is None


def test_three_consistent_soft_signals_yields_provisional_card():
    signals = [
        {**_BASE_SOFT_SIGNAL, "qid": f"gs_00{i}"}
        for i in (1, 2, 3)
    ]
    card = build_provisional_card(
        cluster_id="c1",
        soft_signals_for_cluster=signals,
    )
    assert card is not None
    assert card["cluster_id"] == "c1"
    assert card["root_cause"] == "missing_metric_view"
    assert card["intended_patch_shape"] == "structural"
    assert card["provisional"] is True


def test_inconsistent_root_cause_hints_returns_none():
    """Three signals but pointing to different root causes →
    no consistent inference → no card."""
    signals = [
        {**_BASE_SOFT_SIGNAL, "qid": "gs_001",
         "root_cause_hint": "missing_metric_view"},
        {**_BASE_SOFT_SIGNAL, "qid": "gs_002",
         "root_cause_hint": "count_vs_distinct"},
        {**_BASE_SOFT_SIGNAL, "qid": "gs_003",
         "root_cause_hint": "row_ordering_drift"},
    ]
    card = build_provisional_card(
        cluster_id="c1",
        soft_signals_for_cluster=signals,
    )
    assert card is None


def test_target_qids_aggregated_from_signals():
    signals = [
        {**_BASE_SOFT_SIGNAL, "qid": f"gs_00{i}"}
        for i in (1, 2, 3)
    ]
    card = build_provisional_card(
        cluster_id="c1",
        soft_signals_for_cluster=signals,
    )
    assert card is not None
    assert set(card["target_qids"]) == {"gs_001", "gs_002", "gs_003"}


def test_eligibility_classifier():
    assert ProvisionalCardEligibility.QUORUM_REACHED == "quorum_reached"
    assert ProvisionalCardEligibility.QUORUM_NOT_REACHED == "quorum_not_reached"
    assert ProvisionalCardEligibility.INCONSISTENT_HINTS == "inconsistent_hints"
