"""Phase 2.1 — CLUSTER_BLOCKED_NO_RCA as a hard routing constraint."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.blocked_cluster_filter import (
    filter_clusters_blocked_no_rca,
    blocked_cluster_ids_from_records,
)


def test_no_records_no_clusters_filtered():
    clusters = [{"cluster_id": "c1"}, {"cluster_id": "c2"}]
    result = filter_clusters_blocked_no_rca(
        clusters=clusters,
        decision_records_this_iter=[],
    )
    assert result == clusters


def test_records_for_other_record_type_not_filtered():
    """A record_type != 'cluster_blocked_no_rca' does NOT filter."""
    clusters = [{"cluster_id": "c1"}, {"cluster_id": "c2"}]
    records = [{"record_type": "ag_emitted", "cluster_id": "c1"}]
    result = filter_clusters_blocked_no_rca(
        clusters=clusters,
        decision_records_this_iter=records,
    )
    assert result == clusters


def test_blocked_cluster_removed():
    clusters = [{"cluster_id": "c1"}, {"cluster_id": "c2"}]
    records = [
        {"record_type": "no_rca_ground", "cluster_id": "c1"},
    ]
    result = filter_clusters_blocked_no_rca(
        clusters=clusters,
        decision_records_this_iter=records,
    )
    assert [c["cluster_id"] for c in result] == ["c2"]


def test_multiple_blocked_all_removed():
    clusters = [{"cluster_id": "c1"}, {"cluster_id": "c2"}, {"cluster_id": "c3"}]
    records = [
        {"record_type": "no_rca_ground", "cluster_id": "c1"},
        {"record_type": "no_rca_ground", "cluster_id": "c3"},
    ]
    result = filter_clusters_blocked_no_rca(
        clusters=clusters,
        decision_records_this_iter=records,
    )
    assert [c["cluster_id"] for c in result] == ["c2"]


def test_blocked_cluster_ids_from_records_returns_set():
    records = [
        {"record_type": "no_rca_ground", "cluster_id": "c1"},
        {"record_type": "ag_emitted", "cluster_id": "c2"},
        {"record_type": "no_rca_ground", "cluster_id": "c3"},
    ]
    ids = blocked_cluster_ids_from_records(records)
    assert ids == frozenset({"c1", "c3"})
