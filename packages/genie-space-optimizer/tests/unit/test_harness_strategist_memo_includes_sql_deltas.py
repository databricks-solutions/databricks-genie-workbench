"""Pin sql-delta fingerprint in strategist memo key."""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import _strategist_memo_key


def test_memo_key_changes_with_deltas() -> None:
    clusters = [{"cluster_id": "H001", "question_ids": ["gs_017"]}]
    snapshot = {"revision": "rev1"}
    deltas_a = [{"target_qid": "gs_017", "improved": [], "remaining": ["date_window: 7_vs_30"]}]
    deltas_b = [{"target_qid": "gs_017", "improved": ["removed_filter: x='y'"], "remaining": []}]
    assert _strategist_memo_key(clusters, snapshot, sql_shape_deltas=deltas_a) != \
           _strategist_memo_key(clusters, snapshot, sql_shape_deltas=deltas_b)


def test_memo_key_stable_with_no_deltas() -> None:
    clusters = [{"cluster_id": "H001", "question_ids": ["gs_017"]}]
    snapshot = {"revision": "rev1"}
    assert _strategist_memo_key(clusters, snapshot, sql_shape_deltas=[]) == \
           _strategist_memo_key(clusters, snapshot, sql_shape_deltas=[])
