"""F-2 — RCA regeneration must actually try evidence sources.

Cycle 5 T3 wired the regen control-flow but the helper body was a
stub that returned ``attempted_evidence_sources=()``. Run
``833969815458299`` emitted ``rca_regeneration_exhausted`` for H002
with empty ``attempted_evidence_sources`` even though the run had
``failure_buckets`` and ``asi_metadata`` available. This file pins
the contract for the regenerated helper: failure_buckets first,
ASI as fallback, both attempts recorded.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_regenerate_rca_for_cluster_attempts_failure_buckets_first() -> None:
    """When called, the helper invokes build_rca_card with the
    failure_buckets pack first; if that returns an rca_id, attempted
    list is ('failure_buckets',) and rca_id is set."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )
    cluster = {
        "primary_cluster_id": "H002",
        "target_qids": ("gs_021",),
    }
    metadata_snapshot = {
        "_failure_buckets": {"gs_021": "missing_filter"},
        "_asi_metadata": {},
    }
    with patch(
        "genie_space_optimizer.optimization.rca.build_rca_card",
        return_value={"rca_id": "rca_h002_v2"},
    ) as build_mock:
        result = _regenerate_rca_for_cluster(
            spark=MagicMock(),
            run_id="run-x",
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
        )
    assert result == {
        "rca_id": "rca_h002_v2",
        "attempted_sources": ("failure_buckets",),
    }
    assert build_mock.call_count == 1


def test_regenerate_rca_for_cluster_falls_back_to_asi_when_buckets_empty() -> None:
    """If failure_buckets returns empty rca_id, fall back to ASI;
    attempted list is ('failure_buckets', 'asi')."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )
    cluster = {
        "primary_cluster_id": "H002",
        "target_qids": ("gs_021",),
    }
    metadata_snapshot = {
        "_failure_buckets": {"gs_021": "missing_filter"},
        "_asi_metadata": {"gs_021": {"hint": "time_window"}},
    }
    with patch(
        "genie_space_optimizer.optimization.rca.build_rca_card",
        side_effect=[
            {"rca_id": ""},  # failure_buckets attempt: empty
            {"rca_id": "rca_h002_asi"},  # asi attempt: success
        ],
    ) as build_mock:
        result = _regenerate_rca_for_cluster(
            spark=MagicMock(),
            run_id="run-x",
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
        )
    assert result == {
        "rca_id": "rca_h002_asi",
        "attempted_sources": ("failure_buckets", "asi"),
    }
    assert build_mock.call_count == 2


def test_regenerate_rca_for_cluster_exhausts_when_both_sources_empty() -> None:
    """Both attempts return empty rca_id → exhausted."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )
    cluster = {
        "primary_cluster_id": "H002",
        "target_qids": ("gs_021",),
    }
    metadata_snapshot = {
        "_failure_buckets": {"gs_021": "missing_filter"},
        "_asi_metadata": {"gs_021": {"hint": "time_window"}},
    }
    with patch(
        "genie_space_optimizer.optimization.rca.build_rca_card",
        return_value={"rca_id": ""},
    ) as build_mock:
        result = _regenerate_rca_for_cluster(
            spark=MagicMock(),
            run_id="run-x",
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
        )
    assert result == {
        "rca_id": "",
        "attempted_sources": ("failure_buckets", "asi"),
    }
    assert build_mock.call_count == 2
