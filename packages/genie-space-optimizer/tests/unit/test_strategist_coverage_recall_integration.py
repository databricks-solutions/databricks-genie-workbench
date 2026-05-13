"""Integration test for Section D strategist coverage recall — exercises
the helper that fuses recall AGs into the AG list, without invoking the
real LLM."""

from __future__ import annotations

from unittest.mock import patch


def test_call_focused_returns_empty_when_no_clusters_match_uncovered_ids() -> None:
    from genie_space_optimizer.optimization import optimizer as opt

    clusters = [{"cluster_id": "H001"}, {"cluster_id": "H_other"}]
    result = opt.call_llm_for_strategy_focused(
        clusters=clusters,
        uncovered_cluster_ids=("H_does_not_exist",),
        soft_signal_clusters=[],
        metadata_snapshot={},
        w=None,
    )
    assert result == {
        "action_groups": [],
        "rationale": "no eligible uncovered clusters",
    }


def test_call_focused_passes_addendum_via_metadata_snapshot() -> None:
    from genie_space_optimizer.optimization import optimizer as opt

    captured: dict = {}

    def _fake_strategy(clusters, soft_signal_clusters, metadata_snapshot, w=None):
        captured["metadata_snapshot"] = dict(metadata_snapshot)
        return {"action_groups": []}

    with patch.object(opt, "_call_llm_for_strategy", side_effect=_fake_strategy):
        opt.call_llm_for_strategy_focused(
            clusters=[{"cluster_id": "H002"}],
            uncovered_cluster_ids=("H002",),
            soft_signal_clusters=[],
            metadata_snapshot={"existing_key": "existing_value"},
            w=None,
        )

    assert "existing_key" in captured["metadata_snapshot"]
    addendum = captured["metadata_snapshot"]["_strategist_recall_addendum"]
    assert "STRATEGIST RECALL" in addendum
    assert "H002" in addendum


def test_call_focused_handles_underlying_strategist_returning_empty() -> None:
    from genie_space_optimizer.optimization import optimizer as opt

    with patch.object(
        opt, "_call_llm_for_strategy",
        return_value={"action_groups": [], "rationale": "LLM returned empty"},
    ):
        result = opt.call_llm_for_strategy_focused(
            clusters=[{"cluster_id": "H002"}],
            uncovered_cluster_ids=("H002",),
            soft_signal_clusters=[],
            metadata_snapshot={},
            w=None,
        )
    assert result["action_groups"] == []
