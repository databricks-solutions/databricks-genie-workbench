"""Tests for _call_llm_for_strategy_focused (Phase 2 Action 2.4).

The focused call carries only the uncovered clusters and prepends an
explicit addendum to the strategist prompt asking for either an AG per
uncovered cluster or an explanation."""

from __future__ import annotations

from unittest.mock import patch


def test_focused_addendum_text_lists_uncovered_cluster_ids() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        build_strategy_recall_addendum,
    )

    text = build_strategy_recall_addendum(
        uncovered_cluster_ids=("H002", "H003"),
    )
    assert "previously emitted no AG" in text
    assert "H002" in text
    assert "H003" in text


def test_focused_addendum_text_handles_single_uncovered() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        build_strategy_recall_addendum,
    )

    text = build_strategy_recall_addendum(uncovered_cluster_ids=("H002",))
    assert "H002" in text


def test_focused_addendum_text_raises_on_empty_uncovered() -> None:
    """Caller must not invoke the focused call with zero uncovered
    clusters; raise to surface the bug."""
    import pytest

    from genie_space_optimizer.optimization.optimizer import (
        build_strategy_recall_addendum,
    )

    with pytest.raises(ValueError):
        build_strategy_recall_addendum(uncovered_cluster_ids=())


def test_focused_call_passes_only_uncovered_clusters_to_underlying_strategist() -> None:
    """The focused call must trim the cluster list to the uncovered
    subset before delegating to _call_llm_for_strategy."""
    from genie_space_optimizer.optimization import optimizer as opt

    clusters = [
        {"cluster_id": "H001"},
        {"cluster_id": "H002"},
        {"cluster_id": "H003"},
    ]
    captured: dict[str, list] = {}

    def _fake_strategy(
        clusters, soft_signal_clusters, metadata_snapshot, w=None,
    ):
        captured["clusters"] = list(clusters)
        captured["soft_signal_clusters"] = list(soft_signal_clusters)
        return {"action_groups": []}

    with patch.object(opt, "_call_llm_for_strategy", side_effect=_fake_strategy):
        opt.call_llm_for_strategy_focused(
            clusters=clusters,
            uncovered_cluster_ids=("H002", "H003"),
            soft_signal_clusters=[],
            metadata_snapshot={},
            w=None,
        )

    captured_ids = sorted(c["cluster_id"] for c in captured["clusters"])
    assert captured_ids == ["H002", "H003"]


def test_focused_call_returns_strategy_dict_from_underlying_strategist() -> None:
    from genie_space_optimizer.optimization import optimizer as opt

    clusters = [{"cluster_id": "H002"}]

    def _fake_strategy(*args, **kwargs):
        return {"action_groups": [{"id": "AG_recall_1"}]}

    with patch.object(opt, "_call_llm_for_strategy", side_effect=_fake_strategy):
        result = opt.call_llm_for_strategy_focused(
            clusters=clusters,
            uncovered_cluster_ids=("H002",),
            soft_signal_clusters=[],
            metadata_snapshot={},
            w=None,
        )

    assert result["action_groups"] == [{"id": "AG_recall_1"}]
