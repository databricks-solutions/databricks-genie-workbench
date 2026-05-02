"""Track H — quarantine attribution audit."""
from __future__ import annotations

import pytest


def test_assert_quarantine_attribution_rejects_passing_qid() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        assert_quarantine_attribution_sound,
    )

    with pytest.raises(AssertionError, match="passing qids appear in quarantine"):
        assert_quarantine_attribution_sound(
            quarantined_qids={"gs_009"},
            currently_passing_qids={"gs_009", "gs_010"},
            currently_hard_qids={"gs_026"},
        )


def test_assert_quarantine_attribution_rejects_singleton_hard_quarantine() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        assert_quarantine_attribution_sound,
    )

    with pytest.raises(AssertionError, match="singleton-hard qid cannot be quarantined"):
        assert_quarantine_attribution_sound(
            quarantined_qids={"gs_026"},
            currently_passing_qids={"gs_001", "gs_002"},
            currently_hard_qids={"gs_026"},
        )


def test_assert_quarantine_attribution_accepts_clean_state() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        assert_quarantine_attribution_sound,
    )

    assert_quarantine_attribution_sound(
        quarantined_qids={"gs_005"},
        currently_passing_qids={"gs_001", "gs_002"},
        currently_hard_qids={"gs_005", "gs_026"},
    )


def test_assert_quarantine_attribution_accepts_empty_quarantine() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        assert_quarantine_attribution_sound,
    )

    assert_quarantine_attribution_sound(
        quarantined_qids=set(),
        currently_passing_qids={"gs_001"},
        currently_hard_qids={"gs_026"},
    )


def test_assert_soft_cluster_currency_rejects_passing_qid_in_soft_cluster() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    with pytest.raises(AssertionError, match="soft-cluster currency drift"):
        assert_soft_cluster_currency(
            soft_cluster_qids={"gs_001", "gs_028"},
            currently_passing_qids={"gs_001", "gs_002", "gs_003"},
        )


def test_assert_soft_cluster_currency_accepts_clean_state() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    assert_soft_cluster_currency(
        soft_cluster_qids={"gs_028"},
        currently_passing_qids={"gs_001", "gs_002"},
    )
