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


def _row(qid: str, **judges: str) -> dict:
    """Build a minimal eval-row dict matching the keys ``row_qid`` and
    ``has_individual_judge_failure`` consume. Each kwarg becomes a
    ``feedback/<judge>/value`` entry (slash form), which the row routers
    treat as a scorer judge.
    """
    row: dict = {"question_id": qid}
    for judge, value in judges.items():
        row[f"feedback/{judge}/value"] = value
    return row


def test_assert_soft_cluster_currency_rejects_stale_asi_drift() -> None:
    """May-01 7Now reproducer: ``gs_001`` was a just-fixed target whose
    fresh rows pass every judge, yet the soft-clusterer still emitted it
    in cluster ``S003``. The new invariant must catch that as
    stale-ASI drift.
    """
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    fresh_rows = [
        _row(
            "gs_001",
            arbiter="both_correct",
            result_correctness="yes",
            completeness="yes",
            response_quality="yes",
            logical_accuracy="yes",
            schema_accuracy="yes",
            semantic_equivalence="yes",
            asset_routing="yes",
        ),
    ]
    with pytest.raises(
        AssertionError,
        match="no row in the current eval shows an actionable judge failure",
    ):
        assert_soft_cluster_currency(
            soft_cluster_qids={"gs_001"},
            current_eval_rows=fresh_rows,
        )


def test_assert_soft_cluster_currency_accepts_arbiter_rescued_judge_failure() -> None:
    """Case A: Genie's answer was correct enough that the arbiter rescued
    the row (``arbiter=both_correct``), but at least one non-info judge
    still flagged ``no``. The soft pile is built precisely to surface
    this pattern, so the assertion must accept it.
    """
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    fresh_rows = [
        _row(
            "gs_028",
            arbiter="both_correct",
            result_correctness="yes",
            completeness="no",
            response_quality="yes",
        ),
    ]
    assert_soft_cluster_currency(
        soft_cluster_qids={"gs_028"},
        current_eval_rows=fresh_rows,
    )


def test_assert_soft_cluster_currency_rejects_qid_with_no_row_in_current_eval() -> None:
    """A soft cluster lists a qid that has no row at all in the current
    eval source. That is the source-skew failure mode the helper guards
    against.
    """
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    fresh_rows = [
        _row("gs_002", arbiter="both_correct", completeness="no"),
    ]
    with pytest.raises(
        AssertionError,
        match="no row in the current eval shows an actionable judge failure",
    ):
        assert_soft_cluster_currency(
            soft_cluster_qids={"gs_001"},
            current_eval_rows=fresh_rows,
        )


def test_assert_soft_cluster_currency_accepts_empty_soft_cluster() -> None:
    """No soft clusters present means no invariant to check. The helper
    must short-circuit even when the eval-row source is empty.
    """
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    assert_soft_cluster_currency(
        soft_cluster_qids=set(),
        current_eval_rows=[],
    )


def test_assert_soft_cluster_currency_normalizes_vN_qid_suffixes() -> None:
    """Benchmark suffix variants (``q_002:v3``) collapse to their base
    qid for matching, mirroring the policy in ``_is_quarantined_qid``.
    """
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    fresh_rows = [
        _row(
            "gs_002",
            arbiter="both_correct",
            result_correctness="yes",
            completeness="no",
        ),
    ]
    assert_soft_cluster_currency(
        soft_cluster_qids={"gs_002:v3"},
        current_eval_rows=fresh_rows,
    )


def test_assert_soft_cluster_currency_ignores_info_only_judge_failures() -> None:
    """``repeatability`` and ``previous_sql`` are info-only judges
    (``INFO_ONLY_JUDGES``); their ``no`` values must not satisfy the
    invariant. A soft-cluster qid whose only ``no`` is ``repeatability``
    is still stale-ASI drift.
    """
    from genie_space_optimizer.optimization.control_plane import (
        assert_soft_cluster_currency,
    )

    fresh_rows = [
        _row(
            "gs_001",
            arbiter="both_correct",
            result_correctness="yes",
            completeness="yes",
            response_quality="yes",
            repeatability="no",
        ),
    ]
    with pytest.raises(
        AssertionError,
        match="no row in the current eval shows an actionable judge failure",
    ):
        assert_soft_cluster_currency(
            soft_cluster_qids={"gs_001"},
            current_eval_rows=fresh_rows,
        )
