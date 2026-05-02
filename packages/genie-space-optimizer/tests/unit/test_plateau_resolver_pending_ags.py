"""Track G — plateau resolver must consult the pending diagnostic AG
queue. When any queued AG's signature still covers a live hard qid,
plateau cannot fire.
"""
from __future__ import annotations


def test_pending_diagnostic_ag_overlapping_live_hard_blocks_plateau() -> None:
    """Iter-3 of the 7Now run: gs_019 is hard, queued diagnostic AG
    AG_COVERAGE_H001 targets gs_019. The resolver must return
    should_continue=True so the loop tries the AG instead of
    terminating.
    """
    from genie_space_optimizer.optimization.rca_terminal import (
        resolve_terminal_on_plateau,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"gs_019"},
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[{
            "id": "AG_COVERAGE_H001",
            "_stable_signature": (
                ("plural_top_n_collapse|fact|year",),
                ("gs_019",),
                "plural_top_n_collapse",
            ),
            "affected_questions": ["gs_019"],
        }],
    )

    assert decision.should_continue is True, (
        f"plateau fired despite pending AG covering live hard qid; "
        f"got status={decision.status}, reason={decision.reason}"
    )
    assert "pending_diagnostic_ags" in decision.reason or "AG_COVERAGE_H001" in decision.reason


def test_pending_diagnostic_ag_with_no_live_overlap_does_not_block_plateau() -> None:
    """A queued AG whose qids no longer overlap the live hard set must
    not block plateau. Without this check, drained AGs would keep the
    loop alive forever.
    """
    from genie_space_optimizer.optimization.rca_terminal import (
        resolve_terminal_on_plateau,
        RcaTerminalStatus,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids=set(),
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[{
            "id": "AG_COVERAGE_H_OLD",
            "_stable_signature": (
                ("missing_filter|fact|year",),
                ("gs_999",),  # qid not in live hard set
                "missing_filter",
            ),
            "affected_questions": ["gs_999"],
        }],
    )

    assert decision.should_continue is False
    assert decision.status == RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES


def test_resolver_works_without_pending_diagnostic_ags_arg_for_backwards_compat() -> None:
    """The new parameter must default; existing callers continue to work."""
    from genie_space_optimizer.optimization.rca_terminal import (
        resolve_terminal_on_plateau,
        RcaTerminalStatus,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids=set(),
        regression_debt_qids=set(),
    )
    assert decision.status == RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES
