"""Cover retired-AG enumeration in the plateau resolver."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_terminal import (
    RcaTerminalStatus,
    resolve_terminal_on_plateau,
)


def _ag(ag_id: str, qids: list[str]) -> dict:
    """Build an AG dict in the same shape harness.py constructs.

    The resolver reads ``ag.get("id")`` (not ``"ag_id"``) when scanning
    for hard-qid overlap; it falls back to ``affected_questions`` when
    ``_stable_signature`` is absent.
    """
    return {
        "id": ag_id,
        "_stable_signature": (ag_id, tuple(sorted(qids))),
        "affected_questions": list(qids),
    }


def test_resolver_retires_ag_when_qids_no_longer_hard():
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"q1", "q2"},
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[
            _ag("AG_DEAD", ["q99"]),       # q99 not hard -> retired
            _ag("AG_LIVE", ["q1"]),        # q1 still hard -> resolver continues
        ],
    )
    assert decision.should_continue is True  # AG_LIVE keeps the loop alive
    assert decision.retired_ags == (("AG_DEAD", ("q99",)),)


def test_resolver_returns_clean_plateau_when_no_pending_ags():
    """No pending AGs + no hard failures + no debt → PLATEAU_NO_OPEN_FAILURES."""
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids=set(),
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[],
    )
    assert decision.status == RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES
    assert decision.should_continue is False
    assert decision.retired_ags == ()


def test_resolver_retires_ags_when_no_live_ags_remain_at_plateau():
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids=set(),
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[
            _ag("AG_X", ["q42"]),
            _ag("AG_Y", ["q43", "q44"]),
        ],
    )
    assert decision.should_continue is False
    # both AGs are retired because their qids no longer overlap the (empty) hard set
    assert dict(decision.retired_ags) == {
        "AG_X": ("q42",),
        "AG_Y": ("q43", "q44"),
    }


def test_resolver_handles_ags_without_stable_signature():
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"q1"},
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[
            {"id": "AG_NOSIG", "affected_questions": ["q1"]},  # no _stable_signature
        ],
    )
    assert decision.should_continue is True  # falls back to affected_questions
    assert decision.retired_ags == ()
