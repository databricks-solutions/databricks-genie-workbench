"""Cycle 13 — _reflection_admitted_to_forbidden_set predicate.

Pure-function unit tests for the admission hook. The hook is the
single extension point future cycles (C16-T2 STRUCTURAL_DROP, etc.)
add new branches to.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _reflection_admitted_to_forbidden_set,
)
from genie_space_optimizer.optimization.rollback_class import RollbackClass


def _entry(
    *,
    rollback_class: str,
    accepted: bool = False,
    lever_set: list[int] | None = None,
    root_cause: str = "missing_filter",
    escalation_handled: bool = False,
) -> dict:
    return {
        "rollback_class": rollback_class,
        "accepted": accepted,
        "lever_set": lever_set if lever_set is not None else [5],
        "root_cause": root_cause,
        "escalation_handled": escalation_handled,
        "blame_set": [],
    }


# ── Always-rejected branches ─────────────────────────────────────────


def test_escalation_handled_rejected() -> None:
    e = _entry(
        rollback_class=RollbackClass.CONTENT_REGRESSION.value,
        escalation_handled=True,
    )
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is False


def test_empty_lever_set_rejected() -> None:
    e = _entry(rollback_class=RollbackClass.CONTENT_REGRESSION.value, lever_set=[])
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is False


def test_empty_root_cause_rejected() -> None:
    e = _entry(rollback_class=RollbackClass.CONTENT_REGRESSION.value, root_cause="")
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is False


def test_unknown_class_rejected() -> None:
    e = _entry(rollback_class=RollbackClass.OTHER.value)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is False


# ── Always-admitted branches (existing) ──────────────────────────────


def test_content_regression_admitted_when_not_accepted() -> None:
    e = _entry(rollback_class=RollbackClass.CONTENT_REGRESSION.value, accepted=False)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=False) is True
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is True


def test_content_regression_rejected_when_accepted() -> None:
    e = _entry(rollback_class=RollbackClass.CONTENT_REGRESSION.value, accepted=True)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=False) is False


def test_accepted_with_debt_admitted_even_when_accepted() -> None:
    """Cycle 14B-T2 — the only RollbackClass that may carry
    accepted=True and still contribute to the forbidden set."""
    e = _entry(rollback_class=RollbackClass.ACCEPTED_WITH_DEBT.value, accepted=True)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=False) is True
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is True


# ── NO_ACTION branch (new in C13) — flag-gated ───────────────────────


def test_no_action_admitted_when_flag_on() -> None:
    e = _entry(rollback_class=RollbackClass.NO_ACTION.value, accepted=False)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is True


def test_no_action_rejected_when_flag_off() -> None:
    """Replay byte-stability: NO_ACTION is excluded from the
    forbidden set when the flag is off, matching the pre-C13
    behaviour where these reflections classified as OTHER and
    were rejected by the unknown-class branch."""
    e = _entry(rollback_class=RollbackClass.NO_ACTION.value, accepted=False)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=False) is False


def test_no_action_rejected_when_accepted_under_flag() -> None:
    """Defensive: even with the flag on, an accepted=True NO_ACTION
    reflection (no producer emits this today, but the type system
    allows it) is rejected — admission requires accepted=False
    OR rollback_class==ACCEPTED_WITH_DEBT."""
    e = _entry(rollback_class=RollbackClass.NO_ACTION.value, accepted=True)
    assert _reflection_admitted_to_forbidden_set(e, admit_no_action=True) is False
