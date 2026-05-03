"""AG-outcome journey-emit helper.

Stamps the AG-level outcome event (one of ``ACCEPTED``,
``ACCEPTED_WITH_REGRESSION_DEBT``, ``ROLLED_BACK``) for every qid the
AG targeted. Owned here as part of Phase D Harness Extractions Phase 1;
previously lived in ``harness.py``.

The function is intentionally pure: takes an ``emit`` callable and
plain-data inputs, performs no I/O, and returns ``None``. Unknown
outcome strings are silently dropped (defensive — the harness should
never call with one).
"""
from __future__ import annotations


def _emit_ag_outcome_journey(
    *,
    emit,
    ag_id: str,
    outcome: str,
    affected_qids,
) -> None:
    """Stamp the AG-level outcome event for every qid the AG targeted.

    ``outcome`` must be one of ``accepted``, ``accepted_with_regression_debt``,
    or ``rolled_back``.
    """
    if outcome not in (
        "accepted", "accepted_with_regression_debt", "rolled_back",
    ):
        return
    qids = [str(q) for q in (affected_qids or []) if q]
    if not qids:
        return
    emit(outcome, question_ids=qids, ag_id=ag_id)
