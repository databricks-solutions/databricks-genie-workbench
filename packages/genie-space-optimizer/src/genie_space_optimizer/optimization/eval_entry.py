"""Eval-entry journey-emit helper.

Stamps the per-iteration ``EVALUATED`` event followed by
``ALREADY_PASSING`` / ``SOFT_SIGNAL`` / ``GT_CORRECTION_CANDIDATE``
classification events. Owned here as part of Phase D Harness
Extractions Phase 1; previously lived in ``harness.py``.

The function is intentionally pure: takes an ``emit`` callable and
plain-data inputs, performs no I/O, and returns ``None``. The Phase B
journey-contract validator pins the ordering: ``EVALUATED`` must
appear before any of the three classification events, and
classifications are restricted to the ``eval_qids`` set.
"""
from __future__ import annotations


def _emit_eval_entry_journey(
    *,
    emit,
    eval_qids,
    already_passing_qids,
    hard_qids,
    soft_qids,
    gt_correction_qids,
) -> None:
    """Stamp the evaluated + classification events for every benchmark row.

    Called once per AG iteration immediately after _analyze_and_distribute.
    The hard set is *not* classified at entry — it gets a 'clustered' event
    later in the same iteration.
    """
    seen = {str(q) for q in eval_qids if q}
    if not seen:
        return
    emit("evaluated", question_ids=sorted(seen))
    for q in already_passing_qids or []:
        if str(q) in seen:
            emit("already_passing", question_id=str(q))
    for q in soft_qids or []:
        if str(q) in seen:
            emit("soft_signal", question_id=str(q))
    for q in gt_correction_qids or []:
        if str(q) in seen:
            emit("gt_correction_candidate", question_id=str(q))
