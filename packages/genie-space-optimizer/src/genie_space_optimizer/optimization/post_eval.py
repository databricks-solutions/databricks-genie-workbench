"""Post-eval journey-emit helper.

Stamps the closing ``POST_EVAL`` event for every qid that entered the
iteration's eval set, with the transition classified as one of
``hold_pass`` / ``fail_to_pass`` / ``pass_to_fail`` / ``hold_fail``.
Owned here as part of Phase D Harness Extractions Phase 1; previously
lived in ``harness.py``.

The function is intentionally pure: takes an ``emit`` callable and
plain-data inputs, performs no I/O, and returns ``None``. The Phase B
journey-contract validator pins ``POST_EVAL`` as the closing event in
each qid's journey for an iteration; this helper is the only emitter
that produces it.
"""
from __future__ import annotations


def _emit_post_eval_journey(
    *,
    emit,
    eval_qids,
    was_passing_qids,
    is_passing_qids,
) -> None:
    """Stamp the closing post_eval event for every qid that entered eval.

    The transition is ``fail_to_pass``, ``pass_to_fail``, ``hold_pass``, or
    ``hold_fail``.
    """
    was = {str(q) for q in (was_passing_qids or []) if q}
    is_now = {str(q) for q in (is_passing_qids or []) if q}
    for q in eval_qids or []:
        qid = str(q)
        if not qid:
            continue
        prior = qid in was
        after = qid in is_now
        if prior and after:
            transition = "hold_pass"
        elif not prior and after:
            transition = "fail_to_pass"
        elif prior and not after:
            transition = "pass_to_fail"
        else:
            transition = "hold_fail"
        emit(
            "post_eval",
            question_id=qid,
            was_passing=prior,
            is_passing=after,
            transition=transition,
        )
