"""Per-question pass/fail transition tracking.

Aggregate averages hid that AG2 regressed post-arbiter accuracy on the
retail run: the optimization could be net-positive while flipping
specific qids from passing to failing. Per-question transitions make
the blast radius visible and let the acceptance gate reject any
bundle that breaks a previously-passing qid (unless the qid is
suppressed via the GT-correction queue or quarantine).

The four transitions:

* ``hold_pass``    — passing before, passing after.
* ``hold_fail``    — failing before, failing after.
* ``pass_to_fail`` — passing before, failing after. Non-suppressed
  ``pass_to_fail`` blocks acceptance.
* ``fail_to_pass`` — failing before, passing after. The reason we
  apply patches in the first place.

The module is leaf-level — pure helpers over pass maps. The state
writer for the matching Delta table lives in ``state.py``; the
acceptance wiring lives in ``harness.py::_run_gate_checks``.
"""

from __future__ import annotations

from dataclasses import dataclass


HOLD_PASS = "hold_pass"
HOLD_FAIL = "hold_fail"
PASS_TO_FAIL = "pass_to_fail"
FAIL_TO_PASS = "fail_to_pass"


@dataclass(frozen=True)
class RegressionVerdict:
    """Outcome of a per-question transition check.

    ``transitions`` is a per-qid map of one of the four labels.
    ``blocking_qids`` is the subset of ``pass_to_fail`` qids that are
    NOT in ``suppressed_qids`` — these are the qids whose regression
    rolls the AG back. ``fixed_qids`` is the ``fail_to_pass`` set.
    ``accepted`` is True iff ``blocking_qids`` is empty.
    """

    accepted: bool
    transitions: dict[str, str]
    blocking_qids: list[str]
    fixed_qids: list[str]


def compute_question_transitions(
    *,
    pass_map_before: dict[str, bool],
    pass_map_after: dict[str, bool],
    suppressed_qids: set[str] | None = None,
) -> RegressionVerdict:
    """Diff two pass maps and produce a typed verdict.

    Args:
        pass_map_before: ``{qid: passing}`` snapshot from the previous
            best eval. Missing qids are treated as ``False`` (never
            passed).
        pass_map_after: ``{qid: passing}`` snapshot from the post-AG
            eval. Drives the iteration set; only qids present here are
            classified.
        suppressed_qids: qids that should NOT block acceptance even on
            ``pass_to_fail`` — typically the union of the GT-correction
            queue and the iteration's quarantine set. Suppressed
            transitions are still recorded for review.
    """
    suppressed = suppressed_qids or set()
    transitions: dict[str, str] = {}
    blocking: list[str] = []
    fixed: list[str] = []

    for qid, after in pass_map_after.items():
        before = bool(pass_map_before.get(qid, False))
        after = bool(after)
        if before and after:
            transitions[qid] = HOLD_PASS
        elif not before and not after:
            transitions[qid] = HOLD_FAIL
        elif before and not after:
            transitions[qid] = PASS_TO_FAIL
            if qid not in suppressed:
                blocking.append(qid)
        else:  # not before, after
            transitions[qid] = FAIL_TO_PASS
            fixed.append(qid)

    return RegressionVerdict(
        accepted=not blocking,
        transitions=transitions,
        blocking_qids=blocking,
        fixed_qids=fixed,
    )
