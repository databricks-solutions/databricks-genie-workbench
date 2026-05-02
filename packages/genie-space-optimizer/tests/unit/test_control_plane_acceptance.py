"""Track F — acceptance predicate must accept a candidate that improves
overall accuracy with zero regressions on every budget axis, even when
the named target qid did not specifically move."""
from __future__ import annotations

# IMPORTANT — field-name discipline. ``row_is_hard_failure`` reads via
# ``_arbiter_value`` and ``_result_correctness_value`` in
# ``optimization/control_plane.py:192-207``. Those helpers check the
# following keys (in priority order):
#
#   arbiter:           feedback/arbiter/value -> arbiter/value -> arbiter
#   result correctness: feedback/result_correctness/value
#                       -> result_correctness/value -> result_correctness
#
# ``row_is_hard_failure`` returns True when arbiter is empty OR not in
# the "correct" set (``both_correct``, ``genie_correct``, ``correct``)
# AND result_correctness is in the "incorrect" set (``no``, ``false``,
# ``incorrect``). ``_row(hard=True)`` MUST satisfy that exact predicate.
# Read the helpers above before changing this stub. Step 1 below
# self-checks the stubs by asserting ``hard_failure_qids`` returns the
# expected qids before exercising the predicate.


def _row(qid: str, *, hard: bool) -> dict:
    """Construct an eval-row stub matching the shape ``hard_failure_qids`` reads.

    Uses the namespaced ``feedback/...`` keys that ``_arbiter_value`` and
    ``_result_correctness_value`` check first, so the stub works even if
    the bare-key fallbacks change.
    """
    if hard:
        return {
            "question_id": qid,
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        }
    return {
        "question_id": qid,
        "feedback/result_correctness/value": "yes",
        "feedback/arbiter/value": "both_correct",
    }


def test_row_stubs_are_classified_correctly_by_hard_failure_qids() -> None:
    """Self-check: confirm the stubs in this module actually trigger the
    predicate the predicate-under-test reads. If this test fails, every
    other test in this file is silently a no-op — fix the stubs first.
    """
    from genie_space_optimizer.optimization.control_plane import (
        hard_failure_qids,
    )

    rows = [
        _row("q_hard_1", hard=True),
        _row("q_hard_2", hard=True),
        _row("q_pass_1", hard=False),
    ]
    assert hard_failure_qids(rows) == ("q_hard_1", "q_hard_2"), (
        "stub _row() does not trigger row_is_hard_failure; check field "
        "names against control_plane._arbiter_value / "
        "_result_correctness_value"
    )


def test_acceptance_accepts_net_positive_zero_regression_target_unchanged() -> None:
    """The May-01 7Now iter-2 scenario: target qid did not move but the
    candidate fixed other qids and produced zero regressions on every
    budget axis. Must accept with reason ``accepted_with_attribution_drift``."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        _row("gs_013", hard=True),  # named target — stays hard
        _row("gs_019", hard=True),  # other hard — gets fixed
        _row("gs_001", hard=False),
        _row("gs_002", hard=False),
    ]
    post_rows = [
        _row("gs_013", hard=True),  # target unchanged
        _row("gs_019", hard=False),  # fixed
        _row("gs_001", hard=False),
        _row("gs_002", hard=False),
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=85.0,
        candidate_accuracy=95.0,
        target_qids=("gs_013",),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    assert decision.accepted is True, (
        f"net-positive zero-regression candidate must accept; "
        f"got reason_code={decision.reason_code}"
    )
    assert decision.reason_code == "accepted_with_attribution_drift"
    assert decision.target_fixed_qids == ()
    assert decision.out_of_target_regressed_qids == ()


def test_acceptance_still_rejects_when_passing_to_hard_regression_present() -> None:
    """Track F is narrow: if any regression budget is non-zero, the
    existing ``target_qids_not_improved`` rejection still fires when the
    target did not move."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        _row("gs_013", hard=True),
        _row("gs_019", hard=True),
        _row("gs_001", hard=False),
    ]
    post_rows = [
        _row("gs_013", hard=True),  # target unchanged
        _row("gs_019", hard=False),  # fixed
        _row("gs_001", hard=True),  # passing-to-hard regression
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=85.0,
        candidate_accuracy=88.0,
        target_qids=("gs_013",),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )
    assert decision.accepted is False
    assert decision.reason_code == "target_qids_not_improved"


def test_acceptance_still_accepts_when_target_fixed_and_zero_regressions() -> None:
    """Existing happy path preserved."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [_row("gs_013", hard=True), _row("gs_019", hard=False)]
    post_rows = [_row("gs_013", hard=False), _row("gs_019", hard=False)]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=50.0,
        candidate_accuracy=100.0,
        target_qids=("gs_013",),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted"
    assert decision.target_fixed_qids == ("gs_013",)
