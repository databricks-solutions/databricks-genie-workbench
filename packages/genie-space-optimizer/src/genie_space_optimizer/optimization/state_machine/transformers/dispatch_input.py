"""Plan 11 dispatch input adapter.

Replaces the legacy ``metadata_snapshot["_failing_qids"]`` derivation
that caused the Plan 12 ``failing_qids_count=0`` bug. Reads raw eval
rows directly via the same ``row_is_hard_failure`` predicate the
harness uses.
"""
from __future__ import annotations

from typing import Iterable, Mapping

from genie_space_optimizer.optimization.evaluation import row_is_hard_failure
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
    build_initial_state,
)


def _row_eval_id(row: Mapping) -> str:
    return str(row.get("eval_row_id") or row.get("trace_id") or row.get("question_id", ""))


def _row_score(row: Mapping) -> float:
    try:
        return float(row.get("score", 0.0))
    except (TypeError, ValueError):
        return 0.0


def build_initial_states_from_eval_rows(
    eval_rows: Iterable[Mapping],
    *,
    iteration: int,
) -> tuple[QuestionStateInIteration, ...]:
    """Return one QuestionStateInIteration per hard eval row.

    Uses ``row_is_hard_failure`` (optimization/evaluation.py:3649) so the
    state machine sees exactly the same hard set the harness sees.
    """
    states: list[QuestionStateInIteration] = []
    for row in eval_rows:
        if not row_is_hard_failure(dict(row)):
            continue
        qid = str(row.get("question_id", ""))
        if not qid:
            continue
        seen = HardQidSeenRecord(
            eval_row_id=_row_eval_id(row),
            predicate="row_is_hard_failure",
            score=_row_score(row),
            baseline_sql=str(row.get("sql", "")),
            expected_shape=str(row.get("expected_shape", "")),
            iteration_first_seen=iteration,
        )
        states.append(build_initial_state(qid=qid, iteration=iteration, seen=seen))
    return tuple(states)
