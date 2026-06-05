"""Plan 11 dispatch input adapter.

Replaces the legacy ``metadata_snapshot["_failing_qids"]`` derivation
that caused the Plan 12 ``failing_qids_count=0`` bug. Reads raw eval
rows directly via the same ``row_is_hard_failure`` predicate the
harness uses.
"""
from __future__ import annotations

import logging
from typing import Iterable, Mapping

from genie_space_optimizer.optimization._qid_extraction import (
    extract_question_id,
)
from genie_space_optimizer.optimization.evaluation import row_is_hard_failure
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
    build_initial_state,
)

logger = logging.getLogger(__name__)


def _row_eval_id(row: Mapping) -> str:
    # 2026-05-23 admission fix: prefer the explicit row/trace id when present,
    # else delegate to the canonical qid extractor so MLflow-flattened /
    # nested / request.kwargs qids work as a fallback identifier.
    explicit = row.get("eval_row_id") or row.get("trace_id")
    if explicit:
        return str(explicit)
    qid, _source = extract_question_id(dict(row))
    return qid


def _row_score(row: Mapping) -> float:
    """Trial 18 Step 2 — pre-apply baseline score for
    ``HardQidSeenRecord``.

    Routes through ``row_semantic_score`` so the pre-apply baseline
    used by ``acceptance_gate`` (via ``state.evaluated.pre_apply_score``)
    and the post-apply score (now also canonical via ``evaluated_gate``)
    are symmetric. Without this symmetry the gate compares apples and
    oranges: baseline raw byte-match vs post canonical semantic score.

    Falls back to the legacy ``row["score"]`` shape when
    ``GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0`` for rollback parity. Also
    falls back when ``row_semantic_score`` returns 0.0 *and* the row
    has an explicit ``score`` field — keeps any test fixture that pre-
    seeds ``score`` working.
    """
    try:
        from genie_space_optimizer.optimization.trial18_flags import (
            trial18_acceptance_overhaul_enabled,
        )
        if trial18_acceptance_overhaul_enabled():
            from genie_space_optimizer.optimization.evaluation import (
                row_semantic_score,
            )
            canonical = float(row_semantic_score(row))
            # If the canonical accessor extracted a real signal,
            # prefer it. Otherwise fall through to the legacy ``score``
            # shape (test fixtures sometimes pre-seed only this key).
            has_canonical_signal = any(
                k in row
                for k in (
                    "_is_semantic_correct",
                    "feedback/result_correctness/arbiter_override_value",
                    "feedback/arbiter/value",
                    "feedback/result_correctness/value",
                )
            )
            if has_canonical_signal:
                return canonical
    except Exception:
        # Importer/runtime errors must never crash the dispatch input
        # builder — fall through to legacy behaviour.
        pass

    try:
        return float(row.get("score", 0.0))
    except (TypeError, ValueError):
        return 0.0


def build_initial_states_from_eval_rows(
    eval_rows: Iterable[Mapping],
    *,
    iteration: int,
    quarantined: tuple[str, ...] = (),
    excluded: tuple[str, ...] = (),
) -> tuple[QuestionStateInIteration, ...]:
    """Return one QuestionStateInIteration per hard eval row.

    Delegates admission to the shared ``admit_eval_rows`` helper so the
    legacy Plan 11 dispatch and the v4 state machine agree byte-for-byte
    on the hard qid set (the 2026-05-23 trial surfaced this as
    ``INPUT_PROJECTION_PARITY_PARTIAL_DRIFT``).

    The helper is idempotent: callers that have already filtered may
    pass the hard rows directly with ``quarantined=()`` / ``excluded=()``
    and the result is unchanged. Callers that supply the raw lever-loop
    eval set must pass the quarantine / exclude sets so the SM admission
    matches the harness pre-filter.
    """
    from genie_space_optimizer.optimization.eval_row_admission import (
        admit_eval_rows,
    )

    admitted = admit_eval_rows(
        eval_rows or (),
        quarantined=quarantined,
        excluded=excluded,
    )

    states: list[QuestionStateInIteration] = []
    for row in admitted.hard_rows:
        qid, qid_source = extract_question_id(dict(row))
        if not qid:
            continue
        if qid_source == "trace_fallback":
            logger.warning(
                "SM admitting hard row via trace-id fallback "
                "(client_request_id=%s); expected canonical "
                "inputs/question_id from producer.",
                qid,
            )
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
