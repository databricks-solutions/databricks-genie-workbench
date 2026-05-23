"""Eval-row admission helper.

Shared admission filter for the lever-loop / state-machine boundary. Until
2026-05-23 the harness pre-filter at ``harness.py:14870-14990`` excluded
quarantined qids, opt-out qids, and ground-truth-correction candidates
before feeding hard rows into the legacy Plan 11 dispatch, while the
state-machine admission at ``state_machine/transformers/dispatch_input.py``
only applied ``row_is_hard_failure``. The two paths disagreed on the hard
qid set, which surfaced as ``INPUT_PROJECTION_PARITY_PARTIAL_DRIFT`` in the
2026-05-23 trial postmortems.

This module centralises the filter so both consumers admit the exact same
rows. It is **pure** (no telemetry, no env access, no exceptions for
control flow) so it is safe to call repeatedly at admission sites.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from genie_space_optimizer.optimization._qid_extraction import (
    extract_question_id,
)
from genie_space_optimizer.optimization.evaluation import row_is_hard_failure


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    """Partitioning of eval rows produced by ``admit_eval_rows``.

    The lever loop and the state machine both consume ``hard_rows``.
    ``gt_correction_rows`` are diverted to the corpus-review queue.
    ``quarantined_rows`` / ``excluded_rows`` are returned for telemetry
    parity with the harness summary lines; they are never optimised.
    """

    hard_rows: tuple[dict, ...]
    gt_correction_rows: tuple[dict, ...]
    quarantined_rows: tuple[dict, ...]
    excluded_rows: tuple[dict, ...]
    soft_signal_rows: tuple[dict, ...]
    non_failing_rows: tuple[dict, ...]


def _is_quarantined_qid(qid: str, quarantined: frozenset[str]) -> bool:
    """Match base qid AND ``:vN`` suffix variants.

    Mirrors ``harness._is_quarantined_qid`` so quarantining ``_002``
    excludes ``_002:v1``, ``_002:v2`` from both paths.
    """
    if not qid:
        return False
    if qid in quarantined:
        return True
    if ":" in qid:
        base, _suffix = qid.split(":", 1)
        if base in quarantined:
            return True
    return False


def _row_is_gt_correction_candidate(row: Mapping) -> bool:
    """Cheap, side-effect-free version of ``is_gt_correction_candidate``.

    The harness's version raises ``ValueError`` for malformed rows; here
    we silently treat such rows as non-candidates because the harness
    skip-and-warn behaviour belongs in the harness, not in a pure
    admission helper. Callers that need the strict version can re-check
    after admission.
    """
    arbiter = row.get("feedback/arbiter/value") or row.get("arbiter") or ""
    return str(arbiter) == "genie_correct"


def _row_has_individual_judge_failure(row: Mapping) -> bool:
    """Soft-signal predicate — mirrors ``harness._has_individual_judge_failure``
    closely enough for partitioning. The legacy version inspects nested
    judge breakdowns; here we look at the most common keys.
    """
    rc = row.get("feedback/result_correctness/value") or row.get(
        "result_correctness",
    )
    return str(rc) == "no"


def admit_eval_rows(
    rows: Iterable[Mapping],
    *,
    quarantined: Sequence[str] = (),
    excluded: Sequence[str] = (),
) -> AdmissionResult:
    """Partition ``rows`` into the admission classes the optimiser uses.

    The returned ``hard_rows`` is the set every downstream optimiser
    consumer (legacy Plan 11 dispatch and the v4 state machine) should
    operate on. Filtering is intentionally stable: the input order is
    preserved within each class so replay fixtures remain deterministic.
    """
    q = frozenset(str(q) for q in (quarantined or ()))
    x = frozenset(str(q) for q in (excluded or ()))

    hard: list[dict] = []
    gt: list[dict] = []
    quarantined_out: list[dict] = []
    excluded_out: list[dict] = []
    soft: list[dict] = []
    non_failing: list[dict] = []

    for raw in rows or ():
        row = dict(raw)
        qid, _source = extract_question_id(row)

        if _is_quarantined_qid(qid, q):
            quarantined_out.append(row)
            continue
        if qid in x:
            excluded_out.append(row)
            continue
        if _row_is_gt_correction_candidate(row):
            gt.append(row)
            continue
        if row_is_hard_failure(row):
            hard.append(row)
            continue
        if _row_has_individual_judge_failure(row):
            soft.append(row)
            continue
        non_failing.append(row)

    return AdmissionResult(
        hard_rows=tuple(hard),
        gt_correction_rows=tuple(gt),
        quarantined_rows=tuple(quarantined_out),
        excluded_rows=tuple(excluded_out),
        soft_signal_rows=tuple(soft),
        non_failing_rows=tuple(non_failing),
    )


__all__ = ["AdmissionResult", "admit_eval_rows"]
