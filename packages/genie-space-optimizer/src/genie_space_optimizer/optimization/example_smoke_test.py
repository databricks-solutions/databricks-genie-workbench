"""Pre-promotion smoke test for example-SQL enrichment patches.

Runs a fast eval against baseline ``both_correct`` questions with a
*staged* config that has the candidate patch applied. Rejects the
patch if any baseline-correct question regresses by more than the
configured tolerance.

The eval orchestrator is injected (``run_eval_fn``) so callers — and
unit tests — control which evaluator is used. In production this is
``evaluation.run_evaluation`` adapted with the staged config; in tests
it's a fake that returns canned rows.
"""
from __future__ import annotations

import logging
import os
import random
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmokeTestResult:
    """Outcome of the pre-promotion smoke test.

    ``accept=True`` means the candidate patch is safe to apply.
    ``regressions`` counts baseline ``both_correct`` questions that
    became wrong in the staged eval. ``regression_pp`` is the
    percentage-point delta (regressions / sample_size * 100).
    """

    accept: bool
    reason: str
    regressions: int = 0
    sample_size: int = 0
    regression_pp: float = 0.0
    sampled_question_ids: tuple[str, ...] = ()


def _both_correct_qids(rows: list[dict]) -> list[str]:
    out: list[str] = []
    for row in rows or []:
        verdict = (
            (row.get("arbiter") or {}).get("value")
            if isinstance(row.get("arbiter"), dict)
            else row.get("arbiter/value") or row.get("feedback/arbiter/value")
        )
        qid = str(row.get("question_id") or "")
        if str(verdict or "") == "both_correct" and qid:
            out.append(qid)
    return out


def _verdict_of(row: dict) -> str:
    if isinstance(row.get("arbiter"), dict):
        return str(row["arbiter"].get("value") or "")
    return str(
        row.get("arbiter/value")
        or row.get("feedback/arbiter/value")
        or ""
    )


def run_pre_promotion_smoke_test(
    *,
    candidates: list[dict],
    baseline_both_correct_rows: list[dict],
    staged_config: dict,
    run_eval_fn: Callable[..., dict],
    seed: int = 42,
) -> SmokeTestResult:
    """Stage candidates into ``staged_config``, run eval, gate on regression.

    Parameters
    ----------
    candidates : list[dict]
        Example-SQL candidates to be pre-validated. Used by the
        injected ``run_eval_fn`` (and only by it) to construct the
        staged-config payload.
    baseline_both_correct_rows : list[dict]
        Rows from the baseline eval where the arbiter verdict was
        ``both_correct``. These are the ground truth for "behaviour
        we must not regress".
    staged_config : dict
        Pre-built staged config (with the candidates already merged in
        memory by the caller). Passed through to ``run_eval_fn``.
    run_eval_fn : callable
        Injected eval runner. Must accept ``staged_config``,
        ``question_ids``, and ``baseline_rows`` kwargs and return a
        dict with key ``"rows"`` whose entries carry ``question_id``
        and an arbiter-style verdict field.
    seed : int
        RNG seed for question sampling when the pool exceeds
        ``GSO_EXAMPLE_SQL_SMOKE_MAX_QUESTIONS``.
    """
    from genie_space_optimizer.common.config import (
        EXAMPLE_SQL_SMOKE_MAX_QUESTIONS,
        EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP,
    )

    tolerance_pp = float(
        os.environ.get(
            "GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP",
            str(EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP),
        )
    )

    qids = _both_correct_qids(baseline_both_correct_rows)
    if not qids:
        return SmokeTestResult(accept=True, reason="no_baseline_pool")

    rng = random.Random(seed)
    if len(qids) > EXAMPLE_SQL_SMOKE_MAX_QUESTIONS:
        sampled = sorted(rng.sample(qids, EXAMPLE_SQL_SMOKE_MAX_QUESTIONS))
    else:
        sampled = sorted(qids)

    sampled_set = set(sampled)
    eval_pool = [
        row for row in baseline_both_correct_rows
        if str(row.get("question_id") or "") in sampled_set
    ]

    try:
        result = run_eval_fn(
            staged_config=staged_config,
            question_ids=sampled,
            baseline_rows=eval_pool,
            candidates=candidates,
        )
    except Exception as exc:
        logger.warning("smoke test eval crashed: %s", exc)
        return SmokeTestResult(
            accept=False,
            reason=f"eval_error: {exc}",
            sample_size=len(sampled),
            sampled_question_ids=tuple(sampled),
        )

    rows = (result or {}).get("rows") or []
    by_qid = {str(r.get("question_id") or ""): _verdict_of(r) for r in rows}

    regressions = 0
    for qid in sampled:
        new_verdict = by_qid.get(qid, "")
        if new_verdict != "both_correct":
            regressions += 1

    sample_size = len(sampled)
    regression_pp = (regressions / sample_size) * 100 if sample_size else 0.0

    if regression_pp > tolerance_pp:
        return SmokeTestResult(
            accept=False,
            reason=(
                f"regression_pp={regression_pp:.2f} > "
                f"tolerance_pp={tolerance_pp:.2f}"
            ),
            regressions=regressions,
            sample_size=sample_size,
            regression_pp=regression_pp,
            sampled_question_ids=tuple(sampled),
        )

    return SmokeTestResult(
        accept=True,
        reason="within_tolerance",
        regressions=regressions,
        sample_size=sample_size,
        regression_pp=regression_pp,
        sampled_question_ids=tuple(sampled),
    )
