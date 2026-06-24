"""EvalRunner seam over the official Databricks Genie Benchmark (Eval-Run) API.

Phase 1 of GSO Optimizer v2 (see ``GSO_OPTIMIZER_V2_TODO.md`` §4 Phase 1).

This module introduces the :class:`EvalRunner` protocol — the single seam the
optimizer evaluates through — and :class:`OfficialBenchmarkRunner`, the
implementation that drives the native eval-run methods on the Databricks SDK
``GenieAPI`` (databricks-sdk v0.102.0)::

    w.genie.genie_create_eval_run(space_id, benchmark_question_ids=None)  # None ⇒ all
    w.genie.genie_get_eval_run(space_id, eval_run_id)                     # poll status
    w.genie.genie_list_eval_results(space_id, eval_run_id, page_size, page_token)
    w.genie.genie_get_eval_result_details(space_id, eval_run_id, result_id)

Decision **D1**: the official API is the SOLE eval runner in v2 — scoring is the
server-side ``assessment`` (GOOD/BAD/NEEDS_REVIEW), accuracy is
``num_correct / num_questions``, and lever routing reads ``assessment_reasons``.
We never double-run the retired in-process scorer path.

Result mapping reuses GSO's EXISTING flat per-question result-row dict shape (the
MLflow-flattened form consumed across ``evaluation.py`` / the harness) so no
parallel per-question schema is introduced. The run-level summary is returned in
:class:`EvalRunResult`.

Verified against the installed SDK (v0.102.0,
``databricks/sdk/service/dashboards.py``):

* ``genie_create_eval_run`` passes ``benchmark_question_ids`` through verbatim;
  ``None`` omits the field ⇒ the server evaluates every benchmark question.
* The status field is ``eval_run_status`` (type ``EvaluationStatusType``) on both
  ``GenieEvalRunResponse`` and ``GenieEvalResultDetails`` — *not* ``status`` as
  the planning note assumed.
* ``EvaluationStatusType`` ∈ {NOT_STARTED, RUNNING, DONE, EVALUATION_CANCELLED,
  EVALUATION_FAILED, EVALUATION_TIMEOUT}.
* ``genie_list_eval_results`` is paginated (``next_page_token``).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence, runtime_checkable

from genie_space_optimizer.common import config as _config
from genie_space_optimizer.optimization.genie_eval_taxonomy import (
    GENIE_REASON_RATIONALES,
    MAPPING_VERSION,
)

logger = logging.getLogger(__name__)

# ── Gate labels (mirror the existing ``eval_scope`` vocabulary) ────────────
SLICE = "slice"
P0 = "p0"
FULL = "full"

EVAL_SOURCE = "official_benchmark_api"

# ``EvaluationStatusType`` terminal values. Only ``DONE`` is a success.
_SUCCESS_STATUS = "DONE"
_TERMINAL_STATUSES = frozenset(
    {"DONE", "EVALUATION_CANCELLED", "EVALUATION_FAILED", "EVALUATION_TIMEOUT"}
)


# ── Eval wall-clock accumulator (budget guard, §3.4) ───────────────────────
# Sequential eval-runs ⇒ the budget is the sum of every run's wall-clock. The
# lever loop runs slice/P0/full evals inside one gate call across several
# return paths, so we accumulate the official eval-run wall-clock here (reset
# per iteration by the loop) rather than timing the whole gate call (which
# would also count propagation waits). Single-threaded loop ⇒ a module-level
# accumulator is safe.
_EVAL_WALL_CLOCK = {"seconds": 0.0}


def reset_eval_wall_clock() -> None:
    """Reset the per-iteration official eval-run wall-clock accumulator."""
    _EVAL_WALL_CLOCK["seconds"] = 0.0


def record_eval_wall_clock(seconds: float) -> None:
    """Add one official eval-run's wall-clock to the accumulator."""
    _EVAL_WALL_CLOCK["seconds"] += max(0.0, float(seconds))


def accumulated_eval_wall_clock() -> float:
    """Total official eval-run wall-clock since the last reset."""
    return _EVAL_WALL_CLOCK["seconds"]


def _status_str(status: Any) -> str:
    """Normalise an ``EvaluationStatusType`` (enum or raw string) to upper-case."""
    return str(getattr(status, "value", status) or "").upper()


def _enum_value(item: Any) -> str:
    """Return ``.value`` for an SDK enum, else the stringified item."""
    return str(getattr(item, "value", item) or "")


class EvalRunError(RuntimeError):
    """Raised when an official eval-run cannot be completed."""


class EvalRunTimeoutError(EvalRunError):
    """Raised when an eval-run does not reach a terminal status in time."""

    def __init__(self, eval_run_id: str, last_status: str, timeout_seconds: float):
        super().__init__(
            f"eval-run {eval_run_id!r} did not finish within {timeout_seconds:.0f}s "
            f"(last status={last_status or 'UNKNOWN'})"
        )
        self.eval_run_id = eval_run_id
        self.last_status = last_status
        self.timeout_seconds = timeout_seconds


@dataclass
class EvalRunResult:
    """Run-level summary of a single official eval-run.

    ``rows`` carries the per-question verdicts mapped into GSO's existing flat
    result-row dict shape (see :func:`map_eval_detail_to_row`). ``accuracy`` is
    the server-side headline metric, ``num_correct / num_questions`` (0–100).
    """

    eval_run_id: str
    status: str
    num_correct: int
    num_done: int
    num_needs_review: int
    num_questions: int
    rows: list[dict]
    wall_clock_seconds: float
    eval_scope: str = FULL
    requested_question_ids: tuple[str, ...] | None = None  # None ⇒ all questions

    @property
    def accuracy(self) -> float:
        """Headline accuracy on a 0–100 scale (``num_correct / num_questions``)."""
        if self.num_questions <= 0:
            return 0.0
        return round(100.0 * self.num_correct / self.num_questions, 2)

    @property
    def accuracy_fraction(self) -> float:
        """Headline accuracy as a 0–1 fraction."""
        if self.num_questions <= 0:
            return 0.0
        return self.num_correct / self.num_questions

    @property
    def succeeded(self) -> bool:
        return self.status == _SUCCESS_STATUS

    @property
    def is_complete_success(self) -> bool:
        """True only for a fully-completed DONE run with at least one question.

        A non-DONE terminal status (EVALUATION_FAILED / CANCELLED / TIMEOUT), a
        partial run (``num_done < num_questions``), or an empty set must NEVER
        read as a passing gate — those fail closed in the output mapper.
        """
        return (
            self.status == _SUCCESS_STATUS
            and self.num_questions > 0
            and self.num_done >= self.num_questions
        )

    @property
    def failure_question_ids(self) -> list[str]:
        """Question ids whose assessment is not GOOD (BAD or NEEDS_REVIEW)."""
        out: list[str] = []
        for row in self.rows:
            if str(row.get("assessment", "")).upper() != "GOOD":
                qid = str(row.get("question_id") or "")
                if qid:
                    out.append(qid)
        return out


@runtime_checkable
class EvalRunner(Protocol):
    """The single seam the optimizer evaluates through.

    Implementations run a benchmark eval over ``space_id``. ``benchmark_question_ids``
    selects a subset (e.g. a slice or P0 gate); ``None`` evaluates every benchmark
    question in the space.
    """

    def run(
        self,
        space_id: str,
        benchmark_question_ids: Sequence[str] | None = None,
        *,
        eval_scope: str = FULL,
    ) -> EvalRunResult: ...


def _official_genie_eval_block(assessment: str, reasons: list[str]) -> dict[str, Any]:
    """Build the additive ``genie_equivalent_eval`` block from official outputs.

    Shaped to match ``genie_eval_taxonomy.build_genie_equivalent_eval`` so existing
    readers keep working, but sourced directly from the official API rather than
    reverse-mapped from a retired GSO judge.
    """
    primary = reasons[0] if reasons else None
    return {
        "assessment": assessment or "NEEDS_REVIEW",
        "primary_assessment_reason": primary,
        "assessment_reasons": list(reasons),
        "reason_family": "official_benchmark_api",
        "reason_rationales": {
            r: GENIE_REASON_RATIONALES[r] for r in reasons if r in GENIE_REASON_RATIONALES
        },
        "mapped_from": {"source": EVAL_SOURCE},
        "mapping_confidence": 1.0,
        "unmapped": False,
        "mapping_version": MAPPING_VERSION,
    }


def _first_response_text(responses: Any) -> str:
    """Return the first response payload's ``response`` text (the SQL/text body)."""
    if not responses:
        return ""
    first = responses[0]
    return str(getattr(first, "response", "") or "")


def map_eval_detail_to_row(summary: Any, detail: Any) -> dict[str, Any]:
    """Map an official eval result (summary + details) into a GSO flat row dict.

    Reuses the existing per-question row shape: ``result_correctness/value`` carries
    the pass/fail verdict (``yes`` for GOOD, ``no`` otherwise) under both the legacy
    and ``feedback/``-prefixed keys, while the native ``assessment`` /
    ``assessment_reasons`` are attached for the Phase-3 reason→lever repoint. No
    parallel schema is introduced.
    """
    qid = str(
        getattr(detail, "benchmark_question_id", "")
        or getattr(summary, "benchmark_question_id", "")
        or ""
    )
    question = str(getattr(summary, "question", "") or "")
    assessment = _enum_value(getattr(detail, "assessment", None)).upper() or "NEEDS_REVIEW"
    reasons = [_enum_value(r) for r in (getattr(detail, "assessment_reasons", None) or [])]

    actual_sql = _first_response_text(getattr(detail, "actual_response", None))
    expected_sql = _first_response_text(getattr(detail, "expected_response", None))

    is_correct = assessment == "GOOD"
    rc_value = "yes" if is_correct else "no"

    row: dict[str, Any] = {
        "question_id": qid,
        "inputs/question_id": qid,
        "question": question,
        # Native official outputs (the Phase-3 routing inputs).
        "assessment": assessment,
        "assessment_reasons": reasons,
        "manual_assessment": bool(getattr(detail, "manual_assessment", False) or False),
        "needs_review": assessment == "NEEDS_REVIEW",
        # Legacy verdict keys — both flattened forms recognised by ``_rc_str``.
        "result_correctness/value": rc_value,
        "feedback/result_correctness/value": rc_value,
        # Neutral arbiter verdict (the arbiter judge is retired; no rescue path).
        "arbiter/value": "skipped",
        # Generated / expected SQL in the existing response/expectations shape.
        "response": {"response": actual_sql, "comparison": {}},
        "expectations": {"expected_response": expected_sql},
        # ── Legacy flat aliases (row-schema compatibility, no downstream drift) ──
        # The active harness/state readers consume these flat keys, not the
        # nested forms: ``_get_question_text`` reads ``inputs/question``,
        # ``_get_genie_sql`` reads ``outputs/response``, ``_get_expected_sql``
        # reads ``inputs/expected_response``, and feature-mining reads
        # ``generated_sql`` / ``expected_sql``. Populate every one so official
        # rows are a drop-in for in-process rows.
        "inputs/question": question,
        "outputs/response": actual_sql,
        "inputs/expected_response": expected_sql,
        "generated_sql": actual_sql,
        "expected_sql": expected_sql,
        # Provenance.
        "_eval_source": EVAL_SOURCE,
        "result_id": str(getattr(detail, "result_id", "") or getattr(summary, "result_id", "") or ""),
        "genie_equivalent_eval": _official_genie_eval_block(assessment, reasons),
    }
    return row


class OfficialBenchmarkRunner:
    """:class:`EvalRunner` over the native Genie eval-run SDK methods.

    The runner is duck-typed over ``w.genie`` so unit tests can supply a fake
    client; the clock / sleep are injectable for deterministic poll-loop tests.
    """

    def __init__(
        self,
        w: Any,
        *,
        poll_interval_seconds: float | None = None,
        timeout_seconds: float | None = None,
        page_size: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        progress: Callable[[str, dict], None] | None = None,
    ) -> None:
        self._genie = w.genie
        self._poll_interval = float(
            poll_interval_seconds
            if poll_interval_seconds is not None
            else _config.EVAL_RUN_POLL_INTERVAL_SECONDS
        )
        self._timeout = float(
            timeout_seconds
            if timeout_seconds is not None
            else _config.EVAL_RUN_TIMEOUT_SECONDS
        )
        self._page_size = int(
            page_size if page_size is not None else _config.EVAL_RUN_PAGE_SIZE
        )
        self._clock = clock
        self._sleep = sleep
        self._progress = progress

    # -- public API --------------------------------------------------------
    def run(
        self,
        space_id: str,
        benchmark_question_ids: Sequence[str] | None = None,
        *,
        eval_scope: str = FULL,
    ) -> EvalRunResult:
        start = self._clock()
        qids: list[str] | None = (
            list(benchmark_question_ids)
            if benchmark_question_ids is not None
            else None
        )
        self._emit(
            "eval_run_create",
            {
                "space_id": space_id,
                "eval_scope": eval_scope,
                "question_count": (len(qids) if qids is not None else "all"),
            },
        )
        # ``benchmark_question_ids=None`` omits the field ⇒ server runs all.
        created = self._genie.genie_create_eval_run(
            space_id, benchmark_question_ids=qids
        )
        eval_run_id = str(getattr(created, "eval_run_id", "") or "")
        if not eval_run_id:
            raise EvalRunError(
                f"genie_create_eval_run returned no eval_run_id for space {space_id!r}"
            )

        summary = self._poll(space_id, eval_run_id)
        status = _status_str(getattr(summary, "eval_run_status", None))

        rows: list[dict] = []
        if status == _SUCCESS_STATUS:
            rows = self._collect_rows(space_id, eval_run_id)
        else:
            logger.warning(
                "eval-run %s reached non-success terminal status %s; "
                "returning empty result rows.",
                eval_run_id,
                status,
            )

        wall = self._clock() - start
        result = EvalRunResult(
            eval_run_id=eval_run_id,
            status=status,
            num_correct=int(getattr(summary, "num_correct", 0) or 0),
            num_done=int(getattr(summary, "num_done", 0) or 0),
            num_needs_review=int(getattr(summary, "num_needs_review", 0) or 0),
            num_questions=int(getattr(summary, "num_questions", 0) or 0),
            rows=rows,
            wall_clock_seconds=wall,
            eval_scope=eval_scope,
            requested_question_ids=(tuple(qids) if qids is not None else None),
        )
        self._emit(
            "eval_run_done",
            {
                "eval_run_id": eval_run_id,
                "status": status,
                "accuracy": result.accuracy,
                "num_correct": result.num_correct,
                "num_questions": result.num_questions,
                "wall_clock_seconds": round(wall, 1),
            },
        )
        return result

    # -- internals ---------------------------------------------------------
    def _poll(self, space_id: str, eval_run_id: str) -> Any:
        """Poll ``genie_get_eval_run`` until a terminal status or timeout."""
        deadline = self._clock() + self._timeout
        last_status = ""
        while True:
            summary = self._genie.genie_get_eval_run(space_id, eval_run_id)
            last_status = _status_str(getattr(summary, "eval_run_status", None))
            self._emit(
                "eval_run_poll",
                {
                    "eval_run_id": eval_run_id,
                    "status": last_status,
                    "num_done": int(getattr(summary, "num_done", 0) or 0),
                    "num_questions": int(getattr(summary, "num_questions", 0) or 0),
                },
            )
            if last_status in _TERMINAL_STATUSES:
                return summary
            now = self._clock()
            if now >= deadline:
                raise EvalRunTimeoutError(eval_run_id, last_status, self._timeout)
            # Never sleep past the deadline.
            self._sleep(min(self._poll_interval, max(0.0, deadline - now)))

    def _collect_rows(self, space_id: str, eval_run_id: str) -> list[dict]:
        """List every result (paginated) and fetch its details, mapped to rows."""
        rows: list[dict] = []
        page_token: str | None = None
        while True:
            page = self._genie.genie_list_eval_results(
                space_id,
                eval_run_id,
                page_size=self._page_size,
                page_token=page_token,
            )
            for summary in getattr(page, "eval_results", None) or []:
                detail = self._genie.genie_get_eval_result_details(
                    space_id, eval_run_id, summary.result_id
                )
                rows.append(map_eval_detail_to_row(summary, detail))
            page_token = getattr(page, "next_page_token", None)
            if not page_token:
                break
        return rows

    def _emit(self, event: str, fields: dict) -> None:
        if self._progress is not None:
            try:
                self._progress(event, fields)
            except Exception:  # pragma: no cover - progress must never break eval
                logger.debug("eval-run progress callback failed for %s", event)


def maybe_build_official_runner(
    w: Any,
    *,
    progress: Callable[[str, dict], None] | None = None,
) -> OfficialBenchmarkRunner | None:
    """Return an :class:`OfficialBenchmarkRunner` when it should be the active path.

    Returns ``None`` (⇒ caller keeps the legacy in-process path) when the feature
    switch ``USE_OFFICIAL_BENCHMARK_RUNNER`` is off or ``w`` is not a real
    :class:`~databricks.sdk.WorkspaceClient`. Gating on the concrete client type is
    what keeps the unit/integration suites — which pass ``MagicMock`` workspaces —
    on the legacy path while production (a real client) gets the official runner.
    """
    if not getattr(_config, "USE_OFFICIAL_BENCHMARK_RUNNER", True):
        return None
    try:
        from databricks.sdk import WorkspaceClient
    except Exception:  # pragma: no cover - SDK always present in deploy
        return None
    if not isinstance(w, WorkspaceClient):
        return None
    return OfficialBenchmarkRunner(w, progress=progress)


_EXPLICIT_SPACE_ID_KEYS = ("space_question_id", "genie_question_id", "benchmark_question_id")


def _space_question_text_to_id(w: Any, space_id: str) -> dict[str, str]:
    """Map normalized question text → space-side benchmark question id."""
    from genie_space_optimizer.common.genie_client import (
        _normalize_question_text,
        fetch_space_config,
    )

    cfg = fetch_space_config(w, space_id) or {}
    parsed = cfg.get("_parsed_space")
    if not isinstance(parsed, dict):
        parsed = cfg
    benchmarks = parsed.get("benchmarks") if isinstance(parsed, dict) else None
    questions = (benchmarks or {}).get("questions") if isinstance(benchmarks, dict) else None
    out: dict[str, str] = {}
    for q in questions or []:
        if not isinstance(q, dict):
            continue
        qid = str(q.get("id") or "")
        text = q.get("question")
        if isinstance(text, list):
            text = text[0] if text else ""
        key = _normalize_question_text(str(text or ""))
        if qid and key:
            out[key] = qid
    return out


def resolve_space_benchmark_qids(
    w: Any,
    space_id: str,
    benchmarks: Sequence[dict],
) -> list[str] | None:
    """Resolve GSO benchmark dicts to their space-side benchmark question ids.

    Phase 1 best-effort (robust push + ID resolution is **Phase 2**):

    1. An explicit space-side id already carried on the benchmark dict
       (``space_question_id`` / ``genie_question_id`` / ``benchmark_question_id``).
    2. Else match the benchmark's question text against the live space config's
       ``benchmarks.questions`` (read once via ``fetch_space_config``).

    **Resolution must be COMPLETE.** Scoring a subset of the requested set silently
    drops the rest and makes accuracy meaningless, so if ANY requested benchmark
    does not resolve we return ``None`` (after de-dup). The caller then falls back
    to the legacy in-process path — and crucially does so *before* creating any
    official eval-run, so this never causes a double-run. Robust/guaranteed
    resolution is Phase 2; a partial run is a Phase-1 correctness bug.

    Returns the complete list of resolved space-side ids in benchmark order
    (de-duplicated), or ``None`` when the set cannot be fully resolved.
    """
    if not benchmarks:
        return None

    text_map: dict[str, str] | None = None
    resolved: list[str] = []
    unresolved = 0
    from genie_space_optimizer.common.genie_client import _normalize_question_text

    for b in benchmarks:
        sid = ""
        for k in _EXPLICIT_SPACE_ID_KEYS:
            if b.get(k):
                sid = str(b[k])
                break
        if not sid:
            if text_map is None:
                try:
                    text_map = _space_question_text_to_id(w, space_id)
                except Exception:  # pragma: no cover - network/SDK failure ⇒ fall back
                    logger.exception("Could not read space benchmarks for %s", space_id)
                    text_map = {}
            key = _normalize_question_text(str(b.get("question") or b.get("text") or ""))
            sid = text_map.get(key, "")
        if sid:
            resolved.append(sid)
        else:
            unresolved += 1

    if unresolved:
        logger.info(
            "Official eval runner: %d of %d requested benchmark question(s) could "
            "not be resolved to space-side ids — incomplete resolution, falling "
            "back to in-process eval (no eval-run created). Phase 2 closes the "
            "guaranteed push/ID-resolution gap.",
            unresolved,
            len(benchmarks),
        )
        return None

    seen: set[str] = set()
    out: list[str] = []
    for s in resolved:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    # De-dup must not shrink the requested set into a partial run.
    return out or None


def build_eval_output_from_official(
    result: EvalRunResult,
    *,
    iteration: int,
    eval_scope: str,
    model_id: str | None = None,
) -> dict[str, Any]:
    """Map an :class:`EvalRunResult` into the legacy ``run_evaluation`` output dict.

    Keeps every key the harness / baseline / finalize consume so the official
    runner is a drop-in for the in-process path. Acceptance already gates on the
    post-arbiter accuracy delta (``acceptance_policy.decide_acceptance``), so the
    authoritative field is ``overall_accuracy``; per-judge thresholds are derived
    only from ``result_correctness`` here. Reworking acceptance / per-judge
    thresholds is **Phase 3** — this mapper deliberately does not touch them.

    **Fail-closed (D1):** a non-DONE / partial / empty run NEVER reads as a
    passing gate. Such a result maps to accuracy ``0``, every requested question
    id as a failure, and ``thresholds_met=False`` so the slice / P0 / full gates
    all reject and the iteration rolls back.
    """
    from genie_space_optimizer.optimization.evaluation import (
        all_thresholds_met,
        normalize_scores,
    )

    # Sequential eval-runs ⇒ accumulate wall-clock for the budget guard (§3.4).
    record_eval_wall_clock(result.wall_clock_seconds)

    if not result.is_complete_success:
        # Map ALL requested ids to failures (fall back to row ids, else what the
        # server reported done) so downstream gates see a failed — not green —
        # eval. Never silently pass a failed/partial/empty run.
        failure_ids = list(result.requested_question_ids or [])
        if not failure_ids:
            failure_ids = result.failure_question_ids or [
                str(r.get("question_id") or "") for r in result.rows if r.get("question_id")
            ]
        zero_scores = normalize_scores({"result_correctness": 0.0})
        zero_scores["_pre_arbiter/result_correctness"] = 0.0
        zero_scores["_pre_arbiter/overall_accuracy"] = 0.0
        logger.warning(
            "Official eval-run %s did not complete cleanly (status=%s, "
            "num_done=%d/%d) — failing the gate closed (accuracy=0, %d failures).",
            result.eval_run_id,
            result.status,
            result.num_done,
            result.num_questions,
            len(failure_ids),
        )
        return {
            "run_id": f"official-eval:{result.eval_run_id}",
            "mlflow_run_id": "",
            "run_name": f"iter_{iteration:02d}_{eval_scope}_official_failed",
            "experiment_id": "",
            "iteration": iteration,
            "overall_accuracy": 0.0,
            "pre_arbiter_accuracy": 0.0,
            "total_questions": result.num_questions or len(failure_ids),
            "evaluated_count": result.num_questions or len(failure_ids),
            "correct_count": 0,
            "both_correct_count": 0,
            "both_correct_rate": 0.0,
            "scores": zero_scores,
            "thresholds_met": False,
            "thresholds_passed": False,
            "per_judge": {"result_correctness": 0.0},
            "failures": failure_ids,
            "failure_question_ids": failure_ids,
            "remaining_failures": failure_ids,
            "arbiter_verdicts": {},
            "arbiter_actions": [],
            "model_id": model_id,
            "rows": result.rows,
            "trace_map": {},
            "excluded_count": 0,
            "row_exclusions": [],
            "arbiter_overridden_qids": [],
            "soft_signal_qids": [],
            "eval_run_id": result.eval_run_id,
            "eval_run_status": result.status,
            "eval_run_failed": True,
            "num_done": result.num_done,
            "num_needs_review": result.num_needs_review,
            "_eval_source": EVAL_SOURCE,
            "_eval_wall_clock_seconds": result.wall_clock_seconds,
            "eval_scope": eval_scope,
        }

    frac = result.accuracy_fraction
    per_judge = {"result_correctness": frac}
    scores_100 = normalize_scores(per_judge)
    # Parity with the in-process payload: pre-arbiter == post-arbiter (no arbiter).
    scores_100["_pre_arbiter/result_correctness"] = scores_100.get("result_correctness", 0.0)
    scores_100["_pre_arbiter/overall_accuracy"] = scores_100.get("result_correctness", 0.0)
    thresholds_passed = all_thresholds_met(scores_100)

    failure_ids = result.failure_question_ids

    return {
        "run_id": f"official-eval:{result.eval_run_id}",
        "mlflow_run_id": "",
        "run_name": f"iter_{iteration:02d}_{eval_scope}_official",
        "experiment_id": "",
        "iteration": iteration,
        "overall_accuracy": result.accuracy,
        "pre_arbiter_accuracy": result.accuracy,
        "total_questions": result.num_questions,
        "evaluated_count": result.num_done or result.num_questions,
        "correct_count": result.num_correct,
        "both_correct_count": result.num_correct,
        "both_correct_rate": result.accuracy,
        "scores": scores_100,
        "thresholds_met": thresholds_passed,
        "thresholds_passed": thresholds_passed,
        "per_judge": per_judge,
        "failures": failure_ids,
        "failure_question_ids": failure_ids,
        "remaining_failures": failure_ids,
        "arbiter_verdicts": {},
        "arbiter_actions": [],
        "model_id": model_id,
        "rows": result.rows,
        "trace_map": {},
        "excluded_count": 0,
        "row_exclusions": [],
        "arbiter_overridden_qids": [],
        "soft_signal_qids": [],
        # Native official run-level fields (additive — consumed by Phase 6 UI).
        "eval_run_id": result.eval_run_id,
        "eval_run_status": result.status,
        "eval_run_failed": False,
        "num_done": result.num_done,
        "num_needs_review": result.num_needs_review,
        "_eval_source": EVAL_SOURCE,
        "_eval_wall_clock_seconds": result.wall_clock_seconds,
        "eval_scope": eval_scope,
    }
