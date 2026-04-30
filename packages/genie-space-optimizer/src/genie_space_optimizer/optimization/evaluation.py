"""
Evaluation engine — predict function, shared helpers, MLflow integration,
and benchmark generation.

The central module for the quality measurement system. Provides:
  - ``make_predict_fn()``: factory closure binding workspace/spark context
  - Shared helpers used by all 8 scorers
  - ``run_evaluation()``: wraps ``mlflow.genai.evaluate()``
  - ``generate_benchmarks()``: LLM-powered benchmark creation
  - ``load_benchmarks_from_dataset()``: read from UC eval dataset
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import logging
import os
import re
import time
import traceback
from difflib import get_close_matches
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Iterator, Union

import mlflow
import pandas as pd
from mlflow.entities import AssessmentSource, Feedback, SpanType
from mlflow.genai.scorers import scorer

from genie_space_optimizer.optimization.genie_eval_taxonomy import (
    format_genie_eval_summary,
)

from genie_space_optimizer.common.config import (
    ASI_SCHEMA,
    BENCHMARK_CATEGORIES,
    BENCHMARK_CORRECTION_PROMPT,
    BENCHMARK_COVERAGE_GAP_PROMPT,
    BENCHMARK_GENERATION_PROMPT,
    BENCHMARK_PROMPTS,
    CODE_SOURCE_ID,
    COVERAGE_GAP_SOFT_CAP_FACTOR,
    DEFAULT_THRESHOLDS,
    FAILURE_TAXONOMY,
    INFO_ONLY_JUDGES,
    INSTRUCTION_PROMPT_ALIAS,
    INSTRUCTION_PROMPT_NAME_TEMPLATE,
    JUDGE_PROMPTS,
    LEVER_PROMPTS,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_SOURCE_ID_TEMPLATE,
    LLM_TEMPERATURE,
    MAX_BENCHMARK_COUNT,
    MLFLOW_THRESHOLDS,
    MODEL_NAME_TEMPLATE,
    PROMPT_ALIAS,
    PROMPT_NAME_TEMPLATE,
    BASELINE_RUN_NAME_TEMPLATE,
    RATE_LIMIT_SECONDS,
    RUN_NAME_TEMPLATE,
    TARGET_BENCHMARK_COUNT,
    TEMPLATE_VARIABLES,
    format_mlflow_template,
    scoring_v2_is_legacy,
    scoring_v2_is_on,
    scoring_v2_is_shadow,
)
from genie_space_optimizer.common.delta_helpers import retry_delta_write
from genie_space_optimizer.optimization.eval_progress import (
    EvalProgressLogger,
    eval_force_sequential,
    slice_eval_records_for_debug,
)
from genie_space_optimizer.common.genie_client import (
    detect_asset_type,
    fetch_genie_result_df,
    resolve_sql,
    run_genie_query,
    sanitize_sql,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

CODE_SOURCE = AssessmentSource(source_type="CODE", source_id=CODE_SOURCE_ID)
LLM_SOURCE = AssessmentSource(
    source_type="LLM_JUDGE",
    source_id=format_mlflow_template(LLM_SOURCE_ID_TEMPLATE, endpoint=LLM_ENDPOINT),
)


# ── Judge-failure predicates ──────────────────────────────────────────
#
# These were previously defined in ``harness.py`` but live next to
# ``run_evaluation`` because they are pure over a row dict and downstream
# modules (``ground_truth_corrections``) need to import them without
# pulling in ``harness``. ``harness`` retains thin re-exports under their
# legacy names so existing call sites are unaffected.

_NON_JUDGE_VALUE_SUFFIXES = ("/rationale", "/source", "/metadata", "/error")


def get_failed_judges(row: dict) -> list[str]:
    """Return scorer judge names whose ``value`` field contains ``"no"``.

    Tier 3.6: ``INFO_ONLY_JUDGES`` (e.g. ``repeatability``, ``previous_sql``)
    are excluded — they are diagnostic signals tracked separately, not
    drivers of clustering or soft-signal detection.
    """
    failed: list[str] = []
    for col, val in row.items():
        is_judge = False
        if col.startswith("feedback/") and col.endswith("/value"):
            is_judge = True
        elif col.startswith("feedback/") and not any(
            col.endswith(s) for s in _NON_JUDGE_VALUE_SUFFIXES
        ):
            if "/" not in col.removeprefix("feedback/"):
                is_judge = True
        elif col.endswith("/value") and not col.startswith("feedback/"):
            is_judge = True
        if is_judge and "no" in str(val).lower():
            judge_name = col.replace("feedback/", "").replace("/value", "")
            if judge_name in INFO_ONLY_JUDGES:
                continue
            failed.append(judge_name)
    return failed


def has_individual_judge_failure(row: dict) -> bool:
    """Return ``True`` when at least one non-info-only scorer judge failed.

    Used to detect rows where the arbiter rescued the row (or
    ``result_correctness=yes``) but individual judges still flagged
    suboptimal patterns worth learning from in the soft-signal pathway.
    """
    return len(get_failed_judges(row)) > 0


# ── ASI source telemetry (Task 0 Step 2) ─────────────────────────────────
#
# Each row that flows through the eval gets an ``_asi_source`` stamp from
# ``_merge_judge_assessments_into_row`` describing where its judge
# rationale/metadata came from. The retail run logged ``0`` recovered
# trace IDs across every eval pass — meaning ASI was running in row/UC
# fallback the entire run — but the pipeline had no structured telemetry
# to distinguish that from a healthy run. The dataclass + helpers below
# turn the per-row ``_asi_source`` strings into a per-iteration summary
# so the Task 3 decision-audit table can record it and downstream stages
# can dampen ``signal_quality.combined`` when ASI evidence is missing.

_ASI_SOURCE_TRACE_VALUES: frozenset[str] = frozenset({"trace", "recovered_trace"})
_ASI_SOURCE_ROW_PAYLOAD_VALUES: frozenset[str] = frozenset({"cache", "row_payload"})
_ASI_SOURCE_UC_VALUES: frozenset[str] = frozenset({"uc_metadata", "uc_cache"})
_ASI_SOURCE_KEY = "_asi_source"


@dataclass(frozen=True)
class AsiSourceCounts:
    """Per-iteration aggregate of where each row's ASI evidence came from.

    The four categories are disjoint; their sum equals the total
    classified rows. ``none`` counts rows where neither MLflow trace nor
    row payload nor UC metadata supplied ASI — those are the rows that
    silently slipped through the retail run.
    """

    trace: int = 0
    row_payload: int = 0
    uc_metadata: int = 0
    none: int = 0

    @property
    def total(self) -> int:
        return self.trace + self.row_payload + self.uc_metadata + self.none

    @property
    def coverage_ratio(self) -> float:
        """Fraction of rows that had any ASI evidence (trace ∪ payload ∪ UC)."""
        denom = self.total
        if denom == 0:
            return 0.0
        return (self.trace + self.row_payload + self.uc_metadata) / denom


def _classify_asi_source(row: dict[str, Any]) -> str:
    """Map a single row's ``_asi_source`` (or its absence) to a typed bucket.

    Returns one of ``"trace"``, ``"row_payload"``, ``"uc_metadata"``,
    ``"none"``. Pure over the row dict; no I/O.
    """
    raw = row.get(_ASI_SOURCE_KEY)
    if isinstance(raw, str) and raw:
        if raw in _ASI_SOURCE_TRACE_VALUES:
            return "trace"
        if raw in _ASI_SOURCE_ROW_PAYLOAD_VALUES:
            return "row_payload"
        if raw in _ASI_SOURCE_UC_VALUES:
            return "uc_metadata"
        # Unknown stamp — surface as a payload classification rather than
        # silently dropping the row from the summary.
        return "row_payload"
    # No stamp — check whether the row carries any judge metadata at all.
    for col in row:
        if isinstance(col, str) and col.startswith("feedback/") and col.endswith("/metadata"):
            val = row.get(col)
            if val:
                return "row_payload"
    return "none"


def compute_asi_source_summary(rows: list[dict[str, Any]]) -> AsiSourceCounts:
    """Aggregate per-row ASI source classifications into typed counts."""
    trace = row_payload = uc_metadata = none = 0
    for row in rows or []:
        bucket = _classify_asi_source(row)
        if bucket == "trace":
            trace += 1
        elif bucket == "row_payload":
            row_payload += 1
        elif bucket == "uc_metadata":
            uc_metadata += 1
        else:
            none += 1
    return AsiSourceCounts(
        trace=trace,
        row_payload=row_payload,
        uc_metadata=uc_metadata,
        none=none,
    )


def build_asi_extraction_audit_row(
    *,
    run_id: str,
    iteration: int,
    summary: AsiSourceCounts,
    trace_id_count: int | None = None,
    expected_trace_count: int | None = None,
) -> dict[str, Any]:
    """Build a Task-3 decision-audit row for ASI extraction telemetry.

    ``reason_code`` is one of:

    * ``asi_source_complete`` — every row had ASI evidence and at least one
      came from a trace.
    * ``asi_source_no_traces`` — no row sourced ASI from a trace, but every
      row was covered by row payload or UC metadata.
    * ``asi_source_partial`` — at least one row had no ASI evidence.

    The row is intentionally Delta-friendly: scalar fields plus a single
    JSON column (``metrics_json``) carrying the typed counts.
    """
    if summary.none > 0:
        reason_code = "asi_source_partial"
    elif summary.trace == 0 and summary.total > 0:
        reason_code = "asi_source_no_traces"
    else:
        reason_code = "asi_source_complete"

    metrics: dict[str, Any] = {
        "trace": summary.trace,
        "row_payload": summary.row_payload,
        "uc_metadata": summary.uc_metadata,
        "none": summary.none,
        "total": summary.total,
        "coverage_ratio": round(summary.coverage_ratio, 4),
    }
    if trace_id_count is not None:
        metrics["trace_id_count"] = int(trace_id_count)
    if expected_trace_count is not None:
        metrics["expected_trace_count"] = int(expected_trace_count)

    return {
        "run_id": run_id,
        "iteration": int(iteration),
        "stage_letter": "C",
        "gate_name": "asi_extraction",
        "decision": "ok" if reason_code == "asi_source_complete" else "degraded",
        "reason_code": reason_code,
        "metrics_json": json.dumps(metrics, sort_keys=True),
    }


class _ScorerFeedbackCache:
    """Run-scoped cache for scorer rationale/metadata.

    Scorers call :func:`_cache_scorer_feedback` (via
    :func:`format_asi_markdown`) to tuck away their rationale + metadata so
    that ``run_evaluation`` can re-attach them to rows even when MLflow's
    ``eval_results`` table drops the ``<judge>/rationale`` columns.

    This cache is intentionally *run-scoped* (managed through a
    :class:`~contextvars.ContextVar` via :func:`_scorer_feedback_scope`) so
    that:

    * Two sequential ``run_evaluation`` calls with overlapping ``question_id``
      values cannot cross-contaminate each other.
    * A crash mid-evaluate does not leave poisoned state for the next call.
    * Duplicate ``question_id`` collisions inside a single run are counted
      and surfaced as a warning (each collision overwrites the previous
      entry, matching legacy behavior, but the counter lets us observe it).

    The module-global fallback (:data:`_LEGACY_SCORER_FEEDBACK_CACHE`) is
    retained for one release so any code path that invokes a scorer outside
    an explicit run scope still works exactly as before.
    """

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], dict] = {}
        self._collision_count: int = 0

    def write(
        self,
        question_id: str,
        judge_name: str,
        rationale: str,
        metadata: dict | None = None,
    ) -> None:
        key = (question_id, judge_name)
        if key in self._entries:
            self._collision_count += 1
        self._entries[key] = {
            "rationale": rationale,
            "metadata": metadata or {},
        }

    def drain(self) -> dict[str, dict[str, dict]]:
        """Return ``{question_id: {judge: {rationale, metadata}}}`` and clear."""
        by_question: dict[str, dict[str, dict]] = {}
        for (qid, judge), data in self._entries.items():
            by_question.setdefault(qid, {})[judge] = data
        collisions = self._collision_count
        self._entries.clear()
        self._collision_count = 0
        if collisions:
            logger.warning(
                "Scorer feedback cache observed %d question_id collision(s); "
                "duplicate qids within a single benchmark should be deduped "
                "(see scripts/dedupe_benchmark_qids.py).",
                collisions,
            )
        return by_question

    @property
    def collision_count(self) -> int:
        return self._collision_count


_LEGACY_SCORER_FEEDBACK_CACHE: _ScorerFeedbackCache = _ScorerFeedbackCache()

_current_scorer_feedback_cache: contextvars.ContextVar[_ScorerFeedbackCache | None] = (
    contextvars.ContextVar("gso_scorer_feedback_cache", default=None)
)


@contextlib.contextmanager
def _scorer_feedback_scope() -> Iterator[_ScorerFeedbackCache]:
    """Bind a fresh :class:`_ScorerFeedbackCache` for the current run.

    Use in ``run_evaluation`` (and any other eval orchestration entrypoint)
    inside a ``with`` block. The cache is guaranteed to be reset on exit
    even if the body raises, so a failed evaluate never poisons the next.
    """
    cache = _ScorerFeedbackCache()
    token = _current_scorer_feedback_cache.set(cache)
    try:
        yield cache
    finally:
        cache.drain()
        _current_scorer_feedback_cache.reset(token)


def _get_active_scorer_cache() -> _ScorerFeedbackCache:
    cache = _current_scorer_feedback_cache.get()
    if cache is not None:
        return cache
    return _LEGACY_SCORER_FEEDBACK_CACHE


_REGISTERED_PROMPT_NAMES: dict[str, str] = {}

_PROVENANCE_PRIORITY = [
    "curated", "curated_sql_generated", "reused", "synthetic",
    "auto_corrected", "coverage_gap_fill",
]


def _truncate_benchmarks(benchmarks: list[dict], max_count: int) -> list[dict]:
    """Truncate benchmarks to *max_count* using provenance-based priority.

    Curated benchmarks are kept first, then synthetic, auto_corrected,
    coverage_gap_fill, and finally any other provenance.  Within each
    tier the original order (which respects category diversity) is preserved.
    """
    if len(benchmarks) <= max_count:
        return benchmarks
    buckets: dict[str, list[dict]] = {p: [] for p in _PROVENANCE_PRIORITY}
    buckets["other"] = []
    for b in benchmarks:
        prov = b.get("provenance", "other")
        buckets.get(prov, buckets["other"]).append(b)
    result: list[dict] = []
    for p in _PROVENANCE_PRIORITY + ["other"]:
        for b in buckets[p]:
            if len(result) >= max_count:
                break
            result.append(b)
    logger.warning("Truncated benchmarks from %d to %d", len(benchmarks), len(result))
    return result


_TEMPORAL_QUESTION_RE = re.compile(
    r"\b(this year|last \d+ months?|last \d+ days?|current year"
    r"|year-to-date|ytd|this month|this quarter|past \d+ months?)\b",
    re.IGNORECASE,
)


def _flag_stale_temporal_benchmarks(
    benchmarks: list[dict],
    spark: "SparkSession",
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Flag benchmarks whose GT SQL returns 0 rows due to stale temporal filters.

    Sets ``temporal_stale=True`` on benchmarks where the question contains
    temporal patterns and the GT SQL returns 0 rows.  Flagged benchmarks are
    excluded from accuracy scoring in ``_compute_arbiter_adjusted_accuracy``.

    When *w* and *warehouse_id* are provided, routes the check through the
    SQL warehouse; otherwise uses Spark SQL.
    """
    from genie_space_optimizer.optimization.benchmarks import _quiet_grpc_logs

    flagged_count = 0
    for b in benchmarks:
        q = b.get("question", "")
        sql = b.get("expected_sql", "")
        if not _TEMPORAL_QUESTION_RE.search(q):
            continue
        if not sql:
            continue
        try:
            with _quiet_grpc_logs():
                if w and warehouse_id:
                    result_df = _execute_sql_via_warehouse(
                        w, warehouse_id, f"SELECT * FROM ({sql}) LIMIT 1",
                    )
                    if result_df.empty:
                        b["temporal_stale"] = True
                        flagged_count += 1
                        logger.info(
                            "Temporal benchmark '%s' returns 0 rows -- flagged as stale",
                            q[:60],
                        )
                else:
                    df = spark.sql(sql).limit(1)
                    if df.count() == 0:
                        b["temporal_stale"] = True
                        flagged_count += 1
                        logger.info(
                            "Temporal benchmark '%s' returns 0 rows -- flagged as stale",
                            q[:60],
                        )
        except Exception:
            pass
    if flagged_count:
        logger.warning(
            "Flagged %d/%d benchmarks as temporal-stale (excluded from accuracy)",
            flagged_count,
            len(benchmarks),
        )
    return benchmarks


def _cache_scorer_feedback(
    question_id: str, judge_name: str, rationale: str, metadata: dict | None = None
) -> None:
    """Store scorer feedback for later merge into rows_for_output.

    Called by scorers via ``format_asi_markdown`` so that rationale and
    metadata survive even when MLflow's eval_results table drops them.

    Writes to the active :class:`_ScorerFeedbackCache` bound by
    :func:`_scorer_feedback_scope`; falls back to a module-global cache
    for back-compat when no scope is active.
    """
    _get_active_scorer_cache().write(question_id, judge_name, rationale, metadata)


def _drain_scorer_feedback_cache() -> dict[str, dict[str, dict]]:
    """Return and clear all cached feedback, keyed by question_id then judge.

    Reads from the active :class:`_ScorerFeedbackCache` when a scope is
    bound; otherwise drains the module-global fallback cache.
    """
    return _get_active_scorer_cache().drain()


EVAL_SCOPES = {"full", "slice", "p0", "held_out"}
EVAL_DEBUG = os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_DEBUG", "true").lower() in {"1", "true", "yes", "on"}
EVAL_MAX_ATTEMPTS = int(os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS", "4"))
EVAL_RETRY_SLEEP_SECONDS = int(os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_RETRY_SLEEP_SECONDS", "10"))
EVAL_SINGLE_WORKER_FALLBACK = os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_RETRY_WORKERS", "1")
STRICT_PROMPT_REGISTRATION = (
    os.getenv("GENIE_SPACE_OPTIMIZER_STRICT_PROMPT_REGISTRATION", "true").lower()
    in {"1", "true", "yes", "on"}
)
FAIL_ON_INFRA_EVAL_ERRORS = (
    os.getenv("GENIE_SPACE_OPTIMIZER_FAIL_ON_INFRA_EVAL_ERRORS", "true").lower()
    in {"1", "true", "yes", "on"}
)


# ── Shared Helpers ──────────────────────────────────────────────────────

_CMP_BULKY_KEYS = frozenset({"gt_sample", "genie_sample", "gt_signature", "genie_signature"})


def slim_comparison(cmp: dict) -> dict:
    """Return a lightweight copy of a comparison dict for use in assessments.

    Strips bulky keys (result samples, signatures) to keep MLflow
    trace/assessment payloads well within size limits.
    """
    return {k: v for k, v in cmp.items() if k not in _CMP_BULKY_KEYS}


def build_temporal_note(cmp: dict) -> str:
    """Build a prompt note explaining temporal date rewriting, if applicable."""
    tr = cmp.get("temporal_rewrite")
    if not tr:
        return ""
    return (
        "\nTEMPORAL CONTEXT: The question uses a relative time reference "
        f"('{tr['keyword']}'). The GT SQL dates were auto-adjusted from "
        f"{tr['original_dates']} to {tr['rewritten_dates']} to match the "
        "current date. If there are still minor date differences between "
        "GT and Genie, evaluate whether Genie's date interpretation is "
        "reasonable for the temporal reference in the question.\n"
    )


def _extract_response_text(outputs: Union[dict, Any]) -> str:
    """Extract response text from mlflow.genai.evaluate() serialized format."""
    if isinstance(outputs, str):
        return outputs
    if isinstance(outputs, dict):
        if "response" in outputs:
            return outputs["response"]
        if "output" in outputs:
            output_list = outputs["output"]
            if output_list and len(output_list) > 0:
                item = output_list[0]
                if "content" in item and item["content"]:
                    return item["content"][0].get("text", "")
    return ""


_FENCED_BLOCK_RE = re.compile(
    r"```(?:json|JSON)?\s*\n?(?P<body>.*?)```", re.DOTALL,
)


def _strip_trailing_statement_semicolon(sql: str) -> str:
    """Remove trailing semicolons before embedding SQL in a subquery wrapper.

    Sample-row capture wraps SQL in ``SELECT * FROM (...) _gvse_sample LIMIT n``.
    A trailing ``;`` makes the wrapper SQL syntactically invalid because the
    inner statement terminates the outer query. Strip whitespace + trailing
    semicolons so the wrapper compiles regardless of how the upstream LLM /
    benchmark fixture wrote the statement.
    """
    text = str(sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return text


def _extract_json(content: str | None, *, strict: bool = False) -> dict | list | None:
    """Extract a JSON value from LLM response text with lenient wrapping.

    Returns ``None`` for empty / whitespace-only / fenced-but-empty /
    non-JSON content so callers can treat "no parseable response" as a
    typed soft failure. Pass ``strict=True`` to preserve the legacy
    raise-on-error behaviour for code paths that need a hard failure
    (e.g. ``_traced_llm_call`` ``response_validator``).
    """
    if content is None:
        if strict:
            raise ValueError("No content to parse as JSON")
        return None
    text = content.strip()
    if not text:
        if strict:
            raise ValueError("Empty content cannot be parsed as JSON")
        return None

    # Fenced block anywhere in the string — prefer it over the surrounding
    # prose so a preamble like "Here is the JSON:\n```json\n{...}\n```" works.
    fence_match = _FENCED_BLOCK_RE.search(text)
    if fence_match:
        fenced = fence_match.group("body").strip()
        if fenced:
            try:
                return json.loads(fenced)
            except json.JSONDecodeError:
                # Fall through — the fenced block might itself be malformed
                # but the surrounding text could still contain valid JSON.
                pass
        else:
            # Fenced block with no body. Treat the same as empty content.
            if strict:
                raise ValueError("Empty fenced block cannot be parsed as JSON")
            return None

    _saved_err: json.JSONDecodeError | None = None

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        _saved_err = exc

    if (
        _saved_err is not None
        and hasattr(_saved_err, "pos")
        and _saved_err.msg.startswith("Extra data")
    ):
        try:
            return json.loads(text[: _saved_err.pos])
        except json.JSONDecodeError:
            pass

    # Regex fallbacks — try the first balanced `{...}` and `[...]`; take the
    # one that parses. We prefer whichever is longer so a nested structure
    # wins over a short sub-literal.
    candidates: list[tuple[int, str]] = []
    obj_match = re.search(r"\{.*\}", text, re.DOTALL)
    if obj_match:
        candidates.append((len(obj_match.group(0)), obj_match.group(0)))
    arr_match = re.search(r"\[.*\]", text, re.DOTALL)
    if arr_match:
        candidates.append((len(arr_match.group(0)), arr_match.group(0)))
    # Longest-first maximises the chance of getting the outermost structure.
    for _, candidate in sorted(candidates, key=lambda c: -c[0]):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    if strict:
        assert _saved_err is not None  # pragma: no cover — invariant
        raise _saved_err
    logger.debug(
        "_extract_json could not parse content; returning None. "
        "first_120_chars=%r error=%s",
        text[:120],
        _saved_err,
    )
    return None


def _extract_json_array(content: str) -> list:
    """Extract a JSON array from LLM response text.

    Thin wrapper over :func:`_extract_json` that asserts a list is returned.
    Callers that expect an array (the prose-rule miner) should use this
    function; on non-array output it raises ``ValueError`` so retry logic
    can kick in.
    """
    value = _extract_json(content)
    if isinstance(value, list):
        return value
    raise ValueError(
        f"Expected JSON array from LLM, got {type(value).__name__}"
    )


def get_registered_prompt_name(judge_name: str) -> str:
    """Return the registered prompt name for a judge/lever, or empty string."""
    return _REGISTERED_PROMPT_NAMES.get(judge_name, "")


def _link_prompt_to_trace(prompt_name: str) -> None:
    """Load a registered prompt inside the current trace to link it.

    MLflow automatically associates ``load_prompt()`` calls with the
    active trace, making the prompt version visible in the Linked Prompts
    tab of the trace UI.  Failures are silently ignored so scoring continues.
    """
    if not prompt_name:
        return
    try:
        mlflow.genai.load_prompt(f"prompts:/{prompt_name}@{PROMPT_ALIAS}")
    except Exception:
        try:
            mlflow.genai.load_prompt(f"prompts:/{prompt_name}@latest")
        except Exception:
            logger.debug("Could not load prompt '%s' for trace linking", prompt_name)


def _call_llm_for_scoring(
    w: "WorkspaceClient",
    prompt: str,
    max_retries: int = LLM_MAX_RETRIES,
    prompt_name: str = "",
) -> dict:
    """Call LLM via the OpenAI SDK with retry + exponential backoff.

    Uses the shared ``llm_client`` so that ``mlflow.openai.autolog()``
    captures token usage, cost, and latency automatically.

    If *prompt_name* is provided, loads the registered prompt first to
    link it to the current MLflow trace (visible in Linked Prompts tab).
    """
    from genie_space_optimizer.optimization.llm_client import call_llm

    _link_prompt_to_trace(prompt_name)

    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            text, _response = call_llm(
                w,
                messages=[{"role": "user", "content": prompt}],
                max_retries=1,
                temperature=LLM_TEMPERATURE,
            )
            return _extract_json(text)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
    raise last_err  # type: ignore[misc]


# Allow backtick-quoted identifiers to start with a digit (e.g.
# Databricks measure names like ``5g_orders_diff_mtd`` or any
# digit-prefixed business identifier). When the column or alias is
# wrapped in backticks the leading digit is legal; unquoted
# identifiers still must start with a letter or underscore.
_MEASURE_ALIAS_COLLISION_PATTERN = re.compile(
    r"MEASURE\s*\(\s*(?:`(\w+)`|([A-Za-z_]\w*))\s*\)"
    r"\s+AS\s+(?:`(\w+)`|([A-Za-z_]\w*))",
    re.IGNORECASE,
)


def _alias_collision_match_groups(m: re.Match) -> tuple[str, str]:
    """Return ``(measure_col, alias)`` for a collision-pattern match.

    Each side is matched in two alternatives — backtick-quoted (group
    1 / 3) or bare (group 2 / 4). Exactly one of each pair will be
    non-empty.
    """
    col = m.group(1) or m.group(2) or ""
    alias = m.group(3) or m.group(4) or ""
    return col, alias


def _measure_alias_collision_rename_map(sql: str) -> dict[str, str]:
    """Return lower-case measure name -> safe alias for MEASURE(m) AS m.

    Task 6 helper: lets ``apply_pre_execute_repairs`` know which
    measures the alias-collision repair will rename so it can rewrite
    ``ORDER BY MEASURE(<original_measure>)`` to the renamed alias.
    """
    rename_map: dict[str, str] = {}
    for m in _MEASURE_ALIAS_COLLISION_PATTERN.finditer(sql or ""):
        col, alias = _alias_collision_match_groups(m)
        if not col or not alias:
            continue
        if col.lower() == alias.lower() and col.lower() not in rename_map:
            rename_map[col.lower()] = f"{col}_value"
    return rename_map


def _repair_measure_alias_collisions(sql: str) -> tuple[str, int]:
    """Rewrite ``MEASURE(col) AS col`` to ``MEASURE(col) AS col_value``.

    PR 15 — when a SELECT projects ``MEASURE(col) AS col`` against a
    metric view, Spark's resolver shadows the underlying measure column
    with the alias. Subsequent references in ORDER BY / HAVING that use
    ``MEASURE(col)`` then resolve to the alias output (a regular
    aggregate expression) and fail the planner with::

        [MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION]
        Resolved attribute(s) "col" missing from "..., col, ..." in
        operator !Aggregate ... measure(col#alias_id) AS measure(col)

    The deterministic fix is to rename the alias so it no longer matches
    the underlying column name. We append ``_value`` because it
    survives downstream prompt-matching and is unambiguous in
    user-facing queries. Bare references to the original alias in
    ORDER BY / HAVING / GROUP BY are remapped to the new alias so
    semantically-equivalent queries continue to return the same
    rows in the same order.

    Returns ``(new_sql, num_collisions_fixed)``. The counter feeds the
    unified-pipeline yield diagnostics (PR 18) so operators can see how
    often this repair fires.
    """
    if not sql or "MEASURE" not in sql.upper():
        return sql, 0

    # First pass: identify collisions, build alias-rename map.
    rename_map = _measure_alias_collision_rename_map(sql)
    if not rename_map:
        return sql, 0

    # Second pass: replace each collision-style alias with the safe one.
    def _replace(m: re.Match) -> str:
        col, alias = _alias_collision_match_groups(m)
        if not col or not alias or col.lower() != alias.lower():
            return m.group(0)
        new_alias = rename_map[col.lower()]
        # Preserve backticks on the rendered output when the original
        # column was backtick-quoted (Databricks measure names that
        # start with a digit must stay backticked).
        col_quoted = f"`{col}`" if not col[:1].isalpha() and col[:1] != "_" else col
        alias_quoted = f"`{new_alias}`" if not new_alias[:1].isalpha() and new_alias[:1] != "_" else new_alias
        return f"MEASURE({col_quoted}) AS {alias_quoted}"

    new_sql = _MEASURE_ALIAS_COLLISION_PATTERN.sub(_replace, sql)

    # Third pass: rewrite bare alias references in ORDER BY / HAVING /
    # GROUP BY clauses (the only places where SELECT aliases are
    # legally usable outside the projection list). We match the old
    # alias as a whole identifier and skip occurrences inside
    # ``MEASURE(...)`` (those still resolve to the underlying column).
    # Conservative: only rewrite within ORDER BY / HAVING tail to avoid
    # touching anything in the FROM / WHERE clauses.
    def _rewrite_clause(text: str) -> str:
        for old_col, new_alias in rename_map.items():
            # Match `old_col` as a whole identifier not inside MEASURE(.
            # The lookbehind on ``MEASURE\s*\(\s*`?`` is variable-width
            # so we approximate with a 12-char window check.
            pattern = re.compile(rf"\b{re.escape(old_col)}\b", re.IGNORECASE)

            def _sub(match: re.Match) -> str:
                start = match.start()
                window_start = max(0, start - 12)
                prefix = text[window_start:start]
                if re.search(r"MEASURE\s*\(\s*`?$", prefix, re.IGNORECASE):
                    return match.group(0)
                return new_alias

            text = pattern.sub(_sub, text)
        return text

    # Locate ORDER BY / HAVING / GROUP BY tails. Rewriting the whole
    # tail captures all three clauses regardless of order.
    tail_anchor = re.search(r"\b(ORDER\s+BY|HAVING|GROUP\s+BY)\b", new_sql, re.IGNORECASE)
    if tail_anchor:
        head = new_sql[: tail_anchor.start()]
        tail = new_sql[tail_anchor.start() :]
        new_sql = head + _rewrite_clause(tail)

    return new_sql, len(rename_map)


def _repair_measure_in_where(
    sql: str,
    mv_measures: dict[str, set[str]],
) -> tuple[str, int]:
    """Rewrite ``WHERE <measure_col> …`` into a CTE-first pattern (PR 20).

    Spark's metric-view planner rejects measure column references inside
    ``WHERE`` / ``HAVING`` / ``ON`` clauses with the same
    ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` error class as a bare
    measure in SELECT. The canonical fix per the
    `Databricks Metric Views docs
    <https://docs.databricks.com/aws/en/business-semantics/metric-views/query>`_
    is to materialize the measure in a CTE alias and filter on the
    alias::

        -- BAD: WHERE references a measure column directly
        SELECT zone, MEASURE(total_sales) AS sales
        FROM mv_x
        WHERE store_day_count > 0
        GROUP BY zone;

        -- GOOD: CTE-first; filter on the materialized alias
        WITH __mv_base AS (
          SELECT zone,
                 MEASURE(total_sales) AS sales,
                 MEASURE(store_day_count) AS store_day_count_value
          FROM mv_x
          GROUP BY zone
        )
        SELECT zone, sales
        FROM __mv_base
        WHERE store_day_count_value > 0;

    The function detects measure column references in the WHERE clause
    using ``mv_measures`` (keyed by short MV name → measure names),
    promotes each referenced measure into the inner SELECT as
    ``MEASURE(m) AS m_value``, and rewrites the WHERE clause to use the
    materialized alias. The outer SELECT replays the original
    projections (by output-name) so callers (LLM correction, example
    SQL gates) see the same shape they would have without the repair.

    Returns ``(new_sql, num_measures_lifted)``. Conservative: returns
    ``(sql, 0)`` unchanged when sqlglot is unavailable, parsing fails,
    the root expression is not a single ``SELECT``, the query already
    has a ``WITH`` clause / outer ``JOIN`` / FROM-side subquery / set-op
    (``UNION``/``EXCEPT``/``INTERSECT``), or no relevant measure column
    appears in the WHERE clause. False negatives only — by design we
    prefer leaving the SQL alone over emitting a wrong rewrite.
    """
    if not sql or not mv_measures or "WHERE" not in sql.upper():
        return sql, 0

    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return sql, 0

    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return sql, 0

    if not isinstance(tree, exp.Select):
        # Set-ops (Union/Except/Intersect) and DDL parse to non-Select
        # roots; we don't try to rewrite them.
        return sql, 0

    # Conservative bail-outs: leave anything we don't fully understand
    # alone so the LLM can fix it on retry. sqlglot stores these args
    # under trailing-underscore keys (``with_``, ``from_``) but also
    # exposes ``with``/``from`` aliases on some versions; accept either.
    if tree.args.get("with") or tree.args.get("with_"):
        return sql, 0
    if tree.args.get("joins"):
        return sql, 0

    from_ = tree.args.get("from") or tree.args.get("from_")
    if from_ is not None:
        sources: list[Any] = []
        if hasattr(from_, "expressions") and from_.expressions:
            sources.extend(from_.expressions)
        elif from_.this is not None:
            sources.append(from_.this)
        for src in sources:
            if isinstance(src, exp.Subquery):
                return sql, 0

    where_clause = tree.args.get("where")
    if where_clause is None:
        return sql, 0

    # Subqueries in WHERE (e.g. ``WHERE x IN (SELECT …)``) are out of
    # scope — refusing keeps the rewrite deterministic.
    if any(True for _ in where_clause.find_all(exp.Subquery)):
        return sql, 0

    # Resolve which measures are reachable from the FROM clause of THIS
    # query. Reuses the same alias-aware helper as ``_rewrite_measure_refs``
    # so both repairs see the same set of measure names.
    relevant_measures = _build_relevant_measures(sql, mv_measures)
    if not relevant_measures:
        return sql, 0
    all_measure_names: set[str] = set()
    for s in relevant_measures.values():
        all_measure_names.update(s)
    if not all_measure_names:
        return sql, 0

    # Collect measure-column references inside WHERE. We match on the
    # column's bare name (case-insensitive) — Spark's resolver does the
    # same when matching a measure column on a metric view.
    measures_in_where: list[str] = []
    seen: set[str] = set()
    for col in where_clause.find_all(exp.Column):
        nm = (col.name or "").lower()
        if nm in all_measure_names and nm not in seen:
            seen.add(nm)
            measures_in_where.append(nm)
    if not measures_in_where:
        return sql, 0

    # Build the inner SELECT: original tree minus its WHERE clause, with
    # ``MEASURE(m) AS m_value`` projections appended for each measure
    # that needs to be available to the outer filter.
    inner = tree.copy()
    inner.set("where", None)

    existing_aliases: set[str] = set()
    for proj in inner.expressions:
        if isinstance(proj, exp.Alias):
            alias_id = proj.args.get("alias")
            if alias_id is not None:
                existing_aliases.add(str(alias_id.name or "").lower())

    alias_map: dict[str, str] = {}
    for m in measures_in_where:
        alias_name = f"{m}_value"
        # Avoid colliding with any pre-existing alias on the inner.
        suffix = 2
        while alias_name.lower() in existing_aliases:
            alias_name = f"{m}_value{suffix}"
            suffix += 1
        alias_map[m] = alias_name
        existing_aliases.add(alias_name.lower())
        new_proj = exp.Alias(
            this=exp.Anonymous(
                this="MEASURE",
                expressions=[exp.column(m)],
            ),
            alias=exp.to_identifier(alias_name),
        )
        inner.expressions.append(new_proj)

    # Rewrite the WHERE clause: rebind each measure-column reference to
    # the materialized alias on the CTE.
    new_where = where_clause.copy()
    for col in new_where.find_all(exp.Column):
        nm = (col.name or "").lower()
        if nm in alias_map:
            col.set("this", exp.to_identifier(alias_map[nm]))
            # Drop any table qualifier — the column now lives on the
            # CTE, not the original metric view.
            col.set("table", None)
            col.set("db", None)
            col.set("catalog", None)

    # Outer projection list: replay the original SELECT's output names
    # so callers see the same shape pre- vs post-repair. Anything we
    # can't unambiguously name (e.g. a non-aliased complex expression)
    # forces a ``SELECT *`` fallback.
    outer_projs: list[exp.Expression] = []
    fallback_to_star = False
    for p in tree.expressions:
        out_name: str | None = None
        if isinstance(p, exp.Alias):
            alias_id = p.args.get("alias")
            if alias_id is not None and alias_id.name:
                out_name = str(alias_id.name)
        elif isinstance(p, exp.Column):
            out_name = p.name
        if out_name:
            outer_projs.append(
                exp.Column(this=exp.to_identifier(out_name)),
            )
        else:
            fallback_to_star = True
            break
    if fallback_to_star or not outer_projs:
        outer_projs = [exp.Star()]

    # Assemble the wrapper. ``Select.with_`` is the only sqlglot API
    # that wires ``WITH … AS (…) SELECT …`` such that the WITH renders
    # when the tree is serialised; setting ``args["with"]`` directly
    # silently drops the CTE on render.
    outer = exp.Select(expressions=outer_projs)
    outer.set(
        "from",
        exp.From(this=exp.Table(this=exp.to_identifier("__mv_base"))),
    )
    outer.set("where", new_where)
    outer = outer.with_("__mv_base", inner, copy=False)

    try:
        return outer.sql(dialect="databricks"), len(measures_in_where)
    except Exception:
        return sql, 0


# PR 26 — direct-JOIN-on-metric-view pre-check + CTE-first repair.
#
# Spark's metric-view planner rejects any direct ``JOIN`` whose left or
# right operand is a metric view with the
# ``METRIC_VIEW_JOIN_NOT_SUPPORTED`` error class. The documented fix is
# to materialize each metric view inside a ``WITH`` CTE first (computing
# every required measure with ``MEASURE()`` and projecting the
# dimensions used in the JOIN predicate) and then JOIN the CTE's result
# in the outer query. The benchmark validation path already short-
# circuits this shape upstream of EXPLAIN; PR 26 mirrors the gate into
# the unified example-SQL synthesis path so the LLM either receives an
# auto-repaired candidate or sees the candidate rejected with an
# actionable reason code instead of being re-prompted with an opaque
# Spark error string.


def _check_metric_view_join_pre(
    sql: str,
    mv_set: set[str],
) -> str | None:
    """Reject SQL that JOINs directly against a metric view (PR 26).

    Returns ``"metric_view_join"`` when the SQL contains a ``JOIN``
    whose left or right operand is a known metric view (resolved by
    short-name match against ``mv_set``) and the SQL does NOT already
    use a ``WITH`` clause to materialize the MV. Returns ``None``
    otherwise (no JOIN, no MV in the JOIN, or the MV is wrapped in a
    CTE).

    The ``mv_set`` should contain short basenames (lowercased), e.g.
    ``{"mv_sales", "mv_returns"}``. ``cat.sch.mv_sales`` references in
    the SQL match because the resolver compares the basename
    (``mv_sales``).

    Conservative: returns ``None`` when sqlglot is unavailable, parsing
    fails, the SQL already contains a top-level ``WITH`` (the LLM
    likely emitted the CTE-first pattern already), or there is no
    ``JOIN`` at all.
    """
    if not sql or not mv_set:
        return None
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return None

    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return None

    if not isinstance(tree, exp.Select):
        return None
    if tree.args.get("with") or tree.args.get("with_"):
        # CTE present — assume the LLM already emitted the documented
        # CTE-first pattern. False negatives here only surface as the
        # generic ``metric_view_join`` Spark error downstream, which the
        # correction loop can still fix.
        return None
    joins = tree.args.get("joins") or []
    if not joins:
        return None

    mv_lower = {m.lower() for m in mv_set if m}

    def _table_basename(t: exp.Table) -> str:
        nm = (t.name or "").strip("`").lower()
        return nm

    # Collect every operand on the FROM + JOIN side of the query.
    from_ = tree.args.get("from") or tree.args.get("from_")
    operand_tables: list[exp.Table] = []
    if from_ is not None:
        for src in getattr(from_, "expressions", None) or [from_.this]:
            if isinstance(src, exp.Table):
                operand_tables.append(src)
    for j in joins:
        right = j.this if isinstance(j, exp.Join) else None
        if isinstance(right, exp.Table):
            operand_tables.append(right)

    for t in operand_tables:
        if _table_basename(t) in mv_lower:
            return "metric_view_join"
    return None


def _repair_metric_view_join(
    sql: str,
    mv_set: set[str],
    mv_measures: dict[str, set[str]] | None = None,
) -> tuple[str, int]:
    """Wrap each metric view referenced in a JOIN with a CTE (PR 26).

    For each MV referenced in the FROM or any JOIN clause, builds a
    ``WITH __mv_<n> AS (SELECT <referenced_dims>, MEASURE(<m>) AS <m>,
    … FROM <mv>)`` CTE and rewrites the original Table node to
    reference the CTE alias. Outer-query references to the MV alias's
    columns continue to work because the CTE projects the same column
    names; outer ``MEASURE(alias.measure)`` calls are flattened to
    ``alias.measure`` (the CTE has already materialized the measure).

    Returns ``(new_sql, num_mvs_wrapped)``. Conservative — returns
    ``(sql, 0)`` unchanged when sqlglot is unavailable, parsing fails,
    the SQL already has a ``WITH`` clause, no MV appears in the
    query, the rewrite would be ambiguous (e.g. unqualified column
    references that could resolve to either side of the JOIN), or any
    transform raises.

    The repair is best-effort: when it cannot produce a clean rewrite
    it returns ``(sql, 0)`` so the caller (synthesis pre-check) records
    the candidate as ``metric_view_join`` rejected and lets the
    correction loop / LLM hint do the work on the next round.
    """
    if not sql or not mv_set:
        return sql, 0
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return sql, 0

    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return sql, 0

    if not isinstance(tree, exp.Select):
        return sql, 0
    if tree.args.get("with") or tree.args.get("with_"):
        return sql, 0
    joins = tree.args.get("joins") or []
    if not joins:
        return sql, 0

    mv_lower = {m.lower() for m in mv_set if m}
    measures_by_mv = {k.lower(): set(v) for k, v in (mv_measures or {}).items()}

    def _table_basename(t: exp.Table) -> str:
        return (t.name or "").strip("`").lower()

    # Identify each MV reference (FROM and every JOIN side). Track the
    # alias the LLM used so we can rebind outer-query column refs.
    from_ = tree.args.get("from") or tree.args.get("from_")
    mv_refs: list[tuple[exp.Table, str]] = []  # (table_node, alias)

    def _alias_of(t: exp.Table) -> str:
        a = t.args.get("alias")
        if isinstance(a, exp.TableAlias) and a.name:
            return str(a.name)
        return _table_basename(t)

    if from_ is not None:
        for src in getattr(from_, "expressions", None) or [from_.this]:
            if isinstance(src, exp.Table) and _table_basename(src) in mv_lower:
                mv_refs.append((src, _alias_of(src)))
    for j in joins:
        right = j.this if isinstance(j, exp.Join) else None
        if isinstance(right, exp.Table) and _table_basename(right) in mv_lower:
            mv_refs.append((right, _alias_of(right)))

    if not mv_refs:
        return sql, 0

    # Build a CTE for each MV ref and rewrite the Table node in place.
    # The CTE projects every column referenced from the MV alias in
    # the rest of the SQL plus the MV's known measures (when we have
    # them) wrapped in MEASURE(). Unknown column shapes (no qualifier,
    # ambiguous resolution) cause us to bail.
    new_tree = tree.copy()
    # Re-bind operand_tables on the COPY since we just deep-copied.
    new_from = new_tree.args.get("from") or new_tree.args.get("from_")
    new_joins = new_tree.args.get("joins") or []

    # Record (table_node, original_user_alias, cte_alias, mv_basename)
    # tuples — the basename is captured BEFORE the rewrite swaps the
    # table's identifier to the CTE alias, otherwise the outer-query
    # MEASURE() flatten step below can't find which MV a given alias
    # belongs to.
    new_mv_refs: list[tuple[exp.Table, str, str, str]] = []
    cte_idx = 0
    if new_from is not None:
        for src in getattr(new_from, "expressions", None) or [new_from.this]:
            if isinstance(src, exp.Table) and _table_basename(src) in mv_lower:
                cte_idx += 1
                cte_alias = f"__mv_{cte_idx}"
                new_mv_refs.append(
                    (src, _alias_of(src), cte_alias, _table_basename(src)),
                )
    for j in new_joins:
        right = j.this if isinstance(j, exp.Join) else None
        if isinstance(right, exp.Table) and _table_basename(right) in mv_lower:
            cte_idx += 1
            cte_alias = f"__mv_{cte_idx}"
            new_mv_refs.append(
                (right, _alias_of(right), cte_alias, _table_basename(right)),
            )

    # Collect referenced columns per MV alias from the entire tree.
    cols_per_alias: dict[str, set[str]] = {
        a.lower(): set() for _, a, _, _ in new_mv_refs
    }
    for col in new_tree.find_all(exp.Column):
        tbl = (col.table or "").strip("`").lower()
        if tbl and tbl in cols_per_alias:
            cols_per_alias[tbl].add((col.name or "").lower())

    cte_definitions: list[tuple[str, exp.Select]] = []
    alias_to_basename: dict[str, str] = {
        a.lower(): basename for _, a, _, basename in new_mv_refs
    }
    for table_node, original_alias, cte_alias, basename in new_mv_refs:
        measures_for_mv = measures_by_mv.get(basename, set())
        referenced_cols = cols_per_alias.get(original_alias.lower(), set())
        if not referenced_cols and not measures_for_mv:
            # Nothing tangible to project; bail conservatively.
            return sql, 0

        # Partition referenced columns into dims vs measures.
        dim_cols: list[str] = []
        measure_cols_used: set[str] = set()
        for c in sorted(referenced_cols):
            if c in measures_for_mv:
                measure_cols_used.add(c)
            else:
                dim_cols.append(c)
        # Always include any known measure that wasn't directly
        # referenced — keeping the CTE's projection a superset of the
        # outer query's needs is safer than under-projecting.
        for m in sorted(measures_for_mv):
            measure_cols_used.add(m)

        cte_projections: list[exp.Expression] = []
        for d in dim_cols:
            cte_projections.append(exp.column(d))
        for m in sorted(measure_cols_used):
            cte_projections.append(
                exp.Alias(
                    this=exp.Anonymous(
                        this="MEASURE",
                        expressions=[exp.column(m)],
                    ),
                    alias=exp.to_identifier(m),
                ),
            )
        if not cte_projections:
            return sql, 0

        # FROM the original MV (preserve full qualification by copying
        # the original table node, sans alias). ``exp.select(...).
        # from_(table)`` is the only sqlglot API that wires the FROM
        # clause such that it renders; ``Select.set('from', From(...))``
        # silently drops the FROM on serialization for newly-built
        # SELECT trees.
        from_table = exp.Table(
            this=exp.to_identifier(table_node.name),
            db=table_node.args.get("db"),
            catalog=table_node.args.get("catalog"),
        )
        cte_select = exp.select(*cte_projections).from_(from_table)
        cte_definitions.append((cte_alias, cte_select))

        # Rewrite the original Table node to reference the CTE alias.
        table_node.set("this", exp.to_identifier(cte_alias))
        table_node.set("db", None)
        table_node.set("catalog", None)
        # Re-pin the alias so outer-query qualified references
        # (``alias.col``) keep resolving — alias text is unchanged.
        if not table_node.args.get("alias"):
            table_node.set(
                "alias",
                exp.TableAlias(this=exp.to_identifier(original_alias)),
            )

    # Outer query: flatten ``MEASURE(alias.measure)`` to
    # ``alias.measure`` because the CTE already materialized the
    # measure under the same column name.
    for anon in list(new_tree.find_all(exp.Anonymous)):
        if (anon.this or "").upper() != "MEASURE":
            continue
        args = anon.expressions or []
        if len(args) != 1 or not isinstance(args[0], exp.Column):
            continue
        col = args[0]
        tbl = (col.table or "").strip("`").lower()
        nm = (col.name or "").lower()
        basename_for_alias = alias_to_basename.get(tbl)
        if (
            basename_for_alias
            and nm in measures_by_mv.get(basename_for_alias, set())
        ):
            anon.replace(col.copy())

    # Attach each CTE in order. ``Select.with_`` chains them so the
    # final serialized SQL has ``WITH __mv_1 AS (…), __mv_2 AS (…)
    # SELECT …``.
    out_tree = new_tree
    for alias, sel in cte_definitions:
        out_tree = out_tree.with_(alias, sel, copy=False)

    try:
        return out_tree.sql(dialect="databricks"), len(cte_definitions)
    except Exception:
        return sql, 0


# Tokens that look like an identifier in a "FROM/JOIN <table> <alias>"
# regex but are actually SQL keywords starting the next clause; never
# treat them as aliases.
_NOT_AN_ALIAS = frozenset({
    "on", "using", "where", "group", "order", "having", "limit",
    "union", "intersect", "except", "join", "inner", "left", "right",
    "full", "cross", "outer", "natural", "lateral",
    "as",  # bare AS (no alias word) shouldn't happen but be safe
})


def _build_relevant_measures(
    sql: str,
    metric_view_measures: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Return ``{alias_or_short: {measure_col, …}}`` for every FROM/JOIN
    table that maps to an entry in *metric_view_measures*.

    Alias-aware: registers BOTH the short table name and any explicit
    alias (``FROM mv AS x`` / ``FROM mv x`` / ``JOIN mv x ON …``) so the
    rewriter can recognise ``mv.col`` *and* ``x.col``.
    """
    out: dict[str, set[str]] = {}
    # Negative lookahead on the alias group prevents the pattern from
    # consuming the next clause keyword (``ON`` / ``JOIN`` / ``WHERE`` /
    # …) as an alias when no alias is present. Without it
    # ``FROM mv1 JOIN mv2`` collapses to a single match and the second
    # MV is silently dropped.
    not_an_alias_alts = "|".join(
        sorted(_NOT_AN_ALIAS, key=len, reverse=True),
    )
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+`?([\w.]+)`?"
        rf"(?:\s+(?:AS\s+)?`?(?!(?:{not_an_alias_alts})\b)([A-Za-z_]\w*)`?)?",
        re.IGNORECASE,
    )
    for m in pattern.finditer(sql):
        ident = (m.group(1) or "").replace("`", "").strip()
        if not ident:
            continue
        short = ident.split(".")[-1].lower()
        alias = (m.group(2) or "").strip()
        if alias.lower() in _NOT_AN_ALIAS:
            alias = ""
        measures = metric_view_measures.get(short, set())
        if not measures:
            continue
        out.setdefault(short, set()).update(measures)
        if alias:
            out.setdefault(alias.lower(), set()).update(measures)
    return out


def _rewrite_measure_refs(
    sql: str,
    metric_view_measures: dict[str, set[str]],
) -> str:
    """Wrap bare metric-view measure references with ``MEASURE()``.

    Covers SELECT, HAVING, and ORDER BY clauses. Skips WHERE and ON
    clauses (Spark forbids ``MEASURE()`` there; the diagnostic the user
    sees on a violation is clearer than a silently-wrapped reference).

    Alias-aware: handles both unqualified bare references
    (``SELECT gross_sales FROM mv_x``) and qualified references
    (``SELECT x.gross_sales FROM mv_x x``). The latter mode lets the
    rewriter cover spaces where the LLM emits an alias even when it
    technically isn't required, which the original short-name-only
    parser missed entirely.

    ``metric_view_measures`` maps lowercased short table names to sets
    of lowercased measure column names.
    """
    if not metric_view_measures or not sql:
        return sql

    relevant_measures = _build_relevant_measures(sql, metric_view_measures)
    if not relevant_measures:
        return sql

    all_measure_names: set[str] = set()
    for s in relevant_measures.values():
        all_measure_names |= s

    already_measured = re.compile(r"\bMEASURE\s*\(", re.IGNORECASE)

    # Single combined pattern: optional ``alias.`` prefix + column. The
    # negative lookbehind on ``[\w.]`` prevents matching the middle
    # component of a 3-part identifier (``catalog.schema.table``); the
    # negative lookahead on ``\s*\(`` prevents wrapping function calls.
    measure_token = re.compile(
        r"(?<![\w.])([A-Za-z_]\w*\.)?([A-Za-z_]\w*)\b(?!\s*\()",
    )

    def _rewrite_clause(text: str) -> str:
        def _repl(m: re.Match) -> str:
            full = m.group(0)
            alias_dot = m.group(1) or ""
            col = m.group(2)
            start = m.start()
            window_start = max(0, start - 12)
            if already_measured.search(text[window_start:start]):
                return full
            col_lower = col.lower()
            if alias_dot:
                alias = alias_dot[:-1].lower()
                measures = relevant_measures.get(alias)
                if measures and col_lower in measures:
                    return f"MEASURE({full})"
                return full
            if col_lower in all_measure_names:
                return f"MEASURE({col})"
            return full

        return measure_token.sub(_repl, text)

    def _next_clause_offset(haystack: str) -> int:
        """Return offset (relative to ``haystack`` start) of the next
        clause-boundary keyword, or ``len(haystack)`` when no boundary
        is present.
        """
        m = re.search(
            r"\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|INTERSECT|EXCEPT)\b",
            haystack,
            re.IGNORECASE,
        )
        return m.start() if m else len(haystack)

    # SELECT clause — between SELECT and FROM. Constrained to the head of
    # the statement; nested subqueries are not handled (the existing
    # implementation didn't either).
    select_match = re.search(r"\bSELECT\b", sql, re.IGNORECASE)
    from_match = re.search(r"\bFROM\b", sql, re.IGNORECASE)
    if select_match and from_match and select_match.end() < from_match.start():
        head = sql[: select_match.end()]
        clause = sql[select_match.end() : from_match.start()]
        tail = sql[from_match.start() :]
        sql = head + _rewrite_clause(clause) + tail

    # HAVING clause — between HAVING and the next clause boundary.
    having_match = re.search(r"\bHAVING\b", sql, re.IGNORECASE)
    if having_match:
        offset = _next_clause_offset(sql[having_match.end():])
        having_end = having_match.end() + offset
        head = sql[: having_match.end()]
        clause = sql[having_match.end() : having_end]
        tail = sql[having_end:]
        sql = head + _rewrite_clause(clause) + tail

    # ORDER BY clause — between ORDER BY and the next boundary
    # (LIMIT / set-op / end of statement).
    order_match = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    if order_match:
        offset = _next_clause_offset(sql[order_match.end():])
        order_end = order_match.end() + offset
        head = sql[: order_match.end()]
        clause = sql[order_match.end() : order_end]
        tail = sql[order_end:]
        sql = head + _rewrite_clause(clause) + tail

    return sql


_OUTER_AGG_AROUND_MEASURE_RE = re.compile(
    r"\b(SUM|AVG|COUNT|MIN|MAX|MEDIAN|STDDEV|STDDEV_POP|STDDEV_SAMP|"
    r"VAR|VAR_POP|VAR_SAMP|VARIANCE|ANY_VALUE)\s*\(\s*"
    r"(MEASURE\s*\([^()]*\))\s*\)",
    re.IGNORECASE,
)


def _strip_outer_agg_around_measure(sql: str) -> tuple[str, int]:
    """Strip a redundant aggregate that wraps a single ``MEASURE(x)`` arg.

    The LLM occasionally emits ``SUM(MEASURE(gross_sales))`` even though
    metric-view measure references must NOT be re-aggregated by the user
    — Spark expands ``MEASURE(gross_sales)`` to ``SUM(gross_sales)``
    internally, which yields ``SUM(MEASURE(SUM(gross_sales)))`` and a
    ``NESTED_AGGREGATE_FUNCTION`` rejection. Stripping the outer
    aggregate is the deterministic fix.

    Behaviour:
      - When the aggregate's *only* argument is a ``MEASURE(...)`` call
        (case-insensitive), the aggregate node is replaced with the
        inner ``MEASURE(...)`` call.
      - Non-aggregate wrappers like ``COALESCE(MEASURE(x), 0)`` are left
        alone — only true aggregates on the allowed list are stripped.
      - Multi-arg aggregates such as ``COUNT(MEASURE(x), 1)`` are left
        alone (extremely rare, but the regex requires a single arg).
      - Falls back to a regex-only path when sqlglot fails to parse the
        SQL (best-effort; the regex is intentionally conservative).

    Returns ``(new_sql, count)`` where ``count`` is the number of
    aggregate-strip rewrites applied. Used by the proposal-side and the
    correction pipelines so both fix the same LLM mode identically.
    """
    if not sql or "MEASURE" not in sql.upper():
        return sql, 0

    # Try sqlglot AST first — handles whitespace, comments, and nested
    # parens correctly.
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:  # pragma: no cover - sqlglot is a hard dep, but be safe.
        sqlglot = None  # type: ignore[assignment]

    count = 0
    if sqlglot is not None:
        try:
            tree = sqlglot.parse_one(sql, read="databricks")
        except Exception:
            tree = None
        if tree is not None:
            agg_class_names = {
                "Sum", "Avg", "Count", "Min", "Max", "Median",
                "Stddev", "StddevPop", "StddevSamp",
                "Variance", "VariancePop", "VarianceSamp",
                "AnyValue",
            }
            for node in list(tree.walk()):
                # ``walk()`` yields tuples in some sqlglot versions.
                expr_node = node[0] if isinstance(node, tuple) else node
                if not isinstance(expr_node, exp.AggFunc):
                    continue
                if type(expr_node).__name__ not in agg_class_names:
                    continue
                arg = expr_node.this
                if arg is None:
                    continue
                # Single-arg aggregate only. ``args`` may carry
                # ``distinct``/``order_by`` siblings — those are fine
                # to drop with the outer agg.
                if (
                    isinstance(arg, exp.Anonymous)
                    and str(arg.name or "").upper() == "MEASURE"
                ):
                    expr_node.replace(arg.copy())
                    count += 1
            if count:
                try:
                    return tree.sql(dialect="databricks"), count
                except Exception:
                    pass  # fall through to regex
            else:
                # AST traversed cleanly with no rewrites — done.
                return sql, 0

    # Regex fallback — used when sqlglot fails to parse OR fails to
    # render. Conservative: the inner-MEASURE arg list is matched as
    # a single ``[^()]*`` chunk so MEASURE calls with embedded parens
    # (rare but possible inside CASE expressions) are skipped.
    new_sql, n = _OUTER_AGG_AROUND_MEASURE_RE.subn(r"\2", sql)
    return new_sql, n


def _repair_order_by_measure_alias(sql: str) -> tuple[str, int]:
    """PR 31 — Strip ``MEASURE()`` around a SELECT alias in ``ORDER BY``.

    Spark accepts ``ORDER BY MEASURE(<measure_col>)`` only when the
    operand resolves to an MV measure column on a FROM-side metric
    view. When the LLM emits ``ORDER BY MEASURE(<select_alias>)``
    where the alias is itself a SELECT projection that already
    contains a ``MEASURE(...)`` call, Spark rejects the outer
    ``MEASURE()`` because the alias resolves to an aggregate
    expression, not a measure column.

    The deterministic fix is to replace ``MEASURE(<alias>)`` in the
    ORDER BY clause with the bare ``<alias>``. Conservative: returns
    ``(sql, 0)`` unchanged when sqlglot is unavailable, parsing
    fails, the SQL has no SELECT-list MEASURE-aliased projections,
    or the ORDER BY does not reference any such alias.

    Returns ``(new_sql, count)`` — ``count`` is the number of
    ORDER BY MEASURE-of-alias substitutions applied. Used by the
    shared pre-execute repair hook so unified, preflight, and
    cluster synthesis paths apply the same repair before warehouse
    EXPLAIN/execute.
    """
    if not sql or "ORDER BY" not in sql.upper() or "MEASURE" not in sql.upper():
        return sql, 0
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return sql, 0
    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return sql, 0
    if not isinstance(tree, exp.Select):
        return sql, 0

    # Collect SELECT-list aliases that are MEASURE(...) projections.
    measure_aliases: set[str] = set()
    for proj in tree.expressions or []:
        if not isinstance(proj, exp.Alias):
            continue
        alias_id = proj.args.get("alias")
        if alias_id is None or not alias_id.name:
            continue
        inner = proj.this
        if (
            isinstance(inner, exp.Anonymous)
            and str(inner.name or "").upper() == "MEASURE"
        ):
            measure_aliases.add(str(alias_id.name).lower())
    if not measure_aliases:
        return sql, 0

    order = tree.args.get("order")
    if order is None:
        return sql, 0

    count = 0
    for ob in order.find_all(exp.Ordered):
        inner = ob.this
        if not (
            isinstance(inner, exp.Anonymous)
            and str(inner.name or "").upper() == "MEASURE"
        ):
            continue
        # MEASURE() arity is exactly 1 (a column reference). Only
        # rewrite when the single arg is a bare column whose lower
        # name matches a SELECT alias we identified above.
        args = inner.args.get("expressions") or []
        if len(args) != 1:
            continue
        arg = args[0]
        if not isinstance(arg, exp.Column) or arg.table:
            continue
        if (arg.name or "").lower() not in measure_aliases:
            continue
        ob.set("this", exp.Column(this=exp.to_identifier(arg.name)))
        count += 1

    if not count:
        return sql, 0
    try:
        return tree.sql(dialect="databricks"), count
    except Exception:
        return sql, 0


_NUMERIC_CAST_TYPES: frozenset[str] = frozenset({
    "int", "integer", "bigint", "smallint", "tinyint", "long", "short",
    "float", "double", "decimal", "numeric", "real",
})


def _is_numeric_value(s: Any) -> bool:
    """Return True when *s* parses as a numeric literal.

    Used by the categorical-cast guardrail to decide whether sampled
    values for a column would survive a numeric cast at execute time.
    Tolerates leading/trailing whitespace, signs, decimals, and
    scientific notation. Booleans, dates, and free-text values
    (``"Y"`` / ``"yes"`` / etc.) all return ``False``.
    """
    if s is None:
        return False
    text = str(s).strip()
    if not text:
        return False
    try:
        float(text)
        return True
    except (TypeError, ValueError):
        return False


def _profile_lookup(
    data_profile: dict | None,
    column_name: str,
    *,
    table_hint: str | None = None,
) -> dict | None:
    """Find the per-column profile entry for *column_name* in
    *data_profile*.

    The data profile is keyed by fully-qualified table identifier;
    column names are case-insensitive. When *table_hint* is provided
    we look there first; otherwise we scan every table and return
    the first hit. Returns ``None`` when no profile entry exists for
    the column.
    """
    if not isinstance(data_profile, dict) or not column_name:
        return None
    cn = column_name.strip().lower()
    if not cn:
        return None
    if table_hint:
        tinfo = data_profile.get(table_hint)
        if not isinstance(tinfo, dict):
            tinfo = data_profile.get(table_hint.lower())
        if isinstance(tinfo, dict):
            cols = tinfo.get("columns") or {}
            for k, v in cols.items():
                if isinstance(v, dict) and str(k).lower() == cn:
                    return v
    for tinfo in data_profile.values():
        if not isinstance(tinfo, dict):
            continue
        cols = tinfo.get("columns") or {}
        for k, v in cols.items():
            if isinstance(v, dict) and str(k).lower() == cn:
                return v
    return None


def _repair_order_by_measure_renamed_collision(
    sql: str,
    rename_map: dict[str, str],
) -> tuple[str, int]:
    """Task 6 — Rewrite ``ORDER BY MEASURE(<original>)`` to the renamed alias.

    The alias-collision repair (``_repair_measure_alias_collisions``)
    renames ``MEASURE(m) AS m`` to ``MEASURE(m) AS m_value``. If the
    same SQL also contains ``ORDER BY MEASURE(m) DESC``, Spark's
    planner re-resolves ``MEASURE(m)`` against the SELECT alias
    output rather than the underlying measure column and rejects
    with the same MISSING_ATTRIBUTES error class. The deterministic
    fix is to rewrite the ORDER BY to reference the renamed alias.

    Returns ``(new_sql, count)``.
    """
    if not sql or not rename_map or "ORDER BY" not in sql.upper():
        return sql, 0

    new_sql = sql
    count = 0
    for old_measure, new_alias in rename_map.items():
        pattern = re.compile(
            rf"ORDER\s+BY(?P<body>.*?)(?P<measure>MEASURE\s*\(\s*`?{re.escape(old_measure)}`?\s*\))",
            re.IGNORECASE | re.DOTALL,
        )

        def _sub(match: re.Match) -> str:
            nonlocal count
            count += 1
            return (
                "ORDER BY"
                + match.group("body")
                + new_alias
            )

        new_sql = pattern.sub(_sub, new_sql)

    return new_sql, count


def check_categorical_cast_violations(
    sql: str,
    data_profile: dict | None,
) -> list[tuple[str, str, list[str]]]:
    """PR 32 — Detect ``CAST(<col> AS <numeric_type>)`` whose argument
    is a categorical string column with non-numeric sample values.

    Returns a list of ``(column_name, target_type, sample_values)``
    tuples for each offending cast. Empty list means the SQL has no
    such violation (no casts at all, every cast is on a numeric or
    unknown column, or every cast's column has all-numeric sampled
    values).

    Conservative: returns ``[]`` when *sql* is empty, ``data_profile``
    is empty, or sqlglot is unavailable / fails to parse.
    Cast targets that aren't recognised numeric SQL types (e.g.
    ``CAST(col AS DATE)``) are skipped — only numeric casts are
    guarded because that's the failure class observed in production
    (categorical Y/N flags being cast to BIGINT).
    """
    if not sql or not isinstance(data_profile, dict) or not data_profile:
        return []
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return []
    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return []

    out: list[tuple[str, str, list[str]]] = []
    seen: set[tuple[str, str]] = set()
    for cast in tree.find_all(exp.Cast):
        target = cast.args.get("to")
        if target is None:
            continue
        try:
            target_sql = target.sql(dialect="databricks").strip().lower()
        except Exception:
            continue
        # Strip arity/precision (``DECIMAL(38,2)`` → ``decimal``).
        target_kind = target_sql.split("(")[0].strip()
        if target_kind not in _NUMERIC_CAST_TYPES:
            continue
        operand = cast.this
        if not isinstance(operand, exp.Column):
            continue
        col_name = (operand.name or "").strip()
        if not col_name:
            continue
        table_hint = (operand.table or "").strip() or None
        cinfo = _profile_lookup(data_profile, col_name, table_hint=table_hint)
        if not isinstance(cinfo, dict):
            continue
        vals = cinfo.get("distinct_values")
        if not isinstance(vals, (list, tuple)) or not vals:
            continue
        non_numeric = [str(v) for v in vals if not _is_numeric_value(v)]
        if not non_numeric:
            continue
        key = (col_name.lower(), target_kind)
        if key in seen:
            continue
        seen.add(key)
        out.append((col_name, target_kind, list(non_numeric)[:5]))
    return out


def check_categorical_type_coercion_violations(
    sql: str,
    data_profile: dict | None,
) -> list[tuple[str, str, list[str]]]:
    """Task 7 — Detect implicit numeric coercion of categorical strings.

    Wraps :func:`check_categorical_cast_violations` (explicit casts)
    and additionally surfaces ``WHERE col = 1`` / ``WHERE col IN (0, 1)``
    shapes against columns whose data profile records non-numeric
    distinct values (e.g. ``["Y", "N"]``). Databricks SQL injects an
    implicit cast at planning time which fails as ``CAST_INVALID_INPUT``.

    Returns a list of ``(column, "numeric_comparison" | "<numeric_type>", samples)``
    tuples. The ``numeric_comparison`` kind is new — explicit-cast
    violations keep the legacy kind from
    :func:`check_categorical_cast_violations` so callers that switch
    over still see the same shape.
    """
    out = list(check_categorical_cast_violations(sql, data_profile))
    if not sql or not isinstance(data_profile, dict) or not data_profile:
        return out
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return out
    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return out

    seen = {(col.lower(), kind) for col, kind, _samples in out}

    def _column_non_numeric_samples(col):
        col_name = (getattr(col, "name", "") or "").strip()
        if not col_name:
            return None
        cinfo = _profile_lookup(
            data_profile,
            col_name,
            table_hint=(getattr(col, "table", "") or "").strip() or None,
        )
        if not isinstance(cinfo, dict):
            return None
        vals = cinfo.get("distinct_values")
        if not isinstance(vals, (list, tuple)) or not vals:
            return None
        non_numeric = [str(v) for v in vals if not _is_numeric_value(v)]
        if not non_numeric:
            return None
        return col_name, non_numeric[:5]

    def _is_numeric_literal(expr):
        if isinstance(expr, exp.Literal):
            if expr.is_number:
                return True
            if expr.is_string:
                return _is_numeric_value(expr.this)
        return False

    comparison_types = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
    for comp in tree.find_all(*comparison_types):
        left = comp.left
        right = comp.right
        for maybe_col, maybe_lit in ((left, right), (right, left)):
            if isinstance(maybe_col, exp.Column) and _is_numeric_literal(maybe_lit):
                col_samples = _column_non_numeric_samples(maybe_col)
                if col_samples is None:
                    continue
                col_name, samples = col_samples
                key = (col_name.lower(), "numeric_comparison")
                if key not in seen:
                    seen.add(key)
                    out.append((col_name, "numeric_comparison", samples))

    for in_expr in tree.find_all(exp.In):
        col = in_expr.this
        if not isinstance(col, exp.Column):
            continue
        expressions = in_expr.args.get("expressions") or []
        if not any(_is_numeric_literal(e) for e in expressions):
            continue
        col_samples = _column_non_numeric_samples(col)
        if col_samples is None:
            continue
        col_name, samples = col_samples
        key = (col_name.lower(), "numeric_comparison")
        if key not in seen:
            seen.add(key)
            out.append((col_name, "numeric_comparison", samples))

    return out


def apply_pre_execute_repairs(
    sql: str,
    *,
    mv_measures: dict[str, set[str]] | None = None,
    mv_short_set: set[str] | None = None,
    canonical_assets: list[str] | dict | None = None,
    counters: dict[str, int] | None = None,
) -> str:
    """PR 31 — Apply the deterministic repair pipeline before execute.

    Runs the same MV/CTE rewrites used by the unified-correction
    pipeline so the unified, preflight, and cluster-synthesis paths
    all converge on the same SQL shape before paying for a warehouse
    EXPLAIN/execute. Each repair is conservative: when its
    pre-conditions don't apply (no MEASURE refs, no MV in JOIN,
    sqlglot parse failure), the repair is a no-op.

    Order matters and matches the unified correction sequence:

    1. ``repair_stemmed_identifiers_in_sql`` — promote bare table
       stems to fully-qualified identifiers (so the qualification
       gate doesn't reject SQL that the deterministic repair could
       fix).
    2. ``_rewrite_measure_refs`` — wrap bare measure columns in
       ``MEASURE()`` in SELECT / ORDER BY positions.
    3. ``_strip_outer_agg_around_measure`` — collapse
       ``SUM(MEASURE(x))`` to ``MEASURE(x)``.
    4. ``_repair_measure_alias_collisions`` — rename
       ``MEASURE(x) AS x`` to ``MEASURE(x) AS x_value``.
    5. ``_repair_order_by_measure_alias`` — strip ``MEASURE()`` in
       ORDER BY when the operand is itself a SELECT alias that was
       defined as a MEASURE(...) projection.
    6. ``_repair_measure_in_where`` — lift measure refs from WHERE
       into a CTE-first pattern.
    7. ``_repair_metric_view_join`` — wrap each metric view in a
       JOIN with a CTE so Spark doesn't raise
       ``METRIC_VIEW_JOIN_NOT_SUPPORTED``.

    ``counters`` (when supplied) is mutated in place with a per-step
    increment using the same key names the existing call-sites
    already log:

      - ``repaired_stemmed_identifiers``
      - ``repaired_measure_refs``
      - ``stripped_outer_aggregate_around_measure``
      - ``repaired_measure_alias_collisions``
      - ``repaired_order_by_measure_alias``
      - ``repaired_measure_in_where``
      - ``repaired_metric_view_join``

    Returns the repaired SQL (or *sql* unchanged when no repair
    fired).
    """
    if not sql or not sql.strip():
        return sql

    new_sql = sql

    if canonical_assets:
        try:
            from genie_space_optimizer.optimization.preflight_synthesis import (
                repair_stemmed_identifiers_in_sql,
            )
            repaired, stem_subs = repair_stemmed_identifiers_in_sql(
                new_sql, canonical_assets,
            )
            if stem_subs:
                new_sql = repaired
                if counters is not None:
                    counters["repaired_stemmed_identifiers"] = (
                        counters.get("repaired_stemmed_identifiers", 0)
                        + len(stem_subs)
                    )
        except Exception:
            pass

    # Compute the alias-collision rename map BEFORE either the measure
    # rewrite or the alias-collision repair runs. The rewrite would
    # otherwise wrap a bare ``orders_diff`` alias as ``MEASURE(orders_diff)``
    # and the collision regex would no longer recognize the shape.
    alias_collision_map = _measure_alias_collision_rename_map(new_sql)

    # Task 8: alias-collision repair MUST run before _rewrite_measure_refs
    # so a bare-identifier alias that matches the underlying measure
    # name (``MEASURE(orders_diff) AS orders_diff``) gets renamed to
    # ``MEASURE(orders_diff) AS orders_diff_value`` before the
    # measure-wrap pass sees the bare alias and wraps it.
    try:
        new_sql, alias_fixes = _repair_measure_alias_collisions(new_sql)
        if alias_fixes and counters is not None:
            counters["repaired_measure_alias_collisions"] = (
                counters.get("repaired_measure_alias_collisions", 0) + alias_fixes
            )
    except Exception:
        pass

    if mv_measures:
        try:
            wrapped = _rewrite_measure_refs(new_sql, mv_measures)
            if wrapped != new_sql:
                before = len(re.findall(r"\bMEASURE\s*\(", new_sql, re.IGNORECASE))
                after = len(re.findall(r"\bMEASURE\s*\(", wrapped, re.IGNORECASE))
                new_sql = wrapped
                if counters is not None and after > before:
                    counters["repaired_measure_refs"] = (
                        counters.get("repaired_measure_refs", 0) + (after - before)
                    )
        except Exception:
            pass

        try:
            stripped, strip_count = _strip_outer_agg_around_measure(new_sql)
            if strip_count:
                new_sql = stripped
                if counters is not None:
                    counters["stripped_outer_aggregate_around_measure"] = (
                        counters.get("stripped_outer_aggregate_around_measure", 0)
                        + strip_count
                    )
        except Exception:
            pass

    try:
        new_sql, ob_fixes = _repair_order_by_measure_alias(new_sql)
        if ob_fixes and counters is not None:
            counters["repaired_order_by_measure_alias"] = (
                counters.get("repaired_order_by_measure_alias", 0) + ob_fixes
            )
    except Exception:
        pass

    # Task 6: rewrite ``ORDER BY MEASURE(<original_measure>)`` to the
    # renamed alias when the alias-collision repair fired. Same
    # counter as the alias-strip path so existing diagnostics keep
    # tracking the same ORDER BY signal.
    try:
        new_sql, renamed_ob_fixes = _repair_order_by_measure_renamed_collision(
            new_sql, alias_collision_map,
        )
        if renamed_ob_fixes and counters is not None:
            counters["repaired_order_by_measure_alias"] = (
                counters.get("repaired_order_by_measure_alias", 0)
                + renamed_ob_fixes
            )
    except Exception:
        pass

    if mv_measures:
        try:
            new_sql, where_lifts = _repair_measure_in_where(new_sql, mv_measures)
            if where_lifts and counters is not None:
                counters["repaired_measure_in_where"] = (
                    counters.get("repaired_measure_in_where", 0) + where_lifts
                )
        except Exception:
            pass

    if mv_short_set:
        try:
            join_reason = _check_metric_view_join_pre(new_sql, mv_short_set)
            if join_reason:
                repaired_join, join_wraps = _repair_metric_view_join(
                    new_sql, mv_short_set, mv_measures,
                )
                if join_wraps:
                    new_sql = repaired_join
                    if counters is not None:
                        counters["repaired_metric_view_join"] = (
                            counters.get("repaired_metric_view_join", 0) + join_wraps
                        )
        except Exception:
            pass

    return new_sql


# ─────────────────────────────────────────────────────────────────────
# Centralized metric-view error matchers
# ─────────────────────────────────────────────────────────────────────
# Spark Connect emits metric-view rejections under two spellings of the
# same error class — ``METRIC_VIEW_UNSUPPORTED_USAGE`` is the form we
# observe in GRPC traces today, ``UNSUPPORTED_METRIC_VIEW_USAGE`` is the
# spelling Databricks documentation uses; both refer to the same Spark
# planner rejection. ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` and
# ``METRIC_VIEW_JOIN_NOT_SUPPORTED`` are the two more-specific subclasses
# we already dispatch on. Centralizing the marker list here keeps the
# four call-sites — preflight data profiling, preflight synthesis
# measure-repair, benchmark validation MEASURE-hint gating, and benchmark
# validation MV-join detection — in lock-step when a new MV error class
# eventually shows up.

_MV_ERROR_MARKERS: tuple[str, ...] = (
    "UNSUPPORTED_METRIC_VIEW_USAGE",
    "METRIC_VIEW_UNSUPPORTED_USAGE",
    "METRIC_VIEW_MISSING_MEASURE_FUNCTION",
    "METRIC_VIEW_JOIN_NOT_SUPPORTED",
)


def is_metric_view_error(reason: Any) -> bool:
    """Return True when *reason* names any known metric-view error class.

    Accepts ``None`` and non-string inputs (returns ``False``) so call
    sites can pass exception messages, ``GateResult.reason`` payloads,
    or raw strings interchangeably.
    """
    if reason is None:
        return False
    if not isinstance(reason, str):
        try:
            reason = str(reason)
        except Exception:
            return False
    upper = reason.upper()
    return any(marker in upper for marker in _MV_ERROR_MARKERS)


def metric_view_error_kind(reason: Any) -> str | None:
    """Return a stable kind string for the metric-view error in *reason*.

    Returns one of ``"unsupported_usage"`` (the generic planner rejection
    that surfaces as either ``METRIC_VIEW_UNSUPPORTED_USAGE`` or
    ``UNSUPPORTED_METRIC_VIEW_USAGE``), ``"missing_measure"``,
    ``"join_not_supported"``, or ``None`` when the input does not name a
    metric-view error.

    When the payload mentions multiple kinds (e.g. a generic
    unsupported_usage frame that also references the more specific
    missing_measure subclass), the most specific kind wins so callers
    that dispatch on the kind string get the right repair behaviour.
    """
    if reason is None:
        return None
    if not isinstance(reason, str):
        try:
            reason = str(reason)
        except Exception:
            return None
    upper = reason.upper()
    if "METRIC_VIEW_MISSING_MEASURE_FUNCTION" in upper:
        return "missing_measure"
    if "METRIC_VIEW_JOIN_NOT_SUPPORTED" in upper:
        return "join_not_supported"
    if (
        "UNSUPPORTED_METRIC_VIEW_USAGE" in upper
        or "METRIC_VIEW_UNSUPPORTED_USAGE" in upper
    ):
        return "unsupported_usage"
    return None


def _entry_has_measure_columns(entry: Any) -> bool:
    """Return True if a data-source entry has any measure-typed column.

    Genie's serialized space sometimes places a metric view under
    ``data_sources.tables`` rather than ``data_sources.metric_views``
    (depends on whether the user formally registered it as an MV in
    the space; the underlying UC asset is still a metric view and
    Spark enforces the ``MEASURE()`` contract regardless of where the
    config records it). The deterministic signal is a column_config
    with ``column_type == "measure"`` or ``is_measure: True`` — both
    indicate a column that must be wrapped in ``MEASURE()`` when
    referenced in a SELECT/ORDER BY against the asset. Keeps this
    function side-effect free so callers can reuse it cheaply across
    the synthesis hot path.
    """
    if not isinstance(entry, dict):
        return False
    for cc in entry.get("column_configs", []) or []:
        if not isinstance(cc, dict):
            continue
        if str(cc.get("column_type", "")).lower() == "measure":
            return True
        if cc.get("is_measure"):
            return True
    return False


def _iter_effective_metric_view_entries(config: dict) -> Iterator[dict]:
    """Yield each effective metric-view data-source entry from *config*.

    Walks both ``data_sources.metric_views`` (always treated as MVs) and
    ``data_sources.tables`` (filtered by :func:`_entry_has_measure_columns`).
    Mirrors the canonical Genie shape so downstream callers can extract
    measures / dimensions without caring which list the MV originally
    landed in. De-duplicates by identifier so a snapshot that pre-reclassified
    one of its MVs cannot double-yield it.
    """
    parsed = config.get("_parsed_space", config)
    if not isinstance(parsed, dict):
        return
    ds = parsed.get("data_sources", {})
    if not isinstance(ds, dict):
        return
    seen: set[str] = set()
    for mv in ds.get("metric_views", []) or []:
        if not isinstance(mv, dict):
            continue
        ident = (mv.get("identifier") or "").strip().lower()
        if ident and ident in seen:
            continue
        if ident:
            seen.add(ident)
        yield mv
    for tbl in ds.get("tables", []) or []:
        if not isinstance(tbl, dict):
            continue
        if not _entry_has_measure_columns(tbl):
            continue
        ident = (tbl.get("identifier") or "").strip().lower()
        if ident and ident in seen:
            continue
        if ident:
            seen.add(ident)
        yield tbl


def effective_metric_view_identifiers(config: dict) -> set[str]:
    """Return the set of identifier strings for all effective metric views.

    The "effective" view unifies entries that Genie placed under
    ``metric_views`` with entries placed under ``tables`` whose column
    configs declare measures — the only signal Spark cares about when
    enforcing the ``MEASURE()`` contract. Used by the MV ``SELECT *``
    guard, the MEASURE auto-wrap rewriter, the metric-view prompt
    block, and the data-profile skip-list so all four agree on what
    counts as an MV regardless of how Genie's serializer happened to
    classify it on this fetch.
    """
    out: set[str] = set()
    for mv in _iter_effective_metric_view_entries(config):
        ident = (mv.get("identifier") or "").strip()
        if ident:
            out.add(ident)
    return out


def effective_metric_view_identifiers_with_catalog(config: dict) -> set[str]:
    """Like :func:`effective_metric_view_identifiers` plus catalog detection.

    Unions the column-config heuristic (which only fires when Genie's
    serialized space declares a measure-typed column on the entry) with
    the runtime catalog detection cached at ``config["_metric_view_yaml"]``
    by :func:`preflight._detect_metric_views_via_catalog`.

    Use this variant from sites that gate on "is this asset an MV?" —
    MEASURE auto-wrap, MV ``SELECT *`` guard, MV prompt block, and the
    data-profile skip-list. Without the catalog union we miss MVs that
    Genie serialized under ``data_sources.tables`` without measure
    column configs (the actual failure mode that motivated the helper).

    PR 30 — When ``config["_asset_semantics"]`` is populated, the
    semantics map is the primary source of truth and the legacy union
    is folded in as a safety net for callers that pre-date the
    contract.
    """
    out: set[str] = set()
    base_lower: set[str] = set()

    try:
        from genie_space_optimizer.common.asset_semantics import (
            metric_view_identifiers as _sem_mv_idents,
        )
        for ident in _sem_mv_idents(config):
            if ident:
                out.add(ident)
                base_lower.add(ident.lower())
    except Exception:
        pass

    base = effective_metric_view_identifiers(config)
    for ident in base:
        if ident and ident.lower() not in base_lower:
            out.add(ident)
            base_lower.add(ident.lower())

    cache = config.get("_metric_view_yaml")
    if not isinstance(cache, dict):
        _ps = config.get("_parsed_space")
        if isinstance(_ps, dict):
            cache = _ps.get("_metric_view_yaml")
    if isinstance(cache, dict):
        for ident in cache.keys():
            ident_str = str(ident).strip()
            if ident_str and ident_str.lower() not in base_lower:
                out.add(ident_str)
                base_lower.add(ident_str.lower())
    return out


def effective_table_identifiers(config: dict) -> set[str]:
    """Return identifiers from ``_tables`` that are not effective MVs.

    Excludes ``data_sources.tables`` entries reclassified as metric
    views by :func:`effective_metric_view_identifiers_with_catalog` so
    callers enumerating "real" tables (e.g. data profiling, table
    allowlist rendering) skip MV-shaped entries without manual
    filtering.
    """
    mv_idents = {
        ident.lower()
        for ident in effective_metric_view_identifiers_with_catalog(config)
    }
    out: set[str] = set()
    for tbl in config.get("_tables", []) or []:
        ident = str(tbl).strip()
        if ident and ident.lower() not in mv_idents:
            out.add(ident)
    return out


def build_metric_view_measures(config: dict) -> dict[str, set[str]]:
    """Build ``{lowered_short_name: {measure_col, ...}}`` for all effective MVs.

    "Effective" means we walk three sources and union the results:

    1. ``data_sources.metric_views`` — Genie's explicit MV serialization.
    2. ``data_sources.tables`` entries with at least one measure-typed
       column config (legacy serialization where MVs land under tables).
    3. ``config["_metric_view_yaml"]`` — the catalog-detection cache
       populated by :func:`metric_view_catalog.detect_metric_views_via_catalog`.
       This is the *only* path that catches MVs whose Genie payload omits
       both ``column_type='measure'`` and ``is_measure``, which is the
       common-in-production failure mode that PR 19 fixes.

    This is the single source of truth used by the MEASURE auto-wrap
    rewriter — keeping detection here ensures the unified pipeline, the
    preflight pipeline, and the benchmark/example correction loops all
    rewrite the same set of columns.

    PR 30 — When ``config["_asset_semantics"]`` is populated, the
    semantics map is consulted first and its measures are unioned with
    the legacy paths. This keeps the rewriter consistent across every
    detection ladder while preserving back-compat for snapshots that
    pre-date the contract.
    """
    result: dict[str, set[str]] = {}

    try:
        from genie_space_optimizer.common.asset_semantics import (
            metric_view_measures_by_short_name as _sem_measures,
        )
        for short, ms in _sem_measures(config).items():
            if not short or not ms:
                continue
            existing = result.setdefault(short, set())
            existing.update(ms)
    except Exception:
        pass

    for mv in _iter_effective_metric_view_entries(config):
        identifier = mv.get("identifier", "")
        short_name = identifier.split(".")[-1].lower() if identifier else ""
        if not short_name:
            continue
        measures: set[str] = set()
        for cc in mv.get("column_configs", []) or []:
            if not isinstance(cc, dict):
                continue
            col_name = cc.get("column_name", "")
            if not col_name:
                continue
            col_type = str(cc.get("column_type", "")).lower()
            if col_type == "measure" or cc.get("is_measure"):
                measures.add(col_name.lower())
        if measures:
            result[short_name] = measures

    # PR 19: union with the catalog-detection cache. The cache is keyed
    # by fully-qualified, lower-cased identifier; the rewriter keys on
    # the bare short name so we collapse to the last segment.
    cache = config.get("_metric_view_yaml") or {}
    if not cache:
        parsed = config.get("_parsed_space")
        if isinstance(parsed, dict):
            cache = parsed.get("_metric_view_yaml") or {}
    if isinstance(cache, dict):
        for fq_ident, yaml_doc in cache.items():
            if not isinstance(yaml_doc, dict):
                continue
            short = str(fq_ident).split(".")[-1].lower()
            if not short:
                continue
            measures = result.setdefault(short, set())
            for m in yaml_doc.get("measures") or []:
                if isinstance(m, dict):
                    name = m.get("name")
                    if isinstance(name, str) and name:
                        measures.add(name.lower())
        # Drop any short names whose measure set ended up empty (e.g. an
        # entry made it into the cache but the YAML had no measures
        # block) so callers don't probe rewriter logic against empty
        # sets.
        result = {k: v for k, v in result.items() if v}
    return result


def _count_mv_detection_sources(config: dict) -> dict[str, int]:
    """Count how many MVs each detection path contributed (PR 21).

    Returns ``{"config": int, "column_flags": int, "catalog": int}``::

      - ``config``       — entries declared under ``data_sources.metric_views``.
      - ``column_flags`` — entries serialized under ``data_sources.tables``
        but with at least one measure-typed column_config (Genie's older
        "tables-shaped" MV serialization).
      - ``catalog``      — entries discovered at runtime by
        :func:`metric_view_catalog.detect_metric_views_via_catalog` and
        cached under ``config["_metric_view_yaml"]``. Only catalog-only
        finds (i.e. not already counted by either of the prior buckets)
        are included so the three counts add up to a unique-MV total.

    Used by the unified and preflight banners so log readers can attribute
    a missing-MEASURE() failure cluster to the right detection source — a
    catalog-only count of zero with a positive ``column_flags`` count
    means Genie *did* serialize the MV but stripped the YAML block, and
    a flat zero across all three is the canonical "no MVs at all" state
    that PR 21's adaptive-overdraw short-circuit keys on.
    """
    config_ids: set[str] = set()
    flag_ids: set[str] = set()
    parsed = config.get("_parsed_space", config)
    ds = parsed.get("data_sources", {}) if isinstance(parsed, dict) else {}
    if not isinstance(ds, dict):
        ds = {}
    for mv in (ds.get("metric_views") or []):
        if isinstance(mv, dict):
            ident = str(mv.get("identifier") or mv.get("name") or "").strip().lower()
            if ident:
                config_ids.add(ident)
    for tbl in (ds.get("tables") or []):
        if not isinstance(tbl, dict):
            continue
        ident = str(tbl.get("identifier") or "").strip().lower()
        if not ident or ident in config_ids:
            continue
        for cc in tbl.get("column_configs") or []:
            if not isinstance(cc, dict):
                continue
            col_type = str(cc.get("column_type", "")).lower()
            if col_type == "measure" or cc.get("is_measure"):
                flag_ids.add(ident)
                break

    catalog_ids: set[str] = set()
    cache = config.get("_metric_view_yaml") or {}
    if not cache:
        parsed = config.get("_parsed_space")
        if isinstance(parsed, dict):
            cache = parsed.get("_metric_view_yaml") or {}
    if isinstance(cache, dict):
        for fq in cache.keys():
            ident = str(fq).strip().lower()
            if ident and ident not in config_ids and ident not in flag_ids:
                catalog_ids.add(ident)

    # PR 30 — also fold any semantics-only MVs (e.g. Genie ``metric_views``
    # entries reclassified via profiling that never made it into
    # ``_metric_view_yaml``) into the catalog bucket so the banner
    # diagnostic count is never lower than the semantics count.
    try:
        from genie_space_optimizer.common.asset_semantics import (
            metric_view_identifiers as _sem_mv_idents,
        )
        for ident in _sem_mv_idents(config):
            il = (ident or "").strip().lower()
            if (
                il
                and il not in config_ids
                and il not in flag_ids
                and il not in catalog_ids
            ):
                catalog_ids.add(il)
    except Exception:
        pass

    return {
        "config": len(config_ids),
        "column_flags": len(flag_ids),
        "catalog": len(catalog_ids),
    }


def _parse_struct_field_names(data_type: str) -> list[str]:
    """Return top-level struct field names from a Spark ``struct<…>`` type.

    Mirrors :func:`optimization.optimizer._parse_struct_field_names` but
    duplicated locally to avoid a cross-module import in the hot SQL repair
    path. Tracks angle / paren depth so nested types don't bleed top-level
    fields. Returns an empty list when the type is not a struct.
    """
    if not data_type:
        return []
    s = data_type.strip()
    if not s.lower().startswith("struct<") or not s.endswith(">"):
        return []
    body = s[len("struct<"):-1]
    fields: list[str] = []
    depth = 0
    cursor = 0
    for i, ch in enumerate(body):
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        elif ch == "," and depth == 0:
            chunk = body[cursor:i]
            cursor = i + 1
            colon = chunk.find(":")
            if colon > 0:
                fields.append(chunk[:colon].strip())
    chunk = body[cursor:]
    colon = chunk.find(":")
    if colon > 0:
        fields.append(chunk[:colon].strip())
    return [f for f in fields if f]


def build_table_columns(
    config: dict,
) -> dict[str, dict[str, set[str]]]:
    """Build per-table column / struct-column index from the Genie config.

    Returns ``{lower_short_name: {"columns": {…}, "struct_columns": {…}}}``
    covering both ``data_sources.tables`` and ``data_sources.metric_views``.
    Used by :func:`_check_dangling_qualifiers` to decide whether a
    ``<qual>.<col>`` reference can be resolved against the FROM/JOIN tables.
    """
    result: dict[str, dict[str, set[str]]] = {}
    parsed = config.get("_parsed_space", config)
    if not isinstance(parsed, dict):
        return result
    ds = parsed.get("data_sources", {})
    if not isinstance(ds, dict):
        return result
    sources: list[dict] = []
    sources.extend(ds.get("tables", []) or [])
    sources.extend(ds.get("metric_views", []) or [])
    for tbl in sources:
        if not isinstance(tbl, dict):
            continue
        identifier = (tbl.get("identifier") or tbl.get("name") or "").strip()
        short = identifier.split(".")[-1].lower()
        if not short:
            continue
        columns: set[str] = set()
        struct_columns: set[str] = set()
        for cc in tbl.get("column_configs", []) or []:
            if not isinstance(cc, dict):
                continue
            col_name = (cc.get("column_name") or cc.get("name") or "").strip()
            if not col_name:
                continue
            columns.add(col_name.lower())
            data_type = str(cc.get("data_type", "") or "")
            if _parse_struct_field_names(data_type):
                struct_columns.add(col_name.lower())
        existing = result.setdefault(
            short, {"columns": set(), "struct_columns": set()},
        )
        existing["columns"].update(columns)
        existing["struct_columns"].update(struct_columns)
    return result


# Match an unquoted identifier reference of the form ``qual.tail`` where
# ``tail`` may be a single name. Allows backticks around either side. The
# regex purposely excludes the catalog.schema.table 3-part form by
# requiring the previous character not be a word char or backtick.
_QUALIFIED_REF_RE = re.compile(
    r"(?:(?<![\w`.])|(?<=^))`?([A-Za-z_]\w*)`?\s*\.\s*`?([A-Za-z_]\w*)`?",
)

# Token sets to skip when scanning for ``<qual>.<col>`` shapes:
#   - SQL keywords that often appear before a dotted ref but aren't quals.
#   - Catalog/schema chunks. These appear in the FROM/JOIN clause only;
#     we strip those clauses out before scanning so the head of any
#     remaining dotted ref must be a table or alias.
_SQL_RESERVED_BEFORE_DOT = frozenset({
    "select", "from", "where", "group", "order", "by", "having",
    "join", "inner", "left", "right", "full", "cross", "outer",
    "on", "and", "or", "not", "in", "is", "null", "as", "distinct",
    "case", "when", "then", "else", "end", "with", "union", "intersect",
    "except", "values", "limit", "offset", "fetch", "next", "rows",
    "only", "between", "like", "ilike", "exists", "cast", "interval",
})


def _strip_from_join_clauses(sql: str) -> str:
    """Return *sql* with FROM/JOIN clause heads removed up to the next
    statement keyword.

    We only want to flag ``<qual>.<col>`` references that appear in
    SELECT / WHERE / GROUP BY / HAVING / ORDER BY positions — the FROM
    and JOIN clauses legitimately carry ``catalog.schema.table`` forms
    where the head of the dot is a catalog or schema name, not a column
    qualifier. Stripping those substrings out before scanning avoids
    false-positive flags on the catalog component.
    """
    if not sql:
        return sql
    # Match: FROM/JOIN <ws> <table-spec> until the next clause boundary.
    # The terminator is the next SQL clause keyword or end of statement.
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\b[\s\S]*?"
        r"(?=\b(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|INTERSECT|EXCEPT|"
        r"FROM|JOIN|ON|WHEN|END|CROSS|INNER|LEFT|RIGHT|FULL|OUTER)\b|$|;|\))",
        re.IGNORECASE,
    )
    return pattern.sub(" ", sql)


def _extract_cte_names(sql: str) -> set[str]:
    """PR 31 — Extract top-level CTE names declared in a ``WITH`` clause.

    Recognizes the CTE-first pattern produced by the metric-view
    repair path::

        WITH __mv_1 AS (SELECT ... FROM cat.sch.mv1),
             base   AS (SELECT ... FROM cat.sch.fact)
        SELECT base.col, __mv_1.measure_value FROM base ...

    Without this, the dangling-qualifier check rejects every CTE
    alias as unresolved because ``base`` and ``__mv_1`` never appear
    on a FROM/JOIN clause's *table* slot — only as references later
    in the query body.

    Implementation is regex-based and bounded by the AS-paren depth
    to avoid scanning into subqueries. ``RECURSIVE`` is honored.
    """
    out: set[str] = set()
    if not sql:
        return out

    # Find the first WITH (case-insensitive) at the top level. We don't
    # try to recover from arbitrary leading whitespace/comments — both
    # are tolerated by the simple regex.
    with_match = re.search(
        r"\bWITH\s+(?:RECURSIVE\s+)?",
        sql,
        re.IGNORECASE,
    )
    if not with_match:
        return out

    # CTE names are followed by ``AS`` (with optional column list in
    # parens). We parse depth-aware to locate each CTE definition's
    # closing paren before grabbing the next name.
    pos = with_match.end()
    n = len(sql)
    cte_pattern = re.compile(
        r"\s*`?([A-Za-z_]\w*)`?\s*(?:\([^()]*\))?\s*AS\s*\(",
        re.IGNORECASE,
    )
    while pos < n:
        m = cte_pattern.match(sql, pos)
        if not m:
            break
        cte_name = m.group(1).lower()
        if cte_name and cte_name not in _SQL_RESERVED_BEFORE_DOT:
            out.add(cte_name)
        # Walk past the CTE body — count parens starting at the opening one.
        depth = 1
        i = m.end()
        while i < n and depth > 0:
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        # After the body, expect either ``,`` (next CTE) or end of WITH.
        # Skip whitespace.
        while i < n and sql[i].isspace():
            i += 1
        if i < n and sql[i] == ",":
            pos = i + 1
            continue
        break
    return out


def _extract_from_join_aliases(sql: str) -> set[str]:
    """Return the set of effective qualifiers visible in FROM/JOIN clauses.

    For each entry the set includes:
      - The table's short name (last dot component) — covers unaliased
        references like ``mv_x.col``.
      - The alias when present — covers ``mv_x AS x`` / ``mv_x x``.
      - PR 31 — CTE names declared in any ``WITH`` clause, so
        ``FROM base`` and ``base.col`` references resolve when ``base``
        is a top-level CTE rather than an actual table.

    Implementation is regex-based to avoid a hard sqlglot dependency in
    the hot repair path. Handles backticks and ``AS`` keyword. The alias
    group uses a negative lookahead to avoid consuming the next clause
    keyword (e.g. ``JOIN cat.sch.t ON …`` — ``ON`` is not the alias) so
    multiple FROM/JOIN entries in the same statement all get indexed.
    """
    out: set[str] = set()
    # The alias group's identifier must NOT be a reserved keyword, so we
    # exclude FROM/JOIN/ON/WHERE/etc to keep them anchoring boundaries.
    reserved_alts = "|".join(sorted(_SQL_RESERVED_BEFORE_DOT, key=len, reverse=True))
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+`?([\w.]+)`?"
        rf"(?:\s+(?:AS\s+)?`?(?!(?:{reserved_alts})\b)([A-Za-z_]\w*)`?)?",
        re.IGNORECASE,
    )
    for m in pattern.finditer(sql):
        ident = (m.group(1) or "").strip()
        alias = (m.group(2) or "").strip()
        if ident:
            short = ident.split(".")[-1]
            if short and short.lower() not in _SQL_RESERVED_BEFORE_DOT:
                out.add(short.lower())
        if alias and alias.lower() not in _SQL_RESERVED_BEFORE_DOT:
            out.add(alias.lower())
    # PR 31 — also accept CTE names declared in a ``WITH`` clause.
    out.update(_extract_cte_names(sql))
    return out


def _check_dangling_qualifiers(
    sql: str,
    table_columns: dict[str, dict[str, set[str]]],
) -> list[str]:
    """Detect ``<qual>.<col>`` references whose qualifier isn't in scope.

    A qualifier is in scope when it is one of:
      - A FROM/JOIN table short name (``FROM mv_x`` → ``mv_x``).
      - An explicit alias (``FROM mv_x AS x`` → ``x``; ``JOIN t y`` → ``y``).
      - The name of a struct column on any FROM/JOIN table — covers
        ``dim_location.region`` where ``dim_location`` is a struct column
        on a metric view in FROM.

    Anything else is dangling. The most common shape we want to catch is
    the LLM analogising ``dim_location.region`` (real struct field) onto
    ``dim_date.year`` (a separate metric view that must be JOINed).

    Returns a sorted list of unresolved qualifier strings (deduplicated).
    Empty list means the SQL has no dangling qualifier — does NOT mean the
    SQL is otherwise valid (downstream EXPLAIN still owns truth).
    """
    if not sql or not sql.strip() or not table_columns:
        return []

    aliases = _extract_from_join_aliases(sql)
    if not aliases:
        return []

    # Collect struct column names visible from any FROM/JOIN table.
    visible_struct_cols: set[str] = set()
    for alias in aliases:
        info = table_columns.get(alias)
        if info:
            visible_struct_cols |= info.get("struct_columns", set())

    allowed = aliases | visible_struct_cols

    # Strip out FROM/JOIN tails so catalog.schema.table doesn't generate
    # false positives.
    body = _strip_from_join_clauses(sql)

    unresolved: set[str] = set()
    for m in _QUALIFIED_REF_RE.finditer(body):
        qual = m.group(1).lower()
        if qual in _SQL_RESERVED_BEFORE_DOT:
            continue
        # Skip when the match is part of a longer dotted chain
        # (``cat.sch.tbl.col`` or ``cat.sch.tbl``). The regex matches
        # only the first two segments so the trailing ``.`` would still
        # be present in the body. A 3+ part column reference is fine
        # in Spark SQL when the prefix matches a FROM table; the head
        # catalog/schema components must NOT be flagged.
        end = m.end()
        if end < len(body) and body[end:end + 1] == ".":
            continue
        if qual in allowed:
            continue
        unresolved.add(qual)

    return sorted(unresolved)


_SELECT_STAR_RE = re.compile(r"\bSELECT\s+\*\s+FROM\b", re.IGNORECASE)


def _guard_mv_select_star(
    sql: str,
    metric_view_names: set[str],
) -> tuple[bool, str]:
    """Reject ``SELECT *`` queries that target metric views.

    Returns ``(is_ok, reason)``.  When *is_ok* is False the benchmark
    should be sent to the correction pipeline or quarantined.
    """
    if not _SELECT_STAR_RE.search(sql):
        return True, ""
    sql_lower = sql.lower()
    mv_leaves = {n.lower().split(".")[-1] for n in metric_view_names}
    for mv in mv_leaves:
        if mv in sql_lower:
            return (
                False,
                f"SELECT * not supported on metric view '{mv}' "
                "— must explicitly list dimensions and MEASURE() columns",
            )
    return True, ""


@dataclass(frozen=True)
class TemporalIntent:
    """Detected temporal intent from a question's relative time reference."""
    keyword: str
    start_date: date
    end_date: date


_TEMPORAL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bthis\s+year\b", re.I), "this_year"),
    (re.compile(r"\bytd\b|\byear[\s-]to[\s-]date\b", re.I), "ytd"),
    (re.compile(r"\bthis\s+month\b", re.I), "this_month"),
    (re.compile(r"\bthis\s+quarter\b", re.I), "this_quarter"),
    (re.compile(r"\blast\s+quarter\b", re.I), "last_quarter"),
    (re.compile(r"\blast\s+year\b", re.I), "last_year"),
    (re.compile(r"\blast\s+(\d+)\s+months?\b", re.I), "last_n_months"),
    (re.compile(r"\blast\s+(\d+)\s+days?\b", re.I), "last_n_days"),
]

_DATE_LITERAL_RE = re.compile(r"'(\d{4}-\d{2}-\d{2})'")
_EXPLICIT_YEAR_RE = re.compile(r"\bfor\s+(\d{4})\b|\bin\s+(\d{4})\b|\byear\s+(\d{4})\b", re.I)


def _quarter_start(d: date) -> date:
    """Return the first day of the quarter containing *d*."""
    return date(d.year, ((d.month - 1) // 3) * 3 + 1, 1)


def _month_offset(d: date, months: int) -> date:
    """Shift *d* by *months* (positive or negative), clamping the day."""
    m = d.month + months
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    import calendar
    max_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, max_day))


def _detect_temporal_intent(
    question: str,
    *,
    today: date | None = None,
) -> TemporalIntent | None:
    """Detect relative temporal references in *question* and compute a date range.

    Returns ``None`` when the question has no relative time phrase or when
    an explicit year is mentioned (e.g. "for 2025").
    """
    if not question:
        return None
    today = today or date.today()

    for pat, keyword in _TEMPORAL_PATTERNS:
        m = pat.search(question)
        if not m:
            continue

        if keyword == "this_year" or keyword == "ytd":
            start = date(today.year, 1, 1)
            end = today
        elif keyword == "this_month":
            start = date(today.year, today.month, 1)
            end = today
        elif keyword == "this_quarter":
            start = _quarter_start(today)
            end = today
        elif keyword == "last_quarter":
            qs = _quarter_start(today)
            end = qs - timedelta(days=1)
            start = _quarter_start(end)
        elif keyword == "last_year":
            start = date(today.year - 1, 1, 1)
            end = date(today.year - 1, 12, 31)
        elif keyword == "last_n_months":
            n = int(m.group(1))
            start = _month_offset(today, -n)
            end = today
        elif keyword == "last_n_days":
            n = int(m.group(1))
            start = today - timedelta(days=n)
            end = today
        else:
            continue

        explicit = _EXPLICIT_YEAR_RE.search(question)
        if explicit:
            explicit_year = int(next(g for g in explicit.groups() if g))
            if start.year <= explicit_year <= end.year:
                return None

        return TemporalIntent(keyword=keyword, start_date=start, end_date=end)

    return None


def _rewrite_temporal_dates(
    gt_sql: str,
    intent: TemporalIntent,
) -> tuple[str, dict | None]:
    """Replace hardcoded date literals in *gt_sql* with *intent* dates.

    Returns ``(rewritten_sql, metadata_dict | None)``.
    ``metadata_dict`` is ``None`` when no rewriting was needed.
    """
    if not gt_sql:
        return gt_sql, None

    literals = _DATE_LITERAL_RE.findall(gt_sql)
    if not literals:
        return gt_sql, None

    sorted_dates = sorted(set(literals))
    gt_start = sorted_dates[0]
    gt_end = sorted_dates[-1]

    gt_start_year = int(gt_start[:4])
    gt_end_year = int(gt_end[:4])
    if intent.start_date.year == gt_start_year and intent.end_date.year == gt_end_year:
        return gt_sql, None

    new_start = intent.start_date.isoformat()
    new_end = intent.end_date.isoformat()

    rewritten = gt_sql
    if len(sorted_dates) >= 2:
        rewritten = rewritten.replace(f"'{gt_start}'", f"'{new_start}'")
        rewritten = rewritten.replace(f"'{gt_end}'", f"'{new_end}'")
    else:
        rewritten = rewritten.replace(f"'{gt_start}'", f"'{new_start}'")

    if rewritten == gt_sql:
        return gt_sql, None

    metadata = {
        "keyword": intent.keyword,
        "original_dates": [gt_start, gt_end] if len(sorted_dates) >= 2 else [gt_start],
        "rewritten_dates": [new_start, new_end] if len(sorted_dates) >= 2 else [new_start],
    }
    return rewritten, metadata


def normalize_result_df(df: pd.DataFrame | None) -> pd.DataFrame:
    """Deterministic normalization of a result DataFrame.

    Sort columns alphabetically, sort rows, round floats to 4 decimals,
    normalize timestamps to UTC, strip whitespace.  We use 4 decimals
    rather than 6 because GT (via Spark toPandas) and Genie (via REST API)
    serialize floats at different precisions.

    The Genie Statement Execution API returns all values as strings
    (including scientific notation like ``1.75E7``), so we attempt
    ``pd.to_numeric`` on object columns before rounding.
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df[sorted(df.columns)]
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().any() and converted.notna().sum() >= df[col].notna().sum() * 0.5:
            df[col] = converted
    _BOOL_CANONICAL = {"true": "true", "false": "false"}
    for col in df.select_dtypes(include=["bool"]).columns:
        df[col] = df[col].astype(str).str.lower()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: _BOOL_CANONICAL.get(x.lower(), x)
            if isinstance(x, str) and x.lower() in _BOOL_CANONICAL
            else x
        )
    for col in df.select_dtypes(include=["number"]).columns:
        if df[col].dtype.kind == "i":
            df[col] = df[col].astype("float64")
        if df[col].dtype.kind == "f":
            df[col] = df[col].round(4)
    for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[col] = pd.to_datetime(df[col], utc=True)
    df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    return df


def result_signature(df: pd.DataFrame | None) -> dict:
    """Schema hash + rowcount + numeric sums for result comparison."""
    if df is None or df.empty:
        return {"schema_hash": "", "row_count": 0, "numeric_sums": {}}
    schema_str = ",".join(f"{c}:{df[c].dtype}" for c in sorted(df.columns))
    schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]
    numeric_sums: dict[str, float] = {}
    for col in df.select_dtypes(include=["number"]).columns:
        numeric_sums[col] = round(float(df[col].sum()), 4)
    return {
        "schema_hash": schema_hash,
        "row_count": len(df),
        "numeric_sums": numeric_sums,
    }


def build_asi_metadata(
    failure_type: str = "other",
    severity: str = "minor",
    confidence: float = 0.5,
    wrong_clause: str | None = None,
    blame_set: list[str] | None = None,
    quoted_metadata_text: str | None = None,
    missing_metadata: str | None = None,
    ambiguity_detected: bool = False,
    expected_value: str | None = None,
    actual_value: str | None = None,
    counterfactual_fix: str | None = None,
    affected_question_pattern: str | None = None,
    join_assessment: dict | None = None,
    expected_objects: list[str] | None = None,
    actual_objects: list[str] | None = None,
    rca_kind: str | None = None,
    patch_family: str | None = None,
    recommended_levers: list[int] | None = None,
    **extra: Any,
) -> dict:
    """Build an ASI metadata dict conforming to ASI_SCHEMA."""
    md: dict = {
        "failure_type": failure_type if failure_type in FAILURE_TAXONOMY else "other",
        "severity": severity,
        "confidence": confidence,
        "wrong_clause": wrong_clause,
        "blame_set": blame_set or [],
        "quoted_metadata_text": quoted_metadata_text,
        "missing_metadata": missing_metadata,
        "ambiguity_detected": ambiguity_detected,
        "expected_value": expected_value,
        "actual_value": actual_value,
        "counterfactual_fix": counterfactual_fix,
        "affected_question_pattern": affected_question_pattern,
    }
    if join_assessment and isinstance(join_assessment, dict):
        md["join_assessment"] = join_assessment
    if expected_objects:
        md["expected_objects"] = expected_objects
    if actual_objects:
        md["actual_objects"] = actual_objects
    if rca_kind:
        md["rca_kind"] = rca_kind
    if patch_family:
        md["patch_family"] = patch_family
    if recommended_levers:
        md["recommended_levers"] = recommended_levers
    for key, value in extra.items():
        if value not in (None, "", [], {}):
            md[key] = value
    return md


def format_asi_markdown(
    *,
    judge_name: str,
    value: str,
    rationale: str,
    metadata: dict | None = None,
    extra: dict | None = None,
    question_id: str | None = None,
) -> str:
    """Render scorer feedback in a structured markdown + JSON ASI format.

    When *question_id* is provided, the payload is also written to
    ``_SCORER_FEEDBACK_CACHE`` so that downstream code (``run_evaluation``)
    can recover rationale / metadata even when MLflow's eval_results table
    only stores the verdict value.
    """
    verdict_map = {
        "yes": "Pass",
        "no": "Fail",
        "unknown": "Unknown",
        "skipped": "Skipped",
        "genie_correct": "Pass",
        "both_correct": "Pass",
        "ground_truth_correct": "Fail",
        "neither_correct": "Fail",
    }
    verdict = verdict_map.get(value, value)
    rationale_text = (rationale or "").strip() or "No rationale provided."

    genie_eval_summary = ""
    if isinstance(metadata, dict):
        genie_eval_summary = format_genie_eval_summary(
            metadata.get("genie_equivalent_eval")
        )
    if genie_eval_summary and genie_eval_summary not in rationale_text:
        rationale_text = f"{genie_eval_summary}\n\n{rationale_text}"

    payload: dict[str, Any] = {
        "judge": judge_name,
        "verdict": verdict,
        "raw_value": value,
        "failure_type": None,
        "severity": None,
        "wrong_clause": None,
        "missing_metadata": None,
        "expected_value": None,
        "actual_value": None,
        "counterfactual_fix": None,
        "blame_set": [],
        "confidence": None,
        "rationale": rationale_text,
    }
    if metadata:
        for key in (
            "failure_type",
            "severity",
            "wrong_clause",
            "missing_metadata",
            "expected_value",
            "actual_value",
            "counterfactual_fix",
            "blame_set",
            "confidence",
            "quoted_metadata_text",
            "ambiguity_detected",
            "affected_question_pattern",
            "genie_equivalent_eval",
        ):
            if key in metadata:
                payload[key] = metadata[key]
    if extra:
        payload.update(extra)

    if question_id:
        cache_meta = {
            k: payload[k]
            for k in ("failure_type", "severity", "wrong_clause", "blame_set",
                       "confidence", "counterfactual_fix")
            if payload.get(k) is not None
        }
        _cache_scorer_feedback(question_id, judge_name, rationale_text, cache_meta)

    _MLFLOW_ASSESSMENT_LIMIT = 60_000
    raw = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if len(raw) > _MLFLOW_ASSESSMENT_LIMIT:
        for bulky_key in ("comparison", "llm_response", "extra"):
            if bulky_key in payload:
                payload[bulky_key] = "(truncated — exceeds MLflow 64KB limit)"
        raw = json.dumps(payload, indent=2, sort_keys=True, default=str)

    return (
        f"### {judge_name}\n"
        f"**Verdict:** {verdict}\n\n"
        f"{rationale_text}\n\n"
        "```json\n"
        f"{raw}\n"
        "```"
    )


def _parse_asi_from_rationale(rationale: str) -> dict:
    """Extract the ASI JSON payload embedded in a ``format_asi_markdown`` rationale.

    Handles both real newlines and literal ``\\n`` sequences that arise when
    the rationale survives a SQL round-trip through ``_esc`` / ``_opt_json``.
    """
    if not rationale:
        return {}
    _MARKERS = [
        ("```json\n", "\n```"),
        ("```json\\n", "\\n```"),
    ]
    for start_marker, end_marker in _MARKERS:
        try:
            start = rationale.index(start_marker) + len(start_marker)
            end = rationale.index(end_marker, start)
            json_text = rationale[start:end]
            if "\\n" in json_text and "\n" not in json_text:
                json_text = json_text.replace("\\n", "\n").replace("\\t", "\t")
            return json.loads(json_text)
        except (ValueError, json.JSONDecodeError):
            continue
    return {}


def _extract_assessments_from_traces(results_df) -> dict[int, dict[str, dict]]:
    """Pull scorer rationale + metadata from trace or assessments columns.

    Returns ``{row_index: {judge_name: {"rationale": str, "metadata": dict}}}``.

    Checks three sources in order:
    1. ``trace.data.assessments`` / ``trace.info.assessments`` (legacy path)
    2. Top-level ``assessments`` column (MLflow genai >=2.x puts Feedback
       objects here directly)
    3. Falls back gracefully if nothing is available.
    """
    out: dict[int, dict[str, dict]] = {}

    has_trace = "trace" in results_df.columns
    has_assessments = "assessments" in results_df.columns

    if not has_trace and not has_assessments:
        return out

    for row_idx, (_, row) in enumerate(results_df.iterrows()):
        assessments = None

        if has_trace:
            trace = row.get("trace")
            if trace is not None:
                for attr_chain in [("data", "assessments"), ("info", "assessments")]:
                    obj = trace
                    for attr in attr_chain:
                        obj = getattr(obj, attr, None)
                        if obj is None:
                            break
                    if obj is not None:
                        assessments = obj
                        break

        if not assessments and has_assessments:
            raw = row.get("assessments")
            if isinstance(raw, list):
                assessments = raw
            elif raw is not None and hasattr(raw, "__iter__"):
                try:
                    assessments = list(raw)
                except Exception:
                    pass

        if not assessments:
            continue

        row_data: dict[str, dict] = {}
        for a in assessments:
            if isinstance(a, dict):
                name = a.get("name", "") or ""
                rationale_raw = a.get("rationale", "") or ""
                meta = a.get("metadata")
                if not isinstance(meta, dict):
                    meta = {}
            else:
                name = getattr(a, "name", "") or ""
                rationale_raw = getattr(a, "rationale", "") or ""
                meta = getattr(a, "metadata", None)
                if not isinstance(meta, dict):
                    meta = {}
            if not meta:
                meta = _parse_asi_from_rationale(rationale_raw)
            if name:
                row_data[name] = {"rationale": rationale_raw, "metadata": meta}
        out[row_idx] = row_data
    return out


def _fetch_assessments_for_recovered_qids(
    trace_map: dict[str, str],
) -> dict[str, dict[str, dict]]:
    """Fetch judge rationale/metadata via ``mlflow.get_trace`` for recovered traces.

    Phase 2.2: when ``mlflow.genai.evaluate`` loses trace context and
    ``_recover_trace_map`` falls back to tag/time-window search, the
    recovery returns ``{qid: trace_id}`` but the assessments are still
    not joined onto the data rows. This helper closes that gap by
    fetching each recovered trace and pulling its assessments
    explicitly.

    Returns ``{qid: {judge_name: {"rationale": str, "metadata": dict}}}``.
    Failures (missing trace, RPC error) are tolerated silently per qid;
    the caller treats absent qids as no-data and falls through to other
    assessment sources.
    """
    out: dict[str, dict[str, dict]] = {}
    if not trace_map:
        return out

    for qid, trace_id in trace_map.items():
        if not qid or not trace_id:
            continue
        try:
            trace = mlflow.get_trace(trace_id)
        except Exception:
            logger.debug(
                "Failed to fetch trace %s for qid=%s", trace_id, qid,
                exc_info=True,
            )
            continue
        if trace is None:
            continue

        assessments: Any = None
        for attr_chain in (("data", "assessments"), ("info", "assessments")):
            obj: Any = trace
            for attr in attr_chain:
                obj = getattr(obj, attr, None)
                if obj is None:
                    break
            if obj:
                assessments = obj
                break
        if not assessments:
            continue

        row_data: dict[str, dict] = {}
        for a in assessments:
            if isinstance(a, dict):
                name = a.get("name", "") or ""
                rationale_raw = a.get("rationale", "") or ""
                meta = a.get("metadata")
                if not isinstance(meta, dict):
                    meta = {}
            else:
                name = getattr(a, "name", "") or ""
                rationale_raw = getattr(a, "rationale", "") or ""
                meta = getattr(a, "metadata", None)
                if not isinstance(meta, dict):
                    meta = {}
            if not meta:
                meta = _parse_asi_from_rationale(rationale_raw)
            if name:
                row_data[name] = {
                    "rationale": rationale_raw,
                    "metadata": meta,
                }
        if row_data:
            out[qid] = row_data
    return out


def _merge_row_sources(
    row_dict: dict[str, Any],
    assessment_map_row: dict[str, dict] | None,
    cached_feedback_qid: dict[str, dict] | None,
    recovered_assessments_qid: dict[str, dict] | None = None,
) -> dict[str, Any]:
    """Reconcile judge rationale/metadata from up to four sources.

    Precedence (authoritative first):

    1. Trace assessments by row index (``assessment_map_row``) — what
       MLflow stored in the trace and joined directly to the row.
    2. Phase 2.2: recovered trace assessments by qid
       (``recovered_assessments_qid``) — fetched via ``mlflow.get_trace``
       after ``_recover_trace_map`` reattached a trace_id to a qid that
       lost its row-level join. Same authority level as (1) but reached
       only when (1) is silent.
    3. The run-scoped scorer feedback cache (``cached_feedback_qid``).
    4. Any ``<judge>/rationale`` / ``<judge>/metadata`` column already
       present in ``row_dict``.

    Mutates and returns ``row_dict``. Only overwrites keys for judges that
    actually have data in the higher-priority source; untouched judges
    keep whatever the flat columns contain.
    """
    assessment_map_row = assessment_map_row or {}
    cached_feedback_qid = cached_feedback_qid or {}
    recovered_assessments_qid = recovered_assessments_qid or {}

    judge_names: set[str] = (
        set(assessment_map_row)
        | set(cached_feedback_qid)
        | set(recovered_assessments_qid)
    )

    # Phase 2.3: emit a single per-row source breadcrumb so the
    # iteration log can show "ASI present from N rows" instead of the
    # blanket ``none=100%`` we observed at iter-1.
    _source_used: str | None = None

    for judge_name in judge_names:
        rat_key = f"{judge_name}/rationale"
        meta_key = f"{judge_name}/metadata"

        trace_data = assessment_map_row.get(judge_name) or {}
        trace_rationale = trace_data.get("rationale")
        trace_metadata = trace_data.get("metadata")

        recovered_data = recovered_assessments_qid.get(judge_name) or {}
        recovered_rationale = recovered_data.get("rationale")
        recovered_metadata = recovered_data.get("metadata")

        cache_data = cached_feedback_qid.get(judge_name) or {}
        cache_rationale = cache_data.get("rationale")
        cache_metadata = cache_data.get("metadata")

        if trace_rationale:
            row_dict[rat_key] = trace_rationale
            _source_used = _source_used or "trace"
        elif recovered_rationale:
            row_dict[rat_key] = recovered_rationale
            _source_used = _source_used or "recovered_trace"
        elif cache_rationale:
            row_dict[rat_key] = cache_rationale
            _source_used = _source_used or "cache"

        if trace_metadata:
            row_dict[meta_key] = trace_metadata
            _source_used = _source_used or "trace"
        elif recovered_metadata:
            row_dict[meta_key] = recovered_metadata
            _source_used = _source_used or "recovered_trace"
        elif cache_metadata:
            row_dict[meta_key] = cache_metadata
            _source_used = _source_used or "cache"

    if _source_used:
        row_dict["_asi_source"] = _source_used

    return row_dict


def normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    """Convert 0-1 scale → 0-100 scale; leave 0-100 unchanged."""
    normalized: dict[str, float] = {}
    for key, val in scores.items():
        if 0 <= val <= 1.0:
            normalized[key] = round(val * 100, 2)
        else:
            normalized[key] = round(val, 2)
    return normalized


def all_thresholds_met(
    scores: dict[str, float],
    targets: dict[str, float] | None = None,
) -> bool:
    """Return True only when every judge meets its threshold.

    ``scores`` should be on a 0-100 scale. ``targets`` defaults to
    ``DEFAULT_THRESHOLDS`` from config.
    """
    targets = targets or DEFAULT_THRESHOLDS
    for judge, threshold in targets.items():
        actual = scores.get(judge)
        if actual is None:
            return False
        if actual < threshold:
            return False
    return True


# ── Asset Type Normalization ───────────────────────────────────────────

_VALID_ASSET_TYPES = frozenset({"MV", "TVF", "TABLE"})


def _normalize_expected_asset(
    raw: Any,
    expected_sql: str,
    hint: Any = None,
) -> str:
    """Normalize ``expected_asset`` to a valid type category.

    Resolution precedence (default scoring-v2 mode):

    1. ``raw`` — if it is already one of ``MV``/``TVF``/``TABLE`` use it.
       Benchmarks authored post-fix will populate this explicitly.
    2. ``hint`` (``expected_asset_hint`` on the benchmark) — explicit
       author override used when the stored ``expected_asset`` is a
       table *name* rather than a type category. This beats detection
       and prevents ``detect_asset_type`` from mis-labeling tables that
       happen to start with ``mv_`` (B1 companion fix).
    3. Fallback to ``detect_asset_type(expected_sql)``.

    Under ``GSO_SCORING_V2=off`` the hint is ignored to preserve
    byte-identical legacy behavior.
    """
    upper = raw.strip().upper() if isinstance(raw, str) and raw else ""
    if upper in _VALID_ASSET_TYPES:
        return upper
    if not scoring_v2_is_legacy():
        hint_upper = (
            hint.strip().upper() if isinstance(hint, str) and hint else ""
        )
        if hint_upper in _VALID_ASSET_TYPES:
            return hint_upper
    return detect_asset_type(expected_sql)


# ── Arbiter-Adjusted Accuracy ──────────────────────────────────────────

_ARBITER_CORRECT_VERDICTS = frozenset({"genie_correct", "both_correct"})


def _rc_str(row: dict) -> str:
    """Extract the ``result_correctness`` value as a lowercase string.

    Eval rows may use the MLflow-flattened ``feedback/result_correctness/value``
    form or the legacy ``result_correctness/value`` form; both are recognized.
    """
    val = (
        row.get("feedback/result_correctness/value")
        or row.get("result_correctness/value")
        or row.get("result_correctness")
        or ""
    )
    return str(val).strip().lower()


def _arbiter_str(row: dict) -> str:
    """Extract the arbiter verdict as a lowercase string.

    Eval rows may use the MLflow-flattened ``feedback/arbiter/value`` form or
    the legacy ``arbiter/value`` form; both are recognized.
    """
    val = (
        row.get("feedback/arbiter/value")
        or row.get("arbiter/value")
        or row.get("arbiter")
        or ""
    )
    return str(val).strip().lower()


def row_is_hard_failure(row: dict) -> bool:
    """Tier 1.4: Unified hard-failure predicate shared by accuracy and clustering.

    A row is a *hard* failure iff BOTH:
      - ``result_correctness`` is definitively ``no`` (case-insensitive), AND
      - the arbiter verdict is NOT in the correct set (i.e. not ``both_correct``
        and not ``genie_correct``).

    Rationale: the accept gate already counts rows as correct when either
    ``rc == "yes"`` OR arbiter overrides say so (see
    ``_compute_arbiter_adjusted_accuracy``). Clustering previously used arbiter
    alone, which produced phantom hard clusters for rows where ``rc == "yes"``
    but arbiter flagged a semantic issue. Sharing this predicate closes that
    gap and prevents the ghost-ceiling loop.
    """
    rc = _rc_str(row)
    av = _arbiter_str(row)
    rc_is_no = rc in ("no", "false", "0", "0.0")
    return rc_is_no and av not in _ARBITER_CORRECT_VERDICTS


def classify_genie_shape_patterns(row: dict) -> dict | None:
    """Tier 2.13 / 2.14: detect Genie behaviour patterns from the eval row.

    Returns a dict with ``failure_type`` (one of ``over_filtered_dimension``
    or ``wide_vs_long_shape``) plus ``wrong_clause`` and ``blame_set`` keys
    when the row matches a known pattern, else ``None``. Callers can stamp
    this into the row's ASI metadata before clustering so the strategist
    sees a distinct failure_type instead of a generic
    ``wrong_filter_condition``/``wrong_aggregation``.

    Patterns:

    - ``over_filtered_dimension``: Genie added a ``<col> IS NOT NULL``
      predicate that the ground truth does not have, and Genie returned
      fewer rows than GT. Observed in the lever-loop regression run on
      Q14/Q18 (Genie added ``zone_combination IS NOT NULL`` unprompted).
    - ``wide_vs_long_shape``: Genie returned ``k * gt_rows`` rows with an
      extra dimension column (typically ``time_window``). Observed on Q20.
    """
    import re as _re

    _resp = row.get("response") or {}
    if isinstance(_resp, str):
        try:
            _resp = json.loads(_resp)
        except (json.JSONDecodeError, TypeError):
            _resp = {}
    _comparison = _resp.get("comparison", {}) if isinstance(_resp, dict) else {}
    if not isinstance(_comparison, dict):
        return None

    gt_rows = _comparison.get("gt_row_count")
    genie_rows = _comparison.get("genie_row_count")
    if not isinstance(gt_rows, (int, float)) or not isinstance(genie_rows, (int, float)):
        return None

    genie_sql = (
        _resp.get("response", "") if isinstance(_resp, dict) else ""
    )
    _req = row.get("request") or {}
    if isinstance(_req, str):
        try:
            _req = json.loads(_req)
        except (json.JSONDecodeError, TypeError):
            _req = {}
    expected_sql = _req.get("expected_sql", "") if isinstance(_req, dict) else ""

    if not isinstance(genie_sql, str) or not isinstance(expected_sql, str):
        return None

    genie_upper = genie_sql.upper()
    expected_upper = expected_sql.upper()

    # over_filtered_dimension: Genie added an IS NOT NULL predicate GT doesn't have.
    if int(gt_rows) > int(genie_rows) > 0:
        _isnull_pat = _re.compile(r"`?([\w.]+)`?\s+IS\s+NOT\s+NULL", _re.IGNORECASE)
        genie_isnull_cols = {m.group(1).split(".")[-1].lower() for m in _isnull_pat.finditer(genie_sql)}
        gt_isnull_cols = {m.group(1).split(".")[-1].lower() for m in _isnull_pat.finditer(expected_sql)}
        spurious = genie_isnull_cols - gt_isnull_cols
        if spurious:
            return {
                "failure_type": "over_filtered_dimension",
                "wrong_clause": "WHERE",
                "blame_set": sorted(spurious),
            }

    # wide_vs_long_shape: Genie returned 2× or 3× rows with a time_window-ish col.
    if int(genie_rows) > int(gt_rows) > 0:
        _ratio = int(genie_rows) / max(int(gt_rows), 1)
        if 1.5 <= _ratio <= 4.5 and (int(genie_rows) % int(gt_rows) == 0):
            _select_cols_pat = _re.compile(r"\bSELECT\s+(.+?)\s+FROM", _re.IGNORECASE | _re.DOTALL)
            _gm = _select_cols_pat.search(genie_sql)
            _em = _select_cols_pat.search(expected_sql)
            if _gm and _em:
                _g_cols = {c.strip().split()[-1].strip("`,").lower() for c in _gm.group(1).split(",")}
                _e_cols = {c.strip().split()[-1].strip("`,").lower() for c in _em.group(1).split(",")}
                extra_cols = _g_cols - _e_cols
                for _col in extra_cols:
                    if _col in ("time_window", "time_period", "period", "window", "grain"):
                        return {
                            "failure_type": "wide_vs_long_shape",
                            "wrong_clause": "SELECT",
                            "blame_set": [_col],
                        }
    return None


@dataclass
class RowExclusion:
    """Why a single benchmark row was dropped from the accuracy denominator.

    Feeds the UI drill-down that answers "where did this question go?" — see
    Bug #3 in the plan. ``reason_code`` is a stable enum (UI can swap copy);
    ``reason_detail`` is a human sentence that may include the underlying SQL
    error message for operator debugging.
    """

    question_id: str
    question_text: str | None = None
    reason_code: str = ""
    reason_detail: str = ""


@dataclass
class ArbiterAdjustedResult:
    """Return value of ``_compute_arbiter_adjusted_accuracy``.

    Adding this type replaces a brittle 4-tuple and gives the persistence and
    API layers a single source of truth for the denominator (``evaluated_count``)
    of ``overall_accuracy``.

    Tier 1.7: ``both_correct_count`` / ``both_correct_rate`` expose the stricter
    accuracy anchor (only rows where arbiter said ``both_correct``). Used by
    the lever loop to avoid ghost-ceiling rejections when ``overall_accuracy``
    is inflated by arbiter overrides of rc=yes rows whose SQL is semantically
    wrong.
    """

    accuracy_pct: float
    correct_count: int
    evaluated_count: int
    excluded_count: int
    failure_ids: list[str] = field(default_factory=list)
    exclusions: list[RowExclusion] = field(default_factory=list)
    both_correct_count: int = 0
    both_correct_rate: float = 0.0


# Stable per-row exclusion reason codes. Keep in sync with
# ui/lib/exclusion-reason.ts.
EXCLUSION_GT_EXCLUDED = "gt_excluded"
EXCLUSION_BOTH_EMPTY = "both_empty"
EXCLUSION_GENIE_RESULT_UNAVAILABLE = "genie_result_unavailable"
EXCLUSION_QUARANTINED = "quarantined"
EXCLUSION_TEMPORAL_STALE = "temporal_stale"


_OBJECTIVE_BLOCKING_EXCLUSIONS = frozenset({
    EXCLUSION_GT_EXCLUDED,
    EXCLUSION_GENIE_RESULT_UNAVAILABLE,
})


def objective_blocking_exclusion_count(rows: list[dict]) -> int:
    """Count exclusions that should prevent 100% objective completion."""
    count = 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        err_type = str(
            row.get("outputs/comparison/error_type")
            or row.get("outputs.comparison.error_type")
            or row.get("comparison.error_type")
            or ""
        ).strip().lower()
        rc = str(
            row.get("feedback/result_correctness/value")
            or row.get("result_correctness/value")
            or row.get("result_correctness")
            or ""
        ).strip().lower()
        if rc == "excluded":
            count += 1
        elif err_type == "genie_result_unavailable":
            count += 1
    return count


def _extract_row_signals(row: dict) -> dict[str, Any]:
    """Extract the commonly-needed fields from a raw evaluation row.

    Centralizes the "which key holds what" logic (inputs/question_id vs
    question_id vs inputs.question_id vs request.kwargs) so the denominator
    and the exclusion labeling can't drift.
    """
    rc = str(
        row.get("result_correctness/value", row.get("result_correctness", ""))
    ).lower()

    err_type = str(
        row.get("outputs/comparison/error_type")
        or row.get("comparison/error_type")
        or row.get("comparison.error_type")
        or ""
    ).lower()

    err_message = str(
        row.get("outputs/comparison/error")
        or row.get("comparison/error")
        or row.get("comparison.error")
        or ""
    )

    rq_obj: Any = row.get("request") or {}
    if isinstance(rq_obj, str):
        try:
            rq_obj = json.loads(rq_obj)
        except (json.JSONDecodeError, TypeError):
            rq_obj = {}
    rqk = rq_obj.get("kwargs", {}) if isinstance(rq_obj, dict) else {}

    qid = str(
        row.get("inputs/question_id")
        or (row.get("inputs") or {}).get("question_id", "")
        or row.get("question_id")
        or rqk.get("question_id")
        or (rq_obj.get("question_id") if isinstance(rq_obj, dict) else None)
        or ""
    )

    question_text = (
        row.get("inputs/question")
        or (row.get("inputs") or {}).get("question")
        or row.get("question")
        or rqk.get("question")
    )
    if question_text is not None:
        question_text = str(question_text)

    av = str(row.get("arbiter/value", row.get("arbiter", "skipped"))).lower()

    return {
        "rc": rc,
        "err_type": err_type,
        "err_message": err_message,
        "qid": qid,
        "question_text": question_text,
        "arbiter": av,
    }


def _compute_arbiter_adjusted_accuracy(
    rows: list[dict],
    *,
    quarantined_qids: set[str] | None = None,
    temporal_stale_qids: set[str] | None = None,
) -> ArbiterAdjustedResult:
    """Compute overall accuracy that accounts for arbiter overrides.

    A row is considered correct if:
      - ``result_correctness`` == "yes" (results matched), OR
      - ``result_correctness`` == "no" AND arbiter verdict is
        ``genie_correct`` or ``both_correct``

    Rows where ``result_correctness`` == "excluded" (GT-side infrastructure
    failures), whose question is quarantined, whose comparison error_type is
    ``both_empty`` or ``genie_result_unavailable``, or whose question is
    temporal-stale are removed from the denominator entirely. Each exclusion
    is recorded as a ``RowExclusion`` in the returned ``exclusions`` list so
    the UI can explain "why did this question disappear?".
    """
    if not rows:
        return ArbiterAdjustedResult(
            accuracy_pct=0.0,
            correct_count=0,
            evaluated_count=0,
            excluded_count=0,
            failure_ids=[],
            exclusions=[],
            both_correct_count=0,
            both_correct_rate=0.0,
        )

    _quarantined = quarantined_qids or set()
    _temporal_stale = temporal_stale_qids or set()

    total = 0
    correct = 0
    both_correct = 0
    excluded = 0
    failure_ids: list[str] = []
    exclusions: list[RowExclusion] = []

    for row in rows:
        sig = _extract_row_signals(row)
        rc = sig["rc"]
        err_type = sig["err_type"]
        err_message = sig["err_message"]
        qid = sig["qid"]
        question_text = sig["question_text"]

        if rc == "excluded":
            excluded += 1
            exclusions.append(RowExclusion(
                question_id=qid,
                question_text=question_text,
                reason_code=EXCLUSION_GT_EXCLUDED,
                reason_detail=(
                    "Ground truth SQL could not be executed; benchmark marked excluded."
                    + (f" Error: {err_message}" if err_message else "")
                ),
            ))
            continue

        if err_type == "both_empty":
            excluded += 1
            exclusions.append(RowExclusion(
                question_id=qid,
                question_text=question_text,
                reason_code=EXCLUSION_BOTH_EMPTY,
                reason_detail=(
                    "Both expected and actual SQL returned no rows; cannot judge correctness."
                ),
            ))
            continue

        if err_type == "genie_result_unavailable":
            excluded += 1
            exclusions.append(RowExclusion(
                question_id=qid,
                question_text=question_text,
                reason_code=EXCLUSION_GENIE_RESULT_UNAVAILABLE,
                reason_detail=(
                    "Genie did not return a result (typically a SQL execution failure)."
                    + (f" Error: {err_message}" if err_message else "")
                ),
            ))
            continue

        if qid and qid in _quarantined:
            excluded += 1
            exclusions.append(RowExclusion(
                question_id=qid,
                question_text=question_text,
                reason_code=EXCLUSION_QUARANTINED,
                reason_detail="Benchmark failed pre-evaluation validation and was quarantined.",
            ))
            continue

        if qid and qid in _temporal_stale:
            excluded += 1
            exclusions.append(RowExclusion(
                question_id=qid,
                question_text=question_text,
                reason_code=EXCLUSION_TEMPORAL_STALE,
                reason_detail=(
                    "Benchmark references data outside the workspace's available time window."
                ),
            ))
            continue

        total += 1
        av = sig["arbiter"]

        is_correct = rc in ("yes", "true", "1", "1.0") or (
            rc in ("no", "false", "0", "0.0") and av in _ARBITER_CORRECT_VERDICTS
        )

        # Tier 1.7: count both_correct separately so callers can anchor
        # best_accuracy to the stricter rate (rows where arbiter explicitly
        # agreed, not overrides of rc=yes). Note that a row can have
        # arbiter=both_correct even when rc=yes (the arbiter ran anyway and
        # confirmed); we count that here.
        if av == "both_correct":
            both_correct += 1

        if is_correct:
            correct += 1
        else:
            if qid:
                failure_ids.append(str(qid))

    accuracy_pct = round((correct / total) * 100, 2) if total > 0 else 0.0
    both_correct_rate = round((both_correct / total) * 100, 2) if total > 0 else 0.0
    # Dedup qids while preserving first-seen order: duplicate rows for the
    # same qid (repeatability sub-runs, harness retries) would otherwise
    # inflate the persisted ``failure_count`` metric and render a confusing
    # ``Failed questions: [..., q3, q3, ...]`` list.
    deduped_failure_ids = list(dict.fromkeys(failure_ids))
    return ArbiterAdjustedResult(
        accuracy_pct=accuracy_pct,
        correct_count=correct,
        evaluated_count=total,
        excluded_count=excluded,
        failure_ids=deduped_failure_ids,
        exclusions=exclusions,
        both_correct_count=both_correct,
        both_correct_rate=both_correct_rate,
    )


# ── Benchmark Filtering ─────────────────────────────────────────────────


def filter_benchmarks_by_scope(
    benchmarks: list[dict],
    scope: str = "full",
    patched_objects: list[str] | None = None,
    affected_question_ids: set[str] | None = None,
    *,
    baseline_passing_qids: set[str] | None = None,
    stratified: bool | None = None,
) -> list[dict]:
    """Filter benchmarks based on evaluation scope.

    Scopes: "full" (all), "slice" (affected by patches),
    "p0" (priority P0 only), "held_out" (held-out split).

    For "slice" scope, benchmarks are included if:
    - Their required tables/columns overlap with *patched_objects*, OR
    - Their question id is in *affected_question_ids* (from proposal
      clusters).

    Phase 5.1 — stratified slice composition:
      When *stratified* is True (or env-flag ``GSO_SLICE_STRATIFIED=1``)
      and *baseline_passing_qids* is provided, the slice is augmented
      with a 40% sample of baseline-passing questions. This turns the
      slice gate into a regression detector instead of a rubber-stamp:
      patches that improve the targeted questions but break previously-
      passing ones now fail the slice gate.
    """
    if scope == "full":
        return benchmarks
    if scope == "slice":
        patched = {o.lower() for o in patched_objects} if patched_objects else set()
        affected_qids = affected_question_ids or set()
        targeted: list[dict] = []
        for b in benchmarks:
            qid = b.get("id", "")
            if qid and qid in affected_qids:
                targeted.append(b)
                continue
            if patched and any(
                t.lower() in patched
                for t in b.get("required_tables", []) + b.get("required_columns", [])
            ):
                targeted.append(b)

        if stratified is None:
            stratified = (
                os.getenv("GSO_SLICE_STRATIFIED", "1")
                .strip().lower() not in ("0", "false", "no", "off")
            )
        if not (stratified and baseline_passing_qids):
            return targeted

        # 60/40 split: targeted rows fill 60% of the budget; the
        # remaining 40% comes from baseline-passing questions sampled
        # in deterministic order (sorted by qid for repeatability).
        _targeted_qids = {b.get("id") for b in targeted}
        passing_pool = [
            b for b in benchmarks
            if b.get("id") in baseline_passing_qids
            and b.get("id") not in _targeted_qids
        ]
        passing_pool.sort(key=lambda b: str(b.get("id") or ""))
        n_targeted = len(targeted)
        # Aim for ratio targeted:regression = 60:40
        n_regression = max(1, int(n_targeted * (40 / 60))) if n_targeted else 0
        n_regression = min(n_regression, len(passing_pool))
        result = list(targeted) + passing_pool[:n_regression]
        if n_regression:
            logger.info(
                "Phase 5.1: stratified slice = %d targeted + %d "
                "baseline-passing (regression detector)",
                n_targeted, n_regression,
            )
        return result
    if scope == "p0":
        return [b for b in benchmarks if b.get("priority", "P1") == "P0"]
    if scope == "held_out":
        return [b for b in benchmarks if b.get("split") == "held_out"]
    return benchmarks


def _load_known_functions(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> set[str]:
    """Load functions available in the target schema for fast pre-checks."""
    if not catalog or not schema:
        return set()
    try:
        _set_sql_context(spark, catalog, schema)
        rows = spark.sql(f"SHOW USER FUNCTIONS IN `{catalog}`.`{schema}`").collect()
    except Exception:
        logger.warning("Could not list functions for %s.%s", catalog, schema)
        return set()

    known: set[str] = set()
    for row in rows:
        row_dict = row.asDict() if hasattr(row, "asDict") else {}
        raw_name = str(row_dict.get("function") or row_dict.get("name") or "").strip()
        if not raw_name:
            continue
        known.add(raw_name.lower())
        known.add(raw_name.split(".")[-1].lower())
    return known


def _extract_sql_function_calls(sql: str, catalog: str, schema: str) -> set[str]:
    """Extract fully-qualified function names called with parentheses."""
    if not sql or not catalog or not schema:
        return set()
    pattern = re.compile(
        rf"(?i)\b{re.escape(catalog)}\s*\.\s*{re.escape(schema)}\s*\.\s*([a-zA-Z_][\w]*)\s*\(",
    )
    return {m.group(1).lower() for m in pattern.finditer(sql)}


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _set_sql_context(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Ensure Spark SQL context is aligned to target catalog/schema."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")


def _execute_sql_via_warehouse(
    w: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    catalog: str = "",
    schema: str = "",
    wait_timeout: str = "50s",
) -> pd.DataFrame:
    """Execute SQL via the SQL warehouse Statement Execution API.

    Returns a pandas DataFrame on success (may be empty for DDL/EXPLAIN).
    Raises ``RuntimeError`` on failure with the warehouse error message.
    """
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        catalog=catalog or None,
        schema=schema or None,
        wait_timeout=wait_timeout,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        manifest_schema = resp.manifest.schema if resp.manifest else None
        schema_cols = manifest_schema.columns if manifest_schema else None
        columns = [str(c.name or "") for c in (schema_cols or [])]
        rows: list[dict] = []
        if resp.result and resp.result.data_array:
            for row_data in resp.result.data_array:
                rows.append(dict(zip(columns, row_data)))
        return pd.DataFrame(rows, columns=pd.Index(columns) if columns else None)

    state = str(resp.status.state) if resp.status and resp.status.state else "UNKNOWN"
    statement_id = getattr(resp, "statement_id", None) or ""
    if state in {"PENDING", "RUNNING"}:
        raise RuntimeError(
            "SQL warehouse query did not finish within "
            f"wait_timeout={wait_timeout}; state={state}; statement_id={statement_id}"
        )

    error_msg = ""
    if resp.status and resp.status.error:
        error_msg = resp.status.error.message or str(resp.status.error)
    raise RuntimeError(
        error_msg
        or f"SQL warehouse query failed with state={state}; statement_id={statement_id}"
    )


def _exec_sql(
    sql: str,
    spark: Any,
    *,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
) -> "pd.DataFrame":
    """Execute SQL via warehouse (primary) or Spark (fallback).

    Returns a pandas DataFrame in both cases.  When the warehouse is
    available and *warehouse_id* is set, routes through the Statement
    Execution API.  Otherwise falls back to ``spark.sql().toPandas()``.
    """
    if w and warehouse_id:
        try:
            return _execute_sql_via_warehouse(
                w, warehouse_id, sql,
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.debug(
                "Warehouse SQL failed, falling back to Spark: %s",
                sql[:120], exc_info=True,
            )
    if catalog:
        _set_sql_context(spark, catalog, schema)
    return spark.sql(sql).toPandas()


_SQL_PARAM_RE = re.compile(
    r"(?<![:\w])"     # not preceded by : or word char (avoids ::cast, timestamps)
    r":([a-zA-Z_]\w*)"  # :param_name
    r"(?!\s*:)"        # not followed by : (avoids :: cast operator)
)


def _extract_sql_params(sql: str) -> list[str]:
    """Return SQL named-parameter placeholders (e.g. :min_amount) found in *sql*."""
    if not sql:
        return []
    return _SQL_PARAM_RE.findall(sql)


def _is_infrastructure_sql_error(message: str) -> bool:
    """Detect environment/config errors that should fail evaluation.

    With OBO-first execution the job runs as the triggering user, so
    permission errors (INSUFFICIENT_PERMISSIONS, permission denied) are
    genuine evaluation failures rather than infrastructure mis-config.
    Only SQL context and object-existence errors are treated as infra.
    """
    m = (message or "").lower()
    patterns = (
        "not in the current catalog",
        "please set the current catalog",
        "catalog does not exist",
        "schema does not exist",
        "resource_does_not_exist",
        "table_or_view_not_found",
        "cannot be found. verify the spelling",
        "unresolvable_table_valued_function",
    )
    return any(p in m for p in patterns)


def _extract_sqlstate(message: str) -> str | None:
    match = re.search(r"SQLSTATE:\s*([A-Z0-9]+)", message or "", flags=re.IGNORECASE)
    return match.group(1).upper() if match else None


def _classify_sql_validation_error(message: str) -> str:
    """Classify SQL validation failures into stable reason codes.

    PR 16 added the following codes to enable class-specific repair
    hints in the LLM correction prompt:

    * ``mv_missing_measure_function`` — bare measure column referenced
      against an MV; fix is to wrap with ``MEASURE()``.
    * ``mv_alias_collision`` — ``MEASURE(col) AS col`` shadowed the
      underlying column; fix is to rename the alias.

    PR 20 added:

    * ``mv_measure_in_where`` — a measure column was referenced inside
      a ``WHERE`` / ``HAVING`` / ``ON`` clause (Spark forbids this even
      when wrapped in ``MEASURE()``). Fix is the CTE-first rewrite.
    """
    lowered = (message or "").lower()
    if "metric_view_missing_measure_function" in lowered:
        # Disambiguate: if the planner cited a WHERE/HAVING/ON clause
        # the LLM needs the CTE-first hint, not the wrap-in-MEASURE
        # hint. Spark's error message text varies by release; we look
        # for any of the three clause keywords near the error preamble.
        if any(
            kw in lowered
            for kw in (
                "in where",
                "in the where",
                "where clause",
                "in having",
                "having clause",
                "in on",
                " on clause",
            )
        ):
            return "mv_measure_in_where"
        return "mv_missing_measure_function"
    if (
        "metric_view_unsupported_usage" in lowered
        or "unsupported_metric_view_usage" in lowered
    ):
        return "mv_unsupported_usage"
    if (
        "missing_attributes.resolved_attribute_appear_in_operation" in lowered
        or "resolved attribute" in lowered
        and "appear in the operation" in lowered
    ):
        return "mv_alias_collision"
    if "metric_view_join_not_supported" in lowered:
        return "metric_view_join"
    # PR 32 — categorical string cast to numeric.
    if (
        "cast_invalid_input" in lowered
        or "cannot be cast to" in lowered
        or "cannot be parsed as" in lowered
    ):
        return "cast_invalid_input"
    if "insufficient_permissions" in lowered or "permission denied" in lowered:
        return "permission_blocked"
    if "does not have execute on routine" in lowered:
        return "permission_blocked"
    if "unresolved_column" in lowered:
        if "join" in lowered:
            return "bad_join_key"
        return "unknown_column"
    if "table_or_view_not_found" in lowered or "cannot be found" in lowered:
        return "missing_object"
    if "parseexception" in lowered or "syntax error" in lowered:
        return "syntax_error"
    return "sql_compile_error"


_REPAIR_HINTS_BY_REASON: dict[str, str] = {
    "mv_missing_measure_function": (
        "FIX: A bare measure column was referenced against a metric "
        "view. Wrap every measure column in MEASURE() in the SELECT "
        "and ORDER BY clauses (NEVER in WHERE / HAVING / ON). The "
        "Metric Views section above lists which columns are measures."
    ),
    "mv_measure_in_where": (
        "FIX: A measure column appeared in a WHERE / HAVING / ON "
        "clause. Spark forbids this even when wrapped in MEASURE(). "
        "Use the CTE-first pattern from the Metric Views docs: "
        "materialize each filtered measure as ``MEASURE(m) AS m_value`` "
        "in a WITH-clause SELECT, then filter on the alias in the "
        "outer query. Example:\n"
        "  WITH __mv_base AS (\n"
        "    SELECT zone, MEASURE(total_sales) AS sales,\n"
        "           MEASURE(store_day_count) AS store_day_count_value\n"
        "    FROM mv_x GROUP BY zone\n"
        "  )\n"
        "  SELECT zone, sales FROM __mv_base WHERE store_day_count_value > 0;"
    ),
    "mv_alias_collision": (
        "FIX: MEASURE(col) was aliased back to the same column name "
        "(e.g. MEASURE(cy_sales) AS cy_sales), which Spark resolves as "
        "a re-application of MEASURE on the alias. Rename the alias "
        "to something distinct (e.g. cy_sales_value) and update any "
        "ORDER BY / HAVING references."
    ),
    "unknown_column": (
        "FIX: A column reference doesn't exist on the cited asset. "
        "Replace it with a column from the Column Allowlist that "
        "matches the question intent. NEVER stem or invent column "
        "names; use the FQ identifier as written in the allowlist."
    ),
    "missing_object": (
        "FIX: The SQL references a table / view / function that does "
        "not exist. Replace with an allowlisted asset from VALID Data "
        "Assets. NEVER stem or aliase the asset identifier."
    ),
    "metric_view_join": (
        "FIX: Direct JOIN against a metric view triggered "
        "METRIC_VIEW_JOIN_NOT_SUPPORTED. Use the CTE-first pattern: "
        "materialize the metric view query in a WITH clause, then "
        "JOIN the CTE result to the dimension table."
    ),
    "cast_invalid_input": (
        "FIX: A CAST to a numeric type failed because the column's "
        "actual values are categorical strings (e.g. 'Y'/'N', "
        "'true'/'false'). Do NOT cast categorical flag columns to "
        "BIGINT/INT/DOUBLE. Instead, compare directly to the "
        "string literal (``WHERE flag_col = 'Y'``) or use a CASE "
        "expression to map categories to 0/1 (``CASE WHEN col = 'Y' "
        "THEN 1 ELSE 0 END``). The Column value profile section "
        "above lists the actual sampled values."
    ),
    "bad_join_key": (
        "FIX: The JOIN ON clause references a column that doesn't "
        "exist on one side of the join. Use the Join Specifications "
        "section to pick the correct join keys."
    ),
    "syntax_error": (
        "FIX: SQL parse error. Re-author the query — preserve the "
        "question intent but write it in valid Spark SQL."
    ),
}


def _repair_hint_for_reason(reason: str) -> str:
    """Return a class-specific repair hint or empty string if unknown.

    The hint is appended to the ``benchmarks_to_fix`` payload so the
    LLM correction call gets a deterministic nudge toward the right
    fix instead of guessing from the raw error string.
    """
    return _REPAIR_HINTS_BY_REASON.get(reason, "")


_MV_JOIN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)


def _precheck_benchmarks_for_eval(
    *,
    benchmarks: list[dict],
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    known_functions: set[str],
    metric_view_names: set[str] | None = None,
    metric_view_measures: dict[str, set[str]] | None = None,
    w: WorkspaceClient | None = None,
    warehouse_id: str = "",
) -> tuple[list[dict], list[dict[str, Any]], dict[str, int]]:
    """Apply strict SQL + routine checks before entering mlflow.genai.evaluate()."""
    valid: list[dict] = []
    quarantined: list[dict[str, Any]] = []
    reason_counts = {
        "invalid_benchmark_count": 0,
        "permission_blocked_count": 0,
        "unresolved_column_count": 0,
        "bad_join_key_count": 0,
    }
    mv_names_lower = {n.lower().split(".")[-1] for n in (metric_view_names or set())}
    _mv_measures = metric_view_measures or {}

    from genie_space_optimizer.common.config import REQUIRE_GROUND_TRUTH_SQL

    for idx, benchmark in enumerate(benchmarks):
        question = str(benchmark.get("question") or "").strip()
        qid = str(benchmark.get("id") or benchmark.get("question_id") or f"q-{idx}")
        sql = str(benchmark.get("expected_sql") or "").strip()
        if not sql:
            if REQUIRE_GROUND_TRUTH_SQL:
                quarantined.append(
                    {
                        "question_id": qid,
                        "question": question,
                        "reason": "missing_ground_truth",
                        "sqlstate": None,
                        "error": "Benchmark has no expected SQL — cannot evaluate without ground truth",
                        "expected_sql": "",
                    }
                )
                reason_counts["invalid_benchmark_count"] += 1
                logger.warning(
                    "BENCHMARK REJECTED (no ground truth SQL): id=%s question='%s'",
                    qid, question[:80],
                )
            else:
                valid.append(benchmark)
            continue

        resolved_sql = resolve_sql(sql, catalog=catalog, gold_schema=gold_schema)
        # Task 8 — route benchmark precheck through the same shared
        # ``apply_pre_execute_repairs`` pipeline used by unified and
        # preflight generation, so an alias-collision shape that
        # passes the unified path doesn't get rejected here.
        canonical_assets = sorted(metric_view_names or [])
        resolved_sql = apply_pre_execute_repairs(
            resolved_sql,
            mv_measures=_mv_measures,
            mv_short_set=mv_names_lower,
            canonical_assets=canonical_assets or None,
        )

        _found_params = _extract_sql_params(resolved_sql)
        if _found_params:
            from genie_space_optimizer.optimization.benchmarks import _resolve_params_with_defaults
            _bench_params = benchmark.get("parameters", [])
            _resolved_default, _all_resolved = _resolve_params_with_defaults(
                resolved_sql, _bench_params,
            )
            if _all_resolved:
                logger.info(
                    "Benchmark %s: substituted defaults for %d params — running EXPLAIN",
                    qid, len(_found_params),
                )
                resolved_sql = _resolved_default
            else:
                logger.info(
                    "Benchmark %s has parameterized SQL (some without defaults) — "
                    "skipping EXPLAIN quarantine",
                    qid,
                )
                valid.append(benchmark)
                continue

        # Tier 3.10: move METRIC_VIEW_JOIN pre-check upstream of EXPLAIN.
        # This pattern (direct JOIN between two metric views without a
        # CTE wrapper) is deterministically rejected by Databricks SQL
        # at EXPLAIN time with METRIC_VIEW_JOIN_NOT_SUPPORTED. Catching
        # it here saves the gRPC round-trip (and its triplicated log
        # spam) plus a warehouse call on every such benchmark row.
        _expected_asset_pre = _normalize_expected_asset(
            str(benchmark.get("expected_asset", "")),
            resolved_sql,
            hint=benchmark.get("expected_asset_hint"),
        )
        _uses_measure_pre = "MEASURE(" in resolved_sql.upper()
        _refs_mv_pre = any(
            mv in resolved_sql.lower() for mv in mv_names_lower
        ) if mv_names_lower else False
        _is_mv_context_pre = _expected_asset_pre == "MV" or _uses_measure_pre or _refs_mv_pre
        _uses_cte_pre = bool(
            re.search(r"\bWITH\b\s+\w+\s+AS\s*\(", resolved_sql, re.IGNORECASE)
        )
        if _is_mv_context_pre and _MV_JOIN_RE.search(resolved_sql) and not _uses_cte_pre:
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": "metric_view_join",
                    "sqlstate": None,
                    "error": (
                        "Metric view / MEASURE() benchmarks cannot use direct JOINs "
                        "(METRIC_VIEW_JOIN_NOT_SUPPORTED). Use the CTE-first pattern: "
                        "materialize the metric view in a WITH clause, then JOIN the CTE. "
                        "(Detected upstream of EXPLAIN — Tier 3.10.)"
                    ),
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            continue

        try:
            if w and warehouse_id:
                explain_df = _execute_sql_via_warehouse(
                    w, warehouse_id, f"EXPLAIN {resolved_sql}",
                    catalog=catalog, schema=gold_schema,
                )
                if not explain_df.empty and "plan" in explain_df.columns:
                    plan_text = "\n".join(str(v) for v in explain_df["plan"].tolist())
                    if "Error occurred during query planning" in plan_text:
                        raise RuntimeError(plan_text)
            else:
                _set_sql_context(spark, catalog, gold_schema)
                spark.sql(f"EXPLAIN {resolved_sql}")
        except Exception as exc:
            msg = str(exc)
            if "UNBOUND_SQL_PARAMETER" in msg:
                logger.info(
                    "Benchmark %s hit UNBOUND_SQL_PARAMETER in EXPLAIN — "
                    "treating as valid (parameterized SQL)",
                    qid,
                )
                valid.append(benchmark)
                continue
            reason = _classify_sql_validation_error(msg)
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": reason,
                    "sqlstate": _extract_sqlstate(msg),
                    "error": msg[:500],
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            if reason == "permission_blocked":
                reason_counts["permission_blocked_count"] += 1
            if reason == "unknown_column":
                reason_counts["unresolved_column_count"] += 1
            if reason == "bad_join_key":
                reason_counts["bad_join_key_count"] += 1
            continue

        expected_asset = _normalize_expected_asset(
            str(benchmark.get("expected_asset", "")),
            resolved_sql,
            hint=benchmark.get("expected_asset_hint"),
        )
        uses_measure = "MEASURE(" in resolved_sql.upper()
        refs_metric_view = any(
            mv in resolved_sql.lower() for mv in mv_names_lower
        ) if mv_names_lower else False
        is_mv_context = expected_asset == "MV" or uses_measure or refs_metric_view
        _uses_cte = bool(re.search(r"\bWITH\b\s+\w+\s+AS\s*\(", resolved_sql, re.IGNORECASE))
        if is_mv_context and _MV_JOIN_RE.search(resolved_sql) and not _uses_cte:
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": "metric_view_join",
                    "sqlstate": None,
                    "error": (
                        "Metric view / MEASURE() benchmarks cannot use direct JOINs "
                        "(METRIC_VIEW_JOIN_NOT_SUPPORTED). Use the CTE-first pattern: "
                        "materialize the metric view in a WITH clause, then JOIN the CTE."
                    ),
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            continue

        called_functions = _extract_sql_function_calls(resolved_sql, catalog, gold_schema)
        blocked_functions = sorted(fn for fn in called_functions if fn not in known_functions)
        if blocked_functions:
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": "permission_blocked",
                    "sqlstate": "42501",
                    "blocked_routines": blocked_functions,
                    "error": (
                        "No EXECUTE privilege or function unavailable for one or more routines: "
                        + ", ".join(blocked_functions)
                    ),
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            reason_counts["permission_blocked_count"] += 1
            continue

        valid.append(benchmark)

    return valid, quarantined, reason_counts


# ── Predict Function (Factory Closure) ──────────────────────────────────


def make_predict_fn(
    w: WorkspaceClient,
    space_id: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    metric_view_measures: dict[str, set[str]] | None = None,
    *,
    warehouse_id: str = "",
    optimization_run_id: str = "",
    iteration: int | None = None,
    lever: int | None = None,
    eval_scope: str = "",
    triggered_by: str = "",
    instruction_prompt_name: str = "",
):
    """Return a predict function with bound workspace/spark context.

    The returned closure is suitable for ``mlflow.genai.evaluate(predict_fn=...)``.
    ``metric_view_measures`` maps lowercased metric view short names to sets
    of measure column names — used to auto-rewrite ORDER BY for GT SQL.
    """

    known_functions = _load_known_functions(spark, catalog, schema)
    _mv_measures = metric_view_measures or {}

    progress = EvalProgressLogger(
        logger=logger,
        run_id=optimization_run_id,
        eval_scope=eval_scope,
        iteration=iteration,
    )

    @mlflow.trace
    def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
        """Query Genie, fetch its results via Statement API, execute only GT SQL.

        Steps: rate-limit → Genie call → fetch Genie result via statement_id →
               resolve & execute GT SQL → normalize → compare hashes.

        We never re-execute Genie's SQL ourselves.  Genie runs queries on its
        own SQL warehouse; re-executing via Spark Connect can hit different
        limitations (e.g. METRIC_VIEW_JOIN_NOT_SUPPORTED).
        """
        _qid_for_span = kwargs.get("question_id", "")
        progress.emit(
            "predict_start",
            question_id=_qid_for_span,
            question=question,
        )
        try:
            if instruction_prompt_name:
                _link_prompt_to_trace(instruction_prompt_name)
            _trace_tags: dict[str, str] = {
                "question_id": _qid_for_span,
                "space_id": space_id,
            }
            if optimization_run_id:
                _trace_tags["genie.optimization_run_id"] = optimization_run_id
            if iteration is not None:
                _trace_tags["genie.iteration"] = str(iteration)
            if lever is not None:
                _trace_tags["genie.lever"] = str(lever)
            if eval_scope:
                _trace_tags["genie.eval_scope"] = eval_scope
            _trace_metadata: dict[str, str] = {
                "space_id": space_id,
            }
            if triggered_by:
                _trace_metadata["mlflow.trace.user"] = triggered_by
            if optimization_run_id:
                _trace_metadata["mlflow.trace.session"] = optimization_run_id
            if iteration is not None:
                _trace_metadata["iteration"] = str(iteration)
            if eval_scope:
                _trace_metadata["eval_scope"] = eval_scope
            mlflow.update_current_trace(tags=_trace_tags, metadata=_trace_metadata)
            # Phase 2.1: belt-and-suspenders. Also stamp question_id as
            # a SPAN attribute (in addition to the trace tag above) so
            # downstream consumers that inspect ``trace.data.spans`` —
            # not just ``trace.info.tags`` — can recover the qid even
            # when the trace-tag propagation path is unreliable in
            # ``mlflow.genai.evaluate``.
            try:
                _active_span = mlflow.get_current_active_span()
                if _active_span is not None and _qid_for_span:
                    _active_span.set_attribute("question_id", _qid_for_span)
                    if optimization_run_id:
                        _active_span.set_attribute(
                            "genie.optimization_run_id", optimization_run_id,
                        )
                    if iteration is not None:
                        _active_span.set_attribute(
                            "genie.iteration", str(iteration),
                        )
            except Exception:
                logger.debug("Failed to stamp span attributes", exc_info=True)
        except Exception:
            # Surface tag-update failures so trace-recovery gaps have a
            # breadcrumb in MLflow metrics instead of a silent miss.
            logger.debug("Failed to update trace tags", exc_info=True)
            try:
                mlflow.log_metric("predict_fn.trace_tag_update_failures", 1)
            except Exception:
                pass

        comparison: dict[str, Any] = {
            "match": False,
            "match_type": "mismatch",
            "gt_rows": 0,
            "genie_rows": 0,
            "gt_hash": None,
            "genie_hash": None,
            "gt_signature": None,
            "genie_signature": None,
            "error": None,
        }
        result: dict[str, Any] = {}
        genie_sql = ""
        gt_sql = ""
        temporal_rewrite_meta: dict | None = None
        try:
            with progress.phase("rate_limit_sleep", question_id=_qid_for_span):
                time.sleep(RATE_LIMIT_SECONDS)
            with progress.phase("genie_query", question_id=_qid_for_span, question=question):
                result = run_genie_query(w, space_id, question)
            progress.emit(
                "genie_query_result",
                question_id=_qid_for_span,
                genie_status=result.get("status"),
                conversation_id=result.get("conversation_id"),
                message_id=result.get("message_id"),
                statement_id=result.get("statement_id"),
            )
            genie_sql = sanitize_sql(result.get("sql") or "")
            gt_sql = resolve_sql(expected_sql, catalog, schema)
            from genie_space_optimizer.optimization.benchmarks import fix_mv_alias_sort_collision
            gt_sql = fix_mv_alias_sort_collision(gt_sql)
            if _mv_measures and gt_sql:
                gt_sql = _rewrite_measure_refs(gt_sql, _mv_measures)
                gt_sql, _ = _repair_measure_alias_collisions(gt_sql)
                # PR 20: CTE-first lift for measures referenced in WHERE.
                gt_sql, _ = _repair_measure_in_where(gt_sql, _mv_measures)
            temporal_intent = _detect_temporal_intent(question)
            if temporal_intent and gt_sql:
                gt_sql, temporal_rewrite_meta = _rewrite_temporal_dates(gt_sql, temporal_intent)
                if temporal_rewrite_meta:
                    logger.info(
                        "Temporal rewrite for '%s': %s → %s",
                        temporal_intent.keyword,
                        temporal_rewrite_meta["original_dates"],
                        temporal_rewrite_meta["rewritten_dates"],
                    )
            statement_id = result.get("statement_id")

            if genie_sql and gt_sql:
                _genie_sql_norm = genie_sql.strip().lower()
                _gt_sql_norm = gt_sql.strip().lower()

                if _genie_sql_norm and _gt_sql_norm and _genie_sql_norm == _gt_sql_norm:
                    comparison = {
                        "match": True,
                        "match_type": "identical_sql",
                        "gt_rows": None,
                        "genie_rows": None,
                        "gt_hash": None,
                        "genie_hash": None,
                        "gt_signature": None,
                        "genie_signature": None,
                        "error": None,
                        "identical_sql": True,
                    }
                else:
                    _unbound_params = _extract_sql_params(gt_sql)
                    if _unbound_params:
                        from genie_space_optimizer.optimization.benchmarks import _resolve_params_with_defaults
                        _bench_params = kwargs.get("parameters", [])
                        _gt_resolved, _gt_all = _resolve_params_with_defaults(
                            gt_sql, _bench_params,
                        )
                        if _gt_all:
                            logger.info(
                                "Substituted defaults for %d params in GT SQL for '%s'",
                                len(_unbound_params), question[:60],
                            )
                            gt_sql = _gt_resolved
                        else:
                            logger.warning(
                                "GT SQL contains unbound parameters %s — "
                                "skipping result comparison for '%s'",
                                _unbound_params, question[:80],
                            )
                            comparison["error"] = (
                                f"GT SQL contains parameterized placeholders "
                                f"({', '.join(':' + p for p in _unbound_params)}) "
                                f"that cannot be executed directly"
                            )
                            comparison["error_type"] = "parameterized_sql"

                    if not comparison.get("error"):
                        try:
                            called_functions = _extract_sql_function_calls(gt_sql, catalog, schema)
                            missing_gt_functions = sorted(f for f in called_functions if f not in known_functions)
                            if missing_gt_functions:
                                comparison["error"] = (
                                    "Missing function(s) in GT SQL for schema "
                                    f"{catalog}.{schema}: {', '.join(missing_gt_functions)}"
                                )
                                comparison["error_type"] = "permission_blocked"
                            else:
                                try:
                                    if warehouse_id:
                                        with progress.phase("gt_explain", question_id=_qid_for_span):
                                            _execute_sql_via_warehouse(
                                                w, warehouse_id, f"EXPLAIN {gt_sql}",
                                                catalog=catalog, schema=schema,
                                            )
                                    else:
                                        _set_sql_context(spark, catalog, schema)
                                        with progress.phase("gt_explain", question_id=_qid_for_span):
                                            spark.sql(f"EXPLAIN {gt_sql}")
                                except Exception as explain_exc:
                                    explain_msg = str(explain_exc)
                                    if "UNBOUND_SQL_PARAMETER" in explain_msg:
                                        comparison["error"] = (
                                            f"GT SQL contains parameterized placeholders "
                                            f"that cannot be executed directly: {explain_msg[:300]}"
                                        )
                                        comparison["error_type"] = "parameterized_sql"
                                    else:
                                        comparison["error"] = f"ground_truth SQL compilation failed: {explain_msg[:400]}"
                                        comparison["error_type"] = "infrastructure"
                                    comparison["sqlstate"] = _extract_sqlstate(explain_msg)

                                if not comparison["error"]:
                                    if warehouse_id:
                                        with progress.phase("gt_execute", question_id=_qid_for_span):
                                            raw_gt_df = _execute_sql_via_warehouse(
                                                w, warehouse_id, gt_sql,
                                                catalog=catalog, schema=schema,
                                            )
                                        gt_df = normalize_result_df(raw_gt_df)
                                    else:
                                        _set_sql_context(spark, catalog, schema)
                                        with progress.phase("gt_execute", question_id=_qid_for_span):
                                            gt_df = normalize_result_df(spark.sql(gt_sql).toPandas())

                                genie_df = None
                                if statement_id:
                                    with progress.phase(
                                        "genie_result_fetch",
                                        question_id=_qid_for_span,
                                        statement_id=statement_id,
                                    ):
                                        raw_genie_df = fetch_genie_result_df(w, statement_id)
                                    genie_df = normalize_result_df(raw_genie_df)

                                if genie_df is None or genie_df.empty:
                                    comparison["error"] = (
                                        "Could not retrieve Genie query results"
                                        + (f" (statement_id={statement_id})" if statement_id else " (no statement_id)")
                                    )
                                    comparison["error_type"] = "genie_result_unavailable"
                                    comparison["gt_rows"] = len(gt_df)
                                    comparison["gt_sample"] = gt_df.head(5).to_csv(index=False, float_format="%.4f")
                                elif len(gt_df) == 0 and len(genie_df) == 0:
                                    comparison = {
                                        "match": False,
                                        "match_type": "both_empty",
                                        "gt_rows": 0,
                                        "genie_rows": 0,
                                        "gt_columns": sorted(gt_df.columns.tolist()),
                                        "genie_columns": sorted(genie_df.columns.tolist()),
                                        "gt_column_types": {
                                            str(col): str(dtype)
                                            for col, dtype in gt_df.dtypes.items()
                                        },
                                        "genie_column_types": {
                                            str(col): str(dtype)
                                            for col, dtype in genie_df.dtypes.items()
                                        },
                                        "column_type_difference": False,
                                        "gt_hash": "",
                                        "genie_hash": "",
                                        "error": None,
                                        "error_type": "both_empty",
                                        "note": "Both GT and Genie SQL returned 0 rows",
                                    }
                                else:
                                    mapped_genie_df = genie_df
                                    _FLOAT_FMT = "%.4f"
                                    gt_hash = hashlib.md5(
                                        gt_df.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                    ).hexdigest()[:8]
                                    genie_hash = hashlib.md5(
                                        genie_df.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                    ).hexdigest()[:8]
                                    exact_match = gt_df.shape == genie_df.shape and gt_df.equals(genie_df)
                                    hash_match_ordered = gt_hash == genie_hash

                                    hash_match_sorted = False
                                    gt_hash_sorted = ""
                                    genie_hash_sorted = ""
                                    if (
                                        not hash_match_ordered
                                        and not scoring_v2_is_legacy()
                                        and list(gt_df.columns) == list(genie_df.columns)
                                    ):
                                        try:
                                            _gt_sorted_full = (
                                                gt_df.sort_values(list(gt_df.columns))
                                                .reset_index(drop=True)
                                            )
                                            _ge_sorted_full = (
                                                genie_df.sort_values(list(genie_df.columns))
                                                .reset_index(drop=True)
                                            )
                                            gt_hash_sorted = hashlib.md5(
                                                _gt_sorted_full.to_csv(
                                                    index=False, float_format=_FLOAT_FMT,
                                                ).encode()
                                            ).hexdigest()[:8]
                                            genie_hash_sorted = hashlib.md5(
                                                _ge_sorted_full.to_csv(
                                                    index=False, float_format=_FLOAT_FMT,
                                                ).encode()
                                            ).hexdigest()[:8]
                                            hash_match_sorted = (
                                                gt_hash_sorted == genie_hash_sorted
                                            )
                                        except Exception:
                                            hash_match_sorted = False

                                    _order_sensitive = bool(
                                        kwargs.get("order_sensitive", False)
                                    )
                                    if (
                                        _order_sensitive
                                        or scoring_v2_is_legacy()
                                    ):
                                        hash_match = hash_match_ordered
                                    else:
                                        hash_match = (
                                            hash_match_ordered or hash_match_sorted
                                        )

                                    subset_match = False
                                    subset_type = None
                                    if not hash_match:
                                        genie_cols = set(genie_df.columns)
                                        gt_cols = set(gt_df.columns)
                                        shared_cols = sorted(genie_cols & gt_cols)
                                        all_mapped = genie_cols <= gt_cols

                                        if not all_mapped:
                                            unmatched_genie = sorted(genie_cols - gt_cols)
                                            candidate_gt = sorted(gt_cols - genie_cols)
                                            col_map: dict[str, str] = {}
                                            _ALIAS_SAMPLE = min(50, len(genie_df))
                                            for gc in unmatched_genie:
                                                g_vals = genie_df[gc].head(_ALIAS_SAMPLE).tolist()
                                                for gtc in candidate_gt:
                                                    if gtc in col_map.values():
                                                        continue
                                                    gt_vals = gt_df[gtc].head(_ALIAS_SAMPLE).tolist()
                                                    if g_vals == gt_vals:
                                                        col_map[gc] = gtc
                                                        break
                                                    try:
                                                        import numpy as np
                                                        g_arr = np.array(g_vals, dtype=float)
                                                        gt_arr = np.array(gt_vals, dtype=float)
                                                        if np.allclose(g_arr, gt_arr, rtol=1e-4, atol=1e-4, equal_nan=True):
                                                            col_map[gc] = gtc
                                                            break
                                                    except (ValueError, TypeError):
                                                        pass
                                            if len(col_map) == len(unmatched_genie):
                                                mapped_genie_df = genie_df.rename(columns=col_map)
                                                genie_cols = set(mapped_genie_df.columns)
                                                shared_cols = sorted(genie_cols & gt_cols)
                                                all_mapped = genie_cols <= gt_cols

                                        if shared_cols and all_mapped:
                                            _GENIE_ROW_CAP = 5000
                                            gt_sub = gt_df[shared_cols].sort_values(shared_cols).reset_index(drop=True)
                                            genie_sub = mapped_genie_df[shared_cols].sort_values(shared_cols).reset_index(drop=True)
                                            if len(genie_df) == _GENIE_ROW_CAP and len(gt_df) > _GENIE_ROW_CAP:
                                                gt_sub = gt_sub.head(_GENIE_ROW_CAP)
                                            gt_sub_hash = hashlib.md5(
                                                gt_sub.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                            ).hexdigest()[:8]
                                            genie_sub_hash = hashlib.md5(
                                                genie_sub.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                            ).hexdigest()[:8]
                                            if gt_sub_hash == genie_sub_hash:
                                                subset_match = True
                                                subset_type = "column_subset"
                                                if len(genie_df) == _GENIE_ROW_CAP and len(gt_df) > _GENIE_ROW_CAP:
                                                    subset_type = "column_subset_row_capped"

                                    approx_match = False
                                    _approx_genie = mapped_genie_df if mapped_genie_df is not genie_df else genie_df
                                    if (
                                        not hash_match
                                        and not subset_match
                                        and gt_df.shape == _approx_genie.shape
                                        and list(gt_df.columns) == list(_approx_genie.columns)
                                    ):
                                        try:
                                            import numpy as np

                                            gt_sorted = gt_df.sort_values(list(gt_df.columns)).reset_index(drop=True)
                                            genie_sorted = _approx_genie.sort_values(list(_approx_genie.columns)).reset_index(drop=True)

                                            all_numeric = set(
                                                gt_sorted.select_dtypes(include=["number"]).columns
                                            ) | set(
                                                genie_sorted.select_dtypes(include=["number"]).columns
                                            )
                                            for col in list(all_numeric):
                                                for _df in (gt_sorted, genie_sorted):
                                                    if _df[col].dtype == object:
                                                        _df[col] = pd.to_numeric(_df[col], errors="coerce")

                                            non_numeric = [c for c in gt_sorted.columns if c not in all_numeric]
                                            non_num_match = gt_sorted[non_numeric].equals(genie_sorted[non_numeric]) if non_numeric else True
                                            numeric = sorted(all_numeric)
                                            num_match = (
                                                np.allclose(
                                                    gt_sorted[numeric].values.astype(float),
                                                    genie_sorted[numeric].values.astype(float),
                                                    rtol=1e-4,
                                                    atol=1e-4,
                                                    equal_nan=True,
                                                )
                                                if numeric
                                                else True
                                            )
                                            approx_match = bool(non_num_match and num_match)
                                        except Exception:
                                            approx_match = False

                                    gt_sig = result_signature(gt_df)
                                    genie_sig = result_signature(genie_df)
                                    sig_match = (
                                        gt_sig["schema_hash"] == genie_sig["schema_hash"]
                                        and gt_sig["row_count"] == genie_sig["row_count"]
                                    )

                                    tied_subset = False
                                    if (
                                        not exact_match
                                        and not hash_match
                                        and not subset_match
                                        and not approx_match
                                        and len(gt_df) == len(genie_df)
                                        and len(gt_df) > 0
                                        and bool(re.search(r"\bLIMIT\b", gt_sql, re.I))
                                    ):
                                        try:
                                            import numpy as np

                                            _tg = mapped_genie_df if mapped_genie_df is not genie_df else genie_df
                                            _shared = sorted(set(gt_df.columns) & set(_tg.columns))
                                            if _shared:
                                                _gt_s = gt_df[_shared].sort_values(_shared).reset_index(drop=True)
                                                _ge_s = _tg[_shared].sort_values(_shared).reset_index(drop=True)
                                                _num_cols = sorted(
                                                    set(_gt_s.select_dtypes(include=["number"]).columns)
                                                    | set(_ge_s.select_dtypes(include=["number"]).columns)
                                                )
                                                _non_num = [c for c in _shared if c not in _num_cols]
                                                _nn_ok = _gt_s[_non_num].equals(_ge_s[_non_num]) if _non_num else True
                                                _n_ok = (
                                                    np.allclose(
                                                        _gt_s[_num_cols].values.astype(float),
                                                        _ge_s[_num_cols].values.astype(float),
                                                        rtol=1e-4, atol=1e-4, equal_nan=True,
                                                    )
                                                    if _num_cols
                                                    else True
                                                )
                                                tied_subset = bool(_nn_ok and _n_ok)
                                        except Exception:
                                            tied_subset = False

                                    cosmetic_match = False
                                    if (
                                        not exact_match
                                        and not hash_match
                                        and not subset_match
                                        and not approx_match
                                        and not tied_subset
                                        and len(gt_df) == len(genie_df)
                                        and len(gt_df.columns) == len(genie_df.columns)
                                        and len(gt_df) > 0
                                    ):
                                        try:
                                            _cg = mapped_genie_df if mapped_genie_df is not genie_df else genie_df
                                            _gt_vals = gt_df.values.tolist()
                                            _ge_vals = _cg.values.tolist()
                                            _gt_sorted = sorted(_gt_vals, key=lambda r: [str(v) for v in r])
                                            _ge_sorted = sorted(_ge_vals, key=lambda r: [str(v) for v in r])
                                            if _gt_sorted == _ge_sorted:
                                                cosmetic_match = True
                                            elif not cosmetic_match:
                                                import numpy as np
                                                _match_all = True
                                                for _row_g, _row_e in zip(_gt_sorted, _ge_sorted):
                                                    for _vg, _ve in zip(_row_g, _row_e):
                                                        if _vg == _ve or (str(_vg) == str(_ve)):
                                                            continue
                                                        try:
                                                            if np.isclose(float(_vg), float(_ve), rtol=1e-4, atol=1e-4):
                                                                continue
                                                        except (ValueError, TypeError):
                                                            pass
                                                        _match_all = False
                                                        break
                                                    if not _match_all:
                                                        break
                                                cosmetic_match = _match_all
                                        except Exception:
                                            cosmetic_match = False

                                    if exact_match:
                                        match_type = "exact"
                                    elif hash_match_ordered:
                                        match_type = "hash"
                                    elif hash_match_sorted:
                                        match_type = "hash_sorted"
                                    elif subset_match:
                                        match_type = subset_type
                                    elif approx_match:
                                        match_type = "approx"
                                    elif tied_subset:
                                        match_type = "tied_subset"
                                    elif cosmetic_match:
                                        match_type = "cosmetic"
                                    elif sig_match:
                                        match_type = "signature"
                                    else:
                                        match_type = "mismatch"

                                    def _truncated_sample(df: pd.DataFrame, max_chars: int = 4000) -> str:
                                        sample = df.head(5).copy()
                                        for col in sample.select_dtypes(include=["object"]).columns:
                                            sample[col] = sample[col].apply(
                                                lambda x: (x[:100] + "...") if isinstance(x, str) and len(x) > 100 else x
                                            )
                                        csv = sample.to_csv(index=False, float_format=_FLOAT_FMT)
                                        return csv[:max_chars] if len(csv) > max_chars else csv

                                    gt_col_list = sorted(gt_df.columns.tolist())
                                    genie_col_list = sorted(genie_df.columns.tolist())
                                    gt_column_types = {
                                        str(col): str(dtype)
                                        for col, dtype in gt_df.dtypes.items()
                                    }
                                    genie_column_types = {
                                        str(col): str(dtype)
                                        for col, dtype in genie_df.dtypes.items()
                                    }
                                    try:
                                        column_type_difference = bool(
                                            gt_col_list == genie_col_list
                                            and gt_column_types != genie_column_types
                                            and gt_df.astype(str).equals(genie_df.astype(str))
                                        )
                                    except Exception:
                                        column_type_difference = False
                                    comparison = {
                                        "match": exact_match or hash_match or subset_match or approx_match or tied_subset or cosmetic_match or sig_match,
                                        "match_type": match_type,
                                        "gt_rows": len(gt_df),
                                        "genie_rows": len(genie_df),
                                        "gt_columns": gt_col_list,
                                        "genie_columns": genie_col_list,
                                        "gt_column_types": gt_column_types,
                                        "genie_column_types": genie_column_types,
                                        "column_type_difference": column_type_difference,
                                        "gt_hash": gt_hash,
                                        "genie_hash": genie_hash,
                                        "gt_hash_sorted": gt_hash_sorted,
                                        "genie_hash_sorted": genie_hash_sorted,
                                        "hash_match_ordered": bool(hash_match_ordered),
                                        "hash_match_sorted": bool(hash_match_sorted),
                                        "order_sensitive": bool(_order_sensitive),
                                        "gt_signature": gt_sig,
                                        "genie_signature": genie_sig,
                                        "gt_sample": _truncated_sample(gt_df),
                                        "genie_sample": _truncated_sample(genie_df),
                                        "error": None,
                                    }
                        except Exception as exc:
                            err_msg = str(exc)
                            comparison["error"] = err_msg[:500]
                            if "UNBOUND_SQL_PARAMETER" in err_msg:
                                comparison["error_type"] = "parameterized_sql"
                            elif _is_infrastructure_sql_error(err_msg):
                                comparison["error_type"] = "infrastructure"
                            else:
                                comparison["error_type"] = "query_execution"
                            comparison["sqlstate"] = _extract_sqlstate(err_msg)
            else:
                if not genie_sql:
                    comparison["error"] = "Genie did not return SQL"
                    comparison["error_type"] = "no_genie_sql"
                elif not gt_sql:
                    comparison["error"] = "Missing expected SQL for comparison"
                    comparison["error_type"] = "missing_expected_sql"
        except Exception as exc:
            err_msg = str(exc)
            comparison["error"] = err_msg[:500]
            if "UNBOUND_SQL_PARAMETER" in err_msg:
                comparison["error_type"] = "parameterized_sql"
            elif _is_infrastructure_sql_error(err_msg):
                comparison["error_type"] = "infrastructure"
            else:
                comparison["error_type"] = "predict_fn_error"
            comparison["sqlstate"] = _extract_sqlstate(err_msg)

        if temporal_rewrite_meta:
            comparison["temporal_rewrite"] = temporal_rewrite_meta

        output = {
            "response": genie_sql,
            "status": result.get("status", "ERROR"),
            "conversation_id": result.get("conversation_id", ""),
            "comparison": comparison,
            "analysis_text": result.get("analysis_text"),
        }

        if EVAL_DEBUG:
            qid = kwargs.get("question_id", "?")
            cmp = comparison
            logger.info(
                "\n"
                "═══ EVAL [Q:%s] ═══════════════════════════════════════════════\n"
                "  Question: \"%s\"\n"
                "  Status:   %s\n"
                "  Genie SQL:\n"
                "    %s\n"
                "  GT SQL:\n"
                "    %s\n"
                "  Comparison: match=%s | type=%s | gt_rows=%s | genie_rows=%s\n"
                "              gt_hash=%s | genie_hash=%s\n"
                "  Error:      %s\n"
                "  Analysis:   %s\n"
                "═══════════════════════════════════════════════════════════════",
                qid,
                question,
                output["status"],
                genie_sql or "(none)",
                gt_sql or "(none)",
                cmp.get("match"),
                cmp.get("match_type", "n/a"),
                cmp.get("gt_rows", "?"),
                cmp.get("genie_rows", "?"),
                cmp.get("gt_hash", "n/a"),
                cmp.get("genie_hash", "n/a"),
                cmp.get("error") or "(none)",
                str(output.get("analysis_text") or "(none)")[:200],
            )

        progress.emit(
            "predict_done",
            question_id=_qid_for_span,
            match=comparison.get("match"),
            error_type=comparison.get("error_type"),
        )
        return output

    return genie_predict_fn


# ── MLflow Integration ──────────────────────────────────────────────────


PROMPT_REGISTRY_REQUIRED_PRIVILEGES = ("CREATE FUNCTION", "EXECUTE", "MANAGE")


def _is_ownership_conflict(err_msg: str) -> bool:
    """True when MLflow can't update an existing prompt due to ownership mismatch."""
    lowered = (err_msg or "").lower()
    return "permission_denied" in lowered and "update prompt" in lowered


def _try_drop_prompt(fqn: str) -> bool:
    """Best-effort drop of a stale prompt (UC function) so it can be re-created.

    Returns True if the drop succeeded (or the function didn't exist).
    """
    if "." not in fqn:
        return False
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return False
        spark.sql(f"DROP FUNCTION IF EXISTS {fqn}")
        logger.info("Dropped stale prompt function %s for re-creation", fqn)
        return True
    except Exception:
        logger.debug("Could not drop stale prompt %s", fqn, exc_info=True)
        return False


def _classify_prompt_registration_error(message: str, uc_schema: str) -> dict[str, Any]:
    """Classify prompt registration failure into actionable root-cause buckets."""
    lowered = (message or "").lower()
    permission_markers = (
        "permission",
        "privilege",
        "not authorized",
        "forbidden",
        "insufficient",
        "access denied",
        "permission_denied",
    )
    missing_privileges = [
        priv for priv in PROMPT_REGISTRY_REQUIRED_PRIVILEGES if priv.lower() in lowered
    ]

    if any(marker in lowered for marker in permission_markers):
        if not missing_privileges:
            missing_privileges = list(PROMPT_REGISTRY_REQUIRED_PRIVILEGES)
        schema_target = uc_schema or "<catalog>.<schema>"
        return {
            "reason": "missing_uc_permissions",
            "missing_privileges": missing_privileges,
            "remediation": (
                f"Grant {', '.join(missing_privileges)} on schema {schema_target} "
                "to the Databricks App service principal used by job tasks."
            ),
        }

    if (
        "feature_disabled" in lowered
        or ("not enabled" in lowered and ("prompt" in lowered or "registry" in lowered))
        or ("preview" in lowered and ("prompt" in lowered or "genai" in lowered))
    ):
        return {
            "reason": "feature_not_enabled",
            "missing_privileges": [],
            "remediation": (
                "Enable MLflow Prompt Registry on the workspace. "
                "Contact your workspace admin or enable the GenAI preview in workspace settings."
            ),
        }

    if "does not exist" in lowered or "resource_does_not_exist" in lowered:
        schema_target = uc_schema or "<catalog>.<schema>"
        return {
            "reason": "registry_path_not_found",
            "missing_privileges": [],
            "remediation": (
                f"Verify catalog/schema exists and is accessible: {schema_target}."
            ),
        }

    return {
        "reason": "unknown",
        "missing_privileges": [],
        "remediation": (
            "Inspect full stack trace for prompt registration failure details "
            "and verify Prompt Registry availability."
        ),
    }


def register_instruction_version(
    uc_schema: str,
    space_id: str,
    instruction_text: str,
    *,
    run_id: str = "",
    lever: int = 0,
    iteration: int = 0,
    accuracy: float = 0.0,
    domain: str = "",
) -> dict[str, Any] | None:
    """Register the current Genie Space instruction text as a versioned prompt.

    Best-effort: failures are logged but never raise, so the optimization
    pipeline is never blocked by prompt registration issues.

    Returns ``{"prompt_name": ..., "version": ...}`` on success, ``None`` otherwise.
    """
    if not instruction_text or not instruction_text.strip():
        return None

    safe_space_id = re.sub(r"[^a-zA-Z0-9_]+", "_", space_id or "unknown").strip("_")
    prompt_name = format_mlflow_template(
        INSTRUCTION_PROMPT_NAME_TEMPLATE, uc_schema=uc_schema, space_id=safe_space_id,
    ) if uc_schema else f"genie_instructions_{safe_space_id}"

    commit_msg = (
        f"Genie instructions after lever {lever}, iteration {iteration} "
        f"(accuracy={accuracy:.3f}, run={run_id[:12]})"
    )
    tags = {
        "run_id": run_id,
        "lever": str(lever),
        "iteration": str(iteration),
        "accuracy": f"{accuracy:.4f}",
        "domain": domain,
        "space_id": space_id,
        "type": "genie_instructions",
    }

    def _do_register():
        v = mlflow.genai.register_prompt(
            name=prompt_name,
            template=instruction_text,
            commit_message=commit_msg,
            tags=tags,
        )
        mlflow.genai.set_prompt_alias(
            name=prompt_name,
            alias=INSTRUCTION_PROMPT_ALIAS,
            version=v.version,
        )
        return v

    try:
        version = _do_register()
        logger.info(
            "[Instruction Registry] %s v%s (lever=%d, iter=%d, acc=%.3f)",
            prompt_name, version.version, lever, iteration, accuracy,
        )
        return {"prompt_name": prompt_name, "version": version.version}
    except Exception as exc:
        if _is_ownership_conflict(str(exc)) and _try_drop_prompt(prompt_name):
            try:
                version = _do_register()
                logger.info(
                    "[Instruction Registry] %s v%s (re-created after drop)",
                    prompt_name, version.version,
                )
                return {"prompt_name": prompt_name, "version": version.version}
            except Exception:
                pass
        classification = _classify_prompt_registration_error(
            str(exc), uc_schema=uc_schema,
        )
        logger.warning(
            "Instruction registration failed for space=%s: %s (cause=%s)",
            space_id, str(exc)[:300], classification["reason"],
            exc_info=True,
        )
        return None


def register_benchmark_prompts(
    uc_schema: str,
    domain: str,
    experiment_name: str,
) -> dict[str, dict]:
    """Register only the benchmark prompts to MLflow Prompt Registry.

    Called early in preflight (before benchmark generation) so that
    ``_call_llm_for_scoring`` can link benchmark prompts to traces.
    """
    mlflow.set_experiment(experiment_name)
    registered: dict[str, dict] = {}
    for name, template in BENCHMARK_PROMPTS.items():
        candidates = _prompt_name_candidates(
            uc_schema=uc_schema, domain=domain, judge_name=name,
        )
        for prompt_name in candidates:
            try:
                version = mlflow.genai.register_prompt(
                    name=prompt_name,
                    template=template,
                    commit_message=f"Genie benchmark: {name} (domain: {domain})",
                    tags={"domain": domain, "type": "benchmark"},
                )
                mlflow.genai.set_prompt_alias(
                    name=prompt_name,
                    alias=PROMPT_ALIAS,
                    version=version.version,
                )
                registered[name] = {
                    "prompt_name": prompt_name,
                    "version": str(version.version),
                }
                _REGISTERED_PROMPT_NAMES[name] = prompt_name
                logger.info(
                    "[Benchmark Prompt Registry] %s v%s",
                    prompt_name, version.version,
                )
                break
            except Exception as exc:
                if _is_ownership_conflict(str(exc)) and _try_drop_prompt(prompt_name):
                    try:
                        version = mlflow.genai.register_prompt(
                            name=prompt_name,
                            template=template,
                            commit_message=f"Genie benchmark: {name} (domain: {domain})",
                            tags={"domain": domain, "type": "benchmark"},
                        )
                        mlflow.genai.set_prompt_alias(
                            name=prompt_name,
                            alias=PROMPT_ALIAS,
                            version=version.version,
                        )
                        registered[name] = {
                            "prompt_name": prompt_name,
                            "version": str(version.version),
                        }
                        _REGISTERED_PROMPT_NAMES[name] = prompt_name
                        break
                    except Exception:
                        pass
                logger.debug(
                    "Benchmark prompt registration failed for %s name=%s",
                    name, prompt_name, exc_info=True,
                )
        if name not in registered:
            logger.warning("Could not register benchmark prompt: %s", name)
    return registered


def register_judge_prompts(
    uc_schema: str,
    domain: str,
    experiment_name: str,
    *,
    register_registry: bool = True,
) -> dict[str, dict]:
    """Register judge prompts to MLflow Prompt Registry + experiment artifacts.

    Dual storage: Prompt Registry (versioned, aliased) and experiment
    artifacts (UI visibility). Idempotent.
    """
    registered: dict[str, dict] = {}
    failed_judges: list[str] = []
    failed_details: dict[str, dict[str, Any]] = {}

    mlflow.set_experiment(experiment_name)
    if uc_schema:
        try:
            # Align experiment with target prompt registry schema for discoverability.
            mlflow.set_experiment_tags({"mlflow.promptRegistryLocation": uc_schema})
        except Exception:
            logger.warning(
                "Failed to set experiment prompt registry location to %s",
                uc_schema,
                exc_info=True,
            )

    if register_registry:
        for name, template in JUDGE_PROMPTS.items():
            candidates = _prompt_name_candidates(uc_schema=uc_schema, domain=domain, judge_name=name)
            attempt_failures: list[dict[str, Any]] = []
            for prompt_name in candidates:
                try:
                    version = mlflow.genai.register_prompt(
                        name=prompt_name,
                        template=template,
                        commit_message=f"Genie eval judge: {name} (domain: {domain})",
                        tags={"domain": domain, "type": "judge"},
                    )
                    mlflow.genai.set_prompt_alias(
                        name=prompt_name,
                        alias=PROMPT_ALIAS,
                        version=version.version,
                    )
                    registered[name] = {
                        "prompt_name": prompt_name,
                        "version": version.version,
                    }
                    _REGISTERED_PROMPT_NAMES[name] = prompt_name
                    logger.info("[Prompt Registry] %s v%s", prompt_name, version.version)
                    break
                except Exception as exc:
                    err_msg = str(exc).strip()
                    if _is_ownership_conflict(err_msg) and _try_drop_prompt(prompt_name):
                        try:
                            version = mlflow.genai.register_prompt(
                                name=prompt_name,
                                template=template,
                                commit_message=f"Genie eval judge: {name} (domain: {domain})",
                                tags={"domain": domain, "type": "judge"},
                            )
                            mlflow.genai.set_prompt_alias(
                                name=prompt_name,
                                alias=PROMPT_ALIAS,
                                version=version.version,
                            )
                            registered[name] = {
                                "prompt_name": prompt_name,
                                "version": version.version,
                            }
                            _REGISTERED_PROMPT_NAMES[name] = prompt_name
                            logger.info("[Prompt Registry] %s v%s (re-created after drop)", prompt_name, version.version)
                            break
                        except Exception:
                            pass
                    classification = _classify_prompt_registration_error(
                        err_msg,
                        uc_schema=uc_schema,
                    )
                    attempt_failures.append(
                        {
                            "prompt_name": prompt_name,
                            "error": err_msg[:1500],
                            "classification": classification["reason"],
                            "missing_privileges": classification["missing_privileges"],
                            "remediation": classification["remediation"],
                        },
                    )
                    logger.warning(
                        "Prompt registration attempt failed for judge=%s name=%s cause=%s",
                        name,
                        prompt_name,
                        classification["reason"],
                        exc_info=True,
                    )
            if name not in registered:
                logger.error("Prompt registration failed for judge=%s", name)
                failed_judges.append(name)
                last_attempt = attempt_failures[-1] if attempt_failures else {}
                failed_details[name] = {
                    "attempted_names": [attempt.get("prompt_name", "") for attempt in attempt_failures],
                    "classification": last_attempt.get("classification", "unknown"),
                    "missing_privileges": last_attempt.get("missing_privileges", []),
                    "remediation": last_attempt.get("remediation", ""),
                    "last_error": last_attempt.get("error", ""),
                    "attempts": attempt_failures,
                }

    if register_registry:
        _all_extra: dict[str, dict[str, str]] = {}
        for category_label, prompt_dict, tag_type in [
            ("lever", LEVER_PROMPTS, "lever"),
            ("benchmark", BENCHMARK_PROMPTS, "benchmark"),
        ]:
            for name, template in prompt_dict.items():
                if name in _REGISTERED_PROMPT_NAMES:
                    _all_extra[name] = {
                        "prompt_name": _REGISTERED_PROMPT_NAMES[name],
                        "version": "pre-registered",
                    }
                    continue
                candidates = _prompt_name_candidates(uc_schema=uc_schema, domain=domain, judge_name=name)
                for prompt_name in candidates:
                    try:
                        version = mlflow.genai.register_prompt(
                            name=prompt_name,
                            template=template,
                            commit_message=f"Genie {category_label}: {name} (domain: {domain})",
                            tags={"domain": domain, "type": tag_type},
                        )
                        mlflow.genai.set_prompt_alias(
                            name=prompt_name,
                            alias=PROMPT_ALIAS,
                            version=version.version,
                        )
                        _all_extra[name] = {
                            "prompt_name": prompt_name,
                            "version": str(version.version),
                        }
                        _REGISTERED_PROMPT_NAMES[name] = prompt_name
                        logger.info("[Prompt Registry] %s %s v%s", category_label, prompt_name, version.version)
                        break
                    except Exception as exc:
                        err_msg = str(exc).strip()
                        if _is_ownership_conflict(err_msg) and _try_drop_prompt(prompt_name):
                            try:
                                version = mlflow.genai.register_prompt(
                                    name=prompt_name,
                                    template=template,
                                    commit_message=f"Genie {category_label}: {name} (domain: {domain})",
                                    tags={"domain": domain, "type": tag_type},
                                )
                                mlflow.genai.set_prompt_alias(
                                    name=prompt_name,
                                    alias=PROMPT_ALIAS,
                                    version=version.version,
                                )
                                _all_extra[name] = {
                                    "prompt_name": prompt_name,
                                    "version": str(version.version),
                                }
                                _REGISTERED_PROMPT_NAMES[name] = prompt_name
                                logger.info("[Prompt Registry] %s %s v%s (re-created after drop)", category_label, prompt_name, version.version)
                                break
                            except Exception:
                                pass
                        logger.debug(
                            "Prompt registration attempt failed for %s=%s name=%s",
                            category_label, name, prompt_name, exc_info=True,
                        )
                if name not in _all_extra:
                    logger.warning("Could not register %s prompt: %s", category_label, name)
        registered.update(_all_extra)

    active = mlflow.active_run()
    if active:
        _log_judge_prompt_artifacts(
            domain=domain,
            uc_schema=uc_schema,
            registered=registered,
            register_registry=register_registry,
            failed_judges=failed_judges,
            failed_details=failed_details,
        )
    else:
        logger.warning(
            "register_judge_prompts called without an active MLflow run; "
            "prompt artifacts will not be logged to any run."
        )

    if register_registry and STRICT_PROMPT_REGISTRATION and failed_judges:
        cause_codes = sorted(
            {
                str(details.get("classification") or "unknown")
                for details in failed_details.values()
            }
        )
        missing_privileges = sorted(
            {
                str(priv)
                for details in failed_details.values()
                for priv in details.get("missing_privileges", [])
            }
        )
        root_cause_hint = ""
        if missing_privileges and uc_schema:
            root_cause_hint = (
                f" Root-cause hint: missing UC schema privileges {missing_privileges} on {uc_schema}."
            )
        if cause_codes:
            root_cause_hint += f" Detected cause classes: {cause_codes}."
        raise RuntimeError(
            "Prompt registration failed for judges: "
            + ", ".join(sorted(failed_judges))
            + "."
            + root_cause_hint,
        )

    total_prompt_count = len(JUDGE_PROMPTS) + len(LEVER_PROMPTS) + len(BENCHMARK_PROMPTS)
    logger.info(
        "Registered %d/%d prompts (judges=%d, levers=%d, benchmarks=%d, registry=%s)",
        len(registered), total_prompt_count,
        len(JUDGE_PROMPTS), len(LEVER_PROMPTS), len(BENCHMARK_PROMPTS),
        bool(register_registry and uc_schema),
    )
    return registered


def register_scorers_with_experiment(
    scorers: list,
    experiment_name: str,
) -> dict[str, Any]:
    """Register scorers with the MLflow experiment so they appear in the Judges tab.

    Iterates over *scorers*, calling ``.register(name=...)`` on each.
    Failures are logged but do **not** halt evaluation.
    """
    mlflow.set_experiment(experiment_name)

    registered: dict[str, Any] = {}
    failures: list[tuple[str, Exception]] = []

    for s in scorers:
        name = getattr(s, "name", getattr(s, "__name__", str(s)))
        try:
            reg = s.register(name=name)
            registered[name] = reg
            logger.info("[Scorer Registration] Registered %s", name)
        except ValueError as ve:
            if "already been registered" in str(ve):
                registered[name] = name
                logger.info("[Scorer Registration] %s already registered — skipping", name)
            else:
                failures.append((name, ve))
                logger.warning(
                    "[Scorer Registration] Failed to register %s: %s: %s",
                    name,
                    type(ve).__name__,
                    str(ve)[:400],
                )
        except Exception as exc:
            failures.append((name, exc))
            logger.warning(
                "[Scorer Registration] Failed to register %s: %s: %s",
                name,
                type(exc).__name__,
                str(exc)[:400],
            )

    logger.info(
        "Scorer registration complete: %d/%d registered",
        len(registered),
        len(scorers),
    )
    if failures:
        logger.warning(
            "Scorer registration failures: %s",
            ", ".join(f"{n}: {e}" for n, e in failures),
        )

    return registered


def _log_judge_prompt_artifacts(
    *,
    domain: str,
    uc_schema: str,
    registered: dict[str, dict],
    register_registry: bool,
    failed_judges: list[str] | None = None,
    failed_details: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Log judge definitions to the current run for run-level traceability."""
    judges_manifest: dict[str, Any] = {
        "domain": domain,
        "uc_schema": uc_schema,
        "register_registry": register_registry,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "failed_judges": failed_judges or [],
        "failed_judge_details": failed_details or {},
        "judges": [],
    }
    for name, template in JUDGE_PROMPTS.items():
        prompt_name = format_mlflow_template(PROMPT_NAME_TEMPLATE, uc_schema=uc_schema, judge_name=name) if uc_schema else name
        template_hash = hashlib.sha256(template.encode("utf-8")).hexdigest()
        prompt_meta = registered.get(name, {})
        judges_manifest["judges"].append(
            {
                "name": name,
                "prompt_name": prompt_meta.get("prompt_name", prompt_name),
                "prompt_version": prompt_meta.get("version"),
                "prompt_alias": PROMPT_ALIAS,
                "template_sha256": template_hash,
            }
        )
        mlflow.log_text(template, f"judge_prompts/{name}/template.txt")

    mlflow.log_dict(judges_manifest, "judge_prompts/manifest.json")
    mlflow.log_params(
        {
            "num_prompts": len(JUDGE_PROMPTS),
            "prompt_keys": ",".join(JUDGE_PROMPTS.keys()),
            "domain": domain,
            "judge_registry_logged_to_run": "true",
        },
    )
    mlflow.set_tags(
        {
            "traceability.judges_logged": "true",
            "traceability.judges_count": str(len(JUDGE_PROMPTS)),
            "traceability.uc_schema": uc_schema or "",
        },
    )


def _prompt_name_candidates(uc_schema: str, domain: str, judge_name: str) -> list[str]:
    """Try UC-qualified name first, then portable fallback names."""
    safe_domain = re.sub(r"[^a-zA-Z0-9_]+", "_", domain or "default").strip("_").lower() or "default"
    candidates: list[str] = []
    if uc_schema:
        candidates.append(format_mlflow_template(PROMPT_NAME_TEMPLATE, uc_schema=uc_schema, judge_name=judge_name))
        candidates.append(f"{uc_schema}.genie_opt_{safe_domain}_{judge_name}")
    candidates.append(f"genie_opt_{safe_domain}_{judge_name}")
    return list(dict.fromkeys(candidates))


def _configure_uc_trace_destination(
    *,
    experiment_id: str,
    uc_schema: str,
    warehouse_id: str,
) -> str:
    """Traces are stored in the MLflow experiment (default storage).

    UC OTEL trace storage is intentionally skipped: calling
    ``set_destination(UC)`` before the UC tables are fully provisioned
    causes all traces to be silently lost, breaking the evaluation UI.

    We also actively clear any stale UC destination that a previous run
    (or old code path) may have set in this process.
    """
    os.environ.pop("MLFLOW_TRACING_DESTINATION", None)
    try:
        mlflow.tracing.reset()
    except Exception:
        pass
    logger.info("Traces will be stored in MLflow experiment (default storage)")
    return ""


def _is_retryable_eval_exception(exc: Exception) -> bool:
    """Return True for known transient mlflow.genai.evaluate() harness failures.

    Known patterns (all originate inside mlflow.genai.evaluation.harness):
      1. ``eval_item.trace`` is None  ->  AttributeError: 'NoneType' ... 'info'
      2. ``eval_item.trace.info`` is None  ->  AttributeError on .assessments
      3. Transient gRPC / Spark Connect timeouts during scorer execution
    """
    message = str(exc).lower()
    full_tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tb_text = "".join(full_tb).lower()

    if isinstance(exc, AttributeError):
        if "nonetype" in message and ("info" in message or "assessments" in message or "trace" in message):
            return True
        if "harness" in tb_text and "nonetype" in message:
            return True

    if "grpc" in message or "_multithreadedrendezvous" in message:
        return True

    if "harness" in tb_text and ("nonetype" in tb_text or "trace" in tb_text):
        if isinstance(exc, (AttributeError, TypeError)):
            return True

    return False


def _qid_trace_map_from_search_traces_df(traces_df: Any) -> dict[str, str]:
    """Extract ``{question_id: trace_id}`` from a ``mlflow.search_traces`` DataFrame."""
    recovered: dict[str, str] = {}
    if traces_df is None or len(traces_df) == 0:
        return recovered
    for _, row in traces_df.iterrows():
        tid = row.get("trace_id")
        tags = row.get("tags")
        qid = ""
        if isinstance(tags, dict):
            qid = tags.get("question_id", "") or ""
        if tid and qid:
            recovered[qid] = str(tid)
    return recovered


def _recover_trace_map_via_tags(
    experiment_id: str,
    optimization_run_id: str,
    iteration: int,
    expected_count: int,
) -> dict[str, str]:
    """Strategy 1: tag-based search using ``optimization_run_id`` + ``iteration``."""
    if not experiment_id or not optimization_run_id:
        return {}
    try:
        filter_parts = [
            f"tags.`genie.optimization_run_id` = '{optimization_run_id}'",
            f"tags.`genie.iteration` = '{iteration}'",
        ]
        traces_df = mlflow.search_traces(
            locations=[experiment_id],
            filter_string=" AND ".join(filter_parts),
            max_results=max(500, expected_count * 2),
        )
        return _qid_trace_map_from_search_traces_df(traces_df)
    except Exception:
        logger.debug("Trace recovery strategy 1 (tags) failed", exc_info=True)
        return {}


def _recover_trace_map_via_time_window(
    experiment_id: str,
    start_time_ms: int | None,
    expected_count: int,
) -> dict[str, str]:
    """Strategy 2: match ``tags.question_id`` within the predict_fn time window.

    Useful when Spark Connect swallows the ``optimization_run_id`` /
    ``iteration`` tag updates but the ``question_id`` tag (set earlier in
    the same span) still propagates.
    """
    if not experiment_id or not start_time_ms:
        return {}
    try:
        traces_df = mlflow.search_traces(
            locations=[experiment_id],
            filter_string=f"attributes.timestamp_ms >= {int(start_time_ms)}",
            max_results=max(500, expected_count * 2),
        )
        return _qid_trace_map_from_search_traces_df(traces_df)
    except Exception:
        logger.debug("Trace recovery strategy 2 (time window) failed", exc_info=True)
        return {}


def _recover_trace_map_via_eval_results(eval_result: Any) -> dict[str, str]:
    """Strategy 3: read ``eval_results`` table's ``trace_id`` column (MLflow ≥ 2.18)."""
    if eval_result is None or not hasattr(eval_result, "tables"):
        return {}
    try:
        tables = eval_result.tables
        if "eval_results" not in tables:
            return {}
        df = tables["eval_results"]
        if df is None or len(df) == 0 or "trace_id" not in df.columns:
            return {}

        recovered: dict[str, str] = {}
        for _, row in df.iterrows():
            tid = row.get("trace_id")
            if not tid:
                continue
            qid = (
                row.get("inputs/question_id")
                or (row.get("inputs") or {}).get("question_id", "")
                if isinstance(row.get("inputs"), dict)
                else row.get("inputs/question_id")
            )
            qid = qid or row.get("question_id") or ""
            if qid:
                recovered[str(qid)] = str(tid)
        return recovered
    except Exception:
        logger.debug("Trace recovery strategy 3 (eval_results) failed", exc_info=True)
        return {}


def _log_trace_map_recovery_metric(strategy: str, hit_count: int) -> None:
    """Log per-strategy recovery hit counts as MLflow metrics (best-effort)."""
    try:
        mlflow.log_metric(f"trace_map.recovery.{strategy}.hit_count", float(hit_count))
    except Exception:
        logger.debug(
            "Could not log trace_map.recovery.%s.hit_count", strategy, exc_info=True
        )


def _recover_trace_map(
    experiment_id: str,
    optimization_run_id: str,
    iteration: int,
    expected_count: int = 0,
    *,
    start_time_ms: int | None = None,
    eval_result: Any = None,
) -> dict[str, str]:
    """Recover ``question_id -> trace_id`` when ``mlflow.genai.evaluate()`` loses it.

    Tries three independent strategies in order and UNIONs their results —
    later strategies fill qids that earlier strategies didn't cover. This
    replaces the previous "first non-empty wins" behavior which lost
    traces when strategy 1 returned a partial match (observed symptom:
    ``Recovered 14/22 trace IDs``).

    Strategies are ordered by preference (most authoritative first):

    1. ``tags`` — filter experiment traces by
       ``genie.optimization_run_id`` + ``genie.iteration``.
    2. ``time_window`` — filter by ``start_time_ms`` and match
       ``tags.question_id`` (survives when tag updates are swallowed but
       the earlier ``question_id`` tag made it in).
    3. ``eval_results`` — read ``eval_result.tables['eval_results']
       ['trace_id']`` directly (available on MLflow ≥ 2.18).

    Contract:
      * If two strategies return values for the same qid, the earlier
        strategy's value wins (first-writer-wins per qid).
      * Once ``len(recovered) >= expected_count`` the loop short-circuits
        and remaining strategies are not invoked — preserves the
        zero-extra-API-call happy-path cost.
      * Each strategy's metric reports NEW qids it contributed (not raw
        returned size) so sums across strategies = total distinct
        recovered.
    """
    strategies: list[tuple[str, Any]] = [
        (
            "tags",
            lambda: _recover_trace_map_via_tags(
                experiment_id, optimization_run_id, iteration, expected_count
            ),
        ),
        (
            "time_window",
            lambda: _recover_trace_map_via_time_window(
                experiment_id, start_time_ms, expected_count
            ),
        ),
        (
            "eval_results",
            lambda: _recover_trace_map_via_eval_results(eval_result),
        ),
    ]

    recovered: dict[str, str] = {}
    per_strategy_hits: list[tuple[str, int]] = []

    for idx, (name, fn) in enumerate(strategies):
        if expected_count and len(recovered) >= expected_count:
            for remaining_name, _ in strategies[idx:]:
                per_strategy_hits.append((remaining_name, 0))
            break
        partial = fn() or {}
        new_hits = 0
        for qid, tid in partial.items():
            if qid not in recovered:
                recovered[qid] = tid
                new_hits += 1
        per_strategy_hits.append((name, new_hits))

    for name, count in per_strategy_hits:
        _log_trace_map_recovery_metric(name, count)

    if recovered:
        logger.info(
            "Trace map recovery: %d/%d traces recovered "
            "(per-strategy new hits: %s)",
            len(recovered), expected_count,
            ", ".join(f"{n}={c}" for n, c in per_strategy_hits),
        )
    else:
        logger.info(
            "All trace map recovery strategies returned 0 traces "
            "(iteration=%d, expected=%d)",
            iteration, expected_count,
        )
    return recovered


_HARNESS_PATCHED = False


def _patch_mlflow_harness_none_trace() -> None:
    """Monkey-patch MLflow internals that crash when eval_item.trace is None.

    MLflow >=3.4 has multiple code paths that access ``eval_item.trace.info``
    without guarding against ``trace`` being ``None``:

      1. ``harness._get_new_expectations`` (line ~394) — crashes on
         ``eval_item.trace.info.assessments``
      2. ``trace_utils.batch_link_traces_to_run`` (line ~964) — crashes on
         ``eval_result.eval_item.trace.info.trace_id`` in a list comprehension

    When the predict function involves complex I/O (Genie API + Spark Connect),
    the MLflow trace context can be lost, leaving ``trace = None``.  These patches
    allow evaluation to complete successfully even when traces are missing.
    """
    global _HARNESS_PATCHED
    if _HARNESS_PATCHED:
        return

    patched: list[str] = []

    try:
        import mlflow.genai.evaluation.harness as _harness_mod

        _orig_get_new_expectations = _harness_mod._get_new_expectations

        def _safe_get_new_expectations(eval_item: Any) -> list:
            if eval_item is None:
                return []
            trace = getattr(eval_item, "trace", None)
            if trace is None or getattr(trace, "info", None) is None:
                return []
            try:
                return _orig_get_new_expectations(eval_item)
            except Exception:
                return []

        _harness_mod._get_new_expectations = _safe_get_new_expectations  # type: ignore[assignment]
        patched.append("_get_new_expectations")
    except Exception:
        logger.warning("Could not patch _get_new_expectations", exc_info=True)

    try:
        import mlflow.genai.utils.trace_utils as _trace_utils_mod

        _orig_batch_link = _trace_utils_mod.batch_link_traces_to_run

        def _safe_batch_link_traces_to_run(*args: Any, **kwargs: Any) -> Any:
            eval_results = kwargs.get("eval_results") or (args[1] if len(args) > 1 else [])

            def _has_valid_trace(r: Any) -> bool:
                ei = getattr(r, "eval_item", None)
                if ei is None:
                    return False
                tr = getattr(ei, "trace", None)
                return tr is not None and getattr(tr, "info", None) is not None

            safe_results = [r for r in eval_results if _has_valid_trace(r)]
            if not safe_results:
                logger.info(
                    "batch_link_traces_to_run: %d/%d eval results have None traces, skipping linkage",
                    len(eval_results) - len(safe_results),
                    len(eval_results),
                )
                return None
            kwargs["eval_results"] = safe_results
            if args:
                return _orig_batch_link(args[0], **kwargs)
            return _orig_batch_link(**kwargs)

        _trace_utils_mod.batch_link_traces_to_run = _safe_batch_link_traces_to_run  # type: ignore[assignment]

        # Aggressively patch every module that imported the function directly,
        # scanning sys.modules to catch all references regardless of import style.
        import sys as _sys
        _patched_modules: list[str] = []
        for _mod_name, _mod_obj in list(_sys.modules.items()):
            if _mod_obj is None or _mod_obj is _trace_utils_mod:
                continue
            try:
                if hasattr(_mod_obj, "batch_link_traces_to_run"):
                    _existing = getattr(_mod_obj, "batch_link_traces_to_run")
                    if _existing is not _safe_batch_link_traces_to_run:
                        setattr(_mod_obj, "batch_link_traces_to_run", _safe_batch_link_traces_to_run)
                        _patched_modules.append(_mod_name)
            except Exception:
                pass
        if _patched_modules:
            logger.info(
                "Patched batch_link_traces_to_run in %d modules: %s",
                len(_patched_modules),
                ", ".join(_patched_modules),
            )

        patched.append("batch_link_traces_to_run")
    except Exception:
        logger.warning("Could not patch batch_link_traces_to_run", exc_info=True)

    _HARNESS_PATCHED = True
    if patched:
        logger.info("Patched MLflow None-trace safety: %s", ", ".join(patched))


def _run_evaluate_with_retries(
    *,
    evaluate_kwargs: dict[str, Any],
) -> tuple[Any, list[dict[str, Any]]]:
    """Run mlflow.genai.evaluate() with targeted retry for transient harness errors."""
    _patch_mlflow_harness_none_trace()

    attempts: list[dict[str, Any]] = []
    initial_workers = os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS")
    initial_scorer_workers = os.getenv("MLFLOW_GENAI_EVAL_MAX_SCORER_WORKERS")
    initial_skip_validation = os.getenv("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION")
    os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = "True"
    os.environ["MLFLOW_GENAI_EVAL_MAX_SCORER_WORKERS"] = "10"

    try:
        for attempt in range(1, max(1, EVAL_MAX_ATTEMPTS) + 1):
            workers = "1" if attempt == 1 else EVAL_SINGLE_WORKER_FALLBACK
            os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = workers

            try:
                result = mlflow.genai.evaluate(**evaluate_kwargs)
                attempts.append(
                    {
                        "attempt": attempt,
                        "workers": os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS"),
                        "status": "success",
                    }
                )
                return result, attempts
            except Exception as exc:
                err_type = type(exc).__name__
                err_message = str(exc)
                attempts.append(
                    {
                        "attempt": attempt,
                        "workers": os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS"),
                        "status": "failed",
                        "error_type": err_type,
                        "error_message": err_message[:1000],
                        "traceback": traceback.format_exc(limit=30),
                    }
                )
                retryable = _is_retryable_eval_exception(exc)
                logger.exception(
                    "mlflow.genai.evaluate failed (attempt %d/%d, retryable=%s)",
                    attempt,
                    EVAL_MAX_ATTEMPTS,
                    retryable,
                )
                if attempt >= EVAL_MAX_ATTEMPTS or not retryable:
                    setattr(exc, "_eval_attempts", attempts)
                    raise
                time.sleep(EVAL_RETRY_SLEEP_SECONDS * attempt)
    finally:
        if initial_workers is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_MAX_WORKERS", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = initial_workers
        if initial_scorer_workers is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_MAX_SCORER_WORKERS", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_MAX_SCORER_WORKERS"] = initial_scorer_workers
        if initial_skip_validation is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = initial_skip_validation

    raise RuntimeError("Evaluation retry loop exhausted unexpectedly")


def _run_evaluate_sequential_fallback(
    *,
    evaluate_kwargs: dict[str, Any],
) -> Any:
    """Deterministic fallback path: evaluate one benchmark row at a time.

    Each row is wrapped in try/except so a single harness failure (e.g. a
    None-trace bug in mlflow) does not crash the entire evaluation.
    """
    _patch_mlflow_harness_none_trace()

    data = evaluate_kwargs.get("data")
    if not isinstance(data, pd.DataFrame):
        logger.info("Sequential fallback: converting non-DataFrame data to DataFrame")
        if hasattr(data, "to_dataframe"):
            data = data.to_dataframe()
        elif hasattr(data, "to_df"):
            data = data.to_df()
        else:
            raise RuntimeError("Sequential fallback requires DataFrame-convertible input")
        evaluate_kwargs = dict(evaluate_kwargs)
        evaluate_kwargs["data"] = data
    if data.empty:
        raise RuntimeError("Sequential fallback requires non-empty DataFrame input")

    metrics_accumulator: dict[str, list[float]] = {}
    row_tables: list[pd.DataFrame] = []
    skipped_count = 0
    total_rows = len(data)

    previous_workers = os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS")
    previous_skip = os.getenv("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION")
    os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"
    os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = "True"
    try:
        for row_idx in range(total_rows):
            row_df = data.iloc[[row_idx]].reset_index(drop=True)
            row_kwargs = dict(evaluate_kwargs)
            row_kwargs["data"] = row_df
            try:
                row_result = mlflow.genai.evaluate(**row_kwargs)
            except Exception as row_exc:
                logger.warning(
                    "Sequential fallback: row %d/%d failed, skipping: %s",
                    row_idx + 1,
                    total_rows,
                    str(row_exc)[:300],
                )
                skipped_count += 1
                continue

            if hasattr(row_result, "metrics"):
                for metric_name, value in row_result.metrics.items():
                    if isinstance(value, (int, float)):
                        metrics_accumulator.setdefault(metric_name, []).append(float(value))

            if hasattr(row_result, "tables") and isinstance(row_result.tables, dict):
                eval_table = row_result.tables.get("eval_results")
                if isinstance(eval_table, pd.DataFrame):
                    row_tables.append(eval_table)
    finally:
        if previous_workers is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_MAX_WORKERS", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = previous_workers
        if previous_skip is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = previous_skip

    if skipped_count:
        logger.warning(
            "Sequential fallback completed with %d/%d rows skipped due to harness errors",
            skipped_count,
            total_rows,
        )

    metrics = {
        metric_name: (sum(values) / len(values))
        for metric_name, values in metrics_accumulator.items()
        if values
    }
    merged_eval_results = (
        pd.concat(row_tables, ignore_index=True) if row_tables else pd.DataFrame()
    )
    return SimpleNamespace(
        metrics=metrics,
        tables={"eval_results": merged_eval_results},
        skipped_count=skipped_count,
    )


def _collect_infra_eval_errors(rows: list[dict[str, Any]]) -> list[str]:
    """Extract infrastructure-like SQL errors from eval result rows.

    Only checks specific error/comparison columns — NOT scorer rationales or
    arbitrary string values, which frequently contain error keywords as part of
    legitimate judge explanations (e.g. "TABLE_OR_VIEW_NOT_FOUND" in a
    rationale describing why the Genie response was wrong).
    """
    infra_errors: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        candidates: list[str] = []
        outputs = row.get("outputs")
        if isinstance(outputs, dict):
            comparison = outputs.get("comparison")
            if isinstance(comparison, dict):
                err = comparison.get("error")
                err_type = comparison.get("error_type", "")
                if err and str(err_type) == "infrastructure":
                    candidates.append(str(err))
        for key in (
            "outputs/comparison/error",
            "comparison/error",
            "comparison.error",
        ):
            err = row.get(key)
            if not err:
                continue
            err_type_key = key.replace("/error", "/error_type").replace(".error", ".error_type")
            err_type = row.get(err_type_key, "")
            if str(err_type) == "infrastructure":
                candidates.append(str(err))

        for msg in candidates:
            if _is_infrastructure_sql_error(msg):
                infra_errors.append(msg[:500])

    seen: set[str] = set()
    deduped: list[str] = []
    for msg in infra_errors:
        if msg in seen:
            continue
        seen.add(msg)
        deduped.append(msg)
    return deduped


def create_evaluation_dataset(
    spark: SparkSession,
    benchmarks: list[dict],
    uc_schema: str,
    domain: str,
    space_id: str = "",
    catalog: str = "",
    gold_schema: str = "",
    experiment_id: str = "",
    *,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> Any | None:
    """Create or update the MLflow UC evaluation dataset from benchmarks.

    Uses ``merge_records`` (upsert by question_id) to preserve version history
    rather than dropping and recreating each run.

    Pass *experiment_id* to link the dataset to the experiment so it appears
    in the experiment's Datasets tab in the UI.
    """
    uc_table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    exp_ids = [experiment_id] if experiment_id else None
    try:
        try:
            eval_dataset = mlflow.genai.datasets.get_dataset(name=uc_table_name)
            logger.info("Reusing existing evaluation dataset: %s", uc_table_name)
        except Exception:
            create_kwargs: dict[str, Any] = {"name": uc_table_name}
            if exp_ids:
                create_kwargs["experiment_id"] = exp_ids
            eval_dataset = mlflow.genai.datasets.create_dataset(**create_kwargs)
            logger.info(
                "Created new evaluation dataset: %s (experiment_id=%s)",
                uc_table_name, exp_ids,
            )
        if len(benchmarks) > max_benchmark_count:
            benchmarks = _truncate_benchmarks(benchmarks, max_benchmark_count)
        records = []
        _seen_questions: set[str] = set()
        _dup_count = 0
        for b in benchmarks:
            _q_key = str(b.get("question", "")).lower().strip()
            if _q_key in _seen_questions:
                _dup_count += 1
                continue
            _seen_questions.add(_q_key)

            _expected_sql = b.get("expected_sql", "")
            expectations = {
                "expected_response": _expected_sql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    _expected_sql,
                    hint=b.get("expected_asset_hint"),
                ),
                "category": b.get("category", ""),
                "source": b.get("source", ""),
                "provenance": b.get("provenance", ""),
                "validation_status": b.get("validation_status", ""),
                "validation_reason_code": b.get("validation_reason_code", ""),
                "validation_error": b.get("validation_error", ""),
                "correction_source": b.get("correction_source", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "temporal_stale": b.get("temporal_stale", False),
                "asset_fingerprint": b.get("asset_fingerprint", ""),
                "split": b.get("split", "train"),
            }
            expectations = {k: v for k, v in expectations.items() if v is not None}
            records.append(
                {
                    "inputs": {
                        "question_id": b.get("id", ""),
                        "question": b["question"],
                        "space_id": space_id,
                        "expected_sql": b.get("expected_sql", ""),
                        "catalog": catalog,
                        "gold_schema": gold_schema,
                        "order_sensitive": bool(b.get("order_sensitive", False)),
                    },
                    "expectations": expectations,
                }
            )
        if _dup_count:
            logger.warning(
                "Dropped %d duplicate benchmark(s) by question text before persisting to %s",
                _dup_count, uc_table_name,
            )
        if len(records) > max_benchmark_count:
            records = _truncate_benchmarks(
                [{"provenance": r.get("expectations", {}).get("provenance", "other"), **r} for r in records],
                max_benchmark_count,
            )
            for r in records:
                r.pop("provenance", None)
        retry_delta_write(
            lambda: eval_dataset.merge_records(records),
            operation_name="evaluation_dataset.merge_records",
            table_name=uc_table_name,
        )
        logger.info("UC Evaluation Dataset: %s (%d records merged)", uc_table_name, len(records))
        return eval_dataset
    except Exception:
        logger.exception("UC dataset creation failed for %s", uc_table_name)
        raise


def _drop_benchmark_table(spark: SparkSession, uc_table_name: str) -> None:
    """Best-effort DROP of the benchmark table to clear stale rows."""
    try:
        parts = uc_table_name.split(".")
        quoted = ".".join(f"`{p.strip('`')}`" for p in parts)
        spark.sql(f"DROP TABLE IF EXISTS {quoted}")
        logger.info("Dropped stale benchmark table %s", uc_table_name)
    except Exception:
        logger.warning("Could not drop benchmark table %s (may not exist)", uc_table_name, exc_info=True)


# Judges that ``_compute_arbiter_adjusted_accuracy`` and per_judge aggregation
# will flip from FAIL to PASS when the arbiter rules for Genie. Keep in sync
# with ``_ARBITER_ADJUSTABLE_JUDGES`` below (duplicated at module scope so the
# display helper doesn't reach into run_evaluation()'s inner locals).
_ARBITER_ADJUSTABLE_DISPLAY_JUDGES = frozenset({
    "result_correctness",
    "schema_accuracy",
    "logical_accuracy",
    "semantic_equivalence",
    "completeness",
})


_JUDGE_ORDER = [
    "syntax_validity", "schema_accuracy", "logical_accuracy",
    "semantic_equivalence", "completeness", "response_quality",
    "asset_routing", "result_correctness", "arbiter",
]


def _build_summary_row(row_dict: dict) -> list[dict]:
    """Return a canonical per-judge view used by :func:`_print_eval_summary`.

    Each element has shape::

        {
            "judge": <judge name>,
            "value": <verdict string, possibly empty>,
            "rationale": <str or "">,
        }

    Rationale is resolved in the precedence order installed by
    :func:`_merge_row_sources` (trace > cache > flat col), and is expected
    to be non-empty whenever a non-empty verdict is present. When the
    ``GSO_ASSERT_ROW_CANONICAL=1`` env var is set, this function asserts
    that invariant loudly so regressions show up in CI rather than as
    silent display bugs in the terminal summary. In production the
    assertion is a no-op.

    This helper must only be called with rows that have already been
    merged via :func:`_merge_row_sources`; passing a raw ``results_df``
    row (without the merge step) may produce misaligned rationales.
    """
    out: list[dict] = []
    assert_canonical = os.environ.get("GSO_ASSERT_ROW_CANONICAL") == "1"
    for judge in _JUDGE_ORDER:
        val = row_dict.get(f"{judge}/value", row_dict.get(judge, ""))
        val_str = "" if val is None else str(val)
        rationale = row_dict.get(f"{judge}/rationale", "")
        if not isinstance(rationale, str):
            rationale = str(rationale) if rationale is not None else ""
        if assert_canonical and val_str and not rationale:
            raise AssertionError(
                f"Non-canonical summary row: judge={judge!r} value={val_str!r} "
                f"but rationale is empty; _merge_row_sources likely not called."
            )
        out.append({"judge": judge, "value": val_str, "rationale": rationale})
    return out


_LOGICAL_JUDGES = frozenset({"result_correctness", "semantic_equivalence"})
_ARBITER_LOGICAL_PASS = frozenset({"genie_correct", "both_correct"})


def _compute_pass_buckets(row: dict) -> tuple[bool, bool]:
    """Classify a row into ``(logical_pass, all_judge_pass)``.

    - ``all_judge_pass`` (legacy) fails if *any* judge is ``no`` /
      ``false`` / numeric-zero, or the arbiter is
      ``ground_truth_correct`` / ``neither_correct``.
    - ``logical_pass`` (new, B3 headline) fails only when
      ``result_correctness`` or ``semantic_equivalence`` explicitly
      says ``no`` or the arbiter settled on a non-logical-correct
      verdict. Cosmetic or routing-only failures (e.g. ``asset_routing``,
      ``completeness`` warnings) do **not** flip ``logical_pass``.

    Under ``GSO_SCORING_V2=off`` the caller selects ``all_judge_pass``
    as the headline. Under ``on``/``shadow`` the caller selects
    ``logical_pass``. Both values are always computed so the legacy
    count can be logged as a shadow metric.
    """
    any_judge_fail = False
    for judge in _JUDGE_ORDER:
        val = str(row.get(f"{judge}/value", row.get(judge, ""))).lower()
        if val in ("no", "false", "0", "0.0"):
            if judge == "arbiter":
                if val not in ("genie_correct", "both_correct"):
                    any_judge_fail = True
            else:
                any_judge_fail = True
    arbiter_val = str(
        row.get("arbiter/value", row.get("arbiter", ""))
    ).lower()
    if arbiter_val in ("ground_truth_correct", "neither_correct"):
        any_judge_fail = True
    all_judge_pass = not any_judge_fail

    logical_fail = False
    for judge in _LOGICAL_JUDGES:
        val = str(row.get(f"{judge}/value", row.get(judge, ""))).lower()
        if val in ("no", "false", "0", "0.0"):
            logical_fail = True
    if arbiter_val and arbiter_val not in _ARBITER_LOGICAL_PASS | {
        "",
        "skipped",
        "n/a",
    }:
        logical_fail = True
    logical_pass = not logical_fail

    return logical_pass, all_judge_pass


def _get_nested(row: dict, *paths: str, default: Any = "") -> Any:
    """Try multiple key paths (both flattened and nested dict forms)."""
    for path in paths:
        if "/" in path:
            val = row.get(path)
            if val not in (None, "", {}, []):
                return val
            parts = path.split("/", 1)
            parent = row.get(parts[0])
            if isinstance(parent, dict) and len(parts) == 2:
                val = parent.get(parts[1])
                if val not in (None, "", {}, []):
                    return val
        else:
            val = row.get(path)
            if val not in (None, "", {}, []):
                return val
    return default


def _print_eval_summary(
    rows: list[dict],
    scores_100: dict[str, float],
    thresholds_passed: bool,
    iteration: int,
    eval_scope: str,
    total_questions: int,
) -> None:
    """Print a nicely formatted per-question evaluation summary to stdout."""
    lines: list[str] = []
    lines.append("")
    header = (
        f"  EVALUATION SUMMARY — Iteration {iteration} | "
        f"Scope: {eval_scope} | Questions: {total_questions}"
    )
    width = max(len(header) + 4, 78)
    lines.append("=" * width)
    lines.append(header)
    lines.append("=" * width)

    _logical_pass_count = 0
    _all_judge_pass_count = 0
    _arbiter_rescued_count = 0
    _fail_count = 0
    use_legacy_headline = scoring_v2_is_legacy()

    for qi, row in enumerate(rows, 1):
        _request = row.get("request", {})
        if isinstance(_request, str):
            try:
                _request = json.loads(_request)
            except (json.JSONDecodeError, ValueError):
                _request = {}
        if not isinstance(_request, dict):
            _request = {}

        _response = row.get("response", {})
        if isinstance(_response, str):
            try:
                _response = json.loads(_response)
            except (json.JSONDecodeError, ValueError):
                _response = {}
        if not isinstance(_response, dict):
            _response = {}

        qid = (
            _request.get("question_id")
            or _get_nested(row, "inputs/question_id", "question_id")
            or f"q{qi}"
        )
        question = (
            _request.get("question")
            or _get_nested(row, "inputs/question", "question")
            or ""
        )

        logical_pass, all_judge_pass = _compute_pass_buckets(row)
        if logical_pass:
            _logical_pass_count += 1
        if all_judge_pass:
            _all_judge_pass_count += 1
        arbiter_val = str(
            row.get("arbiter/value", row.get("arbiter", ""))
        ).lower()

        headline_pass = all_judge_pass if use_legacy_headline else logical_pass

        if headline_pass:
            tag = "ALL PASS" if all_judge_pass else "LOGICAL PASS"
            lines.append(
                f"  Q{qi}: [{qid}] \"{question[:80]}\" — {tag} ({arbiter_val})"
            )
            continue

        # Non-headline-pass rows split into two buckets so the header
        # reconciles with ``Overall accuracy: X/Y`` below:
        #   * arbiter-rescued — rc=no but arbiter settled on
        #     ``genie_correct``/``both_correct`` → contributes to
        #     ``Overall accuracy`` numerator.
        #   * real fail — neither judges nor arbiter saved the row.
        if arbiter_val in _ARBITER_CORRECT_VERDICTS:
            _arbiter_rescued_count += 1
        else:
            _fail_count += 1

        genie_sql = (
            _response.get("response")
            or _get_nested(row, "outputs/response")
            or "(none)"
        )
        status = (
            _response.get("status")
            or _get_nested(row, "outputs/status", "status")
            or row.get("state", "?")
        )
        gt_sql = (
            _request.get("expected_sql")
            or _get_nested(
                row, "expectations/expected_response", "expected_response",
                "inputs/expected_sql", "expected_sql",
            )
            or "(none)"
        )

        cmp = _response.get("comparison", {})
        if not isinstance(cmp, dict):
            cmp = {}
        if not cmp:
            outputs_val = row.get("outputs")
            if isinstance(outputs_val, dict):
                cmp = outputs_val.get("comparison", {})
            if not cmp:
                cmp_raw = row.get("outputs/comparison", {})
                cmp = cmp_raw if isinstance(cmp_raw, dict) else {}

        match_str = "YES" if cmp.get("match") else "NO"
        match_type = cmp.get("match_type", "n/a")

        lines.append("")
        lines.append(f"--- Q{qi}: {qid} " + "-" * max(0, width - len(f"--- Q{qi}: {qid} ") - 1))
        lines.append(f"| Question:  \"{question}\"")
        lines.append(f"|")
        lines.append(f"| Genie SQL:")
        lines.append(f"|   {genie_sql}")
        lines.append(f"| Genie Status: {status}")

        analysis = (
            _response.get("analysis_text")
            or _get_nested(row, "outputs/analysis_text")
            or None
        )
        if analysis:
            lines.append(f"| Genie Analysis: {str(analysis)[:200]}")

        lines.append(f"|")
        lines.append(f"| Ground Truth SQL:")
        lines.append(f"|   {gt_sql}")
        lines.append(f"|")
        lines.append(
            f"| Result Comparison: Match: {match_str} ({match_type}) | "
            f"GT rows: {cmp.get('gt_rows', '?')} | Genie rows: {cmp.get('genie_rows', '?')}"
        )
        if cmp.get("gt_hash") or cmp.get("genie_hash"):
            lines.append(
                f"|   GT hash: {cmp.get('gt_hash', 'n/a')} | "
                f"Genie hash: {cmp.get('genie_hash', 'n/a')}"
            )
        if cmp.get("error"):
            lines.append(f"|   Error: {cmp['error']}")
        if cmp.get("gt_sample"):
            lines.append("|   GT Result Sample (first 5 rows):")
            for sample_line in str(cmp["gt_sample"]).strip().split("\n")[:6]:
                lines.append(f"|     {sample_line}")
        if cmp.get("genie_sample"):
            lines.append("|   Genie Result Sample (first 5 rows):")
            for sample_line in str(cmp["genie_sample"]).strip().split("\n")[:6]:
                lines.append(f"|     {sample_line}")
        lines.append(f"|")
        lines.append(f"| Judge Verdicts:")

        for entry in _build_summary_row(row):
            judge = entry["judge"]
            val_str = entry["value"] or "n/a"
            rationale = entry["rationale"]
            short_rat = rationale.split("\n")[0][:120] if rationale else ""

            if val_str.lower() in ("yes", "true", "1", "1.0", "skipped"):
                verdict_label = "PASS" if val_str.lower() != "skipped" else val_str
            elif val_str.lower() in ("no", "false", "0", "0.0"):
                verdict_label = "FAIL"
            elif val_str in ("genie_correct", "both_correct"):
                verdict_label = val_str
            elif val_str in ("ground_truth_correct", "neither_correct"):
                verdict_label = val_str
            else:
                verdict_label = val_str or "n/a"

            override_suffix = ""
            if (
                verdict_label == "FAIL"
                and judge in _ARBITER_ADJUSTABLE_DISPLAY_JUDGES
                and arbiter_val in ("genie_correct", "both_correct")
            ):
                override_suffix = "  (arbiter override → counts as PASS)"
            rat_suffix = f"  -- {short_rat}" if short_rat and verdict_label not in ("PASS", "n/a") else ""
            lines.append(f"|   {judge:<24s} {verdict_label}{override_suffix}{rat_suffix}")

        lines.append("-" * width)

    if use_legacy_headline:
        # Legacy mode keeps the pre-v2 phrasing so reviewers comparing old
        # runs see unchanged output.
        _summary_line = (
            f"  {total_questions} questions: {_all_judge_pass_count} all-pass, "
            f"{_fail_count + _arbiter_rescued_count} with failures (details below)"
        )
    else:
        # v2 header: three buckets that sum to total and reconcile with
        # the ``Overall accuracy: correct/evaluated`` line below.
        _summary_line = (
            f"  {total_questions} questions: "
            f"{_logical_pass_count} logical-pass · "
            f"{_arbiter_rescued_count} arbiter-override-pass · "
            f"{_fail_count} fail"
            f"   [all-judge-pass: {_all_judge_pass_count}]"
        )
    lines.insert(3, _summary_line)

    lines.append("")
    lines.append("--- SCORE SUMMARY " + "-" * max(0, width - 19))
    for judge in _JUDGE_ORDER:
        score = scores_100.get(judge)
        if score is None:
            continue
        threshold = DEFAULT_THRESHOLDS.get(judge, 0.0)
        # T0.4: when threshold is effectively 0 the judge is info-only —
        # a "PASS" tag misleads operators into reading it as a green check.
        # Render an explicit ``info-only`` marker instead so the scoreboard
        # can be eyeballed without reading every threshold value.
        _is_info_only = threshold <= 0.0
        passed = score >= threshold
        if _is_info_only:
            _status = "info-only"
            marker = ""
        else:
            _status = "PASS" if passed else "FAIL"
            marker = "" if passed else "  <<<"
        lines.append(
            f"|   {judge:<24s} {score:6.1f}  (threshold: {threshold:.1f})  "
            f"{_status}{marker}"
        )
    arbiter_counts: dict[str, int] = {
        "both_correct": 0, "genie_correct": 0,
        "ground_truth_correct": 0, "neither_correct": 0, "skipped": 0,
    }
    for row in rows:
        av = str(row.get("arbiter/value", row.get("arbiter", "skipped"))).lower()
        if av in arbiter_counts:
            arbiter_counts[av] += 1
        else:
            arbiter_counts["skipped"] += 1
    arbiter_total = sum(arbiter_counts.values())
    lines.append(f"|")
    # T0.4: Previously this block printed ``Arbiter verdicts (22 questions)``
    # while the accuracy block below reported ``Overall accuracy: 66.7%
    # (14/21)`` — the two denominators (22 vs 21) come from different views
    # of the same rows (all rows vs scored rows) and the mismatch reads as
    # an off-by-one bug. Annotate both denominators explicitly so the
    # reader can reconcile them at a glance.
    _adj_excluded_preview = _compute_arbiter_adjusted_accuracy(rows).excluded_count
    _scored = arbiter_total - _adj_excluded_preview
    if _adj_excluded_preview:
        lines.append(
            f"|   Arbiter verdicts ({arbiter_total} questions, "
            f"{_adj_excluded_preview} excluded → {_scored} scored):"
        )
    else:
        lines.append(f"|   Arbiter verdicts ({arbiter_total} questions):")
    for verdict in ("both_correct", "genie_correct", "ground_truth_correct", "neither_correct", "skipped"):
        cnt = arbiter_counts[verdict]
        pct = (cnt / arbiter_total * 100) if arbiter_total else 0
        lines.append(f"|     {verdict:<24s} {cnt:3d}  ({pct:5.1f}%)")

    _adj_result = _compute_arbiter_adjusted_accuracy(rows)
    adj_accuracy = _adj_result.accuracy_pct
    adj_failures = _adj_result.failure_ids
    adj_excluded = _adj_result.excluded_count
    rc_adjusted_pct = scores_100.get("result_correctness", 0.0)

    # Compute a TRULY pre-arbiter result_correctness (the value users expect
    # when they see "raw"): count the raw yes/no verdict without any arbiter
    # rescue. Mirror the exclusion logic used by the arbiter-adjusted branch
    # so both denominators are directly comparable.
    _rc_pre_total = 0
    _rc_pre_correct = 0
    for _r in rows:
        _val = str(_r.get("result_correctness/value", "")).lower()
        if _val == "excluded":
            continue
        _err_type = str(
            _r.get("outputs/comparison/error_type")
            or _r.get("comparison/error_type")
            or _r.get("comparison.error_type")
            or ""
        ).lower()
        if _err_type in ("both_empty", "genie_result_unavailable"):
            continue
        _rc_pre_total += 1
        if _val in ("yes", "true", "1", "1.0"):
            _rc_pre_correct += 1
    rc_pre_arbiter_pct = (
        round(100 * _rc_pre_correct / _rc_pre_total, 1) if _rc_pre_total else 0.0
    )

    lines.append(f"|")
    # T0.4: When there are excluded rows, print the total-corpus count
    # in parentheses so this line and the ``Arbiter verdicts (N questions,
    # X excluded → Y scored)`` header use visibly-the-same denominators.
    if adj_excluded:
        lines.append(
            f"|   Overall accuracy: {adj_accuracy:.1f}% "
            f"({_adj_result.correct_count}/{_adj_result.evaluated_count} scored, "
            f"{adj_excluded} excluded of {arbiter_total})"
        )
    else:
        lines.append(
            f"|   Overall accuracy: {adj_accuracy:.1f}% "
            f"({_adj_result.correct_count}/{_adj_result.evaluated_count})"
        )
    # Strict metric: fraction of rows where *every* judge passed, without
    # arbiter rescue. This is the number that the lever loop moves when
    # metadata patches land, and the header-only count hid it from readers.
    _all_judge_pct = (
        round(100 * _all_judge_pass_count / total_questions, 1)
        if total_questions else 0.0
    )
    lines.append(
        f"|   All-judge-pass (no arbiter rescue): {_all_judge_pct:.1f}% "
        f"({_all_judge_pass_count}/{total_questions})"
    )
    lines.append(
        f"|   result_correctness (pre-arbiter): {rc_pre_arbiter_pct:.1f}%  "
        f"(arbiter-adjusted: {rc_adjusted_pct:.1f}%)"
    )
    # Diagnostic: rows where non-info judges emitted a failure signal but
    # the arbiter oracle still marks Genie as correct. This is not a
    # "rescue" quality metric; it is a judge/oracle disagreement rate that
    # tells operators to inspect RCA evidence before over-weighting judge
    # failures.
    _disagreement_count = 0
    for _row in rows:
        _sig = _extract_row_signals(_row)
        if _sig["rc"] == "excluded" or _sig["err_type"] in (
            "both_empty",
            "genie_result_unavailable",
        ):
            continue
        _arbiter_val = str(
            _row.get("arbiter/value", _row.get("arbiter", ""))
        ).lower()
        if (
            has_individual_judge_failure(_row)
            and _arbiter_val in _ARBITER_CORRECT_VERDICTS
        ):
            _disagreement_count += 1
    if _adj_result.evaluated_count:
        _disagreement_rate = _disagreement_count / _adj_result.evaluated_count
        if _disagreement_rate > 0.30:
            lines.append(
                f"|   [DIAGNOSTIC] Judge-oracle disagreement rate "
                f"{_disagreement_rate*100:.1f}% > 30% "
                f"({_disagreement_count}/{_adj_result.evaluated_count}) — "
                f"arbiter marked Genie correct despite one or more judge "
                f"failures; inspect RCA evidence."
            )
    if adj_excluded:
        lines.append(f"|   Excluded (GT infra / both-empty / unavailable): {adj_excluded}")
    lines.append(f"|   Thresholds met: {'YES' if thresholds_passed else 'NO'}")
    if adj_failures:
        # T0.4: defense-in-depth against any duplicate qid that escapes the
        # dedup in _compute_arbiter_adjusted_accuracy (e.g. benchmarks with
        # identical inputs/question_id but different payloads). Annotate
        # the first occurrence of any repeated qid with ``(base)`` and
        # subsequent occurrences with ``:vN`` so operators can tell apart
        # "one question failed twice" from "two questions failed".
        _counts: dict[str, int] = {}
        for _q in adj_failures:
            _counts[_q] = _counts.get(_q, 0) + 1
        _has_dups = any(v > 1 for v in _counts.values())
        if _has_dups:
            _seen: dict[str, int] = {}
            _annotated: list[str] = []
            for _q in adj_failures:
                _n = _seen.get(_q, 0) + 1
                _seen[_q] = _n
                if _n == 1 and _counts[_q] > 1:
                    _annotated.append(f"{_q} (base)")
                elif _n > 1:
                    _annotated.append(f"{_q}:v{_n}")
                else:
                    _annotated.append(_q)
            lines.append(f"|   Failed questions: {_annotated}")
        else:
            lines.append(f"|   Failed questions: {adj_failures}")
    lines.append("-" * width)

    print("\n".join(lines))


def run_evaluation(
    space_id: str,
    experiment_name: str,
    iteration: int,
    benchmarks: list[dict],
    domain: str,
    model_id: str | None,
    eval_scope: str,
    predict_fn: Any,
    scorers: list[Any],
    *,
    spark: SparkSession | None = None,
    w: WorkspaceClient | None = None,
    catalog: str = "",
    gold_schema: str = "",
    uc_schema: str = "",
    warehouse_id: str = "",
    patched_objects: list[str] | None = None,
    reference_sqls: dict[str, str] | None = None,
    metric_view_names: set[str] | None = None,
    metric_view_measures: dict[str, set[str]] | None = None,
    optimization_run_id: str = "",
    lever: int | None = None,
    model_creation_kwargs: dict | None = None,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
    run_name: str | None = None,
    extra_tags: dict[str, str] | None = None,
) -> dict:
    """Run ``mlflow.genai.evaluate()`` and return structured results.

    Args:
        reference_sqls: Optional ``{question_id: sql}`` from a prior iteration.
            When provided the ``repeatability_scorer`` is automatically added
            and ``previous_sql`` is injected into each row's expectations.
        run_name: Tier 4 — optional explicit MLflow run name. When provided,
            the function uses it verbatim (typically built via
            ``common.mlflow_names``); when omitted, falls back to the legacy
            timestamp-based template for back-compat.
        extra_tags: Tier 4 — tags merged onto the run alongside the defaults.
            Callers pass v2 tags from ``common.mlflow_names.default_tags``.

    Returns dict with: run_id, run_name, experiment_id, iteration,
    overall_accuracy, per_judge, thresholds_passed, failure_question_ids,
    arbiter_verdicts, etc.
    """
    import re as _re
    domain = _re.sub(r"[^a-z0-9_]+", "_", domain.lower()).strip("_") or "default"

    mlflow.set_experiment(experiment_name)
    exp = mlflow.get_experiment_by_name(experiment_name)
    mlflow_model_id = (
        model_id
        if isinstance(model_id, str) and model_id.startswith("m-")
        else None
    )

    trace_destination = _configure_uc_trace_destination(
        experiment_id=exp.experiment_id if exp else "",
        uc_schema=uc_schema,
        warehouse_id=warehouse_id or os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
    )

    scope_filtered = filter_benchmarks_by_scope(benchmarks, eval_scope, patched_objects)
    if not scope_filtered and benchmarks:
        scope_filtered = benchmarks

    if not run_name:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        _tpl = BASELINE_RUN_NAME_TEMPLATE if iteration == 0 else RUN_NAME_TEMPLATE
        run_name = format_mlflow_template(_tpl, iteration=iteration, timestamp=ts)

    progress = EvalProgressLogger(
        logger=logger,
        run_id=optimization_run_id or "",
        eval_scope=eval_scope,
        iteration=iteration,
    )

    with _scorer_feedback_scope(), mlflow.start_run(run_name=run_name) as run:
        _version_tags: dict[str, str] = {
            "genie.space_id": space_id,
            "genie.domain": domain,
            "genie.iteration": str(iteration),
            "genie.eval_scope": eval_scope,
        }
        if optimization_run_id:
            _version_tags["genie.optimization_run_id"] = optimization_run_id
        if lever is not None:
            _version_tags["genie.lever"] = str(lever)
        else:
            _version_tags["genie.lever"] = "baseline"
        # Tier 4: merge caller-supplied v2 tags (``genie.run_id``,
        # ``genie.run_name_version``, ``genie.stage``, etc.) on top of the
        # local defaults. Caller tags win on key collisions so operators
        # can override iteration/lever for non-standard runs.
        if extra_tags:
            _version_tags.update({str(k): str(v) for k, v in extra_tags.items()})
        mlflow.set_tags(_version_tags)

        progress.emit(
            "eval_run_start",
            space_id=space_id,
            domain=domain,
            scorer_count=len(scorers),
            model_id=model_id or "",
        )

        if model_creation_kwargs:
            from genie_space_optimizer.optimization.models import create_genie_model_version
            with progress.phase("model_creation", space_id=space_id):
                _created_model_id = create_genie_model_version(**model_creation_kwargs)
            if _created_model_id:
                mlflow_model_id = _created_model_id
                model_id = _created_model_id

        # NOTE: We intentionally use the in-memory deduped eval_data DataFrame
        # for evaluation instead of the MLflow EvaluationDataset object.  The
        # underlying Delta table may contain stale duplicate rows (e.g. 54 rows
        # when only 30 unique benchmarks exist) because merge_records upserts
        # by record-id and never deletes old rows.  Using eval_data guarantees
        # the evaluation runs exactly the deduped benchmark set.

        _wh_id = warehouse_id or os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "")
        if spark is not None:
            known_functions = _load_known_functions(spark, catalog, gold_schema)
            with progress.phase("benchmark_precheck", scoped_count=len(scope_filtered)):
                filtered, quarantined_benchmarks, precheck_counts = _precheck_benchmarks_for_eval(
                    benchmarks=scope_filtered,
                    spark=spark,
                    catalog=catalog,
                    gold_schema=gold_schema,
                    known_functions=known_functions,
                    metric_view_names=metric_view_names,
                    metric_view_measures=metric_view_measures,
                    w=w,
                    warehouse_id=_wh_id,
                )
        else:
            filtered = list(scope_filtered)
            quarantined_benchmarks = []
            precheck_counts = {
                "invalid_benchmark_count": 0,
                "permission_blocked_count": 0,
                "unresolved_column_count": 0,
                "bad_join_key_count": 0,
            }

        has_reference_sqls = bool(reference_sqls)
        if has_reference_sqls:
            from genie_space_optimizer.optimization.scorers import repeatability_scorer as _rep_scorer
            if _rep_scorer not in scorers:
                scorers = list(scorers) + [_rep_scorer]

        eval_records = []
        for b in filtered:
            qid = b.get("id", "")
            _esql = b.get("expected_sql", "")
            expectations = {
                "expected_response": _esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    _esql,
                    hint=b.get("expected_asset_hint"),
                ),
            }
            if has_reference_sqls:
                expectations["previous_sql"] = (reference_sqls or {}).get(qid, "")
            eval_records.append(
                {
                    "inputs": {
                        "question_id": qid,
                        "question": b["question"],
                        "space_id": space_id,
                        "expected_sql": b.get("expected_sql", ""),
                        "catalog": catalog,
                        "gold_schema": gold_schema,
                        "order_sensitive": bool(b.get("order_sensitive", False)),
                    },
                    "expectations": expectations,
                }
            )
        if len(eval_records) > max_benchmark_count:
            eval_records = _truncate_benchmarks(
                [{**r, "provenance": r.get("expectations", {}).get("provenance", "other")} for r in eval_records],
                max_benchmark_count,
            )
            for r in eval_records:
                r.pop("provenance", None)
        original_eval_record_count = len(eval_records)
        eval_records = slice_eval_records_for_debug(eval_records)
        if len(eval_records) != original_eval_record_count:
            progress.emit(
                "debug_row_cap_applied",
                original_count=original_eval_record_count,
                capped_count=len(eval_records),
            )
        eval_data = pd.DataFrame(eval_records)

        run_params = {
            "space_id": space_id,
            "iteration": iteration,
            "dataset": f"{domain}_benchmarks",
            "eval_scope": eval_scope,
            "num_scorers": len(scorers),
            "domain": domain,
            "benchmark_count": len(filtered),
            "scope_benchmark_count": len(scope_filtered),
            "invalid_benchmark_count": precheck_counts["invalid_benchmark_count"],
            "permission_blocked_count": precheck_counts["permission_blocked_count"],
            "unresolved_column_count": precheck_counts["unresolved_column_count"],
            "bad_join_key_count": precheck_counts["bad_join_key_count"],
        }
        if model_id:
            run_params["model_id"] = model_id
        if catalog:
            run_params["catalog"] = catalog
        if gold_schema:
            run_params["gold_schema"] = gold_schema
        if uc_schema:
            run_params["uc_schema"] = uc_schema
        if trace_destination:
            run_params["trace_destination"] = trace_destination
        mlflow.log_params(run_params)
        if quarantined_benchmarks:
            mlflow.log_dict(
                {
                    "total_scoped_benchmarks": len(scope_filtered),
                    "evaluable_benchmark_count": len(filtered),
                    "counts": precheck_counts,
                    "quarantined": quarantined_benchmarks,
                },
                "evaluation_runtime/benchmark_precheck.json",
            )
        if not filtered:
            msg = (
                "No evaluable benchmarks remain after strict pre-eval SQL + routine checks. "
                f"Counts: {precheck_counts}"
            )
            mlflow.log_dict(
                {
                    "status": "failed",
                    "error_type": "NoEvaluableBenchmarks",
                    "error_message": msg,
                    "quarantined": quarantined_benchmarks[:50],
                    "counts": precheck_counts,
                },
                "evaluation_failure/no_evaluable_benchmarks.json",
            )
            raise RuntimeError(msg)
        # Ensure every evaluation run carries a full, queryable judge manifest.
        register_judge_prompts(
            uc_schema=uc_schema,
            domain=domain,
            experiment_name=experiment_name,
            register_registry=(iteration == 0 and eval_scope == "full"),
        )

        if iteration == 0 and eval_scope == "full":
            register_scorers_with_experiment(scorers, experiment_name)

        if mlflow_model_id:
            mlflow.set_active_model(model_id=mlflow_model_id)

        evaluate_kwargs: dict[str, Any] = {
            "predict_fn": predict_fn,
            "data": eval_data,
            "scorers": scorers,
        }
        if mlflow_model_id:
            evaluate_kwargs["model_id"] = mlflow_model_id

        _predict_fn_start_ms = int(time.time() * 1000)
        eval_attempts: list[dict[str, Any]] = []
        try:
            progress.emit(
                "mlflow_evaluate_start",
                row_count=len(eval_data),
                scorer_count=len(scorers),
                force_sequential=eval_force_sequential(),
            )
            if eval_force_sequential():
                eval_result = _run_evaluate_sequential_fallback(
                    evaluate_kwargs=evaluate_kwargs,
                )
                eval_attempts.append(
                    {
                        "attempt": 1,
                        "workers": "1",
                        "status": "success",
                        "mode": "forced_sequential",
                    }
                )
                mlflow.set_tag("evaluation_mode", "forced_sequential")
            else:
                eval_result, eval_attempts = _run_evaluate_with_retries(
                    evaluate_kwargs=evaluate_kwargs,
                )
            progress.emit("mlflow_evaluate_done", row_count=len(eval_data))
        except Exception as exc:
            attempts_from_exc = getattr(exc, "_eval_attempts", None)
            if isinstance(attempts_from_exc, list):
                eval_attempts = attempts_from_exc
            is_retryable = _is_retryable_eval_exception(exc)
            if is_retryable:
                logger.warning(
                    "Falling back to sequential evaluation after retryable harness failure: %s",
                    str(exc)[:400],
                )
                eval_result = _run_evaluate_sequential_fallback(
                    evaluate_kwargs=evaluate_kwargs,
                )
                eval_attempts.append(
                    {
                        "attempt": len(eval_attempts) + 1,
                        "workers": "1",
                        "status": "success",
                        "mode": "sequential_fallback",
                    }
                )
                mlflow.set_tag("evaluation_mode", "sequential_fallback")
            else:
                failure_payload = {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:2000],
                    "attempts": eval_attempts,
                }
                try:
                    mlflow.log_dict(
                        failure_payload,
                        "evaluation_failure/evaluate_failure.json",
                    )
                    mlflow.set_tags(
                        {
                            "evaluation_status": "failed",
                            "evaluation_error_type": type(exc).__name__,
                        },
                    )
                except Exception:
                    logger.warning("Could not log evaluation failure artifact", exc_info=True)
                raise

        if eval_attempts:
            mlflow.log_dict(
                {"attempts": eval_attempts},
                "evaluation_runtime/evaluate_attempts.json",
            )
            mlflow.log_param(
                "evaluate_attempt_count",
                str(len(eval_attempts)),
            )
        harness_retry_count = max(0, len(eval_attempts) - 1)
        mlflow.log_metric("harness_retry_count", float(harness_retry_count))

        per_judge: dict[str, float] = {}
        for metric_name in eval_result.metrics:
            if "/mean" in metric_name:
                judge_name = metric_name.replace("/mean", "")
                per_judge[judge_name] = eval_result.metrics[metric_name]

        scores_100 = normalize_scores(per_judge)
        thresholds_passed = all_thresholds_met(scores_100)
        mlflow.log_metric("thresholds_passed", 1.0 if thresholds_passed else 0.0)

        arbiter_verdicts: dict[str, int] = {
            "genie_correct": 0,
            "ground_truth_correct": 0,
            "both_correct": 0,
            "neither_correct": 0,
            "skipped": 0,
        }
        arbiter_actions: list[dict[str, str]] = []
        rows_for_output: list[dict] = []

        _STRIP_COLS = {"trace", "assessments", "spans", "trace_metadata"}
        cached_feedback = _drain_scorer_feedback_cache()

        # I1 — ASI source instrumentation. The forensic review showed
        # ``asi_source histogram: none=29 (100%)`` even though the
        # scorers are emitting ``Feedback(metadata=...)``. The likeliest
        # culprits are (a) the cache never populated (scorers ran
        # outside the active scope), (b) row-level qid extraction
        # failed silently, or (c) cache key mismatch (e.g. ``:vN``
        # suffix on row qids vs base qids in cache). Counters below
        # let us tell those apart in a single eval log line.
        _i1_cache_qid_count = len(cached_feedback)
        _i1_cache_judge_count = sum(
            len(j_map) for j_map in cached_feedback.values()
        )
        _i1_row_qid_extracted = 0
        _i1_row_qid_missing = 0
        _i1_cache_hit_rows = 0
        _i1_dump_row_keys = (
            os.getenv("GENIE_SPACE_OPTIMIZER_ASI_INSTRUMENTATION", "false")
            .lower() in {"1", "true", "yes", "on"}
        )

        if hasattr(eval_result, "tables") and "eval_results" in eval_result.tables:
            results_df = eval_result.tables["eval_results"]

            assessment_map = _extract_assessments_from_traces(results_df)

            # Phase 2.2: pre-compute trace recovery before the row loop
            # so we can backfill assessments for rows whose ``trace``
            # column was lost during ``mlflow.genai.evaluate``. Skipped
            # when the env-flag is off, when no rows are silent on
            # assessments, or when the experiment is missing.
            _enable_recovery = os.getenv(
                "GSO_ASI_RECOVERY_FETCH_ASSESSMENTS", "1",
            ).strip().lower() not in ("0", "false", "no", "off")
            recovered_assessments_by_qid: dict[str, dict[str, dict]] = {}
            if _enable_recovery and exp:
                # Determine whether recovery is even worth attempting:
                # only fetch traces when ``assessment_map`` has fewer
                # populated rows than ``results_df`` (i.e. trace context
                # was at least partially lost).
                _populated_rows = sum(
                    1 for v in assessment_map.values() if v
                )
                _total_rows = len(results_df)
                if _populated_rows < _total_rows:
                    _early_trace_map: dict[str, str] = {}
                    for _r_idx, (_, _r_row) in enumerate(results_df.iterrows()):
                        _r_qid = (
                            _r_row.get("inputs/question_id")
                            if hasattr(_r_row, "get") else None
                        )
                        _r_tid = (
                            _r_row.get("trace_id")
                            if hasattr(_r_row, "get") else None
                        )
                        if _r_qid and _r_tid:
                            _early_trace_map[str(_r_qid)] = str(_r_tid)
                    if len(_early_trace_map) < _total_rows:
                        try:
                            _early_trace_map.update(_recover_trace_map(
                                experiment_id=exp.experiment_id,
                                optimization_run_id=optimization_run_id,
                                iteration=iteration,
                                expected_count=_total_rows,
                                start_time_ms=_predict_fn_start_ms,
                                eval_result=eval_result,
                            ))
                        except Exception:
                            logger.debug(
                                "Early _recover_trace_map call failed",
                                exc_info=True,
                            )
                    if _early_trace_map:
                        recovered_assessments_by_qid = (
                            _fetch_assessments_for_recovered_qids(_early_trace_map)
                        )
                        if recovered_assessments_by_qid:
                            logger.info(
                                "Phase 2.2: fetched assessments for %d/%d "
                                "qids via recovered traces",
                                len(recovered_assessments_by_qid),
                                _total_rows,
                            )

            for row_idx, (_, row) in enumerate(results_df.iterrows()):
                row_dict = {}
                for col in results_df.columns:
                    if col in _STRIP_COLS:
                        continue
                    val = row[col]
                    if hasattr(val, "item"):
                        val = val.item()
                    if not isinstance(val, (str, int, float, bool, type(None), list, dict)):
                        val = str(val)
                    row_dict[col] = val

                _req_raw = row_dict.get("request") or {}
                if isinstance(_req_raw, str):
                    try:
                        _req_raw = json.loads(_req_raw)
                    except (json.JSONDecodeError, TypeError):
                        _req_raw = {}
                _req_kw = _req_raw.get("kwargs", {}) if isinstance(_req_raw, dict) else {}
                qid = (
                    row_dict.get("inputs/question_id")
                    or (row_dict.get("inputs") or {}).get("question_id", "")
                    or row_dict.get("question_id")
                    or _req_kw.get("question_id")
                    or (_req_raw.get("question_id") if isinstance(_req_raw, dict) else None)
                    or ""
                )
                # I1 — track qid extraction outcome. Distinguishing
                # "row had no qid" from "qid present but no cache
                # entry" tells us whether the bug is in benchmark
                # threading or in cache key alignment.
                if qid:
                    _i1_row_qid_extracted += 1
                    if qid in cached_feedback:
                        _i1_cache_hit_rows += 1
                    elif _i1_dump_row_keys:
                        logger.warning(
                            "[I1 ASI] cache miss row=%s qid=%r cache_keys_sample=%r",
                            row_idx, qid, list(cached_feedback)[:5],
                        )
                else:
                    _i1_row_qid_missing += 1
                    if _i1_dump_row_keys:
                        logger.warning(
                            "[I1 ASI] qid missing for row=%s row_keys=%r",
                            row_idx,
                            sorted(
                                k for k in row_dict
                                if "question" in k.lower() or "input" in k.lower()
                            )[:10],
                        )
                _merge_row_sources(
                    row_dict,
                    assessment_map.get(row_idx),
                    cached_feedback.get(qid) if qid else None,
                    recovered_assessments_by_qid.get(qid) if qid else None,
                )

                for col_name in list(row_dict.keys()):
                    if col_name.endswith("/rationale"):
                        jname = col_name.rsplit("/rationale", 1)[0]
                        if jname.startswith("feedback/"):
                            jname = jname[len("feedback/"):]
                        mkey = f"{jname}/metadata"
                        if mkey not in row_dict:
                            parsed = _parse_asi_from_rationale(str(row_dict.get(col_name, "")))
                            if parsed:
                                row_dict[mkey] = parsed

                _ASI_FLAT_FIELDS = ("failure_type", "blame_set", "wrong_clause", "counterfactual_fix", "severity", "confidence")
                for col_name in list(row_dict.keys()):
                    if not col_name.endswith("/metadata"):
                        continue
                    jname = col_name.removesuffix("/metadata")
                    meta = row_dict[col_name]
                    if not isinstance(meta, dict):
                        continue
                    for fld in _ASI_FLAT_FIELDS:
                        flat_key = f"metadata/{jname}/{fld}"
                        if flat_key not in row_dict and meta.get(fld) is not None:
                            row_dict[flat_key] = meta[fld]

                rows_for_output.append(row_dict)

                av = str(row.get("arbiter/value", row.get("arbiter", "skipped")))
                if av in arbiter_verdicts:
                    arbiter_verdicts[av] += 1
                else:
                    arbiter_verdicts["skipped"] += 1

                if av == "genie_correct":
                    _gc_sql = (
                        row.get("outputs/response")
                        or (row.get("outputs") or {}).get("response", "")
                    )
                    _gc_question = (
                        row.get("inputs/question")
                        or (row.get("inputs") or {}).get("question", "")
                    )
                    if _gc_sql and _gc_question:
                        arbiter_actions.append({
                            "question": str(_gc_question),
                            "new_expected_sql": str(_gc_sql),
                            "verdict": "genie_correct",
                        })

        # I1 — emit a single summary log so operators can read it
        # alongside the existing ``ASI source histogram`` line and tell
        # at a glance which of the three failure modes occurred:
        #   * cache=0 / hits=0  → scorers never wrote (scope binding bug)
        #   * cache=N / hits=0  → key mismatch (qid suffix / extraction)
        #   * cache=N / hits=M  → cache works; ASI extraction is the bug
        try:
            logger.info(
                "[I1 ASI] cache populates: qids=%d judges=%d | row qid extract: "
                "ok=%d missing=%d | cache hits: %d/%d rows | dump_row_keys=%s",
                _i1_cache_qid_count,
                _i1_cache_judge_count,
                _i1_row_qid_extracted,
                _i1_row_qid_missing,
                _i1_cache_hit_rows,
                len(rows_for_output),
                _i1_dump_row_keys,
            )
        except Exception:
            logger.debug("[I1 ASI] summary log raised", exc_info=True)

        question_failure_artifacts: list[dict[str, Any]] = []
        for row in rows_for_output:
            error_val = (
                row.get("outputs/comparison/error")
                or row.get("comparison/error")
                or row.get("comparison.error")
            )
            if not error_val:
                continue
            _fa_req = row.get("request") or {}
            if isinstance(_fa_req, str):
                try:
                    _fa_req = json.loads(_fa_req)
                except (json.JSONDecodeError, TypeError):
                    _fa_req = {}
            _fa_kw = _fa_req.get("kwargs", {}) if isinstance(_fa_req, dict) else {}
            question_failure_artifacts.append(
                {
                    "question_id": str(
                        row.get("inputs/question_id")
                        or row.get("question_id")
                        or _fa_kw.get("question_id")
                        or (_fa_req.get("question_id") if isinstance(_fa_req, dict) else None)
                        or ""
                    ),
                    "expected_sql": str(
                        row.get("inputs/expected_sql")
                        or _fa_kw.get("expected_sql")
                        or (_fa_req.get("expected_sql") if isinstance(_fa_req, dict) else None)
                        or ""
                    ),
                    "generated_sql": str(row.get("outputs/response") or row.get("response") or ""),
                    "error_type": str(
                        row.get("outputs/comparison/error_type")
                        or row.get("comparison/error_type")
                        or row.get("comparison.error_type")
                        or ""
                    ),
                    "sqlstate": str(
                        row.get("outputs/comparison/sqlstate")
                        or row.get("comparison/sqlstate")
                        or row.get("comparison.sqlstate")
                        or ""
                    ),
                    "error": str(error_val)[:1000],
                }
            )
        if question_failure_artifacts:
            mlflow.log_dict(
                {
                    "count": len(question_failure_artifacts),
                    "items": question_failure_artifacts,
                },
                "evaluation_runtime/question_failure_artifacts.json",
            )

        infra_errors = _collect_infra_eval_errors(rows_for_output)
        if FAIL_ON_INFRA_EVAL_ERRORS and infra_errors:
            mlflow.log_dict(
                {
                    "status": "failed",
                    "reason": "infrastructure_sql_error",
                    "errors": infra_errors,
                },
                "evaluation_failure/infrastructure_sql_errors.json",
            )
            mlflow.set_tags(
                {
                    "evaluation_status": "failed",
                    "evaluation_error_type": "infrastructure_sql_error",
                },
            )
            raise RuntimeError(
                "Infrastructure SQL errors detected during evaluation: "
                + " | ".join(infra_errors[:3]),
            )

        _temporal_stale_qids: set[str] = set()
        for _ts_row in rows_for_output:
            if ((_ts_row.get("expectations") or {}).get("temporal_stale")
                    or (_ts_row.get("inputs", {}) or {}).get("temporal_stale")):
                _ts_req = _ts_row.get("request") or {}
                if isinstance(_ts_req, str):
                    try:
                        _ts_req = json.loads(_ts_req)
                    except (json.JSONDecodeError, TypeError):
                        _ts_req = {}
                _ts_kw = _ts_req.get("kwargs", {}) if isinstance(_ts_req, dict) else {}
                _ts_qid = str(
                    _ts_row.get("inputs/question_id")
                    or (_ts_row.get("inputs", {}) or {}).get("question_id", "")
                    or _ts_kw.get("question_id", "")
                    or _ts_row.get("question_id", "")
                )
                if _ts_qid:
                    _temporal_stale_qids.add(_ts_qid)

        _arbiter_result = _compute_arbiter_adjusted_accuracy(
            rows_for_output,
            temporal_stale_qids=_temporal_stale_qids if _temporal_stale_qids else None,
        )
        arbiter_adjusted_accuracy = _arbiter_result.accuracy_pct
        arbiter_adjusted_correct = _arbiter_result.correct_count
        failure_ids = _arbiter_result.failure_ids
        excluded_count = _arbiter_result.excluded_count
        evaluated_count = _arbiter_result.evaluated_count
        both_correct_count = _arbiter_result.both_correct_count
        both_correct_rate = _arbiter_result.both_correct_rate

        # Index exclusions by qid for O(1) lookup when annotating rows_for_output.
        _exclusions_by_qid: dict[str, RowExclusion] = {
            ex.question_id: ex for ex in _arbiter_result.exclusions if ex.question_id
        }

        _arbiter_overridden_qids: list[str] = []
        _soft_signal_qids: list[str] = []
        for _ao_row in rows_for_output:
            _ao_rc = str(
                _ao_row.get("result_correctness/value", _ao_row.get("result_correctness", ""))
            ).lower()
            _ao_av = str(
                _ao_row.get("arbiter/value", _ao_row.get("arbiter", "skipped"))
            ).lower()
            _ao_rq = _ao_row.get("request") or {}
            if isinstance(_ao_rq, str):
                try:
                    _ao_rq = json.loads(_ao_rq)
                except (json.JSONDecodeError, TypeError):
                    _ao_rq = {}
            _ao_kw = _ao_rq.get("kwargs", {}) if isinstance(_ao_rq, dict) else {}
            _ao_qid = str(
                _ao_row.get("inputs/question_id")
                or (_ao_row.get("inputs") or {}).get("question_id", "")
                or _ao_row.get("question_id")
                or _ao_kw.get("question_id")
                or (_ao_rq.get("question_id") if isinstance(_ao_rq, dict) else None)
                or ""
            )
            if not _ao_qid:
                continue
            if _ao_rc in ("no", "false", "0", "0.0") and _ao_av in _ARBITER_CORRECT_VERDICTS:
                _arbiter_overridden_qids.append(_ao_qid)
                _has_judge_fail = False
                for _ao_col, _ao_val in _ao_row.items():
                    if (_ao_col.startswith("feedback/") and _ao_col.endswith("/value")
                            and "no" in str(_ao_val).lower()):
                        _has_judge_fail = True
                        break
                if _has_judge_fail:
                    _soft_signal_qids.append(_ao_qid)

        # Arbiter-adjust result_correctness so detect_regressions sees true
        # signal instead of raw hash-mismatch noise.
        #
        # Tier 1.8: also stamp ``result_correctness/arbiter_override_value``
        # on the row so downstream tooling (UI drill-down, clustering, ASI
        # classifiers) can see the semantic verdict, not just the hash
        # result. Without this, rows with arbiter=both_correct but hash
        # mismatch (column/row ordering differences, alias renames) appear
        # as phantom per-judge regressions. The original
        # ``result_correctness/value`` is preserved for audit, but
        # ``_is_semantic_correct`` should be used by gate logic.
        if rows_for_output:
            _rc_total = _rc_correct = 0
            for _rc_row in rows_for_output:
                _rc_val = str(_rc_row.get("result_correctness/value", "")).lower()
                if _rc_val == "excluded":
                    continue
                _rc_err_type = str(
                    _rc_row.get("outputs/comparison/error_type")
                    or _rc_row.get("comparison/error_type")
                    or _rc_row.get("comparison.error_type")
                    or ""
                ).lower()
                if _rc_err_type in ("both_empty", "genie_result_unavailable"):
                    continue
                _rc_total += 1
                _rc_av = str(_rc_row.get("arbiter/value", "")).lower()
                _is_correct = _rc_val in ("yes", "true", "1", "1.0")
                if _is_correct:
                    _rc_correct += 1
                elif _rc_av in _ARBITER_CORRECT_VERDICTS:
                    _rc_correct += 1
                    _rc_row["result_correctness/arbiter_override_value"] = "yes"
                    _rc_row["_is_semantic_correct"] = True
                else:
                    _rc_row.setdefault("_is_semantic_correct", _is_correct)
            if _rc_total > 0:
                per_judge["result_correctness"] = _rc_correct / _rc_total

            _ARBITER_ADJUSTABLE_JUDGES = [
                "logical_accuracy", "semantic_equivalence",
                "completeness", "schema_accuracy",
            ]
            # T0.3: Before applying arbiter rescue, capture the *raw* pre-
            # arbiter rate for each rescuable judge (and for
            # result_correctness above). Threaded into ``scores_100`` as
            # ``_pre_arbiter/<judge>`` so the gate can optimise against
            # the underlying SQL signal instead of the arbiter-adjusted
            # verdicts that bounce on noise.
            _pre_arbiter_per_judge: dict[str, float] = {}
            _rc_pre_total = _rc_pre_correct = 0
            for _row in rows_for_output:
                _rc_val = str(_row.get("result_correctness/value", "")).lower()
                if _rc_val == "excluded":
                    continue
                _err_type = str(
                    _row.get("outputs/comparison/error_type")
                    or _row.get("comparison/error_type")
                    or _row.get("comparison.error_type")
                    or ""
                ).lower()
                if _err_type in ("both_empty", "genie_result_unavailable"):
                    continue
                _rc_pre_total += 1
                if _rc_val in ("yes", "true", "1", "1.0"):
                    _rc_pre_correct += 1
            if _rc_pre_total > 0:
                _pre_arbiter_per_judge["result_correctness"] = (
                    _rc_pre_correct / _rc_pre_total
                )

            for _judge_name in _ARBITER_ADJUSTABLE_JUDGES:
                _j_total = _j_correct = 0
                _pre_total = _pre_correct = 0
                for _row in rows_for_output:
                    _j_val = str(_row.get(f"{_judge_name}/value", "")).lower()
                    if _j_val == "excluded":
                        continue
                    _j_total += 1
                    _pre_total += 1
                    _passed = _j_val in ("yes", "true", "1", "1.0", "pass")
                    if _passed:
                        _j_correct += 1
                        _pre_correct += 1
                    elif str(_row.get("arbiter/value", "")).lower() in _ARBITER_CORRECT_VERDICTS:
                        _j_correct += 1
                if _j_total > 0:
                    per_judge[_judge_name] = _j_correct / _j_total
                if _pre_total > 0:
                    _pre_arbiter_per_judge[_judge_name] = (
                        _pre_correct / _pre_total
                    )

            scores_100 = normalize_scores(per_judge)
            # T0.3: stamp pre-arbiter counterparts as ``_pre_arbiter/<judge>``
            # so downstream readers can distinguish them from the
            # arbiter-adjusted top-line numbers (which keep their plain
            # judge-name keys for backward compatibility).
            for _jn, _frac in _pre_arbiter_per_judge.items():
                scores_100[f"_pre_arbiter/{_jn}"] = round(_frac * 100, 1)
            # B0.1 — stamp an overall-accuracy pre-arbiter key so every
            # eval result (including baseline) carries it. Downstream
            # gate code reads ``_pre_arbiter/overall_accuracy`` from
            # ``best_scores``; without this key the gate silently falls
            # back to post-arbiter accuracy and treats a pre-arbiter
            # improvement as a regression. Result_correctness is the
            # canonical primary signal under the ``pre_arbiter``
            # objective; if it isn't available (e.g. pre-T0.3 evaluator
            # versions), fall back to the arbiter-adjusted value so
            # legacy callers still get a sane number.
            scores_100["_pre_arbiter/overall_accuracy"] = scores_100.get(
                "_pre_arbiter/result_correctness",
                arbiter_adjusted_accuracy,
            )
            thresholds_passed = all_thresholds_met(scores_100)

        row_unresolved_column_count = sum(
            1
            for artifact in question_failure_artifacts
            if _classify_sql_validation_error(artifact.get("error", "")) == "unknown_column"
        )
        row_permission_blocked_count = sum(
            1
            for artifact in question_failure_artifacts
            if (
                artifact.get("error_type") == "permission_blocked"
                or _classify_sql_validation_error(artifact.get("error", "")) == "permission_blocked"
            )
        )
        unresolved_column_count = (
            precheck_counts["unresolved_column_count"] + row_unresolved_column_count
        )
        permission_blocked_count = (
            precheck_counts["permission_blocked_count"] + row_permission_blocked_count
        )
        mlflow.log_metrics({
            "overall_accuracy": arbiter_adjusted_accuracy,
            "correct_count": float(arbiter_adjusted_correct),
            "total_questions": float(len(filtered)),
            "evaluated_count": float(evaluated_count),
            "failure_count": float(len(failure_ids)),
            "excluded_count": float(excluded_count),
        })
        mlflow.set_tags(
            {
                "evaluation_status": "success",
                "invalid_benchmark_count": str(precheck_counts["invalid_benchmark_count"]),
                "permission_blocked_count": str(permission_blocked_count),
                "unresolved_column_count": str(unresolved_column_count),
                "harness_retry_count": str(harness_retry_count),
            }
        )

        trace_map: dict[str, str] = {}
        _rows_without_tid = 0
        for _row in rows_for_output:
            _qid = (
                _row.get("question_id")
                or _row.get("inputs/question_id")
                or (_row.get("inputs") or {}).get("question_id", "")
            )
            _tid = _row.get("trace_id")
            if _qid and _tid:
                trace_map[_qid] = str(_tid)
            elif _qid:
                _rows_without_tid += 1

        if not trace_map:
            logger.warning(
                "Evaluation %s produced 0 trace IDs from %d rows "
                "(trace context may have been lost during Genie API calls)",
                run_name, len(rows_for_output),
            )
            if exp:
                trace_map = _recover_trace_map(
                    experiment_id=exp.experiment_id,
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    expected_count=len(rows_for_output),
                    start_time_ms=_predict_fn_start_ms,
                    eval_result=eval_result,
                )
                if trace_map:
                    print(
                        f"[Eval] Recovered {len(trace_map)}/{len(rows_for_output)} "
                        f"trace IDs via fallback strategies"
                    )
        elif _rows_without_tid:
            logger.info(
                "Evaluation %s: %d/%d rows have trace IDs (%d missing)",
                run_name, len(trace_map), len(rows_for_output), _rows_without_tid,
            )

        if model_id and scores_100:
            from genie_space_optimizer.optimization.models import link_eval_scores_to_model
            try:
                link_eval_scores_to_model(model_id, scores_100, eval_run_id=run.info.run_id)
            except Exception:
                logger.warning("Failed to link scores to model %s", model_id, exc_info=True)

        if run.info.run_id:
            try:
                from mlflow.tracking import MlflowClient as _EvalMlflowClient
                _eval_client = _EvalMlflowClient()
                _eval_client.log_metric(run.info.run_id, "overall_accuracy", arbiter_adjusted_accuracy)
            except Exception:
                logger.debug("Failed to log overall_accuracy metric", exc_info=True)

        # Annotate each row with its exclusion reason (if any) so downstream
        # persistence (state.write_iteration → rows_json) and the UI drill-down
        # can explain "why did this question disappear?" without a second pass
        # over the extraction logic. See Bug #3.
        for _row in rows_for_output:
            _row_qid = (
                _row.get("question_id")
                or _row.get("inputs/question_id")
                or (_row.get("inputs") or {}).get("question_id", "")
            )
            if _row_qid and str(_row_qid) in _exclusions_by_qid:
                _ex = _exclusions_by_qid[str(_row_qid)]
                _row["exclusion"] = {
                    "reason_code": _ex.reason_code,
                    "reason_detail": _ex.reason_detail,
                }

        # Serialize quarantined_benchmarks for persistence. The in-memory shape
        # may include heavy fields we don't want in Delta; keep a compact view.
        _quarantined_for_persist = []
        for _qb in (quarantined_benchmarks or []):
            if isinstance(_qb, dict):
                _quarantined_for_persist.append({
                    "question_id": _qb.get("question_id") or _qb.get("id") or "",
                    "reason_code": _qb.get("reason_code") or _qb.get("reason") or "quarantined",
                    "reason_detail": _qb.get("reason_detail") or _qb.get("error") or "",
                    "question": _qb.get("question") or _qb.get("question_text") or "",
                })

        # Task 0 Step 4: ASI extraction telemetry. Aggregate the per-row
        # ``_asi_source`` stamps that ``_merge_judge_assessments_into_row``
        # set into a typed summary plus a Task-3-shaped audit row. This
        # makes a "no traces, all-row-payload" eval pass visible in Delta
        # rather than silent (the retail run state we are correcting).
        _asi_summary = compute_asi_source_summary(rows_for_output)
        _asi_audit = build_asi_extraction_audit_row(
            run_id=str(optimization_run_id or run.info.run_id or ""),
            iteration=int(iteration or 0),
            summary=_asi_summary,
            trace_id_count=len(trace_map),
            expected_trace_count=len(rows_for_output),
        )

        output: dict[str, Any] = {
            "run_id": run.info.run_id,
            "mlflow_run_id": run.info.run_id,
            "run_name": run_name,
            "experiment_id": exp.experiment_id if exp else "",
            "iteration": iteration,
            "overall_accuracy": arbiter_adjusted_accuracy,
            # T0.3: pre_arbiter_accuracy is the RAW result_correctness rate
            # (no arbiter rescue). This is the signal the gate should
            # optimise against when ``OPTIMIZATION_OBJECTIVE='pre_arbiter'``
            # because the arbiter adjustment masks failures that the
            # underlying SQL hasn't actually fixed. Per-judge counterparts
            # are stamped on ``scores`` under ``_pre_arbiter/<judge>``.
            "pre_arbiter_accuracy": scores_100.get(
                "_pre_arbiter/result_correctness", arbiter_adjusted_accuracy,
            ),
            # NOTE on denominator contract (Bug #2):
            #   - total_questions:   pre-exclusion — retained for back-compat.
            #   - evaluated_count:   denominator of overall_accuracy (use this).
            #   - excluded_count:    rows removed from the denominator at runtime.
            # Downstream readers should prefer evaluated_count; the API layer
            # (_resolve_eval_counts) falls back to total - excluded for old rows.
            "total_questions": len(filtered),
            "evaluated_count": evaluated_count,
            "correct_count": arbiter_adjusted_correct,
            "both_correct_count": both_correct_count,
            "both_correct_rate": both_correct_rate,
            "scores": scores_100,
            "thresholds_met": thresholds_passed,
            "thresholds_passed": thresholds_passed,
            "per_judge": per_judge,
            "failures": failure_ids,
            "failure_question_ids": failure_ids,
            "remaining_failures": failure_ids,
            "arbiter_verdicts": arbiter_verdicts,
            "arbiter_actions": arbiter_actions,
            "model_id": model_id,
            "rows": rows_for_output,
            "trace_map": trace_map,
            "invalid_benchmark_count": precheck_counts["invalid_benchmark_count"],
            "permission_blocked_count": permission_blocked_count,
            "unresolved_column_count": unresolved_column_count,
            "harness_retry_count": harness_retry_count,
            "excluded_count": excluded_count,
            "quarantined_benchmarks": _quarantined_for_persist,
            "row_exclusions": [
                {
                    "question_id": ex.question_id,
                    "question_text": ex.question_text,
                    "reason_code": ex.reason_code,
                    "reason_detail": ex.reason_detail,
                }
                for ex in _arbiter_result.exclusions
            ],
            "arbiter_overridden_qids": _arbiter_overridden_qids,
            "soft_signal_qids": _soft_signal_qids,
            "asi_source_summary": {
                "trace": _asi_summary.trace,
                "row_payload": _asi_summary.row_payload,
                "uc_metadata": _asi_summary.uc_metadata,
                "none": _asi_summary.none,
                "total": _asi_summary.total,
                "coverage_ratio": round(_asi_summary.coverage_ratio, 4),
            },
            "asi_extraction_audit": _asi_audit,
            "trace_id_count": len(trace_map),
        }

        # Must be inside the `with mlflow.start_run(...)` block: log_metric
        # without an active run silently auto-starts a fresh adjective-animal
        # run that never gets closed (RUNNING-status "ghost runs" in the UI).
        _log_pass_bucket_metrics(rows_for_output)

    logger.info(
        "Evaluation complete: %s — accuracy=%.1f%%, thresholds=%s",
        run_name,
        output["overall_accuracy"],
        "PASS" if thresholds_passed else "FAIL",
    )

    if EVAL_DEBUG:
        _print_eval_summary(
            rows_for_output, scores_100, thresholds_passed,
            iteration, eval_scope, len(filtered),
        )

    return output


def _log_pass_bucket_metrics(rows_for_output: list[dict]) -> None:
    """Log ``logical_pass_count`` + ``all_judge_pass_count`` to MLflow.

    Under ``GSO_SCORING_V2=shadow`` the legacy count is also mirrored to
    ``shadow.all_judge_pass_count`` so reviewers can diff the two in the
    MLflow UI without touching the headline metric. Never raises — a
    metric log failure must not take down the evaluation.
    """
    try:
        logical = 0
        all_judge = 0
        for row in rows_for_output:
            lp, ap = _compute_pass_buckets(row)
            if lp:
                logical += 1
            if ap:
                all_judge += 1
        total = len(rows_for_output) or 1
        logical_pct = round(100 * logical / total, 2)
        all_judge_pct = round(100 * all_judge / total, 2)
        mlflow.log_metric("logical_pass_count", float(logical))
        mlflow.log_metric("all_judge_pass_count", float(all_judge))
        mlflow.log_metric("logical_pass_pct", float(logical_pct))
        mlflow.log_metric("all_judge_pass_pct", float(all_judge_pct))
        if scoring_v2_is_shadow():
            mlflow.log_metric("shadow.all_judge_pass_count", float(all_judge))
            mlflow.log_metric("shadow.logical_pass_count", float(logical))
            mlflow.log_metric("shadow.all_judge_pass_pct", float(all_judge_pct))
            mlflow.log_metric("shadow.logical_pass_pct", float(logical_pct))
    except Exception:
        logger.debug("Failed to log pass-bucket metrics", exc_info=True)


# ── Repeatability Evaluation ──────────────────────────────────────────


REPEATABILITY_RUN_NAME_TEMPLATE = "repeatability_{iteration}_eval_{timestamp}"


def run_repeatability_evaluation(
    space_id: str,
    experiment_name: str,
    iteration: int,
    benchmarks: list[dict],
    domain: str,
    reference_sqls: dict[str, str],
    predict_fn: Any,
    *,
    spark: SparkSession | None = None,
    catalog: str = "",
    gold_schema: str = "",
    uc_schema: str = "",
    model_id: str | None = None,
    run_label: str = "",
    reference_result_hashes: dict[str, str] | None = None,
    run_name: str | None = None,
    extra_tags: dict[str, str] | None = None,
) -> dict:
    """Run a repeatability evaluation through ``mlflow.genai.evaluate()``.

    Re-queries Genie via *predict_fn* and uses a repeatability scorer to
    compare the new SQL against *reference_sqls* (``{question_id: sql}``
    from a prior iteration).  Produces full MLflow traces and judge verdicts.

    When *reference_result_hashes* is provided (``{question_id: genie_hash}``
    from a prior iteration), the scorer uses execution-based comparison as
    its primary tier before falling back to structural / exact SQL matching.

    Args:
        reference_sqls: Mapping of question_id → SQL from a previous run.
        reference_result_hashes: Mapping of question_id → normalised
            result-set hash from a previous run (enables Tier 1 scoring).
        run_label: Optional suffix for the run name (e.g. "final_1").
    """
    from genie_space_optimizer.optimization.scorers import make_repeatability_scorers

    mlflow.set_experiment(experiment_name)
    exp = mlflow.get_experiment_by_name(experiment_name)

    trace_destination = _configure_uc_trace_destination(
        experiment_id=exp.experiment_id if exp else "",
        uc_schema=uc_schema,
        warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
    )

    if not run_name:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{run_label}" if run_label else ""
        run_name = f"genie_repeatability_iter{iteration}_{ts}{suffix}"

    scorers = make_repeatability_scorers()

    mlflow_model_id = (
        model_id
        if isinstance(model_id, str) and model_id.startswith("m-")
        else None
    )

    _ref_hashes = reference_result_hashes or {}

    eval_records = []
    for b in benchmarks:
        qid = b.get("id", "")
        prev_sql = reference_sqls.get(qid, "")
        prev_result_hash = _ref_hashes.get(qid, "")
        eval_records.append(
            {
                "inputs": {
                    "question_id": qid,
                    "question": b["question"],
                    "space_id": space_id,
                    "expected_sql": b.get("expected_sql", ""),
                    "catalog": catalog,
                    "gold_schema": gold_schema,
                    "order_sensitive": bool(b.get("order_sensitive", False)),
                },
                "expectations": {
                    "expected_response": b.get("expected_sql", ""),
                    "expected_asset": _normalize_expected_asset(
                        b.get("expected_asset", "TABLE"),
                        b.get("expected_sql", ""),
                        hint=b.get("expected_asset_hint"),
                    ),
                    "previous_sql": prev_sql,
                    "previous_result_hash": prev_result_hash,
                },
            }
        )
    eval_data = pd.DataFrame(eval_records)

    with _scorer_feedback_scope(), mlflow.start_run(run_name=run_name) as run:
        # Tier 4: apply v2 tags from the caller (repeatability passes
        # identified by ``finalize/repeat_pass_{k}`` stage + iteration).
        if extra_tags:
            mlflow.set_tags({str(k): str(v) for k, v in extra_tags.items()})
        mlflow.log_params(
            {
                "space_id": space_id,
                "iteration": iteration,
                "eval_type": "repeatability",
                "domain": domain,
                "benchmark_count": len(benchmarks),
                "reference_sql_count": sum(1 for v in reference_sqls.values() if v),
                "run_label": run_label or "standard",
            }
        )

        if mlflow_model_id:
            mlflow.set_active_model(model_id=mlflow_model_id)

        evaluate_kwargs: dict[str, Any] = {
            "predict_fn": predict_fn,
            "data": eval_data,
            "scorers": scorers,
        }
        if mlflow_model_id:
            evaluate_kwargs["model_id"] = mlflow_model_id

        try:
            eval_result, eval_attempts = _run_evaluate_with_retries(
                evaluate_kwargs=evaluate_kwargs,
            )
        except Exception as exc:
            if _is_retryable_eval_exception(exc):
                logger.warning(
                    "Repeatability eval falling back to sequential: %s",
                    str(exc)[:300],
                )
                eval_result = _run_evaluate_sequential_fallback(
                    evaluate_kwargs=evaluate_kwargs,
                )
            else:
                logger.error("Repeatability evaluation failed: %s", str(exc)[:500])
                raise

        per_judge: dict[str, float] = {}
        for metric_name in eval_result.metrics:
            if "/mean" in metric_name:
                judge_name = metric_name.replace("/mean", "")
                per_judge[judge_name] = eval_result.metrics[metric_name]

        repeatability_raw = per_judge.get("repeatability", 0.0)
        repeatability_pct = repeatability_raw * 100 if repeatability_raw <= 1.0 else repeatability_raw

        rows_for_output: list[dict] = []
        _STRIP_COLS_REP = {"trace", "assessments", "spans", "trace_metadata"}
        if hasattr(eval_result, "tables") and "eval_results" in eval_result.tables:
            rep_df = eval_result.tables["eval_results"]
            rep_assessment_map = _extract_assessments_from_traces(rep_df)
            for row_idx, (_, row) in enumerate(rep_df.iterrows()):
                row_dict = {}
                for col in rep_df.columns:
                    if col in _STRIP_COLS_REP:
                        continue
                    val = row[col]
                    if hasattr(val, "item"):
                        val = val.item()
                    if not isinstance(val, (str, int, float, bool, type(None), list, dict)):
                        val = str(val)
                    row_dict[col] = val
                _merge_row_sources(row_dict, rep_assessment_map.get(row_idx), None)

                for col_name in list(row_dict.keys()):
                    if col_name.endswith("/rationale"):
                        jname = col_name.rsplit("/rationale", 1)[0]
                        if jname.startswith("feedback/"):
                            jname = jname[len("feedback/"):]
                        mkey = f"{jname}/metadata"
                        if mkey not in row_dict:
                            parsed = _parse_asi_from_rationale(str(row_dict.get(col_name, "")))
                            if parsed:
                                row_dict[mkey] = parsed

                rows_for_output.append(row_dict)

        # ── Three-tier sub-metrics ─────────────────────────────────────
        # Recompute tier classification from row data rather than relying
        # on Feedback metadata propagation (which varies by MLflow version).
        _tier_counts: dict[str, int] = {
            "execution": 0,
            "structural": 0,
            "exact": 0,
            "first_eval": 0,
            "none": 0,
            "no_output": 0,
        }
        _total_scored = 0

        from genie_space_optimizer.optimization.scorers.repeatability import (
            _sql_hash,
            _structurally_equivalent,
        )

        for _row in rows_for_output:
            _total_scored += 1

            # First try scorer metadata (works in some MLflow versions)
            _rep_meta = (
                _row.get("repeatability/metadata")
                or _row.get("feedback/repeatability/metadata")
                or {}
            )
            if isinstance(_rep_meta, str):
                try:
                    _rep_meta = json.loads(_rep_meta)
                except (json.JSONDecodeError, TypeError):
                    _rep_meta = {}
            tier = _rep_meta.get("match_tier", "") if isinstance(_rep_meta, dict) else ""

            if tier and tier in _tier_counts:
                _tier_counts[tier] += 1
                continue

            # Recompute tier from the verdict and available reference data
            verdict = str(
                _row.get("repeatability/value")
                or _row.get("feedback/repeatability/value")
                or _row.get("repeatability")
                or ""
            ).lower().strip()

            _prev_sql = (
                (_row.get("expectations") or {}).get("previous_sql", "")
                if isinstance(_row.get("expectations"), dict) else ""
            ) or _row.get("expectations/previous_sql", "")
            _prev_rh = (
                (_row.get("expectations") or {}).get("previous_result_hash", "")
                if isinstance(_row.get("expectations"), dict) else ""
            ) or _row.get("expectations/previous_result_hash", "")
            _curr_sql = (
                (_row.get("outputs") or {}).get("response", "")
                if isinstance(_row.get("outputs"), dict) else ""
            ) or _row.get("outputs/response", "")
            if not _curr_sql:
                _resp = _row.get("response") or {}
                if isinstance(_resp, dict):
                    _curr_sql = _resp.get("response", "")

            _curr_rh = _extract_genie_hash_from_row(_row)

            if not _prev_sql and not _prev_rh:
                _tier_counts["first_eval"] += 1
            elif not _curr_sql:
                _tier_counts["no_output"] += 1
            elif _prev_rh and _curr_rh:
                _tier_counts["execution"] += 1
            elif verdict == "yes":
                if _prev_sql and _curr_sql and _structurally_equivalent(_prev_sql, _curr_sql):
                    _tier_counts["structural"] += 1
                elif _prev_sql and _curr_sql and _sql_hash(_prev_sql) == _sql_hash(_curr_sql):
                    _tier_counts["exact"] += 1
                else:
                    _tier_counts["structural"] += 1
            else:
                _tier_counts["none"] += 1

        if _total_scored > 0:
            _pass_execution = (
                _tier_counts["execution"] + _tier_counts["structural"]
                + _tier_counts["exact"] + _tier_counts["first_eval"]
            )
            _pass_structural = _pass_execution
            _pass_exact = (
                _tier_counts["exact"] + _tier_counts["first_eval"]
            )
            repeatability_execution_pct = (_pass_execution / _total_scored) * 100
            repeatability_structural_pct = (_pass_structural / _total_scored) * 100
            repeatability_exact_pct = (_pass_exact / _total_scored) * 100
        else:
            repeatability_execution_pct = repeatability_pct
            repeatability_structural_pct = repeatability_pct
            repeatability_exact_pct = repeatability_pct

        _scorer_repeatability_pct = repeatability_pct
        repeatability_pct = max(
            repeatability_execution_pct,
            repeatability_pct,
        )

        mlflow.log_metrics({
            "repeatability_pct": repeatability_pct,
            "repeatability_scorer_pct": _scorer_repeatability_pct,
            "repeatability_execution_pct": repeatability_execution_pct,
            "repeatability_structural_pct": repeatability_structural_pct,
            "repeatability_exact_pct": repeatability_exact_pct,
        })
        mlflow.set_tags(
            {
                "evaluation_type": "repeatability",
                "repeatability_pct": f"{repeatability_pct:.1f}",
                "repeatability_execution_pct": f"{repeatability_execution_pct:.1f}",
                "iteration": str(iteration),
            }
        )

    logger.info(
        "Repeatability evaluation complete: %s — "
        "headline=%.1f%% (execution=%.1f%%, structural=%.1f%%, exact=%.1f%%)",
        run_name,
        repeatability_pct,
        repeatability_execution_pct,
        repeatability_structural_pct,
        repeatability_exact_pct,
    )

    _n_ref_hashes = sum(1 for v in _ref_hashes.values() if v)
    _rep_lines = [
        f"\n-- REPEATABILITY EVALUATION: {run_name} " + "-" * 30,
        f"  |  Repeatability (headline):    {repeatability_pct:.1f}%",
        f"  |  Execution equivalence:       {repeatability_execution_pct:.1f}%",
        f"  |  Structural equivalence:      {repeatability_structural_pct:.1f}%",
        f"  |  Exact SQL match:             {repeatability_exact_pct:.1f}%",
        f"  |  Questions:                   {len(benchmarks)}",
        f"  |  Reference SQLs:              {sum(1 for v in reference_sqls.values() if v)}",
        f"  |  Reference result hashes:     {_n_ref_hashes}",
    ]
    if _n_ref_hashes < len(benchmarks) * 0.5:
        _rep_lines.append(
            f"  |  Note: Only {_n_ref_hashes}/{len(benchmarks)} questions have reference hashes"
            " — re-eval scores below have limited coverage"
        )
    for _judge, _score in per_judge.items():
        _disp = _score * 100 if _score <= 1.0 else _score
        _rep_lines.append(f"  |  {_judge} (re-eval): {_disp:.1f}")
    _rep_lines.append("-" * 60)
    print("\n".join(_rep_lines))

    rep_trace_map: dict[str, str] = {}
    for _row in rows_for_output:
        _qid = (
            _row.get("question_id")
            or _row.get("inputs/question_id")
            or (_row.get("inputs") or {}).get("question_id", "")
        )
        _tid = _row.get("trace_id")
        if _qid and _tid:
            rep_trace_map[_qid] = str(_tid)

    return {
        "run_id": run.info.run_id,
        "mlflow_run_id": run.info.run_id,
        "run_name": run_name,
        "repeatability_pct": repeatability_pct,
        "repeatability_execution_pct": repeatability_execution_pct,
        "repeatability_structural_pct": repeatability_structural_pct,
        "repeatability_exact_pct": repeatability_exact_pct,
        "tier_counts": _tier_counts,
        "per_judge": per_judge,
        "rows": rows_for_output,
        "scores": normalize_scores(per_judge),
        "trace_map": rep_trace_map,
    }


def extract_reference_sqls(eval_result: dict) -> dict[str, str]:
    """Extract ``{question_id: generated_sql}`` from an evaluation output.

    Used to build *reference_sqls* for subsequent repeatability evaluations.
    Handles both flat column names (``inputs/question_id``) and nested
    dicts (``request.kwargs.question_id``, ``response.response``).
    """
    ref: dict[str, str] = {}
    rows = eval_result.get("rows", [])
    for row in rows:
        _req = row.get("request") or {}
        _req_kwargs = _req.get("kwargs", {}) if isinstance(_req, dict) else {}
        _resp = row.get("response") or {}
        qid = (
            row.get("inputs/question_id")
            or (row.get("inputs", {}) or {}).get("question_id", "")
            or _req_kwargs.get("question_id", "")
            or row.get("question_id", "")
        )
        sql = (
            row.get("outputs/response")
            or (row.get("outputs", {}) or {}).get("response", "")
            or (_resp.get("response", "") if isinstance(_resp, dict) else "")
        )
        if qid:
            ref[str(qid)] = str(sql or "")
    return ref


def extract_reference_result_hashes(eval_result: dict) -> dict[str, str]:
    """Extract ``{question_id: genie_result_hash}`` from an evaluation output.

    Mirrors :func:`extract_reference_sqls` but pulls the result-set hash
    (``comparison.genie_hash``) computed by the predict function.  Used to
    enable execution-based repeatability comparison in subsequent runs.

    Handles multiple MLflow column formats (``outputs``, ``response``,
    semi-flat, and fully flat variants).
    """
    ref: dict[str, str] = {}
    rows = eval_result.get("rows", [])
    for row in rows:
        _req = row.get("request") or {}
        _req_kwargs = _req.get("kwargs", {}) if isinstance(_req, dict) else {}
        qid = (
            row.get("inputs/question_id")
            or (row.get("inputs", {}) or {}).get("question_id", "")
            or _req_kwargs.get("question_id", "")
            or row.get("question_id", "")
        )
        if not qid:
            continue

        genie_hash = _extract_genie_hash_from_row(row)

        if qid and genie_hash:
            ref[str(qid)] = str(genie_hash)
    return ref


def _extract_genie_hash_from_row(row: dict) -> str:
    """Extract ``genie_hash`` from a single eval-result row.

    Checks ``outputs``, ``response``, semi-flat, and fully flat column
    formats used by different MLflow versions.
    """
    genie_hash = ""

    # 1. Nested outputs dict: outputs.comparison.genie_hash
    outputs = row.get("outputs") or {}
    if isinstance(outputs, dict):
        cmp = outputs.get("comparison") or {}
        if isinstance(cmp, dict):
            genie_hash = cmp.get("genie_hash", "")

    # 2. MLflow 'response' column (some versions store predict output here)
    if not genie_hash:
        _resp = row.get("response") or {}
        if isinstance(_resp, dict):
            cmp = _resp.get("comparison") or {}
            if isinstance(cmp, dict):
                genie_hash = cmp.get("genie_hash", "")

    # 3. Semi-flat: outputs/comparison as a dict or JSON string
    if not genie_hash:
        cmp_raw = row.get("outputs/comparison") or {}
        if isinstance(cmp_raw, dict):
            genie_hash = cmp_raw.get("genie_hash", "")
        elif isinstance(cmp_raw, str):
            try:
                cmp_parsed = json.loads(cmp_raw)
                genie_hash = cmp_parsed.get("genie_hash", "")
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

    # 4. Fully flat: outputs/comparison/genie_hash
    if not genie_hash:
        genie_hash = row.get("outputs/comparison/genie_hash", "")

    return genie_hash or ""


# ── Benchmark Extraction from Genie Space ──────────────────────────────


AUTO_OPTIMIZE_TAG_PREFIX = "[auto-optimize] "


def _coerce_question_text(raw: Any) -> str:
    if isinstance(raw, list):
        return " ".join(str(part) for part in raw).strip()
    return str(raw or "").strip()


def _strip_legacy_auto_optimize_prefix(question: str) -> str:
    text = str(question or "").strip()
    if text.startswith(AUTO_OPTIMIZE_TAG_PREFIX):
        return text[len(AUTO_OPTIMIZE_TAG_PREFIX):].strip()
    return text


def _extract_sql_answer(answers: Any) -> str:
    if not isinstance(answers, list):
        return ""
    for ans in answers:
        if not isinstance(ans, dict):
            continue
        if str(ans.get("format", "")).upper() != "SQL":
            continue
        content = ans.get("content", [])
        if isinstance(content, list):
            return "".join(str(part) for part in content).strip()
        if isinstance(content, str):
            return content.strip()
    return ""


def _normalized_question_key(question: str) -> str:
    text = _strip_legacy_auto_optimize_prefix(str(question or ""))
    return re.sub(r"\s+", " ", text.strip().lower())


def _extract_example_sql_question_keys(config: dict) -> set[str]:
    parsed = config.get("_parsed_space", config)
    if not isinstance(parsed, dict):
        return set()
    keys: set[str] = set()

    def _walk(container: dict) -> None:
        example_sqls = container.get("example_question_sqls")
        if not isinstance(example_sqls, list):
            return
        for item in example_sqls:
            if isinstance(item, dict):
                key = _normalized_question_key(_coerce_question_text(item.get("question", "")))
                if key:
                    keys.add(key)

    _walk(parsed)
    inst = parsed.get("instructions", {})
    if isinstance(inst, dict):
        _walk(inst)
    return keys


def _filter_example_sql_mirrored_benchmarks(
    benchmarks: list[dict],
    config: dict,
) -> list[dict]:
    blocked = _extract_example_sql_question_keys(config)
    if not blocked:
        return benchmarks
    filtered = [
        b for b in benchmarks
        if _normalized_question_key(str(b.get("question", ""))) not in blocked
    ]
    dropped = len(benchmarks) - len(filtered)
    if dropped:
        logger.info(
            "Dropped %d benchmark row(s) mirrored in example_question_sqls",
            dropped,
        )
    return filtered


def extract_genie_space_benchmarks(
    config: dict,
    spark: SparkSession,
    catalog: str = "",
    schema: str = "",
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Extract benchmark questions from a Genie Space config.

    Sources:
      1. ``benchmarks.questions`` — user-authored benchmark questions, with
         optional SQL answers.
      2. ``config.sample_questions`` — user-authored natural-language sample
         questions that need ground-truth SQL generation.

    ``instructions.example_question_sqls`` are training examples and are
    intentionally excluded from the benchmark corpus.
    """
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}

    benchmarks: list[dict] = []
    seen_questions: set[str] = set()

    def _append_question(
        *,
        question: str,
        expected_sql: str,
        source: str,
        category: str,
    ) -> None:
        normalized_question = _strip_legacy_auto_optimize_prefix(question)
        q_lower = normalized_question.lower().strip()
        if not q_lower or q_lower in seen_questions:
            return
        seen_questions.add(q_lower)

        validation_status = "question_only"
        validation_reason_code = "missing_expected_sql"
        sql = expected_sql.strip()
        if sql:
            from genie_space_optimizer.optimization.benchmarks import fix_mv_alias_sort_collision
            sql = fix_mv_alias_sort_collision(sql)
            is_valid, err = validate_ground_truth_sql(
                sql,
                spark,
                catalog=catalog,
                gold_schema=schema,
                w=w,
                warehouse_id=warehouse_id,
            )
            if is_valid:
                validation_status = "valid"
                validation_reason_code = "ok"
            else:
                logger.warning(
                    "Genie space benchmark source SQL failed validation: %s -- %s",
                    normalized_question[:60],
                    err,
                )
                sql = ""
                validation_status = "question_only"
                validation_reason_code = "invalid_source_sql"

        benchmarks.append({
            "question": normalized_question,
            "expected_sql": sql,
            "expected_asset": detect_asset_type(sql) if sql else "TABLE",
            "category": category,
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": source,
            "provenance": "curated",
            "validation_status": validation_status,
            "validation_reason_code": validation_reason_code,
            "validation_error": None if sql else "No valid expected SQL in Genie benchmark source",
        })

    bench_section = parsed_space.get("benchmarks", {})
    if not isinstance(bench_section, dict):
        bench_section = {}
    bench_questions = bench_section.get("questions", [])
    for bq in bench_questions if isinstance(bench_questions, list) else []:
        if not isinstance(bq, dict):
            continue
        question = _coerce_question_text(bq.get("question", ""))
        expected_sql = _extract_sql_answer(bq.get("answer", []))
        _append_question(
            question=question,
            expected_sql=expected_sql,
            source="genie_benchmark",
            category="user_benchmark",
        )

    cfg_block = parsed_space.get("config", {})
    if not isinstance(cfg_block, dict):
        cfg_block = {}
    sample_questions = cfg_block.get("sample_questions", [])
    for sq in sample_questions if isinstance(sample_questions, list) else []:
        if not isinstance(sq, dict):
            continue
        _append_question(
            question=_coerce_question_text(sq.get("question", "")),
            expected_sql="",
            source="sample_question",
            category="sample_question",
        )

    benchmarks = _filter_example_sql_mirrored_benchmarks(benchmarks, config)

    logger.info(
        "Extracted %d benchmark question(s) from Genie space config "
        "(%d with SQL, %d requiring SQL generation)",
        len(benchmarks),
        sum(1 for b in benchmarks if b.get("expected_sql")),
        sum(1 for b in benchmarks if not b.get("expected_sql")),
    )
    return benchmarks


# ── Benchmark Generation ────────────────────────────────────────────────


def _build_valid_assets_context(config: dict) -> str:
    """Build an explicit allowlist of Genie space data assets for the LLM prompt.

    Uses the *effective* MV / table classification so that any
    ``data_sources.tables`` entries Genie serialized but which carry
    measure-typed column configs are surfaced to the LLM as METRIC
    VIEW (the only label that triggers the MEASURE() worked example
    in the prompt). Otherwise the LLM happily emits ``SUM(measure)``
    against an MV and the execute gate rejects every candidate with
    ``METRIC_VIEW_MISSING_MEASURE_FUNCTION``.
    """
    mv_idents = effective_metric_view_identifiers_with_catalog(config)
    table_idents = effective_table_identifiers(config)
    lines: list[str] = []
    for tbl in sorted(table_idents):
        lines.append(f"- TABLE: {tbl}")
    for mv in sorted(mv_idents):
        lines.append(f"- METRIC VIEW: {mv}")
    for fn in config.get("_functions", []):
        lines.append(f"- FUNCTION: {fn}")
    return "\n".join(lines) if lines else "(no assets configured)"


def _space_table_asset_candidates(config: dict) -> set[str]:
    candidates: set[str] = set()
    for raw in sorted(
        effective_table_identifiers(config)
        | effective_metric_view_identifiers_with_catalog(config)
    ):
        candidates.update(_identifier_candidates(str(raw)))
    return {c for c in candidates if c}


def _space_function_candidates(config: dict) -> set[str]:
    candidates: set[str] = set()
    for raw in config.get("_functions", []) if isinstance(config.get("_functions"), list) else []:
        candidates.update(_identifier_candidates(str(raw)))
    return {c for c in candidates if c}


def _uc_column_table_candidates(row: dict) -> set[str]:
    table_name = str(row.get("table_name") or "").strip()
    catalog_name = str(row.get("catalog_name") or "").strip()
    schema_name = str(row.get("schema_name") or "").strip()
    candidates = _identifier_candidates(table_name)
    if catalog_name and schema_name and table_name:
        candidates.update(_identifier_candidates(f"{catalog_name}.{schema_name}.{table_name}"))
    if schema_name and table_name:
        candidates.update(_identifier_candidates(f"{schema_name}.{table_name}"))
    return {c for c in candidates if c}


def _filter_uc_columns_to_space_assets(config: dict, uc_columns: list[dict]) -> list[dict]:
    allowed = _space_table_asset_candidates(config)
    if not allowed:
        return []
    return [
        col for col in uc_columns
        if isinstance(col, dict) and (_uc_column_table_candidates(col) & allowed)
    ]


def _filter_uc_routines_to_space_functions(config: dict, uc_routines: list[dict]) -> list[dict]:
    allowed = _space_function_candidates(config)
    if not allowed:
        return []
    filtered: list[dict] = []
    for routine in uc_routines:
        if not isinstance(routine, dict):
            continue
        raw_name = str(routine.get("routine_name") or routine.get("specific_name") or "").strip()
        if raw_name and (_identifier_candidates(raw_name) & allowed):
            filtered.append(routine)
    return filtered


def _filter_data_profile_to_space_assets(config: dict) -> dict[str, dict]:
    profile = config.get("_data_profile", {})
    if not isinstance(profile, dict):
        return {}
    allowed = _space_table_asset_candidates(config)
    if not allowed:
        return {}
    scoped: dict[str, dict] = {}
    for table, table_info in profile.items():
        if _identifier_candidates(str(table)) & allowed:
            scoped[str(table)] = table_info
    return scoped


def _format_data_profile_context(config: dict, data_profile: dict[str, dict] | None = None) -> str:
    """Build a compact data-profile section for benchmark generation prompts.

    Renders per-table row counts, per-column cardinality, distinct values
    for low-cardinality columns, and min/max ranges for numeric/date columns.
    """
    profile = data_profile if data_profile is not None else config.get("_data_profile", {})
    if not profile:
        return "(no data profile available)"
    lines: list[str] = []
    for table, tinfo in sorted(profile.items()):
        row_count = tinfo.get("row_count", "?")
        lines.append(f"### {table} (~{row_count} rows)")
        for col, cinfo in sorted(tinfo.get("columns", {}).items()):
            card = cinfo.get("cardinality", "?")
            vals = cinfo.get("distinct_values")
            minv = cinfo.get("min")
            maxv = cinfo.get("max")
            parts = [f"cardinality={card}"]
            if vals:
                parts.append(f"values={vals}")
            if minv is not None:
                parts.append(f"range=[{minv}, {maxv}]")
            lines.append(f"  - {col}: {', '.join(parts)}")
    return "\n".join(lines)


def _build_schema_contexts(
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
) -> dict[str, str]:
    """Build the schema context strings for benchmark prompts."""
    scoped_uc_columns = _filter_uc_columns_to_space_assets(config, uc_columns)
    scoped_uc_routines = _filter_uc_routines_to_space_functions(config, uc_routines)

    tables_context = "\n".join(
        f"- {c.get('table_name', '')}.{c.get('column_name', '')} ({c.get('data_type', '')}): {c.get('comment', '')}"
        for c in scoped_uc_columns
    )

    # -- Metric views: enrich with measure/dimension column detail --
    # Walk the *effective* MV set: union of ``data_sources.metric_views``
    # plus any ``data_sources.tables`` entries that carry measure-typed
    # column configs. This catches the case where Genie serialized an
    # MV under ``tables`` (e.g. when ``metric_views: 0`` in the config
    # but Spark plans the asset as MetricView) — without this fixup the
    # prompt's metric-view block reads "(none)", the LLM never gets
    # the MEASURE() worked example, and the execute gate rejects every
    # candidate against the MV.
    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}

    mv_lines: list[str] = []
    for mv in _iter_effective_metric_view_entries(config):
        ident = (mv.get("identifier") or "").strip()
        if not ident:
            continue
        measures: list[str] = []
        dimensions: list[str] = []
        for cc in mv.get("column_configs", []) or []:
            if not isinstance(cc, dict):
                continue
            col = cc.get("column_name", "")
            if not col:
                continue
            if (
                str(cc.get("column_type", "")).lower() == "measure"
                or cc.get("is_measure")
            ):
                measures.append(col)
            else:
                dimensions.append(col)
        parts = [f"- {ident}"]
        if measures:
            parts.append(f"  Measures (use MEASURE() syntax): {', '.join(measures)}")
        if dimensions:
            parts.append(f"  Dimensions (for GROUP BY / WHERE): {', '.join(dimensions)}")
        if not measures and not dimensions:
            parts.append("  (no column detail available)")
        mv_lines.append("\n".join(parts))
    if mv_lines:
        # PR 26 — explicit anti-pattern reminder + a positive minimal
        # example so the LLM has both the rule ("never JOIN MVs
        # directly") AND a worked template ("CTE-first pattern") in
        # the context that lists this run's metric views. The hint is
        # in addition to the per-template ``no direct JOINs`` rule
        # text already baked into the synthesis prompts so even
        # custom prompts that override those rules still surface the
        # anti-pattern alongside the MV detail.
        mv_lines.append(
            "\nAnti-pattern reminder for the metric views above:\n"
            "  Do NOT JOIN metric views directly. Spark rejects every "
            "such query with METRIC_VIEW_JOIN_NOT_SUPPORTED.\n"
            "  Compute every required measure inside a per-MV CTE "
            "(SELECT the dims you need + MEASURE(<m>) AS <m>), then "
            "JOIN the CTE results in the outer query. Example:\n"
            "    WITH __mv_sales AS (\n"
            "      SELECT region, MEASURE(total_sales) AS total_sales\n"
            "      FROM cat.sch.mv_sales\n"
            "    )\n"
            "    SELECT s.region, s.total_sales, d.region_name\n"
            "    FROM __mv_sales s\n"
            "    JOIN cat.sch.dim_region d ON s.region = d.region_code;"
        )
    metric_views_context = "\n".join(mv_lines) if mv_lines else "(none)"

    tvfs = config.get("_functions", [])
    tvfs_context = "\n".join(
        f"- {r.get('routine_name', '')}: {r.get('routine_definition', '')[:200]}"
        for r in scoped_uc_routines
    ) if scoped_uc_routines else (
        "\n".join(f"- {t}" for t in tvfs) if tvfs else "(none)"
    )

    # -- Join specifications --
    inst = parsed_space.get("instructions", {})
    if not isinstance(inst, dict):
        inst = {}
    ds_js = parsed_space.get("data_sources", {})
    if not isinstance(ds_js, dict):
        ds_js = {}
    join_specs = (
        inst.get("join_specs", []) if isinstance(inst.get("join_specs"), list) else []
    ) or (
        ds_js.get("join_specs", []) if isinstance(ds_js.get("join_specs"), list) else []
    )
    if join_specs:
        js_lines: list[str] = []
        for js in join_specs:
            left = js.get("left", {})
            right = js.get("right", {})
            sql_parts = js.get("sql", [])
            predicate = sql_parts[0] if isinstance(sql_parts, list) and sql_parts else str(sql_parts)
            js_lines.append(
                f"- {left.get('identifier', '?')} <-> {right.get('identifier', '?')}: {predicate[:200]}"
            )
        join_specs_context = "\n".join(js_lines)
    else:
        join_specs_context = "(No join specifications configured.)"

    instructions = config.get("_instructions", [])
    instructions_context = "\n".join(
        f"- {i.get('text', i) if isinstance(i, dict) else i}" for i in instructions
    ) if instructions else "(none)"

    cfg_block = parsed_space.get("config", {})
    if not isinstance(cfg_block, dict):
        cfg_block = {}
    sample_questions = cfg_block.get("sample_questions", [])
    if not isinstance(sample_questions, list) or not sample_questions:
        # Legacy serialized spaces stored sample_questions at the top level;
        # keep that fallback so older fixtures still render.
        legacy = parsed_space.get("sample_questions", [])
        if isinstance(legacy, list):
            sample_questions = legacy
    sample_questions_context = "\n".join(
        f"- {_coerce_question_text(q.get('question', q) if isinstance(q, dict) else q)}"
        for q in sample_questions
    ) if sample_questions else "(none)"

    columns_by_table: dict[str, list[str]] = {}
    for c in scoped_uc_columns:
        if not isinstance(c, dict):
            continue
        tbl = str(c.get("table_name") or "").strip()
        col = str(c.get("column_name") or "").strip()
        dtype = str(c.get("data_type") or "").strip().upper()
        if tbl and col:
            entry = f"{col} ({dtype})" if dtype else col
            columns_by_table.setdefault(tbl, []).append(entry)
    column_allowlist_lines: list[str] = []
    for tbl_name in sorted(columns_by_table):
        column_allowlist_lines.append(f"{tbl_name}: {', '.join(columns_by_table[tbl_name])}")
    column_allowlist = "\n".join(column_allowlist_lines) if column_allowlist_lines else "(no columns)"

    return {
        "tables_context": tables_context,
        "metric_views_context": metric_views_context,
        "tvfs_context": tvfs_context,
        "join_specs_context": join_specs_context,
        "instructions_context": instructions_context,
        "sample_questions_context": sample_questions_context,
        "valid_assets_context": _build_valid_assets_context(config),
        "column_allowlist": column_allowlist,
        "data_profile_context": _format_data_profile_context(
            config,
            _filter_data_profile_to_space_assets(config),
        ),
    }


def _validate_benchmark_sql(
    sql: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    execute: bool = False,
    w: Any = None,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Validate a benchmark's expected_sql. Returns (is_valid, error)."""
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    resolved = resolve_sql(sql, catalog, schema)
    sanitized = sanitize_sql(resolved)
    if not sanitized.strip():
        return False, "Empty SQL"
    return validate_ground_truth_sql(
        sanitized, spark, catalog=catalog, gold_schema=schema, execute=execute,
        w=w, warehouse_id=warehouse_id,
    )


def _attempt_sql_correction(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    invalid_candidates: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    allowlist: dict[str, Any],
    *,
    correction_prompt_template: str,
    correction_prompt_registry_key: str,
    warehouse_id: str = "",
    repair_counters: dict[str, int] | None = None,
) -> list[dict]:
    """Send invalid SQL candidates back to the LLM for correction.

    Shared between benchmark and example-SQL generation paths. Callers
    differ only in the prompt template + MLflow registry key — the
    per-candidate error payload (``benchmarks_to_fix`` JSON), the
    schema context, the metadata + SQL revalidation, and the returned
    provenance are all identical. Returns corrected candidates that
    pass both ``_enforce_metadata_constraints`` and
    ``_validate_benchmark_sql`` (the latter named historically; it is
    generic EXPLAIN+execute validation).

    Note: the LLM output field is still ``expected_sql`` regardless of
    caller, because the correction-prompt contracts (both benchmark and
    example variants) share that schema.

    ``repair_counters`` (optional): when provided, F8 deterministic
    repairs (stem qualification + MEASURE() wrapping) are counted
    under the keys ``repaired_stemmed_identifiers`` and
    ``repaired_measure_refs``. The unified pipeline threads this dict
    so its summary banner can surface the same F4/F5 counters that the
    preflight pipeline already displays. When ``None``, repairs still
    fire (they can only help) but the counts are discarded.
    """
    if not invalid_candidates:
        return []

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    def _benchmark_payload(b: dict) -> dict:
        err_str = str(b.get("validation_error", "") or "")
        # PR 16: emit class-specific repair hints so the LLM gets a
        # deterministic nudge toward the correct fix instead of
        # re-deriving the diagnosis from the raw error string. Reuse
        # the validation reason code already attached to the
        # benchmark when available (avoids a re-classification round
        # trip); fall back to classifying the error string when the
        # caller didn't pre-classify.
        reason = str(b.get("validation_reason_code") or "").strip()
        if not reason:
            reason = _classify_sql_validation_error(err_str)
        repair_hint = _repair_hint_for_reason(reason)
        execution_note = (
            "Query returns 0 rows — pick realistic filter values from the Data Profile"
            if err_str == "Query returns 0 rows"
            else ""
        )
        return {
            "question": b["question"],
            "original_expected_sql": b["expected_sql"],
            "error": err_str or "unknown",
            "validation_reason_code": reason,
            "repair_hint": repair_hint,
            "execution_note": execution_note,
        }

    benchmarks_to_fix = json.dumps(
        [_benchmark_payload(b) for b in invalid_candidates],
        indent=2,
    )

    prompt = format_mlflow_template(
        correction_prompt_template,
        valid_assets_context=ctx["valid_assets_context"],
        tables_context=ctx["tables_context"],
        column_allowlist=ctx.get("column_allowlist", "(no columns)"),
        metric_views_context=ctx.get("metric_views_context", "None"),
        tvfs_context=ctx.get("tvfs_context", "None"),
        data_profile_context=ctx.get("data_profile_context", "(no data profile available)"),
        benchmarks_to_fix=benchmarks_to_fix,
    )

    try:
        with mlflow.start_span(
            name="benchmark_correction", span_type=SpanType.CHAIN,
        ) as _corr_span:
            try:
                _corr_span.set_inputs({
                    "candidate_count": len(invalid_candidates),
                    "prompt_registry_key": correction_prompt_registry_key,
                    "prompt_name": get_registered_prompt_name(correction_prompt_registry_key),
                })
            except Exception:
                pass
            response = _call_llm_for_scoring(
                w, prompt,
                prompt_name=get_registered_prompt_name(correction_prompt_registry_key),
            )
            try:
                _corr_span.set_outputs({
                    "correction_count": (
                        len(response) if isinstance(response, list)
                        else len(response.get("benchmarks", []))
                    ),
                })
            except Exception:
                pass
        corrections: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning(
            "SQL correction LLM call failed (registry=%s)",
            correction_prompt_registry_key,
            exc_info=True,
        )
        return []

    # F8 — prepare the identifier/measure universes ONCE for the
    # whole correction batch so the per-candidate repair loop below
    # stays O(1) per candidate in dict-lookup terms. Both helpers are
    # side-effect-free so an empty universe is a clean no-op.
    # Lazy-imported to avoid a module-level cycle: ``preflight_synthesis``
    # already lazy-imports ``evaluation`` inside its own functions.
    from genie_space_optimizer.optimization.preflight_synthesis import (
        repair_stemmed_identifiers_in_sql,
    )

    # Canonical identifiers come from ``config`` (the source of truth)
    # NOT ``allowlist["assets"]``. The allowlist expands every asset
    # into its short-form variants via ``_identifier_candidates`` for
    # metadata enforcement — e.g. ``cat.sch.mv`` also registers
    # ``sch.mv`` and ``mv``. Feeding those short forms into the
    # stem-repair helper would make the leaf stem point to multiple
    # "canonicals", marking it ambiguous and blocking the rewrite
    # exactly where production needs it to fire. Using the config's
    # primary identifiers keeps the unified repair logic 1:1 with
    # ``_repair_stemmed_identifiers`` in the preflight pipeline.
    canonical_assets: list[str] = []
    for key in ("_tables", "_metric_views"):
        for ident in config.get(key, []) or []:
            ident_s = str(ident).strip()
            if ident_s and ident_s not in canonical_assets:
                canonical_assets.append(ident_s)

    mv_measures = build_metric_view_measures(config)
    table_columns = build_table_columns(config)
    # PR 26 — short MV-name set for the direct-JOIN repair, mirrors
    # the unified synthesis path so the correction pipeline applies
    # the same CTE-first rewriter when the LLM's corrected SQL still
    # emits a direct JOIN against an MV.
    _mv_names_corr = effective_metric_view_identifiers_with_catalog(config)
    mv_short_set_corr: set[str] = {
        str(n).split(".")[-1].lower() for n in (_mv_names_corr or set()) if n
    }
    mv_short_set_corr.update(
        k.lower() for k in (mv_measures or {}).keys() if k
    )

    corrected: list[dict] = []
    for c in corrections:
        sql = c.get("expected_sql")
        if not sql or c.get("unfixable_reason"):
            logger.info("Candidate unfixable: %s — %s", c.get("question", "")[:60], c.get("unfixable_reason", ""))
            continue

        # F8 — apply the same deterministic repairs the preflight
        # pipeline runs (F4 stem qualification + F5 MEASURE() wrap)
        # to the LLM's corrected output BEFORE metadata/execute
        # validation. This closes the gap the field log surfaced:
        # the unified pipeline rejected candidates with bare table
        # stems (e.g. ``FROM dim_date`` when the allowlist held
        # ``cat.sch.mv_<domain>_dim_date``) even though the preflight
        # pipeline would have repaired them deterministically. Now
        # both pipelines handle the same failure shape identically.
        sql_str = str(sql)
        repaired_sql, stem_subs = repair_stemmed_identifiers_in_sql(
            sql_str, canonical_assets,
        )
        if stem_subs:
            sql_str = repaired_sql
            c["expected_sql"] = sql_str
            if repair_counters is not None:
                repair_counters["repaired_stemmed_identifiers"] = (
                    repair_counters.get("repaired_stemmed_identifiers", 0)
                    + len(stem_subs)
                )
        if mv_measures:
            wrapped_sql = _rewrite_measure_refs(sql_str, mv_measures)
            if wrapped_sql != sql_str:
                # ``_rewrite_measure_refs`` doesn't return a list of
                # wraps; count the net diff in MEASURE( occurrences
                # as a proxy. This matches the counter semantics the
                # preflight pipeline uses (count of rewrites applied).
                before_count = len(
                    re.findall(r"\bMEASURE\s*\(", sql_str, re.IGNORECASE),
                )
                after_count = len(
                    re.findall(r"\bMEASURE\s*\(", wrapped_sql, re.IGNORECASE),
                )
                sql_str = wrapped_sql
                c["expected_sql"] = sql_str
                if repair_counters is not None and after_count > before_count:
                    repair_counters["repaired_measure_refs"] = (
                        repair_counters.get("repaired_measure_refs", 0)
                        + (after_count - before_count)
                    )
            # Fix 5: strip ``SUM(MEASURE(x))`` → ``MEASURE(x)``. Runs
            # AFTER the measure-wrap so wraps the LLM emitted directly
            # (no outer agg) aren't double-touched, and wraps the
            # rewriter just inserted are normalised the same way as
            # those the LLM wrote inline.
            stripped_sql, strip_count = _strip_outer_agg_around_measure(
                sql_str,
            )
            if strip_count:
                sql_str = stripped_sql
                c["expected_sql"] = sql_str
                if repair_counters is not None:
                    repair_counters["stripped_outer_aggregate_around_measure"] = (
                        repair_counters.get(
                            "stripped_outer_aggregate_around_measure", 0,
                        ) + strip_count
                    )
        # PR 15: rename ``MEASURE(col) AS col`` to avoid Spark's
        # MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION.
        sql_str, _alias_fixes = _repair_measure_alias_collisions(sql_str)
        if _alias_fixes and repair_counters is not None:
            repair_counters["repaired_measure_alias_collisions"] = (
                repair_counters.get("repaired_measure_alias_collisions", 0)
                + _alias_fixes
            )
            c["expected_sql"] = sql_str

        # PR 20: lift measure-column references out of WHERE into a
        # CTE-first pattern. Conservative: no-op when no measure appears
        # in WHERE, when the SQL already has a WITH clause, when there's
        # an outer JOIN / set-op / subquery, or when sqlglot can't parse.
        if mv_measures:
            sql_str, _where_lifts = _repair_measure_in_where(sql_str, mv_measures)
            if _where_lifts and repair_counters is not None:
                repair_counters["repaired_measure_in_where"] = (
                    repair_counters.get("repaired_measure_in_where", 0)
                    + _where_lifts
                )
                c["expected_sql"] = sql_str

        # PR 26 — apply the same CTE-first rewriter for direct JOINs
        # on metric views. When the LLM correction round still emits a
        # raw MV-on-X join, hoist each MV into a CTE before the
        # downstream EXPLAIN/execute gate so we don't wastefully
        # re-prompt for a fix the rewriter can apply deterministically.
        if mv_short_set_corr:
            _join_reason = _check_metric_view_join_pre(
                sql_str, mv_short_set_corr,
            )
            if _join_reason:
                repaired_sql_join, _join_wraps = _repair_metric_view_join(
                    sql_str, mv_short_set_corr, mv_measures,
                )
                if _join_wraps:
                    sql_str = repaired_sql_join
                    c["expected_sql"] = sql_str
                    if repair_counters is not None:
                        repair_counters["repaired_metric_view_join"] = (
                            repair_counters.get(
                                "repaired_metric_view_join", 0,
                            ) + _join_wraps
                        )

        # Fix 3b: short-circuit candidates with dangling qualifiers
        # (``<qual>.<col>`` where ``qual`` is neither a FROM/JOIN table,
        # an explicit alias, nor a struct column on any FROM table).
        # The most common shape we want to catch is the LLM analogising
        # a real struct field (``dim_location.region``) onto a separate
        # dim table (``dim_date.year``) — these always fail the EXPLAIN
        # gate downstream, so we save the round-trip by rejecting here.
        # Auto-injecting JOINs is intentionally out of scope (would
        # require trustworthy FK direction inference); the rejection
        # alone is high signal for the strategist on the next loop.
        if table_columns:
            unresolved = _check_dangling_qualifiers(sql_str, table_columns)
            if unresolved:
                c["unfixable_reason"] = (
                    f"unresolved_qualifier: {','.join(unresolved)} "
                    "(not in FROM/aliases/struct cols)"
                )
                if repair_counters is not None:
                    repair_counters["rejected_unresolved_qualifier"] = (
                        repair_counters.get("rejected_unresolved_qualifier", 0)
                        + 1
                    )
                logger.info(
                    "Candidate rejected for dangling qualifier(s): %s — %s",
                    c.get("question", "")[:60], c["unfixable_reason"],
                )
                continue
        sql = sql_str

        metadata_ok, _reason_code, reason_message = _enforce_metadata_constraints(
            benchmark=c,
            sql=str(sql),
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            logger.warning(
                "Corrected candidate violates metadata constraints: %s — %s",
                c.get("question", "")[:60],
                reason_message,
            )
            continue
        is_valid, err = _validate_benchmark_sql(
            sql, spark, catalog, schema,
            w=w, warehouse_id=warehouse_id,
        )
        if is_valid:
            c["provenance"] = "auto_corrected"
            c["validation_status"] = "valid"
            c["validation_reason_code"] = "ok"
            c["validation_error"] = None
            c["correction_source"] = "llm_correction"
            corrected.append(c)
        else:
            logger.warning(
                "Corrected candidate still invalid: %s — %s", c.get("question", "")[:60], err,
            )
    return corrected


def _attempt_benchmark_correction(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    invalid_benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    allowlist: dict[str, Any],
    *,
    warehouse_id: str = "",
) -> list[dict]:
    """Benchmark-variant adapter for :func:`_attempt_sql_correction`.

    Preserves the historical signature + behaviour so existing call
    sites inside :func:`generate_benchmarks` (including the alignment
    correction loop) stay byte-identical post-refactor.
    """
    return _attempt_sql_correction(
        w=w, config=config, uc_columns=uc_columns, uc_routines=uc_routines,
        invalid_candidates=invalid_benchmarks,
        catalog=catalog, schema=schema, spark=spark, allowlist=allowlist,
        correction_prompt_template=BENCHMARK_CORRECTION_PROMPT,
        correction_prompt_registry_key="benchmark_correction",
        warehouse_id=warehouse_id,
    )


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1 — Unified SQL-examples engine
# ═══════════════════════════════════════════════════════════════════════
#
# Shared core powering both ``generate_benchmarks`` (via its existing
# orchestrator) and ``generate_example_sqls`` (the new producer). The
# core wraps the same validation primitives — ``_enforce_metadata_
# constraints``, ``_apply_metadata_field_drift_corrections``,
# ``_rewrite_measure_refs``, ``_guard_mv_select_star``,
# ``_validate_benchmark_sql``, ``_attempt_sql_correction`` — plus two
# new steps: arbiter approval (opt-in via ``run_arbiter=True``) and a
# leakage firewall (opt-in via a non-None ``leakage_oracle``).
#
# Isolation invariant: this function never iterates a BenchmarkCorpus
# and never inspects benchmark text. See
# ``docs/example-sql-isolation.md``.


# F12 — error-class buckets returned by ``_capture_result_rows`` so the
# arbiter gate (and the unified pipeline counters) can attribute a row-
# capture miss to its underlying cause instead of a single opaque
# ``arbiter_no_result_rows`` reason. ``subquery_unsupported`` covers the
# Spark/DBSQL family of errors emitted when the candidate SQL targets a
# metric view at the top level — e.g. ``MEASURE()`` against an MV is
# legal as a top-level query but rejected when wrapped as an inline
# subquery. ``exec_failed`` is the catch-all for anything else
# (timeouts, permissions, real syntax errors that slipped past EXPLAIN).
ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED = "subquery_unsupported"
ROW_CAPTURE_ERR_EXEC_FAILED = "exec_failed"

# Substrings (case-insensitive) that mark a Spark/DBSQL error as
# stemming from inline-subquery incompatibility with metric views. The
# canonical Spark error code is ``UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY``;
# DBSQL surfaces the metric-view variant with explicit ``metric view`` /
# ``MEASURE`` wording. We match conservatively — any one substring is
# enough to bucket the failure as ``subquery_unsupported`` rather than
# the generic ``exec_failed``.
_SUBQUERY_UNSUPPORTED_MARKERS = (
    "unsupported_subquery_expression_category",
    "subquery_expression_in",
    "metric view",
    "metric_view",
    "measure(",
)


def _classify_row_capture_error(err_msg: str) -> str:
    """Bucket a Spark/DBSQL row-capture exception message.

    Returns one of ``ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED`` or
    ``ROW_CAPTURE_ERR_EXEC_FAILED``. The classification is purely
    string-based — we cannot rely on Spark exception types because
    ``_exec_sql`` may route through the warehouse REST path which
    surfaces errors as plain strings.
    """
    lower = (err_msg or "").lower()
    for marker in _SUBQUERY_UNSUPPORTED_MARKERS:
        if marker in lower:
            return ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED
    return ROW_CAPTURE_ERR_EXEC_FAILED


# F13 — heuristic detection for metric-view / MEASURE-style SQL that the
# Tier 1 inline-subquery wrap cannot handle. The conservative trigger is
# ``MEASURE(`` substring (case-insensitive) anywhere in the resolved
# SQL; other metric-view shapes (e.g. plain ``SELECT * FROM mv`` with
# auto-rewritten measure aliases) get covered by the Tier 1 →
# Tier 2 fallback path on subquery-unsupported errors.
_MV_MEASURE_PATTERN = re.compile(r"\bMEASURE\s*\(", re.IGNORECASE)

# Match a top-level ``LIMIT <n>`` (with optional ``OFFSET <m>``) at the
# tail of the SQL string, after stripping a trailing ``;``. We don't
# attempt to parse nested LIMITs inside CTEs / subqueries — we only
# care about whether the outer query already caps its row count, so
# Tier 2 doesn't need to append.
_TRAILING_LIMIT_RE = re.compile(
    r"\bLIMIT\s+\d+(?:\s+OFFSET\s+\d+)?\s*$", re.IGNORECASE,
)


def _has_top_level_limit(sql: str) -> bool:
    """Heuristic — does the trimmed SQL already end with a LIMIT clause?"""
    s = (sql or "").rstrip().rstrip(";").rstrip()
    return bool(_TRAILING_LIMIT_RE.search(s))


def _inject_limit_clause(sql: str, limit: int) -> str:
    """Append ``LIMIT <n>`` to ``sql`` if no top-level LIMIT is present.

    Preserves a trailing ``;`` if the input had one. Leaves SQL with
    an existing top-level ``LIMIT`` clause untouched (we don't
    second-guess the LLM's row cap — Tier 2 only needs the SQL to
    return a bounded number of rows). Used by Tier 2 of
    :func:`_capture_result_rows` for metric-view / MEASURE-style SQL
    that DBSQL refuses to wrap as a subquery.
    """
    if _has_top_level_limit(sql):
        return sql
    s = (sql or "").rstrip()
    trailing_semi = s.endswith(";")
    if trailing_semi:
        s = s.rstrip(";").rstrip()
    s = f"{s} LIMIT {int(limit)}"
    if trailing_semi:
        s += ";"
    return s


def _capture_result_rows(
    sql: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    w: Any = None,
    warehouse_id: str = "",
    limit: int = 20,
) -> tuple[list[dict] | None, str | None, str | None]:
    """Run ``sql`` once and return the first ``limit`` rows as dicts.

    Used by the unified engine to give the arbiter judge actual result
    rows to evaluate. Uses the shared ``_exec_sql`` helper so the
    warehouse-vs-Spark routing is consistent with every other
    execution path in this module.

    Two-tier execution strategy (added in F13):

    1. **Tier 1 — subquery wrap** (preferred for plain SQL). Wraps as
       ``SELECT * FROM ({sql}) _gvse_sample LIMIT n``. Cheap,
       deterministic row count, doesn't touch the candidate text.
       DBSQL refuses this wrap when the inner query is a top-level
       ``MEASURE()`` against a metric view — the ``measure`` operator
       is not a legal subquery expression.
    2. **Tier 2 — LIMIT injection** (fallback / preferred for
       MEASURE-style SQL). Runs the candidate SQL directly with a
       top-level ``LIMIT n`` appended (no-op if it already has one).
       Avoids the subquery wrap, so metric-view candidates that
       passed EXPLAIN can finally reach the arbiter with rows.

    Tier 2 fires either pro-actively (when ``MEASURE(`` is detected
    in the SQL — skips Tier 1's known-failure case) or reactively
    (when Tier 1 raises a ``subquery_unsupported`` Spark error). Any
    other Tier 1 exception class returns immediately as
    ``(None, exec_failed, ...)`` without a Tier 2 attempt — those
    failures are not subquery-wrap-related and re-running with LIMIT
    injection won't fix them.

    Returns a 3-tuple ``(rows, error_class, error_message)``:

    * ``(rows_list, None, None)`` — SQL ran and produced rows
      (possibly an empty list if the query is valid but matches no
      data; empty rows are NOT a failure).
    * ``(None, error_class, error_message)`` — execution raised, with
      ``error_class`` one of :data:`ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED`
      or :data:`ROW_CAPTURE_ERR_EXEC_FAILED`. The error message is
      truncated to the first ~200 chars for log-line ergonomics.

    The diagnostic tuple was added in F12: ``arbiter_no_result_rows``
    was previously the single opaque code for both "DBSQL refused the
    subquery wrap because the SQL targets a metric view" and "the
    underlying execution genuinely failed" — operators had no signal
    to tell them apart at the banner level. Callers now branch on
    ``error_class`` and emit differentiated counters / reason codes.

    The exception is logged at WARNING (was DEBUG) with the SQL
    preview and error class so it shows up in the standard run log
    — a single ``arbiter_no_result_rows`` line was making field
    diagnosis impossible.
    """
    from genie_space_optimizer.optimization.benchmarks import resolve_sql
    try:
        # NOTE: ``resolve_sql`` accepts only ``sql`` positionally; the
        # rest are kwargs. Earlier call shape ``resolve_sql(sql, catalog,
        # schema)`` raised ``TypeError`` and was caught by the broad
        # except block below — making row capture silently impossible
        # in production. F12's WARNING-level logging surfaces this
        # class of failure, but the actual fix is to pass kwargs as
        # the function declares.
        resolved = resolve_sql(sql, catalog=catalog, gold_schema=schema)
    except Exception as exc:  # pragma: no cover — defensive
        err_msg = str(exc)[:200]
        logger.warning(
            "arbiter result-row capture failed (resolve_sql): %s | sql=%s",
            err_msg, (sql or "")[:200],
        )
        return None, ROW_CAPTURE_ERR_EXEC_FAILED, err_msg

    # Tier selection: skip Tier 1 entirely when MEASURE() is present —
    # the wrap is a known-failure shape there, no point burning a round-
    # trip just to re-discover it. For everything else, prefer Tier 1
    # (cheap, deterministic) and only fall back on subquery_unsupported.
    use_tier2_first = _MV_MEASURE_PATTERN.search(resolved or "") is not None

    if not use_tier2_first:
        sampling_sql = (
            f"SELECT * FROM ({resolved}) _gvse_sample LIMIT {int(limit)}"
        )
        try:
            df = _exec_sql(
                sampling_sql, spark,
                w=w, warehouse_id=warehouse_id,
                catalog=catalog, schema=schema,
            )
            if df is None or df.empty:
                return [], None, None
            return df.head(limit).to_dict(orient="records"), None, None
        except Exception as exc:  # pragma: no cover — defensive
            err_msg = str(exc)[:200]
            err_class = _classify_row_capture_error(err_msg)
            if err_class != ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED:
                # Not a subquery-wrap problem — Tier 2 wouldn't help.
                logger.warning(
                    "arbiter result-row capture failed (%s) [tier=1]: %s "
                    "| sql=%s",
                    err_class, err_msg, (sql or "")[:200],
                )
                return None, err_class, err_msg
            # Tier 1 hit the subquery-unsupported wall — log a downgrade
            # signal at INFO and fall through to Tier 2. This is the
            # expected path for metric-view candidates that don't have
            # an explicit ``MEASURE(`` in the candidate text but trigger
            # the subquery rejection at execution time.
            logger.info(
                "arbiter result-row capture: tier 1 subquery wrap "
                "unsupported, falling back to tier 2 LIMIT-injection "
                "(err=%s | sql=%s)",
                err_msg, (sql or "")[:200],
            )

    # Tier 2 — execute the original (resolved) SQL with a top-level
    # LIMIT injected. Reuses ``_exec_sql`` so warehouse-vs-Spark routing
    # stays consistent with the rest of the module.
    limited_sql = _inject_limit_clause(resolved, limit)
    try:
        df = _exec_sql(
            limited_sql, spark,
            w=w, warehouse_id=warehouse_id,
            catalog=catalog, schema=schema,
        )
        if df is None or df.empty:
            return [], None, None
        return df.head(limit).to_dict(orient="records"), None, None
    except Exception as exc:  # pragma: no cover — defensive
        err_msg = str(exc)[:200]
        err_class = _classify_row_capture_error(err_msg)
        logger.warning(
            "arbiter result-row capture failed (%s) [tier=2]: %s | sql=%s",
            err_class, err_msg, (sql or "")[:200],
        )
        return None, err_class, err_msg


def generate_validated_sql_examples(
    w: WorkspaceClient,
    spark: SparkSession,
    *,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    warehouse_id: str = "",
    target_count: int,
    generation_prompt_template: str,
    correction_prompt_template: str,
    generation_prompt_registry_key: str,
    correction_prompt_registry_key: str,
    existing_questions: list[str] | None = None,
    leakage_oracle: "Any" = None,  # LeakageOracle — forward reference, wired in R1b
    run_arbiter: bool = False,
    provenance: str = "synthetic",
    output_fields: tuple[str, ...] = ("question", "expected_sql"),
) -> tuple[list[dict], dict[str, int]]:
    """Unified SQL-examples generation engine.

    Produces validated (question, SQL) pairs by:
      1. Building schema context + metadata allowlist from ``config``
         and UC metadata.
      2. One batched LLM call via ``generation_prompt_template``.
      3. Per-candidate: metadata enforcement + field-drift correction
         + MV ``SELECT *`` guard + MEASURE auto-wrap + EXPLAIN/execute.
      4. Bounded correction loop (``MAX_CORRECTION_ROUNDS``) via
         ``_attempt_sql_correction`` with ``correction_prompt_template``.
      5. Optional arbiter approval (``run_arbiter=True``): executes the
         SQL once to capture result rows, then calls
         ``score_example_sql_correctness``. Verdict ``"yes"`` keeps
         the candidate; ``"no"``/``"uncertain"`` drops it.
      6. Optional leakage firewall: ``leakage_oracle.contains_sql``
         then ``contains_question`` on each survivor. Matches dropped.

    Returns ``(survivors, rejection_counters)``. The counters dict
    always contains the same set of keys so callers can log distinct
    rejection classes without conditional branches.

    Isolation: when ``leakage_oracle`` is a :class:`LeakageOracle`, the
    function never reads benchmark text. The oracle is a boolean match
    API — see ``docs/example-sql-isolation.md`` for the firewall spec.
    """
    existing_questions = existing_questions or []
    existing_q_lower: set[str] = {
        (q or "").strip().lower() for q in existing_questions if q
    }

    rejection_counters: dict[str, int] = {
        "metadata": 0,
        "mv_select_star": 0,
        "explain_or_execute": 0,
        "arbiter_no": 0,
        # F12 — row-capture diagnostic counters. ``arbiter_no`` is the
        # LLM judge saying "no" / "uncertain"; the two ``arbiter_row_
        # capture_*`` buckets cover the case where ``_capture_result_
        # rows`` raised before the judge was even consulted. Splitting
        # these gives operators an unambiguous banner signal: zero
        # subquery-unsupported counts means PR 13's metric-view-safe
        # fallback is doing its job; non-zero exec_failed counts point
        # at a different infrastructure issue (timeouts, perms).
        "arbiter_row_capture_subquery_unsupported": 0,
        "arbiter_row_capture_exec_failed": 0,
        "firewall_fingerprint": 0,
        "firewall_question_echo": 0,
        "dedup_in_corpus": 0,
        "unfixable_after_correction": 0,
        # F8 — deterministic repairs applied inside
        # ``_attempt_sql_correction``. Mirror the preflight pipeline's
        # F4/F5 counters so the unified banner (rendered by
        # ``harness._print_unified_summary``) can report the same
        # "LLM output was auto-healed before re-validation" signal
        # operators already rely on in the preflight output.
        "repaired_stemmed_identifiers": 0,
        "repaired_measure_refs": 0,
        # PR 17 — number of adaptive overdraw rounds we actually
        # used (1 = single LLM call, no overdraw needed; 2/3 = the
        # first round under-produced and we re-asked the LLM).
        "adaptive_overdraw_rounds_used": 0,
        # PR 26 — synthesis-side counters for the direct-JOIN-on-MV
        # pre-check. ``metric_view_join`` counts candidates the
        # pre-check rejected outright; ``metric_view_join_repaired``
        # counts MV references the deterministic CTE-first rewriter
        # successfully hoisted into a WITH clause, saving an EXPLAIN
        # round-trip + a correction loop iteration.
        "metric_view_join": 0,
        "metric_view_join_repaired": 0,
    }

    allowlist = _build_metadata_allowlist(
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
    )
    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    # ── 2. Per-candidate validation + MEASURE auto-wrap ──────────
    valid: list[dict] = []
    accepted_q_lower: set[str] = set()
    mv_measures = build_metric_view_measures(config)
    # Effective MV identifier set — includes assets Genie serialized
    # under ``data_sources.tables`` whose column_configs declare measures
    # (PR 14). The MV ``SELECT *`` guard and metric-view-aware dedup keys
    # rely on this set being complete or they wave through SQL that the
    # execute gate then rejects.
    mv_names = effective_metric_view_identifiers_with_catalog(config)
    # PR 26 — short-name MV set for the direct-JOIN pre-check. The
    # check matches by basename (``mv_sales``) so the LLM's
    # ``cat.sch.mv_sales`` references resolve regardless of whether
    # ``mv_names`` carries the fully-qualified or short form.
    mv_short_set: set[str] = {
        str(n).split(".")[-1].lower() for n in (mv_names or set()) if n
    }
    mv_short_set.update(
        k.lower() for k in (mv_measures or {}).keys() if k
    )

    def _register_valid(cand: dict) -> None:
        q = str(cand.get("question") or "").strip().lower()
        if not q or q in accepted_q_lower or q in existing_q_lower:
            rejection_counters["dedup_in_corpus"] += 1
            return
        accepted_q_lower.add(q)
        valid.append(cand)

    # ── PR 17: Adaptive overdraw without weakening gates ─────────
    # When the first LLM call returns fewer survivors than ``target_
    # count`` (because the model under-produced or because EXPLAIN /
    # execute / correction rejected most of them), do up to
    # ``ADAPTIVE_OVERDRAW_MAX_ROUNDS`` additional generation rounds
    # to top off the corpus. Each round:
    #   - Excludes already-accepted questions so the LLM doesn't echo
    #     the same questions back.
    #   - Requests exactly the deficit (no inflated multiplier — the
    #     correction loop already amortizes the per-round cost).
    #   - Runs the same strict per-candidate validation + correction
    #     as round 0. Gates are NEVER weakened to hit the target.
    # Arbiter / firewall remain a single post-loop pass; the dominant
    # rejection class in observed runs is the execute gate, so
    # over-drawing pre-arbiter is the highest-leverage knob.
    ADAPTIVE_OVERDRAW_MAX_ROUNDS = 3
    invalid_carry: list[dict] = []
    for gen_round in range(ADAPTIVE_OVERDRAW_MAX_ROUNDS):
        deficit = target_count - len(valid)
        if deficit <= 0:
            break

        rejection_counters["adaptive_overdraw_rounds_used"] = gen_round + 1
        if gen_round == 0:
            request_count = target_count
        else:
            request_count = deficit
            logger.info(
                "gvse adaptive overdraw round %d/%d: %d valid < target %d, "
                "requesting %d more candidates",
                gen_round + 1, ADAPTIVE_OVERDRAW_MAX_ROUNDS,
                len(valid), target_count, request_count,
            )

        excluded_questions: list[str] = list(existing_questions)
        excluded_questions.extend(sorted(accepted_q_lower))
        excluded_questions_context = ""
        if excluded_questions:
            excluded_questions_context = (
                "\n\n## Already Covered Questions (do NOT duplicate these)\n"
                + "\n".join(f"- {q}" for q in excluded_questions)
            )

        prompt = format_mlflow_template(
            generation_prompt_template,
            domain=domain,
            target_count=request_count,
            categories=json.dumps(BENCHMARK_CATEGORIES),
            **ctx,
        )
        if excluded_questions_context:
            prompt += excluded_questions_context

        try:
            response = _call_llm_for_scoring(
                w, prompt,
                prompt_name=get_registered_prompt_name(
                    generation_prompt_registry_key
                ),
            )
        except Exception:
            logger.warning(
                "generate_validated_sql_examples: LLM call failed "
                "(registry=%s, round=%d)",
                generation_prompt_registry_key, gen_round + 1,
                exc_info=True,
            )
            if gen_round == 0:
                return [], rejection_counters
            break

        raw_candidates: list[dict] = (
            response if isinstance(response, list)
            else response.get("benchmarks", [])
        )
        if not raw_candidates:
            # Empty response on a follow-up round means the model
            # has nothing more to add — stop early to avoid
            # spinning on the same exclusion list.
            if gen_round > 0:
                logger.info(
                    "gvse adaptive overdraw round %d returned 0 "
                    "candidates — stopping",
                    gen_round + 1,
                )
                break

        invalid: list[dict] = []

        for b in raw_candidates:
            if not isinstance(b, dict):
                continue
            sql_str = str(b.get("expected_sql") or "").strip()
            question = str(b.get("question") or "").strip()
            if not sql_str or not question:
                continue

            # Skip duplicates of the in-corpus questions and any
            # already-accepted question from a prior overdraw round.
            qlow = question.lower()
            if qlow in existing_q_lower or qlow in accepted_q_lower:
                rejection_counters["dedup_in_corpus"] += 1
                continue

            candidate: dict[str, Any] = {
                "question": question,
                "expected_sql": sql_str,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"), sql_str,
                    hint=b.get("expected_asset_hint"),
                ),
                "category": b.get("category", ""),
                "required_tables": [str(t) for t in b.get("required_tables", []) or []],
                "required_columns": [str(c) for c in b.get("required_columns", []) or []],
                "expected_facts": [str(f) for f in b.get("expected_facts", []) or []],
                "usage_guidance": b.get("usage_guidance", ""),
                "source": "llm_generated",
                "provenance": provenance,
                "validation_status": "valid",
                "validation_reason_code": "ok",
                "validation_error": None,
                "correction_source": "",
            }

            metadata_ok, reason_code, reason_message = _enforce_metadata_constraints(
                benchmark=candidate, sql=sql_str, allowlist=allowlist,
                catalog=catalog, schema=schema,
            )
            if not metadata_ok:
                if reason_code == "unknown_column":
                    corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                        sql=sql_str,
                        required_columns=candidate["required_columns"],
                        allowed_index=allowlist["column_index"],
                    )
                    if replacements and corrected_sql != sql_str:
                        candidate["expected_sql"] = corrected_sql
                        candidate["provenance"] = "auto_corrected"
                        candidate["correction_source"] = "metadata_suggestion"
                        candidate["field_drift_fixes"] = replacements
                        metadata_ok, reason_code, reason_message = (
                            _enforce_metadata_constraints(
                                benchmark=candidate, sql=corrected_sql,
                                allowlist=allowlist,
                                catalog=catalog, schema=schema,
                            )
                        )
                        sql_str = corrected_sql
                if not metadata_ok:
                    candidate["validation_status"] = "invalid"
                    candidate["validation_reason_code"] = reason_code
                    candidate["validation_error"] = reason_message
                    invalid.append(candidate)
                    rejection_counters["metadata"] += 1
                    continue

            is_star_ok, star_reason = _guard_mv_select_star(sql_str, mv_names)
            if not is_star_ok:
                candidate["validation_status"] = "invalid"
                candidate["validation_reason_code"] = "mv_select_star"
                candidate["validation_error"] = star_reason
                invalid.append(candidate)
                rejection_counters["mv_select_star"] += 1
                continue

            if mv_measures:
                sql_str = _rewrite_measure_refs(sql_str, mv_measures)
                sql_str, _alias_fixes = _repair_measure_alias_collisions(sql_str)
                if _alias_fixes:
                    rejection_counters["measure_alias_collisions_repaired"] = (
                        rejection_counters.get("measure_alias_collisions_repaired", 0)
                        + _alias_fixes
                    )
                # PR 20: CTE-first lift for measure-in-WHERE.
                sql_str, _where_lifts = _repair_measure_in_where(sql_str, mv_measures)
                if _where_lifts:
                    rejection_counters["measure_in_where_repaired"] = (
                        rejection_counters.get("measure_in_where_repaired", 0)
                        + _where_lifts
                    )
                candidate["expected_sql"] = sql_str

            # PR 26: detect direct JOINs against a metric view BEFORE
            # we burn an EXPLAIN round-trip. Try the deterministic
            # CTE-first repair first; on failure, reject with the
            # ``metric_view_join`` reason so the correction loop /
            # adaptive overdraw can route the candidate appropriately.
            if mv_short_set:
                _join_reason = _check_metric_view_join_pre(
                    sql_str, mv_short_set,
                )
                if _join_reason:
                    repaired_sql, _wraps = _repair_metric_view_join(
                        sql_str, mv_short_set, mv_measures,
                    )
                    if _wraps:
                        rejection_counters["metric_view_join_repaired"] = (
                            rejection_counters.get(
                                "metric_view_join_repaired", 0,
                            ) + _wraps
                        )
                        sql_str = repaired_sql
                        candidate["expected_sql"] = sql_str
                    else:
                        candidate["validation_status"] = "invalid"
                        candidate["validation_reason_code"] = (
                            "metric_view_join"
                        )
                        candidate["validation_error"] = (
                            "Direct JOIN against a metric view "
                            "(METRIC_VIEW_JOIN_NOT_SUPPORTED). Use the "
                            "CTE-first pattern: materialize the metric "
                            "view in a WITH clause, then JOIN the CTE."
                        )
                        invalid.append(candidate)
                        rejection_counters["metric_view_join"] = (
                            rejection_counters.get("metric_view_join", 0)
                            + 1
                        )
                        continue

            is_valid, err = _validate_benchmark_sql(
                sql_str, spark, catalog, schema, execute=True,
                w=w, warehouse_id=warehouse_id,
            )
            if is_valid:
                candidate["validation_status"] = "valid"
                candidate["validation_reason_code"] = "ok"
                candidate["validation_error"] = None
                _register_valid(candidate)
            else:
                candidate["validation_status"] = "invalid"
                candidate["validation_reason_code"] = _classify_sql_validation_error(err)
                candidate["validation_error"] = err
                invalid.append(candidate)
                # Counter incremented at correction-loop exit if still invalid.

        # ── 3. Bounded correction loop ───────────────────────────
        for correction_round in range(MAX_CORRECTION_ROUNDS):
            if not invalid:
                break
            logger.info(
                "gvse correction round %d/%d: attempting to fix %d invalid candidates",
                correction_round + 1, MAX_CORRECTION_ROUNDS, len(invalid),
            )
            corrected = _attempt_sql_correction(
                w=w, config=config, uc_columns=uc_columns, uc_routines=uc_routines,
                invalid_candidates=invalid,
                catalog=catalog, schema=schema, spark=spark, allowlist=allowlist,
                correction_prompt_template=correction_prompt_template,
                correction_prompt_registry_key=correction_prompt_registry_key,
                warehouse_id=warehouse_id,
                repair_counters=rejection_counters,
            )
            if not corrected:
                break
            for c in corrected:
                _register_valid(c)
            corrected_q = {
                str(c.get("question") or "").strip().lower()
                for c in corrected
            }
            invalid = [
                b for b in invalid
                if str(b.get("question") or "").strip().lower() not in corrected_q
            ]

        # Carry the still-invalid candidates so the post-loop counter
        # update reflects all rounds (not just the last one).
        invalid_carry.extend(invalid)

        # ── Task 5: planner-error MV recovery ──────────────────────
        # Before the adaptive overdraw short-circuit fires on
        # ``no_mv_measures``, mine planner errors from the round's
        # invalid candidates for ``MetricView `cat`.`sch`.`name``` /
        # bare-FQN shapes. If the planner has proven any asset is an
        # MV, stamp semantics with provenance ``planner_error`` and
        # rebuild ``mv_measures`` so the next loop iteration has a
        # chance to wrap measures correctly. This does not invent
        # measures — it only prevents a permanent zero-MV state when
        # ``DESCRIBE`` missed the metadata but the planner confirmed it.
        if not mv_measures and invalid:
            try:
                from genie_space_optimizer.common.asset_semantics import (
                    stamp_metric_views_from_planner_errors,
                )
                planner_errors = [
                    str(c.get("validation_error") or "")
                    for c in invalid
                    if str(c.get("validation_error") or "")
                ]
                stamped_mvs = stamp_metric_views_from_planner_errors(
                    config, planner_errors,
                )
                if stamped_mvs:
                    metric_view_names = effective_metric_view_identifiers_with_catalog(
                        config,
                    )
                    mv_measures = build_metric_view_measures(config)
                    logger.info(
                        "gvse planner-error MV recovery: stamped %d metric "
                        "view(s) from planner errors: %s",
                        len(stamped_mvs), sorted(stamped_mvs),
                    )
            except Exception:
                logger.debug(
                    "gvse planner-error MV recovery failed", exc_info=True,
                )

        # ── PR 21: adaptive overdraw short-circuit ──────────────────
        # When ``mv_measures`` is empty AND the dominant rejection
        # bucket from this round is ``mv_missing_measure_function``,
        # additional rounds are guaranteed to fail the same way:
        # without a measures map the auto-wrap rewriter is a no-op,
        # and Spark will keep rejecting the same SQL shape. Break out
        # to save 2/3 of the LLM budget and stamp a marker so the
        # banner can surface the failure mode. The check fires only
        # when there is still a deficit (otherwise the outer loop
        # already exits via ``deficit <= 0``).
        if not mv_measures and (target_count - len(valid)) > 0:
            round_invalid_reasons: dict[str, int] = {}
            for cand in invalid:
                reason = (
                    str(cand.get("validation_reason_code") or "").strip()
                    or "sql_compile_error"
                )
                round_invalid_reasons[reason] = (
                    round_invalid_reasons.get(reason, 0) + 1
                )
            if round_invalid_reasons:
                _dominant_reason, _dom_count = max(
                    round_invalid_reasons.items(),
                    key=lambda kv: (kv[1], kv[0]),
                )
                if _dominant_reason == "mv_missing_measure_function":
                    rejection_counters[
                        "adaptive_overdraw_short_circuited"
                    ] = "no_mv_measures"  # type: ignore[assignment]
                    logger.info(
                        "gvse adaptive overdraw short-circuited after "
                        "round %d/%d: mv_measures empty and dominant "
                        "rejection bucket is mv_missing_measure_function "
                        "(%d/%d candidates) — additional rounds cannot "
                        "recover without catalog-detected MV measures",
                        gen_round + 1, ADAPTIVE_OVERDRAW_MAX_ROUNDS,
                        _dom_count, sum(round_invalid_reasons.values()),
                    )
                    break

    rejection_counters["explain_or_execute"] += len(invalid_carry)
    rejection_counters["unfixable_after_correction"] = len(invalid_carry)

    # ── PR 18: split EXPLAIN/execute rejected into sub-buckets ───
    # Operators previously saw a single ``EXPLAIN/execute rejected:
    # N`` line in the unified banner with no breakdown. Bucket the
    # unfixable candidates by ``validation_reason_code`` and surface
    # the counts plus up to 3 example questions per bucket so the
    # log immediately points at the failure class (unknown column /
    # missing measure function / alias collision / metric view join /
    # syntax / etc.). The sub-bucket counters live alongside the
    # legacy ``explain_or_execute`` total so downstream consumers
    # that don't know about PR 18 keep working unchanged.
    sub_bucket_counts: dict[str, int] = {}
    sub_bucket_examples: dict[str, list[dict[str, str]]] = {}
    for cand in invalid_carry:
        reason = (
            str(cand.get("validation_reason_code") or "").strip()
            or "sql_compile_error"
        )
        sub_bucket_counts[reason] = sub_bucket_counts.get(reason, 0) + 1
        bucket_examples = sub_bucket_examples.setdefault(reason, [])
        if len(bucket_examples) < 3:
            err_short = str(cand.get("validation_error") or "")[:200]
            bucket_examples.append({
                "question": str(cand.get("question") or "")[:80],
                "error": err_short,
            })
    if sub_bucket_counts:
        rejection_counters["explain_or_execute_subbuckets"] = (
            sub_bucket_counts  # type: ignore[assignment]
        )
        rejection_counters["explain_or_execute_examples"] = (
            sub_bucket_examples  # type: ignore[assignment]
        )

    # ── 4. Arbiter approval (opt-in) ─────────────────────────────
    if run_arbiter and valid:
        try:
            from genie_space_optimizer.optimization.scorers.arbiter import (
                score_example_sql_correctness,
            )
        except Exception:
            logger.warning(
                "gvse: arbiter import failed; skipping arbiter approval",
                exc_info=True,
            )
            score_example_sql_correctness = None  # type: ignore
        arbitrated: list[dict] = []
        for cand in valid:
            if score_example_sql_correctness is None:
                arbitrated.append(cand)
                continue
            rows, capture_err_class, _capture_err_msg = _capture_result_rows(
                cand["expected_sql"], spark, catalog, schema,
                w=w, warehouse_id=warehouse_id,
            )
            # F12 — fail closed when row capture itself raised. The
            # arbiter LLM cannot make a meaningful judgment without
            # rows, and the previous code path silently passed
            # ``rows=None`` to the judge which then said "uncertain"
            # / "no" — masking row-capture failures as ``arbiter_no``.
            # Increment the differentiated counter so operators see
            # the real signal in the banner.
            if rows is None:
                if (
                    capture_err_class
                    == ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED
                ):
                    rejection_counters[
                        "arbiter_row_capture_subquery_unsupported"
                    ] += 1
                else:
                    rejection_counters[
                        "arbiter_row_capture_exec_failed"
                    ] += 1
                logger.info(
                    "gvse: arbiter row-capture failed (%s) — "
                    "dropping candidate: %s",
                    capture_err_class or "unknown",
                    cand.get("question", "")[:80],
                )
                continue
            try:
                verdict = score_example_sql_correctness(
                    question=cand["question"],
                    sql=cand["expected_sql"],
                    result_rows=rows,
                    w=w,
                    metadata_snapshot=config,
                )
            except Exception as exc:
                logger.warning(
                    "gvse: arbiter call failed for candidate (skipping): %s",
                    str(exc)[:200],
                )
                arbitrated.append(cand)  # fail-open — do not reject on infra error
                continue
            value = str((verdict or {}).get("value", "")).lower()
            if value == "yes":
                cand["arbiter_verdict"] = verdict
                arbitrated.append(cand)
            else:
                rejection_counters["arbiter_no"] += 1
                logger.info(
                    "gvse: arbiter verdict=%s dropped candidate: %s",
                    value or "uncertain",
                    cand.get("question", "")[:80],
                )
        valid = arbitrated

    # ── 5. Leakage firewall (opt-in) ─────────────────────────────
    if leakage_oracle is not None and valid:
        shielded: list[dict] = []
        for cand in valid:
            try:
                if leakage_oracle.contains_sql(
                    cand["expected_sql"], w=w,
                ):
                    rejection_counters["firewall_fingerprint"] += 1
                    continue
                if leakage_oracle.contains_question(cand["question"]):
                    rejection_counters["firewall_question_echo"] += 1
                    continue
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning(
                    "gvse: firewall oracle raised (fail-open): %s",
                    str(exc)[:200],
                )
            shielded.append(cand)
        valid = shielded

    # ── 6. Project to requested output fields ────────────────────
    if output_fields:
        fieldset = set(output_fields) | {
            "provenance", "validation_status", "validation_reason_code",
            "validation_error", "correction_source", "source",
            "arbiter_verdict",
        }
        valid = [
            {k: v for k, v in cand.items() if k in fieldset}
            for cand in valid
        ]

    return valid, rejection_counters


MAX_CORRECTION_ROUNDS = 2


def generate_example_sqls(
    w: WorkspaceClient,
    spark: SparkSession,
    *,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    warehouse_id: str = "",
    target_count: int | None = None,
    existing_example_sqls: list[dict] | None = None,
    leakage_oracle: "Any",  # LeakageOracle — REQUIRED kwarg (isolation invariant #1)
) -> tuple[list[dict], dict[str, int]]:
    """Generate validated example SQLs for ``instructions.example_question_sqls``.

    Thin adapter over :func:`generate_validated_sql_examples` that wires
    in the example-SQL prompts, stamps ``synthetic_example_sql``
    provenance, and enforces the Bug #4 isolation contract.

    Isolation invariants (see ``docs/example-sql-isolation.md``):

    1. No ``benchmarks`` parameter. This function CANNOT receive
       benchmark text — the lint rule at
       ``scripts/lint_example_sql_isolation.py`` fails CI if one is
       ever added. The only firewall input is ``leakage_oracle``
       which is an opaque match API, not raw text.
    2. The example prompts (loaded from ``common/config.py``) have
       zero benchmark-derived template variables. Enforced at import
       time by the assertion block at the bottom of ``config.py``.
    3. Every survivor passes the SQL fingerprint firewall via
       ``leakage_oracle.contains_sql``. Matches are dropped.
    4. Every survivor passes the question-echo firewall via
       ``leakage_oracle.contains_question``. Matches are dropped.

    Parameters
    ----------
    target_count
        Number of example_sqls to generate. Defaults to
        :data:`PREFLIGHT_EXAMPLE_SQL_TARGET`.
    existing_example_sqls
        Existing ``instructions.example_question_sqls`` on this
        space. Their questions are added to the ``## Already Covered
        Questions`` block so the LLM doesn't duplicate them, AND the
        caller typically wraps them into the ``leakage_oracle`` to
        firewall against near-duplicates.
    leakage_oracle
        **Required**. A :class:`~genie_space_optimizer.optimization.leakage.LeakageOracle`
        wrapping the benchmark corpus (and typically the existing-examples
        corpus too). Omitting this kwarg raises ``TypeError`` at call
        time — the machine-checkable form of isolation invariant #1.

    Returns
    -------
    (survivors, rejection_counters)
        ``survivors`` is the list of validated + firewalled example
        dicts (shape: ``question``, ``expected_sql``,
        ``usage_guidance``, provenance metadata). ``rejection_counters``
        is the full counter dict from the shared core — distinguishes
        metadata/MV/execute/arbiter/fingerprint/question-echo/dedup
        rejection classes for the pretty-summary block.
    """
    from genie_space_optimizer.common.config import (
        EXAMPLE_SQL_CORRECTION_PROMPT,
        EXAMPLE_SQL_GENERATION_PROMPT,
        PREFLIGHT_EXAMPLE_SQL_TARGET,
    )
    effective_target = (
        target_count if target_count is not None else PREFLIGHT_EXAMPLE_SQL_TARGET
    )
    existing_questions = [
        str((e or {}).get("question", "") or "")
        for e in (existing_example_sqls or [])
    ]
    return generate_validated_sql_examples(
        w=w, spark=spark,
        config=config, uc_columns=uc_columns, uc_tags=uc_tags,
        uc_routines=uc_routines, domain=domain,
        catalog=catalog, schema=schema, warehouse_id=warehouse_id,
        target_count=effective_target,
        generation_prompt_template=EXAMPLE_SQL_GENERATION_PROMPT,
        correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
        generation_prompt_registry_key="example_sql_generation",
        correction_prompt_registry_key="example_sql_correction",
        existing_questions=existing_questions,
        leakage_oracle=leakage_oracle,
        run_arbiter=True,
        provenance="synthetic_example_sql",
        output_fields=("question", "expected_sql", "usage_guidance"),
    )

_SQL_REFERENCE_PATTERN = re.compile(
    r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
    r"(`[^`]+`\.`[^`]+`\.`[^`]+`"
    r"|[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*)",
    re.IGNORECASE,
)

_SQL_FQ_ROUTINE_CALL_PATTERN = re.compile(
    r"(?<![\w`])"
    r"(`[^`]+`|[A-Za-z_]\w*)\s*\.\s*"
    r"(`[^`]+`|[A-Za-z_]\w*)\s*\.\s*"
    r"(`[^`]+`|[A-Za-z_]\w*)\s*\(",
    re.IGNORECASE,
)


def _clean_sql_identifier_part(value: str) -> str:
    return (value or "").strip().strip("`").lower()


def _extract_fully_qualified_routine_calls(sql: str) -> set[str]:
    """Return fully qualified routine calls like ``catalog.schema.name(``.

    The extractor is intentionally catalog/schema-independent. Benchmark
    provenance must not depend on the optimizer's current SQL context because
    the failing 7now case used a valid physical UC routine that was not a
    Genie Space asset.
    """
    calls: set[str] = set()
    for match in _SQL_FQ_ROUTINE_CALL_PATTERN.finditer(sql or ""):
        catalog = _clean_sql_identifier_part(match.group(1))
        schema = _clean_sql_identifier_part(match.group(2))
        name = _clean_sql_identifier_part(match.group(3))
        if catalog and schema and name:
            calls.add(f"{catalog}.{schema}.{name}")
    return calls


def _benchmark_space_routine_violations(sql: str, config: dict) -> list[str]:
    """Return fully-qualified routine calls not registered in the Genie Space."""
    calls = _extract_fully_qualified_routine_calls(sql)
    if not calls:
        return []
    allowed = _space_function_candidates(config)
    violations: list[str] = []
    for call in sorted(calls):
        if not (_identifier_candidates(call) & allowed):
            violations.append(call)
    return violations


def _mark_function_not_in_space_if_needed(candidate: dict, config: dict) -> bool:
    """Mark a benchmark candidate invalid when SQL calls unregistered routines.

    Returns ``True`` when the candidate was mutated (i.e. a routine the
    Genie Space does not own was found). The candidate's
    ``validation_status``, ``validation_reason_code``, ``validation_error``,
    and ``quarantine_reason_*`` keys are stamped so downstream consumers
    treat it as a quarantined invalid benchmark rather than a Genie failure.
    """
    violations = _benchmark_space_routine_violations(
        str(candidate.get("expected_sql") or ""),
        config,
    )
    if not violations:
        return False

    message = (
        "Benchmark SQL references routine(s) that exist in UC but are not "
        f"registered in this Genie Space: {violations[:5]}"
    )
    candidate["validation_status"] = "invalid"
    candidate["validation_reason_code"] = "function_not_in_space"
    candidate["validation_error"] = message
    candidate["quarantine_reason_code"] = "function_not_in_space"
    candidate["quarantine_reason_detail"] = message
    candidate["unregistered_routines"] = violations
    return True


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", (value or "").lower())


def _identifier_candidates(value: str) -> set[str]:
    cleaned = (value or "").replace("`", "").strip().lower()
    if not cleaned:
        return set()
    parts = [p for p in cleaned.split(".") if p]
    candidates = {cleaned}
    if parts:
        candidates.add(parts[-1])
    if len(parts) >= 2:
        candidates.add(".".join(parts[-2:]))
    return candidates


def _build_metadata_allowlist(
    *,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
) -> dict[str, Any]:
    allowed_assets: set[str] = set()
    allowed_columns: set[str] = set()
    normalized_to_column: dict[str, str] = {}
    allowed_routines: set[str] = set()

    for key in ("_tables", "_metric_views", "_functions"):
        for raw in config.get(key, []) if isinstance(config.get(key), list) else []:
            if not raw:
                continue
            allowed_assets.update(_identifier_candidates(str(raw)))

    scoped_columns = _filter_uc_columns_to_space_assets(config, uc_columns)
    scoped_routines = _filter_uc_routines_to_space_functions(config, uc_routines)

    for col in scoped_columns:
        if not isinstance(col, dict):
            continue
        col_name = str(col.get("column_name") or "").strip()
        table_name = str(col.get("table_name") or "").strip()
        if col_name:
            allowed_columns.add(col_name.lower())
            normalized_to_column.setdefault(_normalize_name(col_name), col_name)
        if table_name and col_name:
            fq_col = f"{table_name}.{col_name}".lower()
            allowed_columns.add(fq_col)
            normalized_to_column.setdefault(_normalize_name(fq_col), f"{table_name}.{col_name}")

    for routine in scoped_routines:
        if not isinstance(routine, dict):
            continue
        raw_name = str(
            routine.get("routine_name")
            or routine.get("specific_name")
            or ""
        ).strip()
        if not raw_name:
            continue
        allowed_routines.update(_identifier_candidates(raw_name))

    for fn in config.get("_functions", []) if isinstance(config.get("_functions"), list) else []:
        allowed_routines.update(_identifier_candidates(str(fn)))

    return {
        "assets": allowed_assets,
        "columns": allowed_columns,
        "column_index": normalized_to_column,
        "routines": allowed_routines,
    }


def _extract_sql_asset_references(sql: str) -> set[str]:
    refs: set[str] = set()
    text = sql or ""
    for match in _SQL_REFERENCE_PATTERN.finditer(text):
        # Skip TVF-style references — anything immediately followed by an
        # opening paren is a function call. Routine validation handles those
        # via _extract_sql_function_calls / allowlist["routines"], so we
        # don't want to double-count them as unknown assets.
        end = match.end()
        if end < len(text) and text[end] == "(":
            continue
        refs.update(_identifier_candidates(match.group(1)))
    return refs


_JOIN_TABLE_RE = re.compile(
    r"\bFROM\s+[`\"]?(\w+(?:\.\w+)*)[`\"]?"
    r"|\bJOIN\s+[`\"]?(\w+(?:\.\w+)*)[`\"]?",
    re.IGNORECASE,
)


def _extract_join_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract normalized ``(table_a, table_b)`` pairs from JOIN clauses."""
    refs = [
        (m.group(1) or m.group(2)).replace("`", "").split(".")[-1].lower()
        for m in _JOIN_TABLE_RE.finditer(sql)
    ]
    pairs: set[tuple[str, str]] = set()
    for i in range(1, len(refs)):
        a, b = sorted([refs[0], refs[i]])
        pairs.add((a, b))
    return pairs


def _compute_asset_coverage(
    benchmarks: list[dict],
    config: dict,
) -> dict[str, Any]:
    """Identify which Genie Space assets have/lack benchmark coverage.

    Collects covered assets from ``required_tables`` and ``expected_sql``
    SQL references across all benchmarks, then diffs against the full asset
    list from the Genie Space config.

    Returns a dict with ``covered``, ``uncovered_tables``,
    ``uncovered_mvs``, ``uncovered_functions``, and ``uncovered_joins``
    sets (leaf-name normalised).
    """
    covered: set[str] = set()
    covered_join_pairs: set[tuple[str, str]] = set()
    for b in benchmarks:
        for tbl in b.get("required_tables", []):
            covered.update(_identifier_candidates(str(tbl)))
        sql = str(b.get("expected_sql") or "")
        if sql:
            covered.update(_extract_sql_asset_references(sql))
            covered_join_pairs.update(_extract_join_pairs(sql))

    def _leaf(name: str) -> str:
        parts = name.replace("`", "").strip().split(".")
        return parts[-1].lower() if parts else ""

    all_tables = {_leaf(t) for t in config.get("_tables", []) if t}
    all_mvs = {_leaf(m) for m in config.get("_metric_views", []) if m}
    all_functions = {_leaf(f) for f in config.get("_functions", []) if f}

    covered_leaves = {_leaf(c) for c in covered if c}

    # Configured join pairs from Genie Space join specs
    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}
    _inst = parsed_space.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    _ds = parsed_space.get("data_sources", {})
    if not isinstance(_ds, dict):
        _ds = {}
    join_specs = (
        _inst.get("join_specs", []) if isinstance(_inst.get("join_specs"), list) else []
    ) or (
        _ds.get("join_specs", []) if isinstance(_ds.get("join_specs"), list) else []
    )
    configured_join_pairs: set[tuple[str, str]] = set()
    for js in join_specs:
        l_name = _leaf(js.get("left", {}).get("identifier", ""))
        r_name = _leaf(js.get("right", {}).get("identifier", ""))
        if l_name and r_name:
            pair: tuple[str, str] = (min(l_name, r_name), max(l_name, r_name))
            configured_join_pairs.add(pair)

    return {
        "covered": covered_leaves,
        "uncovered_tables": all_tables - covered_leaves,
        "uncovered_mvs": all_mvs - covered_leaves,
        "uncovered_functions": all_functions - covered_leaves,
        "uncovered_joins": configured_join_pairs - covered_join_pairs,
    }


def _fill_coverage_gaps(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: "SparkSession",
    allowlist: dict[str, Any],
    domain: str,
    existing_questions: set[str],
    category_performance: dict[str, dict] | None = None,
    *,
    warehouse_id: str = "",
    target_benchmark_count: int = TARGET_BENCHMARK_COUNT,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> list[dict]:
    """Generate targeted benchmarks for Genie Space assets with zero coverage.

    Runs after the main generation pipeline. Identifies uncovered assets via
    ``_compute_asset_coverage``, then makes a single LLM call asking for 1-2
    questions per uncovered asset.  Results go through the same metadata
    constraint and SQL validation pipeline as normal benchmarks.

    When *category_performance* is provided, categories performing below the
    median accuracy are highlighted in the prompt so the LLM prioritises
    generating questions for weak areas.

    Returns only validated gap-fill benchmarks (may be empty).
    """
    soft_cap = min(
        int(target_benchmark_count * COVERAGE_GAP_SOFT_CAP_FACTOR),
        max_benchmark_count,
    )
    if len(benchmarks) >= soft_cap:
        logger.info(
            "Skipping coverage gap-fill: benchmark count %d already at soft cap %d",
            len(benchmarks), soft_cap,
        )
        return []

    coverage = _compute_asset_coverage(benchmarks, config)
    uncovered_tables = coverage["uncovered_tables"]
    uncovered_mvs = coverage["uncovered_mvs"]
    uncovered_functions = coverage["uncovered_functions"]
    uncovered_joins: set[tuple[str, str]] = coverage.get("uncovered_joins", set())

    if not uncovered_tables and not uncovered_mvs and not uncovered_functions and not uncovered_joins:
        logger.info("All Genie Space assets and join paths already covered by benchmarks")
        return []

    # Prioritise MVs and TVFs (higher routing-issue risk), then tables, then joins.
    budget = soft_cap - len(benchmarks)
    ordered_uncovered: list[str] = []
    for mv in sorted(uncovered_mvs):
        ordered_uncovered.append(f"METRIC VIEW: {mv}")
    for fn in sorted(uncovered_functions):
        ordered_uncovered.append(f"FUNCTION: {fn}")
    for tbl in sorted(uncovered_tables):
        ordered_uncovered.append(f"TABLE: {tbl}")
    for left, right in sorted(uncovered_joins):
        ordered_uncovered.append(f"JOIN PATH: {left} <-> {right}")

    # Each uncovered asset targets ~2 questions; trim to budget.
    max_assets = max(budget // 2, 1)
    targeted = ordered_uncovered[:max_assets]

    logger.info(
        "Coverage gap-fill: %d uncovered items (%d tables, %d MVs, %d functions, %d join paths). "
        "Targeting %d within budget of %d.",
        len(ordered_uncovered), len(uncovered_tables),
        len(uncovered_mvs), len(uncovered_functions), len(uncovered_joins),
        len(targeted), budget,
    )

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    existing_q_lines = "\n".join(f"- {q}" for q in sorted(existing_questions)) or "(none)"
    uncovered_lines = "\n".join(f"- {a}" for a in targeted)

    weak_categories_context = ""
    if category_performance:
        accuracies = []
        for cat, stats in category_performance.items():
            if cat == "unknown" or stats.get("total", 0) == 0:
                continue
            accuracies.append(stats["correct"] / stats["total"])
        if accuracies:
            median_acc = sorted(accuracies)[len(accuracies) // 2]
            weak_lines = []
            for cat, stats in sorted(category_performance.items()):
                total = stats.get("total", 0)
                if total == 0 or cat == "unknown":
                    continue
                acc = stats["correct"] / total
                if acc < median_acc:
                    weak_lines.append(
                        f"- {cat}: {stats['correct']}/{total} correct ({acc:.0%})"
                    )
            if weak_lines:
                weak_categories_context = (
                    "## Weak Categories (prioritize these)\n"
                    + "\n".join(weak_lines)
                )

    prompt = format_mlflow_template(
        BENCHMARK_COVERAGE_GAP_PROMPT,
        domain=domain,
        categories=json.dumps(BENCHMARK_CATEGORIES),
        uncovered_assets=uncovered_lines,
        existing_questions=existing_q_lines,
        weak_categories_context=weak_categories_context,
        **ctx,
    )

    try:
        response = _call_llm_for_scoring(
            w, prompt,
            prompt_name=get_registered_prompt_name("benchmark_coverage_gap"),
        )
        raw: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning("Coverage gap-fill LLM call failed", exc_info=True)
        return []

    valid: list[dict] = []
    for b in raw:
        if not isinstance(b, dict):
            continue
        expected_sql = str(b.get("expected_sql", "") or "")
        if not expected_sql:
            continue
        q_lower = str(b.get("question", "") or "").lower().strip()
        if q_lower in existing_questions:
            continue

        required_tables = b.get("required_tables", [])
        if not isinstance(required_tables, list):
            required_tables = []
        required_columns = b.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
        expected_facts = b.get("expected_facts", [])
        if not isinstance(expected_facts, list):
            expected_facts = []

        benchmark: dict[str, Any] = {
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"),
                expected_sql,
                hint=b.get("expected_asset_hint"),
            ),
            "category": b.get("category", ""),
            "required_tables": [str(t) for t in required_tables],
            "required_columns": [str(c) for c in required_columns],
            "expected_facts": [str(f) for f in expected_facts],
            "source": "llm_generated",
            "provenance": "coverage_gap_fill",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": None,
            "correction_source": "",
        }

        metadata_ok, _reason_code, _reason_msg = _enforce_metadata_constraints(
            benchmark=benchmark,
            sql=expected_sql,
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            logger.debug(
                "Gap-fill benchmark failed metadata constraints: %s",
                str(benchmark.get("question", ""))[:60],
            )
            continue

        _mv_names = effective_metric_view_identifiers_with_catalog(config)
        _is_star_ok, _ = _guard_mv_select_star(expected_sql, _mv_names)
        if not _is_star_ok:
            continue

        _mv_measures = build_metric_view_measures(config)
        if _mv_measures:
            expected_sql = _rewrite_measure_refs(expected_sql, _mv_measures)
            benchmark["expected_sql"] = expected_sql

        is_valid, err = _validate_benchmark_sql(
            expected_sql, spark, catalog, schema,
            w=w, warehouse_id=warehouse_id,
        )
        if is_valid:
            valid.append(benchmark)
        else:
            logger.debug(
                "Gap-fill benchmark failed SQL validation: %s — %s",
                str(benchmark.get("question", ""))[:60], err,
            )

    logger.info(
        "Coverage gap-fill complete: %d valid out of %d generated for %d uncovered assets",
        len(valid), len(raw), len(targeted),
    )
    return valid


def _suggest_column_name(column: str, allowed_index: dict[str, str]) -> str | None:
    if not column:
        return None
    normalized = _normalize_name(column)
    if not normalized:
        return None
    exact = allowed_index.get(normalized)
    if exact:
        return exact
    candidates = list(allowed_index.keys())
    if not candidates:
        return None
    closest = get_close_matches(normalized, candidates, n=1, cutoff=0.72)
    if not closest:
        return None
    return allowed_index.get(closest[0])


def _apply_metadata_field_drift_corrections(
    *,
    sql: str,
    required_columns: list[str],
    allowed_index: dict[str, str],
) -> tuple[str, list[dict[str, str]]]:
    corrected_sql = sql
    applied: list[dict[str, str]] = []
    seen: set[str] = set()

    for col in required_columns:
        token = str(col or "").strip()
        if not token:
            continue
        col_leaf = token.split(".")[-1]
        if not col_leaf:
            continue
        key = col_leaf.lower()
        if key in seen:
            continue
        seen.add(key)

        suggestion = _suggest_column_name(col_leaf, allowed_index)
        if not suggestion:
            continue
        suggestion_leaf = suggestion.split(".")[-1]
        if suggestion_leaf.lower() == col_leaf.lower():
            continue

        pattern = re.compile(rf"(?i)\b{re.escape(col_leaf)}\b")
        updated_sql, count = pattern.subn(suggestion_leaf, corrected_sql)
        if count > 0:
            corrected_sql = updated_sql
            applied.append(
                {
                    "from": col_leaf,
                    "to": suggestion_leaf,
                    "reason": "metadata_field_drift",
                }
            )

    return corrected_sql, applied


def _enforce_metadata_constraints(
    *,
    benchmark: dict,
    sql: str,
    allowlist: dict[str, Any],
    catalog: str,
    schema: str,
) -> tuple[bool, str, str]:
    refs = _extract_sql_asset_references(sql)
    unknown_refs = sorted(ref for ref in refs if ref not in allowlist["assets"])
    if unknown_refs:
        return (
            False,
            "unknown_asset",
            f"SQL references assets not found in metadata: {unknown_refs[:5]}",
        )

    required_tables = benchmark.get("required_tables", [])
    if isinstance(required_tables, list):
        bad_required_tables: list[str] = []
        for item in required_tables:
            candidates = _identifier_candidates(str(item))
            if candidates and not any(c in allowlist["assets"] for c in candidates):
                bad_required_tables.append(str(item))
        if bad_required_tables:
            return (
                False,
                "unknown_asset",
                f"required_tables contains unknown assets: {bad_required_tables[:5]}",
            )

    required_columns = benchmark.get("required_columns", [])
    if isinstance(required_columns, list):
        bad_columns: list[str] = []
        for col in required_columns:
            raw = str(col or "").strip()
            if not raw:
                continue
            col_candidates = _identifier_candidates(raw)
            if any(c in allowlist["columns"] for c in col_candidates):
                continue
            leaf = raw.split(".")[-1].lower()
            if leaf in allowlist["columns"]:
                continue
            bad_columns.append(raw)
        if bad_columns:
            return (
                False,
                "unknown_column",
                f"required_columns contains unknown metadata fields: {bad_columns[:8]}",
            )

    called_functions = _extract_sql_function_calls(sql, catalog, schema)
    unknown_functions = sorted(fn for fn in called_functions if fn not in allowlist["routines"])
    if unknown_functions:
        return (
            False,
            "unknown_routine",
            f"SQL references routines not found in metadata: {unknown_functions[:5]}",
        )

    return True, "ok", ""


def _generate_sql_for_curated_questions(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    question_only_benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    *,
    warehouse_id: str = "",
) -> list[dict]:
    """Generate and validate expected SQL for curated questions that lack it.

    Uses the same LLM + validation pipeline as synthetic benchmark generation.
    Questions that fail SQL generation after retries are dropped.

    Returns only benchmarks that ended up with valid ``expected_sql``.
    """
    if not question_only_benchmarks:
        return []

    from genie_space_optimizer.common.config import (
        CURATED_SQL_GENERATION_PROMPT,
        CURATED_SQL_GENERATION_MAX_RETRIES,
        format_mlflow_template,
    )
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    questions_json = json.dumps(
        [{"question": b["question"]} for b in question_only_benchmarks],
        indent=2,
    )

    prompt = format_mlflow_template(
        CURATED_SQL_GENERATION_PROMPT,
        valid_assets_context=ctx["valid_assets_context"],
        tables_context=ctx["tables_context"],
        column_allowlist=ctx.get("column_allowlist", "(no columns)"),
        metric_views_context=ctx.get("metric_views_context", "None"),
        tvfs_context=ctx.get("tvfs_context", "None"),
        join_specs_context=ctx.get("join_specs_context", "None"),
        instructions_context=ctx.get("instructions_context", "None"),
        data_profile_context=ctx.get("data_profile_context", "(no data profile available)"),
        questions_json=questions_json,
    )

    try:
        response = _call_llm_for_scoring(
            w, prompt,
            prompt_name=get_registered_prompt_name("curated_sql_generation"),
        )
        generated: list[dict] = (
            response if isinstance(response, list) else response.get("benchmarks", [])
        )
    except Exception:
        logger.warning("Curated SQL generation LLM call failed", exc_info=True)
        return []

    question_map = {b["question"].strip().lower(): b for b in question_only_benchmarks}
    enriched: list[dict] = []

    for g in generated:
        if not isinstance(g, dict):
            continue
        sql = g.get("expected_sql")
        question = str(g.get("question", "")).strip()
        if not sql or g.get("unfixable_reason"):
            logger.info(
                "Curated SQL generation: unfixable '%s' — %s",
                question[:60],
                g.get("unfixable_reason", "no SQL generated"),
            )
            continue

        is_valid, err = validate_ground_truth_sql(
            sql, spark, catalog=catalog, gold_schema=schema,
            w=w, warehouse_id=warehouse_id,
        )
        if not is_valid:
            for _retry in range(CURATED_SQL_GENERATION_MAX_RETRIES):
                corrections = _attempt_benchmark_correction(
                    w, config, uc_columns, uc_routines,
                    [{"question": question, "expected_sql": sql, "validation_error": err}],
                    catalog, schema, spark,
                    _build_metadata_allowlist(config=config, uc_columns=uc_columns, uc_routines=uc_routines),
                    warehouse_id=warehouse_id,
                )
                if corrections:
                    g = corrections[0]
                    sql = g.get("expected_sql", "")
                    is_valid = bool(sql)
                    break
                logger.info(
                    "Curated SQL correction attempt %d failed for '%s'",
                    _retry + 1, question[:60],
                )

        if is_valid and sql:
            original = question_map.get(question.lower(), {})
            enriched.append({
                **original,
                "question": question,
                "expected_sql": sql,
                "expected_asset": g.get("expected_asset", detect_asset_type(sql)),
                "category": g.get("category", original.get("category", "curated")),
                "required_tables": g.get("required_tables", []),
                "required_columns": g.get("required_columns", []),
                "expected_facts": g.get("expected_facts", []),
                "source": "genie_space",
                "provenance": "curated_sql_generated",
                "validation_status": "valid",
                "validation_reason_code": "ok",
                "validation_error": None,
                "correction_source": "curated_sql_generation",
            })
        else:
            logger.warning(
                "Dropping curated question (no valid SQL after retries): %s",
                question[:80],
            )

    logger.info(
        "Curated SQL generation: %d/%d questions got valid SQL",
        len(enriched), len(question_only_benchmarks),
    )

    _data_profile = config.get("_data_profile", {})
    if _data_profile and enriched:
        try:
            from genie_space_optimizer.optimization.benchmarks import (
                validate_predicate_values,
            )
            _pred_results = validate_predicate_values(enriched, _data_profile)
            for _eb, _pr in zip(enriched, _pred_results):
                if not _pr["valid"]:
                    for mm in _pr["mismatches"]:
                        if mm.get("suggestion"):
                            old_sql = _eb.get("expected_sql", "")
                            new_sql = old_sql.replace(
                                f"'{mm['literal']}'", f"'{mm['suggestion']}'",
                            )
                            if new_sql != old_sql:
                                _eb["expected_sql"] = new_sql
                                _eb["correction_source"] = "predicate_value_fix"
                                logger.info(
                                    "Curated SQL auto-corrected predicate: "
                                    "%s='%s' → '%s' in '%s'",
                                    mm["column"], mm["literal"],
                                    mm["suggestion"], _eb["question"][:60],
                                )
        except Exception as exc:
            logger.warning("Predicate value post-check skipped: %s", exc)

    return enriched


def _enforce_instruction_default_filters_on_benchmarks(
    benchmarks: list[dict],
    config: dict,
) -> int:
    """Ensure benchmarks include instruction-mandated default filters in their SQL.

    Reads default filter rules from the Genie Space instructions and checks
    each benchmark's ``expected_sql``. If a benchmark's SQL is missing a
    mandated filter, appends it to the WHERE clause.

    Returns the count of benchmarks patched.
    """
    try:
        from genie_space_optimizer.optimization.optimizer import (
            _extract_instruction_default_filters,
        )
    except ImportError:
        return 0

    parsed_space = config.get("_parsed_space", config)
    default_filters = _extract_instruction_default_filters(parsed_space)
    if not default_filters:
        return 0

    patched = 0
    for b in benchmarks:
        sql = b.get("expected_sql", "")
        if not sql or not sql.strip():
            continue
        sql_lower = sql.lower()
        for df in default_filters:
            col = df["column"]
            val = df["value"]
            if col.lower() in sql_lower:
                continue
            if "where" in sql_lower:
                sql = re.sub(
                    r"(?i)\bWHERE\b",
                    f"WHERE {col} = '{val}' AND",
                    sql,
                    count=1,
                )
            else:
                group_match = re.search(r"(?i)\b(GROUP\s+BY|ORDER\s+BY|LIMIT)\b", sql)
                if group_match:
                    pos = group_match.start()
                    sql = sql[:pos] + f"WHERE {col} = '{val}' " + sql[pos:]
                else:
                    sql = sql.rstrip().rstrip(";") + f" WHERE {col} = '{val}'"
            b["expected_sql"] = sql
            b["_instruction_filter_patched"] = True
            patched += 1
            logger.info(
                "Added instruction-mandated filter '%s=%s' to benchmark: %s",
                col, val, b.get("question", "")[:80],
            )
    return patched


def _compute_synthetic_target(
    *,
    target_count: int,
    curated_count: int,
    existing_count: int,
) -> int:
    """Return how many synthetic benchmarks are needed to reach target_count."""
    return max(target_count - curated_count - existing_count, 0)


def _needs_benchmark_top_up(benchmarks: list[dict]) -> bool:
    from genie_space_optimizer.common.config import (
        MIN_HELD_OUT_BENCHMARK_COUNT,
        MIN_TRAIN_BENCHMARK_COUNT,
    )

    train_n = sum(1 for b in benchmarks if b.get("split") == "train")
    held_out_n = sum(1 for b in benchmarks if b.get("split") == "held_out")
    return train_n < MIN_TRAIN_BENCHMARK_COUNT or held_out_n < MIN_HELD_OUT_BENCHMARK_COUNT


def generate_benchmarks(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    spark: SparkSession,
    target_count: int = TARGET_BENCHMARK_COUNT,
    genie_space_benchmarks: list[dict] | None = None,
    existing_benchmarks: list[dict] | None = None,
    warehouse_id: str = "",
    *,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> list[dict]:
    """Generate benchmark questions via LLM from Genie Space context.

    Pipeline:
      1. Start with curated Genie space benchmarks (if provided)
      2. Calculate how many synthetic benchmarks to generate to reach target
      3. Build schema context from actual Genie Space assets + UC metadata
      4. Call LLM with BENCHMARK_GENERATION_PROMPT (includes valid asset allowlist)
      5. Enforce strict metadata constraints (assets/routines/required fields)
      6. Run deterministic metadata drift auto-correction (field suggestions)
      7. Validate each expected_sql via EXPLAIN + table existence check
      8. Send remaining invalid benchmarks to correction LLM (bounded retries)
      9. Persist provenance + validation metadata per benchmark record

    Args:
        existing_benchmarks: Previously validated benchmarks to keep. When
            provided, these are carried forward and the generation targets
            only the gap (``target_count - len(existing_benchmarks)``).
    """
    curated = genie_space_benchmarks or []
    _existing = existing_benchmarks or []
    curated_questions = {b.get("question", "").lower().strip() for b in curated}
    existing_questions = {b.get("question", "").lower().strip() for b in _existing}
    curated_questions |= existing_questions
    synthetic_target = _compute_synthetic_target(
        target_count=min(target_count, max_benchmark_count),
        curated_count=len(curated),
        existing_count=len(_existing),
    )
    allowlist = _build_metadata_allowlist(
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
    )

    if curated:
        logger.info(
            "Starting with %d curated Genie space benchmarks (%d with SQL). "
            "Generating %d synthetic to reach target of %d.",
            len(curated),
            sum(1 for b in curated if b.get("expected_sql")),
            synthetic_target,
            target_count,
        )

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    all_existing = list(curated) + list(_existing)
    existing_questions_context = ""
    if all_existing:
        existing_questions_context = (
            "\n\n## Already Covered Questions (do NOT duplicate these)\n"
            + "\n".join(f"- {b.get('question', '')}" for b in all_existing)
        )

    if synthetic_target > 0:
        prompt = format_mlflow_template(
            BENCHMARK_GENERATION_PROMPT,
            domain=domain,
            target_count=synthetic_target,
            categories=json.dumps(BENCHMARK_CATEGORIES),
            **ctx,
        )
        if existing_questions_context:
            prompt += existing_questions_context

        with mlflow.start_span(
            name="benchmark_generation", span_type=SpanType.CHAIN,
        ) as _bench_span:
            try:
                _bench_span.set_inputs({
                    "domain": domain,
                    "prompt_name": get_registered_prompt_name("benchmark_generation"),
                })
            except Exception:
                pass
            response = _call_llm_for_scoring(
                w, prompt,
                prompt_name=get_registered_prompt_name("benchmark_generation"),
            )
            try:
                _bench_span.set_outputs({
                    "raw_benchmark_count": (
                        len(response) if isinstance(response, list)
                        else len(response.get("benchmarks", []))
                    ),
                })
            except Exception:
                pass
        raw_benchmarks: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    else:
        logger.info(
            "Skipping synthetic benchmark generation: target met by curated/existing rows "
            "(curated=%d, existing=%d, target=%d, max=%d)",
            len(curated), len(_existing), target_count, max_benchmark_count,
        )
        raw_benchmarks = []

    valid_benchmarks: list[dict] = []
    invalid_benchmarks: list[dict] = []
    accepted_questions: set[str] = set()

    def _register_valid(candidate: dict) -> None:
        question = str(candidate.get("question") or "").strip().lower()
        if not question or question in accepted_questions or question in curated_questions:
            return
        accepted_questions.add(question)
        valid_benchmarks.append(candidate)

    for b in raw_benchmarks:
        if not isinstance(b, dict):
            continue
        expected_sql = str(b.get("expected_sql", "") or "")
        if not expected_sql:
            continue
        q_lower = str(b.get("question", "") or "").lower().strip()
        if q_lower in curated_questions:
            logger.debug("Skipping synthetic duplicate of curated question: %s", q_lower[:50])
            continue

        required_tables = b.get("required_tables", [])
        if not isinstance(required_tables, list):
            required_tables = []
        required_columns = b.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
        expected_facts = b.get("expected_facts", [])
        if not isinstance(expected_facts, list):
            expected_facts = []

        benchmark: dict[str, Any] = {
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"),
                expected_sql,
                hint=b.get("expected_asset_hint"),
            ),
            "category": b.get("category", ""),
            "required_tables": [str(t) for t in required_tables],
            "required_columns": [str(c) for c in required_columns],
            "expected_facts": [str(f) for f in expected_facts],
            "source": "llm_generated",
            "provenance": "synthetic",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": None,
            "correction_source": "",
        }

        # Task 2 — quarantine benchmarks whose SQL calls a routine that is
        # physically resolvable in UC but not registered in this Genie
        # Space's ``data_sources.functions``. Genie cannot see those
        # functions at runtime, so the benchmark would otherwise produce a
        # misleading judge failure that the lever loop would chase.
        if _mark_function_not_in_space_if_needed(benchmark, config):
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark quarantined: function_not_in_space: %s — %s",
                str(benchmark.get("question", ""))[:60],
                benchmark.get("validation_error", ""),
            )
            continue

        metadata_ok, reason_code, reason_message = _enforce_metadata_constraints(
            benchmark=benchmark,
            sql=expected_sql,
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            # Deterministic correction for common field drift before LLM-based correction.
            if reason_code == "unknown_column":
                corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                    sql=expected_sql,
                    required_columns=[str(c) for c in benchmark.get("required_columns", [])],
                    allowed_index=allowlist["column_index"],
                )
                if replacements and corrected_sql != expected_sql:
                    candidate = dict(benchmark)
                    candidate["expected_sql"] = corrected_sql
                    candidate["provenance"] = "auto_corrected"
                    candidate["correction_source"] = "metadata_suggestion"
                    candidate["field_drift_fixes"] = replacements
                    candidate_ok, _, candidate_msg = _enforce_metadata_constraints(
                        benchmark=candidate,
                        sql=corrected_sql,
                        allowlist=allowlist,
                        catalog=catalog,
                        schema=schema,
                    )
                    if candidate_ok:
                        is_candidate_valid, candidate_err = _validate_benchmark_sql(
                            corrected_sql, spark, catalog, schema,
                            w=w, warehouse_id=warehouse_id,
                        )
                        if is_candidate_valid:
                            candidate["validation_status"] = "valid"
                            candidate["validation_reason_code"] = "ok"
                            candidate["validation_error"] = None
                            _register_valid(candidate)
                            continue
                        reason_message = candidate_err
                    else:
                        reason_message = candidate_msg

            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = reason_code
            benchmark["validation_error"] = reason_message
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark failed metadata constraints: %s — %s",
                str(benchmark.get("question", ""))[:60],
                reason_message,
            )
            continue

        # MV guard: reject SELECT * on metric views (PR 14: effective MVs).
        _mv_names = effective_metric_view_identifiers_with_catalog(config)
        _is_star_ok, _star_reason = _guard_mv_select_star(expected_sql, _mv_names)
        if not _is_star_ok:
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = "mv_select_star"
            benchmark["validation_error"] = _star_reason
            invalid_benchmarks.append(benchmark)
            continue

        # MV auto-fix: wrap bare measures in MEASURE()
        _mv_measures = build_metric_view_measures(config)
        if _mv_measures:
            expected_sql = _rewrite_measure_refs(expected_sql, _mv_measures)
            benchmark["expected_sql"] = expected_sql

        is_valid, err = _validate_benchmark_sql(
            expected_sql, spark, catalog, schema, execute=True,
            w=w, warehouse_id=warehouse_id,
        )
        if is_valid:
            benchmark["validation_status"] = "valid"
            benchmark["validation_reason_code"] = "ok"
            benchmark["validation_error"] = None
            _register_valid(benchmark)
        else:
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = _classify_sql_validation_error(err)
            benchmark["validation_error"] = err
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark failed validation: %s — %s",
                str(benchmark.get("question", ""))[:60], err,
            )

    for correction_round in range(MAX_CORRECTION_ROUNDS):
        if not invalid_benchmarks:
            break
        logger.info(
            "Correction round %d: attempting to fix %d invalid benchmarks",
            correction_round + 1, len(invalid_benchmarks),
        )
        metadata_corrected: list[dict] = []
        still_invalid: list[dict] = []
        for invalid in invalid_benchmarks:
            expected_sql = str(invalid.get("expected_sql") or "")
            if not expected_sql:
                still_invalid.append(invalid)
                continue
            corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                sql=expected_sql,
                required_columns=[str(c) for c in invalid.get("required_columns", [])],
                allowed_index=allowlist["column_index"],
            )
            if not replacements or corrected_sql == expected_sql:
                still_invalid.append(invalid)
                continue
            candidate = dict(invalid)
            candidate["expected_sql"] = corrected_sql
            candidate["field_drift_fixes"] = replacements
            candidate["provenance"] = "auto_corrected"
            candidate["correction_source"] = "metadata_suggestion_loop"
            candidate_ok, candidate_reason, candidate_message = _enforce_metadata_constraints(
                benchmark=candidate,
                sql=corrected_sql,
                allowlist=allowlist,
                catalog=catalog,
                schema=schema,
            )
            if not candidate_ok:
                candidate["validation_status"] = "invalid"
                candidate["validation_reason_code"] = candidate_reason
                candidate["validation_error"] = candidate_message
                still_invalid.append(candidate)
                continue
            candidate_valid, candidate_err = _validate_benchmark_sql(
                corrected_sql, spark, catalog, schema,
                w=w, warehouse_id=warehouse_id,
            )
            if candidate_valid:
                candidate["validation_status"] = "valid"
                candidate["validation_reason_code"] = "ok"
                candidate["validation_error"] = None
                metadata_corrected.append(candidate)
                continue
            candidate["validation_status"] = "invalid"
            candidate["validation_reason_code"] = _classify_sql_validation_error(candidate_err)
            candidate["validation_error"] = candidate_err
            still_invalid.append(candidate)

        for corrected in metadata_corrected:
            _register_valid(corrected)
        invalid_benchmarks = still_invalid
        if not invalid_benchmarks:
            break

        corrected = _attempt_benchmark_correction(
            w, config, uc_columns, uc_routines,
            invalid_benchmarks, catalog, schema, spark, allowlist,
            warehouse_id=warehouse_id,
        )
        for corrected_item in corrected:
            _register_valid(corrected_item)
        corrected_questions = {
            str(c.get("question") or "").strip().lower()
            for c in corrected
            if str(c.get("question") or "").strip()
        }
        invalid_benchmarks = [
            b for b in invalid_benchmarks
            if str(b.get("question") or "").strip().lower() not in corrected_questions
        ]

    if invalid_benchmarks:
        logger.warning(
            "Discarded %d benchmarks after %d correction rounds (unfixable): %s",
            len(invalid_benchmarks),
            MAX_CORRECTION_ROUNDS,
            [b.get("question", "")[:50] for b in invalid_benchmarks[:3]],
        )

    # ── Post-validation: check question-SQL alignment via LLM ──────────
    try:
        from genie_space_optimizer.optimization.benchmarks import (
            validate_question_sql_alignment,
        )
        alignment_targets = [b for b in valid_benchmarks if b.get("expected_sql")]
        if alignment_targets:
            alignment_results = validate_question_sql_alignment(alignment_targets)
            _newly_invalid: list[dict] = []
            for b, ar in zip(alignment_targets, alignment_results):
                if not ar.get("aligned", True):
                    b["alignment_issues"] = ar.get("issues", [])
                    b["validation_status"] = "invalid"
                    b["validation_reason_code"] = "alignment_mismatch"
                    b["validation_error"] = "; ".join(ar.get("issues", []))
                    _newly_invalid.append(b)
                    logger.warning(
                        "Benchmark REJECTED (alignment): %s -- %s",
                        b.get("question", "")[:80],
                        "; ".join(ar.get("issues", [])),
                    )
            if _newly_invalid:
                valid_benchmarks = [b for b in valid_benchmarks if b not in _newly_invalid]
                _alignment_corrected = _attempt_benchmark_correction(
                    w, config, uc_columns, uc_routines,
                    _newly_invalid, catalog, schema, spark, allowlist,
                    warehouse_id=warehouse_id,
                )
                for c in _alignment_corrected:
                    _register_valid(c)
                logger.info(
                    "Alignment check: %d rejected, %d corrected, %d discarded",
                    len(_newly_invalid), len(_alignment_corrected),
                    len(_newly_invalid) - len(_alignment_corrected),
                )
    except Exception as _align_err:
        logger.warning("Alignment validation skipped: %s", _align_err)

    all_benchmarks: list[dict] = list(_existing)

    from genie_space_optimizer.common.config import REQUIRE_GROUND_TRUTH_SQL

    curated_with_sql = [b for b in curated if str(b.get("expected_sql", "") or "").strip()]
    curated_no_sql = [b for b in curated if not str(b.get("expected_sql", "") or "").strip()]

    if curated_no_sql and REQUIRE_GROUND_TRUTH_SQL:
        logger.info(
            "Generating ground-truth SQL for %d curated question-only benchmarks",
            len(curated_no_sql),
        )
        enriched_curated = _generate_sql_for_curated_questions(
            w, config, uc_columns, uc_routines,
            curated_no_sql, catalog, schema, spark,
            warehouse_id=warehouse_id,
        )
        curated_with_sql.extend(enriched_curated)
        _dropped = len(curated_no_sql) - len(enriched_curated)
        if _dropped:
            logger.warning(
                "Dropped %d curated questions that could not get valid SQL "
                "(enriched %d/%d)",
                _dropped, len(enriched_curated), len(curated_no_sql),
            )
        _dropped_questions = [
            b["question"][:80] for b in curated_no_sql
            if b["question"].strip().lower() not in {
                e["question"].strip().lower() for e in enriched_curated
            }
        ]
        if _dropped_questions:
            logger.info(
                "Dropped curated questions: %s",
                "; ".join(_dropped_questions[:10]),
            )
    elif curated_no_sql:
        curated_with_sql.extend(curated_no_sql)

    effective_curated = curated_with_sql

    for idx, b in enumerate(effective_curated):
        question_id = f"{domain}_gs_{idx + 1:03d}"
        priority = "P0"
        expected_sql = str(b.get("expected_sql", "") or "")
        curated_status = "question_only" if not expected_sql else str(
            b.get("validation_status", "valid"),
        )
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": expected_sql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    expected_sql,
                    hint=b.get("expected_asset_hint"),
                ),
                "expected_asset_hint": b.get("expected_asset_hint", ""),
                "category": b.get("category", "curated"),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": priority,
                "split": "",
                "source": b.get("source") or "genie_space",
                "provenance": b.get("provenance") or "curated",
                "validation_status": curated_status,
                "validation_reason_code": "ok" if expected_sql else "missing_expected_sql",
                "validation_error": None if expected_sql else "No expected SQL in curated sample question",
                "correction_source": b.get("correction_source", ""),
            }
        )

    offset = len(effective_curated)
    for idx, b in enumerate(valid_benchmarks):
        question_id = f"{domain}_{offset + idx + 1:03d}"
        priority = "P0" if idx < 3 else "P1"
        _b_esql = b.get("expected_sql", "")
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": _b_esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    _b_esql,
                    hint=b.get("expected_asset_hint"),
                ),
                "expected_asset_hint": b.get("expected_asset_hint", ""),
                "category": b.get("category", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": priority,
                "split": "",
                "source": b.get("source") or "llm_generated",
                "provenance": b.get("provenance") or "synthetic",
                "validation_status": b.get("validation_status", "valid"),
                "validation_reason_code": b.get("validation_reason_code", "ok"),
                "validation_error": b.get("validation_error"),
                "correction_source": b.get("correction_source", ""),
            }
        )

    # ── Coverage gap-fill: ensure every asset has at least one benchmark ──
    all_accepted_questions = (
        curated_questions
        | accepted_questions
        | {str(b.get("question", "")).lower().strip() for b in _existing}
    )
    remaining_budget = max(max_benchmark_count - len(all_benchmarks), 0)
    if remaining_budget <= 0:
        gap_fill_benchmarks: list[dict] = []
    else:
        gap_fill_benchmarks = _fill_coverage_gaps(
            w=w,
            config=config,
            uc_columns=uc_columns,
            uc_routines=uc_routines,
            benchmarks=all_benchmarks,
            catalog=catalog,
            schema=schema,
            spark=spark,
            allowlist=allowlist,
            domain=domain,
            existing_questions=all_accepted_questions,
            warehouse_id=warehouse_id,
            target_benchmark_count=min(target_count, max_benchmark_count),
            max_benchmark_count=max_benchmark_count,
        )
    gap_fill_offset = len(curated) + len(valid_benchmarks)
    for idx, b in enumerate(gap_fill_benchmarks):
        question_id = f"{domain}_gf_{gap_fill_offset + idx + 1:03d}"
        _gf_esql = b.get("expected_sql", "")
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": _gf_esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    _gf_esql,
                    hint=b.get("expected_asset_hint"),
                ),
                "category": b.get("category", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": "P1",
                "split": "",
                "source": "llm_generated",
                "provenance": "coverage_gap_fill",
                "validation_status": b.get("validation_status", "valid"),
                "validation_reason_code": b.get("validation_reason_code", "ok"),
                "validation_error": b.get("validation_error"),
                "correction_source": "",
            }
        )

    # ── Post-generation: enforce instruction-mandated default filters ──
    _filter_patched = _enforce_instruction_default_filters_on_benchmarks(
        all_benchmarks, config,
    )
    if _filter_patched:
        logger.info(
            "Post-generation filter enforcement: patched %d benchmark(s) "
            "with instruction-mandated default filters",
            _filter_patched,
        )

    from genie_space_optimizer.optimization.benchmarks import assign_splits

    if len(all_benchmarks) > max_benchmark_count:
        all_benchmarks = _truncate_benchmarks(all_benchmarks, max_benchmark_count)
    all_benchmarks = assign_splits(all_benchmarks)
    _train_n = sum(1 for b in all_benchmarks if b.get("split") == "train")
    _held_n = len(all_benchmarks) - _train_n

    logger.info(
        "Final benchmark set: %d total (%d curated from Genie space, "
        "%d synthetic, %d gap-fill, %d discarded out of %d raw generated, "
        "split: %d train / %d held_out)",
        len(all_benchmarks),
        len(curated),
        len(valid_benchmarks),
        len(gap_fill_benchmarks),
        len(invalid_benchmarks),
        len(raw_benchmarks),
        _train_n,
        _held_n,
    )
    return all_benchmarks


def load_benchmarks_from_dataset(
    spark: SparkSession,
    uc_schema: str,
    domain: str,
    _max_retries: int = 3,
) -> list[dict]:
    """Load benchmarks from an existing MLflow UC evaluation dataset table.

    Issues ``REFRESH TABLE`` before reading to avoid
    ``DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`` when the upstream preflight task
    drops and recreates the table in the same job run.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
        parts = uc_schema.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid uc_schema: {uc_schema}")
        catalog, schema = parts
        table = f"genie_benchmarks_{domain}"

        def _q(identifier: str) -> str:
            return f"`{identifier.replace('`', '``')}`"

        quoted_table_name = f"{_q(catalog)}.{_q(schema)}.{_q(table)}"

        try:
            _exists_df = spark.sql(
                f"SHOW TABLES IN {_q(catalog)}.{_q(schema)} LIKE '{table}'"
            )
            if _exists_df.count() == 0:
                logger.info("Benchmark table %s does not exist yet — skipping load", table_name)
                return []
        except Exception:
            pass

        df = None
        last_err: Exception | None = None
        for attempt in range(_max_retries):
            try:
                from genie_space_optimizer.common.delta_helpers import _safe_refresh
                _safe_refresh(spark, quoted_table_name)
                df = spark.sql(f"SELECT * FROM {quoted_table_name}").toPandas()
                break
            except Exception as read_err:
                last_err = read_err
                err_msg = str(read_err)
                if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" in err_msg and attempt < _max_retries - 1:
                    import time as _time
                    wait = 5 * (attempt + 1)
                    logger.warning(
                        "Delta schema change on attempt %d/%d for %s — retrying in %ds",
                        attempt + 1, _max_retries, table_name, wait,
                    )
                    _time.sleep(wait)
                    continue
                raise

        if df is None:
            raise last_err or RuntimeError(f"Failed to read {table_name} after {_max_retries} attempts")

        benchmarks: list[dict] = []
        for _, row in df.iterrows():
            inputs = row.get("inputs", {})
            expectations = row.get("expectations", {})
            if isinstance(inputs, str):
                inputs = json.loads(inputs)
            if isinstance(expectations, str):
                expectations = json.loads(expectations)
            if not isinstance(inputs, dict):
                inputs = {}
            if not isinstance(expectations, dict):
                expectations = {}

            _cb_esql = inputs.get("expected_sql", expectations.get("expected_response", ""))
            benchmarks.append(
                {
                    "id": inputs.get("question_id", ""),
                    "question": inputs.get("question", ""),
                    "expected_sql": _cb_esql,
                    "expected_asset": _normalize_expected_asset(
                        expectations.get("expected_asset") or inputs.get("expected_asset", "TABLE"),
                        _cb_esql,
                    ),
                    "category": expectations.get("category", ""),
                    "required_tables": expectations.get("required_tables", []),
                    "required_columns": expectations.get("required_columns", []),
                    "expected_facts": expectations.get("expected_facts", []),
                    "source": expectations.get("source") or "",
                    "provenance": expectations.get("provenance") or "",
                    "validation_status": expectations.get("validation_status", ""),
                    "validation_reason_code": expectations.get("validation_reason_code", ""),
                    "validation_error": expectations.get("validation_error"),
                    "correction_source": expectations.get("correction_source", ""),
                    "split": expectations.get("split", "train"),
                }
            )
        pre_dedup = len(benchmarks)
        _seen: set[str] = set()
        deduped: list[dict] = []
        for b in benchmarks:
            key = str(b.get("question", "")).lower().strip()
            if key in _seen:
                continue
            _seen.add(key)
            deduped.append(b)
        if len(deduped) < pre_dedup:
            logger.warning(
                "Dropped %d duplicate benchmark(s) by question text when loading from %s",
                pre_dedup - len(deduped), table_name,
            )
        benchmarks = deduped

        from genie_space_optimizer.common.config import MAX_BENCHMARK_COUNT
        if len(benchmarks) > MAX_BENCHMARK_COUNT:
            benchmarks = _truncate_benchmarks(benchmarks, MAX_BENCHMARK_COUNT)

        logger.info("Loaded %d benchmarks from %s", len(benchmarks), table_name)
        return benchmarks
    except Exception as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            logger.info("Benchmark table %s does not exist yet — will generate", table_name)
        else:
            logger.exception("Failed to load benchmarks from %s", table_name)
        return []


# ── MLflow Feedback Helpers (gate outcomes & ASI on traces) ──────────


def log_gate_feedback_on_traces(
    eval_result: dict,
    gate_type: str,
    gate_result: str,
    regressions: list[dict] | None = None,
    lever: int | None = None,
    iteration: int | None = None,
) -> int:
    """Attach gate outcome as Feedback assessment on each evaluation trace.

    Returns the number of feedback entries successfully logged.
    """
    trace_map = eval_result.get("trace_map", {})
    if not trace_map:
        return 0

    logged = 0
    for qid, trace_id in trace_map.items():
        reg_summary = ""
        if regressions:
            reg_summary = "; regressions: " + ", ".join(
                f"{r.get('judge', '?')} -{r.get('drop', 0):.1f}"
                for r in regressions[:3]
            )
        try:
            mlflow.log_feedback(
                trace_id=trace_id,
                name=f"gate_{gate_type}",
                value=gate_result == "pass",
                rationale=f"Lever {lever} gate {gate_type}: {gate_result}{reg_summary}",
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/gate",
                ),
                metadata={
                    "gate_type": gate_type,
                    "gate_result": gate_result,
                    "lever": lever,
                    "iteration": iteration,
                    "question_id": qid,
                    "regressions": (regressions or [])[:3],
                },
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log gate feedback for trace %s", trace_id, exc_info=True)
    if logged:
        logger.info("Logged gate_%s feedback on %d/%d traces", gate_type, logged, len(trace_map))
    return logged


def log_asi_feedback_on_traces(
    eval_result: dict,
    asi_rows: list[dict],
) -> int:
    """Attach ASI root-cause analysis as Feedback on evaluation traces.

    Returns the number of feedback entries successfully logged.
    """
    trace_map = eval_result.get("trace_map", {})
    if not trace_map or not asi_rows:
        return 0

    logged = 0
    for asi in asi_rows:
        qid = asi.get("question_id", "")
        tid = trace_map.get(qid)
        if not tid:
            continue
        judge = asi.get("judge", "unknown")
        try:
            mlflow.log_feedback(
                trace_id=tid,
                name=f"asi_{judge}",
                value=asi.get("value", "no") == "yes",
                rationale=asi.get("counterfactual_fix") or asi.get("rationale_snippet") or "",
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/asi",
                ),
                metadata={
                    "failure_type": asi.get("failure_type"),
                    "severity": asi.get("severity"),
                    "blame_set": asi.get("blame_set"),
                    "wrong_clause": asi.get("wrong_clause"),
                    "expected_value": asi.get("expected_value"),
                    "actual_value": asi.get("actual_value"),
                    "question_id": qid,
                    "judge": judge,
                },
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log ASI feedback for trace %s judge %s", tid, judge, exc_info=True)
    if logged:
        logger.info("Logged ASI feedback on %d traces", logged)
    return logged


def log_expectations_on_traces(eval_result: dict) -> int:
    """Attach expected SQL as Expectation assessments on evaluation traces.

    Makes traces self-contained for reviewers in labeling sessions — they
    can see the expected SQL alongside Genie's generated SQL without
    needing external context.

    Returns the number of expectations successfully logged.
    """
    trace_map = eval_result.get("trace_map", {})
    if not trace_map:
        return 0

    rows = eval_result.get("rows", [])
    logged = 0
    for row in rows:
        qid = (
            row.get("question_id")
            or row.get("inputs/question_id")
            or (row.get("inputs") or {}).get("question_id", "")
        )
        tid = trace_map.get(qid)
        if not tid:
            continue

        expected_sql = (
            row.get("inputs/expected_sql")
            or (row.get("inputs") or {}).get("expected_sql", "")
        )
        question = (
            row.get("inputs/question")
            or (row.get("inputs") or {}).get("question", "")
        )
        if not expected_sql:
            continue

        try:
            mlflow.log_expectation(
                trace_id=tid,
                name="expected_sql",
                value=expected_sql,
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/benchmark",
                ),
                metadata={
                    "question_id": qid,
                    "question": question[:200] if question else "",
                },
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log expectation for trace %s", tid, exc_info=True)
    if logged:
        logger.info("Logged expected_sql expectations on %d/%d traces", logged, len(trace_map))
    return logged


def log_judge_verdicts_on_traces(eval_result: dict) -> int:
    """Attach per-question judge verdicts as feedback on MLflow traces.

    Enables human reviewers to see all judge scores at a glance in the
    trace UI without re-running the evaluation.
    """
    trace_map = eval_result.get("trace_map", {})
    rows = eval_result.get("rows", [])
    logged = 0
    for row in rows:
        qid = row.get("question_id") or row.get("inputs/question_id") or ""
        tid = trace_map.get(qid)
        if not tid:
            continue
        verdicts: dict[str, Any] = {}
        for judge in [
            "schema_accuracy", "logical_accuracy", "completeness",
            "asset_routing", "result_correctness", "arbiter",
        ]:
            val = row.get(f"{judge}/value") or row.get(judge)
            if val is not None:
                verdicts[judge] = val
        if not verdicts:
            continue
        _passing = ("yes", "both_correct", "genie_correct")
        overall = "PASS" if all(v in _passing for v in verdicts.values()) else "FAIL"
        failed_judges = [j for j, v in verdicts.items() if v not in _passing]

        try:
            mlflow.log_feedback(
                trace_id=tid,
                name="judge_verdicts",
                value=overall == "PASS",
                rationale=json.dumps(verdicts),
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/judges",
                ),
                metadata={"question_id": qid, **verdicts},
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log judge verdicts for trace %s", tid, exc_info=True)

        try:
            mlflow.set_trace_tag(tid, "judge_verdict", overall)
            if failed_judges:
                mlflow.set_trace_tag(tid, "failed_judges", ",".join(failed_judges))
        except Exception:
            logger.debug("Failed to set judge verdict tags for trace %s", tid, exc_info=True)

        try:
            verdict_lines = [f"  {j}: {v}" for j, v in verdicts.items()]
            mlflow.log_expectation(
                trace_id=tid,
                name="judge_verdict_summary",
                value=f"Overall: {overall}\n" + "\n".join(verdict_lines),
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/judges",
                ),
                metadata={"question_id": qid, "overall": overall, **verdicts},
            )
        except Exception:
            logger.debug("Failed to log judge verdict expectation for trace %s", tid, exc_info=True)
    if logged:
        logger.info("Logged judge verdicts on %d/%d traces", logged, len(trace_map))
    return logged


def log_persistence_context_on_traces(
    eval_result: dict,
    persistence_data: dict[str, dict],
    *,
    extra_trace_map: dict[str, list[str]] | None = None,
) -> int:
    """Attach per-question failure persistence context as feedback on traces.

    Lets human reviewers see how many times each question has failed
    across iterations, its persistence classification, and which
    patches have already been attempted.

    When *extra_trace_map* is provided it is used as the primary source
    of trace IDs per question, logging on **all** traces for each
    question (not just the last eval's ``trace_map``).
    """
    fallback_trace_map = eval_result.get("trace_map", {})
    logged = 0
    for qid, ctx in persistence_data.items():
        if extra_trace_map and qid in extra_trace_map:
            tids = extra_trace_map[qid]
        else:
            tid = fallback_trace_map.get(qid)
            tids = [tid] if tid else []
        for tid in tids:
            classification = ctx.get("classification", "UNKNOWN")
            is_persistent = classification not in ("INTERMITTENT", "UNKNOWN")
            try:
                mlflow.log_feedback(
                    trace_id=tid,
                    name="persistence_context",
                    value=is_persistent,
                    rationale=(
                        f"Failed {ctx.get('fail_count', 0)} times, "
                        f"{ctx.get('max_consecutive', 0)} consecutive"
                    ),
                    source=AssessmentSource(
                        source_type="CODE",
                        source_id="genie_space_optimizer/persistence",
                    ),
                    metadata={
                        "question_id": qid,
                        "fail_count": ctx.get("fail_count", 0),
                        "max_consecutive": ctx.get("max_consecutive", 0),
                        "classification": classification,
                        "patches_tried": str(ctx.get("patches_tried", [])),
                        "fail_iterations": ctx.get("fail_iterations", []),
                    },
                )
            except Exception:
                logger.debug("Failed to log persistence feedback for trace %s", tid, exc_info=True)
            try:
                mlflow.set_trace_tag(tid, "persistent_failure", str(is_persistent).lower())
                mlflow.set_trace_tag(tid, "persistence_classification", classification)
                logged += 1
            except Exception:
                logger.debug("Failed to set persistence tags for trace %s", tid, exc_info=True)
    if logged:
        logger.info("Logged persistence context on %d/%d traces", logged, len(persistence_data))
    return logged


def log_patch_history_on_traces(
    question_trace_map: dict[str, list[str]],
    reflection_buffer: list[dict],
    persistent_question_ids: set[str] | None = None,
) -> int:
    """Log per-question patch history from the reflection buffer as feedback on traces.

    For each question in *persistent_question_ids* (or all questions if None),
    extracts which patches were proposed/applied/rolled-back and the score delta,
    then logs as ``mlflow.log_feedback`` with ``name="patch_history"`` on the
    question's **latest** trace from *question_trace_map*.

    Returns the number of traces that received feedback.
    """
    q_history: dict[str, list[dict]] = {}
    for entry in reflection_buffer:
        iteration = entry.get("iteration", 0)
        accepted = entry.get("accepted", False)
        affected = entry.get("affected_question_ids", [])
        action = entry.get("action", "")
        prev_scores = entry.get("prev_scores", {})
        new_scores = entry.get("new_scores", {})
        prev_acc = sum(prev_scores.values()) / max(len(prev_scores), 1) if prev_scores else 0.0
        new_acc = sum(new_scores.values()) / max(len(new_scores), 1) if new_scores else 0.0
        acc_delta = new_acc - prev_acc

        patches_info: list[str] = []
        for part in action.split(", "):
            if " on " in part:
                patches_info.append(part.strip())
            elif part.strip():
                patches_info.append(part.strip())

        record = {
            "iteration": iteration,
            "accepted": accepted,
            "action": action,
            "patches": patches_info,
            "score_delta": round(acc_delta, 2),
        }
        for qid in affected:
            q_history.setdefault(qid, []).append(record)

    target_qids = persistent_question_ids if persistent_question_ids is not None else set(q_history.keys())
    logged = 0
    for qid in target_qids:
        entries = q_history.get(qid, [])
        tids = question_trace_map.get(qid, [])
        if not tids:
            continue
        latest_tid = tids[-1]

        lines: list[str] = []
        iterations_list: list[int] = []
        patches_list: list[str] = []
        accepted_list: list[bool] = []
        delta_list: list[float] = []
        for e in entries:
            status = "ACCEPTED" if e["accepted"] else "ROLLED_BACK"
            delta_str = f"{e['score_delta']:+.1f}%"
            action_str = e["action"][:120] if e["action"] else "unknown"
            lines.append(f"Iter {e['iteration']}: {action_str}, {status} ({delta_str})")
            iterations_list.append(e["iteration"])
            patches_list.extend(e["patches"])
            accepted_list.append(e["accepted"])
            delta_list.append(e["score_delta"])

        rationale = "; ".join(lines) if lines else "No patch history for this question"
        try:
            mlflow.log_feedback(
                trace_id=latest_tid,
                name="patch_history",
                value=bool(entries),
                rationale=rationale,
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/patch_history",
                ),
                metadata={
                    "question_id": qid,
                    "iterations": iterations_list,
                    "patches": patches_list,
                    "accepted": accepted_list,
                    "score_deltas": delta_list,
                },
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log patch history for trace %s", latest_tid, exc_info=True)

    if logged:
        logger.info("Logged patch history on %d traces", logged)
    return logged


def _extract_genie_sql_from_trace(trace_id: str) -> str:
    """Extract Genie's generated SQL from a stored MLflow trace.

    Returns the SQL string if found, or empty string on failure.
    """
    if not trace_id:
        return ""
    try:
        trace = mlflow.get_trace(trace_id)
        if trace is None:
            return ""
        response = trace.data.response if hasattr(trace, "data") else None
        if isinstance(response, dict):
            return response.get("genie_sql", "") or response.get("sql", "")
        if isinstance(response, str):
            try:
                parsed = json.loads(response)
                return parsed.get("genie_sql", "") or parsed.get("sql", "")
            except (json.JSONDecodeError, TypeError):
                pass
    except Exception:
        logger.debug("Failed to extract Genie SQL from trace %s", trace_id, exc_info=True)
    return ""
