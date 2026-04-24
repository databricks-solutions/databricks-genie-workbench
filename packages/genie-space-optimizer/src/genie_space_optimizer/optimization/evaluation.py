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
from mlflow.entities import AssessmentSource, Feedback
from mlflow.genai.scorers import scorer

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


def _extract_json(content: str) -> dict | list:
    """Extract a JSON value from LLM response text with lenient wrapping.

    Handles common LLM output patterns:

    - Pure JSON (object or array)
    - JSON wrapped in markdown code fences (``` or ```json), anywhere in the
      string (not only at the start) — some LLMs prefix with "Here is ...".
    - JSON preceded/followed by prose ("Here are my suggestions: {...}")
    - Trailing "Extra data" (``json.loads`` reports the position of the first
      extraneous byte; we truncate and retry).
    - Array output wrapped in prose (``[{...}, {...}]``) — previously only
      object output was rescued via regex; this caused the instruction-to-SQL
      conversion to silently return ``[]`` when the LLM added any prose around
      the array.

    Empty / whitespace-only input returns ``{}`` rather than raising a
    ``JSONDecodeError`` (Phase 3.R8). The pipeline's LLM wrappers
    occasionally return an empty response when the provider throttles or
    returns a blank completion; the caller's ``try/except`` would have
    caught the exception but the traceback was noise. Returning a clean
    empty dict lets the existing ``result.get("changes", [])`` pattern
    degrade gracefully to "no changes in this batch".

    Return type is a union ``dict | list`` so callers that expect an array
    (the prose-rule miner) aren't forced to re-parse. Existing object-only
    callers continue to work; the return type is dict for their prompts.

    Raises the saved ``JSONDecodeError`` when every strategy fails on
    non-empty input — callers that want lenient behaviour can try-except
    and fall back to ``[]`` / ``{}``.
    """
    content = content.strip()
    if not content:
        return {}

    # Fenced block anywhere in the string — prefer it over the surrounding
    # prose so a preamble like "Here is the JSON:\n```json\n{...}\n```" works.
    fence_match = _FENCED_BLOCK_RE.search(content)
    if fence_match:
        fenced = fence_match.group("body").strip()
        if fenced:
            try:
                return json.loads(fenced)
            except json.JSONDecodeError:
                # Fall through — the fenced block might itself be malformed
                # but the surrounding text could still contain valid JSON.
                pass

    _saved_err: json.JSONDecodeError | None = None

    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        _saved_err = exc

    if (
        _saved_err is not None
        and hasattr(_saved_err, "pos")
        and _saved_err.msg.startswith("Extra data")
    ):
        try:
            return json.loads(content[: _saved_err.pos])
        except json.JSONDecodeError:
            pass

    # Regex fallbacks — try the first balanced `{...}` and `[...]`; take the
    # one that parses. We prefer whichever is longer so a nested structure
    # wins over a short sub-literal.
    candidates: list[tuple[int, str]] = []
    obj_match = re.search(r"\{.*\}", content, re.DOTALL)
    if obj_match:
        candidates.append((len(obj_match.group(0)), obj_match.group(0)))
    arr_match = re.search(r"\[.*\]", content, re.DOTALL)
    if arr_match:
        candidates.append((len(arr_match.group(0)), arr_match.group(0)))
    # Longest-first maximises the chance of getting the outermost structure.
    for _, candidate in sorted(candidates, key=lambda c: -c[0]):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    assert _saved_err is not None  # pragma: no cover — invariant
    raise _saved_err


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


def _rewrite_measure_refs(
    sql: str,
    metric_view_measures: dict[str, set[str]],
) -> str:
    """Wrap bare metric view measure names with MEASURE() in SELECT and ORDER BY.

    Only applies when the SQL references a metric view in its FROM clause.
    ``metric_view_measures`` maps lowercased short table names to sets of
    lowercased measure column names.
    """
    if not metric_view_measures or not sql:
        return sql

    from_tables: list[str] = []
    for m in re.finditer(r"\bFROM\s+([\w.`]+)", sql, re.IGNORECASE):
        from_tables.append(m.group(1).replace("`", "").split(".")[-1].lower())

    relevant_measures: set[str] = set()
    for tbl in from_tables:
        if tbl in metric_view_measures:
            relevant_measures |= metric_view_measures[tbl]

    if not relevant_measures:
        return sql

    already_measured = re.compile(r"\bMEASURE\s*\(", re.IGNORECASE)

    def _wrap(m: re.Match) -> str:
        col = m.group(1)
        if col.lower() in relevant_measures:
            return f"MEASURE({col})"
        return col

    # Rewrite bare measures in SELECT clause
    select_match = re.search(r"\bSELECT\b", sql, re.IGNORECASE)
    from_match = re.search(r"\bFROM\b", sql, re.IGNORECASE)
    if select_match and from_match and select_match.end() < from_match.start():
        select_clause = sql[select_match.end() : from_match.start()]
        rewritten_select = re.sub(
            r"\b([A-Za-z_]\w*)\b(?!\s*\()",
            lambda m: (
                m.group(0)
                if already_measured.search(
                    sql[
                        max(0, select_match.end() + m.start() - 10) : select_match.end() + m.start()
                    ]
                )
                else _wrap(m)
            ),
            select_clause,
        )
        sql = sql[: select_match.end()] + rewritten_select + sql[from_match.start() :]

    # Rewrite bare measures in ORDER BY clause
    order_match = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    if not order_match:
        return sql

    prefix = sql[: order_match.end()]
    tail = sql[order_match.end() :]

    rewritten_tail = re.sub(
        r"\b([A-Za-z_]\w*)\b(?!\s*\()",
        lambda m: m.group(0) if already_measured.search(sql[max(0, order_match.end() + m.start() - 10) : order_match.end() + m.start()]) else _wrap(m),
        tail,
    )
    return prefix + rewritten_tail


def build_metric_view_measures(config: dict) -> dict[str, set[str]]:
    """Build {lowered_short_name: {measure_col, ...}} from the parsed Genie config."""
    parsed = config.get("_parsed_space", config)
    ds = parsed.get("data_sources", {})
    if not isinstance(ds, dict):
        return {}
    mvs = ds.get("metric_views", [])
    result: dict[str, set[str]] = {}
    for mv in mvs:
        identifier = mv.get("identifier", "")
        short_name = identifier.split(".")[-1].lower() if identifier else ""
        if not short_name:
            continue
        measures: set[str] = set()
        for cc in mv.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if not col_name:
                continue
            col_type = str(cc.get("column_type", "")).lower()
            if col_type == "measure" or cc.get("is_measure"):
                measures.add(col_name.lower())
        if measures:
            result[short_name] = measures
    return result


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


def _merge_row_sources(
    row_dict: dict[str, Any],
    assessment_map_row: dict[str, dict] | None,
    cached_feedback_qid: dict[str, dict] | None,
) -> dict[str, Any]:
    """Reconcile judge rationale/metadata from three sources.

    Precedence (authoritative first):
    1. Trace assessments (``assessment_map_row``) — what MLflow stored in
       the trace. This is the ground truth displayed in the MLflow UI.
    2. The run-scoped scorer feedback cache (``cached_feedback_qid``) —
       captured directly from the scorer at return time; only consulted
       when the trace layer is silent for that judge.
    3. Any ``<judge>/rationale`` / ``<judge>/metadata`` column already
       present in ``row_dict`` (copied from ``results_df``) — the weakest
       source, since the ``eval_results`` table is known to misalign
       rationales to the wrong row in some MLflow versions.

    Mutates and returns ``row_dict``. Only overwrites keys for judges that
    actually have data in the higher-priority source; untouched judges
    keep whatever the flat columns contain.
    """
    assessment_map_row = assessment_map_row or {}
    cached_feedback_qid = cached_feedback_qid or {}

    judge_names: set[str] = set(assessment_map_row) | set(cached_feedback_qid)

    for judge_name in judge_names:
        rat_key = f"{judge_name}/rationale"
        meta_key = f"{judge_name}/metadata"

        trace_data = assessment_map_row.get(judge_name) or {}
        trace_rationale = trace_data.get("rationale")
        trace_metadata = trace_data.get("metadata")

        cache_data = cached_feedback_qid.get(judge_name) or {}
        cache_rationale = cache_data.get("rationale")
        cache_metadata = cache_data.get("metadata")

        if trace_rationale:
            row_dict[rat_key] = trace_rationale
        elif cache_rationale:
            row_dict[rat_key] = cache_rationale

        if trace_metadata:
            row_dict[meta_key] = trace_metadata
        elif cache_metadata:
            row_dict[meta_key] = cache_metadata

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
    """Extract the ``result_correctness`` value as a lowercase string."""
    val = row.get("result_correctness/value", row.get("result_correctness", ""))
    return str(val).strip().lower()


def _arbiter_str(row: dict) -> str:
    """Extract the arbiter verdict as a lowercase string."""
    val = row.get("arbiter/value", row.get("arbiter", ""))
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
) -> list[dict]:
    """Filter benchmarks based on evaluation scope.

    Scopes: "full" (all), "slice" (affected by patches),
    "p0" (priority P0 only), "held_out" (held-out split).

    For "slice" scope, benchmarks are included if:
    - Their required tables/columns overlap with *patched_objects*, OR
    - Their question id is in *affected_question_ids* (from proposal clusters).
    """
    if scope == "full":
        return benchmarks
    if scope == "slice":
        patched = {o.lower() for o in patched_objects} if patched_objects else set()
        affected_qids = affected_question_ids or set()
        result = []
        for b in benchmarks:
            qid = b.get("id", "")
            if qid and qid in affected_qids:
                result.append(b)
                continue
            if patched and any(
                t.lower() in patched
                for t in b.get("required_tables", []) + b.get("required_columns", [])
            ):
                result.append(b)
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

    error_msg = ""
    if resp.status and resp.status.error:
        error_msg = resp.status.error.message or str(resp.status.error)
    raise RuntimeError(error_msg or "SQL warehouse query failed")


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
    """Classify SQL validation failures into stable reason codes."""
    lowered = (message or "").lower()
    if "metric_view_join_not_supported" in lowered:
        return "metric_view_join"
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
        if _mv_measures:
            resolved_sql = _rewrite_measure_refs(resolved_sql, _mv_measures)

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

    @mlflow.trace
    def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
        """Query Genie, fetch its results via Statement API, execute only GT SQL.

        Steps: rate-limit → Genie call → fetch Genie result via statement_id →
               resolve & execute GT SQL → normalize → compare hashes.

        We never re-execute Genie's SQL ourselves.  Genie runs queries on its
        own SQL warehouse; re-executing via Spark Connect can hit different
        limitations (e.g. METRIC_VIEW_JOIN_NOT_SUPPORTED).
        """
        try:
            if instruction_prompt_name:
                _link_prompt_to_trace(instruction_prompt_name)
            _trace_tags: dict[str, str] = {
                "question_id": kwargs.get("question_id", ""),
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
            time.sleep(RATE_LIMIT_SECONDS)
            result = run_genie_query(w, space_id, question)
            genie_sql = sanitize_sql(result.get("sql") or "")
            gt_sql = resolve_sql(expected_sql, catalog, schema)
            from genie_space_optimizer.optimization.benchmarks import fix_mv_alias_sort_collision
            gt_sql = fix_mv_alias_sort_collision(gt_sql)
            if _mv_measures and gt_sql:
                gt_sql = _rewrite_measure_refs(gt_sql, _mv_measures)
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
                                        _execute_sql_via_warehouse(
                                            w, warehouse_id, f"EXPLAIN {gt_sql}",
                                            catalog=catalog, schema=schema,
                                        )
                                    else:
                                        _set_sql_context(spark, catalog, schema)
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
                                        raw_gt_df = _execute_sql_via_warehouse(
                                            w, warehouse_id, gt_sql,
                                            catalog=catalog, schema=schema,
                                        )
                                        gt_df = normalize_result_df(raw_gt_df)
                                    else:
                                        _set_sql_context(spark, catalog, schema)
                                        gt_df = normalize_result_df(spark.sql(gt_sql).toPandas())

                                genie_df = None
                                if statement_id:
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
                                    comparison = {
                                        "match": exact_match or hash_match or subset_match or approx_match or tied_subset or cosmetic_match or sig_match,
                                        "match_type": match_type,
                                        "gt_rows": len(gt_df),
                                        "genie_rows": len(genie_df),
                                        "gt_columns": gt_col_list,
                                        "genie_columns": genie_col_list,
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
        eval_dataset.merge_records(records)
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
        passed = score >= threshold
        marker = "" if passed else "  <<<"
        lines.append(
            f"|   {judge:<24s} {score:6.1f}  (threshold: {threshold:.1f})  "
            f"{'PASS' if passed else 'FAIL'}{marker}"
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
    if adj_excluded:
        lines.append(f"|   Excluded (GT infra / both-empty / unavailable): {adj_excluded}")
    lines.append(f"|   Thresholds met: {'YES' if thresholds_passed else 'NO'}")
    if adj_failures:
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

        if model_creation_kwargs:
            from genie_space_optimizer.optimization.models import create_genie_model_version
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
            eval_result, eval_attempts = _run_evaluate_with_retries(
                evaluate_kwargs=evaluate_kwargs,
            )
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

        if hasattr(eval_result, "tables") and "eval_results" in eval_result.tables:
            results_df = eval_result.tables["eval_results"]

            assessment_map = _extract_assessments_from_traces(results_df)

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
                _merge_row_sources(
                    row_dict,
                    assessment_map.get(row_idx),
                    cached_feedback.get(qid) if qid else None,
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
            for _judge_name in _ARBITER_ADJUSTABLE_JUDGES:
                _j_total = _j_correct = 0
                for _row in rows_for_output:
                    _j_val = str(_row.get(f"{_judge_name}/value", "")).lower()
                    if _j_val == "excluded":
                        continue
                    _j_total += 1
                    if _j_val in ("yes", "true", "1", "1.0", "pass"):
                        _j_correct += 1
                    elif str(_row.get("arbiter/value", "")).lower() in _ARBITER_CORRECT_VERDICTS:
                        _j_correct += 1
                if _j_total > 0:
                    per_judge[_judge_name] = _j_correct / _j_total

            scores_100 = normalize_scores(per_judge)
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

        output: dict[str, Any] = {
            "run_id": run.info.run_id,
            "mlflow_run_id": run.info.run_id,
            "run_name": run_name,
            "experiment_id": exp.experiment_id if exp else "",
            "iteration": iteration,
            "overall_accuracy": arbiter_adjusted_accuracy,
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
        }

    logger.info(
        "Evaluation complete: %s — accuracy=%.1f%%, thresholds=%s",
        run_name,
        output["overall_accuracy"],
        "PASS" if thresholds_passed else "FAIL",
    )

    _log_pass_bucket_metrics(rows_for_output)

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


def extract_genie_space_benchmarks(
    config: dict,
    spark: SparkSession,
    catalog: str = "",
    schema: str = "",
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Extract curated benchmark questions from a Genie Space config.

    Sources (in priority order):
      1. ``instructions.example_question_sqls`` — curated Q+SQL pairs the space
         owner has defined. These have the highest fidelity.
      2. ``sample_questions`` — natural-language-only questions from the space
         config (no expected SQL).

    Each returned dict has ``question``, ``expected_sql`` (may be empty for
    sample-only questions), ``source`` = ``"genie_space"``, and
    ``expected_asset``.
    """
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}
    instr = parsed_space.get("instructions", {})
    if not isinstance(instr, dict):
        instr = {}

    benchmarks: list[dict] = []
    seen_questions: set[str] = set()

    example_qs = instr.get("example_question_sqls", [])
    for ex in (example_qs if isinstance(example_qs, list) else []):
        if not isinstance(ex, dict):
            continue
        q_raw = ex.get("question", "")
        if isinstance(q_raw, list):
            q_raw = " ".join(str(c) for c in q_raw)
        question = str(q_raw).strip()
        if not question:
            continue
        q_lower = question.lower()
        if q_lower in seen_questions:
            continue
        seen_questions.add(q_lower)

        sql_raw = ex.get("sql", "")
        if isinstance(sql_raw, list):
            sql_raw = "".join(str(c) for c in sql_raw)
        expected_sql = str(sql_raw).strip()

        if expected_sql:
            from genie_space_optimizer.optimization.benchmarks import fix_mv_alias_sort_collision
            expected_sql = fix_mv_alias_sort_collision(expected_sql)
            is_valid, err = validate_ground_truth_sql(
                expected_sql, spark, catalog=catalog, gold_schema=schema,
                w=w, warehouse_id=warehouse_id,
            )
            if not is_valid:
                logger.warning(
                    "Genie space example_question_sql failed validation: %s — %s",
                    question[:60], err,
                )
                expected_sql = ""

        benchmarks.append({
            "question": question,
            "expected_sql": expected_sql,
            "expected_asset": detect_asset_type(expected_sql) if expected_sql else "TABLE",
            "category": "curated",
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": "genie_space",
        })

    bench_section = parsed_space.get("benchmarks", {})
    if not isinstance(bench_section, dict):
        bench_section = {}
    bench_questions = bench_section.get("questions", [])
    for bq in (bench_questions if isinstance(bench_questions, list) else []):
        if not isinstance(bq, dict):
            continue
        q_raw = bq.get("question", [])
        if isinstance(q_raw, list):
            q_raw = " ".join(str(c) for c in q_raw)
        question = str(q_raw).strip()
        if not question:
            continue
        q_lower = question.lower()
        if q_lower in seen_questions:
            continue
        seen_questions.add(q_lower)

        expected_sql = ""
        answers = bq.get("answer", [])
        if isinstance(answers, list):
            for ans in answers:
                if isinstance(ans, dict) and ans.get("format") == "SQL":
                    content = ans.get("content", [])
                    if isinstance(content, list):
                        expected_sql = "".join(str(c) for c in content).strip()
                    elif isinstance(content, str):
                        expected_sql = content.strip()
                    break

        if expected_sql:
            from genie_space_optimizer.optimization.benchmarks import fix_mv_alias_sort_collision
            expected_sql = fix_mv_alias_sort_collision(expected_sql)
            is_valid, err = validate_ground_truth_sql(
                expected_sql, spark, catalog=catalog, gold_schema=schema,
                w=w, warehouse_id=warehouse_id,
            )
            if not is_valid:
                logger.warning(
                    "Genie space benchmark question failed SQL validation: %s — %s",
                    question[:60], err,
                )
                expected_sql = ""

        benchmarks.append({
            "question": question,
            "expected_sql": expected_sql,
            "expected_asset": detect_asset_type(expected_sql) if expected_sql else "TABLE",
            "category": "curated",
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": "genie_space",
        })

    logger.info(
        "Extracted %d curated benchmarks from Genie space config "
        "(%d with SQL, %d without SQL)",
        len(benchmarks),
        sum(1 for b in benchmarks if b["expected_sql"]),
        sum(1 for b in benchmarks if not b["expected_sql"]),
    )
    return benchmarks


# ── Benchmark Generation ────────────────────────────────────────────────


def _build_valid_assets_context(config: dict) -> str:
    """Build an explicit allowlist of Genie space data assets for the LLM prompt."""
    lines: list[str] = []
    for tbl in config.get("_tables", []):
        lines.append(f"- TABLE: {tbl}")
    for mv in config.get("_metric_views", []):
        lines.append(f"- METRIC VIEW: {mv}")
    for fn in config.get("_functions", []):
        lines.append(f"- FUNCTION: {fn}")
    return "\n".join(lines) if lines else "(no assets configured)"


def _format_data_profile_context(config: dict) -> str:
    """Build a compact data-profile section for benchmark generation prompts.

    Renders per-table row counts, per-column cardinality, distinct values
    for low-cardinality columns, and min/max ranges for numeric/date columns.
    """
    profile = config.get("_data_profile", {})
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
    tables_context = "\n".join(
        f"- {c.get('table_name', '')}.{c.get('column_name', '')} ({c.get('data_type', '')}): {c.get('comment', '')}"
        for c in uc_columns
    )

    # -- Metric views: enrich with measure/dimension column detail --
    mvs_raw = config.get("_metric_views", [])
    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}
    ds = parsed_space.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    mv_sources = ds.get("metric_views", [])

    mv_detail: dict[str, dict] = {}
    for mv in (mv_sources if isinstance(mv_sources, list) else []):
        ident = mv.get("identifier", "")
        if not ident:
            continue
        measures: list[str] = []
        dimensions: list[str] = []
        for cc in mv.get("column_configs", []):
            col = cc.get("column_name", "")
            if not col:
                continue
            if str(cc.get("column_type", "")).lower() == "measure" or cc.get("is_measure"):
                measures.append(col)
            else:
                dimensions.append(col)
        mv_detail[ident] = {"measures": measures, "dimensions": dimensions}

    if mvs_raw:
        mv_lines: list[str] = []
        for mv_ident in mvs_raw:
            detail = mv_detail.get(mv_ident, {})
            m = detail.get("measures", [])
            d = detail.get("dimensions", [])
            parts = [f"- {mv_ident}"]
            if m:
                parts.append(f"  Measures (use MEASURE() syntax): {', '.join(m)}")
            if d:
                parts.append(f"  Dimensions (for GROUP BY / WHERE): {', '.join(d)}")
            if not m and not d:
                parts.append("  (no column detail available)")
            mv_lines.append("\n".join(parts))
        metric_views_context = "\n".join(mv_lines)
    else:
        metric_views_context = "(none)"

    tvfs = config.get("_functions", [])
    tvfs_context = "\n".join(
        f"- {r.get('routine_name', '')}: {r.get('routine_definition', '')[:200]}"
        for r in uc_routines
    ) if uc_routines else (
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

    sample_questions = config.get("_parsed_space", {}).get("sample_questions", [])
    sample_questions_context = "\n".join(
        f"- {q.get('question', q) if isinstance(q, dict) else q}"
        for q in sample_questions
    ) if sample_questions else "(none)"

    columns_by_table: dict[str, list[str]] = {}
    for c in uc_columns:
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
        "data_profile_context": _format_data_profile_context(config),
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
    """
    if not invalid_candidates:
        return []

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    benchmarks_to_fix = json.dumps(
        [
            {
                "question": b["question"],
                "original_expected_sql": b["expected_sql"],
                "error": b.get("validation_error", "unknown"),
                "execution_note": (
                    "Query returns 0 rows — pick realistic filter values from the Data Profile"
                    if b.get("validation_error") == "Query returns 0 rows"
                    else ""
                ),
                "mv_hint": (
                    "METRIC VIEW ALIAS COLLISION: Replace 'ORDER BY alias' with "
                    "'ORDER BY MEASURE(column)' for any MEASURE() expression. "
                    "Do NOT alias MEASURE(col) AS col (same name as source column)."
                    if "MISSING_ATTRIBUTES" in str(b.get("validation_error", ""))
                    or "RESOLVED_ATTRIBUTE" in str(b.get("validation_error", ""))
                    else ""
                ),
            }
            for b in invalid_candidates
        ],
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
        response = _call_llm_for_scoring(
            w, prompt,
            prompt_name=get_registered_prompt_name(correction_prompt_registry_key),
        )
        corrections: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning(
            "SQL correction LLM call failed (registry=%s)",
            correction_prompt_registry_key,
            exc_info=True,
        )
        return []

    corrected: list[dict] = []
    for c in corrections:
        sql = c.get("expected_sql")
        if not sql or c.get("unfixable_reason"):
            logger.info("Candidate unfixable: %s — %s", c.get("question", "")[:60], c.get("unfixable_reason", ""))
            continue
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


def _capture_result_rows(
    sql: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    w: Any = None,
    warehouse_id: str = "",
    limit: int = 20,
) -> list[dict] | None:
    """Run ``sql`` once and return the first ``limit`` rows as dicts.

    Used by the unified engine to give the arbiter judge actual result
    rows to evaluate. Uses the shared ``_exec_sql`` helper so the
    warehouse-vs-Spark routing is consistent with every other
    execution path in this module. Returns ``None`` on any failure —
    the caller fails open (keeps the candidate) because
    ``_validate_benchmark_sql`` has already asserted the SQL executes;
    this helper's failure is pure arbiter-side observability.
    """
    try:
        from genie_space_optimizer.optimization.benchmarks import resolve_sql
        resolved = resolve_sql(sql, catalog, schema)
        sampling_sql = f"SELECT * FROM ({resolved}) _gvse_sample LIMIT {int(limit)}"
        df = _exec_sql(
            sampling_sql, spark,
            w=w, warehouse_id=warehouse_id,
            catalog=catalog, schema=schema,
        )
        if df is None or df.empty:
            return []
        return df.head(limit).to_dict(orient="records")
    except Exception as exc:  # pragma: no cover — defensive
        logger.debug(
            "arbiter result-row capture failed: %s",
            str(exc)[:200],
        )
    return None


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
        "firewall_fingerprint": 0,
        "firewall_question_echo": 0,
        "dedup_in_corpus": 0,
        "unfixable_after_correction": 0,
    }

    allowlist = _build_metadata_allowlist(
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
    )
    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    existing_questions_context = ""
    if existing_questions:
        existing_questions_context = (
            "\n\n## Already Covered Questions (do NOT duplicate these)\n"
            + "\n".join(f"- {q}" for q in existing_questions)
        )

    # ── 1. One-shot batched LLM call ─────────────────────────────
    prompt = format_mlflow_template(
        generation_prompt_template,
        domain=domain,
        target_count=target_count,
        categories=json.dumps(BENCHMARK_CATEGORIES),
        **ctx,
    )
    if existing_questions_context:
        prompt += existing_questions_context

    try:
        response = _call_llm_for_scoring(
            w, prompt,
            prompt_name=get_registered_prompt_name(generation_prompt_registry_key),
        )
    except Exception:
        logger.warning(
            "generate_validated_sql_examples: LLM call failed (registry=%s)",
            generation_prompt_registry_key,
            exc_info=True,
        )
        return [], rejection_counters

    raw_candidates: list[dict] = (
        response if isinstance(response, list)
        else response.get("benchmarks", [])
    )

    # ── 2. Per-candidate validation + MEASURE auto-wrap ──────────
    valid: list[dict] = []
    invalid: list[dict] = []
    accepted_q_lower: set[str] = set()
    mv_measures = build_metric_view_measures(config)
    mv_names = set(config.get("_metric_views", []))

    def _register_valid(cand: dict) -> None:
        q = str(cand.get("question") or "").strip().lower()
        if not q or q in accepted_q_lower or q in existing_q_lower:
            rejection_counters["dedup_in_corpus"] += 1
            return
        accepted_q_lower.add(q)
        valid.append(cand)

    for b in raw_candidates:
        if not isinstance(b, dict):
            continue
        sql_str = str(b.get("expected_sql") or "").strip()
        question = str(b.get("question") or "").strip()
        if not sql_str or not question:
            continue

        # Skip duplicates of the in-corpus questions.
        if question.lower() in existing_q_lower:
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
            candidate["expected_sql"] = sql_str

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

    # ── 3. Bounded correction loop ───────────────────────────────
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

    rejection_counters["explain_or_execute"] += len(invalid)
    rejection_counters["unfixable_after_correction"] = len(invalid)

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
            rows = _capture_result_rows(
                cand["expected_sql"], spark, catalog, schema,
                w=w, warehouse_id=warehouse_id,
            )
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

    for col in uc_columns:
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

    for routine in uc_routines:
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
    for match in _SQL_REFERENCE_PATTERN.finditer(sql or ""):
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

        _mv_names = set(config.get("_metric_views", []))
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
    synthetic_target = max(target_count - len(curated) - len(_existing), 5)
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

    prompt = format_mlflow_template(
        BENCHMARK_GENERATION_PROMPT,
        domain=domain,
        target_count=synthetic_target,
        categories=json.dumps(BENCHMARK_CATEGORIES),
        **ctx,
    )
    if existing_questions_context:
        prompt += existing_questions_context

    response = _call_llm_for_scoring(
        w, prompt,
        prompt_name=get_registered_prompt_name("benchmark_generation"),
    )
    raw_benchmarks: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])

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

        # MV guard: reject SELECT * on metric views
        _mv_names = set(config.get("_metric_views", []))
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
        target_benchmark_count=target_count,
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
