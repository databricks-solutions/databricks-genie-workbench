"""Auto-Optimize router — thin proxy bridging Workbench auth to the GSO engine."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from typing import Annotated, Any, Callable, Literal, TypeVar

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from backend.models import (
    CurrentVersionResponse,
    MvProbeResult,
    PermissionCheckResponse,
    QueryHistoryWarehouseStatus,
    QueryUsageSignal,
    SchemaAccessStatus,
    VersionMatch,
)
from backend.routers._validators import RunId, SpaceId

from backend.services.auth import get_workspace_client, get_service_principal_client, get_databricks_host
from backend.services import gso_lakebase
from backend.services import lakebase as workbench_lakebase
from backend.services.config_fingerprint import benchmark_fingerprint, config_fingerprint
from backend.services.genie_client import get_genie_space
from backend.services.model_catalog import ModelValidationError, validate_chat_model
from backend.services import mv_entitlement
from backend.services.mv_entitlement import MvProbeError
from genie_space_optimizer.backend.utils import safe_int, safe_float, safe_finite, safe_json_parse
from genie_space_optimizer.common.accuracy import (
    compute_run_scores,
    derived_accuracy as _canonical_derived_accuracy,
)
from genie_space_optimizer.common.genie_client import user_can_edit_space
from genie_space_optimizer.integration import (
    trigger_optimization,
    apply_optimization,
    discard_optimization,
    preview_revert_options,
    revert_optimization,
    get_lever_info,
    IntegrationConfig,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auto-optimize")


def _genie_benchmark_url(
    host: str | None,
    space_id: str | None,
    workspace_id: int | str | None = None,
) -> str | None:
    """Build a Databricks UI deep link to a Genie Agent's Benchmarks tab."""
    if not host or not space_id:
        return None
    url = f"{host.rstrip('/')}/genie/rooms/{space_id}/benchmarks"
    if workspace_id is not None and str(workspace_id):
        url += f"?o={workspace_id}"
    return url

# Lightweight column list for iterations queries — excludes rows_json (megabytes per row).
# Bug #2: evaluated_count / excluded_count MUST be
# included so the frontend can compute `accuracy = correct / evaluated` without
# falling back to total_questions (the original Bug #2 regression).
#
# The V2 list is what we WANT. The LEGACY list is what pre-migration Delta tables
# actually have. `_select_iterations_delta` tries V2 first, then degrades to
# LEGACY when the table is behind the GSO job's _migrate_add_columns. This keeps
# the Workbench UI rendering scores when the job bundle and the app are on
# slightly different deploy versions.
_ITER_COLS = _ITER_COLS_V2 = (
    "iteration, eval_scope, overall_accuracy, total_questions, correct_count, "
    "evaluated_count, excluded_count, "
    "scores_json, failures_json, thresholds_met, lever, "
    "reflection_json, rolled_back, "
    # GSO v2 Phase 6 — native official eval-run metadata. The Workbench
    # surfaces these as num_needs_review / eval_run_id / eval_run_status. A
    # table behind the Phase-6 ALTER degrades to _ITER_COLS_LEGACY (these
    # become None in the response — TS-safe no-op).
    "num_needs_review, eval_run_id, eval_run_status"
)
_ITER_COLS_LEGACY = (
    "iteration, eval_scope, overall_accuracy, total_questions, correct_count, "
    "scores_json, failures_json, thresholds_met, lever, "
    "reflection_json"
)

# Lever names — matches GSO common/config.py
LEVER_NAMES: dict[int, str] = {
    0: "Legacy Enrichment",
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Instructions & Examples",
    6: "SQL Expressions",
}

_TERMINAL_RUN_STATUSES = {
    "CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED",
    "APPLIED", "DISCARDED", "SKIPPED",
}

# GSO v2 (arch §5.1 / §7.4) — the closed set of typed loop terminal reasons the
# 03_optimize controller stamps and `publish_and_audit` records as the run's
# convergence_reason (never collapsed). This supersedes the free-text
# convergence_reason for the UI; the typed `terminalReason` field is derived by
# validating convergence_reason against this set (legacy free-text reasons and
# in-progress runs ⇒ None). This includes evaluation-budget, configuration,
# and controller-state hard stops. Mirrored on the frontend as the
# `GSOTerminalReason` union.
_TYPED_TERMINAL_REASONS = (
    "TARGET_REACHED",
    "MAX_ATTEMPTS",
    "NO_NEW_HYPOTHESIS",
    "EVAL_INVALID",
    "CONFIG_VALIDATION_FAILED",
    "LOOP_STATE_INVALID",
    "EVAL_BUDGET_EXHAUSTED",
    "INSUFFICIENT_VALID_BENCHMARKS",
)

# Defaults for the round-tripped loop knobs (arch §13 / D9). target_accuracy is
# on the 0–1 request scale (the job param + databricks.yml default is "0.90");
# the loop normalizes ≤1 to the 0–100 internal/Delta scale. max_attempts bounds
# the native patch/eval loop.
_DEFAULT_TARGET_ACCURACY = 0.90
_DEFAULT_MAX_ATTEMPTS = 3


def _typed_terminal_reason(run: dict | None) -> str | None:
    """Return the run's typed loop terminal reason, or None.

    The Phase-9 `publish_and_audit` stamps `genie_opt_runs.convergence_reason`
    with the ACTUAL terminal reason (e.g. ``TARGET_REACHED``); we validate it
    against the closed `_TYPED_TERMINAL_REASONS` set so the UI gets a typed
    value. Legacy free-text reasons (``threshold_met``, ``job_submission_error:
    …``) and in-progress runs (no reason yet) return None.
    """
    if not run:
        return None
    reason = run.get("convergence_reason")
    if reason and str(reason) in _TYPED_TERMINAL_REASONS:
        return str(reason)
    return None


def _safe_bool(value: Any) -> bool:
    """Coerce Delta/Lakebase bool-ish values without treating "false" as true."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return bool(value)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TriggerRequest(BaseModel):
    space_id: str = Field(..., pattern=r"^[0-9a-zA-Z_-]{1,128}$")
    apply_mode: str = "genie_config"
    levers: list[int] | None = None
    llm_model: str | None = Field(None, max_length=256)
    # GSO v2 loop knobs (arch §13 / D9). Both optional/nullable; when omitted the
    # job's databricks.yml defaults apply (target_accuracy "0.90", max_attempts
    # "3"). target_accuracy is the 0–1 stop-early target; max_attempts bounds the
    # native patch/eval loop. The loop stops at whichever comes first.
    target_accuracy: float | None = Field(None, ge=0.0, le=1.0)
    max_attempts: int | None = Field(None, ge=1, le=20)
    # Backward-compatible API default: callers predating the gate retain the
    # former repair behavior. The Workbench UI sends review_only explicitly.
    benchmark_policy: Literal["review_only", "repair_allowed"] = "repair_allowed"
    workload_warehouse_ids: list[
        Annotated[str, Field(pattern=r"^[0-9a-zA-Z_-]{1,128}$")]
    ] = Field(default_factory=list, max_length=20)


class MvProbeRequest(BaseModel):
    """Body for ``POST /mv/probe`` — where a metric view would be created.

    ``space_id`` is the Genie Agent whose ``data_sources.metric_views`` would be
    patched, so the probe can ask for CAN MANAGE on it. ``source_tables`` are the
    three-part names the view would read; the user needs SELECT on each.
    """

    catalog: str = Field(..., pattern=r"^[A-Za-z0-9_]{1,255}$")
    schema_name: str = Field(..., alias="schema", pattern=r"^[A-Za-z0-9_]{1,255}$")
    space_id: str = Field(..., pattern=r"^[0-9a-zA-Z_-]{1,128}$")
    source_tables: list[
        Annotated[str, Field(pattern=r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")]
    ] = Field(default_factory=list, max_length=50)
    # Consent to materialize is a separate decision from create-and-attach and is
    # never implied by it (MV-D7).
    materialize_consented: bool = False

    model_config = {"populate_by_name": True}


# PermissionCheckResponse + SchemaAccessStatus now live in `backend.models`
# alongside the rest of the API Pydantic shapes (mirrored one-to-one on the
# frontend as `GSOPermissionCheck`). See AGENTS.md §Models for the contract.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_configured() -> bool:
    return bool(os.environ.get("GSO_CATALOG")) and bool(os.environ.get("GSO_JOB_ID"))


def _delta_query(sql: str, *, strict: bool = False) -> list[dict]:
    """Execute a query against the Delta table via SQL Warehouse.

    Returns a list of dicts (rows).

    By default, any error is swallowed and `[]` is returned (legacy behavior —
    most callers only need best-effort reads). Pass ``strict=True`` to re-raise
    the underlying exception so the caller can distinguish "query failed" from
    "table is empty" — required for `_select_iterations_delta` which needs to
    detect the pre-migration schema-drift case.
    """
    config = _build_gso_config()
    if not config.warehouse_id:
        return []
    try:
        from genie_space_optimizer.common.warehouse import sql_warehouse_query
        ws = get_service_principal_client()
        df = sql_warehouse_query(ws, config.warehouse_id, sql)
        if df.empty:
            return []
        return df.to_dict(orient="records")
    except Exception as exc:
        if strict:
            raise
        logger.warning("Delta query failed: %s", exc, exc_info=True)
        return []


_T = TypeVar("_T")


async def _offload(fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    """Run a blocking callable on a worker thread so it never stalls the loop.

    The GSO read path is entirely synchronous — every ``_delta_query`` blocks
    on a SQL-Warehouse ``execute_statement`` (up to ``wait_timeout``), and the
    Jobs-API progress read blocks on a control-plane RPC. These were being
    called directly inside ``async def`` handlers, so the single asyncio event
    loop serialized all six monitoring polls (~15 warehouse statements every
    5s) onto one thread. Wrapping each blocking call in ``asyncio.to_thread``
    restores concurrency: independent polls now overlap instead of queueing.
    """
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _delta_query_async(sql: str, *, strict: bool = False) -> list[dict]:
    """Async wrapper around the blocking ``_delta_query`` (off the event loop)."""
    return await _offload(_delta_query, sql, strict=strict)


def _delta_table(name: str) -> str:
    """Return fully-qualified Delta table name for a GSO table."""
    config = _build_gso_config()
    return f"{config.catalog}.{config.schema_name}.{name}"


# Bug #2 regression (April 2026): `_ITER_COLS_V2` requires columns that
# only land on the Delta table when the GSO job's `_migrate_add_columns`
# runs (see `packages/genie-space-optimizer/.../optimization/state.py`). If
# the app wheel and the job wheel are on different deploy versions — e.g. the
# app was redeployed with the new SELECT before the bundle-deployed job ran
# its first migrated run — the V2 SELECT fails with UNRESOLVED_COLUMN and the
# UI goes blank. We disambiguate "real error / legacy schema" from "empty
# table" with the module-level flag below so the second SELECT doesn't run on
# every empty-iterations run (first-time CONVERGED, brand-new runs, etc.).
_iterations_schema_legacy: bool | None = None  # None = unknown, True = pre-migration, False = migrated


def _reset_iterations_schema_cache() -> None:
    """Test helper — resets the process-wide schema state."""
    global _iterations_schema_legacy
    _iterations_schema_legacy = None


def probe_iterations_schema() -> str:
    """Check the genie_opt_iterations Delta table schema at app startup.

    Returns one of: "ok", "legacy", "unconfigured", "unreachable". Emits an
    ERROR log in the "legacy" case so oncall sees the schema-drift warning
    when the app boots on an un-migrated workspace (Bug #2 regression).
    Designed to be called once from FastAPI startup — all errors are
    swallowed so a probe failure never blocks boot.
    """
    global _iterations_schema_legacy
    if not _is_configured():
        return "unconfigured"
    table = _delta_table("genie_opt_iterations")
    try:
        _delta_query(
            f"SELECT evaluated_count, excluded_count, rolled_back "
            f"FROM {table} LIMIT 0",
            strict=True,
        )
    except Exception as exc:
        if _looks_like_legacy_schema_error(exc):
            logger.error(
                "gso.runs.schema_drift_startup %s is missing Bug #2 denominator "
                "columns. The UI will fall back to stored overall_accuracy but "
                "accuracy may appear stale until the GSO job bundle redeploys "
                "and _migrate_add_columns adds evaluated_count / excluded_count "
                "/ rolled_back. err=%s",
                table,
                str(exc)[:200],
            )
            _iterations_schema_legacy = True
            return "legacy"
        logger.warning("Schema probe failed: %s", str(exc)[:200])
        return "unreachable"
    _iterations_schema_legacy = False
    logger.info("gso.runs.schema_ok %s has all Bug #2 score-selection columns", table)
    return "ok"


_LEGACY_COL_ERROR_MARKERS = (
    "UNRESOLVED_COLUMN",
    "cannot resolve",
    "evaluated_count",
    "excluded_count",
    "rolled_back",
    "observed_config_json",
)


def _looks_like_legacy_schema_error(exc: BaseException) -> bool:
    msg = str(exc)
    # Cheap: if the error mentions any of our new columns by name, or uses
    # Databricks' canonical "UNRESOLVED_COLUMN" error code, we treat it as
    # schema drift and retry with the legacy SELECT.
    return any(marker in msg for marker in _LEGACY_COL_ERROR_MARKERS)


def _select_iterations_delta(run_id: str) -> list[dict]:
    """Load iteration rows from Delta, tolerating the pre-migration schema.

    Tries `_ITER_COLS_V2` first. If the query raises what looks like a
    missing-column error (Databricks' `UNRESOLVED_COLUMN` or the column name
    echoed verbatim), retries with `_ITER_COLS_LEGACY` and flips the module
    flag so subsequent reads skip the first probe until the process restarts.
    `_derived_accuracy` handles the legacy shape transparently (falls back to
    stored `overall_accuracy` when `evaluated_count` is absent).
    """
    global _iterations_schema_legacy
    table = _delta_table("genie_opt_iterations")
    order = f"WHERE run_id = '{run_id}' ORDER BY iteration ASC, timestamp ASC"

    if _iterations_schema_legacy is True:
        return _delta_query(f"SELECT {_ITER_COLS_LEGACY} FROM {table} {order}")

    try:
        rows = _delta_query(f"SELECT {_ITER_COLS_V2} FROM {table} {order}", strict=True)
        _iterations_schema_legacy = False
        return rows
    except Exception as exc:
        if not _looks_like_legacy_schema_error(exc):
            logger.warning("Delta iterations query failed: %s", exc, exc_info=True)
            return []
        logger.warning(
            "gso.runs.schema_drift genie_opt_iterations is missing Bug #2 columns "
            "(evaluated_count / excluded_count / rolled_back). "
            "Falling back to the legacy SELECT — scores render from stored "
            "overall_accuracy. Redeploy the GSO job bundle so "
            "_migrate_add_columns can ALTER TABLE ADD COLUMN. err=%s",
            str(exc)[:200],
        )
        _iterations_schema_legacy = True
        return _delta_query(f"SELECT {_ITER_COLS_LEGACY} FROM {table} {order}")


# GSO v2 Phase 7/8 loop-state columns on genie_opt_iterations (arch §7.4). These
# are read by the /loop-state endpoint and merged onto /iterations rows for the
# Attempt Ladder/Ledger (Phases 11–14). They land only after the Phase-7/8
# additive migration, so the read is tolerant: a pre-migration table raises
# UNRESOLVED_COLUMN and we return [] (the new fields are all optional/nullable,
# so legacy runs simply omit them). `config_json` is included here (not in the
# shared _ITER_COLS_V2) so the high-frequency status poll never pays for it.
_LOOP_STATE_COLS = (
    "iteration, eval_scope, lever, timestamp, overall_accuracy, "
    "attempt_no, attempt_mode, best_accuracy, best_config_version_id, "
    "current_hypothesis, next_hypothesis, do_not_repeat, "
    "decision, decision_reason, terminal_reason, "
    "surgical_attempts_used, target_accuracy, max_attempts, "
    "rolled_back, rollback_reason, is_champion, config_json"
)


def _select_loop_state_delta(run_id: str) -> list[dict]:
    """Load loop-state iteration rows from Delta, tolerating pre-migration tables.

    Returns ``[]`` when the loop-state columns are absent (legacy 6-step runs or
    a table behind the Phase-7/8 additive migration) — detected via the same
    UNRESOLVED_COLUMN heuristic as `_select_iterations_delta`. All loop-state
    fields are optional on the wire, so callers degrade to "no loop artifacts".
    """
    table = _delta_table("genie_opt_iterations")
    order = f"WHERE run_id = '{run_id}' ORDER BY iteration ASC"
    try:
        return _delta_query(f"SELECT {_LOOP_STATE_COLS} FROM {table} {order}", strict=True)
    except Exception as exc:
        if _looks_like_legacy_schema_error(exc):
            logger.info(
                "gso.runs.no_loop_state genie_opt_iterations has no Phase-7/8 "
                "loop-state columns for run %s — returning empty (legacy run or "
                "pre-migration table). err=%s",
                run_id, str(exc)[:160],
            )
        else:
            logger.warning("Delta loop-state query failed: %s", exc, exc_info=True)
        return []


def _load_latest_artifact(run_id: str, artifact_kind: str) -> dict | None:
    """Load the most-recent genie_opt_artifacts blob of ``artifact_kind``.

    Returns the parsed ``artifact_json`` payload as a dict, or None when the
    table/kind is absent (best-effort; tolerates pre-Phase-7 installs without
    the genie_opt_artifacts table). Used by the publish-record and benchmark-QC
    read paths.
    """
    rows = _delta_query(
        f"SELECT artifact_json, created_at FROM {_delta_table('genie_opt_artifacts')} "
        f"WHERE run_id = '{run_id}' AND artifact_kind = '{artifact_kind}' "
        f"ORDER BY created_at DESC LIMIT 1"
    )
    if not rows:
        return None
    payload = _safe_json_parse(rows[0].get("artifact_json"))
    return payload if isinstance(payload, dict) else None


# target_accuracy lives on TWO native scales across the loop artifacts (B3):
#   • genie_opt_iterations.target_accuracy + publish_record.target_accuracy →
#     0–100 (run_03 / run_publish_and_audit normalize ≤1 to 0–100 so the value
#     lines up with the per-attempt accuracies).
#   • run_manifest.target_accuracy + the job-run parameter → 0–1 (run_00 writes
#     the raw 0–1 param; the job param is the raw "0.90" string).
# The request + echo contract is 0–1. We therefore use SOURCE-SPECIFIC
# conversion — NEVER a value-magnitude heuristic, which would mishandle small
# valid targets (e.g. a 0.01 request is stored as 1.0 in the 0–100 column; a
# `>1` heuristic leaves it at 1.0 instead of 0.01).


def _delta_accuracy_to_unit_scale(val: Any) -> float | None:
    """Convert a 0–100 accuracy (Delta loop column / publish_record) → 0–1.

    Unconditional divide-by-100 (the source is always 0–100), so small targets
    round-trip correctly. Returns None when the value is missing/non-numeric.
    """
    f = _safe_float(val)
    if f is None:
        return None
    return round(f / 100.0, 4)


def _loop_state_knobs(run_id: str) -> tuple[float | None, int | None]:
    """Loop knobs from the genie_opt_iterations loop-state columns (0–100 →
    0–1). Cheap read off the latest attempt row; None when the columns/rows are
    absent (legacy run, pre-migration table, or a run with no committed attempt
    yet)."""
    table = _delta_table("genie_opt_iterations")
    try:
        rows = _delta_query(
            f"SELECT target_accuracy, max_attempts FROM {table} "
            f"WHERE run_id = '{run_id}' AND target_accuracy IS NOT NULL "
            f"ORDER BY attempt_no DESC NULLS LAST, iteration DESC LIMIT 1",
            strict=True,
        )
    except Exception as exc:
        if not _looks_like_legacy_schema_error(exc):
            logger.warning("Delta loop-knobs query failed: %s", exc, exc_info=True)
        return None, None
    if not rows:
        return None, None
    return (
        _delta_accuracy_to_unit_scale(rows[0].get("target_accuracy")),
        _safe_int(rows[0].get("max_attempts")),
    )


def _manifest_knobs(run_id: str) -> tuple[float | None, int | None]:
    """Loop knobs from the run_manifest artifact (written by 00_intake, the
    first task). target_accuracy is already on the 0–1 request scale here (run_00
    writes the raw param, unlike run_03). This is the durable run-level source
    that exists from the very start of the pipeline — see _resolve_run_knobs."""
    manifest = _load_latest_artifact(run_id, "run_manifest")
    if not manifest:
        return None, None
    ta = _safe_float(manifest.get("target_accuracy"))
    return (round(ta, 4) if ta is not None else None, _safe_int(manifest.get("max_attempts")))


def _job_param_knobs(run: dict) -> tuple[float | None, int | None]:
    """Loop knobs from the Databricks job-run parameters the trigger set.

    These exist the instant the job is submitted (before 00 writes the
    manifest), so they cover the brief QUEUED/startup window. Best-effort + on
    the 0–1 scale (the job param is the raw "0.90" string). Only consulted for
    non-terminal runs with a job_run_id, so terminal legacy runs never pay a
    Jobs API call."""
    job_run_id = run.get("job_run_id")
    if not job_run_id:
        return None, None
    try:
        sp_ws = get_service_principal_client()
        job_run = sp_ws.jobs.get_run(run_id=int(job_run_id))
        params = getattr(job_run, "job_parameters", None) or []
        by_name = {getattr(p, "name", None): getattr(p, "value", None) for p in params}
        ta = _safe_float(by_name.get("target_accuracy"))
        return (
            round(ta, 4) if ta is not None else None,
            _safe_int(by_name.get("max_attempts")),
        )
    except Exception as exc:
        logger.info("Job-run params knob read failed for run %s: %s", run.get("run_id"), str(exc)[:160])
        return None, None


def _resolve_run_knobs(run: dict) -> tuple[float | None, int | None]:
    """Resolve the loop knobs in force for a run on the 0–1 request scale (B4).

    Layered durable read so the knobs are available from trigger time, NOT only
    after the loop commits its first attempt:
      1. run_manifest artifact (cheap Delta; exists from 00 — covers ~all of the
         run's observable life: 01/02/loop/publish).
      2. loop-state columns (cheap Delta; robust if the manifest write failed).
      3. job-run parameters (Jobs API; covers the brief QUEUED→00 startup window,
         gated to non-terminal runs so terminal legacy runs skip the call).
    Returns (None, None) ONLY for true legacy runs where the knobs are
    unknowable (no manifest, no loop-state, no job params). No DDL — every source
    is an existing run-level artifact/param/column.
    """
    ta, ma = _manifest_knobs(run.get("run_id", ""))
    if ta is not None or ma is not None:
        return ta, ma
    ta, ma = _loop_state_knobs(run.get("run_id", ""))
    if ta is not None or ma is not None:
        return ta, ma
    if str(run.get("status", "")).upper() not in _TERMINAL_RUN_STATUSES:
        return _job_param_knobs(run)
    return None, None


def _build_gso_config(llm_model_override: str | None = None) -> IntegrationConfig:
    return IntegrationConfig(
        catalog=os.environ.get("GSO_CATALOG", ""),
        schema_name=os.environ.get("GSO_SCHEMA", "genie_space_optimizer"),
        warehouse_id=os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", ""),
        job_id=int(os.environ["GSO_JOB_ID"]) if os.environ.get("GSO_JOB_ID", "").isdigit() else None,
        llm_model=llm_model_override or os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6"),
    )


# Type coercion helpers — imported from genie_space_optimizer.backend.utils
# Aliases preserve call-site compatibility with the underscore-prefixed names
# that were used throughout this file before the import was added.
_safe_int = safe_int
_safe_float = safe_float
_finite = safe_finite
_safe_json_parse = safe_json_parse


# Bug #2 — canonical per-iteration accuracy derivation lives in
# `genie_space_optimizer.common.accuracy.derived_accuracy` with a thin
# router-local compatibility wrapper.
# Calls here pass this module's logger so drift lines keep showing up under
# `backend.routers.auto_optimize`, preserving existing log-scraping rules
# and the caplog-scoped unit tests in `test_auto_optimize_router.py`.
def _derived_accuracy(
    iter_row: dict | None,
    *,
    run_id: str | None = None,
    iteration: int | None = None,
) -> float | None:
    """Thin re-export of ``common.accuracy.derived_accuracy`` with this
    module's logger pre-bound. Kept as an underscore-prefixed alias so
    existing call sites and test imports (``from backend.routers.auto_optimize
    import _derived_accuracy``) continue to work unchanged."""
    return _canonical_derived_accuracy(
        iter_row, run_id=run_id, iteration=iteration, logger=logger,
    )


def _parse_detail(stage: dict) -> dict:
    """Parse detail_json column from a stage row into a dict."""
    raw = stage.get("detail_json")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


# ---------------------------------------------------------------------------
# Step summary & IO builders (ported from GSO routes/runs.py)
# ---------------------------------------------------------------------------


def _collect_all_preflight_detail(stages_rows: list[dict]) -> dict:
    """Merge detail_json from ALL PREFLIGHT stages."""
    merged: dict[str, Any] = {}
    for s in stages_rows:
        if str(s.get("stage", "")).startswith("PREFLIGHT"):
            merged.update(_parse_detail(s))
    return merged


def _resolve_parsed_space(config_snapshot: dict) -> dict:
    """Extract the parsed space config for table/column counts."""
    if not isinstance(config_snapshot, dict) or not config_snapshot:
        return {}
    parsed = config_snapshot.get("_parsed_space")
    if isinstance(parsed, dict) and parsed:
        return parsed
    ss = config_snapshot.get("serialized_space")
    if isinstance(ss, str):
        try:
            ss = json.loads(ss)
        except (json.JSONDecodeError, TypeError):
            ss = None
    if isinstance(ss, dict) and ss:
        return ss
    if "data_sources" in config_snapshot:
        return config_snapshot
    return {}


def _extract_proactive_changes(matching: list[dict]) -> dict:
    """Scan matched stages for proactive enrichment results."""
    proactive: dict = {}
    for s in matching:
        stage_name = str(s.get("stage", ""))
        d = _parse_detail(s)
        if not d:
            continue
        if "DESCRIPTION_ENRICHMENT" in stage_name:
            proactive["descriptionsEnriched"] = d.get("total_enriched", 0)
            proactive["tablesEnriched"] = d.get("tables_enriched", 0)
        elif "JOIN_DISCOVERY" in stage_name:
            proactive["joinSpecsDiscovered"] = d.get("total_applied", 0)
        elif "SPACE_METADATA" in stage_name:
            proactive["spaceDescriptionGenerated"] = d.get("description_generated", False)
            proactive["sampleQuestionsGenerated"] = d.get("questions_count", 0)
        elif "SPACE_QUALITY" in stage_name:
            proactive["spaceQualityEnrichments"] = d.get("applied_count", 0)
        elif "INSTRUCTION_SEED" in stage_name:
            proactive["instructionsSeeded"] = d.get("instructions_seeded", False)
        elif "PROMPT_MATCH" in stage_name:
            proactive["promptsMatched"] = d.get("total_matched", 0)
        elif "EXAMPLE_SQL" in stage_name:
            proactive["exampleSqlsMined"] = d.get("total_mined", 0)
    return proactive


def _build_stage_timeline(matching: list[dict]) -> list[dict[str, Any]]:
    """Compact stage timeline for UI drill-down."""
    events: list[dict[str, Any]] = []
    for s in matching:
        events.append({
            "stage": s.get("stage"),
            "status": str(s.get("status", "")).lower(),
            "startedAt": _isoformat(s.get("started_at")),
            "completedAt": _isoformat(s.get("completed_at")),
            "durationSeconds": _safe_float(s.get("duration_seconds")),
            "errorMessage": s.get("error_message"),
        })
    return events


def _build_step_summary(
    defn: dict, matching: list[dict], iterations_rows: list[dict], run_data: dict,
    *, stages_rows: list[dict] | None = None,
) -> str | None:
    """Build human-readable summary for a pipeline step."""
    if not matching:
        return None
    step_name = defn["name"]
    detail: dict = {}
    for s in matching:
        detail.update(_parse_detail(s))

    if step_name in _INTAKE_STEP_NAMES:
        all_pf = _collect_all_preflight_detail(stages_rows or [])
        tables_val = _safe_int(detail.get("table_count")) or _safe_int(all_pf.get("table_count")) or _safe_int(all_pf.get("table_ref_count"))
        columns_val = _safe_int(detail.get("columns_collected")) or _safe_int(detail.get("columnsCollected")) or _safe_int(all_pf.get("columns_collected"))
        instr_val = _safe_int(detail.get("instruction_count")) or _safe_int(all_pf.get("instruction_count"))
        bench_val = _safe_int(detail.get("benchmark_count")) or _safe_int(all_pf.get("benchmark_count")) or _safe_int(run_data.get("benchmark_count"))
        return f"Analyzed {tables_val or '?'} tables, {columns_val or '?'} columns, {instr_val or '?'} instructions, {bench_val or '?'} sample questions"
    if step_name in _QC_STEP_NAMES:
        # GSO v2 (01_benchmark_qc_and_repair): benchmark validity/repair summary
        # from the stage detail (valid_count / repair_tries_used / window_status).
        valid = _safe_int(detail.get("valid_count"))
        tries = _safe_int(detail.get("repair_tries_used"))
        window = detail.get("window")
        window_status = detail.get("window_status") or (
            window.get("status") if isinstance(window, dict) else None
        )
        parts = [f"{valid} valid benchmark questions" if valid is not None else "Benchmark QC complete"]
        if tries:
            parts.append(f"{tries} repair sweep(s)")
        if window_status:
            parts.append(f"window: {window_status}")
        return ", ".join(parts)
    if step_name in _BASELINE_STEP_NAMES:
        baseline_iter = next((r for r in iterations_rows if _safe_int(r.get("iteration")) == 0), None)
        if not baseline_iter:
            return None
        # GSO v2 Phase 6 — assessment-centric summary. Accuracy is the official
        # num_correct / num_questions (no judges); NEEDS_REVIEW rows are
        # surfaced distinctly rather than folded into a pass/fail bucket.
        num_questions = _safe_int(baseline_iter.get("total_questions")) or 0
        num_correct = _safe_int(baseline_iter.get("correct_count")) or 0
        num_needs_review = _safe_int(baseline_iter.get("num_needs_review")) or 0
        if num_questions > 0:
            score = f"{100.0 * num_correct / num_questions:.1f}"
        else:
            score = f"{_finite(baseline_iter.get('overall_accuracy', 0)):.1f}"
        correct_str = str(num_correct) if num_correct else "?"
        denom_str = str(num_questions) if num_questions else "?"
        nr_note = f", {num_needs_review} need review" if num_needs_review else ""
        return (
            f"Scored {num_questions or '?'} benchmark questions with the native "
            f"Genie evaluation. Baseline accuracy: {score}% "
            f"({correct_str}/{denom_str} correct{nr_note})"
        )
    if step_name in _ENRICHMENT_STEP_NAMES:
        descriptions = _safe_int(detail.get("descriptions_enriched")) or 0
        joins = _safe_int(detail.get("joins_discovered")) or 0
        examples = _safe_int(detail.get("examples_mined")) or 0
        instructions = 1 if detail.get("instructions_seeded") else 0
        sql_expressions = _safe_int(detail.get("sql_expressions_seeded")) or 0
        total = _safe_int(detail.get("total_enrichments")) or (descriptions + joins + instructions + examples + sql_expressions)
        parts: list[str] = []
        if descriptions:
            parts.append(f"{descriptions} descriptions")
        if joins:
            parts.append(f"{joins} joins")
        if instructions:
            parts.append(f"{instructions} instructions")
        if examples:
            parts.append(f"{examples} example SQLs")
        if sql_expressions:
            parts.append(f"{sql_expressions} SQL expressions")
        breakdown = ", ".join(parts) if parts else "no changes"
        return f"Applied {total} proactive enrichments: {breakdown}"
    if step_name in _OPTIMIZE_STEP_NAMES:
        patches = detail.get("patches_applied", 0)
        levers_accepted = detail.get("levers_accepted", [])
        before = f"{_finite(run_data.get('baseline_accuracy', 0)):.1f}" if run_data.get("baseline_accuracy") else "?"
        after = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        return f"Applied {patches} optimizations across {len(levers_accepted) if isinstance(levers_accepted, list) else '?'} categories. Score improved from {before}% to {after}%"
    if step_name in _PUBLISH_STEP_NAMES:
        score = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        return f"Final evaluation complete. Optimized score: {score}%."
    return None


def _build_step_io(
    defn: dict, matching: list[dict], iterations_rows: list[dict], run_data: dict,
    *, stages_rows: list[dict] | None = None,
) -> tuple[dict | None, dict | None]:
    """Build rich inputs/outputs for pipeline step drill-down."""
    if not matching:
        return None, None
    step_name = defn["name"]
    detail: dict[str, Any] = {}
    for s in matching:
        detail.update(_parse_detail(s))
    timeline = _build_stage_timeline(matching)

    raw_snap = run_data.get("config_snapshot")
    config_snapshot: dict = {}
    if isinstance(raw_snap, dict):
        config_snapshot = raw_snap
    elif isinstance(raw_snap, str):
        parsed = _safe_json_parse(raw_snap)
        config_snapshot = parsed if isinstance(parsed, dict) else {}

    if step_name in _INTAKE_STEP_NAMES:
        all_pf = _collect_all_preflight_detail(stages_rows or [])
        parsed_space = _resolve_parsed_space(config_snapshot)
        ds = parsed_space.get("data_sources", {}) if isinstance(parsed_space, dict) else {}
        tables = ds.get("tables", []) if isinstance(ds, dict) else []
        functions = ds.get("functions", []) if isinstance(ds, dict) else []
        instr_node = parsed_space.get("instructions", {}) if isinstance(parsed_space, dict) else {}
        text_instructions = instr_node.get("text_instructions", []) if isinstance(instr_node, dict) else []
        examples = instr_node.get("example_question_sqls", []) if isinstance(instr_node, dict) else []
        sample_questions: list[str] = []
        for ex in examples:
            q = str(ex.get("question") or "").strip() if isinstance(ex, dict) else ""
            if q:
                sample_questions.append(q)
        table_count = _safe_int(detail.get("table_count")) or _safe_int(all_pf.get("table_count")) or _safe_int(all_pf.get("table_ref_count")) or len(tables)
        function_count = _safe_int(detail.get("function_count")) or _safe_int(all_pf.get("function_count")) or len(functions)
        instruction_count = _safe_int(detail.get("instruction_count")) or _safe_int(all_pf.get("instruction_count")) or len(text_instructions)
        sample_q_count = _safe_int(detail.get("benchmark_count")) or _safe_int(all_pf.get("benchmark_count")) or _safe_int(detail.get("sample_question_count")) or len(sample_questions)
        prefetched = config_snapshot.get("_prefetched_uc_metadata", {}) if isinstance(config_snapshot, dict) else {}
        uc_columns = prefetched.get("uc_columns", []) if isinstance(prefetched, dict) else []
        uc_tags = prefetched.get("uc_tags", []) if isinstance(prefetched, dict) else []
        column_samples: list[str] = []
        for col in (uc_columns[:12] if isinstance(uc_columns, list) else []):
            if not isinstance(col, dict):
                continue
            t_name = str(col.get("table_name") or col.get("table") or "").strip()
            c_name = str(col.get("column_name") or col.get("column") or "").strip()
            if t_name and c_name:
                column_samples.append(f"{t_name}.{c_name}")
            elif c_name:
                column_samples.append(c_name)
        columns_collected = _safe_int(detail.get("columns_collected")) or _safe_int(detail.get("columnsCollected"))
        if columns_collected is None:
            columns_collected = len(uc_columns) if isinstance(uc_columns, list) else 0
        tags_collected = _safe_int(detail.get("tags_collected")) or _safe_int(detail.get("tagsCollected"))
        if tags_collected is None:
            tags_collected = len(uc_tags) if isinstance(uc_tags, list) else 0
        return (
            {"spaceId": run_data.get("space_id"), "domain": run_data.get("domain"), "catalog": run_data.get("catalog"), "schema": run_data.get("uc_schema")},
            {"tableCount": table_count, "functionCount": function_count, "instructionCount": instruction_count, "sampleQuestionCount": sample_q_count, "columnsCollected": columns_collected, "tagsCollected": tags_collected, "columnSamples": column_samples, "stageEvents": timeline},
        )

    if step_name in _QC_STEP_NAMES:
        # GSO v2 (01_benchmark_qc_and_repair) — surface the repair/window detail
        # the stage stamped. Richer QC metadata (validity / contamination /
        # 30–40 window) is served by the /benchmark-changes `qc` field.
        return (
            {"spaceId": run_data.get("space_id")},
            {
                "validCount": _safe_int(detail.get("valid_count")),
                "repairTriesUsed": _safe_int(detail.get("repair_tries_used")),
                "windowStatus": detail.get("window_status"),
                "stageEvents": timeline,
            },
        )

    if step_name in _BASELINE_STEP_NAMES:
        baseline_iter = next((r for r in iterations_rows if _safe_int(r.get("iteration")) == 0 and str(r.get("eval_scope", "")).lower() == "full"), None)
        if not baseline_iter:
            return None, {"stageEvents": timeline}
        rows_json = baseline_iter.get("rows_json", [])
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                rows_json = []
        if not isinstance(rows_json, list):
            rows_json = []
        # GSO v2 Phase 6 — emit an official assessment/reason summary instead
        # of the retired per-judge scores. State is GOOD / BAD / NEEDS_REVIEW;
        # `assessment_reasons` replaces the per-judge verdict columns.
        #
        # Counts come from the lightweight iteration columns (always loaded);
        # the reason aggregation + sample rows are best-effort from rows_json,
        # which is only present when the caller attaches it (get_run does).
        total_questions = _safe_int(baseline_iter.get("total_questions")) or 0
        correct_count = _safe_int(baseline_iter.get("correct_count")) or 0
        needs_review_count = _safe_int(baseline_iter.get("num_needs_review"))
        reason_counts: dict[str, int] = {}
        sample_rows: list[dict[str, Any]] = []
        row_assessment_counts = {"GOOD": 0, "BAD": 0, "NEEDS_REVIEW": 0}
        for row in rows_json:
            if not isinstance(row, dict):
                continue
            a = str(row.get("assessment") or "").upper()
            if a in row_assessment_counts:
                row_assessment_counts[a] += 1
            raw_reasons = row.get("assessment_reasons")
            if isinstance(raw_reasons, str):
                try:
                    raw_reasons = json.loads(raw_reasons)
                except (json.JSONDecodeError, TypeError):
                    raw_reasons = []
            row_reasons = [str(r).strip() for r in raw_reasons if str(r).strip()] if isinstance(raw_reasons, list) else []
            for r in row_reasons:
                reason_counts[r] = reason_counts.get(r, 0) + 1
            if len(sample_rows) < 5:
                question = ""
                if isinstance(row.get("inputs"), dict):
                    question = str(row.get("inputs", {}).get("question") or "").strip()
                if not question:
                    question = str(row.get("inputs/question") or row.get("question") or "").strip()
                sample_rows.append({
                    "question": question,
                    "assessment": a or None,
                    "reasons": row_reasons,
                    "matchType": row.get("outputs", {}).get("comparison", {}).get("match_type") if isinstance(row.get("outputs"), dict) else None,
                })
        top_reasons = [
            {"reason": reason, "count": count}
            for reason, count in sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:8]
        ]
        # Prefer the persisted column count; fall back to the row tally.
        if needs_review_count is None:
            needs_review_count = row_assessment_counts["NEEDS_REVIEW"] or None
        good_count = correct_count or row_assessment_counts["GOOD"]
        nr = needs_review_count or 0
        bad_count = row_assessment_counts["BAD"] or max(0, total_questions - good_count - nr)
        assessment_counts = {"GOOD": good_count, "BAD": bad_count, "NEEDS_REVIEW": nr}
        failed_count = bad_count
        # Native eval-run surfacing (reuses the StepDetailContent
        # evaluationRunUrl hook). The benchmark eval ran on the live Genie
        # Agent, so link its Benchmarks tab; the SDK exposes no per-eval-run
        # deep link.
        host = get_databricks_host()
        space_id = run_data.get("space_id")
        evaluation_run_url = _genie_benchmark_url(host, space_id)
        return (
            {"benchmarkCount": total_questions, "iteration": 0},
            {
                "assessmentSummary": assessment_counts,
                "assessmentReasons": top_reasons,
                "totalQuestions": total_questions,
                "correctCount": correct_count,
                "needsReviewCount": needs_review_count,
                "failedCount": failed_count,
                "evalRunId": baseline_iter.get("eval_run_id") or None,
                "evalRunStatus": baseline_iter.get("eval_run_status") or None,
                "evaluationRunUrl": evaluation_run_url,
                "invalidBenchmarkCount": _safe_int(detail.get("invalid_benchmark_count")),
                "permissionBlockedCount": _safe_int(detail.get("permission_blocked_count")),
                "unresolvedColumnCount": _safe_int(detail.get("unresolved_column_count")),
                "harnessRetryCount": _safe_int(detail.get("harness_retry_count")),
                "sampleQuestions": sample_rows,
                "stageEvents": timeline,
            },
        )

    if step_name in _ENRICHMENT_STEP_NAMES:
        proactive = _extract_proactive_changes(matching)
        return (
            {"spaceId": run_data.get("space_id")},
            {"proactiveChanges": proactive if proactive else None, "enrichmentModelId": detail.get("enrichment_model_id"), "totalEnrichments": detail.get("total_enrichments", 0), "enrichmentSkipped": detail.get("enrichment_skipped", False), "stageEvents": timeline},
        )

    if step_name in _OPTIMIZE_STEP_NAMES:
        patches_applied = detail.get("patches_applied") or detail.get("patches_count")
        iteration_counter = detail.get("iteration_counter") or run_data.get("best_iteration")
        return (
            {"leverCountConfigured": len(run_data.get("levers", [])) if isinstance(run_data.get("levers"), list) else None, "maxIterations": run_data.get("max_iterations")},
            {"patchesApplied": patches_applied, "leversAccepted": detail.get("levers_accepted", []), "leversRolledBack": detail.get("levers_rolled_back", []), "iterationCounter": iteration_counter, "baselineAccuracy": run_data.get("baseline_accuracy"), "bestAccuracy": _safe_float(run_data.get("best_accuracy")), "stageEvents": timeline},
        )

    if step_name in _PUBLISH_STEP_NAMES:
        return (
            {"bestIteration": run_data.get("best_iteration")},
            {"bestAccuracy": _safe_float(run_data.get("best_accuracy")), "convergenceReason": run_data.get("convergence_reason"), "terminalReason": _typed_terminal_reason(run_data), "stageEvents": timeline},
        )

    return None, {"stageEvents": timeline}


# ---------------------------------------------------------------------------
# Lever builders (ported from GSO routes/runs.py)
# ---------------------------------------------------------------------------


def _patch_for_ui(row: dict) -> dict[str, Any]:
    """Convert patch table row to compact UI object."""
    return {
        "patchType": row.get("patch_type"),
        "scope": row.get("scope"),
        "riskLevel": row.get("risk_level"),
        "targetObject": row.get("target_object"),
        "rolledBack": _safe_bool(row.get("rolled_back")),
        "rollbackReason": row.get("rollback_reason"),
        "command": _safe_json_parse(row.get("command_json")),
        "patch": _safe_json_parse(row.get("patch_json")),
        "appliedAt": str(row.get("applied_at")) if row.get("applied_at") is not None else None,
    }


def _derive_lever_status(stages: list[dict]) -> str:
    """Derive lever status from its stages."""
    statuses = {str(s.get("status", "")).upper() for s in stages}
    if "ROLLED_BACK" in statuses:
        return "rolled_back"
    if "FAILED" in statuses:
        return "failed"
    if "SKIPPED" in statuses:
        return "skipped"
    if "COMPLETE" in statuses:
        return "accepted"
    if "STARTED" in statuses:
        has_eval = any("EVAL" in str(s.get("stage", "")) for s in stages)
        return "evaluating" if has_eval else "running"
    return "pending"


def _normalize_lever_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Avoid stale active lever states after the run is terminal."""
    if status not in {"running", "evaluating"}:
        return status
    normalized = run_status.upper()
    if normalized == "FAILED":
        return "failed"
    if normalized in _TERMINAL_RUN_STATUSES:
        return "skipped"
    return status


def _build_lever_iterations(
    *, lever_num: int, lever_stages: list[dict], iterations_rows: list[dict],
    patches_rows: list[dict], run_status: str, all_stages_rows: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """Build iteration-by-iteration transparency payload for one lever."""
    by_iter: dict[int, dict[str, Any]] = {}
    lever_iterations: set[int] = set()
    for row in iterations_rows:
        if _safe_int(row.get("lever")) == lever_num:
            it = _safe_int(row.get("iteration"))
            if it is not None:
                lever_iterations.add(it)
    _non_zero_lever_iters: set[int] = set()
    if lever_num == 0:
        for row in iterations_rows:
            lv, it = _safe_int(row.get("lever")), _safe_int(row.get("iteration"))
            if lv is not None and lv != 0 and it is not None:
                _non_zero_lever_iters.add(it)
    for p in patches_rows:
        if _safe_int(p.get("lever")) == lever_num:
            it = _safe_int(p.get("iteration"))
            if it is not None and it not in _non_zero_lever_iters:
                lever_iterations.add(it)
    for stage in lever_stages:
        iteration = _safe_int(stage.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["stages"].append(stage)
        entry["detail"].update(_parse_detail(stage))
    for stage in all_stages_rows or []:
        if not str(stage.get("stage", "")).startswith("AG_"):
            continue
        iteration = _safe_int(stage.get("iteration"))
        if iteration is None or iteration not in lever_iterations:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["stages"].append(stage)
        d = _parse_detail(stage)
        if d:
            entry["detail"].update(d)
    for row in iterations_rows:
        iteration = _safe_int(row.get("iteration"))
        if iteration is None:
            continue
        if _safe_int(row.get("lever")) != lever_num and iteration not in lever_iterations:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["rows"].append(row)
    for patch_row in patches_rows:
        if _safe_int(patch_row.get("lever")) != lever_num:
            continue
        iteration = _safe_int(patch_row.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["patches"].append(_patch_for_ui(patch_row))

    payloads: list[dict[str, Any]] = []
    for iteration in sorted(by_iter.keys()):
        entry = by_iter[iteration]
        status = _normalize_lever_status_for_terminal_run(status=_derive_lever_status(entry["stages"]), run_status=run_status)
        d = entry["detail"]
        full_row = next((r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "full"), None)
        score_after = _safe_float(full_row.get("overall_accuracy")) if full_row else _safe_float(d.get("accuracy"))
        score_before = _safe_float(d.get("score_before"))
        score_delta = _safe_float(d.get("score_delta"))
        if score_delta is None and score_before is not None and score_after is not None:
            score_delta = round(score_after - score_before, 2)
        rollback_reason = d.get("reason")
        if not rollback_reason and status == "rolled_back":
            rollback_reason = "regression"
        payloads.append({
            "iteration": iteration, "status": status, "patchCount": len(entry["patches"]),
            "patchTypes": [str(p.get("patchType") or "") for p in entry["patches"] if p.get("patchType")],
            "scoreBefore": score_before, "scoreAfter": score_after, "scoreDelta": score_delta,
            "rollbackReason": rollback_reason, "patches": entry["patches"],
        })
    return payloads


def _build_levers(
    stages_rows: list[dict], *, run_status: str = "",
    configured_levers: list[int] | None = None,
    patches_rows: list[dict] | None = None, iterations_rows: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """Build lever detail from LEVER_* and AG_* stage rows."""
    lever_data: dict[int, dict] = {}
    for configured in configured_levers or []:
        try:
            lever_data[int(configured)] = {"stages": [], "detail": {}, "patches": []}
        except (TypeError, ValueError):
            continue
    iter_to_levers: dict[int, set[int]] = {}
    for p in patches_rows or []:
        it, lv = _safe_int(p.get("iteration")), _safe_int(p.get("lever"))
        if it is not None and lv is not None:
            iter_to_levers.setdefault(it, set()).add(lv)
    for row in iterations_rows or []:
        it, lv = _safe_int(row.get("iteration")), _safe_int(row.get("lever"))
        if it is not None and lv is not None and lv != 0:
            iter_to_levers.setdefault(it, set()).add(lv)
    for s in stages_rows:
        stage_name = str(s.get("stage", ""))
        if stage_name.startswith("LEVER_"):
            lever_num = s.get("lever")
            if lever_num is None:
                try:
                    lever_num = int(stage_name.split("_")[1])
                except (IndexError, ValueError):
                    continue
            try:
                lever_num = int(float(lever_num))
            except (TypeError, ValueError):
                continue
            if lever_num not in lever_data:
                lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
            lever_data[lever_num]["stages"].append(s)
            lever_data[lever_num]["detail"].update(_parse_detail(s))
            continue
        if stage_name.startswith("AG_"):
            iteration = _safe_int(s.get("iteration"))
            if iteration is None:
                continue
            ag_detail = _parse_detail(s)
            target_levers: set[int] = set(iter_to_levers.get(iteration, set()))
            if ag_detail and "levers" in ag_detail:
                for lk in ag_detail["levers"]:
                    try:
                        target_levers.add(int(lk))
                    except (TypeError, ValueError):
                        pass
            for lever_num in target_levers:
                if lever_num not in lever_data:
                    lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
                lever_data[lever_num]["stages"].append(s)
                if ag_detail:
                    lever_data[lever_num]["detail"].update(ag_detail)
    for p in patches_rows or []:
        lever_num = _safe_int(p.get("lever"))
        if lever_num is None:
            continue
        if lever_num not in lever_data:
            lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
        lever_data[lever_num]["patches"].append(_patch_for_ui(p))

    # For legacy lever 0 coverage/enrichment records, match enrichment stages
    # since they don't use LEVER_0_* naming.
    _ENRICHMENT_PREFIXES = ("ENRICHMENT", "DESCRIPTION_ENRICHMENT", "JOIN_DISCOVERY",
                            "SPACE_METADATA", "SPACE_QUALITY", "INSTRUCTION_SEED", "PROACTIVE_INSTRUCTION",
                            "EXAMPLE_SQL", "PROMPT_MATCH")
    if 0 in lever_data:
        for s in stages_rows:
            stage_name = str(s.get("stage", ""))
            if any(stage_name.startswith(pfx) for pfx in _ENRICHMENT_PREFIXES):
                lever_data[0]["stages"].append(s)
                lever_data[0]["detail"].update(_parse_detail(s))

    levers: list[dict[str, Any]] = []
    for lever_num in sorted(lever_data.keys()):
        data = lever_data[lever_num]
        ld = data["detail"]
        status = _normalize_lever_status_for_terminal_run(status=_derive_lever_status(data["stages"]), run_status=run_status)
        lever_patches = data.get("patches", [])
        rollback_reason = ld.get("reason", "regression") if status == "rolled_back" else None
        lever_iterations = _build_lever_iterations(
            lever_num=lever_num, lever_stages=data.get("stages", []),
            iterations_rows=iterations_rows or [], patches_rows=patches_rows or [],
            run_status=run_status, all_stages_rows=stages_rows,
        )
        # patchCount: prefer actual patches array length, fall back to stage detail
        patches_total = len(lever_patches)
        if patches_total == 0:
            # Also count patches aggregated across iterations
            iter_patch_total = sum(it.get("patchCount", 0) for it in lever_iterations)
            if iter_patch_total > 0:
                patches_total = iter_patch_total
            else:
                patches_total = _safe_int(ld.get("patches_applied")) or 0
        levers.append({
            "lever": lever_num, "name": LEVER_NAMES.get(lever_num, f"Lever {lever_num}"),
            "status": status, "patchCount": patches_total,
            "scoreBefore": _safe_float(ld.get("score_before")), "scoreAfter": _safe_float(ld.get("accuracy")),
            "scoreDelta": _safe_float(ld.get("score_delta")), "rollbackReason": rollback_reason,
            "patches": lever_patches, "iterations": lever_iterations,
        })
    return levers


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/health")
async def health():
    """Check if GSO is configured and operational for this deployment."""
    if not _is_configured():
        return {"configured": False, "issues": []}

    issues: list[str] = []
    config = _build_gso_config()

    # Validate job exists and SP can access it
    if config.job_id:
        try:
            sp_ws = get_service_principal_client()
            sp_ws.jobs.get(config.job_id)
        except Exception as exc:
            issues.append(f"Job {config.job_id} not accessible: {exc}")
    else:
        issues.append("GSO_JOB_ID not set")

    # Validate warehouse is configured
    if not config.warehouse_id:
        issues.append("No SQL warehouse configured (GSO_WAREHOUSE_ID or SQL_WAREHOUSE_ID)")

    return {"configured": True, "issues": issues}


@router.get("/permissions/{space_id}")
async def check_permissions(space_id: SpaceId):
    """Pre-check SP and UC permissions for a Genie Agent before optimization."""
    if not _is_configured():
        raise HTTPException(status_code=503, detail="Auto-Optimize is not configured.")

    sp_ws = get_service_principal_client()
    errors: list[str] = []

    # Resolve SP identity — config.client_id is the UUID that UC SQL accepts
    sp_display_name = ""
    sp_application_id = sp_ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
    try:
        me = sp_ws.current_user.me()
        sp_display_name = me.display_name or me.user_name or ""
        if not sp_application_id:
            sp_application_id = getattr(me, "application_id", "") or ""
    except Exception as exc:
        errors.append(f"Could not resolve SP identity: {exc}")
        logger.warning("Could not resolve SP identity", exc_info=True)

    # Validate SP UUID format
    if sp_application_id and not re.match(r'^[a-f0-9-]{36}$', sp_application_id):
        errors.append(
            f"SP identifier '{sp_application_id}' is not a UUID. "
            "Grant SQL may not work. Set DATABRICKS_CLIENT_ID to the SP's application_id."
        )
        logger.warning("SP application_id %r doesn't look like a UUID", sp_application_id)

    # Check SP CAN_MANAGE on the space
    sp_has_manage = False
    try:
        from genie_space_optimizer.common.sp_permissions import get_sp_principal_aliases
        from genie_space_optimizer.common.genie_client import sp_can_manage_space

        sp_aliases = get_sp_principal_aliases(sp_ws)
        # Try ACL fetch with user's OBO client (has access-management scope);
        # fall back to serialized-space fetch via SP if ACL read fails.
        obo_ws = get_workspace_client()
        sp_has_manage = sp_can_manage_space(obo_ws, space_id, sp_aliases, sp_client=sp_ws)
    except Exception as exc:
        errors.append(f"Could not check Genie Agent access: {exc}")
        logger.warning("Could not check SP space access for %s", space_id, exc_info=True)

    # Extract table refs from space config and probe data access
    schemas: list[SchemaAccessStatus] = []
    try:
        from genie_space_optimizer.common.genie_client import fetch_space_config
        from genie_space_optimizer.common.uc_metadata import (
            extract_genie_space_table_refs,
            get_unique_schemas,
        )
        from genie_space_optimizer.common.sp_permissions import probe_sp_required_access

        ws = get_workspace_client()
        try:
            config = fetch_space_config(ws, space_id)
        except Exception:
            config = fetch_space_config(sp_ws, space_id)
        refs = extract_genie_space_table_refs(config)
        unique_schemas = set(get_unique_schemas(refs))

        if unique_schemas:
            read_granted, _write_granted = probe_sp_required_access(sp_ws, unique_schemas)
            # UC SQL requires the application_id (UUID), not the display name
            sp_name_for_grant = sp_application_id or sp_display_name or "<service-principal>"

            for cat, sch in sorted(unique_schemas):
                granted = (cat, sch) in read_granted
                grant_sql = None
                if not granted:
                    grant_sql = (
                        f"GRANT USE CATALOG ON CATALOG `{cat}` TO `{sp_name_for_grant}`;\n"
                        f"GRANT USE SCHEMA ON SCHEMA `{cat}`.`{sch}` TO `{sp_name_for_grant}`;\n"
                        f"GRANT SELECT ON SCHEMA `{cat}`.`{sch}` TO `{sp_name_for_grant}`;\n"
                        f"GRANT EXECUTE ON SCHEMA `{cat}`.`{sch}` TO `{sp_name_for_grant}`;"
                    )
                schemas.append(SchemaAccessStatus(
                    catalog=cat,
                    schema_name=sch,
                    read_granted=granted,
                    grant_sql=grant_sql,
                ))
    except Exception as exc:
        errors.append(f"Could not probe data access: {exc}")
        logger.warning("Could not probe data access for space %s", space_id, exc_info=True)

    all_read = all(s.read_granted for s in schemas) if schemas else True
    can_start = sp_has_manage and all_read

    system_history_available = False
    try:
        from databricks.sdk.service.sql import Disposition, Format, StatementState
        from genie_space_optimizer.common.query_tags import gso_query_tags

        gso_config = _build_gso_config()
        if gso_config.warehouse_id:
            probe = sp_ws.statement_execution.execute_statement(
                warehouse_id=gso_config.warehouse_id,
                statement="SELECT 1 FROM system.query.history LIMIT 1",
                wait_timeout="10s",
                disposition=Disposition.INLINE,
                format=Format.JSON_ARRAY,
                query_tags=gso_query_tags(purpose="history_collection"),
            )
            system_history_available = bool(
                probe.status and probe.status.state == StatementState.SUCCEEDED
            )
    except Exception:
        logger.info("System query history is not available to the GSO SP")

    workload_warehouses: list[QueryHistoryWarehouseStatus] = []
    try:
        sp_visible_warehouse_ids = {
            str(getattr(warehouse, "id", "") or "").strip()
            for warehouse in sp_ws.warehouses.list()
            if str(getattr(warehouse, "id", "") or "").strip()
        }
        for warehouse in get_workspace_client().warehouses.list():
            warehouse_id = str(getattr(warehouse, "id", "") or "").strip()
            if not warehouse_id:
                continue
            workload_warehouses.append(QueryHistoryWarehouseStatus(
                warehouse_id=warehouse_id,
                name=str(getattr(warehouse, "name", "") or warehouse_id),
                accessible=warehouse_id in sp_visible_warehouse_ids,
            ))
    except Exception:
        logger.info("Warehouse query-history discovery is unavailable", exc_info=True)

    accessible_warehouses = [
        warehouse for warehouse in workload_warehouses if warehouse.accessible
    ]
    inaccessible_warehouses = [
        warehouse.name for warehouse in workload_warehouses
        if not warehouse.accessible
    ]
    query_signal = QueryUsageSignal(
        status=(
            "system_table_available"
            if system_history_available
            else "partially_available"
            if accessible_warehouses and inaccessible_warehouses
            else "warehouse_api_available"
            if accessible_warehouses
            else "unavailable"
        ),
        system_table_available=system_history_available,
        warehouse_api_available=bool(accessible_warehouses),
        warehouses=workload_warehouses,
        inaccessible_warehouses=inaccessible_warehouses,
        system_grant_sql=(
            None
            if system_history_available
            else (
                "GRANT USE CATALOG ON CATALOG system TO "
                f"`{sp_application_id or '<gso-service-principal>'}`;\n"
                "GRANT USE SCHEMA ON SCHEMA system.query TO "
                f"`{sp_application_id or '<gso-service-principal>'}`;\n"
                "GRANT SELECT ON TABLE system.query.history TO "
                f"`{sp_application_id or '<gso-service-principal>'}`;"
            )
        ),
    )

    return PermissionCheckResponse(
        sp_display_name=sp_display_name,
        sp_application_id=sp_application_id,
        sp_has_manage=sp_has_manage,
        schemas=schemas,
        can_start=can_start,
        errors=errors,
        query_usage_signal=query_signal,
    )


@router.post("/mv/probe", response_model=MvProbeResult)
async def probe_mv_entitlement(body: MvProbeRequest):
    """Probe the signed-in user's entitlement to create a metric view.

    OBO only — the SP probe at ``GET /permissions/{space_id}`` answers a
    different question (can the *job* read these schemas) and cannot authorize a
    user's write. Read-only: no DDL and no trial CREATE on any path. The consent
    record is best-effort, so a Delta write hiccup returns the probe with
    ``consent_recorded=false`` rather than losing it behind a 500.
    """
    if not _is_configured():
        raise HTTPException(status_code=503, detail="Auto-Optimize is not configured.")

    try:
        # `asyncio.to_thread` copies the current context, so the OBO ContextVar
        # the middleware set is still visible on the worker thread.
        result = await _offload(
            mv_entitlement.probe,
            catalog=body.catalog,
            schema=body.schema_name,
            space_id=body.space_id,
            source_tables=body.source_tables,
        )
    except MvProbeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except RuntimeError as exc:
        # `require_obo_workspace_client` raises this when no user token reached us.
        raise HTTPException(status_code=401, detail=str(exc))
    except Exception as exc:
        logger.exception("Metric view entitlement probe failed: %s", exc)
        raise HTTPException(status_code=500, detail="Entitlement probe failed.")

    result.materialize_consented = body.materialize_consented
    result.consent_recorded = await _offload(
        mv_entitlement.record_consent,
        result,
        materialize_consented=body.materialize_consented,
    )
    return result


@router.post("/trigger")
async def trigger(body: TriggerRequest, request: Request):
    """Trigger an optimization run for a Genie Agent."""
    if not _is_configured():
        raise HTTPException(status_code=503, detail="Auto-Optimize is not configured. Set GSO_CATALOG and GSO_JOB_ID.")

    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    selected_llm_model = (body.llm_model or "").strip() or None
    if selected_llm_model:
        try:
            selected_llm_model = validate_chat_model(selected_llm_model, client=sp_ws)
        except ModelValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    config = _build_gso_config(llm_model_override=selected_llm_model)

    try:
        result = trigger_optimization(
            space_id=body.space_id,
            ws=ws,
            sp_ws=sp_ws,
            config=config,
            user_email=request.headers.get("x-forwarded-email"),
            user_name=request.headers.get("x-forwarded-preferred-username"),
            apply_mode=body.apply_mode,
            levers=body.levers,
            target_accuracy=body.target_accuracy,
            max_attempts=body.max_attempts,
            workload_warehouse_ids=body.workload_warehouse_ids,
            benchmark_policy=body.benchmark_policy,
        )
        # Echo the resolved knobs (request value or the job default) so the UI
        # can confirm what the run will use without re-reading the job config.
        resolved_target = body.target_accuracy if body.target_accuracy is not None else _DEFAULT_TARGET_ACCURACY
        resolved_max_attempts = body.max_attempts if body.max_attempts is not None else _DEFAULT_MAX_ATTEMPTS
        return {
            "runId": result.run_id,
            "jobRunId": result.job_run_id,
            "jobUrl": result.job_url,
            "status": result.status,
            "targetAccuracy": resolved_target,
            "maxAttempts": resolved_max_attempts,
            "benchmarkPolicy": body.benchmark_policy,
        }
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        msg = str(e)
        if "already in progress" in msg:
            raise HTTPException(status_code=409, detail=msg)
        logger.exception("Trigger optimization failed: %s", e)
        raise HTTPException(status_code=500, detail=msg)
    except Exception as e:
        logger.exception("Failed to trigger optimization: %s", e)
        raise HTTPException(status_code=500, detail="Failed to start optimization job.")


# ---------------------------------------------------------------------------
# Pipeline step definitions — group raw sub-stages into the GSO v2 4-task DAG:
# 00_intake_and_snapshot → 01_benchmark_qc_and_repair → 02_optimize →
# 03_publish_and_audit. Baseline evaluation now runs inside optimize through the
# native Benchmark API; there is no separate baseline/triage task.
#
# Each task emits a top-level stage (INTAKE_AND_SNAPSHOT / BENCHMARK_QC_AND_REPAIR
# / OPTIMIZE / PUBLISH_AND_AUDIT). The optimize
# controller runs the whole native patch/eval loop in-process, so baseline/eval
# and any legacy internal LEVER_/AG_/ENRICHMENT/…
# sub-stages all roll up into step 3 "Optimize".
#
# Backward compatibility: the prefixes ALSO cover the legacy 6-notebook stage
# names so legacy runs still render — legacy PREFLIGHT folds into both intake (1)
# and QC (2) since the old preflight did both jobs; legacy BASELINE/ENRICHMENT/
# LEVER fold into Optimize (3); legacy FINALIZE/REPEATABILITY/COMPLETE fold into
# Publish & Audit (4). Legacy DEPLOY/UC_OBO_WRITE stages are
# intentionally unmatched (the deploy step is gone) — they still appear in the
# raw stage-event list. New optional fields keep legacy runs serializable.
_STEP_DEFINITIONS = [
    {"stepNumber": 1, "name": "Intake & Snapshot",      "stage_prefixes": ["INTAKE", "PREFLIGHT"]},
    {"stepNumber": 2, "name": "Benchmark QC & Repair",  "stage_prefixes": ["BENCHMARK_QC", "PREFLIGHT"]},
    {"stepNumber": 3, "name": "Optimize",               "stage_prefixes": ["OPTIMIZE", "BASELINE_EVAL", "LEVER_", "AG_", "ENRICHMENT", "PROMPT_MATCH", "DESCRIPTION_ENRICHMENT", "JOIN_DISCOVERY", "SPACE_METADATA", "SPACE_QUALITY", "INSTRUCTION_SEED", "PROACTIVE_INSTRUCTION", "EXAMPLE_SQL", "POST_ENRICHMENT_EVAL"]},
    {"stepNumber": 4, "name": "Publish & Audit",        "stage_prefixes": ["PUBLISH", "FINALIZE", "REPEATABILITY", "COMPLETE"]},
]

_TOTAL_STEPS = len(_STEP_DEFINITIONS)  # 4-task DAG

# Databricks Job task_key → 4-task rail step. The job's `databricks.yml` tasks
# map 1:1 to the rail (intake → QC → optimize → publish), so the Jobs API's
# native per-task lifecycle state is the authoritative, low-latency progress
# signal — it does NOT depend on the notebook's Delta `write_stage` rows
# becoming visible through the SQL-Warehouse read path (the serverless-write →
# warehouse-read gap that froze the rail at "0/4 · Intake & Snapshot"). Keys
# must match `packages/genie-space-optimizer/databricks.yml` exactly.
_JOB_TASK_KEY_TO_STEP: dict[str, dict[str, Any]] = {
    "intake_and_snapshot":     _STEP_DEFINITIONS[0],
    "benchmark_qc_and_repair": _STEP_DEFINITIONS[1],
    "optimize":                _STEP_DEFINITIONS[2],
    "publish_and_audit":       _STEP_DEFINITIONS[3],
}

# databricks.sdk RunLifeCycleState values (jobs.RunLifeCycleState) that mean a
# task has finished — used to count completed rail steps. A TERMINATED task is
# "done" regardless of result (result_state disambiguates success/failure);
# SKIPPED/INTERNAL_ERROR are terminal too.
_JOB_TASK_TERMINAL_LIFECYCLES = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})
# Lifecycle values that mean the task is the one actively executing.
_JOB_TASK_RUNNING_LIFECYCLES = frozenset({"RUNNING", "TERMINATING"})

# Step-name → semantic builder kind for the current 4-task UI buckets.
_INTAKE_STEP_NAMES = {"Intake & Snapshot"}
_QC_STEP_NAMES = {"Benchmark QC & Repair"}
_BASELINE_STEP_NAMES: set[str] = set()
_ENRICHMENT_STEP_NAMES: set[str] = set()
_OPTIMIZE_STEP_NAMES = {"Optimize"}
_PUBLISH_STEP_NAMES = {"Publish & Audit"}


def _derive_step_status(matching_stages: list[dict]) -> str:
    """Derive a single step status from its matching raw stages."""
    if not matching_stages:
        return "pending"
    latest = matching_stages[-1]
    status = str(latest.get("status", "")).upper()
    if status == "FAILED":
        return "failed"
    if status in {"COMPLETE", "SKIPPED", "ROLLED_BACK"}:
        return "completed"
    if status == "STARTED":
        return "running"
    return "pending"


def _total_duration(matching_stages: list[dict]) -> float | None:
    """Sum durations of all matching stages; None if no positive total."""
    total = 0.0
    for s in matching_stages:
        val = s.get("duration_seconds")
        if val is not None:
            try:
                total += float(val)
            except (TypeError, ValueError):
                pass
    return total if total > 0 else None


def _last_summary(matching_stages: list[dict]) -> str | None:
    """Return the last non-empty summary from matching stages."""
    for s in reversed(matching_stages):
        if s.get("summary"):
            return s["summary"]
    return None


def _normalize_step_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Normalize step status when the overall run is already terminal."""
    normalized = run_status.upper()
    if status == "running":
        if normalized == "FAILED":
            return "failed"
        if normalized in {"CANCELLED", "DISCARDED"}:
            return "pending"
        if normalized in _TERMINAL_RUN_STATUSES:
            return "completed"
    if status == "pending" and normalized in _TERMINAL_RUN_STATUSES:
        return "skipped"
    return status


def _stage_matches_step(stage_name: str, step_def: dict[str, Any]) -> bool:
    normalized = stage_name.upper()
    return any(
        normalized.startswith(prefix)
        for prefix in step_def["stage_prefixes"]
    )


def _matching_step_numbers(stage: dict) -> list[int]:
    stage_name = str(stage.get("stage", ""))
    return [
        int(step_def["stepNumber"])
        for step_def in _STEP_DEFINITIONS
        if _stage_matches_step(stage_name, step_def)
    ]


def _latest_observed_step_number(stages: list[dict]) -> int | None:
    latest: int | None = None
    for stage in stages:
        matches = _matching_step_numbers(stage)
        if matches:
            latest = max(matches)
    return latest


def _coerce_prior_steps_completed(
    steps: list[dict[str, Any]], stages: list[dict],
) -> None:
    """Keep the pipeline rail monotonic when old STARTED rows remain open."""
    latest = _latest_observed_step_number(stages)
    if latest is None:
        return
    for step in steps:
        step_number = _safe_int(step.get("stepNumber"))
        if (
            step_number is not None
            and step_number < latest
            and step.get("status") != "failed"
        ):
            step["status"] = "completed"


def _derive_pipeline_step_statuses(
    stages: list[dict], run_status: str,
) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []
    for step_def in _STEP_DEFINITIONS:
        matching = [
            s for s in stages
            if _stage_matches_step(str(s.get("stage", "")), step_def)
        ]
        status = _derive_step_status(matching)
        status = _normalize_step_status_for_terminal_run(
            status=status,
            run_status=run_status,
        )
        statuses.append({
            "stepNumber": step_def["stepNumber"],
            "name": step_def["name"],
            "status": status,
        })
    _coerce_prior_steps_completed(statuses, stages)
    return statuses


def _extract_lifecycle(state: Any) -> str:
    """Return the RunLifeCycleState name (e.g. 'RUNNING') from a RunState.

    SDK enums stringify as ``RunLifeCycleState.RUNNING``; older/raw shapes may
    already be a bare string. Split on '.' and upper-case so both work.
    """
    if state is None:
        return ""
    lc = getattr(state, "life_cycle_state", None)
    if lc is None:
        return ""
    return str(lc).split(".")[-1].upper()


def _job_task_progress(run: dict) -> tuple[int, str | None] | None:
    """Derive (steps_completed, current_step_name) from the Databricks Jobs API.

    Reads the job run's per-task lifecycle state — the authoritative,
    low-latency progress signal that the 4-task rail needs — and maps each
    task_key to its rail step via ``_JOB_TASK_KEY_TO_STEP``. This bypasses the
    serialized-Delta-write → SQL-Warehouse-read gap that leaves the stage table
    empty for minutes, so the rail advances the instant a task starts/finishes.

    Returns ``None`` (caller falls back to the Delta-stage derivation) when:
      • the run has no ``job_run_id`` yet (pre-submit / legacy run), or
      • the Jobs API read fails, or
      • the run reports no tasks (returns None so we don't clobber a good Delta
        derivation with an all-pending "0/4").

    Completed = tasks in a terminal lifecycle. Current = the lowest-ordered
    task that is RUNNING; if none is running but the run is still active, the
    next not-yet-terminal task in DAG order is surfaced as current.
    """
    job_run_id = run.get("job_run_id")
    if not job_run_id:
        return None
    try:
        sp_ws = get_service_principal_client()
        job_run = sp_ws.jobs.get_run(run_id=int(job_run_id))
    except Exception as exc:
        logger.info(
            "gso.status.job_progress_read_failed run=%s job_run=%s err=%s",
            run.get("run_id"), job_run_id, str(exc)[:160],
        )
        return None

    tasks = getattr(job_run, "tasks", None) or []
    # Collapse to lifecycle-by-step (a task_key appears once per run).
    lifecycle_by_step: dict[int, str] = {}
    for t in tasks:
        step_def = _JOB_TASK_KEY_TO_STEP.get(str(getattr(t, "task_key", "") or ""))
        if not step_def:
            continue
        lifecycle_by_step[int(step_def["stepNumber"])] = _extract_lifecycle(
            getattr(t, "state", None)
        )
    if not lifecycle_by_step:
        return None

    steps_completed = sum(
        1 for lc in lifecycle_by_step.values()
        if lc in _JOB_TASK_TERMINAL_LIFECYCLES
    )

    # Current step: prefer an actively-RUNNING task (lowest step number wins if
    # more than one somehow reports running); else, for a still-active run, the
    # next non-terminal step in DAG order.
    current_step_name: str | None = None
    for step_num in sorted(lifecycle_by_step):
        if lifecycle_by_step[step_num] in _JOB_TASK_RUNNING_LIFECYCLES:
            current_step_name = _STEP_DEFINITIONS[step_num - 1]["name"]
            break
    if current_step_name is None:
        run_status = str(run.get("status", "")).upper()
        if run_status not in _TERMINAL_RUN_STATUSES and steps_completed < _TOTAL_STEPS:
            current_step_name = _STEP_DEFINITIONS[steps_completed]["name"]

    return steps_completed, current_step_name


def _map_stages_to_steps(
    stages: list[dict], run: dict, iterations: list[dict],
) -> list[dict]:
    """Group raw stages by prefix into the 4-task DAG steps with rich IO."""
    run_status = str(run.get("status", "")).upper()
    statuses_by_step = {
        int(s["stepNumber"]): s["status"]
        for s in _derive_pipeline_step_statuses(stages, run_status)
    }

    steps = []
    for step_def in _STEP_DEFINITIONS:
        matching = [
            s for s in stages
            if _stage_matches_step(str(s.get("stage", "")), step_def)
        ]

        status = statuses_by_step.get(int(step_def["stepNumber"]), "pending")

        summary = _build_step_summary(step_def, matching, iterations, run, stages_rows=stages)
        inputs, outputs = _build_step_io(step_def, matching, iterations, run, stages_rows=stages)

        # Fall back to legacy summary if the new builder returned None
        if not summary:
            summary = _last_summary(matching)

        steps.append({
            "stepNumber": step_def["stepNumber"],
            "name": step_def["name"],
            "status": status,
            "durationSeconds": _total_duration(matching),
            "summary": summary,
            "inputs": inputs,
            "outputs": outputs,
        })

    return steps


@router.get("/runs/{run_id}")
async def get_run(run_id: RunId):
    """Get full run detail including stages, iterations, levers, and patches."""
    run = await gso_lakebase.load_gso_run(run_id)
    if not run and _is_configured():
        rows = await _delta_query_async(
            f"SELECT * FROM {_delta_table('genie_opt_runs')} WHERE run_id = '{run_id}'"
        )
        run = rows[0] if rows else None
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    stages = await gso_lakebase.load_gso_stages(run_id)
    if not stages and _is_configured():
        stages = await _delta_query_async(
            f"SELECT * FROM {_delta_table('genie_opt_stages')} "
            f"WHERE run_id = '{run_id}' ORDER BY started_at ASC"
        )

    # Fetch iterations (lightweight — no rows_json)
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = await _offload(_select_iterations_delta, run_id)

    # GSO v2 Phase 6 — attach the baseline iteration's rows_json so the
    # Baseline step detail can emit an assessment/reason summary + sample
    # rows. The lightweight iteration load omits the heavy rows_json column,
    # so fetch it once for iteration 0 (full) only. Not echoed in the
    # response (get_run returns steps/levers, not raw iterations).
    baseline_row = next(
        (r for r in iterations
         if _safe_int(r.get("iteration")) == 0
         and str(r.get("eval_scope", "")).lower() == "full"
         and not r.get("rows_json")),
        None,
    )
    if baseline_row is not None:
        rows_json_str = await gso_lakebase.load_gso_iteration_rows(run_id, 0, "full")
        if not rows_json_str and _is_configured():
            _dr = await _delta_query_async(
                f"SELECT rows_json FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}' AND iteration = 0 AND eval_scope = 'full' "
                f"AND rows_json IS NOT NULL ORDER BY timestamp ASC LIMIT 1"
            )
            rows_json_str = _dr[0]["rows_json"] if _dr else None
        if rows_json_str:
            baseline_row["rows_json"] = rows_json_str

    # Fetch patches for lever detail
    patches = await gso_lakebase.load_gso_patches(run_id)
    if not patches and _is_configured():
        patches = await _delta_query_async(
            f"SELECT * FROM {_delta_table('genie_opt_patches')} "
            f"WHERE run_id = '{run_id}' ORDER BY iteration, lever, patch_index"
        )

    # Canonical baseline + optimized + best_iteration. See
    # ``genie_space_optimizer.common.accuracy.compute_run_scores`` for the
    # contract — closes the "100% Optimized during baseline-only phase" UI
    # bug by enforcing full-scope-only filtering, rolled-back exclusion, and
    # the floor-at-baseline invariant.
    run_scores = compute_run_scores(iterations, run_id=run_id, logger=logger)
    baseline_accuracy = run_scores.baseline
    run["baseline_accuracy"] = baseline_accuracy

    # Build pipeline steps with rich IO
    steps = _map_stages_to_steps(stages, run, iterations)

    # Build levers with patches and iteration detail
    raw_levers = run.get("levers", [])
    if isinstance(raw_levers, str):
        try:
            raw_levers = json.loads(raw_levers)
        except (json.JSONDecodeError, TypeError):
            raw_levers = []
    if not isinstance(raw_levers, list):
        raw_levers = []
    configured_lever_ints: list[int] = []
    for lev in raw_levers:
        try:
            configured_lever_ints.append(int(lev))
        except (TypeError, ValueError):
            continue
    levers = _build_levers(
        stages, run_status=str(run.get("status", "")),
        configured_levers=configured_lever_ints,
        patches_rows=patches, iterations_rows=iterations,
    )

    # Build full stage event list (for Activity tab & Stage Timeline)
    stage_events = [
        {
            "stage": s.get("stage", ""),
            "status": s.get("status", "pending"),
            "durationSeconds": s.get("duration_seconds"),
            "startedAt": _isoformat(s.get("started_at")),
            "completedAt": _isoformat(s.get("completed_at")),
            "summary": s.get("summary"),
        }
        for s in stages
    ]

    baseline_score = run_scores.baseline
    baseline_iteration = run_scores.baseline_iteration
    optimized_score = run_scores.optimized
    best_iteration = run_scores.best_iteration

    # Build resource links (absolute URLs)
    config = _build_gso_config()
    host = get_databricks_host()
    space_id = run.get("space_id", "")
    links = []

    # Resolve workspace_id once for ?o= parameter on deep links.
    workspace_id = None
    if host:
        try:
            ws = get_workspace_client()
            workspace_id = await _offload(ws.get_workspace_id)
        except Exception:
            pass

    benchmark_url = _genie_benchmark_url(host, space_id, workspace_id)
    if benchmark_url:
        links.append({"label": "Genie Agent Benchmarks", "url": benchmark_url, "category": "genie"})

    job_run_id = run.get("job_run_id")
    job_id = run.get("job_id") or config.job_id
    if host and job_id and job_run_id:
        job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}"
        if workspace_id:
            job_url += f"?o={workspace_id}"
        links.append({"label": "Workflow Job Run", "url": job_url, "category": "job"})
    elif host and job_id:
        links.append({"label": "Workflow Job", "url": f"{host}/jobs/{job_id}", "category": "job"})

    # GSO v2 Phase 5 (D3/D7): MLflow experiment / per-iteration eval-run resource
    # links were removed (tracking is Delta-only; the experiment_* and
    # genie_opt_iterations.mlflow_run_id pointer columns were scrubbed).
    # Keep the app-facing resource list focused on operator actions: inspect
    # benchmark runs in Genie, or inspect the workflow job/run in Databricks.

    # Echo the loop knobs in force (0–1 target, surgical max_attempts), resolved
    # from the durable run-level sources (manifest / loop-state / job params).
    target_accuracy, max_attempts = (None, None)
    if _is_configured():
        target_accuracy, max_attempts = await _offload(_resolve_run_knobs, run)

    return {
        "runId": run.get("run_id"),
        "spaceId": run.get("space_id"),
        "spaceName": run.get("space_name", run.get("domain", "")),
        "status": run.get("status"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "initiatedBy": run.get("triggered_by") or "system",
        "baselineScore": baseline_score,
        "optimizedScore": optimized_score,
        "baselineIteration": baseline_iteration,
        "bestIteration": best_iteration,
        "steps": steps,
        "stages": stage_events,
        "levers": levers,
        "links": links,
        "convergenceReason": run.get("convergence_reason"),
        # GSO v2 — typed loop terminal reason + round-tripped loop knobs (echoed
        # from the loop-state columns; None for legacy runs / pre-loop reads).
        "terminalReason": _typed_terminal_reason(run),
        "targetAccuracy": target_accuracy,
        "maxAttempts": max_attempts,
    }


@router.get("/runs/{run_id}/status")
async def get_run_status(run_id: RunId):
    """Lightweight status poll endpoint."""
    run = await gso_lakebase.load_gso_run(run_id)
    if not run and _is_configured():
        rows = await _delta_query_async(
            f"SELECT * FROM {_delta_table('genie_opt_runs')} WHERE run_id = '{run_id}'"
        )
        run = rows[0] if rows else None
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    run_status_str = str(run.get("status", "")).upper()

    # Pipeline progress: prefer the Databricks Jobs API (authoritative, fast,
    # and immune to the serverless-Delta-write → SQL-Warehouse-read visibility
    # gap that froze the rail at "0/4 · Intake & Snapshot"). The 4 job tasks map
    # 1:1 to the 4 rail steps. Only when the Jobs read is unavailable (no
    # job_run_id / API error / legacy run with no tasks) do we fall back to
    # deriving progress from the Delta stage rows — which requires the extra
    # warehouse read that the Jobs path lets us skip entirely on the hot poll.
    steps_completed = 0
    current_step_name: str | None = None
    job_progress = await _offload(_job_task_progress, run) if _is_configured() else None
    if job_progress is not None:
        steps_completed, current_step_name = job_progress
    else:
        stages = await gso_lakebase.load_gso_stages(run_id)
        if not stages and _is_configured():
            stages = await _delta_query_async(
                f"SELECT * FROM {_delta_table('genie_opt_stages')} "
                f"WHERE run_id = '{run_id}' ORDER BY started_at ASC"
            )
        for step_status in _derive_pipeline_step_statuses(stages or [], run_status_str):
            status = step_status["status"]
            if status in ("completed", "skipped"):
                steps_completed += 1
            elif status == "running" and current_step_name is None:
                current_step_name = step_status["name"]
        if current_step_name is None and steps_completed < _TOTAL_STEPS:
            # Next pending step
            current_step_name = _STEP_DEFINITIONS[steps_completed]["name"]

    # Compute baseline vs optimized scores from iterations (lightweight query)
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = await _offload(_select_iterations_delta, run_id) or []

    # Canonical baseline + optimized + best_iteration. See
    # ``genie_space_optimizer.common.accuracy.compute_run_scores``.
    #
    # This endpoint is the one feeding the AutoOptimizeTab ScoreSummary
    # card. Pre-fix: a 100% slice/p0 probe row could win the optimized
    # headline mid-run (the "100% Optimized at step 2/6" screenshot bug),
    # and rolled-back iterations were not filtered. The canonical helper
    # closes both: full-scope only, exclude rolled-back, floor-at-baseline.
    run_scores = compute_run_scores(iterations, run_id=run_id, logger=logger)

    # Echo the loop knobs in force (0–1 target, surgical max_attempts), resolved
    # from durable run-level sources so they are present from trigger time
    # (manifest at 00 / job params during QUEUED), not only once the loop runs.
    target_accuracy, max_attempts = (None, None)
    if _is_configured():
        target_accuracy, max_attempts = await _offload(_resolve_run_knobs, run)

    return {
        "runId": run.get("run_id"),
        "status": run.get("status"),
        "spaceId": run.get("space_id"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "baselineScore": run_scores.baseline,
        "optimizedScore": run_scores.optimized,
        "bestIteration": run_scores.best_iteration,
        "convergenceReason": run.get("convergence_reason"),
        # GSO v2 — typed loop terminal reason (closed set; None for legacy
        # free-text reasons / in-progress runs) + round-tripped loop knobs.
        "terminalReason": _typed_terminal_reason(run),
        "targetAccuracy": target_accuracy,
        "maxAttempts": max_attempts,
        "stepsCompleted": steps_completed,
        "totalSteps": _TOTAL_STEPS,
        "currentStepName": current_step_name,
    }


@router.get("/levers")
async def list_levers():
    """List available optimization levers (1-5, excludes lever 0)."""
    all_levers = get_lever_info()
    return [lev for lev in all_levers if lev.get("id", 0) != 0]


@router.post("/runs/{run_id}/apply")
async def apply_run(run_id: RunId):
    """Apply an optimization run's results to the Genie Agent."""
    ws = get_workspace_client()
    config = _build_gso_config()

    try:
        result = await _offload(apply_optimization, run_id, ws, config)
        await _offload(_invalidate_live_fingerprint_for_run, run_id)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception("Failed to apply optimization %s: %s", run_id, e)
        raise HTTPException(status_code=500, detail="Failed to apply optimization.")


@router.post("/runs/{run_id}/discard")
async def discard_run(run_id: RunId):
    """Discard an optimization run and rollback to pre-optimization state."""
    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    config = _build_gso_config()

    try:
        result = await _offload(discard_optimization, run_id, ws, sp_ws, config)
        await _offload(_invalidate_live_fingerprint_for_run, run_id)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception("Failed to discard optimization %s: %s", run_id, e)
        raise HTTPException(status_code=500, detail="Failed to discard optimization.")


@router.post("/runs/{run_id}/revert")
async def revert_run(
    run_id: RunId,
    config_target: str = Query(
        "champion",
        description="Which configuration to revert to: "
        "'champion' (the run's winning iteration config) or 'baseline' "
        "(the run's pre-run config snapshot).",
    ),
    benchmark_target: str = Query(
        "current",
        description="Whether to preserve 'current' live benchmarks, restore "
        "the winning iteration's 'champion' benchmarks, or restore the run's "
        "pre-run 'baseline' benchmarks.",
    ),
    # Deprecated alias retained for existing callers and bookmarked URLs.
    target: str | None = Query(None, include_in_schema=False),
):
    """Revert a past run with independently selected config/benchmark scopes.

    ``config_target=champion`` uses the champion iteration's authoritative
    observed config (with legacy ``config_json`` fallback), while
    ``config_target=baseline`` uses the run's pre-run ``config_snapshot``.
    ``benchmark_target=current`` composes the live benchmark block into that
    config; ``benchmark_target=champion`` restores the benchmark block captured
    with the winning iteration; and ``benchmark_target=baseline`` restores the
    benchmark block from the pre-run snapshot. Unlike ``/discard``, this leaves
    the historical run status untouched. Active same-Space runs are refused.
    """
    resolved_config_target = target or config_target
    if resolved_config_target not in ("champion", "baseline"):
        raise HTTPException(
            status_code=422,
            detail="config_target must be 'champion' or 'baseline'.",
        )
    if benchmark_target not in ("current", "champion", "baseline"):
        raise HTTPException(
            status_code=422,
            detail="benchmark_target must be 'current', 'champion', or 'baseline'.",
        )
    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    config = _build_gso_config()

    try:
        result = await _offload(
            revert_optimization,
            run_id,
            ws,
            sp_ws,
            config,
            target=resolved_config_target,
            benchmark_target=benchmark_target,
        )
        await _offload(_invalidate_live_fingerprint_for_run, run_id)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception("Failed to revert to run %s: %s", run_id, e)
        raise HTTPException(status_code=500, detail="Failed to revert the Genie Agent.")


@router.get("/runs/{run_id}/revert-options")
async def get_revert_options(run_id: RunId):
    """Preview available revert targets and benchmark snapshot diffs."""
    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    config = _build_gso_config()
    try:
        return await _offload(
            preview_revert_options,
            run_id,
            ws,
            sp_ws,
            config,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to preview revert options for %s: %s", run_id, exc)
        raise HTTPException(status_code=500, detail="Failed to preview revert options.")


# ---------------------------------------------------------------------------
# Current version — live-config fingerprint matching
# ---------------------------------------------------------------------------
#
# Answers the History tab's "which version is the agent on right now?" question:
# the live serialized_space is fingerprinted and compared against every config
# captured by past runs (baselines + champions). A match badges that run; a
# non-match becomes ``drifted`` only when all expected captures are
# authoritative. Partial/legacy history returns ``history_incomplete``.

# Statuses in which the optimize loop may be mutating the live space — matching
# during one of these would be noise (mirrors revert's guard set).
_ACTIVE_RUN_STATUSES = {"QUEUED", "IN_PROGRESS", "RUNNING"}

_RUNS_SELECT_COLS = (
    "run_id, started_at, status, best_iteration, best_accuracy, config_snapshot, job_run_id"
)

# Short-TTL cache for the live-space fingerprints (a Genie API call), keyed by
# space AND principal: the fetch runs under the caller's OBO identity, so a
# space-only key would let one user's cached result bypass another user's
# Genie access check. The badge must move immediately after an app-initiated
# mutation, so apply/discard/revert invalidate all principals' entries for the
# space on success; returning from an external Genie UI forces a refresh, while
# ordinary repeated requests remain cached within the TTL.
_LIVE_FP_CACHE_TTL_S = 60.0
_live_fp_cache: dict[str, tuple[float, str, str, str | None]] = {}


def _principal_key() -> str:
    """Stable per-principal cache-key component derived from the caller's token.

    Hashing the token avoids an extra identity API call per request. SP-
    identity contexts (no OBO token) share one "sp" entry — same identity, so
    no authorization boundary is crossed.
    """
    try:
        token = get_workspace_client().config.token
    except Exception:
        token = None
    if not token:
        return "sp"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _invalidate_live_fingerprint(space_id: str) -> None:
    """Drop every principal's cached live fingerprints for a space."""
    prefix = f"{space_id}:"
    for key in [k for k in _live_fp_cache if k.startswith(prefix)]:
        _live_fp_cache.pop(key, None)


def _invalidate_live_fingerprint_for_run(run_id: str) -> None:
    """Resolve the run's space and clear its cached live fingerprints."""
    safe_run = run_id.replace("'", "''")
    rows = _delta_query(
        f"SELECT space_id FROM {_delta_table('genie_opt_runs')} "
        f"WHERE run_id = '{safe_run}' LIMIT 1"
    )
    if rows and rows[0].get("space_id"):
        _invalidate_live_fingerprint(str(rows[0]["space_id"]))


def _live_space_fingerprints(
    space_id: str,
    principal: str,
    *,
    force_refresh: bool = False,
) -> tuple[str | None, str | None, str | None]:
    """Return ``(config_fp, benchmark_fp, update_time)`` for the live space.

    Uses the OBO client with SP fallback (via ``get_genie_space``), cached for
    ``_LIVE_FP_CACHE_TTL_S`` per (space, principal) so repeated History loads
    do not hammer the Genie API. ``force_refresh`` bypasses that cache after
    the browser returns from Genie. Failures and unparseable configs return
    ``(None, None, None)`` and are not cached — a transient error must not hide
    the badge for a whole TTL.
    """
    key = f"{space_id}:{principal}"
    hit = _live_fp_cache.get(key)
    if not force_refresh and hit and hit[0] > time.time():
        return hit[1], hit[2], hit[3]
    try:
        snapshot = get_genie_space(space_id)
    except Exception as exc:
        logger.info("current-version live fetch failed for %s: %s", space_id, exc)
        return None, None, None
    config_fp = config_fingerprint(snapshot)
    benchmark_fp = benchmark_fingerprint(snapshot)
    if config_fp is None or benchmark_fp is None:
        logger.info("current-version live state for %s is not fingerprintable", space_id)
        return None, None, None
    update_time = snapshot.get("update_time") if isinstance(snapshot, dict) else None
    update_time_str = str(update_time) if update_time else None
    _live_fp_cache[key] = (
        time.time() + _LIVE_FP_CACHE_TTL_S,
        config_fp,
        benchmark_fp,
        update_time_str,
    )
    return config_fp, benchmark_fp, update_time_str


def _has_active_run(runs_rows: list[dict]) -> bool:
    return any(
        str(r.get("status") or "").upper() in _ACTIVE_RUN_STATUSES
        for r in runs_rows
    )


def _runs_select_sql(space_id: str) -> str:
    return (
        f"SELECT {_RUNS_SELECT_COLS} FROM {_delta_table('genie_opt_runs')} "
        f"WHERE space_id = '{space_id}' ORDER BY started_at DESC"
    )


def _reconcile_zombie_runs(space_id: str, runs_rows: list[dict]) -> list[dict]:
    """Reconcile stale QUEUED/IN_PROGRESS rows against the Jobs API.

    Mirrors the /active-run endpoint: rows whose jobs actually terminated are
    stamped terminal, so a zombie run cannot suppress current-version matching
    forever. All reconciliation writes run as the app SP — internal optimizer
    state is SP-owned, so the OBO caller's UC grants must not decide whether
    zombies get cleared (same convention as the revert path).

    Latency-bounded: the reconcile DataFrame is built from the already-fetched
    rows (no second full scan), and the post-reconcile re-read pulls only
    run_id/status — config snapshots are megabytes and don't change on a
    terminal flip. On any failure the original rows are returned
    (conservative — the run keeps being treated as active).
    """
    try:
        import pandas as pd

        from genie_space_optimizer.common.warehouse import wh_reconcile_active_runs

        config = _build_gso_config()
        sp_ws = get_service_principal_client()
        changed = wh_reconcile_active_runs(
            sp_ws, sp_ws, config.warehouse_id, pd.DataFrame(runs_rows),
            config.catalog, config.schema_name,
        )
        if not changed:
            return runs_rows
        # Zombies were stamped — refresh statuses only and merge into the rows
        # we already hold (a full re-read would re-pull every config_snapshot).
        fresh_status = {
            str(r.get("run_id")): str(r.get("status") or "")
            for r in _delta_query(
                f"SELECT run_id, status FROM {_delta_table('genie_opt_runs')} "
                f"WHERE space_id = '{space_id}'"
            )
        }
        return [
            {**row, "status": fresh_status.get(str(row.get("run_id") or ""), row.get("status"))}
            for row in runs_rows
        ]
    except Exception as exc:
        logger.warning(
            "current-version zombie reconcile failed for %s: %s",
            space_id, exc, exc_info=True,
        )
        return runs_rows


@router.get("/spaces/{space_id}/current-version")
async def get_current_version(
    space_id: SpaceId,
    refresh: bool = Query(
        False,
        description="Bypass the short live-state cache after returning from Genie UI.",
    ),
) -> CurrentVersionResponse:
    """Report which known optimization version the live Genie Agent matches.

    Fingerprints config and benchmarks independently for the live
    ``serialized_space`` and every history-visible captured run version.
    Champion identity prefers the authoritative post-PATCH
    ``observed_config_json``; submitted ``config_json`` remains usable for a
    safe positive legacy match, but its absence/mismatch can never prove
    external drift. A component is reported as drifted only when every
    expected visible version has an authoritative, fingerprintable capture
    for that component.
    """
    if not _is_configured():
        return CurrentVersionResponse(status="no_known_versions")
    config = _build_gso_config()
    if not config.warehouse_id:
        return CurrentVersionResponse(status="no_known_versions")

    # Run both warehouse reads concurrently. Strict mode is important here:
    # swallowing a champion query failure as [] would make a partial history
    # look complete and recreate the false-drift bug this endpoint guards.
    champions_sql = (
        f"SELECT i.run_id, i.config_json, i.observed_config_json "
        f"FROM {_delta_table('genie_opt_iterations')} i "
        f"INNER JOIN {_delta_table('genie_opt_runs')} r "
        f"ON i.run_id = r.run_id "
        f"WHERE r.space_id = '{space_id}' "
        f"AND i.is_champion = true "
        f"AND (i.rolled_back IS NULL OR i.rolled_back = false) "
        # Champion ties are resolved like the revert path: highest accuracy,
        # then the latest append-only row.
        f"QUALIFY row_number() OVER (PARTITION BY i.run_id "
        f"ORDER BY i.overall_accuracy DESC NULLS LAST, "
        f"i.timestamp DESC NULLS LAST, i.iteration DESC) = 1"
    )
    runs_result, champions_result = await asyncio.gather(
        _delta_query_async(_runs_select_sql(space_id), strict=True),
        _delta_query_async(champions_sql, strict=True),
        return_exceptions=True,
    )

    if isinstance(runs_result, BaseException):
        logger.warning(
            "current-version runs query failed for %s: %s",
            space_id, runs_result,
        )
        return CurrentVersionResponse(status="unavailable")
    runs_rows = runs_result

    config_history_complete = True
    benchmark_history_complete = True
    if isinstance(champions_result, BaseException):
        if not _looks_like_legacy_schema_error(champions_result):
            logger.warning(
                "current-version champion query failed for %s: %s",
                space_id, champions_result,
            )
            return CurrentVersionResponse(status="unavailable")
        # Pre-migration tables have no observed_config_json. Keep safe semantic
        # legacy matches working, but remember that a non-match is inconclusive.
        config_history_complete = False
        benchmark_history_complete = False
        legacy_champions_sql = champions_sql.replace(
            "i.config_json, i.observed_config_json",
            "i.config_json",
        )
        try:
            champion_rows = await _delta_query_async(
                legacy_champions_sql, strict=True,
            )
        except Exception as exc:
            logger.warning(
                "current-version legacy champion query failed for %s: %s",
                space_id, exc, exc_info=True,
            )
            return CurrentVersionResponse(status="unavailable")
    else:
        champion_rows = champions_result

    try:
        hidden_run_ids = await workbench_lakebase.get_hidden_optimization_run_ids(
            space_id
        )
    except Exception:
        logger.warning(
            "current-version hidden-run lookup failed for %s",
            space_id,
            exc_info=True,
        )
        hidden_run_ids = set()
    if hidden_run_ids:
        runs_rows = [
            row
            for row in runs_rows
            if str(row.get("run_id") or "") not in hidden_run_ids
        ]
        champion_rows = [
            row
            for row in champion_rows
            if str(row.get("run_id") or "") not in hidden_run_ids
        ]

    if _has_active_run(runs_rows):
        runs_rows = await _offload(_reconcile_zombie_runs, space_id, runs_rows)
    if _has_active_run(runs_rows):
        return CurrentVersionResponse(status="optimization_in_progress")
    if not runs_rows:
        return CurrentVersionResponse(status="no_known_versions")

    run_by_id = {str(r.get("run_id")): r for r in runs_rows if r.get("run_id")}
    config_matches_by_fp: dict[str, list[VersionMatch]] = {}
    benchmark_matches_by_fp: dict[str, list[VersionMatch]] = {}

    def _record(
        matches_by_fp: dict[str, list[VersionMatch]],
        fingerprint: str | None,
        run_id: str,
        target: str,
    ) -> None:
        if not fingerprint:
            return
        row = run_by_id.get(run_id) or {}
        started_at = row.get("started_at")
        matches_by_fp.setdefault(fingerprint, []).append(
            VersionMatch(
                run_id=run_id,
                target=target,  # type: ignore[arg-type]
                started_at=str(started_at) if started_at else None,
                best_accuracy=_safe_float(row.get("best_accuracy")),
            )
        )

    champion_by_run = {
        str(row.get("run_id")): row
        for row in champion_rows
        if row.get("run_id")
    }

    for run_id, row in run_by_id.items():
        snapshot = row.get("config_snapshot")
        baseline_config_fp = config_fingerprint(snapshot)
        baseline_benchmark_fp = benchmark_fingerprint(snapshot)
        _record(config_matches_by_fp, baseline_config_fp, run_id, "baseline")
        _record(
            benchmark_matches_by_fp,
            baseline_benchmark_fp,
            run_id,
            "baseline",
        )
        if baseline_config_fp is None:
            config_history_complete = False
        if baseline_benchmark_fp is None:
            benchmark_history_complete = False

        # A completed iteration selection must have one champion capture. Runs
        # that failed before selecting any iteration legitimately have only a
        # baseline version.
        if _safe_int(row.get("best_iteration")) is not None and run_id not in champion_by_run:
            config_history_complete = False
            benchmark_history_complete = False

    for row in champion_rows:
        run_id = str(row.get("run_id") or "")
        if run_id:
            observed = row.get("observed_config_json")
            submitted = row.get("config_json")
            observed_config_fp = config_fingerprint(observed)
            observed_benchmark_fp = benchmark_fingerprint(observed)
            if observed_config_fp is not None:
                _record(
                    config_matches_by_fp,
                    observed_config_fp,
                    run_id,
                    "champion",
                )
            else:
                config_history_complete = False
                # Positive semantic equality with a submitted legacy config is
                # still trustworthy; only a non-match is inconclusive.
                _record(
                    config_matches_by_fp,
                    config_fingerprint(submitted),
                    run_id,
                    "champion",
                )
            if observed_benchmark_fp is not None:
                _record(
                    benchmark_matches_by_fp,
                    observed_benchmark_fp,
                    run_id,
                    "champion",
                )
            else:
                benchmark_history_complete = False
                _record(
                    benchmark_matches_by_fp,
                    benchmark_fingerprint(submitted),
                    run_id,
                    "champion",
                )

    if not config_matches_by_fp and not benchmark_matches_by_fp:
        return CurrentVersionResponse(
            status=(
                "history_incomplete"
                if not config_history_complete or not benchmark_history_complete
                else "no_known_versions"
            )
        )

    live_config_fp, live_benchmark_fp, live_update_time = await _offload(
        _live_space_fingerprints,
        space_id,
        _principal_key(),
        force_refresh=refresh,
    )
    if live_config_fp is None or live_benchmark_fp is None:
        return CurrentVersionResponse(status="unavailable")

    def _sorted_matches(
        matches_by_fp: dict[str, list[VersionMatch]], fingerprint: str,
    ) -> list[VersionMatch]:
        matches = list(matches_by_fp.get(fingerprint) or [])
        matches.sort(key=lambda match: match.started_at or "", reverse=True)
        return matches

    config_matches = _sorted_matches(config_matches_by_fp, live_config_fp)
    benchmark_matches = _sorted_matches(benchmark_matches_by_fp, live_benchmark_fp)
    config_match = config_matches[0] if config_matches else None
    benchmark_match = benchmark_matches[0] if benchmark_matches else None
    common_keys = {
        (match.run_id, match.target)
        for match in benchmark_matches
    }
    full_matches = [
        match
        for match in config_matches
        if (match.run_id, match.target) in common_keys
    ]
    full_matches.sort(key=lambda match: match.started_at or "", reverse=True)

    common_response = {
        "config_match": config_match,
        "config_also_matches": config_matches[1:],
        "benchmark_match": benchmark_match,
        "benchmark_also_matches": benchmark_matches[1:],
        "live_update_time": live_update_time,
    }
    if full_matches:
        return CurrentVersionResponse(
            status="matched",
            current=full_matches[0],
            also_matches=full_matches[1:],
            **common_response,
        )
    if config_matches and benchmark_matches:
        return CurrentVersionResponse(status="mixed", **common_response)

    inconclusive = (
        (not config_matches and not config_history_complete)
        or (not benchmark_matches and not benchmark_history_complete)
    )
    if inconclusive:
        return CurrentVersionResponse(status="history_incomplete", **common_response)

    drifted_dimensions: list[Literal["config", "benchmarks"]] = []
    if not config_matches:
        drifted_dimensions.append("config")
    if not benchmark_matches:
        drifted_dimensions.append("benchmarks")
    return CurrentVersionResponse(
        status="drifted",
        drifted_dimensions=drifted_dimensions,
        **common_response,
    )


@router.get("/spaces/{space_id}/active-run")
async def get_active_run(space_id: SpaceId):
    """Check for an active optimization run by querying the authoritative Delta table.

    Reconciles zombie runs first (same as trigger.py), then returns active run info.
    Falls back gracefully when GSO is not configured or the warehouse is unavailable.
    """
    if not _is_configured():
        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}

    config = _build_gso_config()
    if not config.warehouse_id:
        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}

    try:
        from genie_space_optimizer.common.warehouse import (
            sql_warehouse_query,
            wh_reconcile_active_runs,
        )

        ws = get_workspace_client()
        sp_ws = get_service_principal_client()

        runs_df = sql_warehouse_query(
            ws,
            config.warehouse_id,
            f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
            f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
        )

        # Reconcile zombie runs (stale QUEUED/IN_PROGRESS with terminated jobs)
        if not runs_df.empty:
            if wh_reconcile_active_runs(
                ws, sp_ws, config.warehouse_id, runs_df,
                config.catalog, config.schema_name,
            ):
                # Re-query after reconciliation updated rows
                runs_df = sql_warehouse_query(
                    ws,
                    config.warehouse_id,
                    f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
                    f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
                )

        # Check for active runs
        _ACTIVE = {"QUEUED", "IN_PROGRESS"}
        if not runs_df.empty:
            active = runs_df[runs_df["status"].isin(list(_ACTIVE))]
            if not active.empty:
                row = active.iloc[0]
                return {
                    "hasActiveRun": True,
                    "activeRunId": str(row.get("run_id", "")),
                    "activeRunStatus": str(row.get("status", "")),
                }

        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}

    except Exception as exc:
        logger.warning("Active-run check failed for space %s: %s", space_id, exc, exc_info=True)
        # Fail open — don't block the UI if the check fails
        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}


def _enrich_run_summaries(runs: list[dict]) -> list[dict]:
    """Add the typed `terminal_reason` to each run-summary row (item 3).

    Derived from the stored `convergence_reason` (validated against the closed
    typed set); legacy free-text reasons / in-progress runs ⇒ None. The raw
    `convergence_reason` is preserved for back-compat.

    Also coerces `best_accuracy` to a float: the Delta/SQL-warehouse fallback
    path returns every column as a string (JSON_ARRAY format), and the frontend
    history table guards with `Number.isFinite`, so a string `"53.3"` would
    render as an em dash in the Champion accuracy column.
    """
    for r in runs:
        r["terminal_reason"] = _typed_terminal_reason(r)
        r["best_accuracy"] = _safe_float(r.get("best_accuracy"))
        r["best_iteration"] = _safe_int(r.get("best_iteration"))
        r["has_config_snapshot"] = _safe_bool(r.get("has_config_snapshot"))
        r["benchmark_mutation_count"] = _safe_int(
            r.get("benchmark_mutation_count")
        ) or 0
    return runs


async def load_runs_with_fallback(space_id: str) -> list[dict]:
    """Load optimization runs — Lakebase primary, Delta table fallback.

    Shared by the runs endpoint and the history endpoint.
    """
    runs = await gso_lakebase.load_gso_runs_for_space(space_id)
    if not runs:
        if not _is_configured():
            return []

        base_cols = (
            "run_id, space_id, status, started_at, completed_at, best_accuracy, "
            "best_iteration, convergence_reason, triggered_by, llm_model"
        )
        suffix = (
            "(config_snapshot IS NOT NULL AND length(config_snapshot) > 2) "
            "AS has_config_snapshot"
        )
        table = _delta_table("genie_opt_runs")
        try:
            runs = _delta_query(
                f"SELECT {base_cols}, benchmark_policy, benchmark_mutation_count, "
                f"{suffix} FROM {table} WHERE space_id = '{space_id}' "
                "ORDER BY started_at DESC",
                strict=True,
            )
        except Exception:
            # History remains readable before the next run executes the additive
            # migration. Legacy rows surface their benchmark handling as unknown.
            runs = _delta_query(
                f"SELECT {base_cols}, {suffix} FROM {table} "
                f"WHERE space_id = '{space_id}' ORDER BY started_at DESC"
            )

    hidden_run_ids = await workbench_lakebase.get_hidden_optimization_run_ids(space_id)
    visible_runs = [
        run for run in runs
        if str(run.get("run_id") or "") not in hidden_run_ids
    ]
    return _enrich_run_summaries(visible_runs)


async def _load_run_for_history_removal(run_id: str) -> dict | None:
    """Load the authoritative run envelope needed to authorize a removal."""
    run = await gso_lakebase.load_gso_run(run_id)
    if run or not _is_configured():
        return run
    rows = await _delta_query_async(
        f"SELECT run_id, space_id, status FROM {_delta_table('genie_opt_runs')} "
        f"WHERE run_id = '{run_id}' LIMIT 1",
        strict=True,
    )
    return rows[0] if rows else None


@router.get("/spaces/{space_id}/runs")
async def list_runs_for_space(space_id: SpaceId):
    """List past optimization runs for a space."""
    return await load_runs_with_fallback(space_id)


@router.delete("/runs/{run_id}/history-entry")
async def remove_run_from_history(run_id: RunId, request: Request):
    """Hide a terminal run from Workbench history without deleting GSO audit data."""
    try:
        run = await _load_run_for_history_removal(run_id)
    except Exception as exc:
        logger.warning("Failed to load run %s for history removal: %s", run_id, exc)
        raise HTTPException(
            status_code=503,
            detail="Optimization history is temporarily unavailable.",
        ) from exc

    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    status = str(run.get("status") or "").upper()
    if status in _ACTIVE_RUN_STATUSES:
        raise HTTPException(
            status_code=409,
            detail="An active optimization run cannot be removed from history.",
        )

    space_id = str(run.get("space_id") or "")
    if not space_id:
        raise HTTPException(
            status_code=409,
            detail="Run has no Genie Agent ID and cannot be removed from history.",
        )

    user_email = (
        request.headers.get("x-forwarded-email")
        or request.headers.get("x-forwarded-user")
    )
    user_groups = {
        group.strip().lower()
        for group in request.headers.get("x-forwarded-groups", "").split(",")
        if group.strip()
    }
    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    can_edit = await _offload(
        user_can_edit_space,
        ws,
        space_id,
        user_email=user_email,
        user_groups=user_groups,
        acl_client=sp_ws,
    )
    if not can_edit:
        raise HTTPException(
            status_code=403,
            detail=(
                "You need CAN_EDIT or CAN_MANAGE permission on this Genie Agent "
                "to remove its optimization history entry."
            ),
        )

    hidden_by = user_email or os.environ.get("DEV_USER_EMAIL") or "unknown"
    try:
        await workbench_lakebase.hide_optimization_run_from_history(
            run_id,
            space_id,
            hidden_by,
        )
    except Exception as exc:
        logger.warning("Failed to persist history removal for run %s: %s", run_id, exc)
        raise HTTPException(
            status_code=503,
            detail="History removal could not be persisted. Please try again.",
        ) from exc

    return {
        "status": "removed",
        "runId": run_id,
        "spaceId": space_id,
        "message": "Optimization run removed from Workbench history.",
    }


@router.get("/runs/{run_id}/iterations")
async def list_iterations(run_id: RunId):
    """Get per-iteration evaluation details for a run (excludes rows_json for performance)."""
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = await _offload(_select_iterations_delta, run_id)
    # Coerce key numeric fields — Delta fallback may return strings.
    # Bug #2: evaluated_count / excluded_count must round-trip as ints so the
    # frontend divides by the same denominator the backend uses.
    for it in iterations:
        it["overall_accuracy"] = _safe_float(it.get("overall_accuracy"))
        it["total_questions"] = _safe_int(it.get("total_questions")) or 0
        it["correct_count"] = _safe_int(it.get("correct_count")) or 0
        if it.get("evaluated_count") is not None:
            it["evaluated_count"] = _safe_int(it.get("evaluated_count"))
        if it.get("excluded_count") is not None:
            it["excluded_count"] = _safe_int(it.get("excluded_count")) or 0
        it["iteration"] = _safe_int(it.get("iteration")) or 0
        it["lever"] = _safe_int(it.get("lever"))

        # GSO v2 Phase 6 — assessment-centric counts contract. Headline
        # accuracy is `num_correct / num_questions`; the UI repoints onto
        # these official counts + the native eval-run status.
        num_correct = it["correct_count"]
        num_questions = it["total_questions"]
        evaluated = it.get("evaluated_count")
        is_official = bool(it.get("eval_run_id") or it.get("eval_run_status"))
        it["num_correct"] = num_correct
        it["num_questions"] = num_questions
        # V2 official rows report progress against the full benchmark corpus.
        # Legacy rows without eval-run metadata keep the older evaluated_count
        # fallback for back-compat.
        it["num_done"] = num_questions if is_official else (
            evaluated if evaluated is not None else num_questions
        )
        it["num_needs_review"] = (
            _safe_int(it.get("num_needs_review"))
            if it.get("num_needs_review") is not None else None
        )

        # Expose the OFFICIAL overall accuracy derived from the new counts.
        # An official row (from the native EvalRunner) carries eval-run
        # metadata; for it, accuracy is num_correct / num_questions — NOT the
        # legacy correct_count / evaluated_count denominator (which differs on
        # a partial run where num_done < num_questions). Legacy rows keep their
        # stored overall_accuracy untouched.
        if is_official and num_questions > 0:
            it["overall_accuracy"] = round(100.0 * num_correct / num_questions, 2)

        # Replace the retired per-judge `thresholds_met` with the explicit
        # single API-accuracy gate + a coarse per-iteration gate status. Phase 3
        # collapsed acceptance to one API-accuracy gate, so `thresholds_met`
        # IS the API-accuracy gate verdict.
        gate_met = it.get("thresholds_met")
        api_accuracy_gate_met = _safe_bool(gate_met)
        it["api_accuracy_gate_met"] = api_accuracy_gate_met
        if _safe_bool(it.get("rolled_back")):
            it["eval_gate_status"] = "rolled_back"
        elif api_accuracy_gate_met:
            it["eval_gate_status"] = "passed"
        else:
            it["eval_gate_status"] = "failed"

        # Drop the retired per-judge fields from the response (the TS
        # IterationRow no longer declares scores_json / thresholds_met).
        it.pop("thresholds_met", None)
        it.pop("scores_json", None)

    # GSO v2 (item 5 + Attempt Ledger) — merge the EXPLICIT champion flag +
    # loop-state fields onto each row so the UI stops re-deriving the champion
    # via idxmax(accuracy) and can render the per-attempt ledger. These come
    # from a separate tolerant loop-state read (config_json is kept off the
    # high-frequency status poll). All optional/nullable: legacy runs and
    # pre-migration tables return no loop-state rows, so rows simply omit them
    # and the UI keeps its legacy idxmax fallback.
    if iterations and _is_configured():
        loop_by_key: dict[tuple[int | None, str], dict] = {}
        for lr in await _offload(_select_loop_state_delta, run_id):
            key = (_safe_int(lr.get("iteration")), str(lr.get("eval_scope") or "").lower())
            loop_by_key[key] = lr
        if loop_by_key:
            for it in iterations:
                lr = loop_by_key.get(
                    (_safe_int(it.get("iteration")), str(it.get("eval_scope") or "").lower())
                )
                if not lr:
                    continue
                it["is_champion"] = _safe_bool(lr.get("is_champion"))
                it["config_json"] = lr.get("config_json") or None
                it["attempt_no"] = _safe_int(lr.get("attempt_no"))
                it["attempt_mode"] = lr.get("attempt_mode") or None
                it["decision"] = lr.get("decision") or None
                it["decision_reason"] = lr.get("decision_reason") or None
    return iterations


# The optimize loop scores every current attempt on the FULL benchmark.
# Pre-loop enrichment rows also use eval_scope='enrichment'; both scopes belong
# to the candidate universe of state.load_all_scored_iterations. Any other scope
# on an attempt row (e.g. an older slice/p0 probe) is NOT the authoritative
# per-attempt accuracy.
_FULL_BENCHMARK_SCOPES = frozenset({"full", "enrichment"})


def _attempt_row_sort_key(r: dict) -> tuple:
    """Deterministic recency key for collapsing duplicate rows of one attempt:
    latest iteration, then latest timestamp."""
    return (_safe_int(r.get("iteration")) or 0, str(r.get("timestamp") or ""))


def _evaluation_event_sort_key(r: dict) -> tuple:
    """Chronological key for recovering append-only iteration-0 evaluations."""
    return (str(r.get("timestamp") or ""), _safe_int(r.get("iteration")) or 0)


def _dedup_attempt_rows(loop_rows: list[dict]) -> list[dict]:
    """One authoritative full-benchmark row per ``attempt_no`` (B1).

    Filters to full-benchmark and legacy-enrichment scopes so non-full probe
    rows never become a phantom attempt, and collapses any duplicates of the same
    ``attempt_no`` to the most recent row (so accuracy/decision reflect the final
    committed eval). Returned ordered by ``attempt_no`` ascending, so ``len()`` is
    the DISTINCT attempt count.

    The baseline is NOT an attempt — it is the ladder floor served by
    ``/iterations`` (and synthesized as its own row by the UI ladder/ledger
    builders). The loop writes it as ``attempt_no=0, attempt_mode="baseline"``;
    excluding it here keeps ``attempts[]`` to real patch attempts (≥1) and stops
    a duplicate "Baseline" row from rendering in the Attempt Ledger/Ladder.
    """
    eligible = [
        r for r in loop_rows
        if str(r.get("eval_scope") or "").lower() in _FULL_BENCHMARK_SCOPES
    ]

    # The append-only iteration table can contain multiple iteration-0/full
    # rows when an Optimize task is repaired or re-entered. The earliest row is
    # the true baseline. Later timestamped rows are real official evaluations
    # and must appear in the ladder instead of being discarded with baseline.
    baseline_rows = [
        r for r in eligible
        if (_safe_int(r.get("attempt_no")) in {None, 0})
        and (_safe_int(r.get("iteration")) or 0) == 0
        and str(r.get("eval_scope") or "").lower() == "full"
    ]
    baseline_rows.sort(key=_evaluation_event_sort_key)
    baseline = baseline_rows[0] if baseline_rows else None
    recovered = [
        r for r in baseline_rows[1:]
        if str(r.get("timestamp") or "")
        and _evaluation_event_sort_key(r) != _evaluation_event_sort_key(baseline or {})
    ]

    by_attempt: dict[int, dict] = {}
    for r in eligible:
        attempt_no = _safe_int(r.get("attempt_no"))
        if attempt_no is None or attempt_no == 0:
            continue
        existing = by_attempt.get(attempt_no)
        if existing is None or _attempt_row_sort_key(r) > _attempt_row_sort_key(existing):
            by_attempt[attempt_no] = r

    next_attempt_no = max(by_attempt, default=0) + 1
    for source in recovered:
        row = dict(source)
        row["attempt_no"] = next_attempt_no
        row["attempt_mode"] = "enrichment"
        if str(row.get("decision") or "").lower() in {"", "baseline"}:
            row["decision"] = "accept"
        if str(row.get("decision_reason") or "").lower() in {"", "baseline"}:
            row["decision_reason"] = "post-baseline evaluation recovered from task re-entry"
        by_attempt[next_attempt_no] = row
        next_attempt_no += 1

    rows = [by_attempt[k] for k in sorted(by_attempt)]
    # A run-scoped champion UPDATE can flag every duplicate iteration-0 row.
    # Among returned attempts, retain only the highest flagged row as champion;
    # if the omitted earliest baseline is strictly higher, no attempt is marked
    # and the frontend correctly treats baseline as champion.
    flagged_pool = [
        row
        for row in ([baseline] if baseline else []) + rows
        if _safe_bool(row.get("is_champion"))
    ]
    if flagged_pool:
        champion = max(flagged_pool, key=lambda r: _safe_float(r.get("overall_accuracy")) or 0.0)
        for row in rows:
            row["is_champion"] = row is champion
    return rows


def _build_attempt(lr: dict) -> dict:
    """Map a loop-state iteration row → a GSOAttempt (camelCase) dict.

    ``accuracy`` / ``bestAccuracy`` stay on the 0–100 scale (as everywhere in
    the app); JSON-string columns (hypotheses, do_not_repeat) are parsed to
    objects/lists. Per-attempt ledger fields (``bestConfigVersionId``,
    ``nextHypothesis``, ``doNotRepeat``) are surfaced here in addition to the
    run-level aggregate on ``loopState`` (B2).
    """
    dnr = _safe_json_parse(lr.get("do_not_repeat"))
    return {
        "attemptNo": _safe_int(lr.get("attempt_no")),
        "attemptMode": lr.get("attempt_mode") or None,
        "iteration": _safe_int(lr.get("iteration")),
        "evalScope": lr.get("eval_scope") or None,
        "lever": _safe_int(lr.get("lever")),
        "accuracy": _safe_float(lr.get("overall_accuracy")),
        "bestAccuracy": _safe_float(lr.get("best_accuracy")),
        "decision": lr.get("decision") or None,
        # rejection/rollback explanation so a higher-accuracy-but-rolled-back
        # attempt can be explained (progress §5 resolution).
        "decisionReason": lr.get("decision_reason") or None,
        "rolledBack": _safe_bool(lr.get("rolled_back")),
        "rollbackReason": lr.get("rollback_reason") or None,
        "isChampion": _safe_bool(lr.get("is_champion")),
        "currentHypothesis": _safe_json_parse(lr.get("current_hypothesis")),
        # Per-attempt ledger fields (B2) — surfaced in addition to the run-level
        # aggregate on loopState, parsed the same way as the aggregate.
        "bestConfigVersionId": lr.get("best_config_version_id") or None,
        "nextHypothesis": _safe_json_parse(lr.get("next_hypothesis")),
        "doNotRepeat": dnr if isinstance(dnr, list) else [],
        "terminalReason": (
            lr.get("terminal_reason")
            if lr.get("terminal_reason") in _TYPED_TERMINAL_REASONS else None
        ),
    }


def _build_loop_state(attempt_rows: list[dict]) -> dict | None:
    """Run-level GSOLoopState aggregate from the per-attempt loop-state rows.

    ``targetAccuracy`` is normalized to the 0–1 request scale (matching the
    trigger contract); the per-attempt ``accuracy``/``bestAccuracy`` remain
    0–100. Returns None when there are no attempt rows (legacy / pre-loop run).
    """
    if not attempt_rows:
        return None
    # Head = the latest attempt — it carries the final best_* / terminal_reason
    # / do_not_repeat / next_hypothesis. best_accuracy is monotonic; take the
    # max defensively in case the head row missed a stamp.
    head = max(attempt_rows, key=lambda r: (_safe_int(r.get("attempt_no")) or 0))
    best_acc: float | None = None
    for r in attempt_rows:
        b = _safe_float(r.get("best_accuracy"))
        if b is not None and (best_acc is None or b > best_acc):
            best_acc = b
    terminal_reason = next(
        (r.get("terminal_reason") for r in attempt_rows
         if r.get("terminal_reason") in _TYPED_TERMINAL_REASONS),
        None,
    )
    dnr = _safe_json_parse(head.get("do_not_repeat"))
    return {
        "bestAccuracy": best_acc,
        "bestConfigVersionId": head.get("best_config_version_id") or None,
        # The loop-state column is 0–100; normalize to the 0–1 request scale.
        "targetAccuracy": _delta_accuracy_to_unit_scale(head.get("target_accuracy")),
        "maxAttempts": _safe_int(head.get("max_attempts")),
        "surgicalAttemptsUsed": _safe_int(head.get("surgical_attempts_used")),
        "terminalReason": terminal_reason,
        "doNotRepeat": dnr if isinstance(dnr, list) else [],
        "nextHypothesis": _safe_json_parse(head.get("next_hypothesis")),
        "attemptCount": len(attempt_rows),
    }


@router.get("/runs/{run_id}/loop-state")
async def get_loop_state(run_id: RunId):
    """GSO v2 03_optimize controller loop-state + per-attempt ledger (arch §7.4).

    Surfaces the Phase-7/8 loop-state columns on ``genie_opt_iterations``:
    per attempt — ``attempt_no``, ``attempt_mode`` (current rows use
    ``llm_patch``; legacy rows may use ``coverage`` / ``surgical``),
    full-benchmark accuracy, the ``best_accuracy`` staircase, ``decision`` plus
    its rejection/rollback reason, the rolled-back + champion flags, and the
    active hypothesis; run-level — ``best_accuracy``, ``best_config_version_id``,
    ``target_accuracy`` (0–1), ``max_attempts``, ``surgical_attempts_used``,
    the typed ``terminal_reason``, ``do_not_repeat``, and ``next_hypothesis``.

    Returns ``{runId, loopState, attempts}``. A legacy 6-step run (no loop-state
    columns / rows) returns ``loopState=null`` + ``attempts=[]`` so the UI can
    fall back to the classic iteration view without error.
    """
    loop_rows = await _offload(_select_loop_state_delta, run_id) if _is_configured() else []
    # Attempts = ONE authoritative full-benchmark row per attempt_no
    # (current rows use llm_patch; legacy rows may use coverage/surgical),
    # de-duped so multiple/non-full rows never
    # render as phantom attempts (B1). The baseline (iter 0, no attempt_no) is
    # the ladder floor served by /iterations — it is not itself an attempt.
    attempt_rows = _dedup_attempt_rows(loop_rows)
    attempts = [_build_attempt(r) for r in attempt_rows]  # already ordered by attempt_no
    return {
        "runId": run_id,
        "loopState": _build_loop_state(attempt_rows),
        "attempts": attempts,
    }


def _build_publish_record(payload: dict) -> dict:
    """Map a ``publish_record`` artifact payload → a camelCase GSOPublishRecord.

    The publish_record stores ``target_accuracy`` on the 0–100 scale (run_publish_
    and_audit normalizes the ≤1 job param to 0–100), so it is converted to the
    0–1 echo scale; ``improvement_trajectory`` entries are camelCased explicitly
    (their structural shape is bounded — no leaky free-text, per the §3.6
    firewall).
    """
    trajectory: list[dict] = []
    raw_traj = payload.get("improvement_trajectory")
    if isinstance(raw_traj, list):
        for t in raw_traj:
            if not isinstance(t, dict):
                continue
            trajectory.append({
                "iteration": _safe_int(t.get("iteration")),
                "attemptNo": _safe_int(t.get("attempt_no")),
                "attemptMode": t.get("attempt_mode") or None,
                "evalScope": t.get("eval_scope") or None,
                "accuracy": _safe_float(t.get("accuracy")),
                "deltaVsBaseline": _safe_float(t.get("delta_vs_baseline")),
                "bestAccuracy": _safe_float(t.get("best_accuracy")),
                "decision": t.get("decision") or None,
                "rolledBack": _safe_bool(t.get("rolled_back")),
                "isChampion": _safe_bool(t.get("is_champion")),
            })
    concerns = payload.get("concerns")
    reason = payload.get("terminal_reason")
    return {
        "runId": payload.get("run_id"),
        "spaceId": payload.get("space_id"),
        "finalStatus": payload.get("final_status") or None,
        "terminalReason": reason if reason in _TYPED_TERMINAL_REASONS else None,
        "published": _safe_bool(payload.get("published")),
        "publishOutcome": payload.get("publish_outcome") or None,
        "championIteration": _safe_int(payload.get("champion_iteration")),
        "championAccuracy": _safe_float(payload.get("champion_accuracy")),
        "championConfigVersionId": payload.get("champion_config_version_id") or None,
        # publish_record stores target_accuracy on the 0–100 scale (run_publish_
        # and_audit normalizes ≤1 to 0–100, like run_03); convert to 0–1.
        "targetAccuracy": _delta_accuracy_to_unit_scale(payload.get("target_accuracy")),
        "maxAttempts": _safe_int(payload.get("max_attempts")),
        "auditSummary": payload.get("audit_summary") or None,
        "improvementTrajectory": trajectory,
        "concerns": concerns if isinstance(concerns, list) else [],
    }


@router.get("/runs/{run_id}/publish")
async def get_publish_record(run_id: RunId):
    """GSO v2 ``publish_and_audit`` record (arch §7.3).

    Serves the ``publish_record`` artifact from ``genie_opt_artifacts``: the
    LLM audit summary + structured improvement trajectory + concerns + the
    champion pointer (iteration / accuracy / config-version) + the
    published/outcome verdict gated on the typed terminal reason. Returns
    ``{runId, publishRecord}`` with ``publishRecord=null`` when the run has not
    reached publish yet or predates the artifact (legacy run).
    """
    payload = await _offload(_load_latest_artifact, run_id, "publish_record") if _is_configured() else None
    return {
        "runId": run_id,
        "publishRecord": _build_publish_record(payload) if payload else None,
    }


def _build_benchmark_qc(payload: dict) -> dict:
    """Map a ``benchmark_qc`` artifact payload → camelCase GSOBenchmarkQC.

    The ``window`` recommendation (30–40 status + counts) is passed through
    as-is; repair-try usage and validity/contamination findings are surfaced
    for the QC panel (Phase 13).
    """
    window = payload.get("window")
    repaired = payload.get("repaired_ids")
    gt_candidates = payload.get("gt_correction_candidates")
    still_invalid = payload.get("still_invalid_ids")
    repair_exhausted = payload.get("repair_exhausted_ids")
    quality_counts = payload.get("quality_counts")
    quality_findings = payload.get("quality_findings")
    proposed_changes = payload.get("proposed_changes")
    final_validity = payload.get("final_validity")
    return {
        "validCount": _safe_int(payload.get("valid_count")),
        "persistedCount": _safe_int(payload.get("persisted_count")),
        "repairTriesUsed": _safe_int(payload.get("repair_tries_used")),
        "repairMaxTries": _safe_int(payload.get("benchmark_repair_max_tries")),
        "repairedIds": repaired if isinstance(repaired, list) else [],
        "repairSweeps": payload.get("repair_sweeps"),
        "finalValidity": _safe_bool(final_validity) if final_validity is not None else None,
        "window": window if isinstance(window, dict) else None,
        "windowTargetMin": _safe_int(payload.get("window_target_min")),
        "windowTargetMax": _safe_int(payload.get("window_target_max")),
        "gtCorrectionCandidates": gt_candidates if isinstance(gt_candidates, list) else [],
        "terminalReason": payload.get("terminal_reason") or None,
        "benchmarkPolicy": payload.get("benchmark_policy") or None,
        "benchmarkMutationCount": _safe_int(payload.get("benchmark_mutation_count")),
        "optimizationEligible": (
            _safe_bool(payload.get("optimization_eligible"))
            if payload.get("optimization_eligible") is not None
            else None
        ),
        "minimumValidCount": _safe_int(payload.get("minimum_valid_count")),
        "stillInvalidIds": still_invalid if isinstance(still_invalid, list) else None,
        "repairExhaustedIds": (
            repair_exhausted if isinstance(repair_exhausted, list) else []
        ),
        "repairExhaustedCount": _safe_int(payload.get("repair_exhausted_count")),
        "topUpAttempts": (
            payload.get("top_up_attempts")
            if isinstance(payload.get("top_up_attempts"), list)
            else []
        ),
        "topUpAttemptsUsed": _safe_int(payload.get("top_up_attempts_used")),
        "topUpMaxAttempts": _safe_int(payload.get("top_up_max_attempts")),
        "topUpBatchSize": _safe_int(payload.get("top_up_batch_size")),
        "topUpStopReason": payload.get("top_up_stop_reason") or None,
        "topUpRequestedCount": _safe_int(payload.get("top_up_requested_count")),
        "topUpGeneratedCount": _safe_int(payload.get("top_up_generated_count")),
        "topUpAcceptedCount": _safe_int(payload.get("top_up_accepted_count")),
        "topUpRejectedCount": _safe_int(payload.get("top_up_rejected_count")),
        "topUpDuplicateCount": _safe_int(payload.get("top_up_duplicate_count")),
        "qualityReviewVersion": payload.get("quality_review_version") or None,
        "qualityReviewStatus": payload.get("quality_review_status") or None,
        "semanticReviewCoverage": _safe_float(payload.get("semantic_review_coverage")),
        "qualityCounts": quality_counts if isinstance(quality_counts, dict) else None,
        "qualityFindings": quality_findings if isinstance(quality_findings, list) else [],
        "proposedChanges": proposed_changes if isinstance(proposed_changes, list) else [],
    }


@router.get("/runs/{run_id}/debug-data")
async def debug_data(run_id: RunId):
    """Diagnostic: inspect raw data sources for patches and iterations."""
    config = _build_gso_config()
    diag: dict = {
        "is_configured": _is_configured(),
        "catalog": config.catalog,
        "schema_name": config.schema_name,
        "warehouse_id": config.warehouse_id[:8] + "..." if config.warehouse_id else None,
    }

    # Test Lakebase
    lb_patches = await gso_lakebase.load_gso_patches(run_id)
    lb_iterations = await gso_lakebase.load_gso_iterations(run_id)
    lb_stages = await gso_lakebase.load_gso_stages(run_id)
    lb_run = await gso_lakebase.load_gso_run(run_id)
    diag["lakebase"] = {
        "run_found": bool(lb_run),
        "stages_count": len(lb_stages),
        "iterations_count": len(lb_iterations),
        "patches_count": len(lb_patches),
    }

    # Test Delta fallback
    delta_diag: dict = {"attempted": False}
    if _is_configured():
        delta_diag["attempted"] = True
        try:
            delta_patches = _delta_query(
                f"SELECT count(*) as cnt FROM {_delta_table('genie_opt_patches')} "
                f"WHERE run_id = '{run_id}'"
            )
            delta_diag["patches_count"] = delta_patches[0]["cnt"] if delta_patches else "query_returned_empty"
        except Exception as e:
            delta_diag["patches_error"] = str(e)[:200]
        try:
            delta_iters = _delta_query(
                f"SELECT count(*) as cnt FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}'"
            )
            delta_diag["iterations_count"] = delta_iters[0]["cnt"] if delta_iters else "query_returned_empty"
        except Exception as e:
            delta_diag["iterations_error"] = str(e)[:200]
        try:
            delta_stages = _delta_query(
                f"SELECT count(*) as cnt FROM {_delta_table('genie_opt_stages')} "
                f"WHERE run_id = '{run_id}'"
            )
            delta_diag["stages_count"] = delta_stages[0]["cnt"] if delta_stages else "query_returned_empty"
        except Exception as e:
            delta_diag["stages_error"] = str(e)[:200]
    diag["delta"] = delta_diag

    # Show what get_run actually loaded (stages come from somewhere)
    stages = lb_stages
    if not stages and _is_configured():
        stages = _delta_query(
            f"SELECT stage, status, lever, iteration FROM {_delta_table('genie_opt_stages')} "
            f"WHERE run_id = '{run_id}' ORDER BY started_at ASC LIMIT 10"
        )
    diag["stage_samples"] = stages[:5] if stages else []

    # Sample patches if any exist in Delta
    if _is_configured():
        try:
            raw_patches = _delta_query(
                f"SELECT iteration, lever, patch_type, scope, risk_level FROM {_delta_table('genie_opt_patches')} "
                f"WHERE run_id = '{run_id}' LIMIT 3"
            )
            diag["delta_patch_samples"] = raw_patches
        except Exception as e:
            diag["delta_patch_samples_error"] = str(e)[:200]

    # Sample iteration 0 from Delta
    if _is_configured():
        try:
            raw_iter0 = _delta_query(
                f"SELECT iteration, eval_scope, overall_accuracy, scores_json "
                f"FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}' AND iteration = 0 LIMIT 1"
            )
            if raw_iter0:
                r = raw_iter0[0]
                scores = r.get("scores_json")
                diag["delta_iter0"] = {
                    "iteration": r.get("iteration"),
                    "eval_scope": r.get("eval_scope"),
                    "overall_accuracy": r.get("overall_accuracy"),
                    "scores_json_type": type(scores).__name__,
                    "scores_json_preview": str(scores)[:300] if scores else None,
                }
            else:
                diag["delta_iter0"] = "not_found"
        except Exception as e:
            diag["delta_iter0_error"] = str(e)[:200]

    return diag


async def _load_iteration_rows_json(run_id: str, iteration: int) -> str | None:
    """Load the rows_json blob for an iteration (full scope, any-scope fallback).

    Synced Lakebase reads are disabled today, so this resolves through the
    Delta SQL-warehouse fallback in practice. Shared by the question-results
    and official-eval-results endpoints.
    """
    rows_json_str = await gso_lakebase.load_gso_iteration_rows(run_id, iteration, "full")
    if not rows_json_str:
        rows_json_str = await gso_lakebase.load_gso_iteration_rows(run_id, iteration, None)

    if not rows_json_str and _is_configured():
        logger.info("Lakebase returned no rows_json for run=%s iter=%s, trying Delta", run_id, iteration)
        delta_rows = _delta_query(
            f"SELECT rows_json FROM {_delta_table('genie_opt_iterations')} "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration} AND eval_scope = 'full' "
            f"ORDER BY timestamp ASC LIMIT 1"
        )
        if not delta_rows:
            delta_rows = _delta_query(
                f"SELECT rows_json FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}' AND iteration = {iteration} "
                f"AND rows_json IS NOT NULL ORDER BY timestamp ASC LIMIT 1"
            )
        rows_json_str = delta_rows[0]["rows_json"] if delta_rows else None
    return rows_json_str


@router.get("/runs/{run_id}/eval-results")
async def list_eval_results(run_id: RunId, iteration: int = Query(..., description="Iteration number")):
    """Lightweight official eval-results for an iteration.

    GSO v2 Phase 6: replaces the retired per-judge ASI rows. Returns one row
    per benchmark question carrying the native ``assessment`` (GOOD / BAD /
    NEEDS_REVIEW) and ``assessment_reasons[]`` (the ``failure_type``
    successor) — sourced from the iteration's rows_json.
    """
    rows_json_str = await _load_iteration_rows_json(run_id, iteration)
    return _parse_official_eval_results(rows_json_str)


@router.get("/runs/{run_id}/question-results")
async def list_question_results(run_id: RunId, iteration: int = Query(..., description="Iteration number")):
    """Get per-question results (question text + SQL) for a specific iteration."""
    rows_json_str = await _load_iteration_rows_json(run_id, iteration)
    return _parse_question_rows(rows_json_str)


@router.get("/runs/{run_id}/patches")
async def list_patches(run_id: RunId):
    """Get all optimization patches for a run."""
    patches = await gso_lakebase.load_gso_patches(run_id)
    if not patches and _is_configured():
        patches = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_patches')} "
            f"WHERE run_id = '{run_id}' ORDER BY iteration, lever, patch_index"
        )
    return patches


@router.get("/runs/{run_id}/benchmark-changes")
async def list_benchmark_changes(run_id: RunId):
    """Benchmark provenance ledger for a run (GSO v2 Phase 6, §3.5).

    Serves ``genie_opt_benchmark_mutations`` — every benchmark question GSO
    added / changed in the live Genie Space, excluded from this run, or marked
    for pruning (advisory) — grouped so the Workbench can render the audit trail
    with provenance. The diff is also reconstructable as
    (current space benchmarks) − (preflight snapshot); this ledger is the
    direct, attributable source.

    GSO v2 (item 7): the response also carries a ``qc`` field with the
    01_benchmark_qc_and_repair metadata (30–40 window status, repair tries
    used / max, validity findings) from the ``benchmark_qc`` artifact, so the
    QC + provenance views share one fetch. ``qc`` is null for legacy runs.
    """
    mutations = await gso_lakebase.load_gso_benchmark_mutations(run_id)
    if not mutations and _is_configured():
        mutations = await _delta_query_async(
            f"SELECT run_id, question_id, op, before, after, reason, logged_at "
            f"FROM {_delta_table('genie_opt_benchmark_mutations')} "
            f"WHERE run_id = '{run_id}' ORDER BY logged_at ASC"
        )

    qc_payload = await _offload(_load_latest_artifact, run_id, "benchmark_qc") if _is_configured() else None

    # Older publishers stored only answer.content[0] in the mutation ledger.
    # Recover the authoritative full "before" state from the immutable intake
    # snapshot so historical runs render correctly too. This also lets us hide
    # old false-positive changes caused solely by Genie serializing one SQL
    # statement as many content fragments while GSO serialized it as one.
    snapshot_states: dict[str, dict[str, str]] = {}
    if any(str(m.get("op") or "").strip().lower() == "changed" for m in mutations):
        run_row = await gso_lakebase.load_gso_run(run_id)
        if run_row is None and _is_configured():
            try:
                rows = await _delta_query_async(
                    f"SELECT config_snapshot FROM {_delta_table('genie_opt_runs')} "
                    f"WHERE run_id = '{run_id}' LIMIT 1"
                )
                run_row = rows[0] if rows else None
            except Exception:
                logger.warning(
                    "Could not load intake snapshot for benchmark changes run %s",
                    run_id,
                    exc_info=True,
                )
        if isinstance(run_row, dict):
            snapshot_states = _benchmark_states_from_snapshot(
                run_row.get("config_snapshot"),
            )

    buckets: dict[str, list[dict]] = {
        "added": [], "excluded": [], "removed": [], "changed": [],
        "prune_recommended": [],
    }
    items: list[dict] = []
    for m in mutations:
        op = str(m.get("op") or "").strip().lower()
        before = _safe_json_parse(m.get("before"))
        after = _safe_json_parse(m.get("after"))
        question_id = str(m.get("question_id") or "")
        if op == "changed" and question_id in snapshot_states:
            # Snapshot state is the actual pre-run benchmark, whereas legacy
            # ledger state may contain only the first SQL fragment.
            before = snapshot_states[question_id]
            if (
                isinstance(after, dict)
                and str(before.get("sql") or "").strip()
                == str(after.get("sql") or "").strip()
            ):
                continue
        item = {
            "questionId": m.get("question_id"),
            "op": op,
            "before": before,
            "after": after,
            "reason": m.get("reason"),
            "loggedAt": _isoformat(m.get("logged_at")),
        }
        items.append(item)
        if op in buckets:
            buckets[op].append(item)

    return {
        "runId": run_id,
        "added": buckets["added"],
        "excluded": buckets["excluded"],
        "removed": buckets["removed"],
        "changed": buckets["changed"],
        "pruneRecommended": buckets["prune_recommended"],
        "items": items,
        "counts": {
            "added": len(buckets["added"]),
            "excluded": len(buckets["excluded"]),
            "removed": len(buckets["removed"]),
            "changed": len(buckets["changed"]),
            "pruneRecommended": len(buckets["prune_recommended"]),
            "total": len(items),
        },
        "qc": _build_benchmark_qc(qc_payload) if qc_payload else None,
    }


def _benchmark_states_from_snapshot(raw_snapshot: Any) -> dict[str, dict[str, str]]:
    """Map native benchmark IDs to complete question/SQL intake state."""
    snapshot = _safe_json_parse(raw_snapshot)
    if not isinstance(snapshot, dict):
        return {}
    parsed = _resolve_parsed_space(snapshot)
    if not parsed and isinstance(snapshot.get("benchmarks"), dict):
        parsed = snapshot
    benchmark_node = parsed.get("benchmarks") if isinstance(parsed, dict) else None
    questions = (
        benchmark_node.get("questions")
        if isinstance(benchmark_node, dict)
        else None
    )
    if not isinstance(questions, list):
        return {}

    states: dict[str, dict[str, str]] = {}
    for row in questions:
        if not isinstance(row, dict):
            continue
        question_id = str(row.get("id") or "").strip()
        if not question_id:
            continue
        raw_question = row.get("question")
        question = (
            " ".join(str(part) for part in raw_question).strip()
            if isinstance(raw_question, list)
            else str(raw_question or "").strip()
        )
        sql = ""
        answers = row.get("answer")
        if isinstance(answers, list):
            for answer in answers:
                if not isinstance(answer, dict):
                    continue
                if str(answer.get("format") or "").upper() != "SQL":
                    continue
                content = answer.get("content")
                if isinstance(content, list):
                    sql = "".join(
                        str(fragment)
                        for fragment in content
                        if fragment is not None
                    ).strip()
                else:
                    sql = str(content or "").strip()
                break
        states[question_id] = {"question": question, "sql": sql}
    return states


def _parse_official_eval_results(rows_json_str: str | None) -> list[dict]:
    """Parse rows_json into lightweight official eval-results (GSO v2 Phase 6).

    One entry per benchmark question: ``question_id`` + native ``assessment``
    (GOOD/BAD/NEEDS_REVIEW) + ``assessment_reasons[]``. This is the successor
    to the retired per-judge ASI rows (the reasons list replaces
    ``failure_type``).
    """
    if not rows_json_str:
        return []
    try:
        rows = json.loads(rows_json_str) if isinstance(rows_json_str, str) else rows_json_str
    except Exception:
        return []
    if not isinstance(rows, list):
        return []

    results: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        _req = row.get("request") if isinstance(row.get("request"), dict) else {}
        _req_kw = _req.get("kwargs") if isinstance(_req.get("kwargs"), dict) else {}
        question_id = str(
            row.get("question_id")
            or row.get("inputs/question_id")
            or _req_kw.get("question_id")
            or row.get("request_id")
            or ""
        ).strip()
        assessment = str(row.get("assessment") or row.get("outputs/assessment") or "").strip().upper()
        if not question_id and not assessment:
            continue
        raw_reasons = row.get("assessment_reasons")
        if isinstance(raw_reasons, str):
            try:
                raw_reasons = json.loads(raw_reasons)
            except (json.JSONDecodeError, TypeError):
                raw_reasons = []
        assessment_reasons = (
            [str(r).strip() for r in raw_reasons if str(r).strip()]
            if isinstance(raw_reasons, list) else []
        )
        results.append({
            "question_id": question_id,
            "assessment": assessment or None,
            "assessment_reasons": assessment_reasons,
        })
    return results


def _parse_question_rows(rows_json_str: str | None) -> list[dict]:
    """Parse rows_json from genie_opt_iterations into per-question results."""
    if not rows_json_str:
        return []

    try:
        rows = json.loads(rows_json_str) if isinstance(rows_json_str, str) else rows_json_str
    except Exception:
        return []

    if not isinstance(rows, list):
        return []

    results = []
    for row in rows:
        # --------------- Parse nested request/response dicts ---------------
        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        if not isinstance(_req, dict):
            _req = {}
        _req_kw = _req.get("kwargs", {})
        if not isinstance(_req_kw, dict):
            _req_kw = {}

        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}
        if not isinstance(_resp, dict):
            _resp = {}

        # Fallback: legacy inputs/outputs dicts
        inputs = row.get("inputs") or {}
        if not isinstance(inputs, dict):
            inputs = {}
        outputs = row.get("outputs") or {}
        if not isinstance(outputs, dict):
            outputs = {}

        def _str_or_none(val: object) -> str | None:
            """Return val as string if it's a scalar, None if it's a dict/list/None."""
            if val is None or isinstance(val, (dict, list)):
                return None
            return str(val)

        # --------------- Question text ---------------
        # Primary: request.question  |  Fallback: inputs/question, flat keys
        question = str(
            _req.get("question")
            or row.get("inputs/question")
            or inputs.get("question")
            or row.get("question")
            or row.get("question_text")
            or _req_kw.get("question")
            or ""
        ).strip()

        # --------------- Question ID ---------------
        # Primary: request.kwargs.question_id  |  Fallback: inputs/question_id, flat keys
        question_id = str(
            _req_kw.get("question_id")
            or row.get("inputs/question_id")
            or inputs.get("question_id")
            or row.get("question_id")
            or row.get("request_id")
            or _req.get("question_id")
            or ""
        ).strip()
        if not question_id and question:
            question_id = question[:80]

        if not question_id and not question:
            continue

        # --------------- Generated SQL (Genie's response) ---------------
        # Primary: response.response  |  Fallback: outputs/response, outputs/generated_sql
        generated_sql = (
            _str_or_none(_resp.get("response"))
            or _str_or_none(outputs.get("generated_sql"))
            or _str_or_none(row.get("outputs/generated_sql"))
            or _str_or_none(row.get("outputs/response"))
            or _str_or_none(outputs.get("response"))
            or _str_or_none(row.get("generated_sql"))
        )

        # --------------- Expected SQL (ground truth) ---------------
        # Primary: request.expected_sql  |  Fallback: inputs/expected_sql, outputs/expected_sql
        expected_sql = (
            _str_or_none(_req.get("expected_sql"))
            or _str_or_none(outputs.get("expected_sql"))
            or _str_or_none(row.get("outputs/expected_sql"))
            or _str_or_none(row.get("inputs/expected_sql"))
            or _str_or_none(inputs.get("expected_sql"))
            or _str_or_none(row.get("expected_sql"))
        )

        # --------------- Comparison metadata ---------------
        # Primary: response.comparison  |  Fallback: outputs.comparison
        comparison = _resp.get("comparison") or {}
        if not isinstance(comparison, dict):
            comparison = {}
        if not comparison:
            comparison = outputs.get("comparison") or {}
            if not isinstance(comparison, dict):
                comparison = {}
        if not comparison:
            cmp_raw = row.get("outputs/comparison")
            if isinstance(cmp_raw, dict):
                comparison = cmp_raw
        match_type = (
            comparison.get("match_type")
            or row.get("outputs/comparison/match_type")
            or row.get("match_type")
        )

        # ---------------- Official assessment (GSO v2 Phase 6) ----------------
        # The native Benchmark API verdict drives per-question display state:
        #   GOOD → pass, BAD → fail, NEEDS_REVIEW → a distinct third state
        #   (review-pending, neither pass nor fail). `assessment_reasons`
        #   replaces the retired hardcoded per-judge `judge_verdicts`.
        assessment = str(
            row.get("assessment")
            or row.get("outputs/assessment")
            or ""
        ).strip().upper()

        raw_reasons = row.get("assessment_reasons")
        if isinstance(raw_reasons, str):
            try:
                raw_reasons = json.loads(raw_reasons)
            except (json.JSONDecodeError, TypeError):
                raw_reasons = []
        assessment_reasons: list[str] = (
            [str(r).strip() for r in raw_reasons if str(r).strip()]
            if isinstance(raw_reasons, list) else []
        )

        # Legacy exclusion markers — only relevant for in-process rows that
        # predate the official runner. The official path emits no per-row
        # exclusion (its excluded_count is always 0); ambiguous rows surface
        # as NEEDS_REVIEW instead.
        rc = str(
            row.get("result_correctness/value")
            or row.get("outputs/result_correctness/value")
            or outputs.get("result_correctness/value")
            or ""
        ).lower()
        error_type = str(
            comparison.get("error_type")
            or row.get("outputs/comparison/error_type")
            or ""
        ).lower()
        excluded = (
            rc == "excluded"
            or error_type in ("both_empty", "genie_result_unavailable")
        )

        if assessment in ("GOOD", "BAD", "NEEDS_REVIEW"):
            # Official path — the API assessment is authoritative.
            passed = True if assessment == "GOOD" else (
                False if assessment == "BAD" else None
            )
        else:
            # Legacy fallback — derive from result_correctness + arbiter, then
            # normalise to an assessment string so the UI sees one contract.
            arbiter = str(
                row.get("arbiter/value") or row.get("arbiter") or ""
            ).lower()
            if excluded:
                passed = None
                assessment = ""
            else:
                rc_pass = rc in ("yes", "true", "1", "1.0")
                arbiter_pass = arbiter in ("genie_correct", "both_correct")
                arbiter_fail = arbiter in ("ground_truth_correct", "neither_correct")
                if arbiter_pass:
                    passed = True
                elif arbiter_fail:
                    passed = False
                else:
                    passed = rc_pass
                assessment = "GOOD" if passed else "BAD"

        results.append({
            "question_id": question_id,
            "question": question,
            "generated_sql": generated_sql,
            "expected_sql": expected_sql,
            "passed": passed,
            "assessment": assessment or None,
            "assessment_reasons": assessment_reasons,
            "match_type": match_type,
            "excluded": excluded,
            "genie_sample": comparison.get("genie_sample"),
            "gt_sample": comparison.get("gt_sample"),
            "genie_columns": comparison.get("genie_columns"),
            "gt_columns": comparison.get("gt_columns"),
            "genie_rows": comparison.get("genie_rows"),
            "gt_rows": comparison.get("gt_rows"),
        })

    return results


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _isoformat(val) -> str | None:
    """Safely convert a datetime to ISO format string."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    return val.isoformat()
