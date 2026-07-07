"""GSO v2 — ``publish_and_audit`` core logic (Phase 9, arch §6 / §7.3 / §13.3).

The final task of the reshaped 4-task DAG. Once ``optimize`` has produced a
champion, this module:

1. **Reads the STAMPED terminal reason** off the champion iteration row
   (``genie_opt_iterations.terminal_reason``, written by the Phase-8 controller
   via ``state.update_iteration_loop_state``). It does **NOT** re-derive the
   reason from accuracy vs. target — that re-derivation silently collapses
   ``NO_NEW_HYPOTHESIS`` / ``EVAL_INVALID`` /
   ``EVAL_BUDGET_EXHAUSTED`` into ``TARGET_REACHED`` / ``MAX_ATTEMPTS`` and is the
   correctness bug Phase 9 fixes.
2. **Gates on the terminal reason** (arch §5.1 vocabulary):
   * ``{TARGET_REACHED, MAX_ATTEMPTS}`` -> publish the champion (idempotent
     Delta-only ``models.promote_best_model`` - re-stamps ``is_champion`` + the
     run's ``best_*``; NO live-space mutation: the accepted patches were already
     applied in-place by the loop).
   * anything else -> do **not** publish; still write a ``publish_record`` (it is
     the surface where concerns are raised, arch §7.3) carrying the stop reason
     and residual failures.
3. **Writes an LLM-generated audit summary** into the canonical ``publish_record``
   artifact. The summary call is **best-effort / non-fatal** - a failure never
   aborts the publish; the record is written with ``audit_summary=None`` plus a
   concern.
4. **Stamps the run's terminal status** by reusing the existing terminal statuses
   (``CONVERGED`` / ``MAX_ITERATIONS`` / ``FAILED`` / ``STALLED``). A dedicated
   ``PUBLISHED_AUDITED`` status (arch §13.3) is intentionally NOT introduced -
   it is not in ``update_run_status``'s terminal set, would leave ``completed_at``
   NULL, and needs a DDL/migration. That decision is deferred to the human.

**Leakage discipline (D8 / progress §3.6).** The audit-summary prompt context is
built by :func:`as_audit_context`, which carries ONLY structural / aggregate
fields (per-attempt accuracy, deltas, attempt mode, decision, lever/patch counts
and families, champion pointer). Benchmark question text, ``expected_sql`` /
``generated_sql`` ground-truth, judge rationale, and counterfactual fixes are
**never** included — feeding them to an LLM is the exact leakage path the firewall
guards against on the config-write side.
"""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    AUDIT_SUMMARY_PROMPT,
    LEVER_NAMES,
    PROMPT_NAME_TEMPLATE,
    format_mlflow_template,
)
from genie_space_optimizer.optimization.champion import select_champion_row
from genie_space_optimizer.optimization.evaluation import (
    _link_prompt_to_trace,
    get_registered_prompt_name,
)
from genie_space_optimizer.optimization.llm_client import call_llm
from genie_space_optimizer.optimization.models import promote_best_model
from genie_space_optimizer.optimization.state import (
    load_all_scored_iterations,
    load_patches,
    load_provenance,
    load_run,
    update_run_status,
    write_artifact,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    from databricks.sdk import WorkspaceClient

# ``spark`` is typed ``Any`` (not ``pyspark.sql.SparkSession``) because pyspark is
# not a type-check dependency — matching the ``cast(Any, spark)`` convention in
# the jobs/ notebook entrypoints and avoiding an unresolved-import diagnostic.

logger = logging.getLogger(__name__)


# ── Terminal-reason gating (arch §5.1) ──────────────────────────────────────

#: Terminal reasons that mean "we have a champion worth publishing".
PUBLISH_TERMINAL_REASONS: frozenset[str] = frozenset({"TARGET_REACHED", "MAX_ATTEMPTS"})

#: terminal_reason → reused terminal run status (arch §13.3 — NO new
#: PUBLISHED_AUDITED status). Unknown / absent reasons fall through to STALLED.
_TERMINAL_REASON_TO_RUN_STATUS: dict[str, str] = {
    "TARGET_REACHED": "CONVERGED",
    "MAX_ATTEMPTS": "MAX_ITERATIONS",
    "EVAL_INVALID": "FAILED",
    "NO_NEW_HYPOTHESIS": "STALLED",
    "EVAL_BUDGET_EXHAUSTED": "STALLED",
}

#: Human-readable concern phrasing per non-publishing terminal reason.
_TERMINAL_REASON_CONCERN: dict[str, str] = {
    "EVAL_INVALID": (
        "Run stopped because evaluation became invalid (EVAL_INVALID); the "
        "champion was NOT published."
    ),
    "NO_NEW_HYPOTHESIS": (
        "Run stalled: the strategist produced no new hypothesis to try "
        "(NO_NEW_HYPOTHESIS) before reaching the target."
    ),
    "EVAL_BUDGET_EXHAUSTED": (
        "Run stopped because the evaluation wall-clock budget was exhausted "
        "(EVAL_BUDGET_EXHAUSTED) before reaching the target."
    ),
}

#: Fields that must NEVER appear in the audit-summary prompt context — they carry
#: benchmark answer-key material (the §3.6 leakage surface).
_LEAKY_FIELDS: frozenset[str] = frozenset({
    "question",
    "question_text",
    "expected_sql",
    "generated_sql",
    "expected_response",
    "actual_response",
    "counterfactual_fix",
    "rationale_snippet",
    "wrong_clause",
})


def should_publish(terminal_reason: str | None) -> bool:
    """True iff the stamped ``terminal_reason`` is in the publish set."""
    return terminal_reason in PUBLISH_TERMINAL_REASONS


def run_status_for_terminal_reason(terminal_reason: str | None) -> str:
    """Map the stamped ``terminal_reason`` → a reused terminal run status.

    Never collapses the reason — the reason itself is recorded separately as
    ``convergence_reason``. Unknown / absent reasons fall through to ``STALLED``
    (fail-closed: never fabricate a CONVERGED publish status from accuracy).
    """
    return _TERMINAL_REASON_TO_RUN_STATUS.get(terminal_reason or "", "STALLED")


# ── Champion + terminal-reason resolution ───────────────────────────────────


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
    except (TypeError, ValueError):
        return None
    # pandas NaN guard
    return None if f != f else f


def _is_rolled_back(row: dict) -> bool:
    return bool(row.get("rolled_back"))


def _is_champion_flag(row: dict) -> bool:
    return bool(row.get("is_champion"))


def _is_baseline_row(row: dict) -> bool:
    """Iteration-0 ``eval_scope='full'`` row — the floor (never rolled back)."""
    return _as_int(row.get("iteration")) == 0 and str(row.get("eval_scope")) == "full"


def resolve_champion_row(scored_iters: list[dict]) -> dict | None:
    """Pick the champion over the PROMOTION candidate universe (arch §7.4 / Phase 4).

    Delegates to the same selector used by ``promote_best_model`` so publish/audit
    and champion stamping cannot drift.
    """
    return select_champion_row(scored_iters)


def resolve_terminal_reason(champion_row: dict | None) -> str | None:
    """Read the STAMPED terminal reason off the CHAMPION row ONLY (arch §5.1).

    The champion row is the single authoritative source. A ``terminal_reason``
    stamped on a NON-champion row is NEVER used for gating (using it could publish
    from a non-champion MAX_ATTEMPTS/TARGET_REACHED row). Returns ``None`` when the
    champion is missing or unstamped ⇒ the caller takes the fail-closed no-publish
    path (status STALLED). There is NO accuracy-based re-derivation.
    """
    if champion_row:
        reason = champion_row.get("terminal_reason")
        if reason:
            return str(reason)
    return None


def unstamped_champion_diagnostic(
    champion_row: dict | None, scored_iters: list[dict]
) -> str | None:
    """Diagnostic-only concern when the champion is unstamped but a NON-champion
    row carries a reason. Surfaced in concerns for visibility — NEVER used to
    gate the publish (B1)."""
    if champion_row and champion_row.get("terminal_reason"):
        return None
    champ_iter = _as_int(champion_row.get("iteration")) if champion_row else None
    champ_scope = str(champion_row.get("eval_scope")) if champion_row else None
    others = {
        str(r.get("terminal_reason"))
        for r in scored_iters
        if r.get("terminal_reason")
        and not (
            _as_int(r.get("iteration")) == champ_iter
            and str(r.get("eval_scope")) == champ_scope
        )
    }
    if not others:
        return None
    return (
        "Champion row carries no stamped terminal_reason; non-champion row(s) "
        f"carry {sorted(others)} but that is NOT used for gating (fail-closed)."
    )


def _champion_config_version_id(champion_row: dict | None) -> str | None:
    """Stable champion config pointer.

    Prefers the loop-state ``best_config_version_id`` column; when that is absent /
    empty (base Phase 8 does not reliably populate it on real writes), derives a
    deterministic short hash of the champion's ``config_json`` so the publish_record
    pointer is still complete. Returns ``None`` only when neither is available.
    """
    if not champion_row:
        return None
    vid = champion_row.get("best_config_version_id")
    if vid:
        return str(vid)
    config_json = champion_row.get("config_json")
    if config_json:
        if not isinstance(config_json, str):
            try:
                config_json = json.dumps(config_json, sort_keys=True, default=str)
            except (TypeError, ValueError):
                config_json = str(config_json)
        digest = hashlib.sha256(config_json.encode("utf-8")).hexdigest()[:12]
        return f"cfgsha:{digest}"
    return None


# ── Improvement trajectory + audit context (leak-free) ──────────────────────


def _trajectory_sort_key(row: dict) -> tuple:
    """Order: baseline first, then by attempt_no, then timestamp."""
    attempt_no = _as_int(row.get("attempt_no"))
    primary = attempt_no if attempt_no is not None else -1
    return (primary, str(row.get("timestamp") or ""), _as_int(row.get("iteration")) or 0)


def build_improvement_trajectory(scored_iters: list[dict]) -> list[dict]:
    """Structured per-attempt staircase: baseline -> patch/eval iterations.

    Only STRUCTURAL / bounded fields (B3 §3.6 firewall): the free-text
    ``decision_reason`` is deliberately EXCLUDED — only the bounded ``decision``
    value (accept/reject/continue) is carried. ``delta_vs_baseline`` is the
    accuracy lift over the iteration-0 ``eval_scope='full'`` baseline.
    """
    ordered = sorted(scored_iters, key=_trajectory_sort_key)
    # Baseline is the iteration-0 full row (matches report.py / promote_best_model);
    # never the coverage enrichment row that may also live at iteration 0.
    baseline_acc: float | None = next(
        (
            _as_float(r.get("overall_accuracy"))
            for r in scored_iters
            if _is_baseline_row(r)
        ),
        None,
    )
    trajectory: list[dict] = []
    for row in ordered:
        iteration = _as_int(row.get("iteration"))
        acc = _as_float(row.get("overall_accuracy"))
        delta = (
            round(acc - baseline_acc, 2)
            if (acc is not None and baseline_acc is not None)
            else None
        )
        mode = row.get("attempt_mode")
        if not mode and _is_baseline_row(row):
            mode = "baseline"
        trajectory.append({
            "iteration": iteration,
            "attempt_no": _as_int(row.get("attempt_no")),
            "attempt_mode": mode,
            "eval_scope": row.get("eval_scope"),
            "accuracy": acc,
            "delta_vs_baseline": delta,
            "best_accuracy": _as_float(row.get("best_accuracy")),
            "decision": row.get("decision"),
            "rolled_back": _is_rolled_back(row),
            "is_champion": _is_champion_flag(row),
        })
    return trajectory


def _patch_family_counts(patches_df: "pd.DataFrame | None") -> tuple[int, int, dict[str, int]]:
    """Return (total patches, rolled-back patches, counts by lever family)."""
    if patches_df is None or getattr(patches_df, "empty", True):
        return 0, 0, {}
    families: dict[str, int] = {}
    total = 0
    rolled_back = 0
    for _, p in patches_df.iterrows():
        lever = _as_int(p.get("lever"))
        name = LEVER_NAMES.get(lever, f"Lever {lever}") if lever is not None else "unknown"
        families[name] = families.get(name, 0) + 1
        total += 1
        if bool(p.get("rolled_back")):
            rolled_back += 1
    return total, rolled_back, families


def _root_cause_distribution(
    provenance_df: "pd.DataFrame | None", *, iteration: int | None = None
) -> dict[str, int]:
    """Aggregate ``resolved_root_cause`` counts (structural RCA labels only).

    Free-text / SQL columns (``expected_sql``, ``generated_sql``,
    ``rationale_snippet``, ``counterfactual_fix``, ``wrong_clause``) are NEVER
    read here — only the categorical root-cause label is aggregated.
    """
    if provenance_df is None or getattr(provenance_df, "empty", True):
        return {}
    df = provenance_df
    if iteration is not None and "iteration" in df.columns:
        df = df[df["iteration"] == iteration]
    if "resolved_root_cause" not in df.columns or df.empty:
        return {}
    counts: dict[str, int] = {}
    for value in df["resolved_root_cause"].dropna():
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _residual_failing_clusters(
    champion_row: dict | None, provenance_df: "pd.DataFrame | None"
) -> tuple[int, list[str]]:
    """Count residual failures on the champion + distinct failing cluster ids.

    Returns ``(residual_failure_count, sorted_distinct_cluster_ids)``. Cluster
    ids (e.g. ``H001``) are structural, not benchmark Q/A.
    """
    residual_count = 0
    if champion_row is not None:
        remaining = champion_row.get("remaining_failures")
        if isinstance(remaining, str):
            try:
                remaining = json.loads(remaining)
            except (json.JSONDecodeError, TypeError):
                remaining = None
        if isinstance(remaining, list):
            residual_count = len(remaining)

    clusters: list[str] = []
    champ_iter = _as_int(champion_row.get("iteration")) if champion_row else None
    if (
        provenance_df is not None
        and not getattr(provenance_df, "empty", True)
        and "cluster_id" in provenance_df.columns
    ):
        df = provenance_df
        if champ_iter is not None and "iteration" in df.columns:
            df = df[df["iteration"] == champ_iter]
        clusters = sorted({str(c) for c in df["cluster_id"].dropna()})
    return residual_count, clusters


def _assert_leak_free(obj: Any) -> None:
    """Defensive: the audit context must never carry answer-key fields — at ANY
    depth (B3). Walks nested dicts/lists so a benchmark question / expected_sql
    buried in a nested structure is caught, not just a top-level key."""
    if isinstance(obj, dict):
        leaked = _LEAKY_FIELDS & set(obj.keys())
        if leaked:  # pragma: no cover - guardrail
            raise ValueError(
                f"audit context leaks benchmark answer-key fields: {sorted(leaked)}"
            )
        for value in obj.values():
            _assert_leak_free(value)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            _assert_leak_free(item)


def as_audit_context(
    run_id: str,
    space_id: str,
    scored_iters: list[dict],
    patches_df: "pd.DataFrame | None",
    provenance_df: "pd.DataFrame | None",
    *,
    terminal_reason: str | None,
    champion_row: dict | None,
    target_accuracy: float | None,
    max_attempts: int | None,
) -> dict:
    """Build the LEAK-FREE structural prompt context for the audit summary.

    ONLY structural / aggregate / bounded fields are included (D8 / §3.6): per-attempt
    accuracy + deltas, attempt mode, the bounded ``decision`` value (NOT the free-text
    ``decision_reason``), lever/patch counts and families, root-cause label
    distribution, champion pointer, and the stop reason. NO benchmark question text,
    NO ``expected_sql`` / ground-truth, NO judge rationale — those are the leakage
    surface and are excluded by construction; ``_assert_leak_free`` enforces it
    recursively. ``scored_iters`` is the ``full`` + ``enrichment`` set.
    """
    trajectory = build_improvement_trajectory(scored_iters)
    baseline_accuracy = next(
        (
            _as_float(r.get("overall_accuracy"))
            for r in scored_iters
            if _is_baseline_row(r)
        ),
        None,
    )

    champion_iteration = _as_int(champion_row.get("iteration")) if champion_row else None
    champion_accuracy = _as_float(champion_row.get("overall_accuracy")) if champion_row else None
    champion_config_version_id = _champion_config_version_id(champion_row)
    surgical_attempts_used = (
        _as_int(champion_row.get("surgical_attempts_used")) if champion_row else None
    )

    total_patches, rolled_back_patches, patch_families = _patch_family_counts(patches_df)
    root_cause_distribution = _root_cause_distribution(
        provenance_df, iteration=champion_iteration
    )
    residual_failure_count, residual_clusters = _residual_failing_clusters(
        champion_row, provenance_df
    )

    context = {
        "run_id": run_id,
        "space_id": space_id,
        "terminal_reason": terminal_reason,
        "published": should_publish(terminal_reason),
        "target_accuracy": target_accuracy,
        "max_attempts": max_attempts,
        "surgical_attempts_used": surgical_attempts_used,
        "baseline_accuracy": baseline_accuracy,
        "champion_iteration": champion_iteration,
        "champion_accuracy": champion_accuracy,
        "champion_config_version_id": champion_config_version_id,
        "improvement_trajectory": trajectory,
        "total_patches_applied": total_patches,
        "patches_rolled_back": rolled_back_patches,
        "patch_families": patch_families,
        "root_cause_distribution": root_cause_distribution,
        "residual_failure_count": residual_failure_count,
        "residual_failing_clusters": residual_clusters,
    }
    _assert_leak_free(context)
    return context


# ── LLM audit summary (best-effort / non-fatal) ─────────────────────────────


def _prompt_name_for_key(prompt_key: str, *, catalog: str = "", schema: str = "") -> str:
    registered = get_registered_prompt_name(prompt_key)
    if registered:
        return registered
    uc_schema = ".".join(part for part in (catalog, schema) if part)
    if uc_schema and "." in uc_schema:
        return format_mlflow_template(
            PROMPT_NAME_TEMPLATE,
            uc_schema=uc_schema,
            judge_name=prompt_key,
        )
    return prompt_key


def _start_chain_span(name: str) -> Any:
    try:
        import mlflow
        from mlflow.entities import SpanType

        return mlflow.start_span(name=name, span_type=SpanType.CHAIN)
    except Exception:
        return nullcontext(None)


def build_audit_summary(
    w: "WorkspaceClient | None",
    audit_context: dict,
    *,
    prompt_name: str = "",
) -> tuple[str | None, str | None]:
    """Generate the 1–2 paragraph human-readable audit summary via the LLM.

    Returns ``(summary_text_or_None, concern_or_None)``. The call is best-effort:
    any exception or empty/unusable output yields ``(None, concern)`` so the
    caller can still write the ``publish_record`` and stamp the run — a summary
    failure must NEVER fail the publish task.
    """
    try:
        user_payload = json.dumps(audit_context, default=str, sort_keys=True)
        messages = [
            {"role": "system", "content": AUDIT_SUMMARY_PROMPT},
            {"role": "user", "content": user_payload},
        ]
        context_hash = hashlib.sha256(user_payload.encode("utf-8")).hexdigest()
        prompt_name = prompt_name or _prompt_name_for_key("audit_summary")
        with _start_chain_span("audit_summary") as span:
            _link_prompt_to_trace(prompt_name)
            try:
                if span is not None:
                    span.set_inputs(
                        {
                            "prompt_name": prompt_name,
                            "audit_context_hash": context_hash,
                            "audit_context_chars": len(user_payload),
                            "audit_context_field_count": len(audit_context),
                            "improvement_trajectory_count": len(
                                audit_context.get("improvement_trajectory") or []
                            ),
                            "patch_family_count": len(
                                audit_context.get("patch_families") or {}
                            ),
                            "root_cause_field_count": len(
                                audit_context.get("root_cause_distribution") or {}
                            ),
                        }
                    )
            except Exception:
                pass
            text, _response = call_llm(w, messages=messages)
            try:
                if span is not None:
                    span.set_outputs({"summary_chars": len(text or "")})
            except Exception:
                pass
        text = (text or "").strip()
        if not text:
            return None, "Audit summary generation returned empty output."
        return text, None
    except Exception as exc:
        logger.warning(
            "Audit summary LLM call failed (non-fatal) — publishing without it",
            exc_info=True,
        )
        return None, f"Audit summary generation failed: {type(exc).__name__}."


# ── publish_record payload + orchestrator ───────────────────────────────────


def build_publish_record(
    *,
    run_id: str,
    space_id: str,
    run_status: str,
    terminal_reason: str | None,
    published: bool,
    publish_outcome: str,
    champion_iteration: int | None,
    champion_accuracy: float | None,
    champion_config_version_id: str | None,
    target_accuracy: float | None,
    max_attempts: int | None,
    audit_summary: str | None,
    improvement_trajectory: list[dict],
    concerns: list[str],
) -> dict:
    """Assemble the canonical ``publish_record`` payload (arch §7.3).

    All fields JSON-serializable. ``champion_iteration`` doubles as the champion
    pointer (number); the champion accuracy + config-version reference complete
    the pointer.
    """
    return {
        "run_id": run_id,
        "space_id": space_id,
        "final_status": run_status,
        "terminal_reason": terminal_reason,
        "published": published,
        "publish_outcome": publish_outcome,
        "champion_iteration": champion_iteration,
        "champion_accuracy": champion_accuracy,
        "champion_config_version_id": champion_config_version_id,
        "target_accuracy": target_accuracy,
        "max_attempts": max_attempts,
        "audit_summary": audit_summary,
        "improvement_trajectory": improvement_trajectory,
        "concerns": concerns,
    }


def publish_and_audit(
    spark: Any,
    w: "WorkspaceClient | None",
    run_id: str,
    *,
    space_id: str,
    catalog: str,
    schema: str,
    target_accuracy: float | None = None,
    max_attempts: int | None = None,
    source_notebook: str = "run_publish_and_audit.py",
) -> dict:
    """Run the real ``publish_and_audit`` body (Phase 9).

    Reads the stamped terminal reason, gates publish on it, generates the
    best-effort LLM audit summary, writes the ``publish_record`` artifact, and
    stamps the run's terminal status. Returns a small result dict for the
    notebook's exit JSON.
    """
    # load_all_scored_iterations still includes historical enrichment rows so
    # old runs render a complete trajectory; select_champion_row restricts
    # promotion to full-scope rows for the unified loop.
    scored_iters = load_all_scored_iterations(spark, run_id, catalog, schema)

    champion_row = resolve_champion_row(scored_iters)
    # B1: gate ONLY on the champion row's stamped reason (fail-closed when absent).
    terminal_reason = resolve_terminal_reason(champion_row)

    publish = should_publish(terminal_reason)
    run_status = run_status_for_terminal_reason(terminal_reason)

    concerns: list[str] = []
    champion_iteration = _as_int(champion_row.get("iteration")) if champion_row else None
    champion_accuracy = _as_float(champion_row.get("overall_accuracy")) if champion_row else None
    champion_config_version_id = _champion_config_version_id(champion_row)

    if publish:
        # Idempotent Delta-only champion publish: re-stamps is_champion + the
        # run's best_*. NO live-space mutation (the loop already applied accepted
        # patches in-place); NO example-SQL firewall path is invoked here.
        promoted = promote_best_model(spark, run_id, catalog, schema)
        published = True
        publish_outcome = "published"
        if promoted is not None:
            champion_iteration = promoted
            refreshed = load_run(spark, run_id, catalog, schema) or {}
            if _as_float(refreshed.get("best_accuracy")) is not None:
                champion_accuracy = _as_float(refreshed.get("best_accuracy"))
    else:
        published = False
        publish_outcome = f"not_published:{terminal_reason or 'UNKNOWN'}"
        if terminal_reason is None:
            concerns.append(
                "Champion row carries no stamped terminal_reason; treated as a "
                "non-publishing, fail-closed outcome (no accuracy-based "
                "re-derivation, no non-champion fallback)."
            )
            # Diagnostic-only: surface a reason stamped on a NON-champion row
            # WITHOUT using it to gate (B1).
            diag = unstamped_champion_diagnostic(champion_row, scored_iters)
            if diag:
                concerns.append(diag)
        else:
            concerns.append(
                _TERMINAL_REASON_CONCERN.get(
                    terminal_reason,
                    f"Run stopped with terminal_reason={terminal_reason}; the "
                    "champion was NOT published.",
                )
            )
        residual_count, residual_clusters = _residual_failing_clusters(
            champion_row, None
        )
        if residual_count:
            concerns.append(
                f"{residual_count} benchmark question(s) still failing on the "
                "champion config at stop time."
            )

    # Best-effort LLM audit summary over the LEAK-FREE structural context.
    patches_df = load_patches(spark, run_id, catalog, schema)
    provenance_df = load_provenance(spark, run_id, catalog, schema)
    audit_context = as_audit_context(
        run_id,
        space_id,
        scored_iters,
        patches_df,
        provenance_df,
        terminal_reason=terminal_reason,
        champion_row=champion_row,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
    )
    audit_summary, summary_concern = build_audit_summary(
        w,
        audit_context,
        prompt_name=_prompt_name_for_key(
            "audit_summary",
            catalog=catalog,
            schema=schema,
        ),
    )
    if summary_concern:
        concerns.append(summary_concern)

    improvement_trajectory = audit_context["improvement_trajectory"]

    publish_record = build_publish_record(
        run_id=run_id,
        space_id=space_id,
        run_status=run_status,
        terminal_reason=terminal_reason,
        published=published,
        publish_outcome=publish_outcome,
        champion_iteration=champion_iteration,
        champion_accuracy=champion_accuracy,
        champion_config_version_id=champion_config_version_id,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
        audit_summary=audit_summary,
        improvement_trajectory=improvement_trajectory,
        concerns=concerns,
    )

    write_artifact(
        spark,
        run_id,
        "publish_record",
        publish_record,
        catalog=catalog,
        schema=schema,
        stage_name="PUBLISH_AND_AUDIT",
        iteration=champion_iteration,
        source_notebook=source_notebook,
    )

    # Reuse an existing terminal status (arch §13.3 — PUBLISHED_AUDITED is NOT
    # introduced). convergence_reason carries the ACTUAL terminal_reason; it is
    # never collapsed.
    update_run_status(
        spark,
        run_id,
        catalog,
        schema,
        status=run_status,
        best_iteration=champion_iteration,
        best_accuracy=champion_accuracy,
        convergence_reason=terminal_reason,
        space_id=space_id or None,
    )

    return {
        "run_id": run_id,
        "terminal_reason": terminal_reason,
        "final_status": run_status,
        "published": published,
        "publish_outcome": publish_outcome,
        "champion_iteration": champion_iteration,
        "champion_accuracy": champion_accuracy,
        "concerns": concerns,
        "audit_summary_generated": audit_summary is not None,
    }
