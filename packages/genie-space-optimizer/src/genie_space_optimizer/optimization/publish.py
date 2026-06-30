"""GSO v2 — ``publish_and_audit`` core logic (Phase 9, arch §6 / §7.3 / §13.3).

The fifth and final task of the reshaped 5-task DAG. Once ``03_optimize`` has
produced a champion, this module:

1. **Reads the STAMPED terminal reason** off the champion iteration row
   (``genie_opt_iterations.terminal_reason``, written by the Phase-8 controller
   via ``state.update_iteration_loop_state``). It does **NOT** re-derive the
   reason from accuracy vs. target — that re-derivation silently collapses
   ``NO_NEW_HYPOTHESIS`` / ``EVAL_INVALID`` / ``LOOP_STATE_INVALID`` /
   ``EVAL_BUDGET_EXHAUSTED`` into ``TARGET_REACHED`` / ``MAX_ATTEMPTS`` and is the
   correctness bug Phase 9 fixes.
2. **Gates on the terminal reason** (arch §5.1 vocabulary):
   * ``{TARGET_REACHED, MAX_ATTEMPTS}`` → publish the champion (idempotent
     Delta-only ``models.promote_best_model`` — re-stamps ``is_champion`` + the
     run's ``best_*``; NO live-space mutation: the accepted patches were already
     applied in-place by the loop).
   * anything else → do **not** publish; still write a ``publish_record`` (it is
     the surface where concerns are raised, arch §7.3) carrying the stop reason
     and residual failures.
3. **Writes an LLM-generated audit summary** into the canonical ``publish_record``
   artifact. The summary call is **best-effort / non-fatal** — a failure never
   aborts the publish; the record is written with ``audit_summary=None`` plus a
   concern.
4. **Stamps the run's terminal status** by reusing the existing terminal statuses
   (``CONVERGED`` / ``MAX_ITERATIONS`` / ``FAILED`` / ``STALLED``). A dedicated
   ``PUBLISHED_AUDITED`` status (arch §13.3) is intentionally NOT introduced —
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

import json
import logging
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    AUDIT_SUMMARY_PROMPT,
    LEVER_NAMES,
)
from genie_space_optimizer.optimization.llm_client import call_llm
from genie_space_optimizer.optimization.models import promote_best_model
from genie_space_optimizer.optimization.state import (
    load_all_full_iterations,
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
    "LOOP_STATE_INVALID": "FAILED",
    "NO_NEW_HYPOTHESIS": "STALLED",
    "EVAL_BUDGET_EXHAUSTED": "STALLED",
}

#: Human-readable concern phrasing per non-publishing terminal reason.
_TERMINAL_REASON_CONCERN: dict[str, str] = {
    "EVAL_INVALID": (
        "Run stopped because evaluation became invalid (EVAL_INVALID); the "
        "champion was NOT published."
    ),
    "LOOP_STATE_INVALID": (
        "Run stopped because the loop state was inconsistent "
        "(LOOP_STATE_INVALID); the champion was NOT published."
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


def resolve_champion_row(full_iters: list[dict]) -> dict | None:
    """Pick the champion among ``eval_scope='full'`` rows (arch §7.4 / Phase 4).

    ``is_champion`` (set by ``promote_best_model``) is authoritative when present;
    otherwise the champion is the highest-accuracy NON-rolled-back full row, which
    is exactly the selection ``promote_best_model`` performs — and the row the
    Phase-8 controller stamped ``terminal_reason`` onto.
    """
    if not full_iters:
        return None

    flagged = [r for r in full_iters if _is_champion_flag(r)]
    if flagged:
        # If multiple are flagged (shouldn't happen), take the highest accuracy.
        return max(flagged, key=lambda r: _as_float(r.get("overall_accuracy")) or 0.0)

    candidates = [r for r in full_iters if not _is_rolled_back(r)]
    if not candidates:
        candidates = list(full_iters)
    return max(candidates, key=lambda r: _as_float(r.get("overall_accuracy")) or 0.0)


def resolve_terminal_reason(
    champion_row: dict | None, full_iters: list[dict]
) -> str | None:
    """Read the STAMPED terminal reason — never re-derive it from accuracy.

    Preference order (arch §5.1, progress Phase 9 spec A):
      1. the champion row's ``terminal_reason`` (the controller stamps it there);
      2. the highest-iteration full row that carries any ``terminal_reason``
         (covers the case where the final attempt row ≠ the champion row);
      3. ``None`` — genuinely absent ⇒ treated as a non-publishing, fail-closed
         outcome by the caller (NO accuracy-based re-derivation).
    """
    if champion_row:
        reason = champion_row.get("terminal_reason")
        if reason:
            return str(reason)

    stamped = [
        r for r in full_iters if r.get("terminal_reason")
    ]
    if stamped:
        latest = max(stamped, key=lambda r: _as_int(r.get("iteration")) or 0)
        return str(latest.get("terminal_reason"))

    return None


# ── Improvement trajectory + audit context (leak-free) ──────────────────────


def build_improvement_trajectory(full_iters: list[dict]) -> list[dict]:
    """Structured per-attempt staircase: baseline → coverage → surgical.

    Only structural/aggregate fields. ``delta_vs_baseline`` is the accuracy lift
    of each attempt over the iteration-0 baseline.
    """
    ordered = sorted(full_iters, key=lambda r: _as_int(r.get("iteration")) or 0)
    baseline_acc: float | None = None
    trajectory: list[dict] = []
    for row in ordered:
        iteration = _as_int(row.get("iteration"))
        acc = _as_float(row.get("overall_accuracy"))
        if iteration == 0 and baseline_acc is None:
            baseline_acc = acc
        delta = (
            round(acc - baseline_acc, 2)
            if (acc is not None and baseline_acc is not None)
            else None
        )
        mode = row.get("attempt_mode")
        if not mode and iteration == 0:
            mode = "baseline"
        trajectory.append({
            "iteration": iteration,
            "attempt_no": _as_int(row.get("attempt_no")),
            "attempt_mode": mode,
            "accuracy": acc,
            "delta_vs_baseline": delta,
            "best_accuracy": _as_float(row.get("best_accuracy")),
            "decision": row.get("decision"),
            "decision_reason": row.get("decision_reason"),
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


def _assert_leak_free(context: dict) -> None:
    """Defensive: a built audit context must never carry answer-key fields."""
    leaked = _LEAKY_FIELDS & set(context.keys())
    if leaked:  # pragma: no cover - guardrail
        raise ValueError(
            f"audit context leaks benchmark answer-key fields: {sorted(leaked)}"
        )


def as_audit_context(
    run_row: dict,
    full_iters: list[dict],
    patches_df: "pd.DataFrame | None",
    provenance_df: "pd.DataFrame | None",
    *,
    terminal_reason: str | None,
    champion_row: dict | None,
    target_accuracy: float | None,
    max_attempts: int | None,
) -> dict:
    """Build the LEAK-FREE structural prompt context for the audit summary.

    ONLY structural / aggregate fields are included (D8 / §3.6): per-attempt
    accuracy + deltas, attempt mode, decisions, lever/patch counts and families,
    root-cause label distribution, champion pointer, and the stop reason. NO
    benchmark question text, NO ``expected_sql`` / ground-truth, NO judge
    rationale — those are the leakage surface and are excluded by construction.
    """
    trajectory = build_improvement_trajectory(full_iters)
    baseline_accuracy = trajectory[0]["accuracy"] if trajectory else None

    champion_iteration = _as_int(champion_row.get("iteration")) if champion_row else None
    champion_accuracy = _as_float(champion_row.get("overall_accuracy")) if champion_row else None
    champion_config_version_id = (
        champion_row.get("best_config_version_id") if champion_row else None
    )
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
        "run_id": run_row.get("run_id"),
        "space_id": run_row.get("space_id"),
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


def build_audit_summary(
    w: "WorkspaceClient | None", audit_context: dict
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
        text, _response = call_llm(w, messages=messages)
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
    run_row = load_run(spark, run_id, catalog, schema) or {}
    full_iters = load_all_full_iterations(spark, run_id, catalog, schema)

    champion_row = resolve_champion_row(full_iters)
    terminal_reason = resolve_terminal_reason(champion_row, full_iters)

    publish = should_publish(terminal_reason)
    run_status = run_status_for_terminal_reason(terminal_reason)

    concerns: list[str] = []
    champion_iteration = _as_int(champion_row.get("iteration")) if champion_row else None
    champion_accuracy = _as_float(champion_row.get("overall_accuracy")) if champion_row else None
    champion_config_version_id = (
        champion_row.get("best_config_version_id") if champion_row else None
    )

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
                "No terminal_reason was stamped on any iteration row; treated as "
                "a non-publishing, fail-closed outcome (no accuracy-based "
                "re-derivation)."
            )
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
        run_row,
        full_iters,
        patches_df,
        provenance_df,
        terminal_reason=terminal_reason,
        champion_row=champion_row,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
    )
    audit_summary, summary_concern = build_audit_summary(w, audit_context)
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
