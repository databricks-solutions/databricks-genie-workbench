"""Regression-mining lane for the lever loop.

The lever loop's existing acceptance path is "single-criterion post-
arbiter": when a candidate iteration regresses or under-gains, it
rolls back. That keeps the *space* safe but loses the lesson — the
regression itself often encodes a real metadata gap (e.g. an
abbreviation column-pair the optimizer never disambiguated).

This module turns failed candidate evaluations into structured
``RegressionInsight`` values without changing acceptance, rollback,
state loaders, or score thresholds. Insights are persisted as
non-authoritative audit signals; an opt-in feature flag lets a later
strategist call consume them as compact hints.

The first insight type is ``column_confusion``, sourced from
:func:`genie_space_optimizer.optimization.optimizer.detect_column_confusion`
on the failed candidate's eval rows for newly-regressed questions.

Pure module: no I/O, no LLM, no Genie SDK. The harness owns the
orchestration; the writer call is in :mod:`state` and is called from
the harness rollback branch as a soft-fail step.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable

from genie_space_optimizer.optimization.optimizer import (
    ColumnConfusion,
    detect_column_confusion,
)

logger = logging.getLogger(__name__)


# Production rows store SQL via MLflow-flattened keys; ad-hoc fixtures
# may use the flat shape. Match the chain used by ``proposal_grounding``
# so mining sees what the harness sees.
_EXPECTED_SQL_KEYS: tuple[str, ...] = (
    "inputs.expected_sql",
    "expected_sql",
)
_GENERATED_SQL_KEYS: tuple[str, ...] = (
    "outputs.predictions.sql",
    "generated_sql",
    "genie_sql",
)


@dataclass(frozen=True)
class RegressionInsight:
    """A structured, persistable lesson mined from a failed iteration.

    ``insight_type`` is the controlled vocabulary tag (currently only
    ``column_confusion``). ``intended_column``/``confused_column`` are
    populated for column-confusion insights. ``recommended_patch_types``
    lists the patch types a future strategist call would prefer when
    proposing a contrastive fix; the harness does not auto-apply
    these — the strategist still owns proposal generation.

    ``source`` is always ``"regression_mining"`` so persisted rows are
    queryable as a typed audit lane.
    """

    insight_type: str
    question_id: str
    source: str = "regression_mining"
    intended_column: str = ""
    confused_column: str = ""
    table: str | None = None
    sql_clause: str = ""
    confidence: float = 0.0
    rationale: str = ""
    recommended_patch_types: tuple[str, ...] = field(default_factory=tuple)


# Patch types the contrastive Lever 1 fix should prefer for a column-
# confusion insight: strengthen the intended column's description and
# add a synonym, and clarify the confused column the same way. The
# strategist owns the actual proposal text; this is a hint, not a
# command.
_COLUMN_CONFUSION_LEVER1_PATCHES: tuple[str, ...] = (
    "update_column_description",
    "add_column_synonym",
)


# Patch-type routing: maps an insight type to the *recommended* patch
# types a contrastive fix should produce. The strategist's proposal
# generator already handles Lever 5 example SQL through the existing
# AFS path (which carries its own leakage firewall), so column-
# confusion insights only recommend the safer Lever 1 metadata
# changes here. Adding a Lever 5 entry here would risk pushing the
# strategist toward example-SQL proposals that echo the regressed
# question's expected SQL — exactly the kind of leakage the firewall
# is designed to catch but is cheaper to avoid by construction.
_PATCH_TYPE_ROUTING: dict[str, tuple[str, ...]] = {
    "column_confusion": _COLUMN_CONFUSION_LEVER1_PATCHES,
}


def recommended_patch_types_for_insight(
    insight: RegressionInsight,
) -> tuple[str, ...]:
    """Return the canonical patch-type recommendation for an insight.

    Centralises the routing so other modules (the strategist hint
    renderer, the audit-row projection, future patch synthesis paths)
    all see the same list. Unknown insight types yield an empty tuple
    so callers don't accidentally over-recommend.
    """
    return _PATCH_TYPE_ROUTING.get(insight.insight_type, ())


def _extract_question_id(row: dict) -> str:
    """Extract a row's canonical question id.

    Phase C Task 1: routes through ``_qid_extraction.extract_question_id``
    so the regression-mining lane cannot diverge from the four other
    canonical-qid extractors. Cycle 8 Bug 2 closed two of the four;
    this closes the last two.
    """
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    qid, _source = extract_question_id(row)
    return qid


def _extract_sql(row: dict, keys: Iterable[str]) -> str:
    for key in keys:
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val
    return ""


def mine_regression_insights(
    *,
    failed_eval_rows: list[dict] | None,
    regressed_qids: Iterable[str] | None,
    metadata_snapshot: dict | None = None,
) -> list[RegressionInsight]:
    """Turn failed candidate eval rows into structured insights.

    Args:
        failed_eval_rows: The ``rows`` field of the failed candidate
            eval result (typically ``gate_result["failed_eval_result"]``
            from the harness rollback branch). May be ``None`` or
            empty — the function returns ``[]`` in either case.
        regressed_qids: The qids that flipped from passing to failing
            on this iteration (typically the ``blocking_qids`` from the
            per-question regression verdict). Mining is restricted to
            this set so we don't re-mine long-standing failures.
        metadata_snapshot: Optional metadata snapshot used by
            :func:`detect_column_confusion` to bump confidence when
            both columns belong to the same table with the same data
            type. Absent metadata yields valid insights at lower
            confidence.

    Returns:
        A flat list of :class:`RegressionInsight`. Empty when no rows,
        no regressed qids, or no insight evidence was found.

    Pure: no I/O, no LLM. The function never raises on malformed
    rows; missing SQL keys produce no insight for that row.
    """
    if not failed_eval_rows:
        return []
    qid_set = {str(q).strip() for q in (regressed_qids or []) if str(q).strip()}
    if not qid_set:
        return []

    insights: list[RegressionInsight] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    for row in failed_eval_rows:
        if not isinstance(row, dict):
            continue
        qid = _extract_question_id(row)
        if not qid or qid not in qid_set:
            continue
        expected_sql = _extract_sql(row, _EXPECTED_SQL_KEYS)
        generated_sql = _extract_sql(row, _GENERATED_SQL_KEYS)
        if not expected_sql or not generated_sql:
            continue
        try:
            confusions = detect_column_confusion(
                expected_sql,
                generated_sql,
                metadata_snapshot=metadata_snapshot,
            )
        except Exception:
            logger.debug(
                "detect_column_confusion failed for qid %s; skipping",
                qid,
                exc_info=True,
            )
            continue
        if not confusions:
            continue

        # Collapse multiple clauses on the same column pair to a
        # single insight (highest confidence wins) so persisted audit
        # rows don't multi-count the same lesson.
        best_per_pair: dict[tuple[str, str], ColumnConfusion] = {}
        for c in confusions:
            key = (c.intended_column, c.confused_column)
            existing = best_per_pair.get(key)
            if existing is None or c.confidence > existing.confidence:
                best_per_pair[key] = c

        for c in best_per_pair.values():
            dedup_key = (qid, c.intended_column, c.confused_column, "column_confusion")
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            insight = RegressionInsight(
                insight_type="column_confusion",
                question_id=qid,
                intended_column=c.intended_column,
                confused_column=c.confused_column,
                table=c.table,
                sql_clause=c.sql_clause,
                confidence=float(c.confidence),
                rationale=c.rationale,
            )
            # Routing through the central helper keeps the audit lane,
            # the strategist hint block, and any future patch synthesis
            # path in lock-step on which patch types fix this insight.
            insights.append(
                RegressionInsight(
                    insight_type=insight.insight_type,
                    question_id=insight.question_id,
                    intended_column=insight.intended_column,
                    confused_column=insight.confused_column,
                    table=insight.table,
                    sql_clause=insight.sql_clause,
                    confidence=insight.confidence,
                    rationale=insight.rationale,
                    recommended_patch_types=recommended_patch_types_for_insight(
                        insight,
                    ),
                )
            )

    return insights


def insight_to_audit_metrics(insight: RegressionInsight) -> dict:
    """Render an insight as a JSON-safe dict for the decision audit row.

    Used by the harness when emitting a ``gate_name="regression_mining"``
    audit entry. Keeping the projection here means the ``state.py``
    writer stays oblivious to the insight's internals.
    """
    return {
        "insight_type": insight.insight_type,
        "source": insight.source,
        "question_id": insight.question_id,
        "intended_column": insight.intended_column,
        "confused_column": insight.confused_column,
        "table": insight.table,
        "sql_clause": insight.sql_clause,
        "confidence": float(insight.confidence),
        "rationale": insight.rationale,
        "recommended_patch_types": list(insight.recommended_patch_types),
    }


def summarize_insights_for_reflection(
    insights: list[RegressionInsight],
    *,
    max_items: int = 5,
) -> dict:
    """Build a compact, JSON-safe summary attached to the failed-iter
    reflection.

    Designed to be tucked under ``reflection["regression_mining"]`` so
    the run history can answer "what did we learn from this rollback?"
    without re-querying the decision audit table. Truncated to
    *max_items* entries; the count is reported separately.
    """
    items = []
    for ins in insights[:max_items]:
        items.append({
            "insight_type": ins.insight_type,
            "question_id": ins.question_id,
            "intended_column": ins.intended_column,
            "confused_column": ins.confused_column,
            "sql_clause": ins.sql_clause,
            "confidence": round(float(ins.confidence), 3),
            "recommended_patch_types": list(ins.recommended_patch_types),
        })
    return {
        "total": len(insights),
        "items": items,
    }


# Stable identifiers for the decision-audit lane. Pinning them in this
# module keeps the harness, writer, and tests in lock-step: a SQL query
# that asks "what did regression mining learn?" only needs to filter on
# these constants.
DECISION_AUDIT_GATE_NAME = "regression_mining"
DECISION_AUDIT_DECISION = "insight"
DECISION_AUDIT_STAGE_LETTER = "R"


def build_decision_audit_rows(
    insights: list[RegressionInsight],
    *,
    run_id: str,
    iteration: int,
    ag_id: str | None,
) -> list[dict]:
    """Project mined insights into ``write_lever_loop_decisions`` rows.

    Centralising the projection here keeps the harness a thin caller
    and lets unit tests assert on row shape without spinning up the
    decision-audit writer. The returned dicts are *not* JSON-encoded —
    ``write_lever_loop_decisions`` handles that.

    Empty input yields an empty list (no-op).
    """
    rows: list[dict] = []
    for idx, ins in enumerate(insights, start=1):
        rows.append(
            {
                "run_id": run_id,
                "iteration": int(iteration),
                "ag_id": ag_id,
                "decision_order": idx,
                "stage_letter": DECISION_AUDIT_STAGE_LETTER,
                "gate_name": DECISION_AUDIT_GATE_NAME,
                "decision": DECISION_AUDIT_DECISION,
                "reason_code": ins.insight_type,
                "reason_detail": (ins.rationale[:1000] if ins.rationale else None),
                "affected_qids": [ins.question_id] if ins.question_id else [],
                "metrics": insight_to_audit_metrics(ins),
            }
        )
    return rows


# ─── Strategist input path (feature-flagged) ─────────────────────────


def select_strategist_visible_insights(
    insights: Iterable[RegressionInsight],
    *,
    min_confidence: float,
    enabled: bool,
) -> list[RegressionInsight]:
    """Filter mined insights down to the set the strategist may see.

    The flag check is folded in here so callers don't accidentally feed
    insights to the strategist with the flag off — passing
    ``enabled=False`` always returns ``[]``.

    Insights with confidence strictly below ``min_confidence`` are
    excluded. Insights with empty intended/confused columns are
    excluded too, since the rendered hint would carry no actionable
    signal.

    Pure: stable order is preserved (mined order, then deduped); no
    deduplication is performed here — the miner already collapses
    same-pair clauses per qid.
    """
    if not enabled:
        return []
    selected: list[RegressionInsight] = []
    for ins in insights:
        if ins.confidence < float(min_confidence):
            continue
        if ins.insight_type == "column_confusion" and not (
            ins.intended_column and ins.confused_column
        ):
            continue
        selected.append(ins)
    return selected


def collect_insights_from_reflection_buffer(
    reflection_buffer: Iterable[dict],
) -> list[RegressionInsight]:
    """Reconstruct lightweight ``RegressionInsight`` values from the
    per-iteration reflection buffer's compact summaries.

    The harness writes ``reflection["regression_mining"]`` via
    :func:`summarize_insights_for_reflection`. That summary is the
    canonical in-memory record across iterations within a run, so the
    strategist input path reads from there rather than re-querying
    the decision audit table.

    The reconstructed insights carry only the fields the summary
    preserves (intended/confused column, clause, confidence,
    recommended patches). ``rationale`` and ``table`` are not
    round-tripped — the strategist hint block doesn't need them.
    """
    out: list[RegressionInsight] = []
    for entry in reflection_buffer:
        if not isinstance(entry, dict):
            continue
        rm = entry.get("regression_mining")
        if not isinstance(rm, dict):
            continue
        for item in rm.get("items") or []:
            if not isinstance(item, dict):
                continue
            try:
                out.append(
                    RegressionInsight(
                        insight_type=str(item.get("insight_type") or ""),
                        question_id=str(item.get("question_id") or ""),
                        intended_column=str(item.get("intended_column") or ""),
                        confused_column=str(item.get("confused_column") or ""),
                        sql_clause=str(item.get("sql_clause") or ""),
                        confidence=float(item.get("confidence") or 0.0),
                        recommended_patch_types=tuple(
                            str(p) for p in (item.get("recommended_patch_types") or [])
                        ),
                    )
                )
            except (TypeError, ValueError):
                continue
    return out


# Section header pinned here so test fixtures can match the exact
# string the strategist prompt receives.
STRATEGIST_HINT_HEADER = (
    "## Lessons from rolled-back iterations (regression mining)"
)


def render_strategist_hint_block(
    insights: list[RegressionInsight],
    *,
    max_items: int = 5,
) -> str:
    """Render a compact, leak-safe hint block for the strategist prompt.

    Produces zero output (empty string) when there are no insights so
    the caller can append unconditionally without worrying about a
    dangling header.

    Leak-safety: only column identifiers, the SQL clause name, and the
    insight type leave the function. No question text, no expected SQL,
    no row values. Column identifiers are space metadata (they appear
    in the metric view DDL the strategist already sees), so they are
    not benchmark-verbatim content.

    Each rendered line is intentionally short and structured so the
    strategist can parse it back into proposal intent without
    ambiguity. The rendered text mirrors the contrastive Lever 1 fix
    pattern: strengthen the intended column, clarify the confused
    column.
    """
    if not insights:
        return ""

    # Dedup by (insight_type, intended, confused) so the same lesson
    # mined across multiple qids only renders once. Keeps the prompt
    # compact and avoids over-weighting recurring failures.
    seen: set[tuple[str, str, str]] = set()
    lines: list[str] = []
    for ins in insights:
        key = (ins.insight_type, ins.intended_column, ins.confused_column)
        if key in seen:
            continue
        seen.add(key)
        if ins.insight_type == "column_confusion":
            clause = ins.sql_clause or "SQL"
            recs = ", ".join(ins.recommended_patch_types) or "metadata refresh"
            lines.append(
                f"- column_confusion in {clause}: "
                f"prefer `{ins.intended_column}` over `{ins.confused_column}`; "
                f"contrastive fix → {recs}."
            )
        else:
            lines.append(f"- {ins.insight_type}: {ins.rationale[:200]}")
        if len(lines) >= max_items:
            break

    if not lines:
        return ""

    return STRATEGIST_HINT_HEADER + "\n" + "\n".join(lines)
