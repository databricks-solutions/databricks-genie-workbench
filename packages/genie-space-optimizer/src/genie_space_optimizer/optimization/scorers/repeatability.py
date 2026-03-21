"""Repeatability scorer — three-tier consistency check.

Compares the current Genie output against a reference from a prior iteration
using a waterfall of increasingly strict comparisons:

  Tier 1 – Execution equivalence: compare normalised result-set hashes.
  Tier 2 – Structural SQL equivalence: canonicalise via sqlglot / token overlap.
  Tier 3 – Exact SQL match: MD5 of lowercased SQL text.

Each verdict carries a ``match_tier`` in its metadata so the aggregation
step can decompose the headline repeatability score.
"""

from __future__ import annotations

import hashlib
import re

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    build_asi_metadata,
    format_asi_markdown,
)


def _sql_hash(sql: str) -> str:
    """Stable 8-char MD5 hash of lowercased SQL."""
    return hashlib.md5((sql or "").strip().lower().encode()).hexdigest()[:8]


def _structurally_similar(sql_a: str, sql_b: str) -> bool:
    """Token-overlap heuristic (Jaccard > 0.7)."""
    def _tokens(s: str) -> set[str]:
        return set(re.findall(r"\b\w+\b", s.lower()))

    a_tokens = _tokens(sql_a)
    b_tokens = _tokens(sql_b)
    if not a_tokens or not b_tokens:
        return False
    overlap = len(a_tokens & b_tokens) / max(len(a_tokens), len(b_tokens))
    return overlap > 0.7


def _structurally_equivalent(sql_a: str, sql_b: str) -> bool:
    """Canonicalise both SQL strings via sqlglot and compare.

    Falls back to the token-overlap heuristic when sqlglot cannot parse
    the dialect (e.g. Databricks-specific syntax it doesn't cover).
    """
    try:
        import sqlglot

        norm_a = sqlglot.transpile(sql_a, read="databricks", pretty=False)[0].strip().lower()
        norm_b = sqlglot.transpile(sql_b, read="databricks", pretty=False)[0].strip().lower()
        return norm_a == norm_b
    except Exception:
        return _structurally_similar(sql_a, sql_b)


def _make_pass(question_id: str, match_tier: str, rationale: str) -> Feedback:
    """Helper — build a ``value="yes"`` Feedback with match_tier metadata."""
    metadata = {"match_tier": match_tier}
    return Feedback(
        name="repeatability",
        value="yes",
        rationale=format_asi_markdown(
            judge_name="repeatability",
            value="yes",
            rationale=rationale,
            question_id=question_id,
        ),
        source=CODE_SOURCE,
        metadata=metadata,
    )


@scorer
def repeatability_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    """Three-tier repeatability judge.

    Tier 1 — execution equivalence (result-set hash).
    Tier 2 — structural SQL equivalence (sqlglot / token overlap).
    Tier 3 — exact SQL match (MD5 of lowercased text).
    """
    question_id = inputs.get("question_id", "")
    previous_sql = (expectations or {}).get("previous_sql", "")
    current_sql = (outputs or {}).get("response", "")

    prev_result_hash = (expectations or {}).get("previous_result_hash", "")
    cmp = (outputs or {}).get("comparison") or {}
    curr_result_hash = cmp.get("genie_hash", "") if isinstance(cmp, dict) else ""

    # ── No reference data at all — first evaluation ──────────────────
    if not previous_sql and not prev_result_hash:
        return _make_pass(
            question_id,
            match_tier="first_eval",
            rationale="No reference data available; first evaluation — marked consistent.",
        )

    # ── Current run returned no SQL ──────────────────────────────────
    if not current_sql:
        metadata = build_asi_metadata(
            failure_type="repeatability_issue",
            severity="major",
            confidence=0.9,
            expected_value=f"SQL (hash={_sql_hash(previous_sql)})" if previous_sql else "SQL output",
            actual_value="No SQL returned",
            counterfactual_fix="Investigate why Genie returned no SQL for this question",
        )
        metadata["match_tier"] = "no_output"
        return Feedback(
            name="repeatability",
            value="no",
            rationale=format_asi_markdown(
                judge_name="repeatability",
                value="no",
                rationale="Genie returned no SQL this run but did in the reference run.",
                metadata=metadata,
                question_id=question_id,
            ),
            source=CODE_SOURCE,
            metadata=metadata,
        )

    # ── Tier 1: Execution equivalence (result-set hash) ──────────────
    if prev_result_hash and curr_result_hash:
        if prev_result_hash == curr_result_hash:
            return _make_pass(
                question_id,
                match_tier="execution",
                rationale=(
                    f"Result sets identical across runs "
                    f"(hash={curr_result_hash})."
                ),
            )
        # Hashes differ → genuine functional divergence
        metadata = build_asi_metadata(
            failure_type="repeatability_issue",
            severity="major",
            confidence=0.95,
            expected_value=f"result hash={prev_result_hash}",
            actual_value=f"result hash={curr_result_hash}",
            counterfactual_fix=(
                "Result sets differ between runs. Add structured metadata "
                "(business_definition, synonyms, preferred_questions) to "
                "reduce non-determinism."
            ),
        )
        metadata["match_tier"] = "execution"
        return Feedback(
            name="repeatability",
            value="no",
            rationale=format_asi_markdown(
                judge_name="repeatability",
                value="no",
                rationale=(
                    f"Result sets differ. Reference hash={prev_result_hash}, "
                    f"current hash={curr_result_hash}."
                ),
                metadata=metadata,
                extra={
                    "previous_sql_preview": previous_sql[:200] if previous_sql else "",
                    "current_sql_preview": current_sql[:200],
                },
                question_id=question_id,
            ),
            source=CODE_SOURCE,
            metadata=metadata,
        )

    # ── Tier 2: Structural SQL equivalence ────────────────────────────
    if previous_sql and current_sql:
        if _structurally_equivalent(previous_sql, current_sql):
            return _make_pass(
                question_id,
                match_tier="structural",
                rationale=(
                    "SQL structurally equivalent to reference after "
                    "canonicalisation (cosmetic differences only)."
                ),
            )

    # ── Tier 3: Exact SQL match ───────────────────────────────────────
    if previous_sql and current_sql:
        prev_hash = _sql_hash(previous_sql)
        curr_hash = _sql_hash(current_sql)
        if prev_hash == curr_hash:
            return _make_pass(
                question_id,
                match_tier="exact",
                rationale=f"SQL identical to reference (hash={curr_hash}).",
            )

    # ── All tiers exhausted — mismatch ────────────────────────────────
    prev_hash = _sql_hash(previous_sql) if previous_sql else "n/a"
    curr_hash = _sql_hash(current_sql)
    is_similar = _structurally_similar(previous_sql, current_sql) if previous_sql else False
    metadata = build_asi_metadata(
        failure_type="repeatability_issue",
        severity="minor" if is_similar else "major",
        confidence=0.85,
        expected_value=f"SQL hash={prev_hash}",
        actual_value=f"SQL hash={curr_hash}",
        counterfactual_fix=(
            "Add structured metadata (business_definition, synonyms, "
            "preferred_questions) to reduce non-determinism."
        ),
    )
    metadata["match_tier"] = "none"
    return Feedback(
        name="repeatability",
        value="no",
        rationale=format_asi_markdown(
            judge_name="repeatability",
            value="no",
            rationale=(
                f"SQL changed between runs and no result-set hashes available "
                f"for execution comparison. "
                f"Reference SQL hash={prev_hash}, current SQL hash={curr_hash}."
            ),
            metadata=metadata,
            extra={
                "previous_sql_preview": previous_sql[:200] if previous_sql else "",
                "current_sql_preview": current_sql[:200],
            },
            question_id=question_id,
        ),
        source=CODE_SOURCE,
        metadata=metadata,
    )
