"""
Repeatability testing — measure SQL generation consistency.

Re-queries Genie for each benchmark question multiple times and compares
the SQL hashes across invocations to detect non-determinism.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import Counter
from typing import TYPE_CHECKING, Any, Callable

from genie_space_optimizer.common.config import (
    RATE_LIMIT_SECONDS,
    REPEATABILITY_CLASSIFICATIONS,
    REPEATABILITY_EXTRA_QUERIES,
    REPEATABILITY_FIX_BY_ASSET,
)
from genie_space_optimizer.common.genie_client import detect_asset_type, run_genie_query

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _classify_repeatability(pct: float) -> str:
    """Map a repeatability percentage to a classification label.

    Uses ``REPEATABILITY_CLASSIFICATIONS`` from config (threshold → label).
    """
    for threshold in sorted(REPEATABILITY_CLASSIFICATIONS.keys(), reverse=True):
        if pct >= threshold:
            return REPEATABILITY_CLASSIFICATIONS[threshold]
    return "CRITICAL_VARIANCE"


def run_repeatability_test(
    w: WorkspaceClient,
    space_id: str,
    benchmarks: list[dict],
    spark: SparkSession,
    *,
    extra_queries: int = REPEATABILITY_EXTRA_QUERIES,
    original_sqls: dict[str, str] | None = None,
    heartbeat_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict:
    """Re-query each benchmark and compare SQL hashes across invocations.

    Args:
        w: Databricks WorkspaceClient.
        space_id: Genie Space ID.
        benchmarks: List of benchmark dicts (must have "question" and "id").
        spark: SparkSession (unused here but kept for interface consistency).
        extra_queries: Number of additional Genie queries per benchmark.
        original_sqls: Optional dict mapping question_id → original SQL from
            the predict function. Included as the first hash sample.

    Returns:
        Dict with keys: average_repeatability_pct, total_questions,
        identical, critical_variance, significant_variance, results.
    """
    original_sqls = original_sqls or {}
    results: list[dict] = []

    for idx, b in enumerate(benchmarks):
        question = b.get("question", "")
        question_id = b.get("id", str(idx))
        if not question:
            continue

        original_sql = original_sqls.get(question_id, "")
        original_hash = (
            hashlib.md5(original_sql.lower().encode()).hexdigest()[:8]
            if original_sql
            else "NONE"
        )
        hashes = [original_hash]
        sqls = [original_sql]
        if heartbeat_cb:
            heartbeat_cb(
                {
                    "event": "benchmark_start",
                    "question_id": question_id,
                    "benchmark_index": idx + 1,
                    "benchmark_total": len(benchmarks),
                },
            )

        for query_idx in range(extra_queries):
            time.sleep(RATE_LIMIT_SECONDS)
            retry_result = run_genie_query(w, space_id, question)
            retry_sql = retry_result.get("sql", "") or ""
            retry_hash = (
                hashlib.md5(retry_sql.lower().encode()).hexdigest()[:8]
                if retry_sql
                else "NONE"
            )
            hashes.append(retry_hash)
            sqls.append(retry_sql)
            if heartbeat_cb:
                heartbeat_cb(
                    {
                        "event": "query_attempt",
                        "question_id": question_id,
                        "benchmark_index": idx + 1,
                        "benchmark_total": len(benchmarks),
                        "query_attempt": query_idx + 1,
                        "query_total": extra_queries,
                    },
                )

        hash_counts = Counter(hashes)
        most_common_count = hash_counts.most_common(1)[0][1]
        pct = (most_common_count / len(hashes)) * 100
        asset_type = detect_asset_type(original_sql) if original_sql else "NONE"
        classification = _classify_repeatability(pct)

        logger.info(
            "[%d/%d] %s: %s (%.0f%%, %d variants, asset=%s)",
            idx + 1,
            len(benchmarks),
            question_id,
            classification,
            pct,
            len(set(hashes)),
            asset_type,
        )

        results.append(
            {
                "question_id": question_id,
                "question": question,
                "repeatability_pct": pct,
                "classification": classification,
                "unique_variants": len(set(hashes)),
                "dominant_asset": asset_type,
                "hashes": hashes,
            }
        )

    avg_pct = (
        sum(r["repeatability_pct"] for r in results) / len(results) if results else 0
    )
    identical_count = sum(1 for r in results if r["classification"] == "IDENTICAL")
    critical_count = sum(
        1 for r in results if r["classification"] == "CRITICAL_VARIANCE"
    )
    significant_count = sum(
        1 for r in results if r["classification"] == "SIGNIFICANT_VARIANCE"
    )

    return {
        "average_repeatability_pct": avg_pct,
        "total_questions": len(results),
        "identical": identical_count,
        "critical_variance": critical_count,
        "significant_variance": significant_count,
        "results": results,
    }


def compute_cross_iteration_repeatability(
    iterations: list[dict[str, str]],
) -> dict:
    """Compare SQL across iterations for each question.

    Args:
        iterations: List of dicts where each dict maps question_id → SQL
            for a single iteration.

    Returns:
        Dict mapping question_id → classification string.
    """
    if not iterations:
        return {}

    all_question_ids: set[str] = set()
    for it in iterations:
        all_question_ids.update(it.keys())

    cross_results: dict[str, str] = {}
    for qid in sorted(all_question_ids):
        hashes = []
        for it in iterations:
            sql = it.get(qid, "")
            h = hashlib.md5(sql.lower().encode()).hexdigest()[:8] if sql else "NONE"
            hashes.append(h)

        hash_counts = Counter(hashes)
        most_common_count = hash_counts.most_common(1)[0][1]
        pct = (most_common_count / len(hashes)) * 100
        cross_results[qid] = _classify_repeatability(pct)

    return cross_results


def synthesize_repeatability_failures(
    details: list[dict],
) -> list[dict]:
    """Summarize repeatability failures into actionable recommendations.

    Filters to non-IDENTICAL results and annotates each with a
    suggested fix from ``REPEATABILITY_FIX_BY_ASSET``.
    """
    failures: list[dict] = []
    for d in details:
        if d.get("classification") == "IDENTICAL":
            continue
        asset = d.get("dominant_asset", "NONE")
        fix = REPEATABILITY_FIX_BY_ASSET.get(asset, REPEATABILITY_FIX_BY_ASSET["NONE"])
        failures.append(
            {
                "question_id": d.get("question_id", ""),
                "question": d.get("question", ""),
                "classification": d["classification"],
                "repeatability_pct": d.get("repeatability_pct", 0),
                "unique_variants": d.get("unique_variants", 0),
                "dominant_asset": asset,
                "suggested_fix": fix,
            }
        )
    return failures
