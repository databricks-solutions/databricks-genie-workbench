"""Phase 3.6.2 E5b — independent clustering test.

The Phase 3 anchor replay tests do NOT exercise clustering directly:
clustering is upstream of the lever-loop decision logic that
Phase 0+1+2 changed, so the replay harness tape-serves
``optimizer.cluster_failures`` from ``iteration_payloads[idx].clusters``.
To preserve test coverage for clustering itself, this file gives it
its own test surface — synthetic ASI-enriched eval rows in,
asserted grouping out.

This is intentionally a smoke surface, not exhaustive:
``cluster_failures`` has many internal branches (blame_set
normalization, qid version disambiguation, judge-routing
fallbacks). The point is to assert the boundary contract: given
well-formed eval rows with a failing judge, clustering produces a
non-empty grouping. Existing
``tests/unit/test_cluster_failures_duplicate_qids.py`` and
``tests/unit/test_cluster_blame_normalization.py`` cover the
internal-behavior details.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import cluster_failures


def _failing_row(
    *,
    qid: str,
    question: str,
    expected_sql: str,
    generated_sql: str = "SELECT 1",
    rationale: str = "SQL produced wrong result",
) -> dict:
    """Minimal evaluator-shaped row carrying one failing judge
    (mirrors the schema in
    ``test_cluster_failures_duplicate_qids.py``)."""
    return {
        "question_id": qid,
        "request": {
            "question": question,
            "expected_sql": expected_sql,
            "kwargs": {"question_id": qid},
        },
        "response": {
            "response": generated_sql,
            "comparison": {"error": None},
        },
        "feedback/correctness/value": "no",
        "feedback/correctness/rationale": rationale,
    }


def _empty_metadata() -> dict:
    return {"data_sources": {"tables": [], "metric_views": []}}


def test_cluster_failures_produces_clusters_for_well_formed_rows():
    """Two failing rows → at least one non-empty cluster. The
    exact cluster topology depends on internal grouping logic
    covered by sibling tests; here we only assert that clustering
    can construct clusters at all given valid input."""
    rows = [
        _failing_row(
            qid="q1", question="how many orders?",
            expected_sql="SELECT COUNT(*) FROM orders",
        ),
        _failing_row(
            qid="q2", question="how many customers?",
            expected_sql="SELECT COUNT(*) FROM customers",
        ),
    ]
    clusters = cluster_failures(
        {"rows": rows},
        _empty_metadata(),
        signal_type="hard",
        namespace="H",
    )
    assert clusters, "expected at least one cluster from 2 failing rows"
    total_qids = sum(len(c.get("question_ids", [])) for c in clusters)
    assert total_qids == 2, (
        f"expected 2 qids across clusters, got {total_qids}"
    )
    for c in clusters:
        assert str(c.get("cluster_id", "")).startswith("H"), (
            f"namespace='H' should produce H-prefixed cluster_id, "
            f"got {c.get('cluster_id')!r}"
        )


def test_cluster_failures_empty_input_returns_empty():
    """Empty rows → no clusters. The replay stub for
    ``cluster_failures`` mirrors this contract when the
    iteration_payload is absent."""
    assert cluster_failures(
        {"rows": []}, _empty_metadata(), signal_type="hard",
    ) == []


def test_cluster_failures_soft_namespace_prefix():
    """``namespace='S'`` produces S-prefixed cluster ids."""
    rows = [
        _failing_row(
            qid="q1", question="x", expected_sql="SELECT 1",
        ),
    ]
    clusters = cluster_failures(
        {"rows": rows},
        _empty_metadata(),
        signal_type="soft",
        namespace="S",
    )
    assert clusters
    assert str(clusters[0].get("cluster_id", "")).startswith("S")
