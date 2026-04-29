"""Cluster construction must canonicalise blame_set before emit."""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import cluster_failures


def _row_with_blame(qid: str, blame: list[str]) -> dict:
    return {
        "question_id": qid,
        "request": {
            "question": "What are MTD sales by zone?",
            "expected_sql": "SELECT 1",
            "kwargs": {"question_id": qid},
        },
        "response": {
            "response": "SELECT 1",
            "comparison": {"error": None},
        },
        "feedback/result_correctness/value": "no",
        "feedback/result_correctness/rationale": "wrong filter",
        "feedback/completeness/value": "no",
        "feedback/completeness/rationale": "missing time_window filter",
        "feedback/completeness/metadata": {
            "failure_type": "missing_filter",
            "blame_set": blame,
        },
    }


def _metadata_with_tables() -> dict:
    return {
        "data_sources": {
            "tables": [{"identifier": "cat.sch.fact_sales"}],
            "metric_views": [],
        }
    }


def test_cluster_blame_set_is_canonical_tuple_of_strings() -> None:
    rows = [
        _row_with_blame(
            "q021",
            ['["time_window = mtd"]', "[time_window]", "[]"],
        ),
    ]
    clusters = cluster_failures(
        {"eval_results": rows},
        _metadata_with_tables(),
        verbose=False,
        signal_type="hard",
        namespace="H",
    )
    assert clusters, "expected at least one hard cluster"
    blame = clusters[0].get("asi_blame_set")
    assert blame is not None, f"blame_set unexpectedly None: cluster={clusters[0]}"
    assert isinstance(blame, list)
    for token in blame:
        assert isinstance(token, str)
        assert not token.startswith("[")
        assert not token.endswith("]")
        assert "[" not in token, f"non-canonical blame token: {token!r}"
    assert "time_window = mtd" in blame
    assert "time_window" in blame
