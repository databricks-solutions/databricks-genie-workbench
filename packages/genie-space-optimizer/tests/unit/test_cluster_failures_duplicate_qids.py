"""Runtime dedup of duplicate ``question_id`` rows in ``cluster_failures``.

Benchmark tables can legally contain two rows with the same ``id``
(dedupe_benchmark_qids.py is a one-shot cleanup, not a runtime invariant).
Before S2, ``question_profiles[qid]`` silently merged their failures so
the dominant-cause aggregation was biased toward whichever row's
evaluator ran second.

These tests exercise ``cluster_failures`` through its list-of-rows input
path with three scenarios:

1. Two rows share a qid AND a request signature ⇒ one profile, the
   duplicate is dropped silently (no runtime error, no split cluster).
2. Two rows share a qid but differ on question/expected_sql ⇒ two
   profiles, the second renamed to ``<qid>:v2``.
3. Three rows share a qid with three distinct signatures ⇒ profiles
   ``qid``, ``qid:v2``, ``qid:v3``.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.optimizer import cluster_failures


def _failing_row(
    *,
    qid: str,
    question: str,
    expected_sql: str,
    generated_sql: str = "SELECT 1",
    rationale: str = "SQL produced wrong result",
) -> dict[str, Any]:
    """Build a minimal evaluator-shaped row with one failing judge.

    Matches the shape the clusterer parses: ``request`` / ``response`` dicts
    plus a ``feedback/<judge>/value == "no"`` column that drives the
    failure-enumeration inner loop.
    """
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
    """Minimal metadata_snapshot; cluster_failures does not read it for dedup."""
    return {"data_sources": {"tables": [], "metric_views": []}}


def _profile_qids_from_clusters(clusters: list[dict]) -> list[str]:
    """Flatten all question_ids across cluster buckets; preserve order."""
    qids: list[str] = []
    for c in clusters:
        qids.extend(c.get("question_ids", []))
    return qids


def test_duplicate_qid_with_matching_signature_is_dropped():
    """Same qid + identical (question, expected_sql) => pure duplicate."""
    rows = [
        _failing_row(qid="q1", question="foo?", expected_sql="SELECT a"),
        _failing_row(qid="q1", question="foo?", expected_sql="SELECT a"),
    ]
    clusters = cluster_failures(
        {"eval_results": rows}, _empty_metadata(), verbose=False
    )
    flat = _profile_qids_from_clusters(clusters)
    assert flat == ["q1"], (
        f"pure duplicate must collapse to a single profile; got {flat}"
    )


def test_duplicate_qid_with_different_signature_gets_v2_suffix():
    """Same qid but divergent signature => profile renamed to ``qid:v2``."""
    rows = [
        _failing_row(qid="q1", question="foo?", expected_sql="SELECT a"),
        _failing_row(qid="q1", question="bar?", expected_sql="SELECT b"),
    ]
    clusters = cluster_failures(
        {"eval_results": rows}, _empty_metadata(), verbose=False
    )
    flat = sorted(_profile_qids_from_clusters(clusters))
    assert flat == ["q1", "q1:v2"], (
        f"distinct rows must yield distinct profiles; got {flat}"
    )


def test_triple_duplicate_qid_with_distinct_signatures_gets_v2_v3():
    """Three rows, same qid, three distinct signatures => v2 and v3."""
    rows = [
        _failing_row(qid="q1", question="A", expected_sql="SELECT 1"),
        _failing_row(qid="q1", question="B", expected_sql="SELECT 2"),
        _failing_row(qid="q1", question="C", expected_sql="SELECT 3"),
    ]
    clusters = cluster_failures(
        {"eval_results": rows}, _empty_metadata(), verbose=False
    )
    flat = sorted(_profile_qids_from_clusters(clusters))
    assert flat == ["q1", "q1:v2", "q1:v3"], (
        f"three distinct rows must produce three profiles; got {flat}"
    )


def test_mixed_batch_independent_qids_untouched():
    """Dedup must not affect rows whose qids are already unique."""
    rows = [
        _failing_row(qid="q1", question="X", expected_sql="SELECT 1"),
        _failing_row(qid="q2", question="Y", expected_sql="SELECT 2"),
        _failing_row(qid="q1", question="X'", expected_sql="SELECT 1'"),
    ]
    clusters = cluster_failures(
        {"eval_results": rows}, _empty_metadata(), verbose=False
    )
    flat = sorted(_profile_qids_from_clusters(clusters))
    assert flat == ["q1", "q1:v2", "q2"], (
        f"unique qids must pass through; got {flat}"
    )


def test_cluster_formation_block_surfaces_rewrites(capsys, monkeypatch):
    """The CLUSTER FORMATION debug block must name the rewritten qids."""
    monkeypatch.setenv("CLUSTER_DEBUG", "1")
    rows = [
        _failing_row(qid="q1", question="A", expected_sql="SELECT 1"),
        _failing_row(qid="q1", question="B", expected_sql="SELECT 2"),
    ]
    cluster_failures(
        {"eval_results": rows}, _empty_metadata(), verbose=False
    )

    out = capsys.readouterr().out
    assert "CLUSTER FORMATION" in out
    assert "Duplicate qids detected" in out
    assert "q1 -> q1:v2" in out
