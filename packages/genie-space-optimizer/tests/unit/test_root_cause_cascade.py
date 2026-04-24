"""Tests for the S3 hardening of the root-cause cascade.

The cascade in ``optimizer.cluster_failures`` decides the ``root_cause``
label for every failure entry. Three regressions motivated these tests:

1. Empty ``generated_sql`` used to fall through to ``_classify_sql_diff``,
   which then produced nonsense labels like ``missing_filter`` driven by
   the *absence* of a WHERE clause. Lever 6 then proposed filter snippets
   for a model that never emitted SQL at all. Fix: short-circuit to
   ``missing_sql_generation``.
2. ASI ``failure_type == "other"`` with a non-empty ``blame_set`` used
   to be discarded, even when a blame token resolved to a real table /
   MV / TVF in ``metadata_snapshot``. Fix: rescue as
   ``missing_data_asset`` (routed to Lever 3 via ``_LEVER_TO_PATCH_TYPE``).
3. ``_extract_pattern`` was a loose substring matcher: a rationale
   saying ``"filter is applied correctly"`` was labeled
   ``missing_filter``. Fix: require both a noun AND a failure verb.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.common.config import _LEVER_TO_PATCH_TYPE
from genie_space_optimizer.optimization.optimizer import (
    _blame_set_matches_metadata,
    _extract_pattern,
    _metadata_asset_tokens,
    cluster_failures,
)


def _row(
    *,
    qid: str,
    generated_sql: str,
    rationale: str,
    asi_failure_type: str | None = None,
    asi_blame_set: list[str] | None = None,
    judge: str = "correctness",
    expected_sql: str = "SELECT 1",
    question: str = "demo",
) -> dict[str, Any]:
    """Build an evaluator row that emits one failing judge entry."""
    row: dict[str, Any] = {
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
        f"feedback/{judge}/value": "no",
        f"feedback/{judge}/rationale": rationale,
    }
    if asi_failure_type is not None or asi_blame_set is not None:
        row[f"feedback/{judge}/metadata"] = {
            "failure_type": asi_failure_type,
            "blame_set": asi_blame_set,
        }
    return row


def _metadata_with_tables(identifiers: list[str]) -> dict:
    return {
        "data_sources": {
            "tables": [{"identifier": i} for i in identifiers],
            "metric_views": [],
        }
    }


# ── Empty-SQL short-circuit ────────────────────────────────────────────


def test_empty_generated_sql_short_circuits_to_missing_sql_generation():
    rows = [
        _row(
            qid="q1",
            generated_sql="",
            rationale="filter is applied correctly but result is empty",
        )
    ]
    clusters = cluster_failures(
        {"eval_results": rows}, _metadata_with_tables([]), verbose=False
    )
    assert clusters, "expected at least one cluster"
    root = clusters[0]["root_cause"]
    assert root == "missing_sql_generation", (
        f"empty SQL must short-circuit, got {root!r}"
    )


def test_whitespace_generated_sql_short_circuits():
    rows = [
        _row(
            qid="q1",
            generated_sql="   \n  ",
            rationale="anything",
        )
    ]
    clusters = cluster_failures(
        {"eval_results": rows}, _metadata_with_tables([]), verbose=False
    )
    assert clusters[0]["root_cause"] == "missing_sql_generation"


# ── ASI blame-set rescue ──────────────────────────────────────────────


def test_blame_set_matches_known_table_rescues_to_missing_data_asset():
    metadata = _metadata_with_tables(["cat.sch.orders"])
    rows = [
        _row(
            qid="q1",
            generated_sql="SELECT 1",
            rationale="some prose",
            asi_failure_type="other",
            asi_blame_set=["cat.sch.orders"],
        )
    ]
    clusters = cluster_failures({"eval_results": rows}, metadata, verbose=False)
    assert clusters[0]["root_cause"] == "missing_data_asset"


def test_blame_set_matches_bare_table_name():
    """Blame tokens like ``orders`` must resolve against split identifiers."""
    metadata = _metadata_with_tables(["cat.sch.orders"])
    assert _blame_set_matches_metadata(["orders"], metadata) is True


def test_blame_set_no_match_leaves_cascade_to_rationale_then_sql_diff():
    metadata = _metadata_with_tables(["cat.sch.orders"])
    rows = [
        _row(
            qid="q1",
            generated_sql="SELECT 1",
            rationale="generic prose",
            asi_failure_type="other",
            asi_blame_set=["nonexistent_table"],
        )
    ]
    clusters = cluster_failures({"eval_results": rows}, metadata, verbose=False)
    assert clusters[0]["root_cause"] != "missing_data_asset"


# ── Tightened _extract_pattern ────────────────────────────────────────


def test_affirmative_filter_prose_is_not_missing_filter():
    """Regression: ``"filter is applied correctly"`` must NOT match."""
    assert _extract_pattern("filter is applied correctly") == "other"


def test_missing_filter_phrase_still_matches():
    assert _extract_pattern("Missing filter on is_current") == "missing_scd_filter"
    assert _extract_pattern("No where clause restricting order_date") == "missing_filter"


def test_wrong_join_phrase_requires_failure_verb():
    """Plain ``"join"`` must not trip ``wrong_join``."""
    assert _extract_pattern("the join between fact and dim is natural") == "other"
    assert _extract_pattern("wrong join between fact and dim") == "wrong_join"


def test_bare_filter_does_not_trigger_missing_filter():
    assert _extract_pattern("filter predicate") == "other"


# ── Routing table sanity ──────────────────────────────────────────────


def test_missing_data_asset_routes_to_lever_3_add_example_sql():
    assert _LEVER_TO_PATCH_TYPE[("missing_data_asset", 3)] == "add_example_sql"


def test_missing_sql_generation_has_a_routing_entry():
    assert ("missing_sql_generation", 5) in _LEVER_TO_PATCH_TYPE


# ── Metadata-token helper ─────────────────────────────────────────────


def test_metadata_asset_tokens_includes_identifier_and_parts():
    md = _metadata_with_tables(["cat.sch.orders"])
    tokens = _metadata_asset_tokens(md)
    assert "cat.sch.orders" in tokens
    assert "orders" in tokens


def test_metadata_asset_tokens_gracefully_handles_missing_shape():
    assert _metadata_asset_tokens({}) == set()
    assert _metadata_asset_tokens({"data_sources": {}}) == set()
