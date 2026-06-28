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

import pytest

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


# ── Official-runner row shape (no ``request`` key) ─────────────────────
#
# Regression for the bug where ``cluster_failures`` read ``expected_sql`` and
# ``question`` from ``row["request"]``. The official Benchmark eval runner
# (``eval_runner.map_eval_detail_to_row``) NEVER writes a ``request`` key — it
# carries the gold SQL at top-level ``expected_sql`` /
# ``inputs/expected_response`` / ``expectations.expected_response`` and the
# question at ``inputs/question``. So official rows silently lost their
# ``expected_sql`` + ``question`` (``generated_sql`` survived only because it
# was read from ``response.response``), and verifiable failures were
# mislabelled ``unverifiable_no_expected_sql``. The fix routes all three reads
# through ``eval_row_access`` accessors, which resolve both row shapes.

# Expected has a WHERE filter the generated SQL drops → a verifiable
# ``missing_filter`` diff. The question avoids the "by <x>" / filter-keyword
# heuristics in ``_classify_generated_sql_quality`` so that WITHOUT a resolved
# expected_sql the cascade lands on ``unverifiable_no_expected_sql`` — i.e.
# this row reproduces the bug on the pre-fix code.
_EXPECTED_SQL = (
    "SELECT category, SUM(amount) AS total FROM sales "
    "WHERE region = 'US' GROUP BY category"
)
_GENERATED_SQL = "SELECT category, SUM(amount) AS total FROM sales GROUP BY category"
_QUESTION = "What is the total sales amount per product category?"


def _official_row(
    *,
    qid: str,
    question: str,
    expected_sql: str,
    generated_sql: str,
    judge: str = "result_correctness",
) -> dict[str, Any]:
    """Build an OFFICIAL-runner eval row (mirrors ``map_eval_detail_to_row``).

    Crucially carries NO ``request`` key: the gold SQL lives at top-level
    ``expected_sql`` + ``inputs/expected_response`` + nested
    ``expectations.expected_response``; the generated SQL lives at
    ``response.response`` + ``outputs/response`` + ``generated_sql``; and the
    question lives at top-level ``question`` + ``inputs/question``.
    """
    return {
        "question_id": qid,
        "inputs/question_id": qid,
        "question": question,
        "inputs/question": question,
        "assessment": "BAD",
        "assessment_reasons": [],
        "manual_assessment": False,
        "needs_review": False,
        # A failing judge so the row produces a failure entry.
        "result_correctness/value": "no",
        f"feedback/{judge}/value": "no",
        "arbiter/value": "skipped",
        # Generated SQL: nested response shape + flat aliases. No ``request``.
        "response": {"response": generated_sql, "comparison": {}},
        "expectations": {"expected_response": expected_sql},
        "inputs/expected_response": expected_sql,
        "outputs/response": generated_sql,
        "generated_sql": generated_sql,
        "expected_sql": expected_sql,
        "_eval_source": "official_benchmark",
    }


def test_official_row_resolves_expected_sql_and_question():
    """Official rows (no ``request``) must keep expected_sql + question and
    classify to a verifiable root cause — not ``unverifiable_no_expected_sql``."""
    rows = [
        _official_row(
            qid="q-official",
            question=_QUESTION,
            expected_sql=_EXPECTED_SQL,
            generated_sql=_GENERATED_SQL,
        )
    ]
    assert "request" not in rows[0], "official rows must not carry a request key"

    clusters = cluster_failures(
        {"eval_results": rows}, _metadata_with_tables([]), verbose=False
    )
    assert clusters, "expected at least one cluster"
    cluster = clusters[0]

    ctx = cluster["sql_contexts"][0]
    assert ctx["expected_sql"].strip(), (
        "expected_sql must be resolved from the official row (top-level / "
        "inputs.expected_response / expectations.expected_response)"
    )
    assert ctx["question"].strip(), (
        "question must be resolved from the official row (inputs/question)"
    )
    assert ctx["generated_sql"].strip(), "generated_sql must be resolved"

    assert cluster["root_cause"] != "unverifiable_no_expected_sql", (
        "official row carries both expected and generated SQL; the root cause "
        f"should be verifiable, got {cluster['root_cause']!r}"
    )


def _legacy_row(
    *,
    qid: str,
    question: str,
    expected_sql: str,
    generated_sql: str,
    nest_kwargs: bool,
    judge: str = "correctness",
) -> dict[str, Any]:
    """Build a LEGACY in-process eval row carrying a ``request`` key.

    The harness emits two real ``request`` shapes — siblings
    (``request: {question, expected_sql}``, harness.py:4756/4881) and the
    MLflow kwargs-nested form (``request: {kwargs: {question, expected_sql}}``).
    Both must resolve via ``eval_row_access`` accessors.
    """
    inner = {"question": question, "expected_sql": expected_sql}
    request = {"kwargs": {**inner, "question_id": qid}} if nest_kwargs else inner
    return {
        "question_id": qid,
        "request": request,
        "response": {"response": generated_sql, "comparison": {}},
        f"feedback/{judge}/value": "no",
    }


@pytest.mark.parametrize("nest_kwargs", [False, True], ids=["siblings", "kwargs_nested"])
def test_legacy_request_row_still_resolves_expected_sql_and_question(nest_kwargs):
    """Legacy in-process rows (``request`` key present) must keep working for
    both the sibling and kwargs-nested request shapes."""
    rows = [
        _legacy_row(
            qid="q-legacy",
            question=_QUESTION,
            expected_sql=_EXPECTED_SQL,
            generated_sql=_GENERATED_SQL,
            nest_kwargs=nest_kwargs,
        )
    ]
    assert "request" in rows[0], "legacy rows carry a request key"

    clusters = cluster_failures(
        {"eval_results": rows}, _metadata_with_tables([]), verbose=False
    )
    assert clusters, "expected at least one cluster"
    cluster = clusters[0]

    ctx = cluster["sql_contexts"][0]
    assert ctx["expected_sql"].strip()
    assert ctx["question"].strip()
    assert cluster["root_cause"] != "unverifiable_no_expected_sql"
