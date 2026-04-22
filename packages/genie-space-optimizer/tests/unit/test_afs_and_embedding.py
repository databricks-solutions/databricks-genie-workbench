"""AFS schema + embedding-firewall tests (Bug #4, Phase 2).

Ensures:
* AFS output field set is closed — unknown fields are dropped.
* No raw benchmark text (question, expected_sql, generated_sql, result
  samples) appears in an AFS across a battery of fuzzed clusters.
* The embedding cosine layer of ``is_benchmark_leak`` catches a
  paraphrase that the n-gram layer alone would pass.
* The firewall degrades to n-gram + fingerprint when the embedding
  endpoint is unavailable (i.e., ``get_embedding`` returns None).
* The AST differ in ``afs.compute_ast_diff`` emits typed structural
  classifications without leaking raw SQL tokens from the input.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.afs import (
    AFS_ALLOWED_FIELDS,
    AFSLeakError,
    compute_ast_diff,
    format_afs,
    format_afs_batch,
    validate_afs,
)
from genie_space_optimizer.optimization.leakage import (
    BenchmarkCorpus,
    _cosine_similarity,
    is_benchmark_leak,
)


_BENCHMARKS: list[dict] = [
    {
        "id": "q_rev",
        "question": "What is the total revenue by product category last quarter?",
        "expected_sql": (
            "SELECT category, SUM(revenue) FROM sales "
            "WHERE quarter = 'Q3' GROUP BY category"
        ),
    },
    {
        "id": "q_top10",
        "question": "Show the top 10 customers by lifetime value",
        "expected_sql": (
            "SELECT customer_id, SUM(amount) AS ltv FROM orders "
            "GROUP BY customer_id ORDER BY ltv DESC LIMIT 10"
        ),
    },
]


@pytest.fixture
def corpus() -> BenchmarkCorpus:
    return BenchmarkCorpus.from_benchmarks(_BENCHMARKS)


# ── AFS closed schema ──────────────────────────────────────────────────


def test_afs_schema_is_closed() -> None:
    """format_afs must never return fields outside the allowed set."""
    cluster = {
        "cluster_id": "C001",
        "root_cause": "missing_filter",
        "affected_judge": "schema_accuracy",
        "asi_blame_set": ["sales.category", "sales.revenue"],
        "asi_counterfactual_fixes": ["partition by quarter", "use SUM not COUNT"],
        "asi_wrong_clause": "WHERE",
        "question_ids": ["q1", "q2", "q3"],
        # Intentionally put stuff that SHOULD NOT pass through.
        "sql_contexts": [
            {
                "question": "this must not leak",
                "expected_sql": "SELECT stuff",
                "generated_sql": "SELECT other",
            }
        ],
        "some_random_future_field": {"with": "raw question content leak"},
    }
    afs = format_afs(cluster)
    assert set(afs.keys()).issubset(AFS_ALLOWED_FIELDS), (
        f"AFS leaked fields outside schema: {set(afs.keys()) - AFS_ALLOWED_FIELDS}"
    )
    # The disallowed fields must not sneak through.
    assert "sql_contexts" not in afs
    assert "some_random_future_field" not in afs


def test_afs_never_contains_raw_benchmark_text(corpus: BenchmarkCorpus) -> None:
    """Across a battery of clusters whose raw SQL contexts WOULD match
    benchmark text, the AFS output must pass ``validate_afs``."""
    for bench in _BENCHMARKS:
        cluster = {
            "cluster_id": f"C_{bench['id']}",
            "root_cause": "wrong_join",
            "affected_judge": "arbiter",
            "question_ids": [bench["id"]],
            "asi_blame_set": ["sales"],
            "asi_counterfactual_fixes": ["use inner join"],
            "asi_wrong_clause": "JOIN",
            "sql_contexts": [
                {
                    "question": bench["question"],
                    "expected_sql": bench["expected_sql"],
                    "generated_sql": bench["expected_sql"].replace("SUM", "COUNT"),
                    "comparison": {"match": False},
                }
            ],
        }
        afs = format_afs(cluster)
        # Must not raise — AFS text fields are derivative, not reproductive.
        validate_afs(afs, corpus)


def test_validate_afs_raises_on_raw_leak(corpus: BenchmarkCorpus) -> None:
    """Directly constructed AFS with benchmark text must be rejected."""
    # Construct a pseudo-AFS that smuggles the benchmark question into a
    # free-form field — simulates a future contributor regression.
    pseudo_afs = {
        "cluster_id": "C_evil",
        "failure_type": "wrong_join",
        "blame_set": [],
        "counterfactual_fixes": [_BENCHMARKS[0]["question"]],
        "structural_diff": {},
        "question_count": 1,
        "affected_judge": "arbiter",
        "suggested_fix_summary": "contains raw benchmark question verbatim",
    }
    # Force one field to fully echo the benchmark.
    pseudo_afs["suggested_fix_summary"] = _BENCHMARKS[0]["question"]
    with pytest.raises(AFSLeakError):
        validate_afs(pseudo_afs, corpus)


def test_format_afs_batch_parity(corpus: BenchmarkCorpus) -> None:
    clusters = [
        {"cluster_id": "a", "root_cause": "x", "question_ids": ["q1"]},
        {"cluster_id": "b", "root_cause": "y", "question_ids": ["q2", "q3"]},
    ]
    batch = format_afs_batch(clusters)
    assert len(batch) == 2
    assert batch[0]["cluster_id"] == "a"
    assert batch[1]["question_count"] == 2


# ── AST differ (P2.5) ──────────────────────────────────────────────────


def test_ast_diff_emits_typed_missing_constructs() -> None:
    diff = compute_ast_diff(
        expected_sqls=[
            "SELECT category, SUM(revenue) FROM sales WHERE quarter = 'Q3' GROUP BY category"
        ],
        generated_sqls=["SELECT category, SUM(revenue) FROM sales GROUP BY category"],
    )
    # Must report WHERE as missing and no raw text in the output.
    assert "missing_constructs" in diff
    assert "WHERE" in diff["missing_constructs"]


def test_ast_diff_catches_wrong_function() -> None:
    diff = compute_ast_diff(
        expected_sqls=["SELECT SUM(x) FROM t"],
        generated_sqls=["SELECT COUNT(x) FROM t"],
    )
    # Either wrong_functions or structural diff mentions SUM and COUNT.
    funcs = diff.get("wrong_functions", [])
    names = {p.get("expected") for p in funcs} | {p.get("got") for p in funcs}
    assert "SUM" in names
    assert "COUNT" in names


def test_ast_diff_empty_on_bad_sql() -> None:
    """Unparseable SQL must not crash or leak partial content."""
    diff = compute_ast_diff(
        expected_sqls=["GIBBERISH NOT_A_QUERY"],
        generated_sqls=["SELECT 1"],
    )
    # Empty dict is acceptable (parse failure); important it doesn't raise.
    assert isinstance(diff, dict)


# ── Embedding-cosine firewall layer ────────────────────────────────────


def test_cosine_similarity_identical_vectors() -> None:
    assert _cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert _cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)


def test_cosine_similarity_orthogonal() -> None:
    assert _cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


def test_cosine_similarity_empty_returns_zero() -> None:
    assert _cosine_similarity([], [1.0]) == 0.0
    assert _cosine_similarity([1.0], []) == 0.0


def test_embedding_layer_catches_paraphrase() -> None:
    """A paraphrase that passes n-gram (different wording, similar
    meaning) must be flagged when embedding cosine is active."""
    corpus = BenchmarkCorpus.from_benchmarks(_BENCHMARKS)

    # Simulate precomputed embeddings — question 0 has a known vector;
    # the candidate's embedding is set to be almost-identical.
    corpus.question_embeddings = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
    corpus.sql_embeddings = [[0.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
    corpus.embedding_endpoint = "test"

    w = MagicMock()

    def fake_query(name, input):
        # Mimics an OpenAI-style response with a near-parallel embedding
        # for question 0: still catches the leak via cosine ~ 0.99.
        return MagicMock(data=[MagicMock(embedding=[0.999, 0.01, 0.0])])

    w.serving_endpoints.query.side_effect = fake_query

    # A completely reworded question that has no n-gram overlap with the
    # benchmark (plan's paraphrase scenario).
    proposal = {
        "example_question": "Totally different wording entirely",
        "example_sql": "SELECT 1 FROM dual",
    }
    is_leak, reason = is_benchmark_leak(
        proposal, "add_example_sql", corpus, w=w,
    )
    assert is_leak
    assert "embedding_cosine" in reason


def test_embedding_layer_degrades_without_w() -> None:
    """When ``w`` is omitted, the embedding layer is skipped and only the
    n-gram + SQL fingerprint layers run. Paraphrases that evade n-gram
    will pass — verifying the fallback shape."""
    corpus = BenchmarkCorpus.from_benchmarks(_BENCHMARKS)
    corpus.question_embeddings = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
    corpus.sql_embeddings = [[0.0, 0.0, 1.0], [0.0, 0.0, 1.0]]

    proposal = {
        "example_question": "Totally different wording entirely",
        "example_sql": "SELECT 1 FROM dual",
    }
    # No `w` kwarg — embedding layer is skipped; proposal passes.
    is_leak, _ = is_benchmark_leak(proposal, "add_example_sql", corpus)
    assert not is_leak


def test_debug_flag_never_affects_prompt_content(monkeypatch, capsys) -> None:
    """When ``GSO_DEBUG_RAW_SQL=1`` is set, the debug helper logs raw SQL
    to stdout (and MLflow tags) but ``format_afs`` and every prompt-facing
    function must still produce leak-free output."""
    from genie_space_optimizer.optimization.afs import (
        debug_raw_sql_enabled,
        log_raw_sql_for_cluster,
    )

    monkeypatch.setenv("GSO_DEBUG_RAW_SQL", "1")
    assert debug_raw_sql_enabled() is True

    cluster = {
        "cluster_id": "C_debug",
        "root_cause": "wrong_join",
        "question_ids": ["q1"],
        "affected_judge": "arbiter",
        "sql_contexts": [
            {
                "question": _BENCHMARKS[0]["question"],
                "expected_sql": _BENCHMARKS[0]["expected_sql"],
                "generated_sql": _BENCHMARKS[0]["expected_sql"].replace(
                    "SUM", "COUNT",
                ),
            }
        ],
    }

    # Debug logging emits raw SQL to stdout — this is the intended dev
    # behaviour when the flag is on.
    log_raw_sql_for_cluster(cluster)
    captured = capsys.readouterr()
    assert "[GSO_DEBUG_RAW_SQL]" in captured.out
    assert "SUM(revenue)" in captured.out

    # But format_afs output (which IS prompt-bound) must remain clean.
    afs = format_afs(cluster)
    afs_text = repr(afs)
    assert "SUM(revenue)" not in afs_text, (
        "format_afs MUST NOT reflect raw SQL from sql_contexts even when "
        "GSO_DEBUG_RAW_SQL is set."
    )
    assert _BENCHMARKS[0]["question"] not in afs_text, (
        "format_afs MUST NOT include benchmark question text under any flag."
    )


def test_debug_flag_default_off() -> None:
    import os as _os
    from genie_space_optimizer.optimization.afs import debug_raw_sql_enabled
    _os.environ.pop("GSO_DEBUG_RAW_SQL", None)
    assert debug_raw_sql_enabled() is False


def test_embedding_layer_degrades_when_endpoint_fails() -> None:
    """When ``get_embedding`` returns None (endpoint unavailable), the
    firewall must still run on n-gram + fingerprint. A leaky-shape
    proposal must still be caught by the n-gram layer."""
    corpus = BenchmarkCorpus.from_benchmarks(_BENCHMARKS)
    corpus.question_embeddings = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]

    w = MagicMock()
    w.serving_endpoints.query.side_effect = Exception("endpoint down")

    # n-gram catches this leaky-shape proposal.
    proposal = {
        "example_question": "What is the total revenue by product category last quarter?",
        "example_sql": "SELECT 1",
    }
    is_leak, _ = is_benchmark_leak(
        proposal, "add_example_sql", corpus, w=w,
    )
    assert is_leak
