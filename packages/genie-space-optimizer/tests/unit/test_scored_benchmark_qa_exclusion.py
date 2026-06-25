"""GSO Optimizer v2 — Phase 2 example-SQL leakage guard (§3.6 / D8).

Hard eval-validity invariant: NO scored benchmark question/answer may be
seeded into the Genie Space's *Example SQL Queries* section — including
examples derived from *passing* benchmark rows. The exclusion is keyed on
THREE deterministic, always-on keys (question-id, normalized-SQL hash,
canonical question text) and is INDEPENDENT of the tunable fuzzy
``evaluate_example_sql`` policy / ``GSO_EXAMPLE_SQL_FIREWALL_STRICT``.

A regression on any of these tests re-opens the answer-key leak that
fraudulently inflates the official Benchmark API score.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.leakage import (
    BenchmarkCorpus,
    LeakageOracle,
    canonical_question_key,
    canonicalize_sql,
    is_example_sql_benchmark_leak,
)

# The whole scored benchmark set (no train/held-out split per D8). The
# corpus deliberately mixes a row that Genie would PASS today (b_pass)
# with the rest — passing rows are equally disqualified as example SQL.
_SCORED_BENCHMARKS: list[dict] = [
    {
        "id": "b_fail",
        "question": "What is total revenue by region?",
        "expected_sql": (
            "SELECT region, SUM(amount) AS total FROM cat.sch.sales GROUP BY region"
        ),
    },
    {
        "id": "b_pass",
        "question": "How many orders were placed yesterday?",
        "expected_sql": (
            "SELECT COUNT(*) FROM cat.sch.orders WHERE order_date = current_date - 1"
        ),
    },
]


def _oracle() -> LeakageOracle:
    return LeakageOracle(BenchmarkCorpus.from_benchmarks(_SCORED_BENCHMARKS))


# ── Deterministic three-key exclusion ──────────────────────────────────


def test_excludes_by_question_id() -> None:
    matched, reason = _oracle().is_scored_benchmark_qa(
        question="totally different prompt text",
        sql="SELECT 1",
        question_id="b_pass",
    )
    assert matched is True
    assert "question_id=b_pass" in reason


def test_excludes_by_normalized_sql_hash_even_with_different_question() -> None:
    # Same SQL modulo casing/whitespace, unrelated question text.
    matched, reason = _oracle().is_scored_benchmark_qa(
        question="an analyst sanity check phrased completely differently",
        sql="select region, sum(amount) as total\nfrom cat.sch.sales group by region;",
    )
    assert matched is True
    assert reason == "scored_benchmark_sql_hash"


def test_excludes_by_canonical_question_even_with_different_sql() -> None:
    # Same question modulo punctuation/casing, different SQL.
    matched, reason = _oracle().is_scored_benchmark_qa(
        question="What is total revenue by REGION?!",
        sql="SELECT region, revenue FROM some.other.table",
    )
    assert matched is True
    assert reason == "scored_benchmark_question_text"


def test_passing_row_seeded_verbatim_is_excluded() -> None:
    # The exact leak vector the guard exists to close: seeding a PASSING
    # benchmark row verbatim as a teaching example.
    matched, _ = _oracle().is_scored_benchmark_qa(
        question=_SCORED_BENCHMARKS[1]["question"],
        sql=_SCORED_BENCHMARKS[1]["expected_sql"],
        question_id=_SCORED_BENCHMARKS[1]["id"],
    )
    assert matched is True


def test_novel_example_is_not_excluded() -> None:
    matched, reason = _oracle().is_scored_benchmark_qa(
        question="What is the average basket size for repeat customers?",
        sql="SELECT AVG(items) FROM cat.sch.baskets WHERE repeat_customer = true",
        question_id="synth_001",
    )
    assert matched is False
    assert reason == ""


# ── Applier-side firewall hard-blocks regardless of strict mode ─────────


def test_applier_firewall_blocks_verbatim_passing_row_in_relaxed_mode(monkeypatch) -> None:
    # Even with the fuzzy firewall relaxed, a verbatim scored-benchmark
    # Q/A must be blocked at the applier-side example-SQL chokepoint.
    monkeypatch.setenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", "false")
    corpus = BenchmarkCorpus.from_benchmarks(_SCORED_BENCHMARKS)
    proposal = {
        "patch_type": "add_example_sql",
        "example_question": _SCORED_BENCHMARKS[1]["question"],
        "example_sql": _SCORED_BENCHMARKS[1]["expected_sql"],
        "benchmark_id": "b_pass",
    }
    block, reason = is_example_sql_benchmark_leak(proposal, corpus)
    assert block is True
    assert reason.startswith("scored_benchmark_")


def test_applier_firewall_blocks_sql_hash_match_in_relaxed_mode(monkeypatch) -> None:
    # No question echo, no carried id — only the normalized-SQL hash
    # matches. The relaxed fuzzy policy would merely *warn*; the
    # deterministic guard *blocks*.
    monkeypatch.setenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", "false")
    corpus = BenchmarkCorpus.from_benchmarks(_SCORED_BENCHMARKS)
    proposal = {
        "patch_type": "add_example_sql",
        "example_question": "show sales grouped by region for a dashboard tile",
        "example_sql": _SCORED_BENCHMARKS[0]["expected_sql"],
    }
    block, reason = is_example_sql_benchmark_leak(proposal, corpus)
    assert block is True
    assert reason == "scored_benchmark_sql_hash"


def test_applier_firewall_allows_novel_example(monkeypatch) -> None:
    monkeypatch.setenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", "true")
    corpus = BenchmarkCorpus.from_benchmarks(_SCORED_BENCHMARKS)
    proposal = {
        "patch_type": "add_example_sql",
        "example_question": "What is the average basket size for repeat customers?",
        "example_sql": "SELECT AVG(items) FROM cat.sch.baskets WHERE repeat_customer = true",
    }
    block, _ = is_example_sql_benchmark_leak(proposal, corpus)
    assert block is False


# ── Headline guarantee: no scored benchmark Q/A reaches example SQL ─────


def test_no_scored_benchmark_qa_survives_the_seeding_filter() -> None:
    """Mirror the filter loop in ``_apply_proactive_example_sqls``: every
    candidate that is a scored benchmark Q/A (verbatim, sql-hash-only, or
    passing-row) must be dropped; only the genuinely novel example
    survives into the Example SQL Queries config."""
    corpus = BenchmarkCorpus.from_benchmarks(_SCORED_BENCHMARKS)
    candidates = [
        # verbatim failing row
        {
            "patch_type": "add_example_sql",
            "example_question": _SCORED_BENCHMARKS[0]["question"],
            "example_sql": _SCORED_BENCHMARKS[0]["expected_sql"],
        },
        # verbatim passing row
        {
            "patch_type": "add_example_sql",
            "example_question": _SCORED_BENCHMARKS[1]["question"],
            "example_sql": _SCORED_BENCHMARKS[1]["expected_sql"],
            "benchmark_id": "b_pass",
        },
        # same SQL, paraphrased question
        {
            "patch_type": "add_example_sql",
            "example_question": "sales totals per geography please",
            "example_sql": _SCORED_BENCHMARKS[0]["expected_sql"],
        },
        # genuinely novel teaching example
        {
            "patch_type": "add_example_sql",
            "example_question": "What is the average basket size for repeat customers?",
            "example_sql": "SELECT AVG(items) FROM cat.sch.baskets WHERE repeat_customer = true",
        },
    ]

    survivors = [
        c for c in candidates
        if not is_example_sql_benchmark_leak(c, corpus)[0]
    ]

    assert len(survivors) == 1
    survivor_sql_fp = canonicalize_sql(survivors[0]["example_sql"])
    # The survivor's SQL fingerprint is disjoint from every scored row.
    assert survivor_sql_fp not in corpus.sql_fingerprints
    # And its canonical question matches no scored benchmark question.
    scored_q = {canonical_question_key(b["question"]) for b in _SCORED_BENCHMARKS}
    assert canonical_question_key(survivors[0]["example_question"]) not in scored_q
