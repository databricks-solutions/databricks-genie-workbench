"""MV-D117 (C-6): identifier quoting is cosmetic to the firewall, on both sides."""

from genie_space_optimizer.optimization.leakage import (
    NGRAM_SIMILARITY_THRESHOLD,
    BenchmarkCorpus,
    LeakageOracle,
    canonicalize_sql,
    is_benchmark_leak,
)

QUOTED = "SELECT SUM(`amount`) FROM `main`.`sales`.`orders` WHERE `status` = 'Paid'"
BARE = "SELECT SUM(amount) FROM main.sales.orders WHERE status = 'Paid'"

# Near, not equal: the fingerprint misses these, so only the shingles can match.
# Quoted against bare they score 0.528, under the threshold; folded, 0.857.
QUOTED_FRAGMENT = "SUM(CASE WHEN `status` = 'X' THEN `amt` END)"
BARE_FRAGMENT = "SUM(CASE WHEN status = 'X' THEN amt END)"
NEAR_BARE = "SUM(CASE WHEN status = 'X' THEN amt ELSE 0 END)"
NEAR_QUOTED = "SUM(CASE WHEN `status` = 'X' THEN `amt` ELSE 0 END)"


def _oracle(expected_sql: str) -> LeakageOracle:
    return LeakageOracle(BenchmarkCorpus.from_benchmarks(
        [{"id": "b1", "question": "q", "expected_sql": expected_sql}]
    ))


def test_the_fingerprint_ignores_identifier_quotes():
    assert canonicalize_sql(QUOTED) == canonicalize_sql(BARE)


def test_a_quoted_benchmark_catches_a_bare_probe():
    oracle = LeakageOracle(BenchmarkCorpus.from_benchmarks(
        [{"id": "b1", "question": "q", "expected_sql": QUOTED}]
    ))
    assert oracle.contains_sql(BARE)


def test_a_bare_benchmark_catches_a_quoted_probe():
    oracle = LeakageOracle(BenchmarkCorpus.from_benchmarks(
        [{"id": "b1", "question": "q", "expected_sql": BARE}]
    ))
    assert oracle.contains_sql(QUOTED)


def test_a_quoted_alias_still_folds():
    assert canonicalize_sql("SELECT a AS `x` FROM t") == canonicalize_sql("SELECT a AS `y` FROM t")


def test_a_quoted_benchmark_fragment_catches_a_near_bare_probe():
    assert canonicalize_sql(NEAR_BARE) != canonicalize_sql(QUOTED_FRAGMENT)
    assert _oracle(QUOTED_FRAGMENT).contains_sql(NEAR_BARE)


def test_a_bare_benchmark_fragment_catches_a_near_quoted_probe():
    assert _oracle(BARE_FRAGMENT).contains_sql(NEAR_QUOTED)


def _prose_leak(expected_sql: str, prose: str) -> tuple[bool, str]:
    corpus = BenchmarkCorpus.from_benchmarks(
        [{"id": "b1", "question": "unrelated wording entirely", "expected_sql": expected_sql}]
    )
    return is_benchmark_leak({"example_question": prose}, "add_example_sql", corpus)


def test_prose_pasting_a_quoted_benchmark_sql_is_flagged():
    """A text field is compared with the SQL shingles too, so its probe folds."""
    assert _prose_leak(QUOTED, QUOTED) == (
        True, "add_example_sql.example_question:ngram_similarity_sql_qid=b1",
    )


def test_prose_pasting_bare_sql_against_a_quoted_benchmark_is_flagged():
    assert _prose_leak(QUOTED, BARE)[0] is True


def test_prose_pasting_bare_sql_against_a_bare_benchmark_is_flagged():
    assert _prose_leak(BARE, BARE) == (
        True, "add_example_sql.example_question:ngram_similarity_sql_qid=b1",
    )


def test_sql_echoing_a_quoted_benchmark_question_is_compared_unfolded():
    """Questions are shingled raw on the corpus side, so the probe is too."""
    question = "`net_amount` by `region` for `fiscal_q` in `emea`"
    corpus = BenchmarkCorpus.from_benchmarks(
        [{"id": "b1", "question": question, "expected_sql": "SELECT 1"}]
    )
    assert is_benchmark_leak({"example_sql": question}, "add_example_sql", corpus) == (
        True, "add_example_sql.example_sql:ngram_similarity_question_qid=b1",
    )


def test_example_sql_scoring_folds_quotes():
    decision = _oracle(BARE_FRAGMENT).evaluate_example_sql(
        question="unrelated wording entirely", sql=NEAR_QUOTED,
    )
    assert decision.sql_score >= NGRAM_SIMILARITY_THRESHOLD
