"""Bug #4 invariants — benchmark leakage firewall + no-verbatim-mining.

Together these tests ensure no benchmark expected_sql or question text can
flow verbatim (or near-verbatim) into any persisted optimizer output. A
regression on any of these tests means the leak is re-opened.

Contract coverage:

* ``is_benchmark_leak`` is shape-aware across all patch types listed in the
  plan (P1.3). Leaky proposals are rejected; clean proposals pass.
* The original `_mine_benchmark_example_sqls` function refuses to run
  without ``GSO_ALLOW_VERBATIM_MINING=1`` — it was the primary mining path
  and is now an intentional NOP.
* ``_resolve_lever5_llm_result`` no longer copies representative
  ``question`` / ``expected_sql`` verbatim; it falls through to text
  instruction and bumps ``secondary_mining_blocked``.
* ``publish_benchmarks_to_genie_space`` merges into existing questions
  (never overwrites), tags optimizer rows with ``[auto-optimize]`` +
  structured metadata, and skips questions that would mirror an existing
  ``example_question_sqls`` entry (the exact leak Bug #4 guards against).
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization import leakage
from genie_space_optimizer.optimization.leakage import (
    BenchmarkCorpus,
    canonicalize_sql,
    count_example_sql_leaks,
    is_benchmark_leak,
)


# ── Corpus used across tests ────────────────────────────────────────────

_BENCHMARKS: list[dict] = [
    {
        "id": "q1",
        "question": "What is the total revenue by product category last quarter?",
        "expected_sql": (
            "SELECT category, SUM(revenue) FROM sales "
            "WHERE quarter = 'Q3' GROUP BY category"
        ),
    },
    {
        "id": "q2",
        "question": "Show the top 10 customers by lifetime value",
        "expected_sql": (
            "SELECT customer_id, SUM(amount) AS ltv FROM orders "
            "GROUP BY customer_id ORDER BY ltv DESC LIMIT 10"
        ),
    },
    {
        "id": "q3",
        "question": "List all orders placed in the last 30 days",
        "expected_sql": (
            "SELECT * FROM orders WHERE order_date >= current_date - 30"
        ),
    },
]


@pytest.fixture
def corpus() -> BenchmarkCorpus:
    return BenchmarkCorpus.from_benchmarks(_BENCHMARKS)


# ── canonicalize_sql ───────────────────────────────────────────────────


def test_canonicalize_sql_normalizes_whitespace_and_case() -> None:
    a = "SELECT x  FROM t  WHERE y = 1"
    b = "select x from t where y = 1"
    c = "SELECT x\nFROM t\nWHERE y = 1;"
    assert canonicalize_sql(a) == canonicalize_sql(b) == canonicalize_sql(c)


def test_canonicalize_sql_ignores_trailing_semis_and_comments() -> None:
    a = "SELECT x FROM t WHERE y = 1"
    b = "SELECT x /* inline comment */ FROM t -- trailing\nWHERE y = 1;"
    assert canonicalize_sql(a) == canonicalize_sql(b)


def test_canonicalize_sql_empty_returns_empty() -> None:
    assert canonicalize_sql("") == ""
    assert canonicalize_sql("   \n ") == ""


def test_canonicalize_sql_diff_for_different_queries() -> None:
    a = "SELECT * FROM sales"
    b = "SELECT * FROM customers"
    assert canonicalize_sql(a) != canonicalize_sql(b)


# ── is_benchmark_leak: per patch type ──────────────────────────────────


@pytest.mark.parametrize(
    "patch_type, proposal",
    [
        # add_example_sql — near-verbatim example_sql
        (
            "add_example_sql",
            {
                "example_question": "What is total revenue by product category last quarter?",
                "example_sql": (
                    "SELECT category, SUM(revenue) FROM sales "
                    "WHERE quarter = 'Q3' GROUP BY category"
                ),
            },
        ),
        # add_example_sql — identical-fingerprint SQL with different question
        (
            "add_example_sql",
            {
                "example_question": "A completely unrelated question about the schema",
                "example_sql": (
                    "select category, sum(revenue) from sales "
                    "where quarter = 'Q3' group by category;"
                ),
            },
        ),
        # Note: ``add_instruction``, ``add_column_description``,
        # ``add_sql_snippet_*``, and ``add_join_spec`` / ``update_join_spec``
        # entries were removed from this parametrized list. The firewall
        # is intentionally scoped to example SQL answer-shape artifacts
        # only (see the scoping comment above ``_PATCH_TEXT_FIELDS`` in
        # leakage.py and the dedicated allow-through tests below).
    ],
)
def test_leaky_proposals_are_rejected(
    patch_type: str, proposal: dict, corpus: BenchmarkCorpus,
) -> None:
    is_leak, reason = is_benchmark_leak(proposal, patch_type, corpus)
    assert is_leak, f"Expected leak for {patch_type}, got clean ({reason})"
    assert reason, "Reason must be non-empty on a leak"


@pytest.mark.parametrize(
    "patch_type, proposal",
    [
        # Structural SQL primitives whose fingerprint matches a benchmark
        # ARE allowed through — they are building blocks, not answers.
        # Proof: even a verbatim lift of an expected_sql fragment into a
        # measure / filter / expression / join_spec passes.
        (
            "add_sql_snippet_measure",
            {
                "sql": (
                    "SELECT customer_id, SUM(amount) AS ltv FROM orders "
                    "GROUP BY customer_id ORDER BY ltv DESC LIMIT 10"
                ),
                "display_name": "lifetime_value",
            },
        ),
        (
            "add_sql_snippet_filter",
            {
                "sql": "order_date >= current_date - 30",
                "display_name": "last_30_days",
                "synonyms": ["list all orders placed in the last 30 days"],
            },
        ),
        (
            "add_sql_snippet_expression",
            {
                "sql": "DATE_TRUNC('quarter', order_date)",
                "display_name": "order_quarter",
            },
        ),
        (
            "add_join_spec",
            {
                "description": "Join orders to customers on customer_id",
                "comment": "Captures benchmark pattern but is STRUCTURE, not an ANSWER",
            },
        ),
        (
            "update_join_spec",
            {
                "description": "Refine join on customer_id to include soft-deleted customers",
                "comment": "",
            },
        ),
    ],
)
def test_structural_sql_allowed_even_when_matches_benchmark(
    patch_type: str, proposal: dict, corpus: BenchmarkCorpus,
) -> None:
    """Structural SQL (sql_snippet / join_spec) is intentionally exempt
    from the firewall. Exec-validation + source-gating (arbiter-approved
    source filter for proactive, post-iteration rollback for Lever 6)
    supersede fingerprint matching for these patch types.
    """
    is_leak, reason = is_benchmark_leak(proposal, patch_type, corpus)
    assert not is_leak, (
        f"Structural SQL firewall must be OFF for {patch_type}; "
        f"got leak={is_leak} reason={reason!r}"
    )


@pytest.mark.parametrize(
    "patch_type, proposal",
    [
        (
            "add_example_sql",
            {
                "example_question": "How many distinct SKUs were shipped yesterday?",
                "example_sql": (
                    "SELECT COUNT(DISTINCT sku) FROM shipments "
                    "WHERE ship_date = current_date - 1"
                ),
            },
        ),
        (
            "add_instruction",
            {
                "new_text": (
                    "Prefer the metric view `revenue_mv` when aggregating by "
                    "category. Use `orders.order_date` for date filters."
                ),
            },
        ),
        (
            "add_column_description",
            {
                "description": "Monotonically-increasing surrogate key for the orders table.",
            },
        ),
        (
            "add_sql_snippet_measure",
            {
                "sql": "AVG(order_total)",
                "display_name": "avg_order_total",
            },
        ),
    ],
)
def test_clean_proposals_pass(
    patch_type: str, proposal: dict, corpus: BenchmarkCorpus,
) -> None:
    is_leak, reason = is_benchmark_leak(proposal, patch_type, corpus)
    assert not is_leak, (
        f"Expected clean for {patch_type}; got leak ({reason})"
    )


def test_rca_driven_example_sql_still_rejected_when_it_matches_benchmark(corpus) -> None:
    proposal = {
        "patch_type": "add_example_sql",
        "example_question": "Original RCA-generated prompt",
        "example_sql": (
            "SELECT category, SUM(revenue) FROM sales "
            "WHERE quarter = 'Q3' GROUP BY category"
        ),
        "source": "rca_theme",
        "rca_id": "rca_shape",
    }

    is_leak, reason = is_benchmark_leak(proposal, "add_example_sql", corpus)

    assert is_leak is True
    assert reason


def test_unknown_patch_type_does_not_trigger() -> None:
    # New patch types that don't persist inference-visible content must not
    # be checked (false positives are costly). The owner adds them to
    # _PATCH_TEXT_FIELDS explicitly when they should be tested.
    is_leak, _ = is_benchmark_leak(
        {"some_other_field": "this is a benchmark question verbatim"},
        "some_unknown_patch_type",
        BenchmarkCorpus.from_benchmarks(_BENCHMARKS),
    )
    assert not is_leak


def test_empty_corpus_never_triggers(corpus: BenchmarkCorpus) -> None:
    empty = BenchmarkCorpus.from_benchmarks([])
    is_leak, _ = is_benchmark_leak(
        {"example_question": "any", "example_sql": "select 1"},
        "add_example_sql",
        empty,
    )
    assert not is_leak


# ── count_example_sql_leaks (audit) ────────────────────────────────────


def test_count_example_sql_leaks_catches_persisted_leak(corpus: BenchmarkCorpus) -> None:
    space_config = {
        "example_question_sqls": [
            {
                "question": ["What is the total revenue by product category last quarter?"],
                "sql": [(
                    "SELECT category, SUM(revenue) FROM sales "
                    "WHERE quarter = 'Q3' GROUP BY category"
                )],
            }
        ],
    }
    counts = count_example_sql_leaks(space_config, corpus)
    assert counts.get("add_example_sql") == 1


def test_count_example_sql_leaks_ignores_clean_space(corpus: BenchmarkCorpus) -> None:
    space_config = {
        "example_question_sqls": [
            {
                "question": ["How many distinct SKUs were shipped yesterday?"],
                "sql": ["SELECT COUNT(DISTINCT sku) FROM shipments"],
            }
        ],
    }
    counts = count_example_sql_leaks(space_config, corpus)
    assert counts == {}


# ── No-verbatim-mining invariant ───────────────────────────────────────


def test_deprecated_mine_function_raises_without_flag() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        _DEPRECATED_mine_benchmark_example_sqls_verbatim,
    )
    with pytest.raises(RuntimeError, match="GSO_ALLOW_VERBATIM_MINING"):
        _DEPRECATED_mine_benchmark_example_sqls_verbatim(
            benchmarks=_BENCHMARKS, metadata_snapshot={},
        )


def test_no_import_path_references_old_name() -> None:
    # Make sure no production code imports the legacy verbatim-mining
    # function under its old name. This catches rebase regressions.
    import pathlib
    pkg = pathlib.Path(
        "packages/genie-space-optimizer/src/genie_space_optimizer"
    )
    forbidden = "import _mine_benchmark_example_sqls"
    call_pat = "_mine_benchmark_example_sqls("
    for py in pkg.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert forbidden not in text, (
            f"{py}: must not import _mine_benchmark_example_sqls"
        )
        assert call_pat not in text, (
            f"{py}: must not call _mine_benchmark_example_sqls(...)"
        )


# ── Secondary mining path closed ───────────────────────────────────────


def test_resolve_lever5_does_not_copy_sql_for_sql_pattern() -> None:
    """Bug #4 contract, updated by Phase A3b of the router-and-resilience plan.

    Before A3b: a ``text_instruction`` response for a ``wrong_join`` cluster
    would fall through to ``add_instruction`` (weak text fallback) while the
    secondary-mining counter was bumped to prove verbatim copy was blocked.

    After A3b: for any root cause in ``_SQL_SHAPE_ROOT_CAUSES`` (which is a
    superset of ``_SQL_PATTERN_ROOT_CAUSES``), the resolver returns the
    sentinel ``("skipped_no_example_sql", {...})`` instead of a weak text
    instruction. The ``secondary_mining_blocked`` counter is still bumped
    for observability parity so Bug #4 dashboards continue to show the
    original benchmark-leakage prevention.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _BUG4_COUNTERS,
        _SQL_SHAPE_ROOT_CAUSES,
        _resolve_lever5_llm_result,
        reset_bug4_counters,
    )

    reset_bug4_counters()

    llm_result = {
        "instruction_type": "text_instruction",
        "instruction_text": (
            "JOIN orders with customers on customer_id; do not select "
            "PII columns; use revenue_mv when possible."
        ),
        "rationale": "LLM-level instruction, not a SQL copy.",
    }
    cluster = {
        "root_cause": "wrong_join",
        "sql_contexts": [
            {
                "question": "What is total revenue by product category last quarter?",
                "expected_sql": (
                    "SELECT category, SUM(revenue) FROM sales "
                    "WHERE quarter = 'Q3' GROUP BY category"
                ),
            }
        ],
    }

    patch_type, extra = _resolve_lever5_llm_result(
        llm_result, original_patch_type="add_example_sql", cluster=cluster,
    )

    # A3b contract: structural causes must not emit weak text instructions.
    assert patch_type == "skipped_no_example_sql"
    assert extra.get("root_cause") in _SQL_SHAPE_ROOT_CAUSES
    assert "example_sql" not in extra
    assert "forced_from_sql_pattern" not in extra
    # Bug #4 parity preserved — secondary mining path still counted as blocked.
    assert _BUG4_COUNTERS["secondary_mining_blocked"] >= 1


# ── publish_benchmarks_to_genie_space integrity ────────────────────────


def test_publish_benchmarks_merges_with_existing() -> None:
    """User-authored benchmarks must survive the merge; optimizer rows are
    appended as plain benchmark questions (no [auto-optimize] prefix, no
    GSO metadata payload)."""
    from genie_space_optimizer.common.genie_client import (
        AUTO_OPTIMIZE_TAG_PREFIX,
        publish_benchmarks_to_genie_space,
    )

    existing_q = "User-curated question that predates the run"
    existing_benchmarks_section = {
        "questions": [
            {
                "id": "user-1",
                "question": [existing_q],
                "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
            }
        ],
    }

    captured: dict = {}

    def fake_fetch(w, space_id):
        return {
            "_parsed_space": {
                "benchmarks": existing_benchmarks_section,
                "example_question_sqls": [],
            },
        }

    def fake_patch(w, space_id, parsed):
        captured["parsed"] = parsed

    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=fake_fetch,
    ), patch(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        side_effect=fake_patch,
    ):
        new_count = publish_benchmarks_to_genie_space(
            w=MagicMock(),
            space_id="space-xyz",
            benchmarks=_BENCHMARKS,
            run_id="run-123",
        )

    assert new_count == len(_BENCHMARKS)
    merged = captured["parsed"]["benchmarks"]["questions"]

    # User-authored row survives and is first.
    assert merged[0]["question"] == [existing_q]

    # Every optimizer row is appended as a plain Genie benchmark question:
    # no [auto-optimize] prefix and no GSO metadata payload (the optimizer
    # keeps provenance in the UC evaluation dataset, not in the space).
    optimizer_rows = merged[1:]
    assert len(optimizer_rows) == len(_BENCHMARKS)
    for row in optimizer_rows:
        assert not row["question"][0].startswith(AUTO_OPTIMIZE_TAG_PREFIX)
        assert "metadata" not in row


def test_publish_benchmarks_dedupes_existing_matches() -> None:
    """If an incoming benchmark matches an existing user-authored one
    (n-gram >= 0.90 on normalized question text), it must be skipped."""
    from genie_space_optimizer.common.genie_client import (
        publish_benchmarks_to_genie_space,
    )

    existing_q = _BENCHMARKS[0]["question"]
    existing_benchmarks_section = {
        "questions": [
            {
                "id": "user-dup",
                "question": [existing_q],
                "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
            }
        ],
    }

    captured: dict = {}

    def fake_fetch(w, space_id):
        return {
            "_parsed_space": {
                "benchmarks": existing_benchmarks_section,
                "example_question_sqls": [],
            },
        }

    def fake_patch(w, space_id, parsed):
        captured["parsed"] = parsed

    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=fake_fetch,
    ), patch(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        side_effect=fake_patch,
    ):
        new_count = publish_benchmarks_to_genie_space(
            w=MagicMock(),
            space_id="space-xyz",
            benchmarks=_BENCHMARKS,
            run_id="run-123",
        )

    # Exactly N-1 net-new (q1 dups an existing row).
    assert new_count == len(_BENCHMARKS) - 1


def test_publish_benchmarks_skips_rows_mirrored_in_example_sqls() -> None:
    """If a benchmark's question is already in example_question_sqls, we
    must not publish it to space.benchmarks — mirroring would reinstate
    the exact leak Bug #4 prevents."""
    from genie_space_optimizer.common.genie_client import (
        publish_benchmarks_to_genie_space,
    )

    captured: dict = {}

    def fake_fetch(w, space_id):
        return {
            "_parsed_space": {
                "benchmarks": {"questions": []},
                "example_question_sqls": [
                    {
                        "question": [_BENCHMARKS[0]["question"]],
                        "sql": [_BENCHMARKS[0]["expected_sql"]],
                    }
                ],
            },
        }

    def fake_patch(w, space_id, parsed):
        captured["parsed"] = parsed

    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=fake_fetch,
    ), patch(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        side_effect=fake_patch,
    ):
        new_count = publish_benchmarks_to_genie_space(
            w=MagicMock(),
            space_id="space-xyz",
            benchmarks=_BENCHMARKS,
            run_id="run-123",
        )

    # q1 is mirrored; the remaining N-1 should still publish.
    assert new_count == len(_BENCHMARKS) - 1


def test_publish_benchmarks_writes_plain_questions_without_gso_markers() -> None:
    from genie_space_optimizer.common.genie_client import (
        AUTO_OPTIMIZE_TAG_PREFIX,
        publish_benchmarks_to_genie_space,
    )

    captured: dict = {}

    def fake_fetch(w, space_id):
        return {"_parsed_space": {"benchmarks": {"questions": []}, "instructions": {}}}

    def fake_patch(w, space_id, parsed):
        captured["parsed"] = parsed

    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=fake_fetch,
    ), patch(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        side_effect=fake_patch,
    ):
        new_count = publish_benchmarks_to_genie_space(
            w=MagicMock(),
            space_id="space-xyz",
            benchmarks=_BENCHMARKS[:1],
            run_id="run-123",
        )

    assert new_count == 1
    published = captured["parsed"]["benchmarks"]["questions"][0]
    assert not published["question"][0].startswith(AUTO_OPTIMIZE_TAG_PREFIX)
    assert "metadata" not in published


def test_publish_benchmarks_opt_out_flag_skips_publish(monkeypatch) -> None:
    """Setting GSO_PUBLISH_BENCHMARKS_TO_SPACE=0 must prevent harness from
    calling publish_benchmarks_to_genie_space at all. We can't import the
    harness function directly here (too many deps); instead assert that
    the config flag respects the env."""
    monkeypatch.setenv("GSO_PUBLISH_BENCHMARKS_TO_SPACE", "0")
    # Re-import the config module to pick up the env flag.
    import importlib
    import genie_space_optimizer.common.config as _cfg
    importlib.reload(_cfg)
    assert _cfg.PUBLISH_BENCHMARKS_TO_SPACE is False

    monkeypatch.setenv("GSO_PUBLISH_BENCHMARKS_TO_SPACE", "1")
    importlib.reload(_cfg)
    assert _cfg.PUBLISH_BENCHMARKS_TO_SPACE is True


def test_benchmark_firewall_only_blocks_example_sql_answer_shape() -> None:
    from genie_space_optimizer.optimization.leakage import (
        BenchmarkCorpus,
        is_benchmark_leak,
    )

    corpus = BenchmarkCorpus.from_benchmarks([
        {
            "id": "q1",
            "question": "Use fn_mtd_or_mtday for PY sales MTD.",
            "expected_sql": (
                "SELECT prashanth_subrahmanyam_catalog.sales_reports."
                "fn_mtd_or_mtday(MEASURE(`_7now_py_sales_mtd`))"
            ),
        }
    ])

    example = {
        "example_question": "Use fn_mtd_or_mtday for PY sales MTD.",
        "example_sql": (
            "SELECT prashanth_subrahmanyam_catalog.sales_reports."
            "fn_mtd_or_mtday(MEASURE(`_7now_py_sales_mtd`))"
        ),
    }
    instruction = {
        "new_text": (
            "FUNCTION ROUTING:\n"
            "- When users ask for fn_mtd_or_mtday, use the registered function instead of inlining CASE logic."
        )
    }
    description = {
        "description": "Column supports fn_mtd_or_mtday month-to-date routing semantics."
    }

    assert is_benchmark_leak(example, "add_example_sql", corpus)[0] is True
    assert is_benchmark_leak(instruction, "add_instruction", corpus)[0] is False
    assert is_benchmark_leak(description, "update_column_description", corpus)[0] is False
