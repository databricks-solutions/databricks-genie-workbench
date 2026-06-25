"""GSO Optimizer v2 — Phase 2 example-SQL leakage guard on EVERY write path.

Blocking-3 regression coverage. The deterministic scored-benchmark Q/A
hard-block (question-id / canonical question text / normalized-SQL hash) must
fire on every path that can persist a question+SQL pair into the live space's
Example SQL Queries section, not only the proactive seeding path:

* ``optimizer._validate_lever5_proposals``  → via ``is_benchmark_leak``
* ``synthesis.validate_synthesis_proposal`` → via ``_gate_firewall`` →
  ``is_benchmark_leak``
* ``applier.apply_patch_set``               → last-mile deterministic guard

A regression on any of these re-opens the answer-key leak that fraudulently
inflates the official Benchmark API score.
"""

from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.leakage import BenchmarkCorpus

# A scored benchmark with a distinctive long SQL so the *novel* control rows
# do not collide with it under the fuzzy n-gram firewall (threshold 0.60).
_BENCH_QUESTION = "what is total revenue by region for the prior fiscal year"
_BENCH_SQL = (
    "SELECT region_name, SUM(net_amount) AS total_revenue "
    "FROM cat.sch.fact_sales WHERE fiscal_year = 2024 GROUP BY region_name"
)
_NOVEL_QUESTION = "how many distinct active subscribers signed up last quarter"
_NOVEL_SQL = (
    "SELECT COUNT(DISTINCT subscriber_id) FROM cat.sch.dim_subscriber "
    "WHERE signup_quarter = 'Q3' AND is_active"
)

_SCORED_BENCHMARKS = [
    {"id": "b_rev", "question": _BENCH_QUESTION, "expected_sql": _BENCH_SQL},
]


def _corpus() -> BenchmarkCorpus:
    return BenchmarkCorpus.from_benchmarks(_SCORED_BENCHMARKS)


# ── Lever-5 path: optimizer._validate_lever5_proposals ─────────────────


def _lever5(proposals, **kw):
    from genie_space_optimizer.optimization.optimizer import _validate_lever5_proposals

    return _validate_lever5_proposals(proposals, {}, **kw)


def test_lever5_drops_verbatim_scored_benchmark_example_sql():
    leaky = {
        "patch_type": "add_example_sql",
        "example_question": _BENCH_QUESTION,
        "example_sql": "SELECT 1",  # SQL differs; canonical-question match blocks
    }
    valid = _lever5([leaky], benchmarks=_SCORED_BENCHMARKS)
    assert valid == [], "verbatim scored-benchmark question must be dropped on the Lever-5 path"


def test_lever5_without_corpus_keeps_same_proposal():
    """Control — the SAME proposal is kept when no benchmark corpus is
    supplied, proving the rejection above is the benchmark firewall (not some
    other Lever-5 validation rejecting it for an unrelated reason)."""
    leaky = {
        "patch_type": "add_example_sql",
        "example_question": _BENCH_QUESTION,
        "example_sql": "SELECT 1",
    }
    valid = _lever5([leaky], benchmarks=None)
    assert len(valid) == 1


def test_lever5_drops_by_sql_hash_match():
    # No question echo at all — only the normalized-SQL hash matches.
    leaky = {
        "patch_type": "add_example_sql",
        "example_question": "an unrelated dashboard tile prompt phrased differently",
        "example_sql": _BENCH_SQL,
    }
    valid = _lever5([leaky], benchmarks=_SCORED_BENCHMARKS)
    assert valid == []


# ── Synthesis path: synthesis.validate_synthesis_proposal ──────────────


def _run_synthesis(proposal):
    """Run the REAL ``validate_synthesis_proposal`` with the expensive gates
    stubbed to pass, leaving the real ``_gate_firewall`` (which calls
    ``is_benchmark_leak``) as the only meaningful gate."""
    from genie_space_optimizer.optimization import synthesis
    from genie_space_optimizer.optimization.synthesis import (
        GateResult,
        validate_synthesis_proposal,
    )

    def _pass(name):
        return lambda *a, **k: GateResult(True, name)

    with patch.object(synthesis, "_gate_parse", _pass("parse")), \
         patch.object(synthesis, "_gate_identifier_qualification", _pass("identifier")), \
         patch.object(synthesis, "_gate_execute", _pass("execute")), \
         patch.object(synthesis, "_gate_structural", _pass("structural")), \
         patch.object(synthesis, "_gate_arbiter", _pass("arbiter")):
        return validate_synthesis_proposal(
            proposal,
            archetype=None,
            benchmark_corpus=_corpus(),
        )


def test_synthesis_firewall_blocks_verbatim_scored_benchmark():
    leaky = {
        "patch_type": "add_example_sql",
        "example_question": _BENCH_QUESTION,
        "example_sql": _BENCH_SQL,
    }
    all_passed, results = _run_synthesis(leaky)
    assert all_passed is False
    # The firewall is the last gate; it must be the one that failed.
    assert results[-1].gate == "firewall"
    assert results[-1].passed is False


def test_synthesis_firewall_blocks_sql_hash_only_match():
    leaky = {
        "patch_type": "add_example_sql",
        "example_question": "a paraphrased prompt that does not echo the benchmark",
        "example_sql": _BENCH_SQL,
    }
    all_passed, results = _run_synthesis(leaky)
    assert all_passed is False
    assert results[-1].gate == "firewall"
    assert results[-1].passed is False


def test_synthesis_allows_novel_example():
    novel = {
        "patch_type": "add_example_sql",
        "example_question": _NOVEL_QUESTION,
        "example_sql": _NOVEL_SQL,
    }
    all_passed, results = _run_synthesis(novel)
    assert all_passed is True
    assert results[-1].gate == "firewall"
    assert results[-1].passed is True


# ── Normal apply path: applier.apply_patch_set last-mile guard ─────────


def _add_example_sql_patch(question: str, sql: str) -> dict:
    return {
        "type": "add_example_sql",
        "example_question": question,
        "example_sql": sql,
        "lever": 5,
    }


def test_applier_drops_verbatim_scored_benchmark_example_sql():
    from genie_space_optimizer.optimization.applier import apply_patch_set

    leaky = _add_example_sql_patch(_BENCH_QUESTION, _BENCH_SQL)
    apply_log = apply_patch_set(
        None, "space-1", [leaky], {"instructions": {}},
        apply_mode="genie_config", benchmark_corpus=_corpus(),
    )

    dropped = apply_log.get("dropped_patches", [])
    assert any(
        str(d.get("drop_reason", "")).startswith("benchmark_leak:")
        for d in dropped
    ), "leaky example-SQL patch must be dropped by the last-mile applier guard"
    # And it must not have been applied.
    applied_questions = [
        e.get("patch", {}).get("example_question") for e in apply_log.get("applied", [])
    ]
    assert _BENCH_QUESTION not in applied_questions


def test_applier_drops_by_sql_hash_match():
    from genie_space_optimizer.optimization.applier import apply_patch_set

    leaky = _add_example_sql_patch(
        "a differently worded analyst prompt", _BENCH_SQL,
    )
    apply_log = apply_patch_set(
        None, "space-1", [leaky], {"instructions": {}},
        apply_mode="genie_config", benchmark_corpus=_corpus(),
    )
    dropped = apply_log.get("dropped_patches", [])
    assert any(
        d.get("drop_reason") == "benchmark_leak:scored_benchmark_sql_hash"
        for d in dropped
    )


def test_applier_keeps_novel_example_sql():
    from genie_space_optimizer.optimization.applier import apply_patch_set

    novel = _add_example_sql_patch(_NOVEL_QUESTION, _NOVEL_SQL)
    apply_log = apply_patch_set(
        None, "space-1", [novel], {"instructions": {}},
        apply_mode="genie_config", benchmark_corpus=_corpus(),
    )
    # The novel patch is NOT dropped by the benchmark-leak guard.
    leak_dropped = [
        d for d in apply_log.get("dropped_patches", [])
        if str(d.get("drop_reason", "")).startswith("benchmark_leak:")
    ]
    assert leak_dropped == []


def test_applier_no_corpus_does_not_invoke_guard():
    """Backward compatibility — with no corpus the last-mile guard is a no-op
    (the leaky patch is not dropped for a benchmark-leak reason)."""
    from genie_space_optimizer.optimization.applier import apply_patch_set

    leaky = _add_example_sql_patch(_BENCH_QUESTION, _BENCH_SQL)
    apply_log = apply_patch_set(
        None, "space-1", [leaky], {"instructions": {}},
        apply_mode="genie_config",
    )
    leak_dropped = [
        d for d in apply_log.get("dropped_patches", [])
        if str(d.get("drop_reason", "")).startswith("benchmark_leak:")
    ]
    assert leak_dropped == []
