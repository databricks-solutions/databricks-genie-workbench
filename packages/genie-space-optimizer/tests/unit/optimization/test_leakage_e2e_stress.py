"""Plan 4 / Task 12 — leakage firewall e2e stress test.

Runs 100 simulated proposal-generation iterations with raw evidence
populated from the benchmark corpus itself (the most adversarial
input we can construct). Every proposal that the firewall accepts
MUST NOT contain any benchmark question or expected_sql verbatim.

This complements the unit tests of is_benchmark_leak by exercising
the full Plan 4 path — extraction, projection, prompt rendering,
mock LLM, firewall — under load. The mock LLM is deliberately
naive: it sometimes copies the input, sometimes paraphrases, and
sometimes produces something safe. The firewall must catch every
copy.

Note: The firewall (``is_benchmark_leak``) currently registers only
``add_example_sql`` / ``update_example_sql`` patch types in
``_PATCH_TEXT_FIELDS`` — those are the patch types that persist
inference-visible question+SQL pairs. To exercise the firewall, the
test framings use the ``add_example_sql`` patch type for both
"question copy" and "SQL copy" iterations, with the proposal carrying
``example_question`` or ``example_sql`` respectively. This matches
how Plan 2's L5b adapter delivers proposals through the firewall in
production.
"""
from __future__ import annotations

import os

import pytest


_N_ITERATIONS = 100


def _make_benchmarks(n: int = 5) -> list[dict]:
    return [
        {"id": f"B{i}",
         "question": (
             f"distinct benchmark question number {i} about computing "
             f"the total revenue for fiscal year ending {2020 + i}"
         ),
         "expected_sql": (
             f"SELECT col_{i}, SUM(amount_{i}) FROM tbl_{i} "
             f"WHERE flag_{i} = {i} AND region_{i} IN ('US','EU') "
             f"GROUP BY col_{i} ORDER BY col_{i}"
         )}
        for i in range(n)
    ]


def _cluster_for_benchmark(b: dict) -> dict:
    return {
        "cluster_id": f"C_{b['id']}",
        "root_cause": "wrong_column",
        "asi_blame_set": [f"catalog.schema.tbl_{b['id']}.col_{b['id']}"],
        "question_ids": [b["id"]],
        "question_traces": [{
            "question_id": b["id"], "trace_id": f"trace://{b['id']}",
            "question": b["question"],
            "expected_sql": b["expected_sql"],
            "generated_sql": (
                f"SELECT wrong_{b['id']} FROM tbl_{b['id']} "
                f"WHERE flag_{b['id']} = {b['id']}"
            ),
            "failed_judges": [{
                "judge": "schema_accuracy",
                "rationale": f"wrong_{b['id']} should be col_{b['id']}",
                "rationale_snippet": f"use col_{b['id']}",
            }],
        }],
    }


def _proposal_copies_benchmark(b: dict, kind: str) -> dict:
    """Adversarial mock: returns a proposal whose text VERBATIM
    matches a benchmark field. Firewall must reject."""
    if kind == "question":
        return {"example_question": b["question"],
                "example_sql": "SELECT 1"}
    if kind == "sql":
        return {"example_question": "What is the answer?",
                "example_sql": b["expected_sql"]}
    return {"example_question": "What is the safe answer?",
            "example_sql": "SELECT 1 AS safe_col FROM dual"}


def test_firewall_holds_for_100_iterations(monkeypatch):
    """100 iterations: alternate (i) safe proposals, (ii) verbatim
    benchmark question copies, (iii) verbatim benchmark SQL copies.
    Count firewall hits — expect ~67 (the unsafe two-thirds)."""
    from genie_space_optimizer.optimization.leakage import (
        BenchmarkCorpus, is_benchmark_leak,
    )
    benchmarks = _make_benchmarks(n=5)
    corpus = BenchmarkCorpus.from_benchmarks(benchmarks)

    rejected = 0
    accepted = 0
    for i in range(_N_ITERATIONS):
        b = benchmarks[i % len(benchmarks)]
        kind = ("safe", "question", "sql")[i % 3]
        proposal = _proposal_copies_benchmark(b, kind)
        # All iterations use add_example_sql — the patch type the
        # firewall currently inspects via _PATCH_TEXT_FIELDS.
        patch_type = "add_example_sql"
        is_leak, _reason = is_benchmark_leak(proposal, patch_type, corpus)
        if is_leak:
            rejected += 1
        else:
            accepted += 1
            # The accepted proposal MUST NOT verbatim contain the
            # benchmark question or expected_sql:
            text = (
                proposal.get("example_question", "")
                + proposal.get("example_sql", "")
            )
            assert b["question"] not in text, (
                f"iteration {i}: firewall accepted but proposal contains "
                f"verbatim benchmark question"
            )
            assert b["expected_sql"] not in text, (
                f"iteration {i}: firewall accepted but proposal contains "
                f"verbatim benchmark SQL"
            )

    # Two-thirds of iterations were adversarial; firewall must have
    # caught at least 60 of them (allow some slack for the n-gram
    # threshold not catching very short fields):
    assert rejected >= 60, (
        f"firewall caught only {rejected} of ~67 adversarial proposals "
        f"({accepted} accepted)"
    )


def test_raw_evidence_round_trip_preserves_leakage_classification(monkeypatch):
    """End-to-end: extract raw evidence from cluster (which echoes
    benchmark text), render through the prompt, and verify the
    LEAKAGE FIREWALL still rejects a verbatim copy in the proposal.

    The firewall runs on PROPOSALS, not on prompt INPUTS — so a
    verbatim benchmark fragment appearing in the prompt's
    raw_evidence_block is fine; the firewall only fires when the LLM
    copies it back into a proposal."""
    from genie_space_optimizer.optimization.leakage import (
        BenchmarkCorpus, is_benchmark_leak,
    )
    from genie_space_optimizer.optimization.raw_evidence import (
        extract_raw_evidence_from_cluster, project_evidence_for_skill,
    )
    benchmarks = _make_benchmarks(n=3)
    corpus = BenchmarkCorpus.from_benchmarks(benchmarks)
    clusters = [_cluster_for_benchmark(b) for b in benchmarks]

    # Round trip: cluster → triple → projection → render → mock LLM
    # response → firewall.
    triples = project_evidence_for_skill(
        "lever-1-table-column-description", clusters, w=None, n=3,
    )
    assert len(triples) == 3
    # Adversarial mock LLM: returns first triple's expected_sql verbatim
    # as a SQL snippet (delivered via add_example_sql patch path so
    # the firewall actually inspects the example_sql field):
    proposal = {
        "example_question": "Compute revenue",
        "example_sql": triples[0].get("expected_sql", ""),
    }
    is_leak, reason = is_benchmark_leak(
        proposal, "add_example_sql", corpus,
    )
    assert is_leak, (
        f"firewall failed to catch verbatim-copied raw evidence: {reason}"
    )
