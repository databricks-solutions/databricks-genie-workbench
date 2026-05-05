"""F-1 — Cycle 5 emit-site idempotency.

Today the iteration_budget_decision, soft_cluster_drift_recovered,
rca_regeneration_triggered, and rca_regeneration_exhausted records
are emitted twice per iteration (once at AG-materialization, once
at iteration-end consolidation). Run 833969815458299 evidences this:
two duplicate iteration_budget_consumed records, two duplicate
soft_cluster_drift_recovered records for S001, etc.
"""
from __future__ import annotations


def test_emit_idempotency_key_distinguishes_distinct_records() -> None:
    """Two records with the same (decision_type, reason_code,
    cluster_id, iteration) collapse to a single emit-key."""
    from genie_space_optimizer.optimization.harness import (
        _emit_idempotency_key,
    )
    rec_a = {
        "decision_type": "cluster_selected",
        "reason_code": "soft_cluster_drift_recovered",
        "cluster_id": "S001",
        "iteration": 1,
    }
    rec_b = {
        "decision_type": "cluster_selected",
        "reason_code": "soft_cluster_drift_recovered",
        "cluster_id": "S001",
        "iteration": 1,
    }
    assert _emit_idempotency_key(rec_a) == _emit_idempotency_key(rec_b)


def test_emit_idempotency_key_distinguishes_different_clusters() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_idempotency_key,
    )
    rec_a = {
        "decision_type": "cluster_selected",
        "reason_code": "soft_cluster_drift_recovered",
        "cluster_id": "S001",
        "iteration": 1,
    }
    rec_c = {
        "decision_type": "cluster_selected",
        "reason_code": "soft_cluster_drift_recovered",
        "cluster_id": "S002",
        "iteration": 1,
    }
    assert _emit_idempotency_key(rec_a) != _emit_idempotency_key(rec_c)


def test_emit_idempotency_key_distinguishes_different_iterations() -> None:
    """Same record on a different iteration must be allowed through."""
    from genie_space_optimizer.optimization.harness import (
        _emit_idempotency_key,
    )
    rec_a = {
        "decision_type": "iteration_budget_decision",
        "reason_code": "iteration_budget_consumed",
        "cluster_id": "",
        "iteration": 1,
    }
    rec_b = {
        "decision_type": "iteration_budget_decision",
        "reason_code": "iteration_budget_consumed",
        "cluster_id": "",
        "iteration": 2,
    }
    assert _emit_idempotency_key(rec_a) != _emit_idempotency_key(rec_b)


def test_emit_idempotency_key_does_not_dedupe_proposal_generated() -> None:
    """proposal_generated records intentionally repeat per proposal;
    they must not collapse on the (decision_type, reason_code) tuple
    alone — proposal_id is part of the key."""
    from genie_space_optimizer.optimization.harness import (
        _emit_idempotency_key,
    )
    rec_a = {
        "decision_type": "proposal_generated",
        "reason_code": "proposal_emitted",
        "cluster_id": "AG1",
        "iteration": 1,
        "proposal_id": "L1:P001#1",
    }
    rec_b = {
        "decision_type": "proposal_generated",
        "reason_code": "proposal_emitted",
        "cluster_id": "AG1",
        "iteration": 1,
        "proposal_id": "L1:P001#2",
    }
    assert _emit_idempotency_key(rec_a) != _emit_idempotency_key(rec_b)
