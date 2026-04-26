"""Tests for Task 3: lever-loop decision audit table.

The decoded retail run required scrolling stdout to reconstruct why
AG2 was accepted. After Task 3 every gate decision lands in
``genie_eval_lever_loop_decisions`` so a single SQL query reconstructs
``cluster -> proposal -> patch -> applied -> accepted/rolled_back`` for
any AG.

These tests exercise:
* JSON serialization of plain Python list/dict fields by the state
  writer (callers shouldn't have to call ``json.dumps`` themselves).
* The DDL is registered in ``_ALL_DDL`` so ``ensure_optimization_tables``
  creates it idempotently.
* Empty-list input is a no-op.
* Rows missing required identity fields (run_id / gate_name / decision)
  are skipped defensively rather than written as orphans.
* The attribution invariant: an ``apply_patch`` row carries a
  ``proposal_to_patch_map_json`` value linkable back to the proposals
  the strategist produced.

The Delta layer itself is mocked — we capture rows as they would be
written so the contract is testable without a Spark cluster.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from genie_space_optimizer.optimization.ddl import (
    TABLE_LEVER_LOOP_DECISIONS,
    _ALL_DDL,
)
from genie_space_optimizer.optimization import state as state_module
from genie_space_optimizer.optimization.state import write_lever_loop_decisions


# ── fake spark + patch insert_row ────────────────────────────────────


class _Recorder:
    """Captures rows that the writer would have inserted into Delta."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def insert(self, _spark, _catalog, _schema, table, row):
        self.calls.append({"table": table, **row})


@pytest.fixture
def recorder(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(state_module, "insert_row", rec.insert)
    return rec


# ── DDL registration ─────────────────────────────────────────────────


def test_ddl_registered_in_all_ddl():
    assert TABLE_LEVER_LOOP_DECISIONS == "genie_eval_lever_loop_decisions"
    assert TABLE_LEVER_LOOP_DECISIONS in _ALL_DDL
    ddl = _ALL_DDL[TABLE_LEVER_LOOP_DECISIONS]
    # Carrier columns for attribution
    assert "source_cluster_ids_json" in ddl
    assert "proposal_ids_json" in ddl
    assert "proposal_to_patch_map_json" in ddl
    assert "metrics_json" in ddl
    assert "stage_letter" in ddl


# ── JSON serialization ───────────────────────────────────────────────


def test_writer_serializes_python_lists_and_dicts(recorder):
    write_lever_loop_decisions(
        None,
        [
            {
                "run_id": "run-1",
                "iteration": 2,
                "ag_id": "AG2",
                "decision_order": 1,
                "stage_letter": "N",
                "gate_name": "full_eval_acceptance",
                "decision": "rolled_back",
                "reason_code": "post_arbiter_guardrail",
                "reason_detail": "AG2 dropped post-arbiter -4.6pp",
                "affected_qids": ["q9", "q19"],
                "source_cluster_ids": ["H001", "H002"],
                "proposal_ids": ["p1", "p2"],
                "proposal_to_patch_map": {"p1": "patch_xyz", "p2": "patch_abc"},
                "metrics": {
                    "primary_delta_pp": 18.0,
                    "secondary_delta_pp": -4.6,
                    "min_run_post_arbiter": 78.3,
                },
            },
        ],
        catalog="cat",
        schema="sch",
    )

    assert len(recorder.calls) == 1
    row = recorder.calls[0]
    assert row["table"] == TABLE_LEVER_LOOP_DECISIONS
    # JSON fields are strings
    assert isinstance(row["affected_qids_json"], str)
    assert isinstance(row["source_cluster_ids_json"], str)
    assert isinstance(row["proposal_ids_json"], str)
    assert isinstance(row["proposal_to_patch_map_json"], str)
    assert isinstance(row["metrics_json"], str)
    # Round-trip preserves the structure
    assert json.loads(row["affected_qids_json"]) == ["q9", "q19"]
    assert json.loads(row["source_cluster_ids_json"]) == ["H001", "H002"]
    assert json.loads(row["proposal_to_patch_map_json"]) == {
        "p1": "patch_xyz",
        "p2": "patch_abc",
    }
    metrics = json.loads(row["metrics_json"])
    assert metrics["primary_delta_pp"] == 18.0
    assert metrics["secondary_delta_pp"] == -4.6


def test_writer_passes_through_already_serialized_strings(recorder):
    write_lever_loop_decisions(
        None,
        [
            {
                "run_id": "run-1",
                "iteration": 1,
                "decision_order": 1,
                "gate_name": "asi_extraction",
                "decision": "ok",
                "metrics_json": '{"trace": 17, "none": 0}',
            },
        ],
        catalog="cat",
        schema="sch",
    )

    row = recorder.calls[0]
    assert row["metrics_json"] == '{"trace": 17, "none": 0}'


# ── Defensive paths ────────────────────────────────────────────────


def test_empty_rows_is_no_op(recorder):
    write_lever_loop_decisions(None, [], catalog="cat", schema="sch")

    assert recorder.calls == []


def test_skips_rows_missing_required_identity_fields(recorder):
    write_lever_loop_decisions(
        None,
        [
            # Missing gate_name → skip
            {"run_id": "r", "decision": "ok"},
            # Missing decision → skip
            {"run_id": "r", "gate_name": "x"},
            # Missing run_id → skip
            {"gate_name": "x", "decision": "ok"},
            # Valid → kept
            {"run_id": "r", "iteration": 0, "decision_order": 1, "gate_name": "x", "decision": "ok"},
        ],
        catalog="cat",
        schema="sch",
    )

    assert len(recorder.calls) == 1


def test_truncates_oversized_reason_detail(recorder):
    big = "x" * 5000
    write_lever_loop_decisions(
        None,
        [
            {
                "run_id": "r",
                "iteration": 0,
                "decision_order": 1,
                "gate_name": "x",
                "decision": "ok",
                "reason_detail": big,
            },
        ],
        catalog="cat",
        schema="sch",
    )

    assert len(recorder.calls[0]["reason_detail"]) == 2000


# ── Attribution invariant ──────────────────────────────────────────


def test_attribution_chain_links_acceptance_to_apply_patch(recorder):
    """The audit-table contract: an ``apply_patch`` row in the same
    iteration as a ``full_eval_acceptance`` row must carry a non-empty
    ``proposal_to_patch_map_json`` whose patch ids can be looked up
    against any earlier ``clustering`` row's ``source_cluster_ids``."""
    write_lever_loop_decisions(
        None,
        [
            {
                "run_id": "r-1",
                "iteration": 2,
                "ag_id": "AG2",
                "decision_order": 1,
                "stage_letter": "E",
                "gate_name": "clustering",
                "decision": "ok",
                "source_cluster_ids": ["H001"],
                "metrics": {"hard_clusters": 1, "soft_clusters": 0},
            },
            {
                "run_id": "r-1",
                "iteration": 2,
                "ag_id": "AG2",
                "decision_order": 5,
                "stage_letter": "G",
                "gate_name": "proposal_generation",
                "decision": "ok",
                "source_cluster_ids": ["H001"],
                "proposal_ids": ["p1"],
            },
            {
                "run_id": "r-1",
                "iteration": 2,
                "ag_id": "AG2",
                "decision_order": 8,
                "stage_letter": "J",
                "gate_name": "apply_patch",
                "decision": "ok",
                "source_cluster_ids": ["H001"],
                "proposal_ids": ["p1"],
                "proposal_to_patch_map": {"p1": "applied_xyz"},
            },
            {
                "run_id": "r-1",
                "iteration": 2,
                "ag_id": "AG2",
                "decision_order": 12,
                "stage_letter": "N",
                "gate_name": "full_eval_acceptance",
                "decision": "rolled_back",
                "reason_code": "post_arbiter_guardrail",
                "source_cluster_ids": ["H001"],
                "proposal_to_patch_map": {"p1": "applied_xyz"},
            },
        ],
        catalog="cat",
        schema="sch",
    )

    # All four rows persisted
    assert len(recorder.calls) == 4
    by_gate = {row["gate_name"]: row for row in recorder.calls}
    # Apply row carries the cluster->proposal->patch chain
    apply_row = by_gate["apply_patch"]
    assert json.loads(apply_row["proposal_to_patch_map_json"]) == {
        "p1": "applied_xyz",
    }
    # Acceptance row references the same patch via proposal_to_patch_map
    accept_row = by_gate["full_eval_acceptance"]
    assert json.loads(accept_row["proposal_to_patch_map_json"]) == {
        "p1": "applied_xyz",
    }
    # Cluster id appears in clustering, proposal, apply, and acceptance
    for gate in ("clustering", "proposal_generation", "apply_patch", "full_eval_acceptance"):
        assert "H001" in json.loads(by_gate[gate]["source_cluster_ids_json"])


# ── Stage letter mapping ──────────────────────────────────────────


def test_stage_letter_is_carried_through_to_audit_row(recorder):
    """Plan §Stage-to-Decision-Row mapping requires every audit row to
    carry the stage_letter from the Pipeline Hand-off Contract."""
    write_lever_loop_decisions(
        None,
        [
            {
                "run_id": "r",
                "iteration": 1,
                "decision_order": 1,
                "stage_letter": "C",
                "gate_name": "asi_extraction",
                "decision": "degraded",
                "reason_code": "asi_source_no_traces",
            },
        ],
        catalog="cat",
        schema="sch",
    )

    assert recorder.calls[0]["stage_letter"] == "C"
