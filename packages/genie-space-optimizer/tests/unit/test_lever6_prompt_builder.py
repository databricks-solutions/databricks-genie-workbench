"""Phase 3.7 Task 5 — independent Lever 6 prompt-builder tests.

These tests exercise the production ``_generate_lever6_proposal``
prompt-construction path with a stubbed ``_traced_llm_call``. Their
purpose is to provide a finer-grained test surface around the lever6
prompt-build code — so a regression in Phase 0+1+2 prompt code is
caught HERE (cheap, deterministic) rather than only at the anchor
tape-replay level (expensive, end-to-end).

If any test fails, STOP and report — that is a real prompt-build
behaviour change, not a Phase 3.7 todo.
"""
from __future__ import annotations

import json
from unittest.mock import patch

from genie_space_optimizer.optimization.optimizer import (
    _generate_lever6_proposal,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _enriched_cluster(cluster_id: str = "cl_test_42") -> dict:
    """Synthetic production-shape cluster carrying all the enrichment
    fields that the historic export drops."""
    return {
        "cluster_id": cluster_id,
        "root_cause": "missing_filter",
        "asi_failure_type": "missing_filter",
        "affected_judge": "judge_correctness",
        "question_ids": ["gs_001", "gs_002"],
        "asi_blame_set": ["catalog.schema.orders", "catalog.schema.customers"],
        "failure_features": {"hint_a": 1, "hint_b": 2},
        "counterfactual_fixes": [{"kind": "add_filter", "expr": "active=true"}],
        "structural_diff": {"missing_cols": ["active"]},
        "question_traces": [
            {"qid": "gs_001", "sql_failed": "SELECT 1", "judge_rationale": "wrong join"},
            {"qid": "gs_002", "sql_failed": "SELECT 2", "judge_rationale": "missing filter"},
        ],
    }


def _metadata_snapshot() -> dict:
    return {
        "tables": [
            {
                "name": "catalog.schema.orders",
                "columns": [
                    {"name": "order_id", "type": "string"},
                    {"name": "active", "type": "boolean"},
                ],
            },
        ],
        "sql_snippets": {"measures": [], "filters": [], "expressions": []},
    }


def _good_llm_response() -> str:
    """Stubbed LLM response that passes downstream validation in the
    no-backend (spark=None, warehouse_id='') path."""
    return json.dumps({
        "snippet_type": "filter",
        "display_name": "active_only",
        "sql": "active = true",
        "target_table": "catalog.schema.orders",
        "affected_questions": ["gs_001"],
        "rationale": "Restrict to active rows.",
    })


# ── Tests ─────────────────────────────────────────────────────────────


def test_lever6_prompt_includes_cluster_id_and_blame_set():
    """Prompt-build copies cluster_id + blame_set into the AFS block."""
    captured: dict = {}

    def _stub(w, system_msg, prompt, *, span_name, **kwargs):
        captured["prompt"] = prompt
        captured["span_name"] = span_name
        return _good_llm_response(), None

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_stub,
    ):
        result = _generate_lever6_proposal(
            _enriched_cluster("cl_blame_test"),
            _metadata_snapshot(),
        )

    assert captured["span_name"] == "lever6_llm"
    assert '"cluster_id": "cl_blame_test"' in captured["prompt"]
    # blame_set surfaces in the AFS-projected cluster context
    assert "catalog.schema.orders" in captured["prompt"]
    assert result is not None
    assert result.get("snippet_type") == "filter"


def test_lever6_prompt_includes_raw_evidence_when_provided():
    """When raw_evidence is supplied, its block renders into the prompt
    (this is the field the historic export drops — its absence is what
    drives the SHA mismatch documented in stage-prompt-fidelity-audit.md)."""
    captured: dict = {}

    def _stub(w, system_msg, prompt, **kwargs):
        captured["prompt"] = prompt
        return _good_llm_response(), None

    sentinel = "ZZUNIQUE_RAW_EVIDENCE_TOKEN_42_QQ"
    raw_evidence = (
        {
            "question_id": "gs_001",
            "question": "what?",
            "actual_sql": f"SELECT {sentinel}",
            "expected_sql": "SELECT 1",
            "judge_rationale": "x",
        },
    )

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_stub,
    ):
        _generate_lever6_proposal(
            _enriched_cluster(),
            _metadata_snapshot(),
            raw_evidence=raw_evidence,
        )

    assert sentinel in captured["prompt"]
    # And empty raw_evidence should NOT carry that sentinel:
    captured.clear()
    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_stub,
    ):
        _generate_lever6_proposal(
            _enriched_cluster(),
            _metadata_snapshot(),
            raw_evidence=(),
        )
    assert sentinel not in captured["prompt"]


def test_lever6_rejects_affected_questions_outside_cluster_qids():
    """Reproduces the G2-2026-05-17 affected_questions guard in
    optimizer.py:14027 — proposals whose affected_questions includes IDs
    not in cluster.question_ids return None."""
    bad_response = json.dumps({
        "snippet_type": "filter",
        "display_name": "x",
        "sql": "x = 1",
        "target_table": "catalog.schema.orders",
        "affected_questions": ["gs_999"],  # not in cluster
        "rationale": "x",
    })

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        return_value=(bad_response, None),
    ):
        result = _generate_lever6_proposal(
            _enriched_cluster(),
            _metadata_snapshot(),
        )
    assert result is None


def test_lever6_returns_none_on_empty_sql():
    bad_response = json.dumps({
        "snippet_type": "filter",
        "display_name": "x",
        "sql": "",
        "target_table": "catalog.schema.orders",
        "affected_questions": ["gs_001"],
        "rationale": "x",
    })
    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        return_value=(bad_response, None),
    ):
        result = _generate_lever6_proposal(
            _enriched_cluster(),
            _metadata_snapshot(),
        )
    assert result is None


def test_lever6_returns_none_on_invalid_snippet_type():
    bad_response = json.dumps({
        "snippet_type": "nonsense",  # not in {measure, filter, expression}
        "display_name": "x",
        "sql": "x = 1",
        "target_table": "catalog.schema.orders",
        "affected_questions": ["gs_001"],
        "rationale": "x",
    })
    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        return_value=(bad_response, None),
    ):
        result = _generate_lever6_proposal(
            _enriched_cluster(),
            _metadata_snapshot(),
        )
    assert result is None
