"""Phase 3.7 Task 2 — lever6 binding reconciliation in the trace extractor.

Five focused tests covering the new helpers + the extractor's backfill
behaviour for pre-Phase-3.6-Task-2 historic captures.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from genie_space_optimizer.optimization import mlflow_trace_extractor as mte


# ── Fixtures (kept inline; no conftest needed) ────────────────────────


def _afs_prompt(cluster_id: str, extra_text: str = "") -> str:
    """Synthesize a lever6-style prompt with cluster_id in the AFS block.

    Matches the production shape: ``json.dumps(format_afs(...), indent=2)``
    where cluster_id is the first key.
    """
    afs = {
        "cluster_id": cluster_id,
        "failure_type": "unknown",
        "affected_judge": "unknown",
        "question_count": 0,
        "question_ids": [],
    }
    body = json.dumps(afs, indent=2)
    return f"You are a SQL expert.\n\nCluster context:\n{body}\n\n{extra_text}"


def _make_chat_span(prompt: str, *, parent_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        name="OpenAI.chat.completions.create",
        span_id=f"{parent_id}_child",
        parent_id=parent_id,
        span_type="CHAT_MODEL",
        inputs={
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": prompt},
            ],
            "model": "test-model",
            "temperature": 0.0,
        },
        outputs={
            "choices": [{"message": {"content": "{}"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        },
    )


def _make_parent_span(name: str, *, span_id: str, breadcrumbs: dict | None = None):
    inputs: dict[str, Any] = {"prompt_chars": 100}
    if breadcrumbs:
        inputs.update(breadcrumbs)
    return SimpleNamespace(
        name=name,
        span_id=span_id,
        parent_id=None,
        span_type="CHAIN",
        inputs=inputs,
        outputs=None,
    )


def _make_trace(spans: list) -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(spans=spans))


def _export_with(cluster_id: str, iteration: int, ag_id: str) -> dict:
    """Build a minimal export payload binding cluster→(iteration, ag)."""
    return {
        "iterations": [
            {
                "iteration": iteration,
                "iter_source_clusters_by_id": {cluster_id: {"cluster_id": cluster_id}},
                "strategist_response": {
                    "action_groups": [
                        {"id": ag_id, "source_cluster_ids": [cluster_id]},
                        {"id": "AG_OTHER", "source_cluster_ids": ["other"]},
                    ],
                },
            },
        ],
    }


# ── Tests ─────────────────────────────────────────────────────────────


def test_extract_cluster_id_finds_first_afs_block():
    cid = "cluster_abc123"
    prompt = _afs_prompt(cid, extra_text='strategist hints: "cluster_id": "decoy"')
    parsed = mte._extract_cluster_id_from_lever6_prompt(prompt)
    assert parsed == cid


def test_extract_cluster_id_returns_none_when_missing():
    assert mte._extract_cluster_id_from_lever6_prompt("nothing here") is None
    assert mte._extract_cluster_id_from_lever6_prompt("") is None


def test_reconcile_binding_pairs_iter_and_ag():
    export = _export_with("cl_42", iteration=3, ag_id="AG_lever6")
    assert mte.reconcile_lever6_binding_from_export(
        cluster_id="cl_42", export_payload=export
    ) == (3, "AG_lever6")


def test_reconcile_binding_returns_minus1_when_cluster_unknown():
    export = _export_with("cl_42", iteration=3, ag_id="AG_lever6")
    assert mte.reconcile_lever6_binding_from_export(
        cluster_id="not_in_export", export_payload=export
    ) == (-1, "")


def test_extractor_backfills_lever6_binding_from_export():
    cid = "cluster_xyz"
    prompt = _afs_prompt(cid)
    parent = _make_parent_span(
        "lever6_llm",
        span_id="parent_1",
        # historic capture — breadcrumbs default to (-1, "", "")
        breadcrumbs={"iteration": -1, "ag_id": "", "cluster_id": ""},
    )
    child = _make_chat_span(prompt, parent_id="parent_1")
    trace = _make_trace([parent, child])
    export = _export_with(cid, iteration=2, ag_id="AG_xyz")

    calls = list(
        mte.extract_llm_calls_from_trace(trace, export_payload=export)
    )
    assert len(calls) == 1
    c = calls[0]
    assert c["span_name"] == "lever6_llm"
    assert c["iteration"] == 2
    assert c["ag_id"] == "AG_xyz"
    assert c["cluster_id"] == cid

    # When export is omitted, the historic binding is preserved.
    calls_no_export = list(mte.extract_llm_calls_from_trace(trace))
    assert calls_no_export[0]["iteration"] == -1
    assert calls_no_export[0]["ag_id"] == ""
    assert calls_no_export[0]["cluster_id"] == ""
