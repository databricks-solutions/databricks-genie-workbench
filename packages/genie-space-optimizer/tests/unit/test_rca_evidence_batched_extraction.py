"""Unit tests for Phase 1 P1.2 batched RCA evidence extraction.

Covers:
  * ``_render_batched_user_prompt`` — produces JSON with one entry
    per QID echoing the batch_directive.
  * ``_build_request_for_batch`` — wires the batched system addendum
    and the batched output schema.
  * ``extract_evidence_for_qid_batch`` — translates a batched LLM
    response into per-QID typed evidence, drops bad entries, returns
    {} on decline/error without raising.
  * ``extract_evidence_for_all_qids`` — chunks QIDs, falls back to
    per-QID extraction for the missing ones, and bypasses batching
    when QID count is below the floor.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from genie_space_optimizer.optimization import rca_evidence_extractor as rex
from genie_space_optimizer.optimization.llm_abstain import (
    AbstainReason,
    AbstainVerdict,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.skills.rca_evidence_extraction.output_schema import (
    BatchedPerQidRcaEvidenceOutput,
)


def _ok_entry(qid: str) -> dict:
    return {
        "qid": qid,
        "observed_failure": f"qid {qid} returned wrong rows",
        "generated_sql_issue": "missing GROUP BY",
        "expected_sql_shape": "SELECT ... GROUP BY x",
        "blame_set": ["t.x"],
        "suggested_repair_family": "ambiguity",
        "repair_hint_patch_type": "add_instruction",
        "confidence": 0.7,
        "quoted_evidence": ["judge: missing GROUP BY"],
    }


def _ok_response(qids: tuple[str, ...]) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="rca_evidence.batched.iter_1.n3",
        skill_id="rca-evidence-extraction",
        succeeded=True,
        parsed_output={"evidences": [_ok_entry(q) for q in qids]},
        declined=None,
        raw_text="",
        tokens_input=100,
        tokens_output=200,
        duration_ms=10,
        error=None,
    )


def test_render_batched_user_prompt_contains_one_entry_per_qid() -> None:
    qids = ("q1", "q2", "q3")
    payload = rex._render_batched_user_prompt(
        qids=qids,
        judge_by_qid={q: {"verdict": "wrong_join"} for q in qids},
        asi_by_qid={q: {"sql_diff": f"diff-{q}"} for q in qids},
        sql_by_qid={q: f"SELECT {q}" for q in qids},
    )
    decoded = json.loads(payload)
    assert "batch_directive" in decoded
    assert "Batched RCA" in decoded["batch_directive"]
    assert [e["qid"] for e in decoded["qids"]] == ["q1", "q2", "q3"]
    assert decoded["qids"][0]["judge_verdict"] == "wrong_join"
    assert decoded["qids"][0]["sql_diff"] == "diff-q1"
    assert decoded["qids"][0]["generated_sql"] == "SELECT q1"


def test_render_batched_user_prompt_drops_empty_qids() -> None:
    payload = rex._render_batched_user_prompt(
        qids=("", "q2"),
        judge_by_qid={"q2": {}},
        asi_by_qid={"q2": {}},
        sql_by_qid={"q2": ""},
    )
    decoded = json.loads(payload)
    assert [e["qid"] for e in decoded["qids"]] == ["q2"]


def test_build_request_for_batch_uses_batched_schema_and_addendum() -> None:
    req = rex._build_request_for_batch(
        qids=("q1", "q2", "q3"),
        judge_by_qid={"q1": {}, "q2": {}, "q3": {}},
        asi_by_qid={"q1": {}, "q2": {}, "q3": {}},
        sql_by_qid={"q1": "", "q2": "", "q3": ""},
        iteration=4,
    )
    assert req.result_cls is BatchedPerQidRcaEvidenceOutput
    assert req.skill_id == "rca-evidence-extraction"
    assert "batched_mode_addendum" in req.system_msg
    assert req.call_id == "rca_evidence.batched.iter_4.n3"
    # max_tokens scales with batch size but is capped at the upper bound.
    assert 0 < req.max_tokens <= 8000


def test_extract_evidence_for_qid_batch_translates_each_entry(
    monkeypatch,
) -> None:
    qids = ("q1", "q2", "q3")
    response = _ok_response(qids)

    fake_invoke = MagicMock(return_value=response)
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca_evidence_extractor."
        "LlmReasoningCall.invoke",
        fake_invoke,
    )
    out = rex.extract_evidence_for_qid_batch(
        w=MagicMock(),
        qids=qids,
        judge_by_qid={q: {} for q in qids},
        asi_by_qid={q: {} for q in qids},
        sql_by_qid={q: "" for q in qids},
        iteration=1,
    )
    assert set(out.keys()) == {"q1", "q2", "q3"}
    for q in qids:
        ev = out[q]
        assert isinstance(ev, PerQidRcaEvidence)
        assert ev.qid == q
        assert ev.repair_hint_patch_type == PatchType.ADD_INSTRUCTION


def test_extract_evidence_for_qid_batch_drops_bad_entries(
    monkeypatch,
) -> None:
    qids = ("q1", "q2", "q3")
    bad_entry = {"qid": "q2"}  # missing required fields
    response = LlmReasoningResponse(
        call_id="rca_evidence.batched.iter_1.n3",
        skill_id="rca-evidence-extraction",
        succeeded=True,
        parsed_output={
            "evidences": [_ok_entry("q1"), bad_entry, _ok_entry("q3")],
        },
        declined=None,
        raw_text="",
        tokens_input=100,
        tokens_output=200,
        duration_ms=10,
        error=None,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca_evidence_extractor."
        "LlmReasoningCall.invoke",
        MagicMock(return_value=response),
    )
    out = rex.extract_evidence_for_qid_batch(
        w=MagicMock(),
        qids=qids,
        judge_by_qid={q: {} for q in qids},
        asi_by_qid={q: {} for q in qids},
        sql_by_qid={q: "" for q in qids},
        iteration=1,
    )
    assert set(out.keys()) == {"q1", "q3"}


def test_extract_evidence_for_qid_batch_decline_returns_empty(
    monkeypatch,
) -> None:
    qids = ("q1", "q2", "q3")
    declined = AbstainVerdict(
        reason=AbstainReason.OPTIMIZER_CAPACITY_STARVED,
        explanation="rate limit",
        needed_evidence=(),
        suggested_next_step="retry next iter",
    )
    response = LlmReasoningResponse(
        call_id="rca_evidence.batched.iter_1.n3",
        skill_id="rca-evidence-extraction",
        succeeded=False,
        parsed_output=None,
        declined=declined,
        raw_text="",
        tokens_input=100,
        tokens_output=0,
        duration_ms=10,
        error=None,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca_evidence_extractor."
        "LlmReasoningCall.invoke",
        MagicMock(return_value=response),
    )
    out = rex.extract_evidence_for_qid_batch(
        w=MagicMock(),
        qids=qids,
        judge_by_qid={q: {} for q in qids},
        asi_by_qid={q: {} for q in qids},
        sql_by_qid={q: "" for q in qids},
        iteration=1,
    )
    assert out == {}


def test_extract_evidence_for_qid_batch_error_returns_empty(
    monkeypatch,
) -> None:
    response = LlmReasoningResponse(
        call_id="rca_evidence.batched.iter_1.n3",
        skill_id="rca-evidence-extraction",
        succeeded=False,
        parsed_output=None,
        declined=None,
        raw_text="",
        tokens_input=0,
        tokens_output=0,
        duration_ms=10,
        error="endpoint timeout",
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca_evidence_extractor."
        "LlmReasoningCall.invoke",
        MagicMock(return_value=response),
    )
    out = rex.extract_evidence_for_qid_batch(
        w=MagicMock(),
        qids=("q1", "q2"),
        judge_by_qid={"q1": {}, "q2": {}},
        asi_by_qid={"q1": {}, "q2": {}},
        sql_by_qid={"q1": "", "q2": ""},
        iteration=1,
    )
    assert out == {}


def test_extract_evidence_for_all_qids_below_floor_uses_per_qid(
    monkeypatch,
) -> None:
    """When qid count < BATCH_RCA_MIN_QIDS, the driver bypasses
    batching entirely and dispatches per-QID."""
    sentinel_evidence = PerQidRcaEvidence(
        qid="q1",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=(),
        suggested_repair_family="ambiguity",
        repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
        confidence=0.5,
        quoted_evidence=(),
    )
    batch_calls: list = []
    per_qid_calls: list = []

    def _fake_batch(**kwargs):
        batch_calls.append(kwargs)
        return {}

    def _fake_per_qid(**kwargs):
        per_qid_calls.append(kwargs)
        return sentinel_evidence._replace(qid=kwargs["qid"]) \
            if hasattr(sentinel_evidence, "_replace") else PerQidRcaEvidence(
                qid=kwargs["qid"],
                observed_failure="x",
                generated_sql_issue="x",
                expected_sql_shape="x",
                blame_set=(),
                suggested_repair_family="ambiguity",
                repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
                confidence=0.5,
                quoted_evidence=(),
            )

    monkeypatch.setattr(rex, "extract_evidence_for_qid_batch", _fake_batch)
    monkeypatch.setattr(rex, "extract_evidence_for_qid", _fake_per_qid)

    out = rex.extract_evidence_for_all_qids(
        w=MagicMock(),
        qids=("q1", "q2"),  # 2 < BATCH_RCA_MIN_QIDS=3
        judge_by_qid={"q1": {}, "q2": {}},
        asi_by_qid={"q1": {}, "q2": {}},
        sql_by_qid={"q1": "", "q2": ""},
        iteration=1,
    )
    assert batch_calls == []
    assert [c["qid"] for c in per_qid_calls] == ["q1", "q2"]
    assert set(out.keys()) == {"q1", "q2"}


def test_extract_evidence_for_all_qids_batches_above_floor_with_fallback(
    monkeypatch,
) -> None:
    """When qid count >= floor, the driver dispatches a single
    batched call and falls back to per-QID extraction for any QID
    the batched call didn't return."""
    batch_returns = {
        "q1": PerQidRcaEvidence(
            qid="q1",
            observed_failure="x",
            generated_sql_issue="x",
            expected_sql_shape="x",
            blame_set=(),
            suggested_repair_family="ambiguity",
            repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
            confidence=0.5,
            quoted_evidence=(),
        ),
        # q2 intentionally missing — must fall through to per-QID.
        "q3": PerQidRcaEvidence(
            qid="q3",
            observed_failure="x",
            generated_sql_issue="x",
            expected_sql_shape="x",
            blame_set=(),
            suggested_repair_family="ambiguity",
            repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
            confidence=0.5,
            quoted_evidence=(),
        ),
    }
    batch_calls: list = []
    per_qid_calls: list = []

    def _fake_batch(*, w, qids, **kwargs):
        batch_calls.append(qids)
        return {k: v for k, v in batch_returns.items() if k in qids}

    def _fake_per_qid(*, w, qid, **kwargs):
        per_qid_calls.append(qid)
        return PerQidRcaEvidence(
            qid=qid,
            observed_failure="x",
            generated_sql_issue="x",
            expected_sql_shape="x",
            blame_set=(),
            suggested_repair_family="ambiguity",
            repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
            confidence=0.5,
            quoted_evidence=(),
        )

    monkeypatch.setattr(rex, "extract_evidence_for_qid_batch", _fake_batch)
    monkeypatch.setattr(rex, "extract_evidence_for_qid", _fake_per_qid)

    out = rex.extract_evidence_for_all_qids(
        w=MagicMock(),
        qids=("q1", "q2", "q3"),
        judge_by_qid={"q1": {}, "q2": {}, "q3": {}},
        asi_by_qid={"q1": {}, "q2": {}, "q3": {}},
        sql_by_qid={"q1": "", "q2": "", "q3": ""},
        iteration=1,
    )
    assert batch_calls == [("q1", "q2", "q3")]
    assert per_qid_calls == ["q2"]
    assert set(out.keys()) == {"q1", "q2", "q3"}


def test_extract_evidence_for_all_qids_empty_returns_empty() -> None:
    out = rex.extract_evidence_for_all_qids(
        w=MagicMock(),
        qids=(),
        judge_by_qid={},
        asi_by_qid={},
        sql_by_qid={},
        iteration=1,
    )
    assert out == {}


def test_extract_evidence_for_all_qids_drops_blank_qids(
    monkeypatch,
) -> None:
    """Blank-string QIDs are silently dropped before any LLM call."""
    monkeypatch.setattr(
        rex,
        "extract_evidence_for_qid_batch",
        MagicMock(return_value={}),
    )
    monkeypatch.setattr(
        rex,
        "extract_evidence_for_qid",
        MagicMock(return_value=None),
    )
    out = rex.extract_evidence_for_all_qids(
        w=MagicMock(),
        qids=("", "  ", ""),
        judge_by_qid={},
        asi_by_qid={},
        sql_by_qid={},
        iteration=1,
    )
    assert out == {}
