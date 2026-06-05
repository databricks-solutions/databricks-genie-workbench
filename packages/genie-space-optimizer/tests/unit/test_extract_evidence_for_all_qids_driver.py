"""Plan 3 Task 11 — sequential per-qid driver."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_evidence_extractor import (
    extract_evidence_for_all_qids,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)


def _success_envelope(qid: str) -> str:
    return json.dumps({
        "result": {
            "qid": qid,
            "observed_failure": f"failure for {qid}",
            "generated_sql_issue": "issue",
            "expected_sql_shape": "shape",
            "blame_set": [],
            "suggested_repair_family": "top_n_with_ordering",
            "repair_hint_patch_type": "add_example_sql",
            "confidence": "high",
            "quoted_evidence": [],
        },
        "declined": None,
    })


def _decline_envelope() -> str:
    return json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "ambiguous",
            "needed_evidence": [],
            "suggested_next_step": "skip",
        },
    })


def _batched_envelope(qids: tuple[str, ...]) -> str:
    """A single batched-call response (Phase 1 P1.2) whose ``evidences``
    array carries one entry per diagnosed qid. Qids omitted here fall
    through to the caller's per-QID fallback."""
    return json.dumps({
        "result": {
            "evidences": [
                json.loads(_success_envelope(q))["result"] for q in qids
            ],
        },
        "declined": None,
    })


def _make_client_responding_with(envelope_by_qid_order: list[str]) -> MagicMock:
    """Returns a stub client whose chat.completions.create cycles through
    the provided envelopes in order — one per call."""
    client = MagicMock(name="OpenAIClientStub")
    completions = []
    for env in envelope_by_qid_order:
        choice = MagicMock()
        choice.message.content = env
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=100, completion_tokens=40, total_tokens=140,
        )
        completions.append(completion)
    client.chat.completions.create.side_effect = completions
    return client


def test_driver_returns_dict_keyed_by_successful_qids() -> None:
    qids = ("gs_001", "gs_002", "gs_003")
    # Phase 1 P1.2 batch-first driver (qid count >= BATCH_RCA_MIN_QIDS):
    # one batched call diagnoses gs_001 and gs_003 and omits gs_002; the
    # only uncovered qid (gs_002) then declines on the per-QID fallback.
    envelopes = [
        _batched_envelope(("gs_001", "gs_003")),
        _decline_envelope(),
    ]
    client = _make_client_responding_with(envelopes)
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = extract_evidence_for_all_qids(
            w=None,
            qids=qids,
            judge_by_qid={q: {"verdict": "wrong"} for q in qids},
            asi_by_qid={q: {} for q in qids},
            sql_by_qid={q: f"SELECT * FROM t_{q}" for q in qids},
            iteration=2,
        )

    assert set(result.keys()) == {"gs_001", "gs_003"}
    assert isinstance(result["gs_001"], PerQidRcaEvidence)
    assert isinstance(result["gs_003"], PerQidRcaEvidence)
    assert result["gs_001"].qid == "gs_001"
    assert result["gs_003"].qid == "gs_003"


def test_driver_preserves_input_order_for_dispatch() -> None:
    """Sequential dispatch order matches qids tuple."""
    qids = ("gs_005", "gs_001", "gs_009")
    completions = []
    for q in qids:
        choice = MagicMock()
        choice.message.content = _success_envelope(q)
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=100, completion_tokens=40, total_tokens=140,
        )
        completions.append(completion)

    dispatch_order: list[str] = []

    def _spy_create(**kwargs):
        user_msg = next(
            m["content"] for m in kwargs["messages"] if m["role"] == "user"
        )
        payload = json.loads(user_msg)
        dispatch_order.append(payload["qid"])
        idx = len(dispatch_order) - 1
        return completions[idx]

    client = MagicMock(name="OpenAIClientStub")
    client.chat.completions.create.side_effect = _spy_create
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        extract_evidence_for_all_qids(
            w=None,
            qids=qids,
            judge_by_qid={q: {} for q in qids},
            asi_by_qid={q: {} for q in qids},
            sql_by_qid={q: "" for q in qids},
            iteration=1,
        )

    assert tuple(dispatch_order) == qids


def test_driver_handles_empty_qids_tuple_without_dispatch() -> None:
    """No qids → no LLM calls → empty dict."""
    client = MagicMock(name="OpenAIClientStub")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = extract_evidence_for_all_qids(
            w=None,
            qids=(),
            judge_by_qid={},
            asi_by_qid={},
            sql_by_qid={},
            iteration=1,
        )
    assert result == {}
    assert client.chat.completions.create.call_count == 0


def test_driver_skips_qids_with_missing_per_qid_inputs() -> None:
    """If a qid appears in qids but not in judge/asi/sql dicts, the
    extractor passes empty dicts/strings; the LLM may decline."""
    qids = ("gs_001",)
    client = _make_client_responding_with([_decline_envelope()])
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = extract_evidence_for_all_qids(
            w=None,
            qids=qids,
            judge_by_qid={},
            asi_by_qid={},
            sql_by_qid={},
            iteration=1,
        )
    assert result == {}
