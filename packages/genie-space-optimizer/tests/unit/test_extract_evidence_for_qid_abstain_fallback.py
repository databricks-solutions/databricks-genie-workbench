"""Plan 3 Task 10 — extract_evidence_for_qid abstain + error fallback."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_evidence_extractor import (
    extract_evidence_for_qid,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=100, completion_tokens=40, total_tokens=140,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_extract_returns_none_when_llm_declines() -> None:
    decline_envelope = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "two equally-plausible blame sets",
            "needed_evidence": ["disambiguating_judge_verdict"],
            "suggested_next_step": "re_dispatch_after_judge_clarification",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline_envelope),
    ):
        result = extract_evidence_for_qid(
            w=None,
            qid="gs_037",
            judge={"verdict": "wrong_answer"},
            asi={},
            sql="",
            iteration=1,
        )
    assert result is None


def test_extract_returns_none_when_envelope_parse_fails() -> None:
    malformed = '{"not": "envelope shape"}'
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(malformed),
    ):
        result = extract_evidence_for_qid(
            w=None,
            qid="gs_001",
            judge={"verdict": "x"},
            asi={},
            sql="",
            iteration=1,
        )
    assert result is None


def test_extract_returns_none_when_http_call_fails() -> None:
    """HTTP failure (429 after retries) bubbles into
    LlmReasoningResponse.error; the extractor maps that to None."""
    def _boom(*args, **kwargs):
        raise RuntimeError("Serving endpoint returned 429 after 3 retries")

    with patch.object(optimizer, "_traced_llm_call", side_effect=_boom):
        result = extract_evidence_for_qid(
            w=None,
            qid="gs_001",
            judge={"verdict": "x"},
            asi={},
            sql="",
            iteration=1,
        )
    assert result is None


def test_extract_decline_logs_reason_and_needed_evidence(caplog) -> None:
    """Postmortem-readability requirement: decline must produce a log
    line naming the reason + needed_evidence."""
    import logging
    caplog.set_level(
        logging.INFO,
        logger="genie_space_optimizer.optimization.rca_evidence_extractor",
    )

    decline_envelope = json.dumps({
        "result": None,
        "declined": {
            "reason": "missing_schema_context",
            "explanation": "no UC metadata",
            "needed_evidence": ["table_metadata", "column_descriptions"],
            "suggested_next_step": "re_dispatch_after_uc_enrichment",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline_envelope),
    ):
        extract_evidence_for_qid(
            w=None,
            qid="gs_042",
            judge={"verdict": "wrong_column"},
            asi={},
            sql="",
            iteration=3,
        )

    decline_logs = [
        r.message for r in caplog.records
        if "rca_evidence_extractor.declined" in r.message
    ]
    assert len(decline_logs) == 1
    msg = decline_logs[0]
    assert "qid=gs_042" in msg
    assert "missing_schema_context" in msg
    assert "table_metadata" in msg
    assert "column_descriptions" in msg
