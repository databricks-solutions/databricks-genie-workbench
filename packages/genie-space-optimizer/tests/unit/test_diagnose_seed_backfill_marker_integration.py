"""Trial 13h — integration test: ``diagnose_failing_qids`` plumbs the new
blame-set-source fields all the way into the emitted marker.

Where ``test_diagnose_seed_backfill.py`` mocks the marker function to
inspect ``call_args``, this test drives the real marker by capturing
stdout. It pins the contract that the emitted ``GSO_PLAN11_STAGE1_DIAGNOSIS_V1``
JSON payload contains the three Trial 13h fields with the expected values
when the LLM emits an empty ``blame_set`` and the seed rescues.

This is the test that would have failed in the post-13g workbench replay
if Trial 13h had not landed.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse


_SCHEMA = ["catalog.schema.orders.revenue"]


def _qid_card(qid: str, seed: list[str]) -> dict:
    return {
        "qid": qid,
        "question_text": "Top 10 orders?",
        "ground_truth_sql": "SELECT * FROM orders ORDER BY revenue DESC LIMIT 10",
        "generated_sql": "SELECT * FROM orders",
        "judge_rationale": "Missing top-N pattern",
        "blame_set_seed": seed,
    }


def _empty_blame_llm_response(qid: str) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=True,
        parsed_output={
            "diagnoses": [
                {
                    "qid": qid,
                    "rca_kind_label": "RANK() instead of LIMIT plus defensive filters",
                    "observed_failure": "Wrong rows returned",
                    "generated_sql_issue": "Used MAX() not ORDER BY + LIMIT",
                    "expected_sql_shape": "ORDER BY revenue DESC LIMIT 10",
                    "blame_set": [],
                    "evidence_summary": "High-confidence diagnosis, narrative present",
                    "confidence": "high",
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=100,
        tokens_output=50,
        duration_ms=1234,
        error=None,
    )


def _extract_stage1_marker_payload(stdout_text: str) -> dict:
    """Find the GSO_PLAN11_STAGE1_DIAGNOSIS_V1 marker and parse its JSON."""
    for line in stdout_text.splitlines():
        if line.startswith("GSO_PLAN11_STAGE1_DIAGNOSIS_V1 "):
            return json.loads(line.split(" ", 1)[1])
    raise AssertionError(
        "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 marker not found in stdout; "
        f"captured: {stdout_text!r}"
    )


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
def test_empty_llm_blame_set_emits_seed_backfill_marker(MockLlmCall) -> None:
    """The post-13g workbench failure-mode QID: confident LLM diagnosis
    with empty ``blame_set``. Post-13h, the seed rescues and the marker
    records ``blame_set_source: "seed_backfill"`` so postmortems can
    observe how often this happens."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_empty_blame_llm_response("gs_009")
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        results = diagnose_failing_qids(
            failing_qids=[
                _qid_card("gs_009", ["catalog.schema.orders.revenue"])
            ],
            schema_columns=_SCHEMA,
            optimization_run_id="run_trial13h",
            iteration=1,
            w=MagicMock(),
        )

    assert len(results) == 1
    assert results[0].blame_set == ("catalog.schema.orders.revenue",)

    payload = _extract_stage1_marker_payload(buf.getvalue())
    assert payload["outcome"] == "diagnosed"
    assert payload["qid"] == "gs_009"
    assert payload["blame_set_source"] == "seed_backfill"
    assert payload["blame_set_llm_emitted"] == 0
    assert payload["blame_set_post_schema_dropped"] == 0
    assert payload["blame_set_size"] == 1
    # And critically — Trial 13h's whole point: diagnosis_actionable
    # flips back to True because the gate sees blame_set_size > 0.
    assert payload["diagnosis_actionable"] is True
