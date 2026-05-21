"""Plan 11 — validation pipeline replay tests.

Drives :func:`repair_patch_with_llm` end-to-end against the four
Phase C anchors × three validation scenarios:

  - happy      — patch passes validate_patch on first try
  - repair     — patch fails once, LLM revision passes on attempt 2
  - exhaustion — repair loop reaches max_attempts → returns None and
                 fires GSO_LLM_CONTRACT_FAILURE_V1

All LLM calls are mocked at the per-stage import surface (the same
shape that production sees from
:class:`LlmReasoningResponse` — ``parsed_output`` is a dict,
``tokens_input`` / ``tokens_output`` are direct fields). The
``validate_patch`` dispatcher is mocked so the test runs without a
warehouse / metadata_snapshot.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
    ValidationError,
    ValidationResult,
)


_ANCHOR_QIDS = (
    "airline_ticketing_and_fare_analysis_gs_009",
    "airline_ticketing_and_fare_analysis_gs_024",
    "7now_gs_013",
    "7now_gs_026",
)

_SCENARIOS = (
    (
        "happy",
        {"example_question": "Top 10 orders?",
         "example_sql": "SELECT * FROM t LIMIT 10"},
    ),
    (
        "repair",
        {},  # body empty on attempt 1 → fails; LLM repair returns a full body
    ),
    (
        "exhaustion",
        {},  # body empty + every revision still fails validate_patch
    ),
)


def _make_llm_response_with_body(patch_body: dict) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_repair.replay.attempt_1",
        skill_id="plan11_repair",
        succeeded=True,
        parsed_output={
            "intent_name": "replay revision",
            "intent_description": "fix the validation error",
            "repair_hypothesis": "add the missing example_sql field",
            "patch_type": "add_example_sql",
            "rationale": "addresses Layer 1 schema rejection",
            "confidence": "high",
            "patch_body": patch_body,
            "blame_set": [],
            "target_qids": ["replay_qid"],
        },
        declined=None,
        raw_text=json.dumps({"result": {}, "declined": None}),
        tokens_input=200,
        tokens_output=100,
        duration_ms=1,
        error=None,
    )


@pytest.mark.parametrize("qid", _ANCHOR_QIDS, ids=lambda q: q.split("_")[-1])
@pytest.mark.parametrize(
    "scenario,patch_body",
    _SCENARIOS,
    ids=[s for s, _ in _SCENARIOS],
)
@patch("genie_space_optimizer.optimization.stages.repair_loop.validate_patch")
@patch("genie_space_optimizer.optimization.stages.repair_loop.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.repair_loop.plan11_repair_loop_marker"
)
@patch(
    "genie_space_optimizer.optimization.stages.repair_loop.llm_contract_failure_marker"
)
def test_plan11_validation_pipeline(
    mock_cf,
    mock_marker,
    MockLlmCall,
    mock_validate,
    qid: str,
    scenario: str,
    patch_body: dict,
) -> None:
    from genie_space_optimizer.optimization.stages.repair_loop import (
        repair_patch_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_REPAIR_LOOP_V1 {}"
    mock_cf.return_value = "GSO_LLM_CONTRACT_FAILURE_V1 {}"

    cluster = FailureCluster(
        cluster_id="H001",
        semantic_theme="replay-anchor",
        member_qids=(qid,),
        unifying_evidence="replay test",
        repair_hypothesis="replay test hypothesis",
        primary_blame_set=(),
        confidence="high",
    )
    original = RepairProposal(
        intent_id=f"intent_{qid}",
        intent_name="replay original",
        intent_description="original failing proposal",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="placeholder",
        confidence="high",
        patch_body=patch_body,
        blame_set=(),
        repair_hypothesis="replay hypothesis",
        target_qids=(qid,),
    )
    errors = (
        ValidationError(
            patch_id=f"intent_{qid}",
            error_kind="genie_schema",
            error_detail="example_sql is required",
            failing_location="patch_body.example_sql",
        ),
    )

    repaired_body = {
        "example_question": "Q?",
        "example_sql": "SELECT * FROM t LIMIT 10",
    }
    if scenario == "happy":
        mock_validate.return_value = ValidationResult(
            patch_id=f"intent_{qid}", is_valid=True, errors=(),
        )
    elif scenario == "repair":
        mock_validate.side_effect = [
            ValidationResult(
                patch_id=f"intent_{qid}", is_valid=False, errors=errors,
            ),
            ValidationResult(
                patch_id=f"intent_{qid}", is_valid=True, errors=(),
            ),
        ]
    else:  # exhaustion
        mock_validate.return_value = ValidationResult(
            patch_id=f"intent_{qid}", is_valid=False, errors=errors,
        )

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_llm_response_with_body(repaired_body),
    )

    result = repair_patch_with_llm(
        original,
        errors,
        cluster,
        w=MagicMock(),
        validate_kwargs={
            "config": {},
            "metadata_snapshot": {},
            "spark": None,
            "w": MagicMock(),
            "catalog": "main",
            "gold_schema": "gso",
            "warehouse_id": "abc",
        },
        max_attempts=2,
    )

    if scenario in ("happy", "repair"):
        assert result is not None, (
            f"{qid}/{scenario}: expected repaired proposal, got None"
        )
        assert isinstance(result, RepairProposal)
        assert result.patch_type == PatchType.ADD_EXAMPLE_SQL
        # On the happy path the loop returns after one LLM call + one
        # validate; on the repair path it loops once more.
        expected_marker_count = 1 if scenario == "happy" else 2
        assert mock_marker.call_count == expected_marker_count, (
            f"{qid}/{scenario}: expected {expected_marker_count} markers, "
            f"got {mock_marker.call_count}"
        )
    else:
        assert result is None, (
            f"{qid}/{scenario}: expected None (loop exhausted)"
        )
        mock_cf.assert_called()
        outcomes = [c[1]["outcome"] for c in mock_marker.call_args_list]
        assert outcomes[-1] == "exhausted", (
            f"{qid}/{scenario}: expected last marker outcome=exhausted, "
            f"got {outcomes!r}"
        )
