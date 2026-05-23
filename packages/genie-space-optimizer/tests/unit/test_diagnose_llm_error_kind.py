"""SM Cutover Phase 1.C — structured ``llm_error`` detail.

The 2026-05-23 trial postmortems showed Plan 11 Stage 1 emitting opaque
``GSO_PLAN11_STAGE1_DIAGNOSIS_V1 outcome="llm_error" tokens_input=0``
records with no further detail. This test pins the structured-error
classifier so postmortems can distinguish a pre-flight client failure
from a mid-flight parse failure.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_stage1_diagnosis_marker,
)
from genie_space_optimizer.optimization.stages.diagnose import (
    _classify_llm_error,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)


def _req(user_prompt: str = "u", system_msg: str = "s") -> LlmReasoningRequest:
    return LlmReasoningRequest(
        call_id="t",
        skill_id="plan11_diagnose",
        system_msg=system_msg,
        user_prompt=user_prompt,
        result_cls=type("X", (), {}),
        max_tokens=100,
    )


def test_classifier_recognises_timeout_kind() -> None:
    assert _classify_llm_error("TimeoutError", 0, _req()) == "timeout"
    assert _classify_llm_error("ReadTimeout", 0, _req()) == "timeout"


def test_classifier_recognises_parse_kind() -> None:
    assert _classify_llm_error("EnvelopeContractError", 200, _req()) == "parse"
    assert _classify_llm_error("JSONDecodeError", 200, _req()) == "parse"


def test_classifier_recognises_endpoint_decline_kind() -> None:
    assert (
        _classify_llm_error("ConnectionError", 0, _req())
        == "endpoint_decline"
    )
    assert (
        _classify_llm_error("PermissionDeniedError", 0, _req())
        == "endpoint_decline"
    )


def test_classifier_recognises_client_construction_kind() -> None:
    assert (
        _classify_llm_error("ClientConfigError", 0, _req())
        == "client_construction"
    )
    assert (
        _classify_llm_error("AuthError", 0, _req()) == "client_construction"
    )


def test_classifier_recognises_empty_prompt_kind() -> None:
    """Zero-token call with no prompt content should be ``empty_prompt``."""
    assert _classify_llm_error("", 0, _req(user_prompt="", system_msg="")) == "empty_prompt"


def test_classifier_falls_back_to_unknown_for_unrecognised() -> None:
    assert _classify_llm_error("SomeBizarreError", 100, _req()) == "unknown"


def test_marker_fails_loud_on_llm_error_without_error_kind() -> None:
    """The marker MUST reject ``outcome='llm_error'`` without ``error_kind``."""
    with pytest.raises(ValueError, match="error_kind"):
        plan11_stage1_diagnosis_marker(
            optimization_run_id="r",
            iteration=1,
            qid="gs_009",
            outcome="llm_error",
            tokens_input=0,
            tokens_output=0,
        )


def test_marker_accepts_llm_error_with_error_kind() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="r",
        iteration=1,
        qid="gs_009",
        outcome="llm_error",
        tokens_input=0,
        tokens_output=0,
        error_kind="endpoint_decline",
        exception_class="ConnectionError",
    )
    assert "error_kind" in line
    assert "endpoint_decline" in line
    assert "ConnectionError" in line


def test_marker_does_not_require_error_kind_for_diagnosed_outcome() -> None:
    """Successful diagnosis path must not be coupled to error_kind."""
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="r",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="plural_top_n_collapse",
        confidence="high",
        tokens_input=200,
        tokens_output=80,
    )
    assert "outcome" in line
    assert "diagnosed" in line
