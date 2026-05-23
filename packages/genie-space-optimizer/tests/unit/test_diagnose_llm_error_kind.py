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
    assert _classify_llm_error("TimeoutError", "", 0, _req()) == "timeout"
    assert _classify_llm_error("ReadTimeout", "", 0, _req()) == "timeout"


def test_classifier_recognises_parse_kind() -> None:
    assert _classify_llm_error("EnvelopeContractError", "", 200, _req()) == "parse"
    assert _classify_llm_error("JSONDecodeError", "", 200, _req()) == "parse"


def test_classifier_recognises_endpoint_decline_kind() -> None:
    assert (
        _classify_llm_error("ConnectionError", "", 0, _req())
        == "endpoint_decline"
    )
    # PermissionDeniedError is auth (PR-A — auth is its own bucket so
    # postmortems don't conflate a 403 with a network outage).
    assert (
        _classify_llm_error("PermissionDeniedError", "", 0, _req())
        == "auth"
    )


def test_classifier_recognises_client_construction_kind() -> None:
    assert (
        _classify_llm_error("ClientConfigError", "", 0, _req())
        == "client_construction"
    )
    assert (
        _classify_llm_error("AuthError", "", 0, _req())
        == "client_construction"
    )


def test_classifier_recognises_empty_prompt_kind() -> None:
    """Zero-token call with no prompt content should be ``empty_prompt``."""
    assert (
        _classify_llm_error("", "", 0, _req(user_prompt="", system_msg=""))
        == "empty_prompt"
    )


def test_classifier_falls_back_to_unknown_for_unrecognised() -> None:
    assert (
        _classify_llm_error("SomeBizarreError", "", 100, _req()) == "unknown"
    )


# ── PR-A: BadRequestError sub-classification ──────────────────────────


def test_classifier_recognises_badrequest_token_limit() -> None:
    """A 400 body mentioning context length / token budget routes to
    ``token_limit_exceeded`` so postmortems can pinpoint prompt
    overflow without re-running."""
    msg = (
        "BadRequestError: Error code: 400 - {'error': {'message': "
        "'This model's maximum context length is 200000 tokens, however "
        "you requested 220000 tokens', 'type': 'invalid_request_error'}}"
    )
    assert (
        _classify_llm_error("BadRequestError", msg, 0, _req())
        == "token_limit_exceeded"
    )


def test_classifier_recognises_badrequest_response_format() -> None:
    """A 400 body mentioning ``response_format``/``json_schema`` routes
    to ``response_format_invalid`` — the dominant suspect in the
    2026-05-23 trials where every Stage 1 call 400'd."""
    msg = (
        "BadRequestError: Error code: 400 - {'error': {'message': "
        "\"Invalid 'response_format': json_schema with $ref is not "
        "supported by this endpoint\", 'type': 'invalid_request_error'}}"
    )
    assert (
        _classify_llm_error("BadRequestError", msg, 0, _req())
        == "response_format_invalid"
    )


def test_classifier_badrequest_without_known_signal_is_endpoint_decline() -> None:
    """When the 400 body matches neither token nor response_format
    patterns we still emit a structured ``endpoint_decline`` (never
    ``unknown``) so the failure remains greppable."""
    msg = "BadRequestError: Error code: 400 - {'error': {'message': 'bad'}}"
    assert (
        _classify_llm_error("BadRequestError", msg, 0, _req())
        == "endpoint_decline"
    )


def test_classifier_routes_message_only_400_to_endpoint_decline() -> None:
    """Even when the exception class is something generic like
    ``APIStatusError`` the ``code: 400`` substring in the message
    is enough to route to ``endpoint_decline`` (with the same
    token/response_format precedence)."""
    msg = "APIStatusError: Error code: 400 - upstream rejected request"
    assert (
        _classify_llm_error("APIStatusError", msg, 0, _req())
        == "endpoint_decline"
    )


# ── PR-1C: request_envelope_invalid arm ───────────────────────────────


def test_classifier_recognises_tool_name_regex_violation() -> None:
    """The 98ec8950 / dc89d1a9 trials surfaced this exact body:
    ``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$``. With PR-1B
    in place the next out-of-spec name will look the same — the
    classifier must route it to ``request_envelope_invalid`` so the
    postmortem skill points at the request builder, not the prompt
    or the endpoint."""
    msg = (
        "BadRequestError: Error code: 400 - {'error': {'message': "
        "\"tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$\", "
        "'type': 'invalid_request_error'}}"
    )
    assert (
        _classify_llm_error("BadRequestError", msg, 0, _req())
        == "request_envelope_invalid"
    )


def test_classifier_request_envelope_invalid_overrides_response_format_invalid() -> None:
    """A 400 that mentions BOTH the tool-name regex AND a
    response_format keyword (e.g. ``json_schema``) must route to
    ``request_envelope_invalid``, not ``response_format_invalid`` —
    the name violation is the more specific cause and ``schema``
    appears legitimately in the surrounding payload."""
    msg = (
        "BadRequestError: Error code: 400 - {'error': {'message': "
        "\"json_schema validation failed: tools.0.custom.name failed "
        "^[a-zA-Z0-9_-]{1,128}$\"}}"
    )
    assert (
        _classify_llm_error("BadRequestError", msg, 0, _req())
        == "request_envelope_invalid"
    )


def test_classifier_maps_request_envelope_invalid_error_class() -> None:
    """PR-2C will introduce a typed ``RequestEnvelopeInvalidError``
    raised by the local pre-flight validator BEFORE the OpenAI client
    is invoked. The classifier maps on the exception class name so the
    arm fires regardless of message format."""
    assert (
        _classify_llm_error(
            "RequestEnvelopeInvalidError",
            "constraint_violations=[tool_name_regex]",
            0,
            _req(),
        )
        == "request_envelope_invalid"
    )


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


# ── PR-A: error_message + endpoint marker fields ─────────────────────


def test_marker_carries_error_message_truncated_to_500() -> None:
    """``error_message`` must be captured on the marker (the dominant
    PR-A gap) and truncated to 500 chars to keep stdout bounded."""
    long_msg = "BadRequestError: " + ("x" * 800)
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="r",
        iteration=1,
        qid="gs_009",
        outcome="llm_error",
        tokens_input=0,
        tokens_output=0,
        error_kind="endpoint_decline",
        exception_class="BadRequestError",
        error_message=long_msg,
        endpoint="databricks-claude-opus-4-6",
    )
    assert "error_message" in line
    assert "BadRequestError" in line
    assert "databricks-claude-opus-4-6" in line
    # Truncation invariant: payload chunk for error_message stays ≤ 500.
    # We can't directly assert on the JSON-encoded length without parsing,
    # so check that the original 800-char body did not land in full.
    assert "x" * 800 not in line


def test_marker_default_error_message_is_empty_string() -> None:
    """Successful diagnoses should not carry an error_message field
    populated by the caller; the default is the empty string."""
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="r",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        tokens_input=100,
        tokens_output=50,
    )
    # Field is always present (uniform schema) but empty.
    assert '"error_message":""' in line


# ── PR-A: request fingerprint marker ─────────────────────────────────


def test_request_marker_emits_fingerprint_fields() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage1_request_marker,
    )

    line = plan11_stage1_request_marker(
        optimization_run_id="r",
        iteration=1,
        qid="gs_009",
        skill_id="plan11_diagnose",
        call_id="plan11_stage1_diagnose.iter_1",
        system_msg_chars=1234,
        user_prompt_chars=5678,
        max_tokens=4096,
        response_format_keywords=[
            "type",
            "json_schema",
            "json_schema.name",
            "json_schema.schema",
            "json_schema.strict",
            "schema.type",
            "schema.properties",
            "schema.additionalProperties",
        ],
        endpoint="databricks-claude-opus-4-6",
    )
    assert "GSO_PLAN11_STAGE1_REQUEST_V1" in line
    assert "plan11_diagnose" in line
    assert '"system_msg_chars":1234' in line
    assert '"user_prompt_chars":5678' in line
    assert '"max_tokens":4096' in line
    assert "databricks-claude-opus-4-6" in line
    assert "additionalProperties" in line
