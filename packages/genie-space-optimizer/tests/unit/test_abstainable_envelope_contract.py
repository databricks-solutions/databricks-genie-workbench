"""Plan 2 Task 2 — AbstainableEnvelope[T] Pydantic generic contract."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.llm_abstain import (
    AbstainReason,
    AbstainVerdict,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
    EnvelopeContractError,
    parse_envelope,
)
from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
)


class _DummyResult(LLMOutputContract):
    answer: str


def test_envelope_is_generic_with_result_and_declined_fields() -> None:
    EnvCls = AbstainableEnvelope[_DummyResult]
    schema_fields = EnvCls.model_fields
    assert set(schema_fields.keys()) == {"result", "declined"}
    assert schema_fields["result"].default is None
    assert schema_fields["declined"].default is None


def test_envelope_parse_returns_result_when_only_result_present() -> None:
    raw = '{"result": {"answer": "yes"}, "declined": null}'
    parsed = parse_envelope(raw, _DummyResult)
    assert isinstance(parsed, _DummyResult)
    assert parsed.answer == "yes"


def test_envelope_parse_returns_verdict_when_only_declined_present() -> None:
    raw = """{
        "result": null,
        "declined": {
            "reason": "missing_schema_context",
            "explanation": "no metadata",
            "needed_evidence": ["table_metadata"],
            "suggested_next_step": "re_dispatch"
        }
    }"""
    parsed = parse_envelope(raw, _DummyResult)
    assert isinstance(parsed, AbstainVerdict)
    assert parsed.reason == AbstainReason.MISSING_SCHEMA_CONTEXT
    assert parsed.needed_evidence == ("table_metadata",)


def test_envelope_parse_rejects_both_populated() -> None:
    raw = (
        '{"result": {"answer": "y"}, "declined": '
        '{"reason": "other", "explanation": "", "needed_evidence": [], '
        '"suggested_next_step": ""}}'
    )
    with pytest.raises(EnvelopeContractError, match="exactly one"):
        parse_envelope(raw, _DummyResult)


def test_envelope_parse_rejects_both_empty() -> None:
    raw = '{"result": null, "declined": null}'
    with pytest.raises(EnvelopeContractError, match="exactly one"):
        parse_envelope(raw, _DummyResult)


def test_envelope_parse_propagates_malformed_json() -> None:
    with pytest.raises(EnvelopeContractError):
        parse_envelope("not json at all", _DummyResult)


def test_envelope_build_response_format_is_databricks_safe() -> None:
    """The envelope's response_format must lack the JSON Schema
    keywords Databricks Foundation Model API rejects.

    PR-C note: this asserts the SUFFICIENT condition (no forbidden
    structural keys). The NECESSARY condition (typed result/declined
    branches survive the strip) lives in
    ``test_abstainable_envelope_schema_branches.py`` — the dc89d1a9
    failure proved this test alone is not enough.
    """
    from tests._schema_utils import assert_no_forbidden_schema_keys

    EnvCls = AbstainableEnvelope[_DummyResult]
    fmt = build_response_format(EnvCls)
    assert_no_forbidden_schema_keys(fmt)
