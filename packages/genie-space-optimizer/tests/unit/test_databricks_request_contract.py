"""PR-2A — pure ``DatabricksEndpointRequestContract.validate(call_kwargs)``.

Pins the five constraint rules enumerated in
``docs/llmdrivenarchitecture/v5/
stage1-tool-name-and-request-envelope-contract_e7b21f04.plan.md``:

  1. ``response_format.json_schema.name`` matches
     ``^[a-zA-Z0-9_-]{1,128}$`` (the dc89d1a9 / 98ec8950 root cause).
  2. ``response_format.json_schema.schema`` is free of
     Databricks-unsupported JSON-Schema keywords (defends PR-C from
     regressing).
  3. ``tools[*].custom.name`` regex (defense in depth — we never set
     this ourselves, but middleware might).
  4. ``max_tokens`` ≤ configured per-endpoint ceiling.
  5. Sum of ``messages[].content`` lengths ≤ configured context
     budget.

This is a pure-function test suite — no I/O, no LLM client, no
Databricks calls. Every case constructs a minimal ``call_kwargs`` dict
and asserts the violations list.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.databricks_request_contract import (
    DEFAULT_CONTRACT,
    ConstraintViolation,
    DatabricksEndpointRequestContract,
    RequestEnvelopeInvalidError,
)


def _ok_call_kwargs(**overrides):
    """Return a baseline call_kwargs dict that is contract-valid."""
    base = {
        "model": "databricks-claude-opus-4-6",
        "messages": [
            {"role": "system", "content": "system"},
            {"role": "user", "content": "user"},
        ],
        "temperature": 0.1,
        "max_tokens": 1024,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "AbstainableEnvelope_Plan11DiagnoseOutput",
                "schema": {
                    "type": "object",
                    "properties": {
                        "declined": {
                            "type": "object",
                            "properties": {"reason": {"type": "string"}},
                            "additionalProperties": False,
                        },
                        "result": {
                            "type": "object",
                            "properties": {"value": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    },
                    "required": [],
                    "additionalProperties": False,
                },
                "strict": True,
            },
        },
    }
    base.update(overrides)
    return base


# ── happy path ──────────────────────────────────────────────────────


def test_baseline_call_kwargs_has_no_violations() -> None:
    """A canonical Plan-11 envelope after PR-1B sanitization should
    pass every contract rule."""
    assert DEFAULT_CONTRACT.validate(_ok_call_kwargs()) == []


def test_call_kwargs_without_response_format_is_valid() -> None:
    """Plain (non-structured-output) calls — e.g. the free-text
    description-enrichment paths — must not trip the schema-name rule."""
    kwargs = _ok_call_kwargs()
    kwargs.pop("response_format")
    assert DEFAULT_CONTRACT.validate(kwargs) == []


# ── rule 1: response_format.json_schema.name regex ──────────────────


def test_schema_name_with_brackets_violates_regex() -> None:
    """The exact dc89d1a9 / 98ec8950 wire bug: assigning
    ``AbstainableEnvelope[Plan11DiagnoseOutput]`` straight to the name
    field."""
    kwargs = _ok_call_kwargs()
    kwargs["response_format"]["json_schema"]["name"] = (
        "AbstainableEnvelope[Plan11DiagnoseOutput]"
    )
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert len(violations) == 1
    v = violations[0]
    assert v.field == "response_format.json_schema.name"
    assert "AbstainableEnvelope[Plan11DiagnoseOutput]" in str(v.value)
    assert "a-zA-Z0-9_-" in v.constraint


def test_empty_schema_name_violates_regex() -> None:
    kwargs = _ok_call_kwargs()
    kwargs["response_format"]["json_schema"]["name"] = ""
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert any(
        v.field == "response_format.json_schema.name" for v in violations
    )


def test_schema_name_longer_than_128_violates() -> None:
    kwargs = _ok_call_kwargs()
    kwargs["response_format"]["json_schema"]["name"] = "x" * 129
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert any(
        v.field == "response_format.json_schema.name" for v in violations
    )


# ── rule 2: schema keyword allowlist ────────────────────────────────


@pytest.mark.parametrize(
    "forbidden_keyword",
    ["anyOf", "oneOf", "allOf", "$ref", "pattern", "prefixItems"],
)
def test_unsupported_schema_keyword_is_a_violation(forbidden_keyword: str) -> None:
    """Defends PR-C: any future regression in ``_flatten_nullable_anyof``
    or ``_strip_unsupported`` that re-introduces an unsupported keyword
    must light up here, not at deploy time."""
    kwargs = _ok_call_kwargs()
    kwargs["response_format"]["json_schema"]["schema"]["properties"]["bad"] = {
        forbidden_keyword: [{"type": "string"}, {"type": "null"}],
    }
    violations = DEFAULT_CONTRACT.validate(kwargs)
    field_paths = [v.field for v in violations]
    assert any(
        ".schema." in fp and forbidden_keyword in v.constraint
        for fp, v in zip(field_paths, violations)
    ), violations


# ── rule 3: tools[*].custom.name regex (defense in depth) ───────────


def test_tool_custom_name_with_brackets_violates() -> None:
    kwargs = _ok_call_kwargs()
    kwargs["tools"] = [
        {"type": "custom", "custom": {"name": "broken[name]"}},
    ]
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert any(
        v.field.startswith("tools.0.custom.name") for v in violations
    )


def test_tools_can_be_empty_or_missing() -> None:
    kwargs = _ok_call_kwargs()
    kwargs["tools"] = []
    assert DEFAULT_CONTRACT.validate(kwargs) == []


# ── rule 4: max_tokens ceiling ──────────────────────────────────────


def test_max_tokens_above_ceiling_is_a_violation() -> None:
    """The contract carries a per-endpoint ceiling. The default is
    intentionally well above any current Plan 11 budget so this rule
    only fires on accidental misuse (e.g. ``max_tokens=200_000``)."""
    kwargs = _ok_call_kwargs(max_tokens=DEFAULT_CONTRACT.max_tokens_ceiling + 1)
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert any(v.field == "max_tokens" for v in violations)


def test_max_tokens_at_ceiling_is_ok() -> None:
    kwargs = _ok_call_kwargs(max_tokens=DEFAULT_CONTRACT.max_tokens_ceiling)
    assert DEFAULT_CONTRACT.validate(kwargs) == []


# ── rule 5: total messages chars ≤ context budget ───────────────────


def test_messages_total_chars_above_budget_is_a_violation() -> None:
    big_user = "x" * (DEFAULT_CONTRACT.context_chars_budget + 10)
    kwargs = _ok_call_kwargs()
    kwargs["messages"] = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": big_user},
    ]
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert any(v.field == "messages.total_chars" for v in violations)


def test_messages_total_chars_at_budget_is_ok() -> None:
    big_user = "x" * (DEFAULT_CONTRACT.context_chars_budget - len("sys"))
    kwargs = _ok_call_kwargs()
    kwargs["messages"] = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": big_user},
    ]
    assert DEFAULT_CONTRACT.validate(kwargs) == []


# ── multi-violation aggregation ─────────────────────────────────────


def test_validate_reports_all_violations_not_just_first() -> None:
    """The validator must aggregate so the postmortem sees every
    failing rule in one shot — not whack-a-mole."""
    kwargs = _ok_call_kwargs(max_tokens=DEFAULT_CONTRACT.max_tokens_ceiling + 1)
    kwargs["response_format"]["json_schema"]["name"] = "bad[name]"
    violations = DEFAULT_CONTRACT.validate(kwargs)
    field_set = {v.field for v in violations}
    assert "response_format.json_schema.name" in field_set
    assert "max_tokens" in field_set
    assert len(violations) >= 2


# ── error class — used by PR-1C classifier arm + PR-2C runtime ──────


def test_request_envelope_invalid_error_str_lists_violations() -> None:
    violations = [
        ConstraintViolation(
            field="response_format.json_schema.name",
            value="bad[name]",
            constraint="must match ^[a-zA-Z0-9_-]{1,128}$",
        ),
        ConstraintViolation(
            field="max_tokens",
            value=999_999,
            constraint="must be ≤ 32000",
        ),
    ]
    err = RequestEnvelopeInvalidError(violations)
    s = str(err)
    assert "response_format.json_schema.name" in s
    assert "max_tokens" in s
    assert err.violations == violations


def test_request_envelope_invalid_error_class_name_matches_classifier() -> None:
    """The PR-1C classifier maps on the lowercase class name.
    ``"requestenvelopeinvalid" in "requestenvelopeinvaliderror".lower()``
    must hold so the routing fires automatically."""
    assert "requestenvelopeinvalid" in RequestEnvelopeInvalidError.__name__.lower()


# ── contract is configurable, default is stable ─────────────────────


def test_contract_is_a_frozen_dataclass_with_stable_defaults() -> None:
    """The ``DEFAULT_CONTRACT`` is a singleton instance that callers
    import. Pinning its defaults guards against silent ceiling drift."""
    assert isinstance(DEFAULT_CONTRACT, DatabricksEndpointRequestContract)
    assert DEFAULT_CONTRACT.max_tokens_ceiling >= 4096
    assert DEFAULT_CONTRACT.context_chars_budget >= 50_000
    assert DEFAULT_CONTRACT.schema_name_regex == r"^[a-zA-Z0-9_-]{1,128}$"
