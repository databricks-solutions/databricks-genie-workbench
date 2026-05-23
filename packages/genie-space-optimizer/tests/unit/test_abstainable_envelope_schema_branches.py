"""PR-C — pin the AbstainableEnvelope[T] response_format branch typing.

The pre-PR-C ``test_envelope_build_response_format_is_databricks_safe``
test only checked that no forbidden keyword (``anyOf``/``$ref``/...)
leaked into the response_format. That assertion is necessary but not
sufficient: an empty schema ``{}`` also passes the "no forbidden
keyword" check while being completely meaningless.

The 2026-05-22 trial reviewer reproduced exactly that failure mode for
``AbstainableEnvelope[Plan11DiagnoseOutput]``. The schema sent to
Databricks model serving collapsed to::

    {
      "result":   {"default": null},
      "declined": {"default": null}
    }

— both branches stripped of their typed payload, because Pydantic
emitted ``anyOf: [{"$ref": "..."}, {"type": "null"}]`` for the
nullable Generic field and ``_strip_unsupported`` removed the
``anyOf`` keyword wholesale instead of preserving the non-null
branch. Databricks rejects the resulting schema before inference,
which matches the production failure signature (zero-token
BadRequestError, ~5s wallclock, no usage billed).

This file pins the structural invariants the envelope's
response_format MUST satisfy regardless of the inner result class:

  1. ``result`` and ``declined`` properties exist.
  2. Both branches carry a top-level ``type`` (i.e., the typed schema
     survived the strip).
  3. The ``result`` branch's object properties (when ``T`` is itself
     an object) carry their declared property keys — not just an empty
     ``properties: {}`` shell.

These invariants are what makes the schema actionable to the LLM.
Without them the model has no guidance on what to emit, and the
provider rejects the request before inference even when the LLM
itself would be capable.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
)
from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
)
from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
    Plan11DiagnoseOutput,
)


class _SimpleResult(LLMOutputContract):
    """Minimal result type — a single primitive field. Pydantic emits
    a `$ref` to this class when it appears as `_T | None` in the
    AbstainableEnvelope, exercising the same collapse path as
    Plan11DiagnoseOutput."""

    answer: str


def _properties(fmt: dict) -> dict:
    return fmt["json_schema"]["schema"]["properties"]


# ── Generic invariants (apply to every AbstainableEnvelope[T]) ──────


def test_envelope_result_branch_carries_type_after_build() -> None:
    """``result`` must declare a JSON-schema ``type`` so the LLM and
    the provider both know what shape to expect/accept. The pre-PR-C
    schema collapsed this to ``{"default": null}`` (no type),
    triggering the Databricks 400 in the dc89d1a9 trial."""
    EnvCls = AbstainableEnvelope[_SimpleResult]
    fmt = build_response_format(EnvCls)
    result_branch = _properties(fmt)["result"]
    assert "type" in result_branch, (
        f"result branch lost its type during strip: {result_branch!r}. "
        f"This collapse is the dc89d1a9 root cause."
    )


def test_envelope_declined_branch_carries_type_after_build() -> None:
    """Same invariant as ``result`` but for the declined verdict
    branch. Both branches need typed shape; one untyped branch is
    enough to break Databricks strict-mode validation."""
    EnvCls = AbstainableEnvelope[_SimpleResult]
    fmt = build_response_format(EnvCls)
    declined_branch = _properties(fmt)["declined"]
    assert "type" in declined_branch, (
        f"declined branch lost its type during strip: "
        f"{declined_branch!r}."
    )


def test_envelope_result_branch_preserves_inner_properties() -> None:
    """When ``T`` is an object, the result branch must carry the
    object's declared properties. ``{"type": "object", "properties":
    {}}`` is degenerate — the LLM has nothing to fill."""
    EnvCls = AbstainableEnvelope[_SimpleResult]
    fmt = build_response_format(EnvCls)
    result_branch = _properties(fmt)["result"]
    assert result_branch.get("type") == "object", result_branch
    inner_props = result_branch.get("properties") or {}
    assert "answer" in inner_props, (
        f"result branch's inner properties were stripped: "
        f"{inner_props!r}. The envelope-wrapping path must keep the "
        f"inner schema intact."
    )


# ── Concrete regression test for Plan11DiagnoseOutput ────────────────


def test_plan11_envelope_preserves_diagnoses_branch_after_strip() -> None:
    """The exact schema dc89d1a9 sent — the wrapper around
    ``Plan11DiagnoseOutput``. Pins that the diagnoses list type
    survives end-to-end through build_response_format."""
    EnvCls = AbstainableEnvelope[Plan11DiagnoseOutput]
    fmt = build_response_format(EnvCls)
    result_branch = _properties(fmt)["result"]
    assert result_branch.get("type") == "object", result_branch
    inner_props = result_branch.get("properties") or {}
    assert "diagnoses" in inner_props, (
        f"Plan11DiagnoseOutput.diagnoses field disappeared from the "
        f"response_format schema: {inner_props!r}. Until this passes "
        f"the lever-loop trial will continue to 400 at the endpoint."
    )
    diagnoses_field = inner_props["diagnoses"]
    assert diagnoses_field.get("type") == "array", diagnoses_field


# ── Reaffirm prior invariants (no forbidden keywords leak) ──────────


def test_plan11_envelope_remains_strip_clean() -> None:
    """The PR-C fix must not regress the existing Databricks-safe
    invariant — no anyOf/oneOf/$ref leakage.

    Uses a STRUCTURAL key walk, not ``forbidden in repr(fmt)``, because
    the post-PR-C schema preserves description text that may contain
    the literal substring ``pattern`` etc. The forbidden check is about
    JSON Schema keywords, not prose content.
    """
    from tests._schema_utils import assert_no_forbidden_schema_keys

    EnvCls = AbstainableEnvelope[Plan11DiagnoseOutput]
    fmt = build_response_format(EnvCls)
    assert_no_forbidden_schema_keys(fmt)


# ── Round-trip: post-fix schema still admits XOR parse_envelope ─────


def test_envelope_round_trip_with_only_result_still_validates() -> None:
    """Even after the schema fix flattens nullable anyOf, the typed
    envelope class must continue to accept a JSON body with only
    ``result`` populated — XOR semantics live in parse_envelope, not
    in JSON Schema."""
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        parse_envelope,
    )

    raw = '{"result": {"answer": "yes"}, "declined": null}'
    parsed = parse_envelope(raw, _SimpleResult)
    assert isinstance(parsed, _SimpleResult)
    assert parsed.answer == "yes"


def test_envelope_round_trip_with_only_declined_still_validates() -> None:
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        parse_envelope,
    )

    raw = """{
        "result": null,
        "declined": {
            "reason": "missing_schema_context",
            "explanation": "no metadata",
            "needed_evidence": ["table_metadata"],
            "suggested_next_step": "re_dispatch"
        }
    }"""
    parsed = parse_envelope(raw, _SimpleResult)
    assert isinstance(parsed, AbstainVerdict)
    assert parsed.reason == AbstainReason.MISSING_SCHEMA_CONTEXT
