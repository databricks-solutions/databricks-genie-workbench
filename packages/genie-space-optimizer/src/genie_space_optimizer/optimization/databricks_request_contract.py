"""Local pre-flight validation of OpenAI-SDK ``call_kwargs`` against
the Databricks Foundation Model endpoint's observed constraints.

Background
----------
The 2026-05-23 lever-loop trials (98ec8950 attempt 8, dc89d1a9 attempt
8) ran with PR-A diagnostic instrumentation live and surfaced the
provider body inline: every Plan 11 Stage 1 call failed with
``BadRequestError: tools.0.custom.name failed
^[a-zA-Z0-9_-]{1,128}$``. The Databricks endpoint translates
``response_format.json_schema.name`` into an internal tool name and
rejects on a regex; Pydantic generic aliases like
``AbstainableEnvelope[Plan11DiagnoseOutput]`` violate that regex
because their ``__name__`` carries ``[`` and ``]``.

This is the *second* envelope-shape Databricks rejection in a row
(PR-C fixed the ``T | None`` ``anyOf`` collapse, which was constraint
#1). The governing principle from the cross-analyst review:

  > Every architecture fix must be validated at the full
  > provider-request boundary, not just at its own internal data
  > structure.

This module implements that boundary as a pure function.

Design
------
``DatabricksEndpointRequestContract.validate(call_kwargs)`` returns a
``list[ConstraintViolation]``. Empty list ⇒ ok-to-dispatch. Non-empty
⇒ the caller MUST raise ``RequestEnvelopeInvalidError(violations)``
without invoking the OpenAI client; ``_classify_llm_error`` routes the
exception to ``error_kind="request_envelope_invalid"`` (PR-1C arm).

Two consumers:

  1. **CI golden test** (PR-2B,
     ``tests/integration/test_databricks_request_contract_golden.py``)
     — enumerates every Plan 11 / legacy LLM call site, captures the
     ``call_kwargs`` each would send for a canonical input, asserts
     ``validate(...) == []``. Catches future regressions at PR time.
  2. **Runtime pre-flight** (PR-2C, ``optimizer._traced_llm_call``)
     — same call, same module. Defense in depth: if anything sneaks
     past CI, the runtime check catches it before the wire.

Reference plan: ``docs/llmdrivenarchitecture/v5/
stage1-tool-name-and-request-envelope-contract_e7b21f04.plan.md``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# Databricks Foundation Model endpoint regex for tool / schema names.
# Pinned from the dc89d1a9 / 98ec8950 trial provider bodies — quoted
# verbatim: ``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$``.
_NAME_REGEX = r"^[a-zA-Z0-9_-]{1,128}$"

# Mirror of ``prompt_io._UNSUPPORTED_KEYWORDS``. Kept as its own
# constant (rather than imported) so a refactor of ``prompt_io`` does
# not silently widen the allowlist here. The CI golden test will
# surface any new keyword the endpoint introduces.
_UNSUPPORTED_SCHEMA_KEYWORDS: frozenset[str] = frozenset({
    "pattern",
    "anyOf",
    "oneOf",
    "allOf",
    "prefixItems",
    "$ref",
    "maxProperties",
    "minProperties",
    "maxLength",
    "minLength",
    "maxItems",
    "minItems",
    "format",
})


@dataclass(frozen=True)
class ConstraintViolation:
    """One contract failure.

    ``field`` is a dotted path into ``call_kwargs`` (e.g.
    ``"response_format.json_schema.name"``) — used by the postmortem
    skill to point the reader directly at the broken field.

    ``value`` is the actual value the caller would have sent. We
    truncate string values to 200 chars at format time so a multi-KB
    schema doesn't bloat the marker.

    ``constraint`` is a human-readable description of the rule that
    fired (e.g. ``"must match ^[a-zA-Z0-9_-]{1,128}$"``).
    """

    field: str
    value: Any
    constraint: str

    def __post_init__(self) -> None:  # noqa: D401 — dataclass hook
        # Truncate string values defensively so emitting a violation
        # via str() or json.dumps cannot blow up a stdout marker.
        if isinstance(self.value, str) and len(self.value) > 200:
            object.__setattr__(self, "value", self.value[:200] + "...")


class RequestEnvelopeInvalidError(Exception):
    """Raised by runtime pre-flight when ``validate(call_kwargs)``
    returns a non-empty list.

    The class name is what ``_classify_llm_error`` keys on to route the
    failure to ``error_kind="request_envelope_invalid"`` automatically
    (the lowercase class name contains ``requestenvelopeinvalid``).

    ``__str__`` lists every violation deterministically so the rendered
    text is grep-friendly in stdout markers; the structured
    ``violations`` attribute is what the marker emitter serialises into
    the ``constraint_violations`` field of
    ``GSO_PLAN11_STAGE1_REQUEST_V1``.
    """

    def __init__(self, violations: list[ConstraintViolation]) -> None:
        self.violations = list(violations)
        rendered = "; ".join(
            f"{v.field}={v.value!r} violates {v.constraint}"
            for v in self.violations
        )
        super().__init__(
            f"Databricks request envelope is invalid: {rendered}"
        )


@dataclass(frozen=True)
class DatabricksEndpointRequestContract:
    """Configurable bundle of endpoint constraints.

    Defaults are conservative: thresholds are set well above any
    current Plan 11 / legacy budget so the rules only fire on
    accidental misuse, never on a legitimately-sized prompt. The CI
    golden test pins the defaults — any narrowing must come with a
    test bump.

    ``max_tokens_ceiling``
        Upper bound on the ``max_tokens`` field. Set high (32k) — the
        intent is to catch typos like ``max_tokens=200_000``, not to
        ration tokens across stages.

    ``context_chars_budget``
        Conservative char-level cap on the total ``messages[].content``
        payload. 700k chars ≈ ~175k tokens at ~4 chars/token, which is
        below the documented Claude / GPT-4 context windows but well
        above any current Plan 11 prompt. The exact tokenizer count
        is endpoint-specific; chars are a deterministic, locally-
        computable proxy that does NOT require a tokenizer.

    ``schema_name_regex``
        Pinned from the trial provider body. Identical to the regex
        the endpoint enforces on tool names.
    """

    max_tokens_ceiling: int = 32_000
    context_chars_budget: int = 700_000
    schema_name_regex: str = _NAME_REGEX
    unsupported_schema_keywords: frozenset[str] = field(
        default_factory=lambda: _UNSUPPORTED_SCHEMA_KEYWORDS,
    )

    # ── public API ──────────────────────────────────────────────────

    def validate(
        self, call_kwargs: dict[str, Any]
    ) -> list[ConstraintViolation]:
        """Return every violation a single ``call_kwargs`` payload
        would trigger on a Databricks Foundation Model endpoint. Empty
        list ⇒ ok-to-dispatch. Aggregating (rather than failing-fast)
        is intentional: postmortems see every failing rule at once.
        """
        violations: list[ConstraintViolation] = []

        # Rule 1 + 2 — response_format introspection
        rf = call_kwargs.get("response_format")
        if isinstance(rf, dict):
            self._check_response_format(rf, violations)

        # Rule 3 — tools[*].custom.name defense in depth
        tools = call_kwargs.get("tools")
        if isinstance(tools, list):
            self._check_tools(tools, violations)

        # Rule 4 — max_tokens ceiling
        max_tokens = call_kwargs.get("max_tokens")
        if isinstance(max_tokens, int) and max_tokens > self.max_tokens_ceiling:
            violations.append(
                ConstraintViolation(
                    field="max_tokens",
                    value=max_tokens,
                    constraint=f"must be ≤ {self.max_tokens_ceiling}",
                )
            )

        # Rule 5 — context budget (cheap char-level proxy)
        messages = call_kwargs.get("messages")
        if isinstance(messages, list):
            self._check_messages_total_chars(messages, violations)

        return violations

    # ── internal rule implementations ───────────────────────────────

    def _check_response_format(
        self,
        rf: dict[str, Any],
        violations: list[ConstraintViolation],
    ) -> None:
        js = rf.get("json_schema")
        if not isinstance(js, dict):
            return
        name = js.get("name")
        regex = re.compile(self.schema_name_regex)
        if not isinstance(name, str) or not regex.match(name):
            violations.append(
                ConstraintViolation(
                    field="response_format.json_schema.name",
                    value=name,
                    constraint=f"must match {self.schema_name_regex}",
                )
            )
        schema = js.get("schema")
        if isinstance(schema, dict):
            self._check_schema_keywords(
                schema,
                "response_format.json_schema.schema",
                violations,
            )

    def _check_schema_keywords(
        self,
        node: Any,
        path: str,
        violations: list[ConstraintViolation],
    ) -> None:
        """Walk ``node`` and flag every unsupported JSON-Schema
        keyword. Defends PR-C: if any future regression re-introduces
        an ``anyOf`` / ``oneOf`` / ``$ref`` somewhere in the schema
        tree, this fires."""
        if isinstance(node, dict):
            for k, v in node.items():
                if k in self.unsupported_schema_keywords:
                    violations.append(
                        ConstraintViolation(
                            field=f"{path}.{k}",
                            value=_summarise(v),
                            constraint=(
                                f"keyword {k!r} is in the Databricks-"
                                "unsupported set "
                                f"{sorted(self.unsupported_schema_keywords)}"
                            ),
                        )
                    )
                else:
                    self._check_schema_keywords(
                        v, f"{path}.{k}", violations
                    )
        elif isinstance(node, list):
            for idx, item in enumerate(node):
                self._check_schema_keywords(
                    item, f"{path}.{idx}", violations
                )

    def _check_tools(
        self,
        tools: list[Any],
        violations: list[ConstraintViolation],
    ) -> None:
        regex = re.compile(self.schema_name_regex)
        for idx, tool in enumerate(tools):
            if not isinstance(tool, dict):
                continue
            custom = tool.get("custom")
            if not isinstance(custom, dict):
                continue
            name = custom.get("name")
            if isinstance(name, str) and not regex.match(name):
                violations.append(
                    ConstraintViolation(
                        field=f"tools.{idx}.custom.name",
                        value=name,
                        constraint=f"must match {self.schema_name_regex}",
                    )
                )

    def _check_messages_total_chars(
        self,
        messages: list[Any],
        violations: list[ConstraintViolation],
    ) -> None:
        total = 0
        for m in messages:
            if not isinstance(m, dict):
                continue
            content = m.get("content")
            if isinstance(content, str):
                total += len(content)
        if total > self.context_chars_budget:
            violations.append(
                ConstraintViolation(
                    field="messages.total_chars",
                    value=total,
                    constraint=(
                        f"must be ≤ {self.context_chars_budget} "
                        "(chars across all message contents — "
                        "deterministic proxy for context window)"
                    ),
                )
            )


def _summarise(value: Any) -> Any:
    """Project an arbitrary schema-keyword value to a marker-friendly
    summary. Long lists / dicts collapse to a length-tagged shape so
    the violation marker stays under the stdout truncation threshold."""
    if isinstance(value, list):
        return f"<list len={len(value)}>"
    if isinstance(value, dict):
        return f"<dict keys={sorted(value.keys())[:5]}>"
    return value


# Singleton instance every caller imports. The defaults are pinned by
# ``test_contract_is_a_frozen_dataclass_with_stable_defaults``; bumping
# any value requires updating that test in lockstep.
DEFAULT_CONTRACT: DatabricksEndpointRequestContract = (
    DatabricksEndpointRequestContract()
)


__all__ = [
    "ConstraintViolation",
    "DEFAULT_CONTRACT",
    "DatabricksEndpointRequestContract",
    "RequestEnvelopeInvalidError",
]
