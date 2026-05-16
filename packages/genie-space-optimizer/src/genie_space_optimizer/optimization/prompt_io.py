"""Typed I/O contracts for LLM prompts.

Every active prompt has a Pydantic ``LLMOutputContract`` subclass that
declares the expected JSON shape of the model's response. The shape is
fed to Databricks Foundation Model APIs as ``response_format={"type":
"json_schema"}`` to ENFORCE the contract server-side, and the response
text is then re-parsed locally as a defense-in-depth check.

Databricks Foundation Model APIs support a SUBSET of JSON Schema:
  * Supported: type, properties, required, items (single type), enum,
    description.
  * Not supported: pattern, anyOf, oneOf, allOf, prefixItems, $ref,
    maxProperties, minProperties, maxLength.
  * Special case: ``[type, "null"]`` for nullable fields is OK.

Pydantic v2's default ``model_json_schema()`` emits some of these for
Optional/Union types and nested models. ``build_response_format()``
flattens / strips them so the payload is Databricks-safe.

Plan reference: docs/prompt_improvements/2026-05-17-prompt-registry-and-typed-io-hygiene.md
"""
from __future__ import annotations

import re
from typing import Any, TypeVar

from pydantic import BaseModel

_T = TypeVar("_T", bound="LLMOutputContract")


class LLMOutputContract(BaseModel):
    """Base class for every LLM output contract.

    Subclasses MUST declare every field that the prompt's
    ``<output_schema>`` block in ``SKILL.md`` documents. The Pydantic
    schema is the source of truth — if the SKILL.md and the contract
    disagree, the contract wins (and a test under
    ``test_prompt_io_contracts.py`` should pin the SKILL.md to match).
    """

    model_config = {
        "extra": "forbid",  # reject unexpected fields server-side too
        "str_strip_whitespace": True,
    }


_UNSUPPORTED_KEYWORDS: frozenset[str] = frozenset({
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
    "format",  # date-time etc. — Databricks may reject specific formats
})


def _strip_unsupported(node: Any) -> Any:
    """Recursively strip Databricks-unsupported JSON Schema keywords."""
    if isinstance(node, dict):
        return {
            k: _strip_unsupported(v)
            for k, v in node.items()
            if k not in _UNSUPPORTED_KEYWORDS
        }
    if isinstance(node, list):
        return [_strip_unsupported(item) for item in node]
    return node


def build_response_format(model_cls: type[BaseModel]) -> dict[str, Any]:
    """Build a Databricks-safe ``response_format`` payload from a
    Pydantic model.

    Returns the dict you pass directly to
    ``openai.chat.completions.create(..., response_format=...)``.
    """
    schema = model_cls.model_json_schema()
    # Pydantic emits $defs for nested models; inline them shallowly by
    # walking refs once.
    defs = schema.pop("$defs", {})

    def _inline(node: Any) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                target = defs.get(ref.removeprefix("#/$defs/"))
                if target is not None:
                    return _inline(target)
            return {k: _inline(v) for k, v in node.items() if k != "$ref"}
        if isinstance(node, list):
            return [_inline(item) for item in node]
        return node

    schema = _inline(schema)
    schema = _strip_unsupported(schema)
    schema.setdefault("type", "object")
    return {
        "type": "json_schema",
        "json_schema": {
            "name": model_cls.__name__,
            "schema": schema,
            "strict": True,
        },
    }


_JSON_BRACE_RE = re.compile(r"\{[\s\S]*\}")
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```")


def _extract_json_text(raw: str) -> str:
    """Return the first JSON-object substring in ``raw``.

    Handles code-fence wrappers (```json ... ```) and stray prose.
    """
    fence = _CODE_FENCE_RE.search(raw)
    if fence:
        raw = fence.group(1)
    match = _JSON_BRACE_RE.search(raw)
    if not match:
        raise ValueError(
            f"no JSON object found in response (first 200 chars: {raw[:200]!r})"
        )
    return match.group(0)


def validate_and_parse(raw_text: str, model_cls: type[_T]) -> _T:
    """Parse ``raw_text`` as JSON, validate against ``model_cls``.

    Raises ``ValueError`` on any failure. The caller can pass this
    function as ``response_validator=`` to ``_traced_llm_call`` so
    malformed responses retry with exponential backoff before
    propagating.
    """
    json_text = _extract_json_text(raw_text)
    try:
        return model_cls.model_validate_json(json_text)
    except Exception as exc:
        raise ValueError(
            f"LLM response did not match {model_cls.__name__} contract: {exc}"
        ) from exc


# ── Per-prompt output contracts ───────────────────────────────────────
# Plan 2026-05-17-prompt-registry-and-typed-io-hygiene Phase F.

from typing import Literal  # noqa: E402
from pydantic import Field  # noqa: E402


class Stage1SkillPick(LLMOutputContract):
    """One pick in the Stage-1 discovery output.

    Mirrors the JSON shape declared in
    skills/stage-1-discovery/SKILL.md <output_schema>.
    """

    skill_id: str = Field(
        description="One of the skill_ids declared as pickable_by_stage_1=true",
    )
    target_objects: list[str] = Field(
        default_factory=list,
        description="Fully-qualified table/column/MV/function identifiers",
    )
    expected_impact_qids: list[str] = Field(
        default_factory=list,
        description="Question IDs from the cluster_briefs Question IDs: line",
    )
    evidence_refs: list[str] = Field(
        default_factory=list,
        description="Cluster IDs or trace URIs the pick was derived from",
    )
    why: str = Field(description="One-line routing rationale")
    priority: Literal[1, 2, 3] = Field(
        description="1 = must-do this iteration, 2 = should-do, 3 = nice-to-have",
    )


class Stage1DiscoveryOutput(LLMOutputContract):
    """Top-level Stage-1 discovery output. Field name matches SKILL.md:
    ``discovery_rationale`` (NOT ``reasoning``)."""

    applicable_skills: list[Stage1SkillPick] = Field(default_factory=list)
    discovery_rationale: str = Field(default="")
