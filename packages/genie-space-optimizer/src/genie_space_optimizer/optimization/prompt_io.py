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


class Lever6SqlExpressionOutput(LLMOutputContract):
    """Lever-6 SQL-expression proposal. The actual SKILL.md emits a
    single proposal object (not wrapped in a ``proposals: [...]`` list).
    """

    snippet_type: Literal["measure", "filter", "expression"]
    display_name: str
    alias: str = Field(
        default="",
        description="snake_case identifier (required for measure/expression, empty for filter)",
    )
    sql: str = Field(description="The SQL expression (no trailing semicolon)")
    synonyms: list[str] = Field(default_factory=list)
    instruction: str = Field(default="")
    rationale: str = Field(default="")
    target_table: str = Field(default="")
    affected_questions: list[str] = Field(default_factory=list)


class Lever1RcaBridgeOutput(LLMOutputContract):
    """Lever-1 RCA-bridge proposal output. Table-level invocations
    omit synonyms; column-level invocations include them. The
    SKILL.md template fills the synonyms_schema_field slot
    conditionally."""

    description: str = Field(description="1-3 sentence description")
    synonyms: list[str] = Field(
        default_factory=list,
        description="2-5 lowercase NL phrases (column-level only; empty for table-level)",
    )


class Lever12ColumnChange(LLMOutputContract):
    """One column-level change in the Lever-1/2 column-description
    proposal output."""

    table: str
    column: str
    entity_type: Literal["column_dim", "column_measure", "column_key"]
    sections: dict[str, str] = Field(default_factory=dict)


class Lever12TableChange(LLMOutputContract):
    """One table-level change in the Lever-1/2 output."""

    table: str
    sections: dict[str, str] = Field(default_factory=dict)


class Lever12ColumnOutput(LLMOutputContract):
    """Top-level Lever-1/2 column-description proposal output."""

    changes: list[Lever12ColumnChange] = Field(default_factory=list)
    table_changes: list[Lever12TableChange] = Field(default_factory=list)
    rationale: str = Field(default="")


# ── Strategist family (Task 18) ───────────────────────────────────────
#
# The strategist prompts emit deeply nested action_groups with
# lever_directives keyed by string lever number ("1".."6"). Each lever's
# directive value is shape-specific (lever 1: tables/columns; lever 4:
# join_specs; lever 5: instruction_guidance + example_sqls; etc.). Rather
# than pin every nested shape with Literal-checked discriminators (which
# Databricks JSON schema flatteners would reject), we capture the
# top-level structure and pass nested dicts through. Downstream code
# (apply_patch_set in optimizer.py) already validates the inner shapes
# per-lever at apply time.


class _StrategistActionGroupBase(LLMOutputContract):
    """Permissive base for action groups across the strategist family."""

    # Permit unknown fields here because the three strategist variants
    # carry slightly different action-group shapes (triage is leaner;
    # detail and adaptive carry full lever_directives).
    model_config = {"extra": "allow", "str_strip_whitespace": True}

    id: str = Field(default="")
    root_cause_summary: str = Field(default="")
    source_cluster_ids: list[str] = Field(default_factory=list)
    affected_questions: list[str] = Field(default_factory=list)
    priority: int = Field(default=2)
    lever_directives: dict[str, Any] = Field(default_factory=dict)
    coordination_notes: str = Field(default="")
    escalation: str = Field(default="")
    proposals: list[dict[str, Any]] = Field(default_factory=list)


class AdaptiveStrategistOutput(LLMOutputContract):
    """Top-level adaptive-strategist output (loaded from
    adaptive-strategist/SKILL.md).

    Emits exactly one action group plus an optional
    global_instruction_rewrite block keyed by canonical instruction
    section headers (PURPOSE, ASSET ROUTING, etc.).
    """

    action_groups: list[_StrategistActionGroupBase] = Field(default_factory=list)
    global_instruction_rewrite: dict[str, str] = Field(default_factory=dict)
    rationale: str = Field(default="")


class StrategistTriageOutput(LLMOutputContract):
    """Top-level STRATEGIST_TRIAGE_PROMPT output. Triage produces a
    sketch of action groups without full lever_directives."""

    action_groups: list[_StrategistActionGroupBase] = Field(default_factory=list)
    rationale: str = Field(default="")


class StrategistDetailOutput(LLMOutputContract):
    """Top-level STRATEGIST_DETAIL_PROMPT output. Detail expands one
    triaged action group with full lever_directives."""

    action_groups: list[_StrategistActionGroupBase] = Field(default_factory=list)
    global_instruction_rewrite: dict[str, str] = Field(default_factory=dict)
    rationale: str = Field(default="")


# ── Lever-4 join discovery (Task 19) ──────────────────────────────────


class Lever4JoinEndpoint(LLMOutputContract):
    identifier: str
    alias: str = Field(default="")


class Lever4JoinSpec(LLMOutputContract):
    left: Lever4JoinEndpoint
    right: Lever4JoinEndpoint
    sql: list[str] = Field(default_factory=list)
    instruction: str = Field(default="")


class Lever4JoinDiscoveryOutput(LLMOutputContract):
    join_specs: list[Lever4JoinSpec] = Field(default_factory=list)
    rationale: str = Field(default="")


# ── Lever-5a instructions (Task 19) ───────────────────────────────────


class Lever5aInstructionsOutput(LLMOutputContract):
    """LEVER_5A_INSTRUCTION_PROMPT output: prose-only instruction
    document. No example_sqls allowed.

    Class name matches the registry key ``lever_5a_instructions``
    (plural) so the auto-derived class-name convention in
    ``test_every_active_lever_prompt_has_typed_output_contract`` finds it.
    """

    instruction_text: str = Field(default="")
    rationale: str = Field(default="")


# Backwards-compat alias for callers that may have imported the singular form.
Lever5aInstructionOutput = Lever5aInstructionsOutput


class Lever5bExampleSqlOutput(LLMOutputContract):
    """LEVER_5B_EXAMPLE_SQL_PROMPT output (loaded from
    lever-5b-example-sql/SKILL.md). Emits one example_question +
    example_sql + usage_guidance + rationale."""

    example_question: str = Field(description="NL question the example answers")
    example_sql: str = Field(description="SQL teaching the structural pattern")
    usage_guidance: str = Field(default="")
    rationale: str = Field(default="")


# ── Lever-5 holistic / instruction (Task 19) ──────────────────────────


class Lever5InstructionOutput(LLMOutputContract):
    """LEVER_5_INSTRUCTION_PROMPT output — instruction_type discriminates
    three shapes: example_sql, text_instruction, sql_expression.
    Permissive base allows the variant-specific fields."""

    model_config = {"extra": "allow", "str_strip_whitespace": True}

    instruction_type: Literal["example_sql", "text_instruction", "sql_expression"]
    rationale: str = Field(default="")
