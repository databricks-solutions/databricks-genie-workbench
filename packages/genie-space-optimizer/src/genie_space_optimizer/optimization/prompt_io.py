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


def _flatten_nullable_anyof(node: Any) -> Any:
    """Promote ``T`` out of Pydantic's ``T | None`` ``anyOf`` shape.

    Pydantic emits nullable Generic fields (e.g.
    ``AbstainableEnvelope[T].result: T | None = None``) as::

        {"anyOf": [<schema-for-T>, {"type": "null"}], "default": null,
         "title": "Result"}

    Naively dropping ``anyOf`` in ``_strip_unsupported`` deletes the
    typed branch entirely and leaves only ``{"default": null}`` — which
    Databricks model serving rejects with a 400 BadRequestError before
    inference (root cause of the 2026-05-22 dc89d1a9 trial).

    This helper detects that exact pattern (any number of branches where
    AT MOST one is ``{"type": "null"}`` and the rest are a single typed
    branch) and inlines the non-null branch's keys into the parent,
    preserving sibling annotations like ``default``, ``title``,
    ``description``. The post-parse XOR check in ``parse_envelope`` keeps
    enforcing "exactly one of result/declined" — JSON Schema doesn't have
    to encode that constraint.

    Conservative on purpose: if ``anyOf`` has multiple non-null branches
    (a true union like ``int | str | None``), we leave it alone so
    ``_strip_unsupported`` can still drop it. The downstream contract
    must not rely on such unions for active prompts; if one shows up the
    schema test will surface it.
    """
    if isinstance(node, dict):
        any_of = node.get("anyOf")
        if isinstance(any_of, list):
            null_branches = [
                b for b in any_of
                if isinstance(b, dict) and b.get("type") == "null"
            ]
            non_null = [
                b for b in any_of
                if not (isinstance(b, dict) and b.get("type") == "null")
            ]
            if len(null_branches) >= 1 and len(non_null) == 1:
                inlined_branch = _flatten_nullable_anyof(non_null[0])
                merged: dict[str, Any] = {}
                if isinstance(inlined_branch, dict):
                    merged.update(inlined_branch)
                for k, v in node.items():
                    if k == "anyOf":
                        continue
                    if k in merged and k in {
                        "type", "properties", "items", "enum",
                        "additionalProperties", "required",
                    }:
                        # Non-null branch is the source of truth for shape.
                        continue
                    merged[k] = _flatten_nullable_anyof(v)
                return merged
        return {k: _flatten_nullable_anyof(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_flatten_nullable_anyof(item) for item in node]
    return node


# Databricks Foundation Model endpoint enforces this regex on
# ``tools[*].custom.name`` and, internally, on
# ``response_format.json_schema.name`` (which it translates into a tool
# name). Pinned by the 2026-05-23 dc89d1a9 / 98ec8950 lever-loop trials
# where every Plan 11 ``AbstainableEnvelope[T]`` call was rejected with
# ``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$`` before tokens
# were consumed (root cause analysis in
# docs/llmdrivenarchitecture/v5/
# stage1-tool-name-and-request-envelope-contract_e7b21f04.plan.md).
_DATABRICKS_SCHEMA_NAME_REGEX = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")
_SCHEMA_NAME_FORBIDDEN_RE = re.compile(r"[^a-zA-Z0-9_-]")


def _safe_schema_name(raw: str) -> str:
    """Project a Pydantic class ``__name__`` (possibly a generic alias
    like ``"AbstainableEnvelope[Plan11DiagnoseOutput]"``) to a string
    that satisfies the Databricks endpoint's tool-name regex
    ``^[a-zA-Z0-9_-]{1,128}$``.

    Rules (intentionally minimal — every char that the regex already
    accepts is preserved exactly so existing names like ``_Example``
    or ``Plan11DiagnoseOutput`` round-trip unchanged):
      * Every forbidden char is replaced with ``_``. Brackets, commas,
        spaces, dots, slashes — all become ``_``.
      * Leading/trailing ``_`` are NOT stripped — ``_`` is in the
        accept set, and pre-existing names like ``_Example`` rely on
        the underscore staying put.
      * Runs of ``_`` are NOT collapsed (would also mutate
        already-safe names that legitimately use ``__``).
      * If the input is missing or empty, the sentinel ``"schema"`` is
        used so the length ≥1 bound is never violated.
      * If the result exceeds 128 chars, it is truncated. (No active
        envelope name approaches this length so this branch is
        defensive.)
    """
    if not isinstance(raw, str) or not raw:
        return "schema"
    sanitized = _SCHEMA_NAME_FORBIDDEN_RE.sub("_", raw)
    if len(sanitized) > 128:
        sanitized = sanitized[:128]
    if not sanitized:
        sanitized = "schema"
    return sanitized


def build_response_format(model_cls: type[BaseModel]) -> dict[str, Any]:
    """Build a Databricks-safe ``response_format`` payload from a
    Pydantic model.

    Returns the dict you pass directly to
    ``openai.chat.completions.create(..., response_format=...)``.

    Strict-mode rules:
      * Models inheriting ``extra: forbid`` (the LLMOutputContract default)
        emit ``strict: True`` and have ``additionalProperties: False``
        injected at every object node — Databricks strict-mode requires
        this combo.
      * Models that override to ``extra: allow`` emit ``strict: False``
        so Databricks does not reject the request before the LLM runs.
      * A model that is strict at the top level but contains a nested
        permissive child also emits ``strict: False`` — strict mode
        cannot coexist with any additionalProperties: True descendant.

    Plan: 2026-05-17-active-callsite-typed-output-wiring.md Task 1
    """
    schema = model_cls.model_json_schema()
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
    # PR-C step 2: collapse Pydantic's nullable ``anyOf`` shape into the
    # non-null branch BEFORE stripping. Otherwise ``T | None`` Generic
    # fields lose their typed payload entirely (the dc89d1a9 root cause).
    schema = _flatten_nullable_anyof(schema)
    schema = _strip_unsupported(schema)
    schema.setdefault("type", "object")

    permissive = _schema_has_permissive_node(schema)
    if not permissive:
        _inject_additional_properties_false(schema)

    return {
        "type": "json_schema",
        "json_schema": {
            # Pydantic generic aliases like ``AbstainableEnvelope[T]``
            # set ``__name__`` to a string containing ``[`` and ``]``,
            # which violates the Databricks endpoint's tool-name regex
            # and causes a 400 ``BadRequestError`` with tokens_input=0
            # before inference. ``_safe_schema_name`` projects the raw
            # name onto the regex's accept set.
            "name": _safe_schema_name(model_cls.__name__),
            "schema": schema,
            "strict": not permissive,
        },
    }


def _schema_has_permissive_node(node: Any) -> bool:
    """Return True if any object node in the schema declares
    ``additionalProperties`` as anything other than False — including
    True, a dict (subschema), or absent.

    For the absent case, we treat it as permissive only when the node is
    an object with declared properties (a real model node, not a leaf
    primitive declaration).
    """
    if isinstance(node, dict):
        if node.get("type") == "object" and "properties" in node:
            ap = node.get("additionalProperties", None)
            if ap is None or ap is True or isinstance(ap, dict):
                return True
        for v in node.values():
            if _schema_has_permissive_node(v):
                return True
    elif isinstance(node, list):
        for item in node:
            if _schema_has_permissive_node(item):
                return True
    return False


def _inject_additional_properties_false(node: Any) -> None:
    """Walk the schema and set ``additionalProperties: False`` on every
    object node that does not already declare it. Required for strict
    mode."""
    if isinstance(node, dict):
        if node.get("type") == "object" and "properties" in node:
            node.setdefault("additionalProperties", False)
        for v in node.values():
            _inject_additional_properties_false(v)
    elif isinstance(node, list):
        for item in node:
            _inject_additional_properties_false(item)


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
from pydantic import Field, field_validator  # noqa: E402


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


# Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 3 —
# whitelist of relationship-type sentinels embedded in Lever4JoinSpec
# `sql` entries. The Genie API accepts only these three values.
_LEVER_4_VALID_RT_SENTINELS: tuple[str, ...] = (
    "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
    "FROM_RELATIONSHIP_TYPE_ONE_TO_MANY",
    "FROM_RELATIONSHIP_TYPE_ONE_TO_ONE",
)
_LEVER_4_RT_REGEX = "|".join(re.escape(s) for s in _LEVER_4_VALID_RT_SENTINELS)


class Lever4JoinSpec(LLMOutputContract):
    """Wire contract for a single join spec emitted by L4.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 3.
    Accepts ``instruction`` as either ``str`` or ``list[str]`` (Trial-5
    trace-0 evidence: model emits ``list[str]`` 1/2 traces). The list
    form is coerced to a newline-joined string at the boundary so
    downstream Genie API patch consumers see the canonical single-
    string shape.

    The ``sql`` list must contain BOTH the equijoin predicate AND a
    ``--rt=FROM_RELATIONSHIP_TYPE_*--`` sentinel from the whitelist.
    """

    left: Lever4JoinEndpoint
    right: Lever4JoinEndpoint
    sql: list[str] = Field(default_factory=list)
    instruction: str = Field(default="")

    @field_validator("instruction", mode="before")
    @classmethod
    def _coerce_instruction_to_str(cls, v: Any) -> str:
        if isinstance(v, list):
            return "\n".join(str(x) for x in v if x is not None)
        if v is None:
            return ""
        return str(v)

    @field_validator("sql")
    @classmethod
    def _require_predicate_and_sentinel(cls, v: list[str]) -> list[str]:
        if len(v) < 2:
            raise ValueError(
                "sql must contain at least 2 elements: the equijoin "
                "predicate and the --rt=FROM_RELATIONSHIP_TYPE_*-- sentinel"
            )
        if not any(
            re.search(rf"--rt=({_LEVER_4_RT_REGEX})--", str(s))
            for s in v
        ):
            raise ValueError(
                "sql must contain a relationship-type sentinel of the form "
                f"--rt=<one of: {', '.join(_LEVER_4_VALID_RT_SENTINELS)}>--"
            )
        return v


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

    ``instruction_text`` is constrained by Pydantic's ``max_length``
    to ``MAX_HOLISTIC_INSTRUCTION_CHARS`` (=8000). This catches
    silent post-call truncation at parse time. Note: the FM-API
    JSON-schema subset does NOT support ``maxLength`` so this
    constraint is client-side post-LLM only. The system message
    + SKILL.md output-budget guidance (Task 9) communicates the
    budget to the model.
    """
    # Lazy import inside the class body to avoid circulars at module
    # load time (config imports prompt_io for related contracts).
    from genie_space_optimizer.common.config import (
        MAX_HOLISTIC_INSTRUCTION_CHARS as _MAX_L5A_CHARS,
    )

    instruction_text: str = Field(
        default="",
        max_length=_MAX_L5A_CHARS,
        description=(
            "Plain-text instruction document using ALL-CAPS section "
            "headers. Empty string means no fix this iteration."
        ),
    )
    rationale: str = Field(
        default="",
        description="Brief per-cluster explanation of changes made.",
    )


# Backwards-compat alias for callers that may have imported the singular form.
Lever5aInstructionOutput = Lever5aInstructionsOutput


class Lever5bExampleSqlOutput(LLMOutputContract):
    """LEVER_5B_EXAMPLE_SQL_PROMPT output (loaded from
    lever-5b-example-sql/SKILL.md). Emits one example_question +
    example_sql + usage_guidance + rationale.

    Plan 10 Phase B3 — schema pin: ``example_question`` and
    ``example_sql`` are required ``str`` fields with no ``Optional``
    wrapper and no default. Pydantic rejects ``null`` and missing keys
    at validation time so the synthesizer can emit a
    ``GSO_LLM_CONTRACT_FAILURE_V1`` decline record instead of silently
    treating the empty value as a regular gate failure and burning a
    retry slot. The 2026-05-19 ``ab65fefe`` (7now) postmortem traced
    one of the four anchors to this exact silent path.
    """

    example_question: str = Field(description="NL question the example answers")
    example_sql: str = Field(description="SQL teaching the structural pattern")
    usage_guidance: str = Field(default="")
    rationale: str = Field(default="")


# ── Cluster-driven teaching-kit synthesis (Task 1 of
#    2026-05-17-cluster-driven-example-synthesis-hardening.md) ──
#
# Models the nested teaching-kit shape the cluster-driven prompt's
# <output_schema> emits:
#   {
#     "kit_summary": "...",
#     "example_sql": { example_question, example_sql, usage_guidance },
#     "supporting_changes": [<patch>, <patch>, ...]
#   }
#
# Variant-specific fields for ``supporting_changes`` are modeled with the
# nullable-variant-fields workaround: the FM-API JSON-schema subset
# rejects ``oneOf``/``anyOf`` so we cannot discriminate the patch
# variants via a Union at the JSON-Schema layer. Instead, every variant-
# specific field is declared as ``Optional`` on a single
# ``_SupportingChange`` model and the model is permissive (extra: allow)
# so the LLM can include the variant fields without forcing a schema
# union. Runtime validators (``normalize_teaching_kit`` and friends)
# enforce the discriminator post-hoc.


class _KitExampleSql(LLMOutputContract):
    """Nested example_sql under :class:`TeachingKitOutput`. Strict — the
    teaching-kit's example is always exactly one shape."""

    example_question: str = Field(
        description="Customer-style NL question the example answers."
    )
    example_sql: str = Field(
        description="Valid Databricks SQL teaching the pattern; no semicolon."
    )
    usage_guidance: str = Field(
        default="",
        description="Short note on when Genie should reuse this example.",
    )


class _SupportingChange(BaseModel):
    """One entry in ``supporting_changes``. Permissive so the FM-API
    schema does not require oneOf/anyOf to express the variant shapes."""

    model_config = {"extra": "allow", "str_strip_whitespace": True}

    patch_type: Literal[
        "add_instruction",
        "add_column_synonym",
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
    ]

    # add_instruction
    section_name: str | None = None
    new_text: str | None = None
    # add_column_synonym
    table: str | None = None
    column: str | None = None
    synonyms: list[str] | None = None
    # add_sql_snippet_*
    display_name: str | None = None
    sql: str | None = None
    instruction: str | None = None
    target_table: str | None = None


class TeachingKitOutput(LLMOutputContract):
    """Cluster-driven teaching-kit shape emitted by
    :data:`CLUSTER_DRIVEN_EXAMPLE_SYNTHESIS_PROMPT`. One example_sql
    surrounded by short, narrowly-scoped supporting changes — the
    reactive counterpart to pre-flight that addresses a specific RCA
    failure cluster.
    """

    kit_summary: str = Field(
        description="Short explanation of the failure pattern this kit teaches."
    )
    example_sql: _KitExampleSql = Field(
        description="Exactly one example_sql object."
    )
    supporting_changes: list[_SupportingChange] = Field(
        default_factory=list,
        description="0-3 supporting patches that complement the example.",
    )


# Alias matching the inventory test's CamelCase derivation from the
# registry key ``cluster_driven_example_synthesis``. Lets the registry
# inventory test find a Pydantic model under its derived name without
# renaming the canonical TeachingKitOutput.
ClusterDrivenExampleSynthesisOutput = TeachingKitOutput


# ── Lever-5 holistic / instruction (Task 19) ──────────────────────────


class Lever5InstructionOutput(LLMOutputContract):
    """LEVER_5_INSTRUCTION_PROMPT output — instruction_type discriminates
    three shapes: example_sql, text_instruction, sql_expression.
    Permissive base allows the variant-specific fields."""

    model_config = {"extra": "allow", "str_strip_whitespace": True}

    instruction_type: Literal["example_sql", "text_instruction", "sql_expression"]
    rationale: str = Field(default="")
