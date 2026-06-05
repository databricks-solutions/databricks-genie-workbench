"""Trial 17 — Lever Selection Contract.

Descriptive bookkeeping that documents which patch_types each of the 6
levers can produce. This is **NOT** deterministic lever-picking — the
LLM still chooses ``selected_lever``. This module:

1. Provides ``LEVER_TO_PATCH_TYPES`` — a closed-set membership table
   used by the consistency validator and by code that needs to infer a
   coarse lever for legacy proposals that don't carry
   ``selected_lever``.
2. Exposes ``validate_plan_vs_proposal_consistency(plan, proposal)``
   that returns a typed reason string when the LLM emits a proposal
   whose ``patch_type`` is inconsistent with its declared
   ``selected_lever``.
3. Exposes ``infer_lever_from_patch_type(patch_type)`` for the
   ``forbidden_signature`` enrichment in ``acceptance_gate`` and
   ``applier_gate`` — used when a proposal landed before Trial 17
   selected_lever was wired and therefore carries an empty lever
   string.

Architectural note: this module **does not** map RCA → lever. The
Stage 3 LLM picks the lever based on the diagnosis it sees in the
prompt. Code only validates self-consistency of the LLM's own output.
"""
from __future__ import annotations

from typing import Iterable, Optional

from genie_space_optimizer.optimization.repair_intent import PatchType


# ── Lever ID set (closed) ────────────────────────────────────────────

LEVER_IDS: tuple[str, ...] = (
    "lever-1",
    "lever-2",
    "lever-3",
    "lever-4",
    "lever-5",
    "lever-6",
)


# ── Lever → patch_types membership table ─────────────────────────────
# The Stage 3 LLM is shown this membership when emitting a proposal so
# its self-declared ``selected_lever`` is consistent with the
# ``patch_type`` it picked. Levers 5a and 5b share the integer lever
# "lever-5" (prose + example_sql variants).

LEVER_TO_PATCH_TYPES: dict[str, frozenset[PatchType]] = {
    "lever-1": frozenset({
        PatchType.ADD_DESCRIPTION,
        PatchType.UPDATE_DESCRIPTION,
        PatchType.ADD_COLUMN_DESCRIPTION,
        PatchType.UPDATE_COLUMN_DESCRIPTION,
        PatchType.ADD_COLUMN_SYNONYM,
        PatchType.REMOVE_COLUMN_SYNONYM,
        PatchType.RENAME_COLUMN_ALIAS,
        PatchType.HIDE_COLUMN,
        PatchType.UNHIDE_COLUMN,
        PatchType.ADD_TABLE,
        PatchType.REMOVE_TABLE,
    }),
    # Metric-view column refinement uses the same description ops as
    # lever-1; the distinction is made by the LLM looking at the target
    # asset_kind. We keep both members so the validator is permissive.
    "lever-2": frozenset({
        PatchType.UPDATE_COLUMN_DESCRIPTION,
        PatchType.ADD_COLUMN_DESCRIPTION,
    }),
    "lever-3": frozenset({
        PatchType.ADD_TVF,
        PatchType.REMOVE_TVF,
        PatchType.ADD_TVF_DESCRIPTION,
    }),
    "lever-4": frozenset({
        PatchType.ADD_JOIN_SPEC,
        PatchType.UPDATE_JOIN_SPEC,
        PatchType.REMOVE_JOIN_SPEC,
    }),
    "lever-5": frozenset({
        # 5a — prose instructions
        PatchType.ADD_INSTRUCTION,
        PatchType.UPDATE_INSTRUCTION,
        PatchType.UPDATE_INSTRUCTION_SECTION,
        PatchType.REWRITE_INSTRUCTION,
        PatchType.REMOVE_INSTRUCTION,
        # 5b — example SQL
        PatchType.ADD_EXAMPLE_SQL,
        PatchType.UPDATE_EXAMPLE_SQL,
        PatchType.REMOVE_EXAMPLE_SQL,
        # Phase 2 P2.4 — negative example SQL belongs to the same
        # lever family (5b) because it occupies the same Genie slot.
        PatchType.ADD_EXAMPLE_SQL_NEGATIVE,
    }),
    "lever-6": frozenset({
        PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        PatchType.ADD_SQL_SNIPPET_FILTER,
        PatchType.ADD_SQL_SNIPPET_MEASURE,
    }),
}


# Reverse index for fast inference. When a patch_type belongs to
# multiple levers (e.g. lever-1 and lever-2 share metadata ops), the
# inference returns the lowest-numbered lever — this is only used for
# legacy proposals that don't carry ``selected_lever``; the
# determination is descriptive, not normative.
_PATCH_TYPE_TO_LEVER: dict[PatchType, str] = {}
for _lever_id, _patch_types in LEVER_TO_PATCH_TYPES.items():
    for _pt in _patch_types:
        if _pt not in _PATCH_TYPE_TO_LEVER:
            _PATCH_TYPE_TO_LEVER[_pt] = _lever_id


# ── Inference and validation API ─────────────────────────────────────


def infer_lever_from_patch_type(patch_type: str | PatchType | None) -> str:
    """Return the lever a patch_type most likely belongs to.

    Used by the ``forbidden_signature`` enrichment in
    ``acceptance_gate`` and ``applier_gate`` when the proposal landed
    without an LLM-declared ``selected_lever`` (legacy / pre-Trial-17
    serialized proposals). Returns ``""`` for unknown patch_types so
    the signature falls back to ``"?"`` instead of asserting.
    """
    if patch_type is None:
        return ""
    if isinstance(patch_type, str):
        try:
            patch_type = PatchType(patch_type)
        except ValueError:
            return ""
    return _PATCH_TYPE_TO_LEVER.get(patch_type, "")


def is_lever_id(value: str) -> bool:
    """``True`` iff ``value`` is one of the closed-set lever ids."""
    return value in LEVER_IDS


def validate_plan_vs_proposal_consistency(
    *,
    selected_lever: str,
    patch_type: str | PatchType,
) -> Optional[str]:
    """Validate that an LLM-emitted ``(selected_lever, patch_type)``
    pair is internally consistent.

    Returns ``None`` when consistent; returns a typed reason string
    when inconsistent. The reason is suitable for stamping onto a
    ``TerminalRecord.forbidden_signature`` so the next iteration's
    LLM sees its own self-contradiction.

    The validator is permissive when ``selected_lever`` is empty
    (legacy proposals pre-Trial-17) — the contract only applies when
    the LLM actually declared a lever.
    """
    if not selected_lever:
        return None
    if not is_lever_id(selected_lever):
        return (
            f"lever_plan_violation:unknown_lever={selected_lever!r}"
        )

    if isinstance(patch_type, str):
        try:
            pt = PatchType(patch_type)
        except ValueError:
            return (
                f"lever_plan_violation:unknown_patch_type={patch_type!r}"
            )
    else:
        pt = patch_type

    allowed = LEVER_TO_PATCH_TYPES.get(selected_lever, frozenset())
    if pt not in allowed:
        return (
            f"lever_plan_violation:plan={selected_lever},"
            f"patch={pt.value}"
        )
    return None


# ── Trial 17.1: semantic descriptions ────────────────────────────────
#
# The lever menu used to embed only ``id`` + ``allowed_patch_types``.
# Live-LLM sweep on 2026-05-25 showed that on iteration 1 (no forbidden
# signatures yet) the model defaults to the lowest-friction member of
# lever-5 (``add_instruction``) for grammar-shape pivots like "RANK()
# instead of LIMIT N", because nothing in the prompt told it that
# lever-6 / lever-5b were the better tools for that diagnosis family.
#
# These descriptions are **always** in the prompt (they don't depend on
# the existence of a prior forbidden signature) and surface:
#   - ``description``: one-line "what this lever does".
#   - ``prefer_when``: closed-vocabulary RCA-shape tokens that bias —
#     but never force — selection. The Stage 3 LLM still picks
#     ``selected_lever`` freely; code only validates self-consistency.
_LEVER_DESCRIPTIONS: dict[str, dict[str, object]] = {
    "lever-1": {
        "description": (
            "Metadata refinement: add/update table or column descriptions, "
            "synonyms, alias renames, hide/unhide columns, add tables to the "
            "Genie Space."
        ),
        "prefer_when": [
            "ambiguous_or_missing_column_meaning",
            "wrong_table_inferred",
            "missing_synonym",
            "needs_hidden_column",
            "missing_table",
        ],
    },
    "lever-2": {
        "description": (
            "Metric-view column refinement: same description ops as lever-1 "
            "but targeting metric_view assets specifically."
        ),
        "prefer_when": [
            "metric_view_column_meaning_unclear",
        ],
    },
    "lever-3": {
        "description": (
            "TVF (table-valued function) routing: add/remove TVFs to expose "
            "or hide parameterized query templates."
        ),
        "prefer_when": [
            "parameterized_query_template_missing",
            "tvf_routing_failure",
        ],
    },
    "lever-4": {
        "description": (
            "Join discovery: add/update/remove join specs so the planner "
            "stops hallucinating join keys or finds previously-missing "
            "cross-table joins."
        ),
        "prefer_when": [
            "hallucinated_join_key",
            "missing_cross_table_join",
            "wrong_join_direction",
        ],
    },
    "lever-5": {
        "description": (
            "Instructions: lever-5a adds/updates prose hints in the space "
            "instructions; lever-5b adds/updates example SQL that anchors "
            "a specific SQL grammar pattern by demonstration. Prose alone "
            "(5a) RARELY changes generated SQL grammar — when the "
            "diagnosis names a concrete SQL shape (RANK/LIMIT/ORDER BY/"
            "GROUP BY/WHERE filter), prefer 5b (add_example_sql) or "
            "lever-6 over 5a (add_instruction)."
        ),
        "prefer_when": [
            "missing_business_definition_or_terminology",
            "soft_policy_or_convention",
            "needs_concrete_query_example",
        ],
    },
    "lever-6": {
        "description": (
            "SQL expression primitives: add reusable sql_snippet_filter / "
            "sql_snippet_expression / sql_snippet_measure entries the "
            "planner can compose. The most direct way to teach the space "
            "a specific SQL grammar shape (top-N, moving average, YoY "
            "delta, custom filter)."
        ),
        "prefer_when": [
            "grammar_pivot:rank_to_limit",
            "grammar_pivot:missing_order_by",
            "grammar_pivot:missing_group_by",
            "grammar_pivot:missing_filter",
            "grammar_pivot:missing_window",
            "reusable_metric_or_measure",
        ],
    },
}


def lever_menu_for_prompt() -> list[dict]:
    """Return the closed lever menu shape the Stage 3 prompt embeds.

    Each entry has: ``id``, ``allowed_patch_types``, ``description`` and
    ``prefer_when``. Trial 17.1 adds the latter two so the iteration-1
    LLM has semantic context (not just structural allow-lists) when
    picking ``selected_lever``. The Stage 3 LLM still picks the lever
    — these fields bias but do not force.
    """
    out: list[dict] = []
    for lever_id in LEVER_IDS:
        entry: dict = {
            "id": lever_id,
            "allowed_patch_types": sorted(
                pt.value for pt in LEVER_TO_PATCH_TYPES[lever_id]
            ),
        }
        desc = _LEVER_DESCRIPTIONS.get(lever_id, {})
        entry["description"] = str(desc.get("description", ""))
        entry["prefer_when"] = list(desc.get("prefer_when", []))
        out.append(entry)
    return out


def patch_types_for_lever(
    lever_id: str,
) -> frozenset[PatchType]:
    """Return the closed set of patch_types valid for ``lever_id``."""
    return LEVER_TO_PATCH_TYPES.get(lever_id, frozenset())


def archetype_catalog_menu_for_prompt() -> list[dict]:
    """Return a JSON-serialisable archetype menu for Stage 3 prompts.

    Trial 17 Step 7 — the archetype catalog stops being a deterministic
    control-flow gate (``pick_archetype`` no longer hard-rejects clusters
    that don't match a shipped archetype) and instead becomes context the
    LLM sees alongside the lever menu. The LLM may reference an archetype
    name in ``selected_lever``'s justification but is not bound to one.
    """
    try:
        from genie_space_optimizer.optimization.archetypes import (
            ARCHETYPES,
        )
    except Exception:
        return []
    out: list[dict] = []
    for arch in ARCHETYPES:
        try:
            out.append({
                "name": str(getattr(arch, "name", "")),
                "applicable_root_causes": sorted(
                    str(rc) for rc in (
                        getattr(arch, "applicable_root_causes", set()) or set()
                    )
                ),
                "required_constructs": list(
                    (getattr(arch, "output_shape", {}) or {})
                    .get("requires_constructs", [])
                ),
                "patch_type": str(getattr(arch, "patch_type", "")),
            })
        except Exception:
            continue
    return out


__all__ = [
    "LEVER_IDS",
    "LEVER_TO_PATCH_TYPES",
    "infer_lever_from_patch_type",
    "is_lever_id",
    "validate_plan_vs_proposal_consistency",
    "lever_menu_for_prompt",
    "patch_types_for_lever",
    "archetype_catalog_menu_for_prompt",
]
