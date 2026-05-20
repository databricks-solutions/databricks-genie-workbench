"""Plan 11 — single dispatcher into the EXISTING per-PatchType validators.

This module adds NO new validation logic. It wires RepairProposal.patch_body
to the correct combination of:
  - Layer 1: applier.py per-entry Pydantic validators
  - Layer 2: metadata_snapshot asset-reference checks
  - Layer 3: benchmarks.validate_sql_snippet (3-phase pipeline)
  - Layer 4: instruction_publishability.validate_instruction_text

See design spec §4.1 for the full dispatch table.

The thin wrappers (``_validate_example_sql_entry``, ``validate_sql_snippet``,
``validate_instruction_text``) re-expose the underlying validators at the
new module's namespace so the Plan 11 repair loop can patch ALL validation
calls at one symbol-set (and the unit tests can mock them without reaching
into ``applier`` / ``benchmarks``).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.optimization.applier import (
    _validate_example_sql_entry as _applier_validate_example_sql_entry,
    _validate_join_spec_entry,
    _validate_sql_snippet_entry,
    validate_instruction_text as _applier_validate_instruction_text,
)
from genie_space_optimizer.optimization.benchmarks import (
    validate_sql_snippet as _benchmarks_validate_sql_snippet,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages.plan11_types import (
    ValidationError,
    ValidationResult,
)

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal

logger = logging.getLogger(__name__)


# ----- Thin wrappers to give all validators a (bool, list[str]) contract.
#
# applier._validate_example_sql_entry returns bool; the Plan 11 dispatcher
# unpacks (ok, msgs) so it can populate ValidationError.error_detail with
# structured messages. We can't change applier without a wider refactor —
# the wrapper preserves the existing behavior and only swaps the return
# shape.
#
# benchmarks.validate_sql_snippet returns (ok, normalized, err); we drop
# the third tuple element here because the dispatcher only needs (ok, msg).
# The wrappers also let unit tests patch the validation surface at a
# single namespace (this module) — the tests in test_plan11_validate_patch.py
# rely on this.

def _validate_example_sql_entry(
    entry: dict,
    config: dict | None = None,
) -> tuple[bool, list[str]]:
    """Plan 11 wrapper — applier._validate_example_sql_entry returns bool;
    we synthesize a (bool, list[str]) tuple so the dispatcher can attach
    a structured error_detail when validation fails.
    """
    ok = _applier_validate_example_sql_entry(entry, config)
    if ok:
        return True, []
    return False, ["example_sql entry failed applier Pydantic validation"]


def validate_sql_snippet(
    sql: str,
    snippet_type: str,
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Plan 11 wrapper — benchmarks.validate_sql_snippet returns
    (ok, normalized, err). The dispatcher only needs (ok, msg) where msg
    is the normalized SQL on success or the error string on failure.
    """
    ok, normalized, err = _benchmarks_validate_sql_snippet(
        sql=sql,
        snippet_type=snippet_type,
        metadata_snapshot=metadata_snapshot,
        spark=spark,
        catalog=catalog,
        gold_schema=gold_schema,
        w=w,
        warehouse_id=warehouse_id,
    )
    return ok, (normalized if ok else err)


def validate_instruction_text(text: str | list[str]) -> tuple[bool, list[str]]:
    """Plan 11 wrapper — pass-through onto
    applier.validate_instruction_text. Kept so the dispatcher and tests
    can patch validation at a single namespace.
    """
    return _applier_validate_instruction_text(text)


# PatchType arms that have no validator (trivial toggles).
_NO_VALIDATION_ARMS: frozenset[PatchType] = frozenset(
    {
        PatchType.ENABLE_EXAMPLE_VALUES,
        PatchType.DISABLE_EXAMPLE_VALUES,
        PatchType.ENABLE_VALUE_DICTIONARY,
        PatchType.DISABLE_VALUE_DICTIONARY,
    }
)

# PatchType arms that carry SQL requiring validate_sql_snippet.
_SQL_ARMS: dict[PatchType, str] = {
    PatchType.ADD_EXAMPLE_SQL: "example_sql",
    PatchType.UPDATE_EXAMPLE_SQL: "example_sql",
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: "expressions",
    PatchType.ADD_SQL_SNIPPET_FILTER: "filters",
    PatchType.ADD_SQL_SNIPPET_MEASURE: "measures",
    PatchType.UPDATE_TVF_SQL: "expressions",
    PatchType.ADD_DEFAULT_FILTER: "filters",
    PatchType.UPDATE_FILTER_CONDITION: "filters",
    PatchType.ADD_MV_MEASURE: "measures",
    PatchType.UPDATE_MV_MEASURE: "measures",
    PatchType.ADD_MV_DIMENSION: "measures",
}

# PatchType arms requiring instruction canonical check.
_INSTRUCTION_ARMS: frozenset[PatchType] = frozenset(
    {
        PatchType.ADD_INSTRUCTION,
        PatchType.UPDATE_INSTRUCTION,
        PatchType.UPDATE_INSTRUCTION_SECTION,
        PatchType.REWRITE_INSTRUCTION,
    }
)

# PatchType arms requiring join spec schema check.
_JOIN_ARMS: frozenset[PatchType] = frozenset(
    {
        PatchType.ADD_JOIN_SPEC,
        PatchType.UPDATE_JOIN_SPEC,
        PatchType.REMOVE_JOIN_SPEC,
    }
)

# PatchType arms requiring example SQL schema check.
_EXAMPLE_SQL_ARMS: frozenset[PatchType] = frozenset(
    {PatchType.ADD_EXAMPLE_SQL, PatchType.UPDATE_EXAMPLE_SQL}
)

# PatchType arms requiring sql_snippet schema check (matches the
# snippet_type vocabulary used by _validate_sql_snippet_entry).
_SNIPPET_ARMS: dict[PatchType, str] = {
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: "expressions",
    PatchType.ADD_SQL_SNIPPET_FILTER: "filters",
    PatchType.ADD_SQL_SNIPPET_MEASURE: "measures",
}

# PatchType arms requiring an asset-reference check on body.table /
# body.column.
_TABLE_COLUMN_REF_ARMS: frozenset[PatchType] = frozenset(
    {
        PatchType.ADD_DESCRIPTION,
        PatchType.UPDATE_DESCRIPTION,
        PatchType.ADD_COLUMN_DESCRIPTION,
        PatchType.UPDATE_COLUMN_DESCRIPTION,
        PatchType.ADD_TVF_DESCRIPTION,
        PatchType.HIDE_COLUMN,
        PatchType.UNHIDE_COLUMN,
        PatchType.RENAME_COLUMN_ALIAS,
        PatchType.ADD_COLUMN_SYNONYM,
        PatchType.REMOVE_COLUMN_SYNONYM,
    }
)


def validate_patch(
    patch: "RepairProposal",
    *,
    config: dict,
    metadata_snapshot: dict,
    spark: Any,
    w: Any,
    catalog: str,
    gold_schema: str,
    warehouse_id: str,
) -> ValidationResult:
    """Plan 11 — single dispatcher into the EXISTING per-PatchType validators.

    Adds NO new validators. Returns :class:`ValidationResult`; never raises.
    """
    errors: list[ValidationError] = []

    # Step 0: closed-vocabulary precheck — fail fast if patch_type is
    # unknown. The Plan 11 LLM emits patch_type as a string; an unknown
    # value is the most common contract failure and we don't want it to
    # propagate into the per-arm validators (where it would crash).
    raw_patch_type = (
        patch.patch_type.value
        if hasattr(patch.patch_type, "value")
        else str(patch.patch_type)
    )
    try:
        pt = PatchType(raw_patch_type)
    except ValueError as exc:
        return ValidationResult(
            patch_id=patch.intent_id,
            is_valid=False,
            errors=(
                ValidationError(
                    patch_id=patch.intent_id,
                    error_kind="patch_type_unknown",
                    error_detail=f"patch_type={raw_patch_type!r}: {exc}",
                    failing_location="patch_type",
                ),
            ),
        )

    body = patch.patch_body or {}

    # Step 1a: no-validation trivial toggles.
    if pt in _NO_VALIDATION_ARMS:
        return ValidationResult(
            patch_id=patch.intent_id, is_valid=True, errors=(),
        )

    # Step 1b: Layer 1 — per-PatchType applier Pydantic validators.
    if pt in _EXAMPLE_SQL_ARMS:
        ok, msgs = _validate_example_sql_entry(body, config)
        if not ok:
            for msg in msgs:
                errors.append(
                    ValidationError(
                        patch_id=patch.intent_id,
                        error_kind="genie_schema",
                        error_detail=str(msg)[:2048],
                        failing_location="patch_body",
                    )
                )
    elif pt in _JOIN_ARMS:
        ok = _validate_join_spec_entry(body)
        if not ok:
            errors.append(
                ValidationError(
                    patch_id=patch.intent_id,
                    error_kind="genie_schema",
                    error_detail="join_spec entry failed Pydantic validation",
                    failing_location="patch_body",
                )
            )
            # Asset-reference fan-out for join LEFT/RIGHT sides.
            known_tables = {
                t.get("name", "") for t in metadata_snapshot.get("tables", [])
            }
            for side in ("left", "right"):
                ref = body.get(side, "")
                table = ref.split(".")[0] if "." in ref else ref
                if table and known_tables and table not in known_tables:
                    errors.append(
                        ValidationError(
                            patch_id=patch.intent_id,
                            error_kind="asset_reference",
                            error_detail=(
                                f"join {side!r} table {table!r} not in "
                                "metadata_snapshot"
                            ),
                            failing_location=f"patch_body.{side}",
                        )
                    )
    elif pt in _SNIPPET_ARMS:
        snippet_type = _SNIPPET_ARMS[pt]
        ok = _validate_sql_snippet_entry(body, snippet_type)
        if not ok:
            errors.append(
                ValidationError(
                    patch_id=patch.intent_id,
                    error_kind="genie_schema",
                    error_detail=(
                        f"sql_snippet entry ({snippet_type}) failed Pydantic "
                        "validation"
                    ),
                    failing_location="patch_body",
                )
            )
    elif pt in _INSTRUCTION_ARMS:
        instruction_text = body.get("instruction_text", "")
        if not instruction_text:
            errors.append(
                ValidationError(
                    patch_id=patch.intent_id,
                    error_kind="patch_body_missing_field",
                    error_detail=(
                        "patch_body.instruction_text is required for "
                        "instruction patch types"
                    ),
                    failing_location="patch_body.instruction_text",
                )
            )
        else:
            ok, msgs = validate_instruction_text(instruction_text)
            if not ok:
                for msg in msgs:
                    errors.append(
                        ValidationError(
                            patch_id=patch.intent_id,
                            error_kind="instruction_canonical",
                            error_detail=str(msg)[:2048],
                            failing_location="patch_body.instruction_text",
                        )
                    )

    # Step 2: Layer 2 — asset-reference checks for description/column
    # patch types.
    if pt in _TABLE_COLUMN_REF_ARMS:
        table_ref = body.get("table", "")
        column_ref = body.get("column", "")
        known_tables = {
            t.get("name", "") for t in metadata_snapshot.get("tables", [])
        }
        known_columns = {
            c.get("name", "") for c in metadata_snapshot.get("columns", [])
        }
        if table_ref and known_tables and table_ref not in known_tables:
            errors.append(
                ValidationError(
                    patch_id=patch.intent_id,
                    error_kind="asset_reference",
                    error_detail=(
                        f"table {table_ref!r} not found in metadata_snapshot"
                    ),
                    failing_location="patch_body.table",
                )
            )
        if column_ref and known_columns and column_ref not in known_columns:
            errors.append(
                ValidationError(
                    patch_id=patch.intent_id,
                    error_kind="asset_reference",
                    error_detail=(
                        f"column {column_ref!r} not found in metadata_snapshot"
                    ),
                    failing_location="patch_body.column",
                )
            )

    # Step 3: Layer 3 — SQL execution for SQL-carrying patch types.
    # Skip if Layer 1/2 already rejected — running SQL against an invalid
    # body would just produce duplicate noise in the validator-error
    # payload sent to the repair LLM.
    if pt in _SQL_ARMS and not errors:
        sql_field = "example_sql" if pt in _EXAMPLE_SQL_ARMS else "sql_expression"
        sql = body.get(sql_field) or body.get("sql", "")
        if sql:
            try:
                ok, msg = validate_sql_snippet(
                    sql=sql,
                    snippet_type=_SQL_ARMS[pt],
                    metadata_snapshot=metadata_snapshot,
                    spark=spark,
                    w=w,
                    catalog=catalog,
                    gold_schema=gold_schema,
                    warehouse_id=warehouse_id,
                )
                if not ok:
                    errors.append(
                        ValidationError(
                            patch_id=patch.intent_id,
                            error_kind="sql_execution",
                            error_detail=(
                                f"validate_sql_snippet rejected: {msg!r}"
                            )[:2048],
                            failing_location=sql_field,
                        )
                    )
            except Exception as exc:
                errors.append(
                    ValidationError(
                        patch_id=patch.intent_id,
                        error_kind="sql_execution",
                        error_detail=(
                            f"validate_sql_snippet raised: {exc!r}"
                        )[:2048],
                        failing_location=sql_field,
                    )
                )

    return ValidationResult(
        patch_id=patch.intent_id,
        is_valid=not errors,
        errors=tuple(errors),
    )
