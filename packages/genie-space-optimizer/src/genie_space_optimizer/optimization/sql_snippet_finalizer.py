"""Plan 9 Task 6.1 — finalize ADD_SQL_SNIPPET_* proposal dicts.

Bridges RepairProposal.to_proposal_dict()'s flat shape to the
nested sql_snippet shape applier.py:3162 reads, and stamps
validation_passed after running validate_sql_snippet.

Lift-and-shift from _generate_lever6_proposal_legacy_body
(optimizer.py:14323-14468) — fields named identically so the
applier renders the Plan-9 direct path the same way it renders
the legacy body's output.

Only called for ADD_SQL_SNIPPET_{MEASURE, FILTER, EXPRESSION}.
Other patch types do not need finalization (no applier hard
assertion + no validate_sql_snippet step).
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
)

logger = logging.getLogger(__name__)


_PATCH_TYPE_TO_SNIPPET_TYPE = {
    PatchType.ADD_SQL_SNIPPET_MEASURE: "measure",
    PatchType.ADD_SQL_SNIPPET_FILTER: "filter",
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: "expression",
}


def _first_table_identifier(proposal: RepairProposal) -> str:
    for t in proposal.target_objects:
        if t.asset_kind == AssetKind.TABLE:
            return t.identifier
    # Fallback to the first blame_set entry that looks like a table.
    for b in proposal.blame_set:
        if "." in str(b):
            return str(b)
    return ""


def _snippet_id_for(proposal: RepairProposal, sql_expression: str) -> str:
    """Stable snippet id — hash of (intent_id, sql) so re-emissions
    are idempotent and the applier can locate it for rollback."""
    h = hashlib.sha256()
    h.update(proposal.intent_id.encode("utf-8"))
    h.update(b"\0")
    h.update(sql_expression.encode("utf-8"))
    # 32-char lowercase hex to satisfy Genie's serialized_space ID
    # validation (genie_schema._validate_id_format). Pinned to
    # producer_snippet_validator._mint_snippet_id so both producers
    # mint the same id for the same (intent_id, sql) pair.
    return h.hexdigest()[:32]


def finalize_sql_snippet_proposal_dict(
    proposal: RepairProposal,
    base_dict: dict[str, Any],
    *,
    cluster: dict,
    metadata_snapshot: dict,
    w: Any,
    spark: Any,
    catalog: str,
    gold_schema: str,
    warehouse_id: str,
) -> dict[str, Any] | None:
    """Wrap base_dict (flat to_proposal_dict() output) into the
    nested applier-expected shape and stamp validation fields.

    Returns None when the SQL fails identifier validation; caller
    treats this as a decline → falls through to safety-net legacy
    generator.
    """
    if proposal.patch_type not in _PATCH_TYPE_TO_SNIPPET_TYPE:
        # Defensive — caller should only invoke for these types.
        return dict(base_dict)

    snippet_type = _PATCH_TYPE_TO_SNIPPET_TYPE[proposal.patch_type]
    sql_expression = str(base_dict.get("sql_expression", ""))
    name = str(base_dict.get("name", ""))
    usage_guidance = str(base_dict.get("usage_guidance", ""))

    if not sql_expression or not name:
        logger.warning(
            "plan9.finalizer.missing_field intent_id=%s name_empty=%s sql_empty=%s",
            proposal.intent_id, not name, not sql_expression,
        )
        return None

    # Identifier validation against the metadata allowlist (cheap,
    # no backend required). Mirrors legacy body line 14349.
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist,
        _validate_sql_identifiers,
    )
    id_allowlist = _build_identifier_allowlist(metadata_snapshot)
    sql_ok, violations = _validate_sql_identifiers(
        sql_expression, id_allowlist,
    )
    if not sql_ok:
        logger.warning(
            "plan9.finalizer.identifier_validation_failed intent_id=%s "
            "violations=%s — treating as decline.",
            proposal.intent_id, violations,
        )
        return None

    target_table = _first_table_identifier(proposal)
    cluster_id = str(cluster.get("cluster_id", "?"))

    # EXPLAIN + execute validation. Mirrors legacy body lines 14358-14392.
    validation_passed = False
    if spark is not None or (w is not None and warehouse_id):
        from genie_space_optimizer.optimization.benchmarks import (
            validate_sql_snippet,
        )
        valid_result = validate_sql_snippet(
            sql_expression, snippet_type, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        )
        if not valid_result[0]:
            logger.info(
                "plan9.finalizer.validate_sql_snippet FAILED "
                "cluster_id=%s kind=%s target=%s reason=%s",
                cluster_id, snippet_type,
                target_table or "n/a", valid_result[1],
            )
            return None
        # Some validators rewrite the SQL (e.g. canonicalize) — use it.
        sql_expression = valid_result[2] if len(valid_result) > 2 else sql_expression
        validation_passed = True
        logger.info(
            "plan9.finalizer.validate_sql_snippet PASSED "
            "cluster_id=%s kind=%s target=%s",
            cluster_id, snippet_type, target_table or "n/a",
        )
    else:
        logger.info(
            "plan9.finalizer.validate_sql_snippet SKIPPED (no backend) "
            "cluster_id=%s kind=%s — applier-gate will drop the patch.",
            cluster_id, snippet_type,
        )

    snippet_id = _snippet_id_for(proposal, sql_expression)
    patch_type_to_applier = {
        PatchType.ADD_SQL_SNIPPET_MEASURE: "add_sql_snippet_measure",
        PatchType.ADD_SQL_SNIPPET_FILTER: "add_sql_snippet_filter",
        PatchType.ADD_SQL_SNIPPET_EXPRESSION: "add_sql_snippet_expression",
    }
    return {
        "patch_type": patch_type_to_applier[proposal.patch_type],
        "lever": 6,
        "snippet_type": snippet_type,
        "display_name": proposal.intent_name or name,
        "alias": "",
        "sql": sql_expression,
        "synonyms": [],
        "instruction": usage_guidance or proposal.rationale,
        "target_table": target_table,
        "rationale": proposal.rationale,
        "affected_questions": list(
            str(q) for q in (cluster.get("question_ids") or [])
        ),
        "confidence": proposal.confidence,
        "questions_fixed": len(cluster.get("question_traces", []) or []),
        "validation_passed": validation_passed,
        # Nested sql_snippet object — applier.py:3162 reads this.
        "sql_snippet": {
            "id": snippet_id,
            "name": name,
            "sql": sql_expression,
            "type": snippet_type,
            "description": usage_guidance or proposal.rationale,
        },
    }
