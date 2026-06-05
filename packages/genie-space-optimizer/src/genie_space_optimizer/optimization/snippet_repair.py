"""Trial 23 W7 — snippet repair loop (repair, not drop).

``producer_snippet_validator.validate_and_stamp_snippet_patch_body``
returns ``declined`` (``AbstainReason.SNIPPET_INVALID``) when the
snippet SQL fails the canonical ``validate_sql_snippet`` wrapper. The
pre-W7 caller in ``stages/synthesize.py`` dropped the proposal
immediately and told the strategist to "pivot to a different
mechanism" — abandoning an almost-right SQL the validator could pinpoint
the defect in.

W7 re-prompts the LLM ONCE with the exact canonical validator error +
the resolved schema slice, re-validates the returned SQL, and drops
only if the repair attempt also fails. The corrective mechanism family
is preserved instead of abandoned.

This module is the *pure* payload/parse/apply + marker brain. The
synthesizer owns the flag gate, the LLM call, and the re-validation.
"""
from __future__ import annotations

import json
from collections.abc import Mapping

# Snippet patch types whose body carries SQL under ``sql_expression``;
# example patch types carry it under ``example_sql``. Mirrors the keys
# the producer validator reads (``sql_expression or example_sql``).
_EXAMPLE_PATCH_TYPES = frozenset({"add_example_sql", "update_example_sql"})


def _sql_key_for(patch_type_wire: str) -> str:
    return (
        "example_sql"
        if str(patch_type_wire or "").lower() in _EXAMPLE_PATCH_TYPES
        else "sql_expression"
    )


def build_snippet_repair_payload(
    *,
    patch_type_wire: str,
    patch_body: Mapping,
    validator_error: str,
    schema_slice: Mapping | None,
) -> dict:
    """Build the user-prompt payload for the single repair re-prompt.

    Carries the invalid body, the EXACT canonical validator error, and
    the resolved schema slice so the LLM can fix the SQL against assets
    that exist instead of guessing. The instruction pins the contract:
    re-emit ONE proposal of the SAME patch_type with corrected SQL.
    """
    return {
        "task": "repair_invalid_sql_snippet",
        "patch_type": str(patch_type_wire),
        "invalid_patch_body": dict(patch_body or {}),
        "validator_error": str(validator_error or ""),
        "schema_slice": schema_slice or {},
        "instructions": (
            "The SQL in invalid_patch_body failed validation with "
            "validator_error. Re-emit EXACTLY ONE proposal of the SAME "
            "patch_type with the SQL corrected to pass validation. Anchor "
            "every table/column to schema_slice; do not invent names. Do "
            "NOT change the patch_type or switch mechanisms — fix the SQL."
        ),
    }


def extract_repaired_sql(parsed_output: object) -> str:
    """Extract the repaired SQL from a Plan11-synthesize-shaped output.

    Reads ``proposals[0].patch_body.sql_expression`` (falling back to
    ``example_sql``). Returns ``""`` on any missing/garbage shape so the
    caller treats it as a failed repair.
    """
    if not isinstance(parsed_output, dict):
        return ""
    proposals = parsed_output.get("proposals") or []
    if not proposals or not isinstance(proposals[0], dict):
        return ""
    body = proposals[0].get("patch_body") or {}
    if not isinstance(body, dict):
        return ""
    return str(body.get("sql_expression") or body.get("example_sql") or "")


def apply_repaired_sql(
    patch_body: Mapping,
    repaired_sql: str,
    *,
    patch_type_wire: str,
) -> dict:
    """Return a COPY of ``patch_body`` with the repaired SQL set on the
    key the validator reads for this patch type (no in-place mutation).
    """
    new = dict(patch_body or {})
    new[_sql_key_for(patch_type_wire)] = str(repaired_sql or "")
    return new


def snippet_repair_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    intent_id: str,
    patch_type: str,
    outcome: str,
    validator_error: str = "",
) -> str:
    """Build the ``GSO_TRIAL23_SNIPPET_REPAIR_V1`` marker line.

    ``outcome`` ∈ {``"repaired"`` (re-validation passed),
    ``"repair_failed"`` (re-validation still declined → dropped),
    ``"repair_no_sql"`` (LLM returned no usable SQL → dropped)}.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "intent_id": str(intent_id),
        "patch_type": str(patch_type),
        "outcome": str(outcome),
        "validator_error": str(validator_error or "")[:200],
    }
    return (
        "GSO_TRIAL23_SNIPPET_REPAIR_V1 "
        + json.dumps(payload, sort_keys=True)
    )


__all__ = [
    "build_snippet_repair_payload",
    "extract_repaired_sql",
    "apply_repaired_sql",
    "snippet_repair_marker",
]
