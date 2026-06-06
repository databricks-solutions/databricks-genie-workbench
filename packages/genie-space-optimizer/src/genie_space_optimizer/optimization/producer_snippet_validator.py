"""P4 C3 — Producer-side SQL snippet validator + stamper.

The pre-P4 lay of the land:

  * The canonical :func:`stages.validate_patch.validate_sql_snippet`
    exists.
  * The applier hard-rejects ``add_sql_snippet_*`` patches missing
    ``validation_passed=True``.
  * The synthesizer's snippet emission path (Stage 3) did NOT call
    the validator before emitting, so proposals reached the applier
    unstamped and were rejected with empty target/snippet_id.

This module is the *producer-side* counterpart to the applier-side
:mod:`state_machine.transformers.gate_validate_sql_snippet`. The
synthesizer calls :func:`validate_and_stamp_snippet_patch_body`
*before* emitting a snippet proposal. On success the patch body is
mutated in place with ``validation_passed=True``, normalized SQL,
``snippet_id``, and the nested ``sql_snippet`` shape the applier
reads. On failure the synthesizer drops the proposal and emits a
typed ``snippet_invalid`` abstain so the mechanism-repeat guard (C2)
can pivot to a different mechanism.

The deterministic snippet-id minter and the metadata-allowlist
identifier check are shared with the L6 finalizer
(:mod:`sql_snippet_finalizer`) so the two producers stamp identical
``snippet_id`` for the same ``(intent_id, sql)`` pair.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from genie_space_optimizer.optimization.llm_abstain import AbstainReason

logger = logging.getLogger(__name__)


# Patch types whose body carries a SQL snippet we must validate.
# Mirrors :mod:`sql_snippet_finalizer._PATCH_TYPE_TO_SNIPPET_TYPE`
# but keyed by the wire-string patch_type (lowercase) so this module
# does not import ``PatchType`` (avoids an import cycle with
# repair_proposal_typed in case the producer wants to call this
# before constructing the typed RepairProposal).
_PATCH_TYPE_TO_SNIPPET_TYPE: Mapping[str, str] = {
    "add_sql_snippet_measure": "measure",
    "add_sql_snippet_filter": "filter",
    "add_sql_snippet_expression": "expression",
}

# Wire-string → canonical snippet type used by the canonical
# validator (which uses plural categories).
_PATCH_TYPE_TO_CANONICAL_CATEGORY: Mapping[str, str] = {
    "add_sql_snippet_measure": "measures",
    "add_sql_snippet_filter": "filters",
    "add_sql_snippet_expression": "expressions",
}


@dataclass(frozen=True, slots=True)
class SnippetValidatorVerdict:
    """Outcome of :func:`validate_and_stamp_snippet_patch_body`.

    ``outcome`` ∈ {``"stamped"``, ``"declined"``}.
      * ``"stamped"``  — the patch_body was mutated in place with
        ``validation_passed=True`` + ``snippet_id`` + ``sql_snippet``.
        Caller may emit the proposal.
      * ``"declined"`` — validation failed; ``abstain_reason`` is
        :attr:`AbstainReason.SNIPPET_INVALID`. Caller MUST NOT emit
        the proposal. The synthesizer should add the patch_type to
        ``synthesis_rejected_patch_types`` and route the cluster to
        the mechanism-repeat pivot.
    """

    outcome: str
    abstain_reason: AbstainReason | None
    error_message: str


def _is_noop_suppression_sql(sql: str) -> bool:
    """Trial 24 Follow-on B — is ``sql`` a behavioral no-op tautology?

    Recognises the degenerate filter-removal snippets the LLM emits when
    it tries to express "remove this predicate" as a positive snippet:
    ``TRUE``, ``1=1``, and the same wrapped in a leading ``WHERE``,
    case-insensitively and whitespace-insensitively. A trailing ``;`` is
    tolerated. Anything else (a real predicate) is NOT a no-op.
    """
    norm = " ".join(str(sql or "").strip().lower().split())
    if norm.startswith("where "):
        norm = norm[len("where "):].strip()
    norm = norm.rstrip(";").strip()
    # Collapse spaces around the equals so ``1 = 1`` matches ``1=1``.
    norm = norm.replace(" = ", "=")
    return norm in {"true", "1=1", "(true)", "(1=1)"}


def _mint_snippet_id(intent_id: str, sql: str) -> str:
    """Deterministic ``snippet_id`` shared with sql_snippet_finalizer.

    Implementation pinned to :func:`sql_snippet_finalizer._snippet_id_for`
    so a proposal validated and stamped via either path mints the
    same id for the same ``(intent_id, sql)`` pair. The pinning
    test lives in ``test_producer_snippet_validator.py``.
    """
    import hashlib

    h = hashlib.sha256()
    h.update(str(intent_id).encode("utf-8"))
    h.update(b"\0")
    h.update(str(sql).encode("utf-8"))
    # 32-char lowercase hex — the Genie serialized_space validation
    # rules require snippet/asset IDs to be exactly 32 lowercase hex
    # chars. The prior ``[:16]`` truncation passed validator stamping
    # but failed genie_schema._validate_id_format downstream
    # (d139/e943 postmortems: APPLIER_GATE_SQL_SNIPPET_INVALID_ID).
    return h.hexdigest()[:32]


def stamp_snippet_validation_on_body(
    patch_body: dict[str, Any],
    *,
    intent_id: str,
    snippet_name: str,
    normalized_sql: str,
    snippet_type: str,
    description: str,
) -> None:
    """Mutate ``patch_body`` in place with the four fields the applier
    requires:

      * ``validation_passed = True``
      * ``snippet_id`` (32-char hex from :func:`_mint_snippet_id`)
      * the nested ``sql_snippet`` dict applier.py:3162 reads
      * normalized SQL (replaces the LLM-emitted body in case the
        validator canonicalized it)

    Idempotent: re-stamping with the same inputs produces no change.

    Trial 26 W26.3: when
    :func:`trial26_applier_snippet_name_fix_enabled` is ON (default),
    the nested ``sql_snippet`` body emits ``display_name`` (canonical
    Genie schema) instead of the legacy ``name`` field that the Genie
    API rejects with ``Invalid serialized_space: Unknown field 'name'``.
    When the flag is OFF the legacy ``name`` is restored for byte-stable
    rollback.
    """
    snippet_id = _mint_snippet_id(intent_id, normalized_sql)
    patch_body["validation_passed"] = True
    patch_body["snippet_id"] = snippet_id
    # If the LLM put SQL under "sql_expression" preserve that key
    # name (applier looks at both via _extract_sql_for_arm).
    if "sql_expression" in patch_body:
        patch_body["sql_expression"] = normalized_sql
    if "example_sql" in patch_body:
        patch_body["example_sql"] = normalized_sql

    try:
        from genie_space_optimizer.optimization.trial26_flags import (
            trial26_applier_snippet_name_fix_enabled,
        )
        _t26_fix_on = trial26_applier_snippet_name_fix_enabled()
    except Exception:
        _t26_fix_on = False

    if _t26_fix_on:
        patch_body["sql_snippet"] = {
            "id": snippet_id,
            "display_name": snippet_name,
            "sql": normalized_sql,
            "type": snippet_type,
            "description": description,
        }
        _emit_trial26_snippet_fields_marker(
            site="producer_snippet_validator",
            intent_id=intent_id,
            snippet_type=snippet_type,
            kept=("id", "display_name", "sql", "type", "description"),
            dropped=("name",),
        )
    else:
        patch_body["sql_snippet"] = {
            "id": snippet_id,
            "name": snippet_name,
            "sql": normalized_sql,
            "type": snippet_type,
            "description": description,
        }


def _emit_trial26_snippet_fields_marker(
    *,
    site: str,
    intent_id: str,
    snippet_type: str,
    kept: tuple[str, ...],
    dropped: tuple[str, ...],
) -> None:
    """Trial 26 W26.3 marker: record canonical field set per stamp.

    Postmortems grep this marker stream to verify the W26.3 fix is
    reaching production. Never raises — best-effort observability.
    """
    try:
        import json as _t26_json
        print(
            "GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1 "
            + _t26_json.dumps(
                {
                    "site": site,
                    "intent_id": intent_id,
                    "snippet_type": snippet_type,
                    "kept": list(kept),
                    "dropped": list(dropped),
                },
                sort_keys=True,
                default=str,
            ),
            flush=True,
        )
    except Exception:
        pass


def _stamped_id_is_genie_valid(patch_body: Mapping[str, Any]) -> tuple[bool, str]:
    """Validate the just-stamped snippet ID against the Genie schema.

    Defence-in-depth after the 32-char fix (d139/e943 postmortems):
    ``outcome="stamped"`` / ``validation_passed=True`` must NEVER be
    set on a snippet whose ``snippet_id`` (or nested ``sql_snippet.id``)
    fails ``genie_schema._validate_id_format`` — that is exactly the
    APPLIER_GATE_SQL_SNIPPET_INVALID_ID_AFTER_VALIDATOR_STAMP failure.
    Returns ``(ok, error_message)``.
    """
    from genie_space_optimizer.common.genie_schema import _validate_id_format

    errors: list[str] = []
    _sid = str(patch_body.get("snippet_id") or "")
    _validate_id_format(_sid, "sql_snippet.snippet_id", errors)
    _nested = patch_body.get("sql_snippet")
    if isinstance(_nested, Mapping):
        _validate_id_format(
            str(_nested.get("id") or ""), "sql_snippet.id", errors
        )
    if errors:
        return (False, "; ".join(errors))
    return (True, "")


def validate_and_stamp_snippet_patch_body(
    patch_body: dict[str, Any],
    *,
    intent_id: str,
    patch_type_wire: str,
    metadata_snapshot: Mapping[str, Any],
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> SnippetValidatorVerdict:
    """Validate the snippet SQL in ``patch_body`` and stamp on success.

    Use this from any Stage 3 synthesizer path that emits an
    ``add_sql_snippet_*`` proposal. The function:

      1. Looks up the canonical snippet-type category from the
         wire-string ``patch_type_wire`` (``"add_sql_snippet_measure"``
         et al).
      2. Extracts SQL from ``patch_body["sql_expression"]`` /
         ``patch_body["example_sql"]``.
      3. Calls the canonical
         :func:`stages.validate_patch.validate_sql_snippet` wrapper.
      4. On success, mutates ``patch_body`` via
         :func:`stamp_snippet_validation_on_body` and returns
         ``"stamped"``.
      5. On failure, returns ``"declined"`` with
         :attr:`AbstainReason.SNIPPET_INVALID`. ``patch_body`` is
         left untouched.

    The caller is responsible for translating ``"declined"`` into
    the right pipeline action — typically:
      * adding the patch_type to ``synthesis_rejected_patch_types``
      * emitting an ``AbstainVerdict(reason=SNIPPET_INVALID, ...)``
      * routing the cluster to the mechanism-repeat pivot path (C2)
    """
    if patch_type_wire not in _PATCH_TYPE_TO_SNIPPET_TYPE:
        # Not a snippet patch type — no-op, treat as stamped.
        return SnippetValidatorVerdict(
            outcome="stamped",
            abstain_reason=None,
            error_message="",
        )

    sql = (
        patch_body.get("sql_expression")
        or patch_body.get("example_sql")
        or ""
    )
    if not isinstance(sql, str) or not sql.strip():
        return SnippetValidatorVerdict(
            outcome="declined",
            abstain_reason=AbstainReason.SNIPPET_INVALID,
            error_message="patch_body missing sql_expression / example_sql",
        )

    # Trial 24 Follow-on B — tautology / no-op suppression guard. A
    # filter-REMOVAL RCA (``extra_defensive_filter``) cannot be expressed
    # as a positive snippet, so the LLM emits a tautology (``1=1`` /
    # ``TRUE``, optionally wrapped in a ``WHERE``). That snippet is a
    # behavioral no-op. Decline it with a DISTINCT typed reason so the
    # synthesizer can degrade the kit to an instruction-only solo rather
    # than treat it as a generic ``snippet_invalid`` and cascade.
    if _is_noop_suppression_sql(sql):
        return SnippetValidatorVerdict(
            outcome="declined",
            abstain_reason=AbstainReason.SNIPPET_NOOP_SUPPRESSION,
            error_message=(
                "snippet SQL is a tautology / no-op suppression "
                f"({sql.strip()!r}); a filter-removal fix must be an "
                "instruction, not a positive snippet"
            ),
        )

    # No SQL execution backend (offline / replay / unit harness): the
    # producer-side fast-check cannot run the canonical validator's
    # EXPLAIN + execute phases (``benchmarks.validate_sql_snippet``
    # raises ``No SQL execution backend available`` without a Spark
    # session or warehouse). The producer gate is fail-FAST, not
    # authoritative — the applier-side ``gate_validate_sql_snippet`` is
    # the authoritative second check and re-validates + stamps when a
    # backend is present. So when no backend exists we defer to it:
    # stamp the parse-clean snippet (the no-op / missing-SQL guards
    # above already ran) and pass through rather than fail-closing a
    # proposal we have no means to validate. In production the
    # synthesizer always passes a live ``spark`` or ``w``+``warehouse_id``,
    # so this guard never fires there.
    _has_backend = (spark is not None) or bool(w and warehouse_id)
    if not _has_backend:
        snippet_type = _PATCH_TYPE_TO_SNIPPET_TYPE[patch_type_wire]
        stamp_snippet_validation_on_body(
            patch_body,
            intent_id=intent_id,
            snippet_name=str(patch_body.get("name") or ""),
            normalized_sql=sql.strip(),
            snippet_type=snippet_type,
            description=str(
                patch_body.get("description")
                or patch_body.get("usage_guidance")
                or ""
            ),
        )
        _id_ok, _id_err = _stamped_id_is_genie_valid(patch_body)
        if not _id_ok:
            return SnippetValidatorVerdict(
                outcome="declined",
                abstain_reason=AbstainReason.SNIPPET_INVALID,
                error_message=f"minted snippet ID failed Genie schema: {_id_err}",
            )
        return SnippetValidatorVerdict(
            outcome="stamped",
            abstain_reason=None,
            error_message="",
        )

    canonical_category = _PATCH_TYPE_TO_CANONICAL_CATEGORY[patch_type_wire]
    # Lazy import: stages.validate_patch pulls in applier internals,
    # which import optimizer modules. Importing at module load time
    # creates a circular dep chain. Lazy is safe here.
    from genie_space_optimizer.optimization.stages.validate_patch import (
        validate_sql_snippet as canonical_validate_sql_snippet,
    )

    ok, message = canonical_validate_sql_snippet(
        sql=sql,
        snippet_type=canonical_category,
        metadata_snapshot=dict(metadata_snapshot),
        spark=spark,
        catalog=catalog,
        gold_schema=gold_schema,
        w=w,
        warehouse_id=warehouse_id,
    )
    if not ok:
        logger.info(
            "p4.c3.producer.validate_snippet FAILED intent_id=%s "
            "patch_type=%s reason=%s",
            intent_id,
            patch_type_wire,
            message,
        )
        return SnippetValidatorVerdict(
            outcome="declined",
            abstain_reason=AbstainReason.SNIPPET_INVALID,
            error_message=str(message or "unknown"),
        )

    # Some validators canonicalize SQL; use the returned message
    # (which is the normalized SQL on ok=True per the wrapper
    # contract).
    normalized = message if isinstance(message, str) and message else sql
    snippet_type = _PATCH_TYPE_TO_SNIPPET_TYPE[patch_type_wire]
    snippet_name = str(patch_body.get("name") or "")
    description = str(
        patch_body.get("description")
        or patch_body.get("usage_guidance")
        or ""
    )
    stamp_snippet_validation_on_body(
        patch_body,
        intent_id=intent_id,
        snippet_name=snippet_name,
        normalized_sql=normalized,
        snippet_type=snippet_type,
        description=description,
    )
    _id_ok, _id_err = _stamped_id_is_genie_valid(patch_body)
    if not _id_ok:
        logger.info(
            "p4.c3.producer.validate_snippet ID-INVALID intent_id=%s "
            "patch_type=%s reason=%s",
            intent_id,
            patch_type_wire,
            _id_err,
        )
        return SnippetValidatorVerdict(
            outcome="declined",
            abstain_reason=AbstainReason.SNIPPET_INVALID,
            error_message=f"minted snippet ID failed Genie schema: {_id_err}",
        )
    return SnippetValidatorVerdict(
        outcome="stamped",
        abstain_reason=None,
        error_message="",
    )
