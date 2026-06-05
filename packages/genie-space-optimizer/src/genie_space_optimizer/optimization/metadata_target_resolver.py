"""P4 C4 — Shared metadata-patch target resolver.

Pre-P4 the only consumer of :func:`patch_applyability.check_patch_applyability`
for metadata patches was the applier-side preflight inside
:func:`state_machine.transformers.applier_gate._preflight_metadata_patch`.
The e943 postmortem showed metadata patches reaching the applier
unresolvable, the preflight rejecting them at apply-time, and the
producer (Stage 3) emitting the proposal with no target information.
The producer never resolved the target table/column on its side, so
the LLM kept emitting the same uncanonical body across iterations.

This module is the *shared* helper. Both the producer (Stage 3) and
the applier-side preflight call :func:`resolve_metadata_patch_target`.
On a resolved target the producer stamps ``target_resolved=True`` on
the patch_body so the applier-side preflight short-circuits the
expensive ``check_patch_applyability`` call. On an unresolved target
the producer aborts emission with abstain
``target_unresolvable``.

Patch types covered (all column- or table-touching metadata edits):
  * ``update_column_description``
  * ``add_column_description``
  * ``add_column_synonym``
  * ``remove_column_synonym``
  * ``add_description`` / ``update_description``
  * ``hide_column`` / ``unhide_column``
  * ``rename_column_alias``
  * ``add_join_spec`` / ``update_join_spec`` / ``remove_join_spec``

Wire-string patch types match the applier dispatch identifiers
(:class:`PatchType` lowercase values).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from genie_space_optimizer.optimization.llm_abstain import AbstainReason


# Patch types whose body carries a ``table`` or ``object_id`` target
# that must canonicalize against the metadata snapshot.
METADATA_PATCH_TYPES_WITH_TARGETS: frozenset[str] = frozenset(
    {
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "remove_column_synonym",
        "hide_column",
        "unhide_column",
        "rename_column_alias",
        "add_description",
        "update_description",
        "add_join_spec",
        "update_join_spec",
        "remove_join_spec",
    }
)


@dataclass(frozen=True, slots=True)
class TargetResolverVerdict:
    """Outcome of :func:`resolve_metadata_patch_target`.

    ``outcome`` ∈ {``"resolved"``, ``"unresolvable"``, ``"skipped"``}.

      * ``"resolved"``     — target canonicalized; ``resolved_table``
        and ``resolved_column`` (when applicable) are non-empty.
        Caller MUST stamp ``target_resolved=True`` on the body via
        :func:`stamp_target_resolved_on_body` (or accept the in-place
        stamp this function performs when ``stamp=True``).
      * ``"unresolvable"`` — target table/column does not exist in
        the metadata snapshot. ``abstain_reason`` is
        :attr:`AbstainReason.TARGET_UNRESOLVABLE`; caller must
        decline the proposal.
      * ``"skipped"``      — patch type is not metadata-shaped, or
        the snapshot was empty, or the body uses an opaque
        ``object_id`` encoding the applier-side dispatch parses
        natively. Caller treats this as no-op (fall through).
    """

    outcome: str
    abstain_reason: AbstainReason | None
    error_message: str
    resolved_table: str = ""
    resolved_column: str = ""


def stamp_target_resolved_on_body(
    patch_body: dict[str, Any],
    *,
    resolved_table: str,
    resolved_column: str = "",
) -> None:
    """Stamp ``target_resolved=True`` + canonical ``table`` / ``column``
    fields on the body. Idempotent.

    W7: the canonical ``resolved_table`` (FQN) always wins over any
    bare-name value the LLM may have emitted, so downstream consumers
    see the resolved identifier even when the body initially carried
    a bare table name.
    """
    patch_body["target_resolved"] = True
    if resolved_table:
        patch_body["table"] = resolved_table
    if (
        resolved_column
        and not str(patch_body.get("column") or "").strip()
    ):
        patch_body["column"] = resolved_column


def _canonicalize_bare_table_name(
    raw_table: str,
    *,
    metadata_snapshot: Mapping[str, Any],
) -> str:
    """Resolve a bare table name to its FQN identifier via snapshot lookup.

    Returns the input unchanged when the input is already FQN-looking
    (contains a ``.``) OR when the bare-name lookup is ambiguous or has
    no match. Only resolves an unambiguous single match — anything else
    is left for the applier-side check to handle with full error context.

    Operates on the same ``data_sources.{tables, metric_views}`` lists
    that :func:`applier._find_table_in_config` scans, so the resolver and
    the applier agree on which identifiers count as known.
    """
    candidate = (raw_table or "").strip()
    if not candidate or "." in candidate:
        return candidate

    ds = (metadata_snapshot or {}).get("data_sources") or {}
    matches: list[str] = []
    for source_key in ("tables", "metric_views"):
        for entry in ds.get(source_key) or []:
            if not isinstance(entry, dict):
                continue
            identifier = str(entry.get("identifier") or "").strip()
            if not identifier:
                continue
            trailing = identifier.rsplit(".", 1)[-1]
            if trailing == candidate:
                matches.append(identifier)

    if len(matches) == 1:
        return matches[0]
    return candidate


def resolve_metadata_patch_target(
    patch_body: Mapping[str, Any],
    *,
    patch_type_wire: str,
    metadata_snapshot: Mapping[str, Any],
    space_id: str = "",
    stamp: bool = False,
) -> TargetResolverVerdict:
    """Resolve the metadata-patch target via
    :func:`patch_applyability.check_patch_applyability`.

    When ``stamp=True`` and the verdict is ``"resolved"``,
    ``patch_body`` is mutated in place via
    :func:`stamp_target_resolved_on_body`. When ``stamp=False`` the
    caller is responsible for stamping.

    The skip cases (no-snapshot / opaque object_id / non-metadata
    patch type) mirror the pre-P4 applier-side preflight semantics so
    routing through this helper does not regress on tests / synthetic
    replays that intentionally leave the snapshot unwired.
    """
    wire = str(patch_type_wire or "").lower()
    if wire not in METADATA_PATCH_TYPES_WITH_TARGETS:
        return TargetResolverVerdict(
            outcome="skipped",
            abstain_reason=None,
            error_message="not a metadata patch type",
        )

    if not metadata_snapshot:
        return TargetResolverVerdict(
            outcome="skipped",
            abstain_reason=None,
            error_message="no metadata snapshot wired",
        )

    raw_table = patch_body.get("table") or patch_body.get("target")
    target_table_str = ""
    if isinstance(raw_table, str):
        target_table_str = raw_table.strip()
    if not target_table_str:
        # Opaque ``object_id`` encoding (``{"object_id": "t:c"}``) —
        # let the applier-side dispatcher parse it natively. The
        # producer cannot canonicalize without knowing the encoding.
        return TargetResolverVerdict(
            outcome="skipped",
            abstain_reason=None,
            error_message="opaque target encoding (object_id form)",
        )

    # W7 fix — bare-name canonicalization.
    # Run B postmortem (d13938e7) showed the LLM emit
    # ``{"table": "mv_7now_store_sales"}`` while the deployed Genie
    # config stores the table as ``prashanth_subrahmanyam_catalog.
    # sales_reports.mv_7now_store_sales``. Strict identifier equality
    # in :func:`_find_table_in_config` missed and we returned
    # ``missing_table`` even though the table exists.
    #
    # Resolution policy:
    #   1. If the input is already FQN (contains a ``.`` or matches an
    #      identifier exactly), proceed unchanged.
    #   2. Else, search the snapshot for tables/metric_views whose
    #      identifier trailing component equals the bare name.
    #   3. If exactly one such table exists, rewrite the body's
    #      ``table`` to its identifier and proceed. If zero or more
    #      than one match, fall through and let applyability return
    #      the original ``missing_table`` verdict — we prefer
    #      ambiguous-bare-name failure over the wrong canonicalization.
    canonical_table = _canonicalize_bare_table_name(
        target_table_str,
        metadata_snapshot=metadata_snapshot,
    )
    pb = dict(patch_body)
    if canonical_table and canonical_table != target_table_str:
        pb["table"] = canonical_table
        target_table_str = canonical_table
    pb.setdefault("patch_type", wire)
    pb.setdefault("type", wire)

    # Lazy import — patch_applyability pulls applier internals.
    from genie_space_optimizer.optimization.patch_applyability import (
        check_patch_applyability,
    )

    decision = check_patch_applyability(
        patch=pb,
        metadata_snapshot=dict(metadata_snapshot),
        space_id=str(space_id or ""),
    )
    if decision.applyable:
        resolved_table = decision.table or target_table_str
        resolved_column = (
            str(patch_body.get("column") or "").strip()
            if isinstance(patch_body.get("column"), str)
            else ""
        )
        if stamp and isinstance(patch_body, dict):
            stamp_target_resolved_on_body(
                patch_body,
                resolved_table=resolved_table,
                resolved_column=resolved_column,
            )
        return TargetResolverVerdict(
            outcome="resolved",
            abstain_reason=None,
            error_message="",
            resolved_table=resolved_table,
            resolved_column=resolved_column,
        )

    if decision.reason in {
        "missing_table",
        "invalid_column_target",
        "missing_column",
    }:
        return TargetResolverVerdict(
            outcome="unresolvable",
            abstain_reason=AbstainReason.TARGET_UNRESOLVABLE,
            error_message=str(decision.reason),
            resolved_table=decision.table or target_table_str,
            resolved_column="",
        )

    # Other applyability failures (render_exception, apply_exception)
    # are not target-resolution problems — the applier-side gate
    # handles them with full context.
    return TargetResolverVerdict(
        outcome="skipped",
        abstain_reason=None,
        error_message=str(decision.reason or "unknown"),
    )


def validate_and_stamp_metadata_patch_target(
    patch_body: dict[str, Any],
    *,
    patch_type_wire: str,
    metadata_snapshot: Mapping[str, Any],
    space_id: str = "",
) -> TargetResolverVerdict:
    """Producer-side convenience wrapper.

    Calls :func:`resolve_metadata_patch_target` with ``stamp=True``.
    Use from Stage 3 synthesizer to canonicalize column-touching
    proposals BEFORE emitting them. On ``"unresolvable"`` the caller
    MUST NOT emit the proposal and should route the cluster to the
    mechanism-repeat pivot (C2) so the LLM tries a different
    mechanism instead of resubmitting the same unresolvable body
    next iteration.
    """
    return resolve_metadata_patch_target(
        patch_body,
        patch_type_wire=patch_type_wire,
        metadata_snapshot=metadata_snapshot,
        space_id=space_id,
        stamp=True,
    )
