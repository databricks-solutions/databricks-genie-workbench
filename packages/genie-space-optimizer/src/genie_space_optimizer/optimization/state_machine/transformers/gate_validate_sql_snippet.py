"""Phase 3 P3.1 — SINGLE entry point for SQL snippet validation.

Background — pre-P3.1 the optimizer had three bypass paths around the
canonical validator at ``stages.validate_patch.validate_sql_snippet``:

  1. ``stages/narrow_replacement.py`` — invoked the underlying
     ``benchmarks.validate_sql_snippet`` directly when re-shaping a
     blast-radius reject so the narrow patch could land without a
     second round-trip;
  2. ``stages/synthesize.py`` strategist fallback path — called
     ``_validate_sql_snippet_entry`` from the applier shape
     dispatcher instead of going through ``validate_patch``;
  3. ``applier.py`` hot-fix path — re-validated the patch_body just
     before dispatch, occasionally producing a contradiction with the
     earlier ``validate_patch`` verdict when the metadata snapshot
     drifted mid-iteration.

Each bypass had a defensible local rationale, but their existence
made it impossible to audit "did this patch see the canonical
validator?" without grepping three call sites. Postmortems on Trial
20 confirmed leakage: at least one ``add_sql_snippet_*`` patch landed
on the live Genie space without ever hitting Phase 3 of the snippet
pipeline.

P3.1 introduces this state-machine transformer as the SINGLE
admissibility gate. The gate predicate calls the canonical
``stages.validate_patch.validate_sql_snippet`` wrapper (which itself
delegates to ``benchmarks.validate_sql_snippet``) and emits the
verdict via a typed ``GateVerdict``. Downstream gates — including
narrow_replacement, the strategist fallback, and the applier — are
expected to consult the verdict stamped on ``ctx.extras`` rather
than re-running their own validator. The legacy direct calls remain
as functions on those modules for now (to preserve byte-stable
behaviour on disabled flag rollback), but the state-machine
orchestrator MUST route through this transformer to mint the
verdict that the legacy callers then short-circuit on.

The transformer does NOT itself mutate the proposal — validation is
a READ-only verdict. It writes the result into
``ctx.extras["_p3_1_validate_sql_snippet_results"]`` keyed by
``(intent_id, snippet_field)`` so concurrent gates can read the
verdict without re-invoking the SQL parser.
"""
from __future__ import annotations

from typing import Any, Mapping

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    ValidationGate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict,
    TransformerContext,
)
from genie_space_optimizer.optimization.stages.validate_patch import (
    _SQL_ARMS,
    validate_sql_snippet as canonical_validate_sql_snippet,
)


_RESULTS_KEY = "_p3_1_validate_sql_snippet_results"


def _extract_sql_for_arm(
    proposal: Any, snippet_type: str,
) -> tuple[str, str]:
    """Return ``(sql_text, body_field_name)`` for the given snippet
    arm, or ``("", "")`` when the proposal does not carry SQL we can
    validate.

    The body field names follow the conventions in
    :mod:`stages.validate_patch`:

      * ``example_sql``                → ``patch_body["example_sql"]``
      * ``expressions`` / ``filters`` /
        ``measures``                   → ``patch_body["sql_expression"]``

    Non-string / empty values short-circuit by returning ``("", "")``;
    the caller treats that as a no-op (the upstream validators have
    already required the field). This keeps the gate strictly
    additive — it never invents a new rejection.
    """
    body: Mapping[str, Any] = getattr(proposal, "patch_body", {}) or {}
    if snippet_type == "example_sql":
        raw = body.get("example_sql")
    else:
        raw = body.get("sql_expression")
    if not isinstance(raw, str) or not raw.strip():
        return ("", "")
    return (raw, "example_sql" if snippet_type == "example_sql" else "sql_expression")


def lookup_sql_snippet_verdict(
    ctx: TransformerContext,
    intent_id: str,
    snippet_field: str,
) -> tuple[bool, str] | None:
    """Phase 3 P3.1 — read the canonical sql-snippet verdict stamped
    on ``ctx.extras`` by :func:`_predicate`.

    Returns ``(ok, message)`` when the gate has run for this
    ``(intent_id, snippet_field)`` pair; returns ``None`` otherwise
    (the legacy direct validators in narrow_replacement / strategist
    fallback / applier hot-fix should then either call the canonical
    wrapper themselves or skip — the gate is the single source of
    truth, never the bypasses).
    """
    extras = getattr(ctx, "extras", None) or {}
    bucket = extras.get(_RESULTS_KEY)
    if not isinstance(bucket, dict):
        return None
    return bucket.get((intent_id, snippet_field))


def _predicate(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> GateVerdict:
    """Validate the latest proposal's SQL snippet via the canonical
    ``stages.validate_patch.validate_sql_snippet`` wrapper.

    Skip semantics:
      * No proposal on the state                       → pass-through
      * Proposal's ``patch_type`` not in ``_SQL_ARMS`` → pass-through
      * SQL field missing/blank                        → pass-through

    Failure terminates the qid with a typed
    ``snippet_validation_failed:<patch_type>:<reason>`` forbidden
    signature so the next iteration's strategist sees the
    incoherency directly. Success records the verdict and lets the
    state machine advance.
    """
    if not state.proposals:
        return GateVerdict.success(record=None)
    latest = state.proposals[-1]
    proposal = ctx.proposal_store.lookup(latest.intent_id)
    if proposal is None:
        return GateVerdict.success(record=None)

    patch_type_str = str(latest.patch_type or "")
    try:
        patch_type = PatchType(patch_type_str)
    except ValueError:
        # Unknown patch_type — defer to downstream validators that
        # already classify ``patch_type_unknown``.
        return GateVerdict.success(record=None)

    snippet_type = _SQL_ARMS.get(patch_type)
    if snippet_type is None:
        return GateVerdict.success(record=None)

    sql_text, body_field_name = _extract_sql_for_arm(
        proposal, snippet_type,
    )
    if not sql_text:
        return GateVerdict.success(record=None)

    # Pull the SQL warehouse / Spark session off ctx when the run
    # wired them; absent values mean the underlying validator skips
    # the runtime EXPLAIN phase (Phase 3) and only does Phase 1/2
    # parse/lint checks. Both modes are valid: the gate trusts the
    # canonical wrapper's own degradation policy.
    metadata = dict(getattr(ctx, "metadata_snapshot", {}) or {})
    ok, message = canonical_validate_sql_snippet(
        sql=sql_text,
        snippet_type=snippet_type,
        metadata_snapshot=metadata,
        spark=getattr(ctx, "spark", None),
        catalog=str(getattr(ctx, "catalog", "") or ""),
        gold_schema=str(getattr(ctx, "gold_schema", "") or ""),
        w=getattr(ctx, "w", None),
        warehouse_id=str(getattr(ctx, "warehouse_id", "") or ""),
    )

    # Stamp the verdict regardless of outcome so legacy bypass call
    # sites can short-circuit on this single source of truth.
    try:
        extras = ctx.extras
        if isinstance(extras, dict):
            bucket = extras.setdefault(_RESULTS_KEY, {})
            bucket[(latest.intent_id, body_field_name)] = (ok, message)
    except Exception:
        # Verdict bookkeeping must never block the gate decision.
        pass

    if ok:
        # P4 C3 — transform-and-retry-once. The producer-side
        # ``producer_snippet_validator`` should have stamped
        # ``validation_passed=True`` + ``snippet_id`` + nested
        # ``sql_snippet`` before the proposal arrived. If the
        # proposal arrived UNSTAMPED but the canonical validator
        # passes here, we close the gap by stamping in place — this
        # is the "transform" half of transform-and-retry-once. The
        # applier will then see ``validation_passed=True`` and not
        # short-circuit the patch. The applier-side validator IS the
        # one-shot retry that recovers from a producer that skipped
        # the producer-side helper.
        try:
            body = getattr(proposal, "patch_body", None)
            if (
                isinstance(body, dict)
                and not body.get("validation_passed")
            ):
                from genie_space_optimizer.optimization.producer_snippet_validator import (
                    stamp_snippet_validation_on_body,
                )

                patch_type_wire = str(patch_type_str or "").lower()
                wire_to_snippet_type = {
                    "add_sql_snippet_measure": "measure",
                    "add_sql_snippet_filter": "filter",
                    "add_sql_snippet_expression": "expression",
                }
                normalized_sql = (
                    message if isinstance(message, str) and message else sql_text
                )
                snippet_kind = wire_to_snippet_type.get(patch_type_wire)
                if snippet_kind is not None:
                    stamp_snippet_validation_on_body(
                        body,
                        intent_id=str(getattr(proposal, "intent_id", "")),
                        snippet_name=str(body.get("name") or ""),
                        normalized_sql=normalized_sql,
                        snippet_type=snippet_kind,
                        description=str(
                            body.get("description")
                            or body.get("usage_guidance")
                            or ""
                        ),
                    )
                else:
                    body["validation_passed"] = True
                    if isinstance(message, str) and message:
                        if "sql_expression" in body:
                            body["sql_expression"] = message
                        if "example_sql" in body:
                            body["example_sql"] = message
        except Exception:
            # Transform must never block on bookkeeping —
            # downstream applier_gate is the authoritative
            # second check.
            pass
        return GateVerdict.success(record=None)

    forbidden_signature = (
        f"snippet_validation_failed:{patch_type_str}:{message or 'unknown'}"
    )
    terminal = TerminalRecord(
        kind="OPTIMIZER_STALLED_SAFE_NOOP",
        reason=f"snippet_validation_failed:{message or 'unknown'}",
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature=forbidden_signature,
    )
    return GateVerdict.reject_terminal(terminal)


# ── Public transformer instance ──────────────────────────────────────


gate_validate_sql_snippet = ValidationGate(
    name="gate_validate_sql_snippet",
    from_stage=FunnelStage.APPLYABLE,
    # Pass-through stage: success means "no new state, advance via
    # the next gate's own success transition". We piggyback on
    # APPLYABLE → APPLYABLE because there is no intermediate stage
    # for "snippet validated"; a future refactor may mint one.
    to_stage_on_success=FunnelStage.APPLYABLE,
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
