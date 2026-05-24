"""Stage 1 input-evidence preflight for the local workbench.

The probe asks one question: "would the SM canonical lane dispatch the
Stage 1 LLM for this hard QID right now?" To guarantee that probe-green
means SM-will-dispatch, the probe MUST share its Stage 1 card builder
with the runtime SM lane. We get that by routing every probe call
through the production helper
:func:`genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm._build_failing_qid_payload`
— the exact same function the SM transformer calls at
``diagnose_llm._invoke_stage1_llm``.

The probe touches no network and no Databricks state. Builds a minimal
``QuestionStateInIteration`` so the runtime helper accepts it as input;
the helper only reads ``state.qid``.

Trial 13 typed-evidence cutover
-------------------------------
Plan 12 produces per-QID :class:`PerQidRcaEvidence`. Both the SM
canonical lane (via ``ctx.rca_evidence_typed``) and the workbench
probe (via the optional ``rca_evidence_typed`` kwarg) thread that
typed evidence into the builder. Before the cutover the SM lane
silently dropped typed evidence at the Stage 1 boundary and hard QIDs
whose rows lacked embedded blame/rca aborted with
``evidence_card_empty:blame_set_empty,rca_evidence_empty`` (see the
Trial 12 / 13 postmortems and the workbench
``test_sm_canonical_lane_accepts_full_production_replay_corpus`` gate).
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from local_lever_workbench.models import (
    Stage1ProbeFinding,
    Stage1ProbeResult,
    WorkbenchHardCase,
    WorkbenchInputBundle,
)


def _rebuild_typed_evidence(typed_payload: Mapping[str, Any] | None):
    """Reconstruct a :class:`PerQidRcaEvidence` from a workbench bundle dict.

    Bundles serialize typed evidence as plain JSON-friendly dicts; the
    runtime helper expects a real :class:`PerQidRcaEvidence` (or
    ``None``). This is bundle-deserialization, not workbench-side
    Stage 1 logic — the builder it feeds is production code.
    """
    if not isinstance(typed_payload, Mapping):
        return None
    from genie_space_optimizer.optimization.rca_evidence_typed import (
        PerQidRcaEvidence,
    )
    from genie_space_optimizer.optimization.repair_intent import PatchType

    payload = dict(typed_payload)
    repair_hint_raw = payload.pop("repair_hint_patch_type", None)
    repair_hint = None
    if isinstance(repair_hint_raw, str) and repair_hint_raw:
        try:
            repair_hint = PatchType[repair_hint_raw]
        except KeyError:
            repair_hint = None
    return PerQidRcaEvidence(
        qid=str(payload.get("qid") or ""),
        observed_failure=str(payload.get("observed_failure") or ""),
        generated_sql_issue=str(payload.get("generated_sql_issue") or ""),
        expected_sql_shape=str(payload.get("expected_sql_shape") or ""),
        blame_set=tuple(str(b) for b in payload.get("blame_set") or ()),
        suggested_repair_family=str(
            payload.get("suggested_repair_family") or ""
        ),
        repair_hint_patch_type=repair_hint,  # type: ignore[arg-type]
        confidence=str(payload.get("confidence") or "high"),  # type: ignore[arg-type]
        quoted_evidence=tuple(
            str(q) for q in payload.get("quoted_evidence") or ()
        ),
    )


def _build_card_via_runtime_helper(
    qid: str, row: Mapping[str, Any], typed_evidence: Any | None,
) -> dict:
    """Run the SM canonical lane's Stage 1 card builder.

    Wraps ``diagnose_llm._build_failing_qid_payload`` so the probe and
    the SM share one implementation. The wrapper constructs the
    smallest valid :class:`QuestionStateInIteration` the helper needs;
    the helper only reads ``state.qid`` today, but the contract is
    pinned by :mod:`tests.workbench.test_local_workbench_stage1_probe`.
    """
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )
    from genie_space_optimizer.optimization.state_machine.records import (
        HardQidSeenRecord,
    )
    from genie_space_optimizer.optimization.state_machine.state import (
        QuestionStateInIteration,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
        _build_failing_qid_payload,
    )

    seen = HardQidSeenRecord(
        eval_row_id="probe-synthetic",
        predicate="row_is_hard_failure",
        score=0.0,
        baseline_sql="",
        expected_shape="",
        iteration_first_seen=0,
    )
    state = QuestionStateInIteration(
        qid=qid,
        iteration=0,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=seen,
    )
    return _build_failing_qid_payload(
        state, dict(row), typed_evidence=typed_evidence,
    )


def probe_case(
    case: WorkbenchHardCase,
    *,
    rca_evidence_typed: Mapping[str, Any] | None = None,
) -> Stage1ProbeFinding:
    """Probe a single hard case via the SM canonical lane's builder.

    ``rca_evidence_typed`` is a per-QID map. The runtime SM lane
    receives the same map via ``ctx.rca_evidence_typed`` (threaded
    from ``metadata_snapshot["_rca_evidence_typed"]``); the probe
    accepts it directly so workbench bundles that committed typed
    evidence can predict the SM's behaviour exactly. When the map is
    empty or does not contain ``case.qid``, the probe mirrors the
    SM's row-only Stage 1 path.
    """
    from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
        DEFAULT_STAGE1_CONTRACT,
    )

    typed_payload = (rca_evidence_typed or {}).get(case.qid)
    typed_ev = (
        typed_payload
        if typed_payload is None
        or not isinstance(typed_payload, Mapping)
        else _rebuild_typed_evidence(typed_payload)
    )
    card = _build_card_via_runtime_helper(
        case.qid, case.row, typed_ev,
    )
    violations = tuple(
        str(getattr(v, "field", v))
        for v in DEFAULT_STAGE1_CONTRACT.validate(card)
    )
    field_sources = dict(DEFAULT_STAGE1_CONTRACT.field_sources(card))
    return Stage1ProbeFinding(
        qid=case.qid,
        violations=violations,
        field_sources=field_sources,
        would_dispatch_llm=not violations,
    )


def _typed_evidence_map_from_bundle(
    bundle: WorkbenchInputBundle,
) -> dict[str, Any]:
    """Materialize the per-QID typed evidence map a bundle carries.

    Bundles serialize typed evidence as JSON dicts (see
    :class:`WorkbenchHardCase`). Production runtime expects raw dicts
    on ``metadata_snapshot["_rca_evidence_typed"]`` (the Plan 11 batch
    path consumes the same shape), so the probe forwards the raw dicts
    untouched and the runtime helper handles type reconstruction.
    """
    return {
        c.qid: c.typed_evidence
        for c in bundle.hard_cases
        if c.typed_evidence is not None
    }


def probe_bundle(
    bundle: WorkbenchInputBundle,
    *,
    rca_evidence_typed: Mapping[str, Any] | None = None,
) -> Stage1ProbeResult:
    """Probe every hard case in the bundle, in deterministic QID order.

    If ``rca_evidence_typed`` is not supplied, the typed evidence map
    is derived from the bundle's hard cases (the workbench commits
    typed evidence alongside each case). Pass an explicit map to
    override.
    """
    typed_map = (
        dict(rca_evidence_typed)
        if rca_evidence_typed is not None
        else _typed_evidence_map_from_bundle(bundle)
    )
    findings = tuple(
        probe_case(c, rca_evidence_typed=typed_map) for c in bundle.hard_cases
    )
    return Stage1ProbeResult(
        findings=findings,
        all_pass=all(not f.violations for f in findings),
    )


def probe_cases(
    cases: Iterable[WorkbenchHardCase],
    *,
    rca_evidence_typed: Mapping[str, Any] | None = None,
) -> Stage1ProbeResult:
    """Convenience helper that takes an iterable of cases directly."""
    findings = tuple(
        probe_case(c, rca_evidence_typed=rca_evidence_typed) for c in cases
    )
    return Stage1ProbeResult(
        findings=findings,
        all_pass=all(not f.violations for f in findings),
    )


__all__ = [
    "probe_bundle",
    "probe_case",
    "probe_cases",
    "_rebuild_typed_evidence",
    "_typed_evidence_map_from_bundle",
]
