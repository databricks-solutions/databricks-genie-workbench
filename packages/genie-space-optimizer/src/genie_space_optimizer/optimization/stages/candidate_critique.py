"""Plan 6 — candidate-critique stage.

Public surface:
  * ``STAGE_KEY``                — registry key (string constant).
  * ``CritiqueInput``            — typed input dataclass (Task 8).
  * ``CritiqueOutcome``          — typed output dataclass (Task 8).
  * ``execute(ctx, inp)``        — stage entry (Task 9).
  * ``critique_candidate_for_proposal(...)`` — per-proposal LLM
    driver (this task, Task 6).

Private helpers:
  * ``_build_request(...)``      — pure Plan-2 LlmReasoningRequest builder.
  * ``_filter_predicted_regressions(...)`` — strip qids not in
    passing_qids_at_risk (data hygiene).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from typing import TYPE_CHECKING

from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.skills._loader import _SKILL_LOADER

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.rca_evidence_typed import (
        PerQidRcaEvidence,
    )

logger = logging.getLogger(__name__)

STAGE_KEY: str = "candidate_critique"

_SKILL_ID = "candidate_critique"
_PROMPT_CONST = "CANDIDATE_CRITIQUE_PROMPT"
_HYPHEN_SKILL_ID = "candidate-critique"


def _evidence_to_prompt_dict(ev: PerQidRcaEvidence) -> dict[str, Any]:
    """Project PerQidRcaEvidence to the prompt-facing dict shape.
    Mirrors the SKILL.md ``<context_inputs>.per_qid_evidence`` schema."""
    return {
        "qid": ev.qid,
        "observed_failure": ev.observed_failure,
        "generated_sql_issue": ev.generated_sql_issue,
        "expected_sql_shape": ev.expected_sql_shape,
        "blame_set": list(ev.blame_set),
        "confidence": ev.confidence,
    }


def _render_user_prompt(
    *,
    proposal: dict[str, Any],
    cluster_id: str,
    ag_id: str,
    iteration: int,
    cluster_semantic_theme: str,
    per_qid_evidence: dict[str, PerQidRcaEvidence],
    passing_qids_at_risk: tuple[str, ...],
) -> str:
    """Render the per-proposal user prompt as one JSON-shaped block."""
    from genie_space_optimizer.optimization.repair_intent import (
        extract_repair_intent_from_proposal,
    )
    intent = extract_repair_intent_from_proposal(proposal)
    repair_intent_payload: dict | None = None
    if intent is not None:
        repair_intent_payload = {
            "intent_name": intent.intent_name,
            "intent_description": intent.intent_description,
            "repair_shape": intent.repair_shape.value,
            "patch_type": intent.patch_type.value,
            "rationale": intent.rationale,
            "source": intent.source,
        }
    patch_body = {
        k: v for k, v in proposal.items()
        if k not in {
            "repair_intent", "intent_id", "proposal_id",
            "content_fingerprint", "cross_lever_override",
            "rca_id", "primary_cluster_id", "source_cluster_ids",
            "critique_verdict",
        }
    }
    payload = {
        "proposal_id": str(proposal.get("proposal_id") or ""),
        "cluster_id": cluster_id,
        "ag_id": ag_id,
        "iteration": int(iteration),
        "repair_intent": repair_intent_payload,
        "patch_body": patch_body,
        "blame_set": list(intent.blame_set) if intent is not None else [],
        "per_qid_evidence": [
            _evidence_to_prompt_dict(per_qid_evidence[qid])
            for qid in sorted(per_qid_evidence.keys())
        ],
        "passing_qids_at_risk": sorted(passing_qids_at_risk),
        "cluster_semantic_theme": cluster_semantic_theme,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _build_request(
    *,
    proposal: dict[str, Any],
    cluster_id: str,
    ag_id: str,
    iteration: int,
    cluster_semantic_theme: str,
    per_qid_evidence: dict[str, PerQidRcaEvidence],
    passing_qids_at_risk: tuple[str, ...],
):
    """Build a Plan-2 LlmReasoningRequest. Pure — no LLM dispatch.
    Exposed for testability."""
    # Lazy import to break the circular import chain:
    # stages/__init__ → _registry → candidate_critique → llm_reasoning_call →
    # llm_abstain → stages._json_io → stages/__init__.
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    user_prompt = _render_user_prompt(
        proposal=proposal,
        cluster_id=cluster_id, ag_id=ag_id, iteration=iteration,
        cluster_semantic_theme=cluster_semantic_theme,
        per_qid_evidence=per_qid_evidence,
        passing_qids_at_risk=passing_qids_at_risk,
    )
    proposal_id = str(proposal.get("proposal_id") or "")
    call_id = (
        f"candidate_critique.iter_{int(iteration)}.{proposal_id}"
    )
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=_HYPHEN_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
        model_override=rsm.model_override,
    )


def _filter_predicted_regressions(
    predicted: tuple[str, ...],
    *,
    passing_qids_at_risk: tuple[str, ...],
) -> tuple[str, ...]:
    """Drop predicted qids not in passing_qids_at_risk (hygiene).

    Preserves input order of the at-risk list (deterministic for
    postmortem comparison). Logs a debug line when filtering fires."""
    risk_set = set(passing_qids_at_risk)
    filtered = tuple(q for q in predicted if q in risk_set)
    if len(filtered) != len(predicted):
        unknown = sorted(set(predicted) - risk_set)
        logger.debug(
            "critique.dropped_unknown_qids predicted=%s unknown=%s "
            "kept=%s",
            list(predicted), unknown, list(filtered),
        )
    return filtered


def critique_candidate_for_proposal(
    *,
    w: Any,
    proposal: dict[str, Any],
    cluster_id: str,
    ag_id: str,
    iteration: int,
    cluster_semantic_theme: str,
    per_qid_evidence: dict[str, PerQidRcaEvidence],
    passing_qids_at_risk: tuple[str, ...],
) -> CritiqueVerdict | None:
    """Dispatch one LLM critique call for ONE proposal.

    Returns:
      CritiqueVerdict on success (envelope parsed; predicted
        regressions filtered to passing_qids_at_risk).
      None on: missing repair_intent stamp (short-circuit, no LLM
        call); LLM decline; LLM error; envelope-parse failure. Caller
        records the None outcome as a typed iteration outcome rather
        than blocking the proposal.
    """
    from genie_space_optimizer.optimization.repair_intent import (
        extract_repair_intent_from_proposal,
    )
    intent = extract_repair_intent_from_proposal(proposal)
    if intent is None:
        logger.info(
            "critique.short_circuit reason=no_intent proposal_id=%s",
            proposal.get("proposal_id"),
        )
        return None

    request = _build_request(
        proposal=proposal,
        cluster_id=cluster_id, ag_id=ag_id, iteration=iteration,
        cluster_semantic_theme=cluster_semantic_theme,
        per_qid_evidence=per_qid_evidence,
        passing_qids_at_risk=passing_qids_at_risk,
    )
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        LlmReasoningCall,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)

    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "critique.declined proposal_id=%s reason=%s",
                proposal.get("proposal_id"),
                response.declined.reason.value,
            )
        elif response.error is not None:
            logger.warning(
                "critique.error proposal_id=%s err=%s",
                proposal.get("proposal_id"), response.error,
            )
        return None

    parsed = response.parsed_output
    proposal_id = str(proposal.get("proposal_id") or "")
    raw_neighbors = tuple(
        str(q) for q in (parsed.get("likely_neighbor_regressions") or ())
    )
    filtered_neighbors = _filter_predicted_regressions(
        raw_neighbors, passing_qids_at_risk=passing_qids_at_risk,
    )
    return CritiqueVerdict(
        proposal_id=proposal_id,
        addresses_target_failure=bool(parsed["addresses_target_failure"]),
        is_overgeneralized=bool(parsed["is_overgeneralized"]),
        likely_neighbor_regressions=filtered_neighbors,
        matches_intended_shape=bool(parsed["matches_intended_shape"]),
        overall_recommendation=parsed["overall_recommendation"],
        rationale=str(parsed["rationale"]),
    )


# ── Plan 6 Task 8 — typed I/O dataclasses ─────────────────────────────

from dataclasses import dataclass, field  # noqa: E402
from typing import Mapping  # noqa: E402

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip  # noqa: E402

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.repair_intent import (
        RepairIntent,
    )


@dataclass
class CritiqueInput(JsonRoundTrip):
    """Plan 6 — typed input to the candidate-critique stage."""

    proposals_by_ag: dict[str, tuple[Mapping[str, Any], ...]]
    repair_intents_by_id: dict[str, RepairIntent] = field(default_factory=dict)
    rca_evidence_typed_by_cluster: dict[str, dict[str, PerQidRcaEvidence]] = (
        field(default_factory=dict)
    )
    passing_qids_at_risk_by_proposal_id: dict[str, tuple[str, ...]] = (
        field(default_factory=dict)
    )
    cluster_semantic_theme_by_cluster: dict[str, str] = field(default_factory=dict)
    cluster_id_by_proposal_id: dict[str, str] = field(default_factory=dict)
    ag_id_by_proposal_id: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "proposals_by_ag": {
                ag_id: [dict(p) for p in props]
                for ag_id, props in (self.proposals_by_ag or {}).items()
            },
            "repair_intents_by_id": {
                intent_id: intent.to_json()
                for intent_id, intent in (self.repair_intents_by_id or {}).items()
            },
            "rca_evidence_typed_by_cluster": {
                cid: {qid: ev.to_json() for qid, ev in evs.items()}
                for cid, evs in (self.rca_evidence_typed_by_cluster or {}).items()
            },
            "passing_qids_at_risk_by_proposal_id": {
                pid: list(qids)
                for pid, qids in (
                    self.passing_qids_at_risk_by_proposal_id or {}
                ).items()
            },
            "cluster_semantic_theme_by_cluster": dict(
                self.cluster_semantic_theme_by_cluster or {}
            ),
            "cluster_id_by_proposal_id": dict(
                self.cluster_id_by_proposal_id or {}
            ),
            "ag_id_by_proposal_id": dict(self.ag_id_by_proposal_id or {}),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "CritiqueInput":  # type: ignore[override]
        from genie_space_optimizer.optimization.rca_evidence_typed import (
            PerQidRcaEvidence as _PerQidRcaEvidence,
        )
        from genie_space_optimizer.optimization.repair_intent import (
            RepairIntent as _RepairIntent,
        )
        return cls(
            proposals_by_ag={
                ag_id: tuple(dict(p) for p in props)
                for ag_id, props in (
                    payload.get("proposals_by_ag") or {}
                ).items()
            },
            repair_intents_by_id={
                intent_id: _RepairIntent.from_json(p)
                for intent_id, p in (
                    payload.get("repair_intents_by_id") or {}
                ).items()
            },
            rca_evidence_typed_by_cluster={
                cid: {
                    qid: _PerQidRcaEvidence.from_json(p)
                    for qid, p in evs.items()
                }
                for cid, evs in (
                    payload.get("rca_evidence_typed_by_cluster") or {}
                ).items()
            },
            passing_qids_at_risk_by_proposal_id={
                pid: tuple(str(q) for q in qids)
                for pid, qids in (
                    payload.get("passing_qids_at_risk_by_proposal_id") or {}
                ).items()
            },
            cluster_semantic_theme_by_cluster=dict(
                payload.get("cluster_semantic_theme_by_cluster") or {}
            ),
            cluster_id_by_proposal_id=dict(
                payload.get("cluster_id_by_proposal_id") or {}
            ),
            ag_id_by_proposal_id=dict(
                payload.get("ag_id_by_proposal_id") or {}
            ),
        )


@dataclass
class CritiqueOutcome(JsonRoundTrip):
    """Plan 6 — typed output of the candidate-critique stage."""

    proposals_by_ag: dict[str, tuple[Mapping[str, Any], ...]]
    verdict_by_proposal_id: dict[str, CritiqueVerdict] = field(default_factory=dict)
    dropped_by_critique: tuple[str, ...] = ()
    advised_count: int = 0

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "proposals_by_ag": {
                ag_id: [dict(p) for p in props]
                for ag_id, props in (self.proposals_by_ag or {}).items()
            },
            "verdict_by_proposal_id": {
                pid: v.to_json()
                for pid, v in (self.verdict_by_proposal_id or {}).items()
            },
            "dropped_by_critique": list(self.dropped_by_critique or ()),
            "advised_count": int(self.advised_count),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "CritiqueOutcome":  # type: ignore[override]
        return cls(
            proposals_by_ag={
                ag_id: tuple(dict(p) for p in props)
                for ag_id, props in (
                    payload.get("proposals_by_ag") or {}
                ).items()
            },
            verdict_by_proposal_id={
                pid: CritiqueVerdict.from_json(p)
                for pid, p in (
                    payload.get("verdict_by_proposal_id") or {}
                ).items()
            },
            dropped_by_critique=tuple(
                str(p) for p in (payload.get("dropped_by_critique") or ())
            ),
            advised_count=int(payload.get("advised_count") or 0),
        )


INPUT_CLASS = CritiqueInput
OUTPUT_CLASS = CritiqueOutcome


# ── Plan 6 Task 9 — stage execute() with advisory + enforcing modes ───

from genie_space_optimizer.common.config import (  # noqa: E402
    critique_gate_enforcing_enabled,
)
from genie_space_optimizer.optimization.rca_decision_trace import (  # noqa: E402
    DecisionOutcome,
    DecisionRecord,
    DecisionType,
)


def _emit_verdict_decision(
    *,
    ctx: Any,
    verdict: CritiqueVerdict,
    cluster_id: str,
    ag_id: str,
    is_blocked: bool,
) -> None:
    """Emit one CANDIDATE_CRITIQUED decision record per verdict.

    ``outcome`` mirrors the verdict semantics:
      proceed → INFO   (verdict.reason_code() = CRITIQUE_PROCEED)
      rework  → INFO   (verdict.reason_code() = CRITIQUE_REWORK)
      discard → INFO if advisory; DROPPED if enforcing-and-blocked
    Postmortem groups by reason_code; the outcome distinguishes
    "blocked the patch" from "noted but let through".
    """
    outcome = (
        DecisionOutcome.DROPPED if is_blocked else DecisionOutcome.INFO
    )
    rec = DecisionRecord(
        run_id=str(getattr(ctx, "run_id", "") or ""),
        iteration=int(getattr(ctx, "iteration", 0) or 0),
        decision_type=DecisionType.CANDIDATE_CRITIQUED,
        outcome=outcome,
        reason_code=verdict.reason_code(),
        cluster_id=cluster_id,
        ag_id=ag_id,
        proposal_id=verdict.proposal_id,
        evidence_refs=(f"proposal:{verdict.proposal_id}",),
        affected_qids=verdict.likely_neighbor_regressions,
        target_qids=verdict.likely_neighbor_regressions,
        expected_effect=verdict.rationale,
        metrics={
            "addresses_target_failure": verdict.addresses_target_failure,
            "is_overgeneralized": verdict.is_overgeneralized,
            "matches_intended_shape": verdict.matches_intended_shape,
            "overall_recommendation": verdict.overall_recommendation,
            "is_blocked_by_critique": is_blocked,
        },
    )
    ctx.decision_emit(rec)


def _filter_slate_in_enforcing_mode(
    *,
    proposals_by_ag: dict[str, tuple[Mapping[str, Any], ...]],
    verdicts_by_pid: dict[str, CritiqueVerdict],
) -> tuple[dict[str, tuple[Mapping[str, Any], ...]], tuple[str, ...]]:
    """Drop proposals whose verdict is blocking. Preserves AG keys
    (empty tuple when every proposal in the AG drops)."""
    filtered: dict[str, tuple[Mapping[str, Any], ...]] = {}
    dropped: list[str] = []
    for ag_id, props in proposals_by_ag.items():
        kept: list[Mapping[str, Any]] = []
        for p in props:
            pid = str(p.get("proposal_id") or "")
            v = verdicts_by_pid.get(pid)
            if v is not None and v.is_blocking():
                dropped.append(pid)
                continue
            kept.append(p)
        filtered[ag_id] = tuple(kept)
    return filtered, tuple(dropped)


def execute(ctx: Any, inp: CritiqueInput) -> CritiqueOutcome:
    """Plan 6 stage entry. One LLM critique call per proposal.

    Behaviour:
      * Walk ``proposals_by_ag`` in canonical (sorted) AG order;
        within each AG, walk proposals in their input order.
      * For each proposal, look up the cluster_id / ag_id from the
        provenance maps and dispatch ``critique_candidate_for_proposal``.
      * Driver returns ``None`` when the proposal has no intent OR
        the LLM declines / errors — those proposals are silently
        passed through (advisory) and do NOT contribute to
        ``advised_count``.
      * For every non-None verdict: stamp it on the proposal dict as
        ``proposal["critique_verdict"] = verdict.to_json()`` for
        downstream postmortem, increment ``advised_count``, and emit
        a ``CANDIDATE_CRITIQUED`` decision record.
      * When ``GSO_CRITIQUE_GATE_ENFORCING=true``: filter blocking
        verdicts out of the output slate; record their proposal_ids
        in ``dropped_by_critique``. Otherwise output slate is
        byte-stable from input.
    """
    enforcing = critique_gate_enforcing_enabled()
    verdicts: dict[str, CritiqueVerdict] = {}
    stamped_proposals_by_ag: dict[str, tuple[Mapping[str, Any], ...]] = {}
    advised = 0

    for ag_id in sorted(inp.proposals_by_ag.keys()):
        props = inp.proposals_by_ag[ag_id]
        stamped_props: list[Mapping[str, Any]] = []
        for proposal in props:
            stamped = dict(proposal)
            pid = str(stamped.get("proposal_id") or "")
            cluster_id = inp.cluster_id_by_proposal_id.get(pid, "")
            verdict = critique_candidate_for_proposal(
                w=None,
                proposal=stamped,
                cluster_id=cluster_id,
                ag_id=inp.ag_id_by_proposal_id.get(pid, ag_id),
                iteration=int(getattr(ctx, "iteration", 0) or 0),
                cluster_semantic_theme=(
                    inp.cluster_semantic_theme_by_cluster.get(cluster_id, "")
                ),
                per_qid_evidence=(
                    inp.rca_evidence_typed_by_cluster.get(cluster_id, {})
                ),
                passing_qids_at_risk=(
                    inp.passing_qids_at_risk_by_proposal_id.get(pid, ())
                ),
            )
            if verdict is not None:
                advised += 1
                verdicts[pid] = verdict
                stamped["critique_verdict"] = verdict.to_json()
                _emit_verdict_decision(
                    ctx=ctx, verdict=verdict,
                    cluster_id=cluster_id,
                    ag_id=inp.ag_id_by_proposal_id.get(pid, ag_id),
                    is_blocked=(enforcing and verdict.is_blocking()),
                )
            stamped_props.append(stamped)
        stamped_proposals_by_ag[ag_id] = tuple(stamped_props)

    if enforcing:
        filtered_slate, dropped = _filter_slate_in_enforcing_mode(
            proposals_by_ag=stamped_proposals_by_ag,
            verdicts_by_pid=verdicts,
        )
        return CritiqueOutcome(
            proposals_by_ag=filtered_slate,
            verdict_by_proposal_id=verdicts,
            dropped_by_critique=dropped,
            advised_count=advised,
        )

    return CritiqueOutcome(
        proposals_by_ag=stamped_proposals_by_ag,
        verdict_by_proposal_id=verdicts,
        dropped_by_critique=(),
        advised_count=advised,
    )
