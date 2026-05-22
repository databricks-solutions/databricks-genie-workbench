"""Plan 12 routing as a typed ValidationGate.

The gate writes ``effective_target_lever`` and ``routing_evidence_kind``
onto ClusterMembershipRecord. The marker
``GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1`` becomes a *witness* of the
gate's decision; it cannot diverge from ``directives_present`` because
``directives_present`` is read from the cluster record.
"""
from __future__ import annotations

from dataclasses import replace

from genie_space_optimizer.optimization.evidence_to_lever_policy import (
    eligible_lever_families,
)
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    ValidationGate,
)
from genie_space_optimizer.optimization.state_machine.verdict import GateVerdict


def _predicate(state, ctx):
    if state.clustered is None:
        return GateVerdict.reject_terminal(TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason="routing_gate_invoked_without_cluster_record",
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ))

    evidence_kind = state.diagnosed.rca_kind_label if state.diagnosed else ""
    if not evidence_kind:
        return GateVerdict.reject_terminal(TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason="routing_gate_empty_evidence_kind",
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ))

    eligible = eligible_lever_families(evidence_kind)
    chosen_lever = _choose_lever(eligible)

    new_cluster = replace(
        state.clustered,
        effective_target_lever=chosen_lever,
        routing_evidence_kind=evidence_kind,
    )
    return GateVerdict.success(record=new_cluster)


def _choose_lever(eligible: tuple) -> int:
    """Pick the preferred lever from the eligible family tuple.

    Prefer 6 (structural SQL snippet) → 5/5b (example SQL) → fallback to
    the first non-Lever-1 numeric entry. Never returns 1: the Plan 12
    routing policy refuses non-generating lanes for any structural
    failure mode.
    """
    str_eligible = tuple(str(x) for x in eligible)
    if "6" in str_eligible:
        return 6
    if "5" in str_eligible:
        return 5
    if "5b" in str_eligible:
        return 5
    for candidate in str_eligible:
        if candidate != "1" and candidate.isdigit():
            return int(candidate)
    return 6  # safest structural default


routing_gate = ValidationGate(
    name="plan12_routing_gate",
    from_stage=FunnelStage.CLUSTERED,
    to_stage_on_success=FunnelStage.CLUSTERED,  # decoration; stays at CLUSTERED
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
