"""SM1–SM10 invariants for the optimizer state machine.

See design document section 9. Each check returns a list of violation dicts;
empty list = invariant satisfied. Plugged into contract_health.py with HIGH tier.
"""
from __future__ import annotations

from collections import Counter
from typing import Iterable, Mapping

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
    stage_index,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)


def check_sm1_terminal_coverage(
    *,
    states: Iterable[QuestionStateInIteration],
) -> list[dict]:
    """SM1: every state at end-of-iteration is ACCEPTED or TERMINATED."""
    violations: list[dict] = []
    for s in states:
        if s.current_stage not in (FunnelStage.ACCEPTED, FunnelStage.TERMINATED):
            violations.append({
                "invariant": "SM1",
                "message": (
                    f"SM1: qid={s.qid} iteration={s.iteration} ended at "
                    f"current_stage={s.current_stage.value} (expected ACCEPTED or TERMINATED)."
                ),
            })
    return violations


def check_sm2_single_transition_origin(
    *,
    transitions: Iterable,
) -> list[dict]:
    """SM2: every StageTransition is unique by (at_ms, transformer_name, from_stage, to_stage)."""
    keys = [(t.at_ms, t.transformer_name, t.from_stage, t.to_stage) for t in transitions]
    counts = Counter(keys)
    violations: list[dict] = []
    for key, n in counts.items():
        if n > 1:
            violations.append({
                "invariant": "SM2",
                "message": f"SM2: transition {key} occurs {n} times; expected exactly one origin.",
            })
    return violations


def check_sm3_marker_correspondence(
    *,
    transitions_by_qid: Mapping[str, Iterable],
    markers: Iterable[Mapping],
) -> list[dict]:
    """SM3: every emitted GSO_QSTATE_TRANSITION_V1 marker matches a recorded transition."""
    transition_keys: set[tuple] = set()
    for qid, transitions in transitions_by_qid.items():
        for t in transitions:
            transition_keys.add((qid, t.from_stage.value, t.to_stage.value, t.at_ms))

    violations: list[dict] = []
    for m in markers:
        key = (m["qid"], m["from_stage"], m["to_stage"], m["at_ms"])
        if key not in transition_keys:
            violations.append({
                "invariant": "SM3",
                "message": (
                    f"SM3: orphan marker {key} has no matching StageTransition. "
                    "Markers must witness transitions, not be fired beside them."
                ),
            })
    return violations


def check_sm5_stage_monotonicity(
    *,
    states: Iterable[QuestionStateInIteration],
) -> list[dict]:
    """SM5: deepest_stage_reached >= max(stage) across transitions."""
    violations: list[dict] = []
    for s in states:
        observed_max = s.current_stage
        for t in s.transitions:
            if (
                t.to_stage != FunnelStage.TERMINATED
                and stage_index(t.to_stage) > stage_index(observed_max)
            ):
                observed_max = t.to_stage
        if stage_index(s.deepest_stage_reached) < stage_index(observed_max):
            violations.append({
                "invariant": "SM5",
                "message": (
                    f"SM5: qid={s.qid} deepest_stage_reached={s.deepest_stage_reached.value} "
                    f"< observed_max={observed_max.value} across transitions."
                ),
            })
    return violations


def check_sm6_terminal_record_presence(
    *,
    states: Iterable[QuestionStateInIteration],
) -> list[dict]:
    """SM6: current_stage == TERMINATED iff terminal is not None."""
    violations: list[dict] = []
    for s in states:
        if (s.current_stage == FunnelStage.TERMINATED) != (s.terminal is not None):
            violations.append({
                "invariant": "SM6",
                "message": (
                    f"SM6: qid={s.qid} current_stage={s.current_stage.value} but "
                    f"terminal={'present' if s.terminal else 'absent'}; must match."
                ),
            })
    return violations


def check_sm7_proposal_attempt_typing(
    *,
    proposals: Iterable,
) -> list[dict]:
    """SM7: escalated_to_attempt_index set iff outcome == 'escalated'."""
    violations: list[dict] = []
    for pa in proposals:
        is_escalated = pa.outcome == "escalated"
        has_index = pa.escalated_to_attempt_index is not None
        if is_escalated != has_index:
            violations.append({
                "invariant": "SM7",
                "message": (
                    f"SM7: ProposalAttempt(intent_id={pa.intent_id}, outcome={pa.outcome}) "
                    f"has escalated_to_attempt_index={'set' if has_index else 'unset'}; "
                    f"must be set iff outcome == 'escalated'."
                ),
            })
    return violations
