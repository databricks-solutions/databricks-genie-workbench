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
