"""QuestionTrajectory — cross-iteration aggregate of per-QID state."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
    stage_index,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)


@dataclass(frozen=True, slots=True)
class QuestionTrajectory(JsonRoundTrip):
    qid: str
    iterations: tuple[QuestionStateInIteration, ...]
    first_seen_iteration: int
    deepest_stage_ever: FunnelStage
    cumulative_terminal_reasons: tuple[str, ...]
    forbidden_signatures: tuple[str, ...]

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "QuestionTrajectory":  # type: ignore[override]
        # Nested QuestionStateInIteration tuple + FunnelStage enum need
        # explicit reconstruction (the base mixin only narrows lists →
        # tuples; it does not rebuild nested dataclasses or enums).
        return cls(
            qid=str(payload["qid"]),
            iterations=tuple(
                QuestionStateInIteration.from_json(s)
                for s in (payload.get("iterations") or ())
            ),
            first_seen_iteration=int(payload["first_seen_iteration"]),
            deepest_stage_ever=FunnelStage(payload["deepest_stage_ever"]),
            cumulative_terminal_reasons=tuple(
                str(r) for r in (payload.get("cumulative_terminal_reasons") or ())
            ),
            forbidden_signatures=tuple(
                str(s) for s in (payload.get("forbidden_signatures") or ())
            ),
        )


def build_trajectory(
    *,
    qid: str,
    iterations: tuple[QuestionStateInIteration, ...],
) -> QuestionTrajectory:
    if not iterations:
        raise ValueError(f"cannot build trajectory for qid={qid} with no iterations")
    first_seen = min(s.iteration for s in iterations)
    deepest = max(
        iterations, key=lambda s: stage_index(s.deepest_stage_reached),
    ).deepest_stage_reached
    reasons = tuple(s.terminal.reason for s in iterations if s.terminal is not None)
    signatures = tuple(
        s.terminal.forbidden_signature
        for s in iterations
        if s.terminal is not None and s.terminal.forbidden_signature
    )
    return QuestionTrajectory(
        qid=qid,
        iterations=iterations,
        first_seen_iteration=first_seen,
        deepest_stage_ever=deepest,
        cumulative_terminal_reasons=reasons,
        forbidden_signatures=signatures,
    )
