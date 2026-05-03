"""StageContext: the single per-iteration context object plumbed into every stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class StageContext:
    """Context plumbed into every stage's execute() call.

    Owned in the stages package so harness wiring is the only place that
    builds it. Stages depend on the package, not on harness.py.

    Phase G freezes this dataclass and adds slots=True. Phase H reads
    ``mlflow_anchor_run_id`` to attach per-stage I/O captures to a
    deterministic run.
    """

    run_id: str
    iteration: int
    space_id: str
    domain: str
    catalog: str
    schema: str
    apply_mode: str
    journey_emit: Callable[..., None]
    decision_emit: Callable[..., None]
    mlflow_anchor_run_id: str | None = None
    feature_flags: dict[str, Any] = field(default_factory=dict)
