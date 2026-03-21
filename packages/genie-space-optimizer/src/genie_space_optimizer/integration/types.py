"""Return types for integration functions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TriggerResult:
    """Returned by :func:`trigger_optimization`."""

    run_id: str
    job_run_id: str
    job_url: str | None
    status: str  # "IN_PROGRESS"


@dataclass
class ActionResult:
    """Returned by :func:`apply_optimization` and :func:`discard_optimization`."""

    status: str   # "applied" or "discarded"
    run_id: str
    message: str
