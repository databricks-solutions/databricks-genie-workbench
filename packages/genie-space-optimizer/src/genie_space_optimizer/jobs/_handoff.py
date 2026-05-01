"""Cross-task state resilience helpers for the GSO 6-task DAG.

Every notebook task in the optimization DAG reads upstream state via
``dbutils.jobs.taskValues.get(...)``. On a Repair Run, taskValues from
prior runs are NOT propagated, so each call returns the empty default
and the task silently runs on degenerate inputs.

This module wraps every cross-task read with a 3-step lookup:

  1. taskValues first (the happy path).
  2. Delta fallback against the durable state already persisted in
     ``genie_opt_runs`` and ``genie_opt_iterations``.
  3. Loud failure (or documented default) if neither exists.

Each value carries the ``HandoffSource`` it came from so log readers can
distinguish "the run resumed correctly" from "the run silently fell
back to Delta state". See the cross-task state resilience plan
(``docs/2026-05-01-cross-task-state-resilience-plan.md``) for the
problem statement and design.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class HandoffSource(str, Enum):
    """Where a HandoffValue was sourced from."""

    TASK_VALUES = "task_values"       # taskValues returned a real value
    DELTA_FALLBACK = "delta_fallback"  # taskValues empty; Delta filled in
    DEFAULT = "default"               # both empty; documented default applied
    MISSING = "missing"               # both empty; no default — caller decides


@dataclass(frozen=True)
class HandoffValue:
    """A typed read of a cross-task value.

    Attributes:
        key: Logical key being read (e.g. ``"run_id"``,
            ``"overall_accuracy"``).
        value: The resolved value (already typed — int / float / dict /
            list / str / None).
        source: Where ``value`` came from, for audit logs.
        delta_query: Optional SQL string of the Delta query used when
            ``source == DELTA_FALLBACK``. Captured for log audits;
            never None on the fallback path.
    """

    key: str
    value: Any
    source: HandoffSource
    delta_query: str | None = None
