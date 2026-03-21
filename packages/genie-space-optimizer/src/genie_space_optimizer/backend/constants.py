"""Shared constants for the Genie Space Optimizer backend."""

from __future__ import annotations

ACTIVE_RUN_STATUSES: frozenset[str] = frozenset({"QUEUED", "IN_PROGRESS", "RUNNING"})
TERMINAL_JOB_STATES: frozenset[str] = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})
