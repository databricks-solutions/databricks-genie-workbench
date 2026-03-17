"""Lightweight configuration for integration callers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class IntegrationConfig:
    """Minimal configuration for GSO integration callers.

    Construct this directly -- no pydantic-settings or env-prefix magic
    required.  Pass it to :func:`trigger_optimization`,
    :func:`apply_optimization`, or :func:`discard_optimization`.
    """

    catalog: str
    schema_name: str
    warehouse_id: str
    job_id: int | None = None
