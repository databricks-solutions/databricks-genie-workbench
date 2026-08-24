"""Column visibility helpers shared across the optimizer.

A Genie Agent hides a column from the model by setting ``exclude: true``
(modern ``serialized_space``) or ``visible: false`` (legacy/normalized) on
its ``column_configs`` / ``columns`` entry. Several optimizer subsystems
previously rolled their own visibility check — the applier
(:func:`genie_space_optimizer.optimization.applier._is_hidden`) looked at
both flags while the IQ scan
(:func:`genie_space_optimizer.iq_scan.scoring.calculate_score`) only looked
at ``exclude`` — and the SQL-generation / benchmark paths checked neither.

This module is the single source of truth. Hidden columns must never reach
LLM prompt allowlists or benchmark validation, so every call site that
filters the optimizer's column universe goes through :func:`is_column_hidden`.
"""

from __future__ import annotations

from typing import Any


def is_column_hidden(column_config: dict[str, Any] | None) -> bool:
    """Return True when the column is hidden from the Genie Agent.

    Honors both the modern ``exclude: true`` flag and the legacy
    ``visible: false`` flag. A missing/``None`` config is treated as visible.

    Mirrors the prior behaviour of
    :func:`genie_space_optimizer.optimization.applier._is_hidden` so callers
    can be consolidated without changing semantics.
    """
    if not isinstance(column_config, dict):
        return False
    if column_config.get("exclude") is True:
        return True
    if column_config.get("visible") is False:
        return True
    return False
