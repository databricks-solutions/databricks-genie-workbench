"""Eval-row provenance contract — Track I (Phase A burn-down).

The optimizer's eval loop persists per-question trace lineage at the
Genie API call site. ``EvalRowProvenance`` is the dataclass attached
to each row dict under the ``provenance`` key. Its ``mlflow_trace_id``
field is required to be non-empty so the row contract is checkable at
construction time rather than being "recovered" by post-hoc fallback.

The fallback path remains as a backstop for cases where the primary
trace context did not propagate (e.g., an outer process boundary the
predict_fn cannot reach). When fallback fires, callers construct an
``EvalRowProvenance`` with ``source="fallback"`` and increment
``trace_id_fallback_rate`` via ``record_fallback_recovery``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)

_VALID_SOURCES: frozenset[str] = frozenset({"primary", "fallback"})


@dataclass(frozen=True)
class EvalRowProvenance:
    """Provenance contract for one eval row.

    Attributes:
        mlflow_trace_id: Required non-empty MLflow trace identifier.
        genie_conversation_id: Genie conversation id (may be empty when
            the Genie call failed before a conversation was opened).
        source: ``"primary"`` when persisted from the active MLflow span
            inside the predict_fn; ``"fallback"`` when recovered later
            via ``_recover_trace_map``.
    """

    mlflow_trace_id: str
    genie_conversation_id: str
    source: Literal["primary", "fallback"]

    def __post_init__(self) -> None:
        if not str(self.mlflow_trace_id or "").strip():
            raise ValueError(
                "EvalRowProvenance.mlflow_trace_id must be non-empty; "
                "got empty/whitespace value. The eval-row contract "
                "(Track I, Phase A burn-down) requires every row to "
                "carry its trace id at construction time, not via "
                "post-hoc recovery."
            )
        if self.source not in _VALID_SOURCES:
            raise ValueError(
                f"EvalRowProvenance.source must be one of {sorted(_VALID_SOURCES)}; "
                f"got {self.source!r}"
            )


@dataclass
class _FallbackCounter:
    """In-memory counter for ``trace_id_fallback_rate``.

    Phase B's scoreboard reads ``recovered`` and ``total`` to compute
    the rate as a percentage. This counter is process-local; resetting
    happens when the optimizer process restarts or when callers invoke
    :func:`reset_fallback_counter` (used by tests).
    """

    recovered: int = 0
    total: int = 0


_COUNTER = _FallbackCounter()


def record_primary_provenance() -> None:
    """Record one row that landed via the primary (predict_fn) path."""
    _COUNTER.total += 1


def record_fallback_recovery(*, recovered_count: int, total_rows: int) -> None:
    """Record fallback-recovered rows.

    ``recovered_count`` is the number of rows that the recovery
    strategies successfully reattached a trace id to; ``total_rows`` is
    the total iteration row count. Both increment the global counter so
    ``trace_id_fallback_rate`` reflects the cumulative state across an
    optimizer run.
    """
    _COUNTER.recovered += int(recovered_count)
    _COUNTER.total += int(total_rows)
    logger.warning(
        "Track I: trace ID fallback recovered %d/%d rows; cumulative "
        "fallback rate = %.1f%% (%d/%d)",
        recovered_count,
        total_rows,
        100.0 * _COUNTER.recovered / max(_COUNTER.total, 1),
        _COUNTER.recovered,
        _COUNTER.total,
    )


def trace_id_fallback_rate() -> float:
    """Return the cumulative fallback rate as a fraction in [0.0, 1.0]."""
    if _COUNTER.total <= 0:
        return 0.0
    return _COUNTER.recovered / _COUNTER.total


def reset_fallback_counter() -> None:
    """Reset the counter to zero. Used by tests."""
    _COUNTER.recovered = 0
    _COUNTER.total = 0
