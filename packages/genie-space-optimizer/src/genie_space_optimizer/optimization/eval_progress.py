from __future__ import annotations

import contextlib
import json
import logging
import os
import time
from collections.abc import Callable, Iterator
from typing import Any

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; using %d", name, raw, default)
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in _TRUTHY


def eval_debug_row_cap() -> int:
    """Return max rows for debug eval; 0 means no cap."""
    return max(0, _env_int("GENIE_SPACE_OPTIMIZER_EVAL_MAX_ROWS_FOR_DEBUG", 0))


def eval_force_sequential() -> bool:
    """Return True when eval should bypass the MLflow batch harness."""
    return _env_bool("GENIE_SPACE_OPTIMIZER_EVAL_FORCE_SEQUENTIAL", False)


def eval_row_timeout_seconds() -> int:
    """Return row timeout budget for observability and future watchdog logic."""
    return max(30, _env_int("GENIE_SPACE_OPTIMIZER_EVAL_ROW_TIMEOUT_SECONDS", 300))


def slice_eval_records_for_debug(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cap = eval_debug_row_cap()
    if cap <= 0:
        return records
    return records[:cap]


class EvalProgressLogger:
    """Emit single-line JSON progress events for long-running evaluation."""

    def __init__(
        self,
        *,
        logger: logging.Logger,
        run_id: str = "",
        eval_scope: str = "",
        iteration: int | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.logger = logger
        self.run_id = run_id
        self.eval_scope = eval_scope
        self.iteration = iteration
        self._clock = clock

    def emit(self, phase: str, **fields: Any) -> None:
        payload: dict[str, Any] = {
            "phase": phase,
            "run_id": self.run_id,
            "eval_scope": self.eval_scope,
            "ts": round(self._clock(), 3),
        }
        if self.iteration is not None:
            payload["iteration"] = self.iteration
        payload.update({k: v for k, v in fields.items() if v is not None})
        question = payload.pop("question", None)
        if question is not None:
            payload["question_prefix"] = str(question)[:120]
        self.logger.info("GSO_EVAL_PROGRESS %s", json.dumps(payload, sort_keys=True, default=str))

    @contextlib.contextmanager
    def phase(self, name: str, **fields: Any) -> Iterator[None]:
        start = self._clock()
        # Emit the start event without calling _clock again: reuse `start` so
        # tests can drive the context with exactly two clock values.
        start_payload: dict[str, Any] = {
            "phase": f"{name}_start",
            "run_id": self.run_id,
            "eval_scope": self.eval_scope,
            "ts": round(start, 3),
        }
        if self.iteration is not None:
            start_payload["iteration"] = self.iteration
        for k, v in fields.items():
            if v is not None:
                start_payload[k] = v
        question = start_payload.pop("question", None)
        if question is not None:
            start_payload["question_prefix"] = str(question)[:120]
        self.logger.info(
            "GSO_EVAL_PROGRESS %s",
            json.dumps(start_payload, sort_keys=True, default=str),
        )
        try:
            yield
        except Exception as exc:
            end = self._clock()
            elapsed = round(end - start, 3)
            done_payload: dict[str, Any] = {
                "phase": f"{name}_failed",
                "run_id": self.run_id,
                "eval_scope": self.eval_scope,
                "ts": round(end, 3),
                "elapsed_seconds": elapsed,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
            }
            if self.iteration is not None:
                done_payload["iteration"] = self.iteration
            for k, v in fields.items():
                if v is not None:
                    done_payload[k] = v
            question = done_payload.pop("question", None)
            if question is not None:
                done_payload["question_prefix"] = str(question)[:120]
            self.logger.info(
                "GSO_EVAL_PROGRESS %s",
                json.dumps(done_payload, sort_keys=True, default=str),
            )
            raise
        else:
            end = self._clock()
            elapsed = round(end - start, 3)
            done_payload = {
                "phase": f"{name}_done",
                "run_id": self.run_id,
                "eval_scope": self.eval_scope,
                "ts": round(end, 3),
                "elapsed_seconds": elapsed,
            }
            if self.iteration is not None:
                done_payload["iteration"] = self.iteration
            for k, v in fields.items():
                if v is not None:
                    done_payload[k] = v
            question = done_payload.pop("question", None)
            if question is not None:
                done_payload["question_prefix"] = str(question)[:120]
            self.logger.info(
                "GSO_EVAL_PROGRESS %s",
                json.dumps(done_payload, sort_keys=True, default=str),
            )
