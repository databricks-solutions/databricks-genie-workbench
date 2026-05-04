"""Per-stage I/O capture decorator (Phase H).

Wraps a stage's ``execute(ctx, inp) -> out`` callable. On every call:

  1. Serializes ``inp`` to JSON via dataclasses.asdict + json.dumps.
  2. Invokes the wrapped ``execute`` to get ``out``.
  3. Serializes ``out`` similarly.
  4. Hooks ``ctx.decision_emit`` to capture emitted DecisionRecords.
  5. Logs ``input.json``, ``output.json``, ``decisions.json`` to
     MLflow under ``ctx.mlflow_anchor_run_id`` at the path
     ``gso_postmortem_bundle/iterations/iter_NN/stages/<NN>_<key>/``.
  6. Returns ``out`` unchanged.

The decorator NEVER raises. If MLflow logging fails (network, missing
anchor, serialization error, etc.) it logs a warning and continues.
The optimizer must not break because diagnostic capture is unavailable.

The capture path is computed via ``run_output_contract.stage_artifact_paths``
so the directory naming matches the bundle's ordering.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import asdict
from typing import Any, Callable

from genie_space_optimizer.optimization.run_output_contract import (
    stage_artifact_paths,
)

logger = logging.getLogger(__name__)


def _log_text(*, run_id: str, text: str, artifact_file: str) -> None:
    """Thin shim around MlflowClient().log_text(...).

    Lives at module scope so tests can monkeypatch it.
    """
    try:
        from mlflow.tracking import MlflowClient  # type: ignore[import-not-found]
    except Exception:
        logger.warning("mlflow client unavailable; skipping log_text")
        return
    MlflowClient().log_text(
        run_id=run_id, text=text, artifact_file=artifact_file,
    )


def _safe_dumps(value: Any) -> str:
    """Serialize a value to JSON, falling back to str() for opaque
    objects so the diagnostic capture never silently drops a stage."""
    return json.dumps(value, sort_keys=True, default=str, indent=2)


def _normalize_for_json(value: Any) -> Any:
    """Recursively convert sets to sorted lists for deterministic JSON."""
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_normalize_for_json(v) for v in value)
    return value


def _serialize_io(obj: Any) -> str:
    """Convert a dataclass instance (or plain dict / list) to a JSON string.

    For dataclasses, uses dataclasses.asdict to recursively convert
    nested dataclasses. Sets become sorted lists for deterministic
    output.
    """
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        as_dict: Any = asdict(obj)
    else:
        as_dict = obj
    return _safe_dumps(_normalize_for_json(as_dict))


def wrap_with_io_capture(
    *,
    execute: Callable[[Any, Any], Any],
    stage_key: str,
) -> Callable[[Any, Any], Any]:
    """Return a wrapper around ``execute`` that captures I/O to MLflow.

    The wrapper has the same signature as ``execute``. It:

      - Builds the artifact paths via stage_artifact_paths(ctx.iteration, stage_key).
      - Serializes ``inp`` and writes input.json.
      - Hooks ``ctx.decision_emit`` to capture emitted decisions.
      - Calls ``execute(ctx, inp)``.
      - Serializes ``out`` and writes output.json.
      - Writes decisions.json from the captured decisions list.
      - Returns ``out`` unchanged.

    All MLflow operations are wrapped in try/except to guarantee no
    propagation. If ``ctx.mlflow_anchor_run_id`` is None, the wrapper
    skips logging entirely (used by replay tests).
    """
    def wrapper(ctx: Any, inp: Any) -> Any:
        anchor = getattr(ctx, "mlflow_anchor_run_id", None)
        iteration = getattr(ctx, "iteration", 0)

        try:
            paths = stage_artifact_paths(int(iteration), stage_key)
        except KeyError:
            logger.warning(
                "stage_io_capture: unknown stage_key %s; skipping",
                stage_key,
            )
            return execute(ctx, inp)

        if anchor:
            try:
                _log_text(
                    run_id=str(anchor),
                    text=_serialize_io(inp),
                    artifact_file=paths["input"],
                )
            except Exception:
                logger.warning(
                    "stage_io_capture[%s]: input.json log_text failed",
                    stage_key, exc_info=True,
                )

        captured_decisions: list[Any] = []
        original_emit = ctx.decision_emit

        def _capturing_emit(record: Any) -> None:
            captured_decisions.append(record)
            original_emit(record)

        ctx.decision_emit = _capturing_emit
        try:
            out = execute(ctx, inp)
        finally:
            ctx.decision_emit = original_emit

        if anchor:
            try:
                _log_text(
                    run_id=str(anchor),
                    text=_serialize_io(out),
                    artifact_file=paths["output"],
                )
            except Exception:
                logger.warning(
                    "stage_io_capture[%s]: output.json log_text failed",
                    stage_key, exc_info=True,
                )
            try:
                _log_text(
                    run_id=str(anchor),
                    text=_serialize_io(captured_decisions),
                    artifact_file=paths["decisions"],
                )
            except Exception:
                logger.warning(
                    "stage_io_capture[%s]: decisions.json log_text failed",
                    stage_key, exc_info=True,
                )

        return out
    return wrapper
