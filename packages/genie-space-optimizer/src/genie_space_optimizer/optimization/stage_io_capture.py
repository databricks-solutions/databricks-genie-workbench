"""Per-stage I/O capture decorator (Phase H).

Wraps a stage's ``execute(ctx, inp) -> out`` callable. On every call:

  1. Serializes ``inp`` to JSON via a cycle-safe dataclass walker.
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


_MAX_SERIALIZE_DEPTH = 50


def _cycle_marker(value: Any) -> str:
    return f"<cycle:{type(value).__name__}>"


def _depth_marker(value: Any) -> str:
    return f"<truncated:depth>{type(value).__name__}</truncated>"


def _sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _normalize_for_json(
    value: Any,
    seen: set[int] | None = None,
    *,
    _depth: int = 0,
) -> Any:
    """Recursively convert values to deterministic, JSON-safe shapes.

    Unlike ``dataclasses.asdict()``, this walker tracks the active object
    stack so diagnostic capture can represent cyclic payloads instead of
    recursing until Python raises ``RecursionError`` (Cycle 6 F-6 — run
    833969815458299 hit RecursionError on action_group_selection because
    nested cluster back-pointers exceeded the recursion limit). A depth
    cap at ``_MAX_SERIALIZE_DEPTH`` provides a second guard for very
    deep but acyclic payloads.
    """
    if seen is None:
        seen = set()

    if _depth > _MAX_SERIALIZE_DEPTH:
        return _depth_marker(value)

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        obj_id = id(value)
        if obj_id in seen:
            return _cycle_marker(value)
        seen.add(obj_id)
        try:
            return {
                field.name: _normalize_for_json(
                    getattr(value, field.name), seen, _depth=_depth + 1,
                )
                for field in dataclasses.fields(value)
            }
        finally:
            seen.remove(obj_id)

    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in seen:
            return _cycle_marker(value)
        seen.add(obj_id)
        try:
            return {
                str(k): _normalize_for_json(v, seen, _depth=_depth + 1)
                for k, v in value.items()
            }
        finally:
            seen.remove(obj_id)

    if isinstance(value, (list, tuple)):
        obj_id = id(value)
        if obj_id in seen:
            return _cycle_marker(value)
        seen.add(obj_id)
        try:
            return [
                _normalize_for_json(v, seen, _depth=_depth + 1)
                for v in value
            ]
        finally:
            seen.remove(obj_id)

    if isinstance(value, (set, frozenset)):
        obj_id = id(value)
        if obj_id in seen:
            return _cycle_marker(value)
        seen.add(obj_id)
        try:
            return sorted(
                (
                    _normalize_for_json(v, seen, _depth=_depth + 1)
                    for v in value
                ),
                key=_sort_key,
            )
        finally:
            seen.remove(obj_id)

    return value


# Cycle 6 F-6 — capture-failure ledger. Module-level buffer so
# ``wrap_with_io_capture`` records exceptions without coupling to the
# manifest assembly site; the harness drains the buffer and stamps
# every entry on ``manifest.missing_pieces`` so a silent
# stage_io_capture failure never produces a lying manifest.
_CAPTURE_FAILURES: list[dict] = []


def record_capture_failure(
    *,
    stage_key: str,
    artifact_path: str,
    error_class: str,
) -> None:
    """Record a stage-io capture failure for later manifest propagation."""
    _CAPTURE_FAILURES.append({
        "stage_key": str(stage_key),
        "artifact_path": str(artifact_path),
        "error_class": str(error_class),
    })


def consume_capture_failures() -> list[dict]:
    """Drain the capture-failure buffer (returns and empties)."""
    failures = list(_CAPTURE_FAILURES)
    _CAPTURE_FAILURES.clear()
    return failures


def _serialize_io(obj: Any) -> str:
    """Convert a dataclass instance (or plain dict / list) to a JSON string.

    Dataclasses are walked field-by-field so cyclic nested payloads become
    ``<cycle:...>`` markers. Sets become sorted lists for deterministic
    output.
    """
    return _safe_dumps(_normalize_for_json(obj))


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
            except Exception as exc:
                logger.warning(
                    "stage_io_capture[%s]: input.json log_text failed",
                    stage_key, exc_info=True,
                )
                record_capture_failure(
                    stage_key=stage_key,
                    artifact_path=paths["input"],
                    error_class=type(exc).__name__,
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
            except Exception as exc:
                logger.warning(
                    "stage_io_capture[%s]: output.json log_text failed",
                    stage_key, exc_info=True,
                )
                record_capture_failure(
                    stage_key=stage_key,
                    artifact_path=paths["output"],
                    error_class=type(exc).__name__,
                )
            try:
                _log_text(
                    run_id=str(anchor),
                    text=_serialize_io(captured_decisions),
                    artifact_file=paths["decisions"],
                )
            except Exception as exc:
                logger.warning(
                    "stage_io_capture[%s]: decisions.json log_text failed",
                    stage_key, exc_info=True,
                )
                record_capture_failure(
                    stage_key=stage_key,
                    artifact_path=paths["decisions"],
                    error_class=type(exc).__name__,
                )

        return out
    return wrapper
