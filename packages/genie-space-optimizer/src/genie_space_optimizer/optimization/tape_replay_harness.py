"""Context manager that wires a LeverLoopTape into the lever loop.

Responsibilities:
    1. Install ``_LLM_CALLER_OVERRIDE`` (consulted by ``_traced_llm_call``).
    2. Patch ``evaluation.run_evaluation`` to return tape-recorded eval
       rows for the current iteration binding.
    3. Patch ``common.genie_client.patch_space_config`` so replay never
       hits the Genie API; calls are captured for assertions.
    4. Patch ``optimization.state.write_stage`` so replay never writes
       to Delta; calls are captured for assertions.

Iteration binding:
    The harness exposes ``bind_iteration(i)`` and ``bind_ag(ag, cluster)``.
    Tests call these to advance the binding. When the lever loop is
    driven end-to-end, the harness consults the iteration counter that
    ``_run_lever_loop`` already maintains via the binding hook
    registered in ``__enter__``.
"""
from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch as _mock_patch

from genie_space_optimizer.optimization.optimizer import (
    _LLM_CALLER_OVERRIDE,
)
from genie_space_optimizer.optimization.tape import LeverLoopTape
from genie_space_optimizer.optimization.tape_llm_caller import (
    TapeCallContext,
)


@dataclass
class LeverLoopReplayHarness:
    """Context manager that wires a tape into the lever loop.

    Usage:
        with LeverLoopReplayHarness(tape=tape) as h:
            h.bind_iteration(0)
            ... drive harness._run_lever_loop ...
            assert h.captured_patches == [...]
    """

    tape: LeverLoopTape
    captured_patches: list[dict] = field(default_factory=list)
    captured_write_stage_calls: list[dict] = field(default_factory=list)
    _context: TapeCallContext | None = None
    _token: Any = None
    _exit_stack: contextlib.ExitStack | None = None

    def __enter__(self) -> "LeverLoopReplayHarness":
        self._context = TapeCallContext(tape=self.tape)
        self._token = _LLM_CALLER_OVERRIDE.set(self._context.caller())
        self._exit_stack = contextlib.ExitStack()

        # Replace MLflow run/logging operations with no-ops so the
        # lever loop's tracing does not hit a real backend during
        # replay. ``mlflow.start_run`` is the only one that needs to
        # return a context-manager — we wrap a synthetic Run shape.
        import contextlib as _ctxlib

        @_ctxlib.contextmanager
        def _noop_start_run(*args, **kwargs):
            class _NoopRun:
                class _Info:
                    run_id = "replay-noop"
                    experiment_id = "replay-experiment"
                info = _Info()
            yield _NoopRun()

        for _target in (
            "mlflow.start_run",
            "mlflow.log_param",
            "mlflow.log_params",
            "mlflow.log_metric",
            "mlflow.log_metrics",
            "mlflow.log_text",
            "mlflow.log_artifact",
            "mlflow.log_dict",
            "mlflow.set_tag",
            "mlflow.set_tags",
            "mlflow.set_experiment",
            "mlflow.set_tracking_uri",
            "mlflow.active_run",
            "mlflow.last_active_run",
            "mlflow.end_run",
        ):
            try:
                if _target == "mlflow.start_run":
                    self._exit_stack.enter_context(
                        _mock_patch(_target, side_effect=_noop_start_run)
                    )
                elif _target == "mlflow.active_run":
                    self._exit_stack.enter_context(
                        _mock_patch(_target, return_value=None)
                    )
                else:
                    self._exit_stack.enter_context(
                        _mock_patch(_target, return_value=None)
                    )
            except (AttributeError, ModuleNotFoundError):
                continue

        # Patch evaluation.run_evaluation to return tape-recorded eval rows.
        def _replay_run_evaluation(*args, **kwargs) -> dict:
            iteration = int(kwargs.get("iteration") or 0)
            rows = self.tape.evals_by_iteration.get(iteration, [])
            return {
                "eval_rows": list(rows),
                "evaluated_count": len(rows),
                "model_id": "replay",
            }

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.optimization.evaluation.run_evaluation",
                side_effect=_replay_run_evaluation,
            )
        )
        # Some lever-loop call sites import via the harness module's
        # local ``run_evaluation`` reference (re-export). Patch that too
        # if it exists.
        try:
            self._exit_stack.enter_context(
                _mock_patch(
                    "genie_space_optimizer.optimization.harness.run_evaluation",
                    side_effect=_replay_run_evaluation,
                )
            )
        except AttributeError:
            pass

        # Patch patch_space_config to a no-op that captures calls.
        def _replay_patch_space_config(w, space_id, config, **kwargs) -> dict:
            self.captured_patches.append({
                "space_id": str(space_id),
                "config_keys": sorted((config or {}).keys()),
            })
            return {"replay": True}

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.common.genie_client.patch_space_config",
                side_effect=_replay_patch_space_config,
            )
        )

        # Patch write_stage to capture-only.
        def _replay_write_stage(
            spark, run_id, stage, status, *, task_key=None, catalog="",
            schema="", **kwargs,
        ) -> None:
            self.captured_write_stage_calls.append({
                "run_id": str(run_id),
                "stage": str(stage),
                "status": str(status),
                "task_key": str(task_key or ""),
            })

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.optimization.state.write_stage",
                side_effect=_replay_write_stage,
            )
        )

        # Install the tape binding hook so _run_lever_loop can advance
        # iteration/ag tracking on this harness's TapeCallContext.
        def _binding_hook(
            iteration: int, *, ag_id: str = "", cluster_id: str = "",
        ) -> None:
            if ag_id:
                self.bind_ag(ag_id, cluster_id=cluster_id)
            else:
                self.bind_iteration(iteration)

        from genie_space_optimizer.optimization import harness as _harness
        self._exit_stack.enter_context(
            _mock_patch.object(
                _harness, "_TAPE_BINDING_HOOK", _binding_hook,
            )
        )

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._exit_stack is not None:
            self._exit_stack.close()
            self._exit_stack = None
        if self._token is not None:
            _LLM_CALLER_OVERRIDE.reset(self._token)
            self._token = None
        self._context = None

    # ── Binding API ─────────────────────────────────────────────────

    def bind_iteration(self, iteration: int) -> None:
        if self._context is None:
            raise RuntimeError("Harness must be entered before binding.")
        self._context.set_iteration(iteration)

    def bind_ag(self, ag_id: str, *, cluster_id: str = "") -> None:
        if self._context is None:
            raise RuntimeError("Harness must be entered before binding.")
        self._context.bind_ag(ag_id, cluster_id=cluster_id)

    def clear_ag(self) -> None:
        if self._context is None:
            raise RuntimeError("Harness must be entered before binding.")
        self._context.clear_ag()
