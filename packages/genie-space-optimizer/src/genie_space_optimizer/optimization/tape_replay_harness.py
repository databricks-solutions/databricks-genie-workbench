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

    Phase 3.5 (2026-05-17) — ``stub_side_effects_only`` mode installs
    every side-effect stub (eval, patch, write_stage, MLflow) but
    skips the LLM override + tape binding hook. Use it for capture-
    mode tests that need to run ``_run_lever_loop`` offline against
    a stubbed OpenAI client (e.g. Phase 3.5 Task 3 integration).
    """

    tape: LeverLoopTape | None = None
    stub_side_effects_only: bool = False
    captured_patches: list[dict] = field(default_factory=list)
    captured_write_stage_calls: list[dict] = field(default_factory=list)
    _context: TapeCallContext | None = None
    _token: Any = None
    _exit_stack: contextlib.ExitStack | None = None

    def __enter__(self) -> "LeverLoopReplayHarness":
        if not self.stub_side_effects_only:
            if self.tape is None:
                raise ValueError(
                    "LeverLoopReplayHarness requires a tape unless "
                    "stub_side_effects_only=True (capture mode)."
                )
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

        # Phase 3.6.2 E2 (2026-05-18) — replay stub for
        # ``state.load_latest_full_iteration``. Production reads this
        # via a Spark/Delta query (``genie_opt_iterations``); under
        # MagicMock spark the real loader returns None and the
        # lever loop short-circuits at no_actionable_clusters before
        # reaching any iteration body. Serve the tape's
        # iteration_payloads instead: pick the highest 0-indexed
        # iteration with ``eval_scope='full'`` and ``rolled_back !=
        # True``, respecting the optional ``before_iteration``
        # filter (1-indexed in the call site).
        def _replay_load_latest_full_iteration(
            spark, run_id, catalog, schema,
            *, include_rolled_back=False, before_iteration=None,
        ):
            payloads = self.tape.iteration_payloads or {}
            if not payloads:
                return None
            candidates: list[tuple[int, dict]] = []
            for idx, payload in payloads.items():
                if str(payload.get("eval_scope", "full")) != "full":
                    continue
                if not include_rolled_back and bool(
                    payload.get("rolled_back", False)
                ):
                    continue
                # ``before_iteration`` filters by the human-readable
                # 1-indexed value in the payload; loader call sites
                # pass that form (e.g., ``before_iteration=_iter_num``).
                if before_iteration is not None and int(
                    payload.get("iteration", idx + 1)
                ) >= int(before_iteration):
                    continue
                candidates.append((int(idx), payload))
            if not candidates:
                return None
            # Highest idx wins (latest by construction).
            candidates.sort(key=lambda kv: kv[0], reverse=True)
            return dict(candidates[0][1])

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.optimization.state."
                "load_latest_full_iteration",
                side_effect=_replay_load_latest_full_iteration,
            )
        )
        # Also patch the harness's local re-import.
        try:
            self._exit_stack.enter_context(
                _mock_patch(
                    "genie_space_optimizer.optimization.harness."
                    "load_latest_full_iteration",
                    side_effect=_replay_load_latest_full_iteration,
                )
            )
        except AttributeError:
            pass

        # Phase 3.6.2 E3 (2026-05-18) — replay stub for
        # ``state.load_latest_state_iteration``. Same source as
        # ``load_latest_full_iteration`` but accepts ``eval_scope IN
        # ('full', 'enrichment')`` and orders by (iteration desc,
        # timestamp desc). For tape replay we don't have timestamps,
        # so the highest-index payload with a matching scope wins.
        def _replay_load_latest_state_iteration(
            spark, run_id, catalog, schema,
            *, include_rolled_back=False,
        ):
            payloads = self.tape.iteration_payloads or {}
            if not payloads:
                return None
            candidates: list[tuple[int, dict]] = []
            for idx, payload in payloads.items():
                scope = str(payload.get("eval_scope", "full"))
                if scope not in ("full", "enrichment"):
                    continue
                if not include_rolled_back and bool(
                    payload.get("rolled_back", False)
                ):
                    continue
                candidates.append((int(idx), payload))
            if not candidates:
                return None
            candidates.sort(key=lambda kv: kv[0], reverse=True)
            return dict(candidates[0][1])

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.optimization.state."
                "load_latest_state_iteration",
                side_effect=_replay_load_latest_state_iteration,
            )
        )
        try:
            self._exit_stack.enter_context(
                _mock_patch(
                    "genie_space_optimizer.optimization.harness."
                    "load_latest_state_iteration",
                    side_effect=_replay_load_latest_state_iteration,
                )
            )
        except AttributeError:
            pass

        # Phase 3.6.2 E3 — replay stub for
        # ``state.load_all_full_iterations``. Returns ALL ``full``
        # scope payloads ordered by iteration ASC (mirrors
        # production's ``ORDER BY iteration ASC``).
        def _replay_load_all_full_iterations(
            spark, run_id, catalog, schema,
        ):
            payloads = self.tape.iteration_payloads or {}
            rows = [
                dict(p) for idx, p in sorted(payloads.items())
                if str(p.get("eval_scope", "full")) == "full"
                and not bool(p.get("rolled_back", False))
            ]
            return rows

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.optimization.state."
                "load_all_full_iterations",
                side_effect=_replay_load_all_full_iterations,
            )
        )
        try:
            self._exit_stack.enter_context(
                _mock_patch(
                    "genie_space_optimizer.optimization.harness."
                    "load_all_full_iterations",
                    side_effect=_replay_load_all_full_iterations,
                )
            )
        except AttributeError:
            pass

        # Phase 3.6.2 E3 — replay stub for ``state.load_run``.
        # Production reads the run-metadata row from
        # ``genie_opt_runs``; under replay we synthesize a minimal
        # dict from tape-level fields. Most consumers read
        # ``space_id``, ``levers``, ``config_snapshot``,
        # ``started_at`` — we return enough for graceful
        # back-compat; downstream consumers that need richer
        # metadata will surface as the next stop-and-report.
        def _replay_load_run(spark, run_id, catalog, schema):
            return {
                "run_id": str(run_id),
                "space_id": "",
                "status": "running",
                "levers": [],
                "config_snapshot": {},
                "started_at": str(self.tape.captured_at or ""),
                "source_run_id": str(self.tape.source_run_id or ""),
            }

        self._exit_stack.enter_context(
            _mock_patch(
                "genie_space_optimizer.optimization.state.load_run",
                side_effect=_replay_load_run,
            )
        )
        try:
            self._exit_stack.enter_context(
                _mock_patch(
                    "genie_space_optimizer.optimization.harness.load_run",
                    side_effect=_replay_load_run,
                )
            )
        except AttributeError:
            pass

        # Phase 3.6.2 E4 (2026-05-18) — empty-stub the two secondary
        # state.py loaders that fire mid-loop / post-loop but are
        # not iteration-critical for the anchor regression contract:
        #
        # ``load_stages`` reads the genie_opt_stages Delta table for
        # stage transitions; consumed by post-loop summary and the
        # ``rerun_history`` helper. Empty pd.DataFrame is benign.
        #
        # ``load_provenance`` reads the genie_opt_provenance Delta
        # table; consumed by proposal provenance lookups. Empty
        # pd.DataFrame is benign — provenance lookups return
        # "no prior provenance" rather than crashing.
        #
        # Documented in ``tape-replay-protocol.md``: extend if a
        # future replay test asserts on post-loop / provenance state.
        import pandas as _pd_for_replay

        def _replay_load_stages(spark, run_id, catalog, schema):
            return _pd_for_replay.DataFrame()

        def _replay_load_provenance(spark, run_id, catalog, schema):
            return _pd_for_replay.DataFrame()

        for _target, _side in (
            ("genie_space_optimizer.optimization.state.load_stages",
             _replay_load_stages),
            ("genie_space_optimizer.optimization.state.load_provenance",
             _replay_load_provenance),
            ("genie_space_optimizer.optimization.harness.load_stages",
             _replay_load_stages),
            ("genie_space_optimizer.optimization.harness.load_provenance",
             _replay_load_provenance),
        ):
            try:
                self._exit_stack.enter_context(
                    _mock_patch(_target, side_effect=_side)
                )
            except AttributeError:
                # Re-export may not exist in the harness module.
                continue

        # Phase 3.6.2 E5b (2026-05-18) — clustering is UPSTREAM of
        # the lever-loop decision logic Phase 0+1+2 changed; it
        # consumes per-row ASI metadata that lives in
        # ``genie_eval_asi_results`` (a Delta table the historic
        # export does not bundle) and produces a deterministic
        # grouping. Re-deriving clusters under replay from raw
        # rows + empty ASI returns 0 clusters and every iteration
        # exits ``no_actionable_clusters``.
        #
        # The right boundary is to tape-serve the COMPUTED clusters
        # that production already wrote (one ``clusters`` + one
        # ``soft_clusters`` per iteration_payload). The lever-loop
        # decision code receives byte-identical input to what
        # production saw. Clustering itself has its own test
        # surface (``tests/unit/test_cluster_failures.py``), not
        # the anchor replay.
        from genie_space_optimizer.optimization.llm_call_recorder import (
            _RECORDER_BINDING as _phase35e5b_binding,
        )

        def _replay_cluster_failures(
            eval_results, metadata_snapshot=None,
            *, signal_type="hard", **kwargs,
        ):
            binding = _phase35e5b_binding.get()
            iter_idx = (
                int(binding.iteration)
                if binding.iteration is not None and binding.iteration >= 0
                else 0
            )
            payload = (self.tape.iteration_payloads or {}).get(iter_idx)
            if not payload:
                return []
            if str(signal_type) == "hard":
                return [dict(c) for c in (payload.get("clusters") or [])]
            if str(signal_type) == "soft":
                return [dict(c) for c in (payload.get("soft_clusters") or [])]
            return []

        for _target in (
            "genie_space_optimizer.optimization.optimizer.cluster_failures",
            "genie_space_optimizer.optimization.stages.clustering.cluster_failures",
        ):
            try:
                self._exit_stack.enter_context(
                    _mock_patch(_target, side_effect=_replay_cluster_failures)
                )
            except (AttributeError, ModuleNotFoundError):
                continue

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

        if not self.stub_side_effects_only:
            # Install the tape binding hook so _run_lever_loop can
            # advance iteration/ag tracking on this harness's
            # TapeCallContext. In stub_side_effects_only mode we
            # skip this so production binding helpers (Phase 3.5
            # recorder binding) fire instead.
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

    # ── stub_side_effects_only callers don't have a TapeCallContext
    # to bind. The binding API raises a clear error in that mode.

    def _require_context(self, op: str) -> None:
        if self._context is None:
            if self.stub_side_effects_only:
                raise RuntimeError(
                    f"{op} unavailable in stub_side_effects_only mode "
                    f"(no tape installed; capture-mode tests do not "
                    f"need binding)."
                )
            raise RuntimeError(
                f"Harness must be entered before {op}."
            )

    # ── Binding API ─────────────────────────────────────────────────

    def bind_iteration(self, iteration: int) -> None:
        self._require_context("bind_iteration")
        self._context.set_iteration(iteration)

    def bind_ag(self, ag_id: str, *, cluster_id: str = "") -> None:
        self._require_context("bind_ag")
        self._context.bind_ag(ag_id, cluster_id=cluster_id)

    def clear_ag(self) -> None:
        self._require_context("clear_ag")
        self._context.clear_ag()
