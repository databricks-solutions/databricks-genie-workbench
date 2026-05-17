"""Phase 4.5 (2026-05-16) — Task 1 feasibility spike.

This test is INSTRUMENTATION, not pass/fail. It wraps every public
method on the WorkspaceClient and SparkSession that the lever loop
touches in a recorder so the engineer can see which methods the
harness actually calls. The recorded manifest becomes the input
spec for Tasks 3-5 (stub design) and the decision data for Task 2.

The test exits via a sentinel exception after the recorder captures
~200 unique I/O paths so we get the manifest without running the
full loop.

----------------------------------------------------------------------
STATUS (2026-05-16): Phase 4.5 was ABANDONED after this spike. The
plan file (``docs/prompt_improvements/2026-05-16-phase-4-5-harness-
integration-stubbed-io.md``) and the decision doc (``docs/prompt_
improvements/2026-05-16-phase-4-5-decision.md``) record the rationale.
This file remains as documentation of the I/O surface so a future
harness refactor can be measured against it.

KNOWN ENVIRONMENTAL CAVEAT — read before interpreting a re-run.

The spike wraps the ``w`` (WorkspaceClient) and ``spark`` (SparkSession)
parameters with ``_RecordingProxy``, but it does NOT intercept the
module-level ``mlflow`` import at ``harness.py:49``. When ``_run_
lever_loop`` makes its first ``mlflow.*`` call (e.g. ``mlflow.get_
experiment_by_name`` or ``_mlflow.start_run``), it talks to the real
mlflow library, which connects to whatever tracking store
``MLFLOW_TRACKING_URI`` points at on the executor's machine.

If that store is a stale local ``mlflow.db`` SQLite file with an
out-of-date schema, the first mlflow call raises ``MlflowException``
with a message like:

    Detected out-of-date database schema (found version <X>, but
    expected <Y>) ... run 'mlflow db upgrade'

That is what happened on the 2026-05-16 spike run: termination
``crashed:MlflowException`` after 13 ``spark.sql`` calls. The
crash is **environmental**, not architectural — it does not by
itself prove anything about the harness's stubbability.

To take a clean I/O measurement on a future re-run, redirect mlflow
to a throwaway file-store BEFORE calling ``_run_lever_loop``, using
the same pattern as ``tests/integration/test_mlflow_smoke_one_
iteration.py:19``::

    mlflow.set_tracking_uri(f"file://{tmp_path}/mlruns_spike")
    mlflow.set_experiment("phase_4_5_spike")

With that override in place, expect ONE of two outcomes depending
on whether the fixture provides realistic ``benchmarks``:

  * Empty ``benchmarks=[]`` (the current 2026-05-16 default from
    ``load_run_b_59a173d3()``):
      The harness short-circuits via the no-benchmarks early exit
      and reports ``termination=completed_iteration`` with
      ``iteration_counter=0``. The 35 captured calls are run-start
      logging (~13) + run-end / Phase H / contract-health
      emit (~22). **The iteration body was never entered**, so
      this number does NOT measure the architectural cost driver.
      Reading this manifest as "stubbing cost is low" is a category
      error — the spike never reached the cost surface.

  * Fixture-derived non-empty ``benchmarks`` plus structured
    returns from ``w.genie.fetch_space(...)`` and
    ``spark.sql(...).collect()``:
      The spike crashes a few hundred calls later when a MagicMock
      surfaces where a typed ``Row`` / serialized space proto is
      expected. THAT is the architectural cost driver the
      abandonment decision is grounded in (~1,200-1,500 LoC of
      structured-data fixture wiring).
----------------------------------------------------------------------
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest


class _SpikeBreak(Exception):
    pass


class _RecordingProxy:
    """Wrap a target object; record every attribute / call as a path
    string so the spike produces a deterministic manifest at exit."""

    def __init__(
        self,
        name: str,
        target: Any,
        manifest: list[str],
        break_after: int = 200,
    ):
        self._name = name
        self._target = target
        self._manifest = manifest
        self._break_after = break_after

    def __getattr__(self, attr: str):
        path = f"{self._name}.{attr}"
        try:
            child = getattr(self._target, attr)
        except AttributeError:
            child = MagicMock(name=path)
        if callable(child):
            def _wrapped(*args, **kwargs):
                self._manifest.append(
                    f"{path}({len(args)} args, "
                    f"{sorted(kwargs.keys())})"
                )
                if len(self._manifest) >= self._break_after:
                    raise _SpikeBreak(
                        f"{len(self._manifest)} calls recorded; "
                        "spike terminating"
                    )
                try:
                    return child(*args, **kwargs)
                except Exception:
                    return MagicMock(name=path + ":returned")
            return _wrapped
        self._manifest.append(path)
        return child


def test_spike_records_lever_loop_io_surface_for_run_b(tmp_path):
    """Spike instrumentation — records WorkspaceClient + SparkSession
    + LLM call manifest for the first iteration of Run B and writes
    it to a deterministic location. The test passes as long as the
    spike terminates cleanly OR captures non-zero I/O before the
    harness throws.

    Per the docstring caveat above, mlflow is redirected to a fresh
    file-store at ``tmp_path/mlruns_spike`` BEFORE the harness call
    so the environmental schema-mismatch on a stale local
    ``mlflow.db`` does not mask the real architectural I/O surface.
    """
    pytest.importorskip("genie_space_optimizer.optimization.harness")

    # Redirect mlflow to a throwaway file-store so the spike measures
    # the harness's structured-data surface, not whatever stale local
    # tracking store happens to be configured on the executor.
    import mlflow
    mlflow.set_tracking_uri(f"file://{tmp_path}/mlruns_spike")
    try:
        mlflow.set_experiment("phase_4_5_spike")
    except Exception:
        # set_experiment can raise if the new store is not yet
        # writable; we don't care — the harness will re-create the
        # experiment on demand.
        pass

    from genie_space_optimizer.optimization import harness as _h
    from tests.replay.active._postmortem_fixtures import (
        load_run_b_59a173d3,
    )

    fixture = load_run_b_59a173d3()

    manifest: list[str] = []
    w = _RecordingProxy("w", MagicMock(name="WorkspaceClient"), manifest)
    spark = _RecordingProxy("spark", MagicMock(name="SparkSession"), manifest)

    termination: str = "unknown"
    exc_repr: str = ""
    try:
        _h._run_lever_loop(
            w=w,  # type: ignore[arg-type]
            spark=spark,  # type: ignore[arg-type]
            run_id="spike",
            space_id=str(fixture.get("space_id") or "s1"),
            domain="airline",
            benchmarks=list(fixture.get("benchmarks") or ()),
            exp_name="spike",
            prev_scores={},
            prev_accuracy=0.0,
            prev_model_id="",
            config=dict(fixture.get("config") or {}),
            catalog="main",
            schema="genie_space_optimizer",
            max_iterations=1,
            thresholds={"overall_accuracy": 1.0},
        )
        termination = "completed_iteration"
    except _SpikeBreak as exc:
        termination = "spike_break"
        exc_repr = repr(exc)
    except Exception as exc:
        termination = f"crashed:{type(exc).__name__}"
        exc_repr = repr(exc)[:300]

    # Write manifest to a deterministic file that survives the test.
    manifest_path = (
        Path(__file__).parent / ".spike-manifest-2026-05-16.txt"
    )
    lines = [
        f"# Phase 4.5 spike manifest — Run B (59a173d3)",
        f"# Total recorded I/O paths: {len(manifest)}",
        f"# Termination: {termination}",
        f"# Exception (if any): {exc_repr}",
        "",
    ]
    lines.extend(manifest)
    manifest_path.write_text("\n".join(lines))

    # Print summary for the engineer.
    print(f"\nrecorded {len(manifest)} I/O calls; termination={termination}")
    print(f"manifest at: {manifest_path}")
    print(f"exception: {exc_repr or '(none)'}")
    print("\nFirst 40 calls:")
    for line in manifest[:40]:
        print(f"  {line}")
    if len(manifest) > 40:
        print(f"  ... ({len(manifest) - 40} more)")

    assert manifest_path.exists()
