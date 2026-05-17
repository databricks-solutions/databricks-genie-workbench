"""Phase 4.5 (2026-05-16) — Task 1 feasibility spike.

This test is INSTRUMENTATION, not pass/fail. It wraps every public
method on the WorkspaceClient and SparkSession that the lever loop
touches in a recorder so the engineer can see which methods the
harness actually calls. The recorded manifest becomes the input
spec for Tasks 3-5 (stub design) and the decision data for Task 2.

The test exits via a sentinel exception after the recorder captures
~200 unique I/O paths so we get the manifest without running the
full loop.
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


def test_spike_records_lever_loop_io_surface_for_run_b():
    """Spike instrumentation — records WorkspaceClient + SparkSession
    + LLM call manifest for the first iteration of Run B and writes
    it to a deterministic location. The test passes as long as the
    spike terminates cleanly OR captures non-zero I/O before the
    harness throws.
    """
    pytest.importorskip("genie_space_optimizer.optimization.harness")

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
