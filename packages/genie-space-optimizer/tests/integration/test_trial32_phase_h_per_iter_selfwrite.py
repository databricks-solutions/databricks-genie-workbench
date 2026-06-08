"""Trial 32 W32.4 — Phase-H per-iteration self-write completeness.

The Phase-H strict validator (``_run_phase_h_strict_validation``) runs
BEFORE ``_materialize_per_iter_contract_paths`` writes the 8 per-iteration
contract artifacts (same ``iterations`` / ``anchor_run_id``). Before W32.4
the validator only registered 4 PARENT-level paths as ``self_write_paths``,
so every per-iteration declared path was false-flagged
``manifest_path_missing`` (airline live ``missing_count=29``, 7now ``=37``),
holding ``architecture_invariants_held=false``.

W32.4 teaches the validator that the per-iteration paths are
unconditionally about-to-be-written, and has the materializer return its
successfully-written paths so the post-upload completeness check is not at
the mercy of eventually-consistent MLflow listing.

All cases use a non-anchor synthetic run id to prove generality (no anchor
space-id / QID literals).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import pytest

from genie_space_optimizer.optimization.harness import (
    _materialize_per_iter_contract_paths,
    _run_phase_h_strict_validation,
)
from genie_space_optimizer.optimization.run_output_contract import (
    bundle_artifact_paths,
    validate_phase_h_manifest_paths,
)

_SYNTH_RUN = "opt_synth_w324"
_SYNTH_ANCHOR = "anchor_synth_w324"


@dataclass
class _Artifact:
    path: str
    is_dir: bool = False


class _EmptyMlflowClient:
    """Lists nothing — simulates the validator running before any write."""

    def list_artifacts(self, run_id: str, prefix: str):
        return []


def _flatten_per_iter_paths(iterations: list[int]) -> set[str]:
    decl = bundle_artifact_paths(iterations=iterations)
    out: set[str] = set()
    for iter_paths in (decl.get("iterations") or {}).values():
        for p in (iter_paths or {}).values():
            if isinstance(p, str):
                out.add(p)
    return out


def _run_validator():
    return _run_phase_h_strict_validation(
        optimization_run_id=_SYNTH_RUN,
        iterations_completed=[1, 2],
        anchor_run_id=_SYNTH_ANCHOR,
        bundle_artifact_paths_fn=bundle_artifact_paths,
        validate_paths_fn=validate_phase_h_manifest_paths,
        flag_enabled_fn=lambda: True,
        mlflow_client_factory=_EmptyMlflowClient,
    )


def test_per_iter_paths_excluded_from_missing_when_flag_on(monkeypatch) -> None:
    """Default-ON: no per-iteration path is flagged missing — they are
    self-writes the materializer will produce for the same iterations."""
    monkeypatch.delenv("GSO_TRIAL32_PHASE_H_PER_ITER_SELFWRITE", raising=False)
    monkeypatch.delenv("GSO_TRIAL32", raising=False)

    new_missing, payload = _run_validator()

    per_iter = _flatten_per_iter_paths([1, 2])
    missing_paths = {m["artifact_path"] for m in new_missing}
    assert per_iter, "fixture must declare per-iteration paths"
    assert not (missing_paths & per_iter), (
        "per-iteration paths must be treated as self-writes, not missing: "
        f"{sorted(missing_paths & per_iter)}"
    )
    # self_write_count grows to cover the 4 parent paths + every per-iter path.
    assert payload["self_write_count"] >= 4 + len(per_iter)


def test_byte_stable_when_flag_off(monkeypatch) -> None:
    """Master OFF restores Trial-31 behaviour: per-iteration paths are NOT
    self-writes, so the validator flags every one of them missing."""
    monkeypatch.setenv("GSO_TRIAL32", "0")

    new_missing, payload = _run_validator()

    per_iter = _flatten_per_iter_paths([1, 2])
    missing_paths = {m["artifact_path"] for m in new_missing}
    assert per_iter <= missing_paths, (
        "with W32.4 off, every per-iteration path is flagged missing "
        "(byte-stable Trial-31 behaviour)"
    )
    assert payload["self_write_count"] == 4


def test_subflag_off_is_byte_stable(monkeypatch) -> None:
    """The dedicated sub-flag opt-out alone restores Trial-31 behaviour."""
    monkeypatch.delenv("GSO_TRIAL32", raising=False)
    monkeypatch.setenv("GSO_TRIAL32_PHASE_H_PER_ITER_SELFWRITE", "0")

    new_missing, _payload = _run_validator()

    per_iter = _flatten_per_iter_paths([1, 2])
    missing_paths = {m["artifact_path"] for m in new_missing}
    assert per_iter <= missing_paths


class _RecordingMlflowClient:
    """Records every ``log_text`` artifact_file so the test can confirm the
    materializer reports exactly what it wrote."""

    def __init__(self) -> None:
        self.written: list[str] = []

    def log_text(self, *, run_id: str, text: str, artifact_file: str) -> None:
        self.written.append(artifact_file)


def test_materializer_returns_written_paths() -> None:
    """``_materialize_per_iter_contract_paths`` returns the set of paths it
    successfully wrote, so the post-upload completeness check can union them
    rather than trust an eventually-consistent re-listing."""
    client = _RecordingMlflowClient()
    result = _materialize_per_iter_contract_paths(
        client=client,
        anchor_run_id=_SYNTH_ANCHOR,
        iterations=[1, 2],
        iter_summaries={},
        iter_decision_records={},
        iter_journey_reports={},
        iter_rca_ledgers={},
        iter_proposal_inventories={},
        iter_transcripts={},
        stage_capture_index={},
        iter_invariant_violations={},
    )

    per_iter = _flatten_per_iter_paths([1, 2])
    assert "written_paths" in result
    written = set(result["written_paths"])
    # Every declared per-iteration path was written (best-effort succeeded
    # against the recording stub), and the writer reports them all back.
    assert written == per_iter
    assert result["written"] == len(per_iter)
    assert set(client.written) == per_iter
