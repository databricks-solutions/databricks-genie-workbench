"""RCO-4b consolidating-trial postflight — validates the captured
trial-run evidence against ``expected_outcomes.json``.

Skip-by-default: the test runs only when the operator points it at a
captured evidence-bundle directory via the
``RCO4B_TRIAL_EVIDENCE_DIR`` and ``RCO4B_TRIAL_ANCHOR_NAME`` env
vars.

Expected workflow (Task 7 / Task 8 of the consolidating-trial plan):

  export RCO4B_TRIAL_EVIDENCE_DIR=packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>/evidence
  export RCO4B_TRIAL_ANCHOR_NAME=f9_3b050ec5  # or airline_clean
  uv run pytest tests/integration/test_rco4b_trial_postflight_artifact_capture.py -v

The directory layout the test expects matches what the
``evidence-bundle`` CLI writes (see
``tools.evidence_layout.bundle_paths_for``).
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Mapping

import pytest

from genie_space_optimizer.optimization.contract_health import (
    ContractHealthSummary,
    MergeGateStatus,
)
from genie_space_optimizer.tools.marker_parser import parse_markers

EXPECTED_OUTCOMES_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "rco4b_trial"
    / "expected_outcomes.json"
)


def _evidence_dir() -> pathlib.Path | None:
    raw = os.environ.get("RCO4B_TRIAL_EVIDENCE_DIR")
    if not raw:
        return None
    p = pathlib.Path(raw)
    return p if p.is_dir() else None


def _anchor_name() -> str | None:
    return os.environ.get("RCO4B_TRIAL_ANCHOR_NAME") or None


@pytest.fixture(scope="module")
def evidence_dir() -> pathlib.Path:
    p = _evidence_dir()
    if p is None:
        pytest.skip(
            "RCO4B_TRIAL_EVIDENCE_DIR not set or directory missing — "
            "Task 7 has not produced evidence yet"
        )
    return p


@pytest.fixture(scope="module")
def anchor_name() -> str:
    name = _anchor_name()
    if name is None:
        pytest.skip("RCO4B_TRIAL_ANCHOR_NAME not set")
    return name


@pytest.fixture(scope="module")
def expected_outcome(anchor_name) -> Mapping[str, Any]:
    payload = json.loads(EXPECTED_OUTCOMES_PATH.read_text())
    anchors = payload.get("anchors") or {}
    if anchor_name not in anchors:
        pytest.fail(
            f"anchor {anchor_name!r} not in expected_outcomes.json — "
            f"valid anchors: {sorted(anchors)}"
        )
    return anchors[anchor_name]


@pytest.fixture(scope="module")
def stdout_text(evidence_dir) -> str:
    """Locate the captured stdout. The evidence-bundle CLI writes
    two candidate files:

      * ``lever_loop_stdout.txt`` — sometimes empty on Databricks
        runs where the job's stdout sink wasn't materialized
      * ``lever_loop_latest_export_run_<task_run_id>_text.txt`` —
        the decoded notebook export, which always contains the
        full transcript including end-of-run markers

    Prefer the latter when present and non-empty. Fall back to
    any non-empty ``stdout*.txt``. Skip if both are missing or empty.
    """
    export = sorted(evidence_dir.glob("lever_loop_latest_export_run_*_text.txt"))
    for path in export:
        text = path.read_text()
        if text.strip():
            return text
    for path in sorted(evidence_dir.glob("stdout*.txt")) + sorted(
        evidence_dir.glob("lever_loop_stdout.txt")
    ):
        text = path.read_text()
        if text.strip():
            return text
    pytest.fail(
        f"no non-empty stdout transcript under {evidence_dir} — "
        f"evidence-bundle did not capture the lever-loop stdout"
    )


@pytest.fixture(scope="module")
def marker_log(stdout_text):
    return parse_markers(stdout_text)


def test_contract_health_marker_is_present(marker_log):
    assert marker_log.contract_health is not None, (
        "GSO_CONTRACT_HEALTH_V1 marker absent from captured stdout — "
        "RCO-2a end-of-run emission did not fire; trial does not "
        "satisfy the RCO-2b named blocker"
    )


def test_contract_health_payload_roundtrips(marker_log):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    assert summary.optimization_run_id, (
        "ContractHealthSummary missing optimization_run_id"
    )


def test_merge_gate_status_matches_expected(marker_log, expected_outcome):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    expected = MergeGateStatus(
        str(expected_outcome["expected_merge_gate_status"])
    )
    assert summary.merge_gate_status == expected, (
        f"merge_gate_status mismatch: got "
        f"{summary.merge_gate_status.value}, expected {expected.value}"
    )


def test_high_tier_invariants_match_expected(marker_log, expected_outcome):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    expected_ids = set(
        expected_outcome.get("expected_high_tier_invariant_ids") or []
    )
    min_count = int(
        expected_outcome.get("expected_high_tier_min_count") or 0
    )
    actual_ids = {
        str(v.get("invariant_id") or "")
        for v in summary.high_tier_violations
    }
    if expected_ids:
        missing = expected_ids - actual_ids
        assert not missing, (
            f"expected HIGH-tier invariant IDs {sorted(missing)} not "
            f"emitted; got {sorted(actual_ids)}"
        )
    assert len(summary.high_tier_violations) >= min_count, (
        f"too few HIGH-tier violations: got "
        f"{len(summary.high_tier_violations)}, expected ≥ {min_count}"
    )


def test_bundle_status_matches_expected(marker_log, expected_outcome):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    allowed = set(expected_outcome["expected_bundle_status_in"])
    assert summary.bundle_status in allowed, (
        f"bundle_status {summary.bundle_status!r} not in allowed "
        f"set {sorted(allowed)} for anchor"
    )


def test_run_manifest_and_convergence_markers_present(marker_log):
    assert marker_log.run_manifest is not None, (
        "GSO_RUN_MANIFEST_V1 missing — Phase B / end-of-run emission "
        "broken"
    )
    assert marker_log.convergence is not None, (
        "GSO_CONVERGENCE_V1 missing — lever-loop did not finalize"
    )


def test_keystone_marker_emission_order(stdout_text):
    """The four keystone end-of-run markers must fire in the order:

      GSO_RUN_MANIFEST_V1 (event=start)
        < GSO_CONVERGENCE_V1
        < GSO_RUN_MANIFEST_V1 (event=end)
        < GSO_CONTRACT_HEALTH_V1

    This is the stdout-observable invariant that proves the
    end-of-run emission pipeline ran to completion. The per-stage
    ``gate_name="..."`` sentinels live in the persisted decision-trace
    JSON (Phase H bundle), not stdout, so they are pinned by the
    source-level guard in ``tests/unit/test_rco4b_run_gate_checks_sequence_guard.py``,
    not here.
    """
    manifest_start_pos = stdout_text.find('GSO_RUN_MANIFEST_V1 {"databricks_job_id"')
    while manifest_start_pos != -1:
        line_end = stdout_text.find("\n", manifest_start_pos)
        line = stdout_text[manifest_start_pos:line_end if line_end > 0 else len(stdout_text)]
        if '"event":"start"' in line:
            break
        manifest_start_pos = stdout_text.find(
            'GSO_RUN_MANIFEST_V1 {"databricks_job_id"', manifest_start_pos + 1
        )
    assert manifest_start_pos >= 0, (
        "GSO_RUN_MANIFEST_V1 event=start not found in captured stdout"
    )

    convergence_pos = stdout_text.find("GSO_CONVERGENCE_V1 ")
    assert convergence_pos >= 0, (
        "GSO_CONVERGENCE_V1 not found in captured stdout — lever-loop "
        "did not reach end-of-run"
    )

    manifest_end_pos = stdout_text.find('"event":"end"', convergence_pos)
    assert manifest_end_pos >= 0, (
        "GSO_RUN_MANIFEST_V1 event=end not found after convergence — "
        "end-of-run emission did not fire"
    )

    contract_health_pos = stdout_text.find("GSO_CONTRACT_HEALTH_V1 ")
    assert contract_health_pos >= 0, (
        "GSO_CONTRACT_HEALTH_V1 not found in captured stdout — "
        "RCO-2a keystone marker did not fire"
    )

    assert manifest_start_pos < convergence_pos < manifest_end_pos < contract_health_pos, (
        f"end-of-run marker order violated: start={manifest_start_pos}, "
        f"convergence={convergence_pos}, end={manifest_end_pos}, "
        f"contract_health={contract_health_pos}"
    )


def test_replay_violation_count_within_expected(
    marker_log, expected_outcome
):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    min_count = expected_outcome.get("expected_replay_violation_min_count")
    max_count = expected_outcome.get("expected_replay_violation_max_count")
    if min_count is not None:
        assert summary.replay_violation_count >= int(min_count), (
            f"replay violation count too low: "
            f"{summary.replay_violation_count} < {min_count}"
        )
    if max_count is not None:
        assert summary.replay_violation_count <= int(max_count), (
            f"replay violation count too high: "
            f"{summary.replay_violation_count} > {max_count}"
        )
