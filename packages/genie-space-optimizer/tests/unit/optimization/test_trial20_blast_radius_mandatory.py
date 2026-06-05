"""Trial 20 Workstream E — blast-radius mandatory gating.

Pins:

* E2 — when ``passing_dependents`` is absent on a patch and
  ``GSO_TRIAL20_BLAST_RADIUS_MANDATORY`` is ON, the gate returns
  ``safe=False`` with ``reason="passing_dependents_missing"`` instead
  of taking the pre-Trial-20 safe-by-default branch.
* E2 — when the flag is OFF, the legacy ``safe=True`` /
  ``reason="no_passing_dependents_field"`` behaviour is preserved so
  the byte-stable rollback path still works.
* E2 — when ``passing_dependents`` IS stamped, the gate evaluates the
  dependents/target set normally (regression guard around the new
  fallback branch).
* E1 — :func:`compute_passing_dependents_for_proposal` returns the
  expected (dependents, high_risk) pair so the SM stamping path
  matches the harness-direct contract.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.proposal_grounding import (
    compute_passing_dependents_for_proposal,
    patch_blast_radius_is_safe,
)


@pytest.fixture
def trial20_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL20_BLAST_RADIUS_MANDATORY", "1")


@pytest.fixture
def trial20_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "0")
    monkeypatch.setenv("GSO_TRIAL20_BLAST_RADIUS_MANDATORY", "0")


def test_e2_missing_passing_dependents_rejects_under_flag_on(
    trial20_on,
) -> None:
    patch = {
        "patch_type": "add_instruction",
        "intent_id": "AG_X",
    }
    out = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("gs_001",),
    )
    assert out["safe"] is False
    assert out["reason"] == "passing_dependents_missing"


def test_e2_missing_passing_dependents_safe_under_flag_off(
    trial20_off,
) -> None:
    patch = {
        "patch_type": "add_instruction",
        "intent_id": "AG_X",
    }
    out = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("gs_001",),
    )
    assert out["safe"] is True
    assert out["reason"] == "no_passing_dependents_field"


def test_e2_stamped_passing_dependents_evaluates_normally(
    trial20_on,
) -> None:
    """When the field IS stamped, gate runs its real logic."""
    patch = {
        "patch_type": "add_instruction",
        "intent_id": "AG_X",
        "passing_dependents": ["gs_001"],
        "high_collateral_risk": False,
    }
    out = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("gs_001",),
    )
    assert out["safe"] is True
    assert out["reason"] != "passing_dependents_missing"


def test_e1_compute_passing_dependents_empty_corpus() -> None:
    """Empty benchmarks -> no dependents, low risk."""
    deps, high_risk = compute_passing_dependents_for_proposal(
        {"patch_type": "add_instruction", "intent_id": "AG_X"},
        benchmarks=(),
        ag_target_qids=("gs_001",),
        prev_failure_qids=(),
    )
    assert list(deps) == []
    assert high_risk is False


def test_e1_compute_passing_dependents_returns_pair() -> None:
    """Smoke test — the helper exists, returns 2-tuple, never raises
    on a well-formed minimal proposal."""
    result = compute_passing_dependents_for_proposal(
        {
            "patch_type": "add_instruction",
            "intent_id": "AG_X",
        },
        benchmarks=(
            {"qid": "gs_001", "result_correctness": "yes"},
            {"qid": "gs_002", "result_correctness": "yes"},
        ),
        ag_target_qids=("gs_001",),
        prev_failure_qids=(),
    )
    assert isinstance(result, tuple)
    assert len(result) == 2
    deps, high_risk = result
    assert isinstance(list(deps), list)
    assert isinstance(high_risk, bool)
