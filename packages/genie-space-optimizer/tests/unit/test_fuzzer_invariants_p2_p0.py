"""Phase 2/0 outer-rails invariants — narrow unit tests for K6/K7/K8
in ``devtools/local_lever_workbench/fuzzer/invariants.py``. The
fixture-driven harness in ``tests/workbench/test_state_machine_invariants.py``
already validates these against committed production-replay tapes;
this file covers the predicate logic in isolation so a CI failure
points at the predicate rather than at the fixture chain.
"""
from __future__ import annotations

import sys
from pathlib import Path

# The workbench package lives under ``devtools/`` and is not on the
# default sys.path. Make it importable for these unit tests without
# disturbing the production tree.
_DEVTOOLS = (
    Path(__file__).resolve().parents[2] / "devtools"
)
if str(_DEVTOOLS) not in sys.path:
    sys.path.insert(0, str(_DEVTOOLS))

from local_lever_workbench.fuzzer.invariants import (  # noqa: E402
    _REGISTRY,
    _check_k6_atomic_bundle_apply,
    _check_k7_kit_completeness,
    _check_k8_iteration_itpm_ceiling,
)


class _Artifacts:
    final_states = ()
    stdout_text = ""


def test_registry_contains_k6_k7_k8_and_retires_k5() -> None:
    ids = [k for k, _ in _REGISTRY]
    assert "K5" not in ids
    assert "K6" in ids
    assert "K7" in ids
    assert "K8" in ids


def test_k6_passes_when_no_partial_marker_present() -> None:
    violations = _check_k6_atomic_bundle_apply(_Artifacts(), {})
    assert violations == []


def test_k6_passes_when_partial_paired_with_typed_terminal() -> None:
    markers = {
        "GSO_BUNDLE_APPLY_OUTCOME_V1": [
            {"status": "partial", "bundle_id": "b1", "iteration": 3, "run_id": "r"},
        ],
        "GSO_ITERATION_NO_CANDIDATE_V1": [
            {"terminal_reason": "bundle_partial_apply", "iteration": 3, "run_id": "r"},
        ],
    }
    violations = _check_k6_atomic_bundle_apply(_Artifacts(), markers)
    assert violations == []


def test_k6_flags_partial_without_paired_terminal() -> None:
    markers = {
        "GSO_BUNDLE_APPLY_OUTCOME_V1": [
            {"status": "partial", "bundle_id": "b1", "iteration": 3, "run_id": "r"},
        ],
        "GSO_ITERATION_NO_CANDIDATE_V1": [
            {"terminal_reason": "no_applied_patches", "iteration": 3, "run_id": "r"},
        ],
    }
    violations = _check_k6_atomic_bundle_apply(_Artifacts(), markers)
    assert len(violations) == 1
    assert violations[0].invariant_id == "K6"


def test_k7_passes_when_no_violation_markers_present() -> None:
    violations = _check_k7_kit_completeness(_Artifacts(), {})
    assert violations == []


def test_k7_flags_each_kit_for_rca_violation_marker() -> None:
    markers = {
        "GSO_PHASE2_KIT_FOR_RCA_VIOLATION_V1": [
            {
                "qid": "q1",
                "rca_kind": "missing_join_path",
                "selected_levers": ["lever-3"],
                "reason": "missing_companion_lever-4",
                "iteration": 2,
            },
            {
                "qid": "q2",
                "rca_kind": "ambiguous_aggregation",
                "selected_levers": [],
                "reason": "singleton_kit_for_mandated_rca",
                "iteration": 2,
            },
        ],
    }
    violations = _check_k7_kit_completeness(_Artifacts(), markers)
    assert len(violations) == 2
    assert {v.qid for v in violations} == {"q1", "q2"}
    assert all(v.invariant_id == "K7" for v in violations)


def test_k8_passes_when_usage_below_ceiling() -> None:
    markers = {
        "GSO_ITERATION_TOKEN_BUDGET_V1": [
            {"iteration": 1, "run_id": "r", "input_tokens_used": 119_999},
        ],
    }
    violations = _check_k8_iteration_itpm_ceiling(_Artifacts(), markers)
    assert violations == []


def test_k8_passes_when_usage_at_ceiling_exactly() -> None:
    markers = {
        "GSO_ITERATION_TOKEN_BUDGET_V1": [
            {"iteration": 1, "run_id": "r", "input_tokens_used": 120_000},
        ],
    }
    violations = _check_k8_iteration_itpm_ceiling(_Artifacts(), markers)
    assert violations == []


def test_k8_flags_iteration_exceeding_ceiling() -> None:
    markers = {
        "GSO_ITERATION_TOKEN_BUDGET_V1": [
            {"iteration": 1, "run_id": "r", "input_tokens_used": 120_001},
        ],
    }
    violations = _check_k8_iteration_itpm_ceiling(_Artifacts(), markers)
    assert len(violations) == 1
    v = violations[0]
    assert v.invariant_id == "K8"
    assert v.evidence["input_tokens_used"] == 120_001
    assert v.evidence["ceiling"] == 120_000


def test_k8_uses_latest_observed_value_per_iteration() -> None:
    # Multiple samples for the same (run, iteration) — the
    # predicate takes the LATEST (last in stream) so intermediate
    # values from an in-progress iteration don't false-positive.
    markers = {
        "GSO_ITERATION_TOKEN_BUDGET_V1": [
            {"iteration": 1, "run_id": "r", "input_tokens_used": 50_000},
            {"iteration": 1, "run_id": "r", "input_tokens_used": 90_000},
            {"iteration": 1, "run_id": "r", "input_tokens_used": 119_500},
        ],
    }
    violations = _check_k8_iteration_itpm_ceiling(_Artifacts(), markers)
    assert violations == []
