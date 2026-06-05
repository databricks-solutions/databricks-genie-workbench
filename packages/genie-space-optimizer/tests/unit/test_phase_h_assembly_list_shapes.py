"""P4 C9 regression tests — Phase H bundle assembly does NOT raise
``AttributeError: 'list' object has no attribute 'get'`` on
list-shaped iteration blobs or fixture-level manifest sections.

Anchors:
  * e943's ``GSO_BUNDLE_ASSEMBLY_FAILED_V1`` postmortem traced an
    AttributeError to ``assemble_bundle_for_replay`` consuming a
    list-shaped iteration blob.
  * Cycle 14-V / 14-W previously closed a similar gap inside
    ``_normalize_stage_capture``; P4 C9 closes the boundary call
    sites that bypassed it.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.run_output_bundle import (
    _ensure_dict,
    _normalize_stage_capture,
    assemble_bundle_for_replay,
)


def test_ensure_dict_passes_through_dict():
    assert _ensure_dict({"a": 1}) == {"a": 1}


def test_ensure_dict_collapses_list_of_dict_to_first_dict():
    assert _ensure_dict([{"a": 1}, {"a": 2}]) == {"a": 1}


def test_ensure_dict_empty_list_returns_empty_dict():
    assert _ensure_dict([]) == {}


def test_ensure_dict_none_returns_empty_dict():
    assert _ensure_dict(None) == {}


def test_ensure_dict_scalar_returns_empty_dict():
    assert _ensure_dict("scalar") == {}
    assert _ensure_dict(42) == {}


def test_assemble_bundle_for_replay_with_dict_fixture_succeeds():
    bundle = assemble_bundle_for_replay({
        "fixture_id": "fx_001",
        "baseline_accuracy": 0.5,
        "final_accuracy": 0.6,
        "delta_pp": 10.0,
        "iterations": [
            {
                "iteration": 1,
                "decision_records": [],
                "journey_violations": [],
                "stages": {},
            }
        ],
    })
    assert "manifest" in bundle
    assert "run_summary" in bundle


def test_assemble_bundle_for_replay_with_list_shaped_iteration_blob():
    """Regression for e943: an iteration blob arrives as a list
    instead of a dict (e.g. when the upstream report serialized
    decision records into a list directly). Must NOT raise."""
    fixture = {
        "fixture_id": "fx_e943_regression",
        "baseline_accuracy": 0.875,
        "final_accuracy": 0.957,
        "delta_pp": 8.2,
        "iterations": [
            # The buggy shape: a list-wrapped iteration blob.
            [
                {
                    "iteration": 1,
                    "decision_records": [{"id": "r1"}],
                    "journey_violations": [],
                    "stages": {"stage_a": {"payload": "ok"}},
                }
            ],
        ],
    }
    bundle = assemble_bundle_for_replay(fixture)
    assert "manifest" in bundle
    assert "run_summary" in bundle


def test_assemble_bundle_for_replay_with_list_shaped_fixture_top_level():
    """Even the top-level fixture being a list (single-element wrap)
    must not raise."""
    fixture_as_list = [
        {
            "fixture_id": "fx_top_list",
            "baseline_accuracy": 0.5,
            "final_accuracy": 0.6,
            "delta_pp": 10.0,
            "iterations": [],
        }
    ]
    bundle = assemble_bundle_for_replay(fixture_as_list)
    assert "manifest" in bundle


def test_assemble_bundle_for_replay_with_empty_iterations_list():
    bundle = assemble_bundle_for_replay({
        "fixture_id": "fx_empty",
        "baseline_accuracy": 0.0,
        "final_accuracy": 0.0,
        "delta_pp": 0.0,
        "iterations": [],
    })
    assert "manifest" in bundle


def test_assemble_bundle_for_replay_with_list_shaped_stages_section():
    """An iteration's ``stages`` field is a list rather than a dict.
    Must not raise; the list is simply skipped (no per-stage
    normalization happens, but bundle assembly succeeds)."""
    fixture = {
        "fixture_id": "fx_list_stages",
        "baseline_accuracy": 0.5,
        "final_accuracy": 0.5,
        "delta_pp": 0.0,
        "iterations": [
            {
                "iteration": 1,
                "decision_records": [],
                "journey_violations": [],
                "stages": [{"stage_a": "ok"}],  # list-shape
            }
        ],
    }
    bundle = assemble_bundle_for_replay(fixture)
    assert "manifest" in bundle


def test_normalize_stage_capture_existing_dict_passthrough():
    """The pre-P4 contract still holds: dicts pass through unchanged."""
    payload = {"key": "value", "nested": {"a": 1}}
    out = _normalize_stage_capture(payload, stage_key="s", iteration=1)
    assert out == payload
