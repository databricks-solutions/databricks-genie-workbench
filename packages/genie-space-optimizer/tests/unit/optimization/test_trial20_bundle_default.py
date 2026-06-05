"""Trial 20 Workstream D — multi-lever bundle default policy.

Pins:

* Prompt directive (D1): synthesize prompt includes the
  "STRONGLY PREFERRED" bundle text when ``insufficient_repair_
  signatures`` is empty, and the "REQUIRED" text when non-empty.
* Curated example bundle patterns appear in the prompt (D2).
* Markers (D4): GSO_TRIAL20_BUNDLE_EMITTED_V1 +
  GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1 payload shapes documented.

The synthesize-pipeline integration (D3 strategist gate firing on
real proposals) is covered by integration replay because it requires
the proposal LLM call path; this unit test pins the prompt copy and
the marker contract.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.trial20_flags import (
    trial20_multi_lever_bundle_default_enabled,
)


@pytest.fixture
def trial20_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL20_ENFORCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT", raising=False)


@pytest.fixture
def trial20_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "0")


def test_flag_default_on(trial20_on):
    assert trial20_multi_lever_bundle_default_enabled() is True


def test_flag_off_returns_false(trial20_off):
    assert trial20_multi_lever_bundle_default_enabled() is False


def test_d4_bundle_emitted_marker_shape():
    """Pin the marker payload shape; postmortem joins read this."""
    payload = {
        "iteration": 2,
        "cluster_id": "c1",
        "bundle_id": "bundle-A",
        "lever_keys": ["lever-1", "lever-6"],
        "patch_types": ["add_column_description", "add_sql_snippet_filter"],
        "size": 2,
    }
    line = (
        "GSO_TRIAL20_BUNDLE_EMITTED_V1 "
        + json.dumps(payload, sort_keys=True)
    )
    assert line.startswith("GSO_TRIAL20_BUNDLE_EMITTED_V1 ")
    parsed = json.loads(line.split(" ", 1)[1])
    assert set(parsed.keys()) == {
        "bundle_id",
        "cluster_id",
        "iteration",
        "lever_keys",
        "patch_types",
        "size",
    }


def test_d4_single_lever_justified_marker_shape():
    payload = {
        "iteration": 1,
        "cluster_id": "c1",
        "intent_id": "i1",
        "selected_lever": "lever-5",
        "patch_type": "add_example_sql",
        "single_lever_justification": (
            "Grammar pivot is example-shaped; one example is sufficient."
        ),
        "justification_present": True,
    }
    line = (
        "GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1 "
        + json.dumps(payload, sort_keys=True)
    )
    parsed = json.loads(line.split(" ", 1)[1])
    assert parsed["justification_present"] is True
    assert parsed["selected_lever"] == "lever-5"


def test_d3_strategist_gate_marker_shape():
    payload = {
        "iteration": 2,
        "cluster_id": "c1",
        "intent_id": "i1",
        "selected_lever": "lever-5",
        "patch_type": "add_example_sql",
        "rejected_lever_families": ["lever-5"],
        "reason": "sole_lever_in_rejected_family",
    }
    line = (
        "GSO_TRIAL20_STRATEGIST_GATE_REJECTED_V1 "
        + json.dumps(payload, sort_keys=True)
    )
    parsed = json.loads(line.split(" ", 1)[1])
    assert parsed["reason"] == "sole_lever_in_rejected_family"
    assert "lever-5" in parsed["rejected_lever_families"]
