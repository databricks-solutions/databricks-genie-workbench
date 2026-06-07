"""Trial 27 W27.3 — Starting Point Gate force-lever-loop override.

Pre-Trial-27 the lever_loop notebook's Starting Point Gate skipped
the loop whenever ``thresholds_met=True`` (sourced from baseline_eval
or post-enrichment). That made the kit gate unverifiable on the
airline anchor (baseline 91.3% already ``thresholds_met=true``) —
the live W26.5 verification reported
``verdict=LEVER_LOOP_SKIPPED_POST_ENRICHMENT_MEETS_THRESHOLDS`` with
zero kit-loop evidence.

W27.3 adds a verification-only override: when the harness sets the
per-run signal AND the deploy-time capability flag
``GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE`` is ON, the gate emits an
observability marker (``GSO_TRIAL27_FORCE_LEVER_LOOP_V1``) carrying
the would-have-skipped reason but does NOT skip — the lever loop
runs anyway so the kit gate is exercised.

No per-anchor / per-QID / per-space_id logic anywhere — pure harness
knob owned by the trial.

Pins (pure decision function):

* Default behaviour byte-stable: ``thresholds_met=False`` → no skip,
  no override marker.
* Default behaviour byte-stable: ``thresholds_met=True`` + signal
  False → skip, no override marker.
* Override engaged: ``thresholds_met=True`` + signal True +
  capability ON → DO NOT skip, override marker emitted.
* Capability OFF kills the override: ``thresholds_met=True`` +
  signal True + capability OFF → skip (signal alone insufficient).
* Override never engages when ``thresholds_met=False`` (it would
  be a no-op anyway, but the marker MUST stay silent so postmortems
  can count override engagements faithfully).
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.starting_point_gate import (
    SkipDecision,
    should_skip_starting_point_gate,
    starting_point_gate_force_marker,
)


def _decide(
    monkeypatch,
    *,
    thresholds_met,
    accuracy_source,
    force_signal,
    capability_env_value=None,
):
    """Helper: set the capability env var and call the gate."""
    if capability_env_value is None:
        monkeypatch.delenv(
            "GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE", raising=False
        )
        monkeypatch.delenv("GSO_TRIAL27_STAGE3_DESTARVE", raising=False)
    else:
        monkeypatch.setenv(
            "GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE",
            capability_env_value,
        )
        monkeypatch.delenv("GSO_TRIAL27_STAGE3_DESTARVE", raising=False)
    return should_skip_starting_point_gate(
        thresholds_met=thresholds_met,
        accuracy_source=accuracy_source,
        force_lever_loop_signal=force_signal,
    )


# ---- Default byte-stable behaviour --------------------------------


def test_thresholds_not_met_no_skip(monkeypatch):
    """When thresholds aren't met the gate never skips, regardless of
    the override signal/capability."""
    d = _decide(
        monkeypatch,
        thresholds_met=False,
        accuracy_source="baseline_eval",
        force_signal=False,
    )
    assert isinstance(d, SkipDecision)
    assert d.skip is False
    assert d.override_engaged is False
    assert d.would_have_skipped_reason is None


def test_thresholds_met_default_skips(monkeypatch):
    """thresholds_met=True with no override signal → skip
    (pre-Trial-27 behaviour). Reason matches the live notebook's
    decoded skip string."""
    d = _decide(
        monkeypatch,
        thresholds_met=True,
        accuracy_source="enrichment.post_enrichment_accuracy",
        force_signal=False,
    )
    assert d.skip is True
    assert d.override_engaged is False
    assert d.reason == "post_enrichment_meets_thresholds"


def test_thresholds_met_baseline_source_skips_with_baseline_reason(
    monkeypatch,
):
    d = _decide(
        monkeypatch,
        thresholds_met=True,
        accuracy_source="baseline_eval",
        force_signal=False,
    )
    assert d.skip is True
    assert d.reason == "baseline_meets_thresholds"


# ---- Override engaged ---------------------------------------------


def test_override_engaged_runs_lever_loop_anyway(monkeypatch):
    """Signal True + capability ON + thresholds_met → DO NOT skip;
    override engaged; the would-have-skipped reason is captured for
    observability."""
    d = _decide(
        monkeypatch,
        thresholds_met=True,
        accuracy_source="enrichment.post_enrichment_accuracy",
        force_signal=True,
    )
    assert d.skip is False
    assert d.override_engaged is True
    assert (
        d.would_have_skipped_reason
        == "post_enrichment_meets_thresholds"
    )


def test_override_engaged_baseline_source(monkeypatch):
    """Captures the baseline-source skip reason when override engages
    against a sub-threshold baseline."""
    d = _decide(
        monkeypatch,
        thresholds_met=True,
        accuracy_source="baseline_eval",
        force_signal=True,
    )
    assert d.skip is False
    assert d.override_engaged is True
    assert d.would_have_skipped_reason == "baseline_meets_thresholds"


# ---- Capability flag forces the override OFF ----------------------


def test_capability_off_disables_override(monkeypatch):
    """Deploy-time capability OFF means the signal is ignored — the
    gate behaves like pre-Trial-27 and skips on thresholds_met=True.
    """
    d = _decide(
        monkeypatch,
        thresholds_met=True,
        accuracy_source="enrichment.post_enrichment_accuracy",
        force_signal=True,
        capability_env_value="0",
    )
    assert d.skip is True
    assert d.override_engaged is False
    assert d.reason == "post_enrichment_meets_thresholds"


def test_master_off_disables_capability(monkeypatch):
    """Master OFF cascades to capability OFF — confirms the master
    is the single emergency rollback knob even for the W27.3 path.
    """
    monkeypatch.setenv("GSO_TRIAL27_STAGE3_DESTARVE", "0")
    monkeypatch.delenv(
        "GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE", raising=False
    )
    d = should_skip_starting_point_gate(
        thresholds_met=True,
        accuracy_source="enrichment.post_enrichment_accuracy",
        force_lever_loop_signal=True,
    )
    assert d.skip is True
    assert d.override_engaged is False


# ---- Override silent when thresholds not met ----------------------


def test_override_silent_when_thresholds_not_met(monkeypatch):
    """When thresholds aren't met the override is a no-op AND must
    NOT emit the override marker — postmortems count engagements,
    not capability presence.
    """
    d = _decide(
        monkeypatch,
        thresholds_met=False,
        accuracy_source="baseline_eval",
        force_signal=True,
    )
    assert d.skip is False
    assert d.override_engaged is False
    assert d.would_have_skipped_reason is None


# ---- Marker payload ----------------------------------------------


def test_marker_payload_shape():
    line = starting_point_gate_force_marker(
        optimization_run_id="run_x",
        would_have_skipped_reason="post_enrichment_meets_thresholds",
        accuracy_source="enrichment.post_enrichment_accuracy",
        post_enrichment_accuracy=95.65,
        baseline_accuracy=91.3,
    )
    assert line.startswith("GSO_TRIAL27_FORCE_LEVER_LOOP_V1 ")
    payload = json.loads(line.split(" ", 1)[1])
    assert (
        payload["would_have_skipped_reason"]
        == "post_enrichment_meets_thresholds"
    )
    assert (
        payload["accuracy_source"]
        == "enrichment.post_enrichment_accuracy"
    )
    assert payload["post_enrichment_accuracy"] == 95.65
    assert payload["baseline_accuracy"] == 91.3
    assert payload["optimization_run_id"] == "run_x"


def test_marker_handles_missing_post_enrichment_accuracy():
    """Baseline-only runs (post_enrichment_accuracy=None) still
    produce a well-formed marker."""
    line = starting_point_gate_force_marker(
        optimization_run_id="run_x",
        would_have_skipped_reason="baseline_meets_thresholds",
        accuracy_source="baseline_eval",
        post_enrichment_accuracy=None,
        baseline_accuracy=91.3,
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["post_enrichment_accuracy"] is None
    assert payload["baseline_accuracy"] == 91.3
