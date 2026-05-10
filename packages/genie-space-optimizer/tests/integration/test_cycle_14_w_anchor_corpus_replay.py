"""Cycle 14-W T7 / Cycle 14-W Hardening Delta T1 — anchor corpus
replay end-to-end, driven by vendored anchor fixtures.

Replays both vendored anchor fixtures and asserts all six C14-W
defect closures hold simultaneously:

  D-3 ext  T1 — `target_soft_passing_qids` field present in payload;
                payload renders the fixture's actual delta state
                without contradiction.
  D-4      T2 — `_normalize_stage_capture` survives every stage
                capture in the fixture.
  D-5      T3 — manifest IDs non-blank + trace marker emitted with
                a valid `resolution_path` (env-driven path).
  D-1      T4 — admission default-on; NO_ACTION reflections in the
                fixture contribute to forbidden set without env
                override.
  D-7      T5 — `check_iteration_summary_totality` returns ``None``
                on a clean (counter == summary_count == record_length)
                replay.
  D-6 + D-8 T6 — `detect_phase_h_acceptance_drift` /
                ``_journey_drift`` return ``False`` on
                canonical-vs-canonical comparison.

Discipline C: regressed-defect closures require integration tests
driven by the actual production failure shapes (vendored anchor
fixtures), not synthesised hand-built objects.
"""

from __future__ import annotations

import io
import json
import os
import re
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    _detect_render_contradictions,
    format_full_eval_marker_payload,
)
from genie_space_optimizer.optimization.harness import (
    _compute_forbidden_ag_set,
    _databricks_ids_from_env,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    check_iteration_summary_totality,
    detect_phase_h_acceptance_drift,
    detect_phase_h_journey_drift,
)
_DOCS_DIR = (
    Path(__file__).resolve().parents[2] / "docs" / "runid_analysis"
)
_ANCHOR_7NOW = (
    _DOCS_DIR / "3b050ec5-4032-457f-a785-2d1a3942a097"
    / "evidence" / "replay_fixture_from_latest_export_960148942255012.json"
)
_ANCHOR_AIRLINE = (
    _DOCS_DIR / "1099b152-8655-4f1e-ab43-1240a9400280"
    / "evidence" / "replay_fixture_from_latest_export_1105451933925748.json"
)


# ── Fixture loaders ──────────────────────────────────────────────────


@pytest.fixture(scope="module")
def fixture_7now() -> dict:
    if not _ANCHOR_7NOW.exists():
        pytest.skip(f"7Now anchor fixture not vendored: {_ANCHOR_7NOW}")
    return json.loads(_ANCHOR_7NOW.read_text())


@pytest.fixture(scope="module")
def fixture_airline() -> dict:
    if not _ANCHOR_AIRLINE.exists():
        pytest.skip(f"airline anchor fixture not vendored: {_ANCHOR_AIRLINE}")
    return json.loads(_ANCHOR_AIRLINE.read_text())


# ── Fixture shape parsers ────────────────────────────────────────────
#
# The vendored fixture stores acceptance metadata under
# ``decision_records[*].decision_type == 'acceptance_decided'`` with
# all bucket fields nested inside ``record["metrics"]`` (not at the
# record's top level). The ``target_delta_states`` array is a list of
# ``[qid, state]`` pairs.


def _first_acceptance_decided(records: list[dict]) -> dict | None:
    for r in records:
        if str(r.get("decision_type", "")).lower() == "acceptance_decided":
            return r
    return None


def _decision_from_fixture(
    fixture: dict, *, iteration: int,
) -> ControlPlaneAcceptance | None:
    """Reconstruct a ControlPlaneAcceptance from a captured fixture
    iteration. Reads from the first ``acceptance_decided`` record's
    ``metrics`` block (the canonical bucket-shape store). Returns
    ``None`` when the iteration has no acceptance_decided record
    (e.g., no-proposal iterations)."""
    iters = fixture.get("iterations") or []
    if iteration < 1 or iteration > len(iters):
        return None
    iter_blob = iters[iteration - 1]
    rec = _first_acceptance_decided(iter_blob.get("decision_records") or [])
    if rec is None:
        return None
    metrics = rec.get("metrics") or {}
    delta_states_raw = metrics.get("target_delta_states") or []
    delta_states = tuple(
        (str(qid), str(state)) for qid, state in delta_states_raw
    )
    return ControlPlaneAcceptance(
        accepted=str(rec.get("outcome", "")).lower() == "accepted",
        reason_code=str(rec.get("reason_code", "")),
        baseline_accuracy=float(metrics.get("baseline_accuracy", 0.0)),
        candidate_accuracy=float(metrics.get("candidate_accuracy", 0.0)),
        delta_pp=float(metrics.get("delta_pp", 0.0)),
        target_qids=tuple(rec.get("target_qids") or ()),
        target_fixed_qids=tuple(metrics.get("target_fixed_qids") or ()),
        target_still_hard_qids=tuple(
            metrics.get("target_still_hard_qids") or ()
        ),
        out_of_target_regressed_qids=tuple(
            metrics.get("out_of_target_regressed_qids") or ()
        ),
        regression_debt_qids=tuple(metrics.get("regression_debt_qids") or ()),
        soft_to_hard_regressed_qids=tuple(
            metrics.get("soft_to_hard_regressed_qids") or ()
        ),
        passing_to_hard_regressed_qids=tuple(
            metrics.get("passing_to_hard_regressed_qids") or ()
        ),
        unknown_to_hard_regressed_qids=tuple(
            metrics.get("unknown_to_hard_regressed_qids") or ()
        ),
        target_delta_states=delta_states,
    )


def _ag_id_from_fixture(fixture: dict, *, iteration: int) -> str:
    iters = fixture.get("iterations") or []
    if iteration < 1 or iteration > len(iters):
        return "AG1"
    outcomes = iters[iteration - 1].get("ag_outcomes") or {}
    if not outcomes:
        return "AG1"
    return next(iter(outcomes))


def _accepted_label_from_fixture(fixture: dict, iteration: int) -> str:
    iters = fixture.get("iterations") or []
    if iteration < 1 or iteration > len(iters):
        return "ROLLBACK"
    outcomes = iters[iteration - 1].get("ag_outcomes") or {}
    if not outcomes:
        return "ROLLBACK"
    first_outcome = str(next(iter(outcomes.values())))
    return "PASS -- ACCEPTED" if first_outcome.startswith("accepted") else "ROLLBACK"


# ── D-3 ext: target_soft_passing_qids + payload self-consistency ────


@pytest.mark.parametrize("fixture_name", ["7now", "airline"])
def test_d3_ext_canonical_render_self_consistent_on_anchor_fixtures(
    fixture_name: str,
    fixture_7now: dict,
    fixture_airline: dict,
) -> None:
    """D-3 ext binary criterion (fixture-driven): every payload
    rendered from a captured ``acceptance_decided`` carries the
    ``target_soft_passing_qids`` field AND no QID appears in two
    state buckets simultaneously. Anchor 11's iter-1 actual delta
    state (`lookup_failed`) is what the fixture stores; anchor 13's
    iter-2/3 store `fixed`. Either way, the rendered payload must
    be self-consistent."""
    fixture = fixture_7now if fixture_name == "7now" else fixture_airline
    seen_iterations = 0
    for it in range(1, len(fixture.get("iterations") or []) + 1):
        decision = _decision_from_fixture(fixture, iteration=it)
        if decision is None:
            continue
        seen_iterations += 1
        payload = format_full_eval_marker_payload(
            decision,
            ag_id=_ag_id_from_fixture(fixture, iteration=it),
            iteration=it,
            accepted_label=_accepted_label_from_fixture(fixture, it),
        )
        # New first-class bucket field exists.
        assert "target_soft_passing_qids" in payload
        # Self-consistency: contradiction rail silent.
        assert _detect_render_contradictions(payload) == [], (
            f"{fixture_name} iter {it} payload contradicts itself: "
            f"{_detect_render_contradictions(payload)}"
        )
    assert seen_iterations >= 1, (
        f"fixture {fixture_name} produced no acceptance_decided records"
    )


# ── D-4: bundle assembler list normaliser survives fixture stages ───


@pytest.mark.parametrize("fixture_name", ["7now", "airline"])
def test_d4_normalize_stage_capture_handles_every_iteration_stage(
    fixture_name: str,
    fixture_7now: dict,
    fixture_airline: dict,
    capsys,
) -> None:
    """D-4: drive the full bundle assembler against the fixture and
    confirm zero ``GSO_BUNDLE_ASSEMBLY_FAILED_V1`` markers fire.

    Cycle 14-W hardening T2 upgraded this from a unit-level walker
    over ``_normalize_stage_capture`` to an integration-level call
    of ``assemble_bundle_for_replay`` so D-4's binary criterion is
    actually exercised end-to-end, not just at the normaliser shim.
    """
    from genie_space_optimizer.optimization.run_output_bundle import (
        assemble_bundle_for_replay,
    )
    fixture = fixture_7now if fixture_name == "7now" else fixture_airline
    result = assemble_bundle_for_replay(fixture)

    out = capsys.readouterr().out
    assert "GSO_BUNDLE_ASSEMBLY_FAILED_V1" not in out, (
        f"{fixture_name} assembler emitted failure markers:\n{out}"
    )
    assert "manifest" in result
    assert "iteration_summaries" in result
    assert "decision_trace_all" in result
    assert "journey_validation_all" in result
    assert isinstance(result["manifest"].get("iterations"), list)


# ── D-5: manifest IDs non-blank + trace marker (env-driven) ─────────


def test_d5_manifest_ids_non_blank_and_trace_marker_emitted(
    monkeypatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_JOB_ID", "488860692117207")
    monkeypatch.setenv("DATABRICKS_RUN_ID", "503586982599093")
    monkeypatch.setenv("DATABRICKS_TASK_RUN_ID", "1105451933925748")

    buf = io.StringIO()
    with redirect_stdout(buf):
        ids = _databricks_ids_from_env()

    assert all(v not in ("", "unknown") for v in ids.values())
    assert "GSO_DATABRICKS_IDS_RESOLVED_V1" in buf.getvalue()
    payload = json.loads(re.search(
        r"GSO_DATABRICKS_IDS_RESOLVED_V1\s+(\{.*\})", buf.getvalue()
    ).group(1))
    assert payload["resolution_path"] == "env"


# ── D-1: admission default-on; NO_ACTION reflections from fixture ──


def test_d1_admission_default_on_admits_fixture_no_action_reflections(
    fixture_7now: dict,
) -> None:
    """The 7Now fixture's iters 2-5 emitted ``no_proposals`` so the
    NO_ACTION reflections classify as such. With C14-W T4's
    default-flip in place, the forbidden set is non-empty after
    processing these reflections, even without an env override."""
    no_action_reflections: list[dict] = []
    for it_idx, it_blob in enumerate(fixture_7now.get("iterations") or [], start=1):
        outcomes = it_blob.get("ag_outcomes") or {}
        for ag_id, outcome in outcomes.items():
            if str(outcome).lower() not in ("no_proposals", "ag_collision_with_forbidden_set"):
                continue
            # Reconstruct a NO_ACTION-shaped reflection from the
            # fixture's iteration metadata. The fixture doesn't
            # always carry full reflection_buffer state; we
            # synthesise the minimum identity tuple needed for
            # admission.
            no_action_reflections.append({
                "iteration": it_idx,
                "rollback_class": "no_action",
                "rollback_reason": str(outcome),
                "accepted": False,
                "escalation_handled": False,
                "root_cause": "plural_top_n_collapse",
                "blame_set": ("mv_esr_dim_location.zone_vp_name",),
                "lever_set": [1, 5],
            })

    if not no_action_reflections:
        pytest.skip("7Now fixture has no NO_ACTION reflections to admit")

    with mock.patch.dict(os.environ, {}, clear=True):
        forbidden = _compute_forbidden_ag_set(no_action_reflections)
    assert forbidden, (
        "expected the default-on flag to admit at least one NO_ACTION "
        "reflection from the 7Now fixture"
    )


# ── D-7: iteration summary totality on a clean fixture ──────────────


@pytest.mark.parametrize("fixture_name", ["7now", "airline"])
def test_d7_check_iteration_summary_totality_helper_clean_when_aligned(
    fixture_name: str,
    fixture_7now: dict,
    fixture_airline: dict,
) -> None:
    """The pure helper returns ``None`` when all three cardinalities
    match. The fixture-driven aspect: we read the iteration count
    directly from the fixture and prove the helper would not fire on
    a hypothetically-clean replay."""
    fixture = fixture_7now if fixture_name == "7now" else fixture_airline
    n = len(fixture.get("iterations") or [])
    assert check_iteration_summary_totality(
        iteration_counter=n,
        iteration_summary_count=n,
        phase_b_iter_record_counts_length=n,
    ) is None


# ── D-6 / D-8: drift detectors silent on canonical-vs-canonical ─────


@pytest.mark.parametrize("fixture_name", ["7now", "airline"])
def test_d6_acceptance_drift_silent_on_canonical_self_compare(
    fixture_name: str,
    fixture_7now: dict,
    fixture_airline: dict,
) -> None:
    """For each iteration that has an acceptance decision, comparing
    the canonical decision against itself must return False (no
    drift). This is the precondition for the production wiring to
    only fire on real disagreement."""
    fixture = fixture_7now if fixture_name == "7now" else fixture_airline
    seen = 0
    for it in range(1, len(fixture.get("iterations") or []) + 1):
        decision = _decision_from_fixture(fixture, iteration=it)
        if decision is None:
            continue
        seen += 1
        outcome_str = "accepted" if decision.accepted else "rolled_back"
        assert detect_phase_h_acceptance_drift(
            canonical_outcome=outcome_str,
            canonical_reason_code=decision.reason_code,
            phase_h_outcome=outcome_str,
            phase_h_reason_code=decision.reason_code,
        ) is False
    assert seen >= 1, f"{fixture_name} fixture produced no acceptance decisions"


@pytest.mark.parametrize("fixture_name", ["7now", "airline"])
def test_d8_journey_drift_silent_on_canonical_self_compare(
    fixture_name: str,
    fixture_7now: dict,
    fixture_airline: dict,
) -> None:
    fixture = fixture_7now if fixture_name == "7now" else fixture_airline
    total_violations = 0
    for it_blob in fixture.get("iterations") or []:
        jv = it_blob.get("journey_validation") or {}
        violations = jv.get("violations") or []
        total_violations += len(violations)
    assert detect_phase_h_journey_drift(
        canonical_violation_count=total_violations,
        phase_h_violation_count=total_violations,
    ) is False


# ── Cross-defect: contradiction rail silent on every fixture iter ───


@pytest.mark.parametrize("fixture_name", ["7now", "airline"])
def test_both_anchors_emit_zero_canonical_render_contradictions(
    fixture_name: str,
    fixture_7now: dict,
    fixture_airline: dict,
    capsys,
) -> None:
    fixture = fixture_7now if fixture_name == "7now" else fixture_airline
    for it in range(1, len(fixture.get("iterations") or []) + 1):
        decision = _decision_from_fixture(fixture, iteration=it)
        if decision is None:
            continue
        format_full_eval_marker_payload(
            decision,
            ag_id=_ag_id_from_fixture(fixture, iteration=it),
            iteration=it,
            accepted_label=_accepted_label_from_fixture(fixture, it),
        )
    out = capsys.readouterr().out
    assert "GSO_CANONICAL_RENDER_INVARIANT_V1" not in out, out
