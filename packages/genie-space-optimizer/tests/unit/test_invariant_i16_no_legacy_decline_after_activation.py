"""Plan 10 Phase B2 — I16 illegal-decline-after-activation.

Catches the silent fall-through to the legacy archetype path even when
individual Plan 10 fixes regress: within a single ``(ag_id, iteration)``
window, if a marker with status ``anchor_entered_plan5_dispatch`` fires,
then ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` records with skipped_reason
``no_top_n_archetype`` or ``no_archetype_or_slice`` are illegal UNLESS
the same window also carries a ``plan5_intent_validator_rejected``
marker with a concrete typed reason.

Verification scenarios (both required by the plan spec):

* The marker stream from the two failing production runs flags 4
  violations — one per AG-iteration that entered Plan 5 dispatch and
  then fell through to ``no_top_n_archetype`` / ``no_archetype_or_slice``
  without a typed validator-rejected marker.

* A hypothetical post-fix stream where the LLM declines explicitly with
  ``plan5_intent_validator_rejected`` + a concrete reason reports zero
  violations.
"""

from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.invariants import (
    check_i16_no_legacy_decline_after_activation,
    run_invariants,
)


def _entered(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
) -> dict:
    return {
        "marker_name": "GSO_PLAN5_ANCHOR_ACTIVATION_V1",
        "optimization_run_id": run_id,
        "iteration": iteration,
        "ag_id": ag_id,
        "cluster_id": cluster_id,
        "status": "anchor_entered_plan5_dispatch",
        "reason": "",
        "patch_type": "",
        "intent_id": "",
    }


def _validator_rejected(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    reason: str,
) -> dict:
    return {
        "marker_name": "GSO_PLAN5_ANCHOR_ACTIVATION_V1",
        "optimization_run_id": run_id,
        "iteration": iteration,
        "ag_id": ag_id,
        "cluster_id": cluster_id,
        "status": "plan5_intent_validator_rejected",
        "reason": reason,
        "patch_type": "",
        "intent_id": "intent-1",
    }


def _no_structural(
    *,
    ag_id: str,
    iteration: int,
    skipped_reason: str,
) -> dict:
    return {
        "marker_name": "GSO_NO_STRUCTURAL_CANDIDATE_V1",
        "ag_id": ag_id,
        "iteration": iteration,
        "attempted_archetypes": [],
        "skipped_reason": skipped_reason,
    }


def _two_failing_runs_pre_fix_evidence() -> dict:
    """Activation markers for the 4 entered AGs + matching
    ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` records with
    ``no_top_n_archetype`` / ``no_archetype_or_slice`` — exactly the
    silent fall-through pattern Plan 10 Phase B2 must catch."""
    return {
        "activation_markers": [
            _entered(
                run_id="59a173d3-airline",
                iteration=2,
                ag_id="AG_DECOMPOSED_H001",
                cluster_id="C_gs009",
            ),
            _entered(
                run_id="59a173d3-airline",
                iteration=2,
                ag_id="AG_DECOMPOSED_H002",
                cluster_id="C_gs024",
            ),
            _entered(
                run_id="ab65fefe-7now",
                iteration=3,
                ag_id="AG_DECOMPOSED_H001",
                cluster_id="C_gs013",
            ),
            _entered(
                run_id="ab65fefe-7now",
                iteration=3,
                ag_id="AG_DECOMPOSED_H002",
                cluster_id="C_gs026",
            ),
        ],
        "no_structural_candidate_markers": [
            _no_structural(
                ag_id="AG_DECOMPOSED_H001",
                iteration=2,
                skipped_reason="no_top_n_archetype",
            ),
            _no_structural(
                ag_id="AG_DECOMPOSED_H002",
                iteration=2,
                skipped_reason="no_archetype_or_slice",
            ),
            _no_structural(
                ag_id="AG_DECOMPOSED_H001",
                iteration=3,
                skipped_reason="no_top_n_archetype",
            ),
            _no_structural(
                ag_id="AG_DECOMPOSED_H002",
                iteration=3,
                skipped_reason="no_top_n_archetype",
            ),
        ],
    }


def _post_fix_evidence() -> dict:
    """Hypothetical post-fix run: every entered marker is paired with a
    typed ``plan5_intent_validator_rejected`` marker carrying a concrete
    reason. The same ``no_top_n_archetype`` / ``no_archetype_or_slice``
    skipped_reasons are now legal because the typed decline explains
    them."""
    pre = _two_failing_runs_pre_fix_evidence()
    return {
        "activation_markers": pre["activation_markers"] + [
            _validator_rejected(
                run_id="59a173d3-airline",
                iteration=2,
                ag_id="AG_DECOMPOSED_H001",
                cluster_id="C_gs009",
                reason="blame_set_outside_identifier_allowlist",
            ),
            _validator_rejected(
                run_id="59a173d3-airline",
                iteration=2,
                ag_id="AG_DECOMPOSED_H002",
                cluster_id="C_gs024",
                reason="target_object_unknown_column",
            ),
            _validator_rejected(
                run_id="ab65fefe-7now",
                iteration=3,
                ag_id="AG_DECOMPOSED_H001",
                cluster_id="C_gs013",
                reason="required_constructs_empty",
            ),
            _validator_rejected(
                run_id="ab65fefe-7now",
                iteration=3,
                ag_id="AG_DECOMPOSED_H002",
                cluster_id="C_gs026",
                reason="repair_shape_unsupported",
            ),
        ],
        "no_structural_candidate_markers": pre["no_structural_candidate_markers"],
    }


# ── Direct invariant tests ───────────────────────────────────────────────


def test_i16_pre_fix_stream_reports_four_violations() -> None:
    """Evidence fed from the two failing runs reports exactly 4
    violations — one per AG-iteration with ``anchor_entered_plan5_dispatch``
    + a legacy skipped_reason + no paired validator-rejected marker."""
    evidence = _two_failing_runs_pre_fix_evidence()
    violations = check_i16_no_legacy_decline_after_activation(evidence)
    assert len(violations) == 4, (
        f"expected exactly 4 violations on the pre-fix stream, "
        f"got {len(violations)}: {violations!r}"
    )
    assert {v["invariant_id"] for v in violations} == {"I16"}
    keys = {(v["ag_id"], v["iteration"], v["skipped_reason"])
            for v in violations}
    assert keys == {
        ("AG_DECOMPOSED_H001", 2, "no_top_n_archetype"),
        ("AG_DECOMPOSED_H002", 2, "no_archetype_or_slice"),
        ("AG_DECOMPOSED_H001", 3, "no_top_n_archetype"),
        ("AG_DECOMPOSED_H002", 3, "no_top_n_archetype"),
    }


def test_i16_post_fix_stream_reports_zero_violations() -> None:
    """Hypothetical post-fix run where the LLM declines with a typed
    ``plan5_intent_validator_rejected`` marker (carrying a concrete
    reason) makes the same skipped_reasons legal — zero violations."""
    violations = check_i16_no_legacy_decline_after_activation(
        _post_fix_evidence()
    )
    assert violations == [], (
        f"expected zero violations on the post-fix stream, "
        f"got: {violations!r}"
    )


# ── Carve-out semantics ──────────────────────────────────────────────────


def test_i16_validator_rejected_with_empty_reason_does_not_carve_out() -> None:
    """The carve-out requires a CONCRETE typed reason. A
    validator-rejected marker with empty/missing reason does NOT
    satisfy the carve-out — that would just be relabelling the silent
    fall-through."""
    evidence = {
        "activation_markers": [
            _entered(
                run_id="run-1",
                iteration=1,
                ag_id="AG_A",
                cluster_id="C_A",
            ),
            _validator_rejected(
                run_id="run-1",
                iteration=1,
                ag_id="AG_A",
                cluster_id="C_A",
                reason="",
            ),
        ],
        "no_structural_candidate_markers": [
            _no_structural(
                ag_id="AG_A",
                iteration=1,
                skipped_reason="no_top_n_archetype",
            ),
        ],
    }
    violations = check_i16_no_legacy_decline_after_activation(evidence)
    assert len(violations) == 1
    assert violations[0]["ag_id"] == "AG_A"


@pytest.mark.parametrize(
    "legal_skipped_reason",
    [
        "format_afs_failed",
        "validate_afs_rejected",
        "safety_cap_reached",
        "missing_rca_card",
    ],
)
def test_i16_silent_on_non_legacy_skipped_reasons(
    legal_skipped_reason: str,
) -> None:
    """Other ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` skipped_reasons are
    legitimate synthesizer outcomes (format/validate gate hits, cap
    reached, no RCA card) — they are NOT the silent-fall-through
    pattern I16 catches."""
    evidence = {
        "activation_markers": [
            _entered(
                run_id="run-1",
                iteration=1,
                ag_id="AG_A",
                cluster_id="C_A",
            ),
        ],
        "no_structural_candidate_markers": [
            _no_structural(
                ag_id="AG_A",
                iteration=1,
                skipped_reason=legal_skipped_reason,
            ),
        ],
    }
    violations = check_i16_no_legacy_decline_after_activation(evidence)
    assert violations == [], (
        f"skipped_reason {legal_skipped_reason!r} must not trigger "
        f"I16; got: {violations!r}"
    )


def test_i16_silent_when_no_anchor_entered_in_window() -> None:
    """``GSO_NO_STRUCTURAL_CANDIDATE_V1`` with a legacy skipped_reason
    is fine when the corresponding AG-iteration never entered Plan 5
    dispatch (e.g. forbidden-set filter dropped it first, or the
    legacy lever-5 path ran without the LLM lane firing). I16 only
    catches the post-activation fall-through."""
    evidence = {
        "activation_markers": [
            _entered(
                run_id="run-1",
                iteration=1,
                ag_id="AG_A",
                cluster_id="C_A",
            ),
        ],
        "no_structural_candidate_markers": [
            _no_structural(
                ag_id="AG_B",
                iteration=1,
                skipped_reason="no_top_n_archetype",
            ),
        ],
    }
    violations = check_i16_no_legacy_decline_after_activation(evidence)
    assert violations == []


def test_i16_silent_on_legacy_fixtures_without_markers() -> None:
    """Pre-Plan-9 fixtures carry neither ``activation_markers`` nor
    ``no_structural_candidate_markers``. I16 must stay silent."""
    violations = check_i16_no_legacy_decline_after_activation({
        "iterations": [{"iteration": 1, "decision_records": []}],
    })
    assert violations == []


def test_i16_violation_payload_is_json_serialisable() -> None:
    """Violations bubble through ``run_invariants`` -> contract-health
    -> stdout marker. The payload must be JSON-serialisable."""
    violations = check_i16_no_legacy_decline_after_activation(
        _two_failing_runs_pre_fix_evidence()
    )
    encoded = json.dumps(violations)
    decoded = json.loads(encoded)
    assert len(decoded) == 4
    keys_of_first = set(decoded[0].keys())
    assert {"invariant_id", "title", "detail", "ag_id", "iteration",
            "skipped_reason"}.issubset(keys_of_first)


def test_i16_wired_into_run_invariants_aggregator() -> None:
    """``run_invariants`` aggregates I16 alongside I15 + the other
    canonical checks."""
    evidence = _two_failing_runs_pre_fix_evidence()
    out = run_invariants(evidence)
    i16s = [v for v in out if v.get("invariant_id") == "I16"]
    assert len(i16s) == 4
