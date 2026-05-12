"""Offline acceptance-policy replay tool.

Reads a hand-curated ReplayPayload JSON fixture (extracted from a real
captured ``parsed_stdout_summary_*.json``), runs it through the
existing ``decide_control_plane_acceptance`` / ``evaluate_regression_debt``
classifier, and emits a single ``replay_classifier_decision`` JSON
line per payload to stdout.

This is a Phase-0 validation tool: it answers the question "would the
existing pilot RegressionDebtPolicy accept any candidate from the
captured runs?" without touching production code, the Databricks API,
or any cached state. Pure I/O against on-disk fixtures.

Invocation:

    python -m genie_space_optimizer.tools.policy_replay \\
        --fixtures-dir tests/replay/fixtures/policy_replay/ \\
        --predictions tests/replay/fixtures/policy_replay/predictions.json
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from dataclasses import asdict, dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class ReplayPayload:
    """One iteration's eval/acceptance bundle as captured offline.

    Field semantics mirror ``ControlPlaneAcceptance`` so a payload can
    be lifted directly into a synthesized decision in
    ``classify_payload`` (Task 5). ``payload_present=False`` is the
    sentinel for runs where no candidate was ever built (e.g. the
    airline 31ecd96f run, where ``acceptance_decision`` was ``{}``).
    """
    fixture_id: str
    run_id: str
    iteration: int
    ag_id: str | None
    payload_present: bool
    baseline_post_arbiter: float
    candidate_post_arbiter: float
    baseline_pre_arbiter: float | None
    candidate_pre_arbiter: float | None
    target_qids: tuple[str, ...]
    target_fixed_qids: tuple[str, ...]
    target_still_hard_qids: tuple[str, ...]
    out_of_target_regressed_qids: tuple[str, ...]
    soft_to_hard_regressed_qids: tuple[str, ...]
    passing_to_hard_regressed_qids: tuple[str, ...]
    unknown_to_hard_regressed_qids: tuple[str, ...]
    accepted_in_recorded_run: bool
    reason_code_in_recorded_run: str
    source_notes: str = ""


_REQUIRED_KEYS: tuple[str, ...] = (
    "fixture_id",
    "run_id",
    "iteration",
    "ag_id",
    "payload_present",
    "baseline_post_arbiter",
    "candidate_post_arbiter",
    "baseline_pre_arbiter",
    "candidate_pre_arbiter",
    "target_qids",
    "target_fixed_qids",
    "target_still_hard_qids",
    "out_of_target_regressed_qids",
    "soft_to_hard_regressed_qids",
    "passing_to_hard_regressed_qids",
    "unknown_to_hard_regressed_qids",
    "accepted_in_recorded_run",
    "reason_code_in_recorded_run",
)


def _qid_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TypeError(
            f"expected list for qid bucket, got {type(value).__name__}"
        )
    return tuple(str(q) for q in value if str(q))


def load_payload(path: pathlib.Path) -> ReplayPayload:
    """Parse a hand-curated ReplayPayload fixture from disk.

    Raises ``KeyError`` (with the missing field name) if a required
    field is absent. Does NOT validate semantic consistency between
    fields (e.g., that ``out_of_target_regressed_qids`` equals the
    union of the three sub-buckets) — that is enforced by the runtime
    invariant ``assert_regression_debt_partition_complete`` and is
    out of scope for the offline replay.
    """
    raw = json.loads(path.read_text())
    for key in _REQUIRED_KEYS:
        if key not in raw:
            raise KeyError(f"required field {key!r} missing from {path}")

    return ReplayPayload(
        fixture_id=str(raw["fixture_id"]),
        run_id=str(raw["run_id"]),
        iteration=int(raw["iteration"]),
        ag_id=None if raw["ag_id"] is None else str(raw["ag_id"]),
        payload_present=bool(raw["payload_present"]),
        baseline_post_arbiter=float(raw["baseline_post_arbiter"]),
        candidate_post_arbiter=float(raw["candidate_post_arbiter"]),
        baseline_pre_arbiter=(
            None
            if raw["baseline_pre_arbiter"] is None
            else float(raw["baseline_pre_arbiter"])
        ),
        candidate_pre_arbiter=(
            None
            if raw["candidate_pre_arbiter"] is None
            else float(raw["candidate_pre_arbiter"])
        ),
        target_qids=_qid_tuple(raw["target_qids"]),
        target_fixed_qids=_qid_tuple(raw["target_fixed_qids"]),
        target_still_hard_qids=_qid_tuple(raw["target_still_hard_qids"]),
        out_of_target_regressed_qids=_qid_tuple(
            raw["out_of_target_regressed_qids"]
        ),
        soft_to_hard_regressed_qids=_qid_tuple(
            raw["soft_to_hard_regressed_qids"]
        ),
        passing_to_hard_regressed_qids=_qid_tuple(
            raw["passing_to_hard_regressed_qids"]
        ),
        unknown_to_hard_regressed_qids=_qid_tuple(
            raw["unknown_to_hard_regressed_qids"]
        ),
        accepted_in_recorded_run=bool(raw["accepted_in_recorded_run"]),
        reason_code_in_recorded_run=str(raw["reason_code_in_recorded_run"]),
        source_notes=str(raw.get("source_notes", "")),
    )


from genie_space_optimizer.optimization.acceptance_policy import (
    RegressionDebtPolicy,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    evaluate_regression_debt,
)


# Map evaluate_regression_debt's reason_code (when under_policy=False)
# to the field name on RegressionDebtPolicy that gated the candidate.
# This is the structured "missing tier identification" channel: a
# mismatch between prediction and classification surfaces this field
# so a designer can decide which threshold to relax or which new
# tier to introduce.
_GATE_FOR_REASON: dict[str, str] = {
    "no_target_clusters_fixed": "min_target_clusters_fixed",
    "aggregate_gain_below_floor": "min_aggregate_improvement_pp",
    "threshold_pass_rate_below_floor": "min_threshold_pass_rate",
    "debt_exceeds_per_iter_max": "max_debt_qids",
    "debt_bucket_disallowed": "allowed_debt_buckets",
    "cumulative_debt_cap_hit": "cumulative_debt_max",
}


@dataclass(frozen=True)
class ReplayClassification:
    """Result of running one ReplayPayload through one
    RegressionDebtPolicy.

    ``accepted=None`` and ``reason_code='no_payload'`` when the
    payload has ``payload_present=False`` (the airline 31ecd96f
    sentinel). ``first_failed_gate`` is non-None only when the
    classifier rejected the candidate AND the rejection mapped to
    a known policy gate; this is the structured handle the
    prediction-comparison test uses to satisfy the spec's pass
    criterion ("mismatch identifies a missing tier").
    """
    fixture_id: str
    policy_name: str
    payload_present: bool
    accepted: bool | None
    reason_code: str
    debt_qids: tuple[str, ...]
    first_failed_gate: str | None
    policy_diagnostics: dict = field(default_factory=dict)


def _synthesize_decision(payload: ReplayPayload) -> ControlPlaneAcceptance:
    """Build a ControlPlaneAcceptance from a ReplayPayload.

    The synthesized decision is constructed with ``accepted=False``
    and ``reason_code='target_qids_not_improved'`` so that
    ``evaluate_regression_debt`` is called with the canonical
    "rejected legacy decision that may or may not be promotable to
    accept-with-debt" shape — the same shape
    ``decide_control_plane_acceptance`` synthesizes internally
    before delegating to the debt evaluator (see control_plane.py
    lines 1632–1660).
    """
    delta = round(
        float(payload.candidate_post_arbiter)
        - float(payload.baseline_post_arbiter),
        1,
    )
    target_delta_states: tuple[tuple[str, str], ...] = ()
    if payload.target_qids:
        states: list[tuple[str, str]] = []
        fixed_set = set(payload.target_fixed_qids)
        still_set = set(payload.target_still_hard_qids)
        for q in payload.target_qids:
            if q in fixed_set:
                states.append((q, DeltaState.FIXED.value))
            elif q in still_set:
                states.append((q, DeltaState.STILL_HARD.value))
            else:
                states.append((q, DeltaState.LOOKUP_FAILED.value))
        target_delta_states = tuple(sorted(states))

    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=round(float(payload.baseline_post_arbiter), 1),
        candidate_accuracy=round(float(payload.candidate_post_arbiter), 1),
        delta_pp=delta,
        target_qids=payload.target_qids,
        target_fixed_qids=payload.target_fixed_qids,
        target_still_hard_qids=payload.target_still_hard_qids,
        out_of_target_regressed_qids=payload.out_of_target_regressed_qids,
        regression_debt_qids=(),
        protected_regressed_qids=(),
        soft_to_hard_regressed_qids=payload.soft_to_hard_regressed_qids,
        passing_to_hard_regressed_qids=payload.passing_to_hard_regressed_qids,
        unknown_to_hard_regressed_qids=payload.unknown_to_hard_regressed_qids,
        target_delta_states=target_delta_states,
    )


def classify_payload(
    *,
    payload: ReplayPayload,
    policy: RegressionDebtPolicy,
    policy_name: str,
    cumulative_debt: int = 0,
    threshold_pass_rate: float = 1.0,
) -> ReplayClassification:
    """Classify one ReplayPayload under one RegressionDebtPolicy.

    Pure: no I/O, no globals. Reuses production
    ``evaluate_regression_debt`` so the classification reflects
    exactly what a Stage-9 acceptance call would do for the same
    bucket lists.
    """
    if not payload.payload_present:
        return ReplayClassification(
            fixture_id=payload.fixture_id,
            policy_name=policy_name,
            payload_present=False,
            accepted=None,
            reason_code="no_payload",
            debt_qids=(),
            first_failed_gate=None,
            policy_diagnostics={},
        )

    decision = _synthesize_decision(payload)
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=policy,
        cumulative_debt=int(cumulative_debt),
        threshold_pass_rate=float(threshold_pass_rate),
    )

    if verdict.under_policy and verdict.debt_qids:
        return ReplayClassification(
            fixture_id=payload.fixture_id,
            policy_name=policy_name,
            payload_present=True,
            accepted=True,
            reason_code="accepted_with_partial_harvest_debt",
            debt_qids=verdict.debt_qids,
            first_failed_gate=None,
            policy_diagnostics=dict(verdict.policy_diagnostics),
        )

    if verdict.under_policy and not verdict.debt_qids:
        # Verdict says "under policy but no debt to harvest". In
        # production decide_control_plane_acceptance would keep the
        # legacy `accepted` reason for this case (control_plane.py
        # line 1859 onwards). For replay, surface as accepted with
        # a distinct reason_code so the comparison test can tell the
        # two branches apart.
        return ReplayClassification(
            fixture_id=payload.fixture_id,
            policy_name=policy_name,
            payload_present=True,
            accepted=True,
            reason_code="no_debt_to_harvest",
            debt_qids=(),
            first_failed_gate=None,
            policy_diagnostics=dict(verdict.policy_diagnostics),
        )

    return ReplayClassification(
        fixture_id=payload.fixture_id,
        policy_name=policy_name,
        payload_present=True,
        accepted=False,
        reason_code=verdict.reason_code,
        debt_qids=verdict.debt_qids,
        first_failed_gate=_GATE_FOR_REASON.get(verdict.reason_code),
        policy_diagnostics=dict(verdict.policy_diagnostics),
    )


def format_replay_classifier_decision(
    *,
    classification: ReplayClassification,
    prediction: dict | None,
) -> dict:
    """Build the JSONL row written by the CLI per fixture.

    Schema: ``{"event": "replay_classifier_decision", ...}``. The
    ``match`` boolean is true when classification matches prediction
    exactly. ``structured_mismatch`` is true when classification
    differs from prediction AND ``first_failed_gate`` is non-None
    (the spec's "mismatch identifies a missing tier" channel).
    """
    row: dict = {
        "event": "replay_classifier_decision",
        "fixture_id": classification.fixture_id,
        "policy_name": classification.policy_name,
        "payload_present": classification.payload_present,
        "observed_accepted": classification.accepted,
        "observed_reason_code": classification.reason_code,
        "debt_qids": list(classification.debt_qids),
        "first_failed_gate": classification.first_failed_gate,
        "policy_diagnostics": classification.policy_diagnostics,
    }
    if prediction is None:
        row["match"] = False
        row["structured_mismatch"] = False
        row["match_status"] = "no_prediction_registered"
        return row

    row["predicted_accepted"] = prediction.get("predicted_accepted")
    row["predicted_reason_code"] = prediction.get("predicted_reason_code")
    matched = (
        bool(prediction["predicted_accepted"]) == bool(classification.accepted)
        and prediction["predicted_reason_code"] == classification.reason_code
        if prediction["predicted_accepted"] is not None
        else (
            classification.accepted is None
            and prediction["predicted_reason_code"] == classification.reason_code
        )
    )
    row["match"] = matched
    row["structured_mismatch"] = (
        not matched
        and classification.payload_present
        and classification.accepted is False
        and classification.first_failed_gate is not None
    )
    if matched:
        row["match_status"] = "exact_match"
    elif row["structured_mismatch"]:
        row["match_status"] = "mismatch_identifies_missing_tier"
    else:
        row["match_status"] = "unstructured_mismatch"
    return row


def _iter_fixture_paths(
    fixtures_dir: pathlib.Path, predictions_path: pathlib.Path
) -> Iterable[tuple[str, pathlib.Path]]:
    raw = json.loads(predictions_path.read_text())
    for prediction in raw["predictions"]:
        fixture_id = str(prediction["fixture_id"])
        yield fixture_id, fixtures_dir / f"{fixture_id}.json"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m genie_space_optimizer.tools.policy_replay",
        description=(
            "Phase 0 offline acceptance-policy replay. Reads ReplayPayload "
            "fixtures, classifies each under the pilot RegressionDebtPolicy, "
            "compares to pre-registered predictions, and emits one "
            "replay_classifier_decision JSON line per fixture to stdout."
        ),
    )
    parser.add_argument(
        "--fixtures-dir",
        type=pathlib.Path,
        required=True,
        help="Directory containing <fixture_id>.json ReplayPayload fixtures",
    )
    parser.add_argument(
        "--predictions",
        type=pathlib.Path,
        required=True,
        help="Path to predictions.json file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    raw = json.loads(args.predictions.read_text())
    predictions_by_id = {
        str(p["fixture_id"]): p for p in raw["predictions"]
    }
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )
    policy = regression_debt_policy_pilot_default()

    matches = 0
    structured_mismatches = 0
    unstructured_mismatches = 0
    for fixture_id, fixture_path in _iter_fixture_paths(
        args.fixtures_dir, args.predictions
    ):
        if not fixture_path.exists():
            row = {
                "event": "replay_classifier_decision",
                "fixture_id": fixture_id,
                "match_status": "fixture_missing",
                "fixture_path": str(fixture_path),
            }
            print(json.dumps(row))
            unstructured_mismatches += 1
            continue
        payload = load_payload(fixture_path)
        classification = classify_payload(
            payload=payload,
            policy=policy,
            policy_name="regression_debt_policy_pilot_default",
        )
        row = format_replay_classifier_decision(
            classification=classification,
            prediction=predictions_by_id.get(fixture_id),
        )
        print(json.dumps(row))
        if row["match"]:
            matches += 1
        elif row["structured_mismatch"]:
            structured_mismatches += 1
        else:
            unstructured_mismatches += 1

    summary = {
        "event": "replay_classifier_summary",
        "policy_name": "regression_debt_policy_pilot_default",
        "matches": matches,
        "structured_mismatches": structured_mismatches,
        "unstructured_mismatches": unstructured_mismatches,
        "pass_criterion_met": unstructured_mismatches == 0,
    }
    print(json.dumps(summary))
    return 0 if summary["pass_criterion_met"] else 1


if __name__ == "__main__":
    sys.exit(main())
