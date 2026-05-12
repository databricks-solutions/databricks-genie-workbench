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
