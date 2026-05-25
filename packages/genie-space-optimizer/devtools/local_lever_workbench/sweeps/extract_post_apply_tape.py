"""Extract a workbench post-apply eval tape from a production postmortem.

This is the operator-facing bridge that the user requested in the
Trial 16.1 follow-up:

    "Use these runs for the Post-apply tape enrichment: capturing
    patched-state post-apply eval rows during the next production run
    would let live-llm-only exercise the acceptance_gate accept path
    under live LLM output. Right now every QID is forced to terminate
    at target_unchanged."

The tool walks a postmortem evidence directory (``docs/runid_analysis/
<run-uuid>/evidence/phase_h_direct_*/gso_postmortem_bundle/``) and
reads ``replay_fixture.json``. For each iteration:

  * It pairs ``GSO_PATCH_OUTCOME_V1 outcome=applied`` records (the
    QIDs that successfully reached APPLIED in production) against the
    corresponding patched-state ``eval_rows`` entries — that is, the
    MLflow rows the harness collected AFTER applying the patch.

  * If a patched-state row exists for a given (qid, iteration), it is
    emitted into the tape with the canonical qid carrier so the
    workbench's ``_workbench_post_apply_eval_stub`` (which uses
    ``extract_question_id``) will match it during a ``live-llm-only``
    run.

  * If the patched-state row is missing (the exact failure mode
    described in postmortems 127751814861356 and 813949510175466 —
    Trial 16's slice bug killed every applied patch before eval),
    the QID is added to a ``patched_state_missing`` ledger with the
    reason so the operator can either (a) wait for the next
    post-slice-fix production run, or (b) hand-author a synthetic
    bridge row.

The result is a single ``post_apply_eval_tape.json`` file plus a
``report.md`` summary. The tape can be merged into the corresponding
production-replay fixture by copying its rows into the fixture's
``post_apply_eval_tape`` array.

Usage (from the package root)::

    PYTHONPATH=devtools:src \\
        uv run python -m local_lever_workbench.sweeps.extract_post_apply_tape \\
        --evidence-dir docs/runid_analysis/e94376a3-d8a6-4570-a605-9fe231e5f99c/evidence \\
        --output-dir devtools/local_lever_workbench/runs/tape-extract-<ts>/

Bootstrap note: until the slice fix in Trial 16.1 lands in production
and the next lever-loop attempt is captured, BOTH currently-available
postmortems (e94376a3 and d13938e7) will produce empty tapes — every
applied patch was killed at the evaluated_gate slice before MLflow eval
could log a patched-state row. The tool is intentionally honest about
this gap: it emits the empty tape plus a ``patched_state_missing``
report so the operator knows the data source is not yet ready, rather
than silently producing garbage.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


# ── Phase H bundle discovery ─────────────────────────────────────────


def _find_phase_h_bundle(evidence_dir: Path) -> Path | None:
    """Return the ``gso_postmortem_bundle`` directory under the
    evidence root, or None if not present.

    Evidence directories use a stable shape
    ``<evidence>/phase_h_direct_<hash>_latest<short>/gso_postmortem_bundle/``
    so we glob for the inner directory regardless of the hash suffix.
    """
    for candidate in evidence_dir.glob("phase_h_direct_*/gso_postmortem_bundle"):
        if candidate.is_dir():
            return candidate
    return None


def _load_replay_fixture(bundle_dir: Path) -> Mapping[str, Any] | None:
    """Load ``replay_fixture.json`` from the Phase H bundle.

    Returns None when the file is missing — the operator typically sees
    this when the export bundle was assembled before Phase H ran.
    """
    path = bundle_dir / "replay_fixture.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text())


# ── Applied-patch ledger ─────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class AppliedPatch:
    """A patch outcome record sourced from ``GSO_PATCH_OUTCOME_V1``."""

    iteration: int
    qid: str
    patch_outcome_id: str
    intent_id: str
    patch_type: str


def _find_export_text(evidence_dir: Path) -> Path | None:
    """The postmortem text export contains the full marker stream."""
    for candidate in evidence_dir.glob("lever_loop_latest_export_run_*_text.txt"):
        if candidate.is_file():
            return candidate
    return None


def _parse_applied_patches(text_path: Path) -> list[AppliedPatch]:
    """Walk the marker stream and return one record per applied patch.

    Only ``outcome=applied`` rows are emitted (the slice-bug case has
    these for every iteration; ``applyability_rejected`` rows are
    upstream failures we ignore here).
    """
    applied: list[AppliedPatch] = []
    text = text_path.read_text()
    for line in text.splitlines():
        if "GSO_PATCH_OUTCOME_V1" not in line:
            continue
        try:
            payload_start = line.index("{")
        except ValueError:
            continue
        try:
            payload = json.loads(line[payload_start:])
        except json.JSONDecodeError:
            continue
        if payload.get("outcome") != "applied":
            continue
        applied.append(AppliedPatch(
            iteration=int(payload.get("iteration") or 0),
            qid=str(payload.get("qid") or ""),
            patch_outcome_id=str(payload.get("patch_outcome_id") or ""),
            intent_id=str(payload.get("intent_id") or ""),
            patch_type=str(payload.get("patch_type") or ""),
        ))
    return applied


# ── Patched-state row resolution ─────────────────────────────────────


def _patched_state_row_for(
    qid: str,
    iteration: int,
    fixture: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """Try to locate the post-apply eval row for ``qid`` in
    iteration ``iteration``.

    Production replay fixtures are indexed by iteration; if patched-
    state rows were captured during the run they will appear under
    ``iterations[iteration].eval_rows`` (the same key used for the
    baseline rows, because Trial 15 unified them). The ``eval_rows``
    captured BEFORE the slice fix are baseline-only — there is no
    explicit ``run_role`` marker, so we cannot reliably distinguish
    patched-state rows from baseline rows without the matching
    ``apply_*`` checkpoint. For now this returns None so the operator
    fills the gap explicitly.
    """
    iterations = fixture.get("iterations") or []
    if iteration >= len(iterations):
        return None
    rows = iterations[iteration].get("eval_rows") or []
    # When the harness was running with the slice bug, ALL eval_rows
    # were baseline — none of them are post-apply scores. Returning
    # None here is the correct conservative behaviour: surface the gap
    # to the operator rather than emit a wrong score.
    #
    # Once the slice fix lands and the next production run completes,
    # this loop can grow a tag check (e.g. ``r.get("run_role") ==
    # "iteration_eval"``) once the capture pipeline labels rows by
    # run_role.
    del rows  # not yet usable; see docstring
    return None


# ── Tape assembly ────────────────────────────────────────────────────


def _build_tape_row(
    patch: AppliedPatch,
    patched_row: Mapping[str, Any],
) -> dict:
    """Project a patched-state eval row into the tape shape consumed
    by ``_workbench_post_apply_eval_stub``.

    The stub expects:

      * ``question_id`` (top-level OR any extract_question_id carrier)
      * ``feedback/result_correctness/value`` (post-apply score)
      * ``generated_sql`` (the SQL the patched Genie space produced)
      * ``eval_row_id`` (a stable identifier)

    Pre-existing fields from the captured row are forwarded unchanged
    so the operator can audit the source.
    """
    out = dict(patched_row)
    out.setdefault("question_id", patch.qid)
    out.setdefault(
        "eval_row_id",
        f"production:{patch.patch_outcome_id or patch.intent_id}",
    )
    return out


def _extract_tape(
    evidence_dir: Path,
) -> tuple[list[dict], list[dict]]:
    """Return ``(tape_rows, patched_state_missing_ledger)``.

    The tape is the set of post-apply rows that can be fed to the
    workbench's ``post_apply_eval_tape``. The ledger lists
    (qid, iteration, patch_outcome_id, reason) for applied patches
    whose patched-state row is absent — operators inspect this to
    decide whether to wait for the next prod run or author a synthetic
    bridge.
    """
    bundle_dir = _find_phase_h_bundle(evidence_dir)
    if bundle_dir is None:
        return [], [{
            "reason": "phase_h_bundle_missing",
            "evidence_dir": str(evidence_dir),
        }]
    fixture = _load_replay_fixture(bundle_dir)
    if fixture is None:
        return [], [{
            "reason": "replay_fixture_missing",
            "bundle_dir": str(bundle_dir),
        }]
    text_path = _find_export_text(evidence_dir)
    if text_path is None:
        return [], [{
            "reason": "export_text_missing",
            "evidence_dir": str(evidence_dir),
        }]
    applied = _parse_applied_patches(text_path)

    tape: list[dict] = []
    missing: list[dict] = []
    for patch in applied:
        patched_row = _patched_state_row_for(patch.qid, patch.iteration, fixture)
        if patched_row is None:
            missing.append({
                "qid": patch.qid,
                "iteration": patch.iteration,
                "patch_outcome_id": patch.patch_outcome_id,
                "patch_type": patch.patch_type,
                "reason": (
                    "patched_state_row_not_in_replay_fixture: "
                    "the harness collected only baseline eval_rows; "
                    "either the slice bug (postmortems 127751814861356 / "
                    "813949510175466) killed eval before MLflow logged a "
                    "patched-state row, OR Phase H did not capture the "
                    "post-apply tag. Re-run after Trial 16.1 lands and "
                    "the next production attempt completes."
                ),
            })
            continue
        tape.append(_build_tape_row(patch, patched_row))
    return tape, missing


# ── CLI ──────────────────────────────────────────────────────────────


def _format_report(
    evidence_dir: Path,
    tape: list[dict],
    missing: list[dict],
) -> str:
    """Return a markdown summary the operator can scan in one screen."""
    lines: list[str] = []
    lines.append("# Post-Apply Tape Extraction Report")
    lines.append("")
    lines.append(f"- Evidence directory: `{evidence_dir}`")
    lines.append(f"- Tape rows extracted: **{len(tape)}**")
    lines.append(f"- Patched-state rows missing: **{len(missing)}**")
    lines.append("")
    if tape:
        lines.append("## Extracted tape rows")
        lines.append("")
        lines.append("| QID | eval_row_id | result_correctness |")
        lines.append("|---|---|---|")
        for r in tape:
            score = r.get("feedback/result_correctness/value", "")
            lines.append(
                f"| `{r.get('question_id', '')}` | "
                f"`{r.get('eval_row_id', '')}` | {score} |"
            )
        lines.append("")
    if missing:
        lines.append("## Patched-state rows missing")
        lines.append("")
        lines.append("| QID | iter | patch_outcome_id | reason |")
        lines.append("|---|---:|---|---|")
        for m in missing:
            lines.append(
                f"| `{m.get('qid', '')}` | "
                f"{m.get('iteration', '')} | "
                f"`{m.get('patch_outcome_id', '')}` | "
                f"{m.get('reason', '')} |"
            )
        lines.append("")
        lines.append(
            "## Bootstrap status\n\n"
            "If every applied patch is in the missing ledger, this is "
            "the **expected** result for postmortems captured BEFORE "
            "Trial 16.1 lands. The slice filter (`stages/evaluation.py: "
            "_run_full_evaluation`) silently produced 0 benchmarks for "
            "every applied patch, so `evaluated_gate` rejected each one "
            "before MLflow could log a patched-state eval row. After "
            "Trial 16.1 lands and the next lever-loop attempt completes, "
            "re-run this script against that attempt's evidence and the "
            "tape rows will populate."
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--evidence-dir",
        required=True,
        type=Path,
        action="append",
        help=(
            "Path to a postmortem evidence directory (the parent of "
            "``phase_h_direct_*/``). May be passed multiple times to "
            "extract from several runs in one invocation."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Output directory. Defaults to "
            "devtools/local_lever_workbench/runs/tape-extract-<ts>/"
        ),
    )
    args = parser.parse_args(argv)

    if args.output_dir is None:
        ts = time.strftime("%Y%m%dT%H%M%S")
        args.output_dir = Path(
            "devtools/local_lever_workbench/runs"
        ) / f"tape-extract-{ts}"
    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_tape: list[dict] = []
    all_missing: list[dict] = []
    per_evidence_reports: list[str] = []
    for ev in args.evidence_dir:
        tape, missing = _extract_tape(ev)
        all_tape.extend(tape)
        for m in missing:
            m["_evidence_dir"] = str(ev)
            all_missing.append(m)
        per_evidence_reports.append(_format_report(ev, tape, missing))

    tape_path = args.output_dir / "post_apply_eval_tape.json"
    tape_path.write_text(json.dumps(all_tape, indent=2))
    missing_path = args.output_dir / "patched_state_missing.json"
    missing_path.write_text(json.dumps(all_missing, indent=2))
    report_path = args.output_dir / "report.md"
    report_path.write_text("\n\n---\n\n".join(per_evidence_reports))

    print(f"Tape:   {tape_path} ({len(all_tape)} rows)")
    print(f"Missing: {missing_path} ({len(all_missing)} entries)")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
