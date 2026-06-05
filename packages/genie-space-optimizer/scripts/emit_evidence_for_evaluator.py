#!/usr/bin/env python3
"""Deterministic ``EVIDENCE FOR EVALUATOR`` block emitter for the /goal harness.

This is the only emitter for the block. It is invoked by the
``gso-emit-evidence-for-evaluator`` skill at the end of every assistant
turn under a ``/goal`` session.

Contract (full spec):
``packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/04-evidence-for-evaluator-protocol.md``

Tracker convention (canonical):
``packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md``
uses ``## Trial N — Title`` (H2, em-dash, integer N) and is APPENDED to
(newest-at-bottom). The latest trial is the LAST canonical match in
document order.

Anchors (canonical):
``e94376a3-d8a6-4570-a605-9fe231e5f99c`` (airline) and
``d13938e7-405d-4444-833a-03f5ac9f7523`` (7now). The legacy ``dc89d1a9``
and ``98ec8950`` anchors are deprecated; use the canonical IDs above.

Inputs (all CLI args):
  ``--trial INT`` (required)
      Latest ``## Trial N`` row number in the canonical tracker.
      The skill resolves this with ``grep -E '^## Trial [0-9]+ — '``
      before invoking.
  ``--phase {offline,trigger,postmortem,plan,land,idle}`` (required)
      The dominant work this turn.
  ``--opt-run-id-airline STR`` and ``--opt-run-id-7now STR``
      Most recent GSO optimization_run_id per anchor, or ``none`` if
      no trial has run yet. ``airline`` = ``e94376a3``; ``7now`` =
      ``d13938e7``.
  ``--offline-iterations INT``
      Number of forward-pipeline test invocations this turn.
  ``--offline-target-stage STR``
      Funnel-stage vocabulary; see file ``02-funnel-and-goal-conditions.md``.
  ``--offline-target-test PATH``
      Pytest path matching the target stage (forward-pipeline file or
      tape replay file).
  ``--offline-target-test-status {pass,fail,skipped,unknown}``
  ``--real-trial-required-reason STR``
      ``none``, ``genie_api``, ``mlflow_reeval``, or ``other:<reason>``.
  ``--next STR``
      action_id the harness plans to take on turn N+1.
  ``--architecture-invariants-held {true,false,unknown}``
  ``--deepest-stage-airline STR`` and ``--deepest-stage-7now STR``
      One of: hard_qid_seen, diagnosed, clustered, proposed, normalized,
      applyable, applied, evaluated, accepted, unknown.
  ``--funnel-advanced-this-trial {true,false,unknown}``
  ``--funnel-advanced-vs-prior-trial {true,false,unknown}``
  ``--tracker PATH`` (optional override; default resolves to canonical path)
  ``--postmortem-root PATH`` (optional override; default ``runid_analysis/``)

The script reads the tracker file to extract the latest ``## Trial N``
section (verbatim), and the per-anchor postmortem files for the three
literal ``## Verdict`` lines. If a file or section is missing it emits
the documented ``unknown`` fallback rather than failing — the block
must always be well-formed.

Output: writes the block to stdout, then exits 0. No other output is
produced on stdout; stderr carries warnings.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Optional


VALID_PHASES = {"offline", "trigger", "postmortem", "plan", "land", "idle"}
VALID_STAGES = {
    "hard_qid_seen",
    "diagnosed",
    "clustered",
    "proposed",
    "normalized",
    "applyable",
    "applied",
    "evaluated",
    "accepted",
    "unknown",
}
VALID_TRIBOOL = {"true", "false", "unknown"}


CANONICAL_ANCHOR_AIRLINE = "e94376a3-d8a6-4570-a605-9fe231e5f99c"
CANONICAL_ANCHOR_7NOW = "d13938e7-405d-4444-833a-03f5ac9f7523"


def _default_tracker() -> Path:
    """Default tracker path: the CANONICAL architecture tracker.

    The legacy v5/ tracker
    (``docs/llmdrivenarchitecture/v5/lever-loop-architecture-and-iteration-tracker.md``)
    is deprecated; it stopped getting authoritative updates around
    Trial 21. All Trial 20+ rows live in the canonical tracker below.
    """
    here = Path(__file__).resolve()
    repo_root = here.parents[3]
    return (
        repo_root
        / "packages"
        / "genie-space-optimizer"
        / "docs"
        / "architecture"
        / "lever-loop-iteration-tracker.md"
    )


def _default_postmortem_root() -> Path:
    here = Path(__file__).resolve()
    repo_root = here.parents[3]
    return (
        repo_root
        / "packages"
        / "genie-space-optimizer"
        / "docs"
        / "runid_analysis"
    )


# Canonical trial-row heading: ``## Trial <N> — <title>`` with the
# em-dash + literal-space separator. ``<N>`` is captured as an integer.
#
# H3 ``### Trial ...`` headings (e.g. ``### Trial 21 status snapshot``)
# intentionally do NOT match because the canonical tracker is organized
# as one ``## Trial N`` per trial, with ``###`` reserved for sub-sections
# (Hypothesis, Workstreams, Status, etc.) inside a trial row.
CANONICAL_TRIAL_HEADING = re.compile(r"^## Trial (\d+) — ", re.MULTILINE)


def _extract_latest_trial_row(tracker_path: Path) -> str:
    """Return the latest canonical ``## Trial <N> — ...`` section verbatim.

    "Latest" is the LAST canonical-format heading in document order, on
    the convention (documented in the tracker preamble) that new trial
    rows are APPENDED to the bottom of the tracker. In the canonical
    tracker as of Trial 24, line offsets are: Trial 20 = line 15,
    Trial 21 = line 110, Trial 22 = line 198, Trial 23 = line 303,
    Trial 24 = line 454 — newest at bottom.

    The section runs from the matching heading until the next ``## ``
    heading (top-level only — ``### Trial`` sub-sections inside the row
    are part of the row, not a delimiter) or EOF.
    """
    if not tracker_path.exists():
        return f"(tracker not found at {tracker_path})"

    text = tracker_path.read_text(encoding="utf-8")
    matches = list(CANONICAL_TRIAL_HEADING.finditer(text))
    if not matches:
        return "(no canonical '## Trial N — ' rows found in tracker)"

    latest = matches[-1]
    section_start = latest.start()

    after = text[latest.end():]
    next_section_pattern = re.compile(r"^## ", re.MULTILINE)
    next_match = next_section_pattern.search(after)
    if next_match:
        section_end = latest.end() + next_match.start()
    else:
        section_end = len(text)

    return text[section_start:section_end].rstrip()


def _self_test(tracker_override: Optional[Path] = None) -> int:
    """Sanity-check tracker shape against the canonical trial-row format.

    Called by ``goal_bootstrap.sh`` so future tracker-format drift fails
    bootstrap loudly instead of silently emitting the wrong excerpt.

    Returns 0 on success. Returns non-zero with a ``SELF_TEST_FAIL:`` line
    on stderr describing what's wrong. Non-fatal observations are emitted
    as ``SELF_TEST_WARN:`` lines and do not affect the exit code.
    """
    tracker = tracker_override or _default_tracker()
    if not tracker.exists():
        sys.stderr.write(f"SELF_TEST_FAIL: tracker not found at {tracker}\n")
        return 1

    text = tracker.read_text(encoding="utf-8")
    canonical_matches = list(CANONICAL_TRIAL_HEADING.finditer(text))
    if not canonical_matches:
        sys.stderr.write(
            "SELF_TEST_FAIL: no canonical '## Trial N — ' rows in tracker. "
            "Either the file is empty, the tracker has moved, or every "
            "trial heading uses a non-canonical separator (e.g. ASCII '-' "
            "instead of em-dash '—'). The evidence emitter cannot identify "
            "a latest trial.\n"
        )
        return 2

    seen: dict[str, list[int]] = {}
    for m in canonical_matches:
        seen.setdefault(m.group(1), []).append(m.start())

    duplicates = {tid: starts for tid, starts in seen.items() if len(starts) > 1}
    if duplicates:
        details = ", ".join(
            f"Trial {tid} (x{len(starts)})" for tid, starts in duplicates.items()
        )
        sys.stderr.write(
            f"SELF_TEST_FAIL: canonical '## Trial N — ' format has duplicates: "
            f"{details}. Rename all but one so the evidence emitter can uniquely "
            "identify the active trial.\n"
        )
        return 3

    excerpt = _extract_latest_trial_row(tracker)
    if not excerpt.strip() or excerpt.startswith("("):
        sys.stderr.write(
            f"SELF_TEST_FAIL: _extract_latest_trial_row returned empty / error "
            f"string: {excerpt!r}\n"
        )
        return 4

    # Verify the latest trial section has at least one ``### Status``
    # sub-section AND parse its checklist. The /goal driver branches on
    # whether the latest trial has open ``- [ ]`` items.
    if "### Status" not in excerpt:
        sys.stderr.write(
            "SELF_TEST_WARN: latest trial row is missing a '### Status' "
            "sub-section. The /goal driver cannot determine open items "
            "without it. Add a 'Status' checklist to the latest trial.\n"
        )

    # Warn (don't fail) if the tracker still references the legacy anchor
    # IDs (dc89d1a9 / 98ec8950). These are deprecated; canonical anchors
    # are e94376a3 (airline) + d13938e7 (7now).
    legacy_anchors_in_tracker = []
    for legacy in ("dc89d1a9", "98ec8950"):
        if legacy in text:
            legacy_anchors_in_tracker.append(legacy)
    if legacy_anchors_in_tracker:
        sys.stderr.write(
            f"SELF_TEST_WARN: tracker still references legacy anchors "
            f"{legacy_anchors_in_tracker}. Canonical anchors are "
            f"{CANONICAL_ANCHOR_AIRLINE!r} (airline) and "
            f"{CANONICAL_ANCHOR_7NOW!r} (7now). Update tracker prose to "
            "match the canonical anchors before launching /goal mode.\n"
        )

    latest_id = canonical_matches[-1].group(1)
    first_line = excerpt.splitlines()[0] if excerpt else "(empty)"
    sys.stderr.write(
        f"SELF_TEST_OK: latest canonical trial is Trial {latest_id}; "
        f"section starts with: {first_line!r}\n"
    )
    return 0


def _extract_verdict_lines(postmortem_path: Optional[Path]) -> str:
    """Return the three literal lines from `## Verdict` section.

    Format:
        final_accuracy_pct = <X>
        architecture_invariants_held = <bool>
        verdict = <CODE>

    If file missing or section absent, returns the documented `unknown`
    fallback.
    """
    fallback = (
        "  final_accuracy_pct = unknown\n"
        "  architecture_invariants_held = unknown\n"
        "  verdict = NO_TRIAL_YET"
    )

    if postmortem_path is None or not postmortem_path.exists():
        return fallback

    text = postmortem_path.read_text(encoding="utf-8")
    verdict_match = re.search(
        r"^##\s+Verdict\s*$([\s\S]*?)(?=^##\s|\Z)",
        text,
        re.MULTILINE,
    )
    if not verdict_match:
        return fallback

    verdict_block = verdict_match.group(1)

    acc = _grep_line(verdict_block, r"final_accuracy_pct\s*=\s*(\S+)") or "unknown"
    inv = _grep_line(verdict_block, r"architecture_invariants_held\s*=\s*(\S+)") or "unknown"
    code = _grep_line(verdict_block, r"verdict\s*=\s*([A-Z0-9_]+)") or "UNKNOWN"

    return (
        f"  final_accuracy_pct = {acc}\n"
        f"  architecture_invariants_held = {inv}\n"
        f"  verdict = {code}"
    )


def _grep_line(block: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, block)
    if m is None:
        return None
    return m.group(1).strip()


def _resolve_postmortem(
    root: Path,
    opt_run_id: str,
) -> Optional[Path]:
    if opt_run_id in ("none", "unknown", ""):
        return None
    candidate = root / opt_run_id / "postmortem.md"
    if candidate.exists():
        return candidate
    return None


def main(argv: list[str]) -> int:
    # Short-circuit: ``--self-test`` runs sanity checks against the tracker
    # and exits, bypassing the normal required-args path. Honors an
    # optional ``--tracker <path>`` override even in self-test mode.
    if "--self-test" in argv:
        tracker_override: Optional[Path] = None
        for i, arg in enumerate(argv):
            if arg == "--tracker" and i + 1 < len(argv):
                tracker_override = Path(argv[i + 1])
                break
        return _self_test(tracker_override=tracker_override)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial", type=int, required=True)
    parser.add_argument("--phase", required=True, choices=sorted(VALID_PHASES))
    parser.add_argument(
        "--opt-run-id-airline",
        required=True,
        help=f"GSO optimization_run_id on the airline anchor ({CANONICAL_ANCHOR_AIRLINE}), or 'none'.",
    )
    parser.add_argument(
        "--opt-run-id-7now",
        required=True,
        dest="opt_run_id_7now",
        help=f"GSO optimization_run_id on the 7now anchor ({CANONICAL_ANCHOR_7NOW}), or 'none'.",
    )
    parser.add_argument("--offline-iterations", type=int, default=0)
    parser.add_argument("--offline-target-stage", default="unknown")
    parser.add_argument("--offline-target-test", default="unknown")
    parser.add_argument(
        "--offline-target-test-status",
        default="unknown",
        choices=["pass", "fail", "skipped", "unknown"],
    )
    parser.add_argument("--real-trial-required-reason", default="none")
    parser.add_argument("--next", dest="next_action", default="unknown")
    parser.add_argument(
        "--architecture-invariants-held",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
    )
    parser.add_argument(
        "--deepest-stage-airline",
        default="unknown",
        choices=sorted(VALID_STAGES),
    )
    parser.add_argument(
        "--deepest-stage-7now",
        dest="deepest_stage_7now",
        default="unknown",
        choices=sorted(VALID_STAGES),
    )
    parser.add_argument(
        "--funnel-advanced-this-trial",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
    )
    parser.add_argument(
        "--funnel-advanced-vs-prior-trial",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
    )
    parser.add_argument("--tracker", type=Path, default=None)
    parser.add_argument("--postmortem-root", type=Path, default=None)

    args = parser.parse_args(argv)

    tracker_path = args.tracker or _default_tracker()
    postmortem_root = args.postmortem_root or _default_postmortem_root()

    tracker_excerpt = _extract_latest_trial_row(tracker_path)
    pm_airline = _resolve_postmortem(postmortem_root, args.opt_run_id_airline)
    pm_7now = _resolve_postmortem(postmortem_root, args.opt_run_id_7now)
    verdict_airline = _extract_verdict_lines(pm_airline)
    verdict_7now = _extract_verdict_lines(pm_7now)

    block = "\n".join(
        [
            "EVIDENCE FOR EVALUATOR",
            "---tracker excerpt---",
            tracker_excerpt,
            "---postmortem excerpts---",
            f"airline ({CANONICAL_ANCHOR_AIRLINE}) ## Verdict:",
            verdict_airline,
            f"7now ({CANONICAL_ANCHOR_7NOW}) ## Verdict:",
            verdict_7now,
            "---funnel status---",
            f"deepest_stage_reached_airline = {args.deepest_stage_airline}",
            f"deepest_stage_reached_7now = {args.deepest_stage_7now}",
            f"funnel_advanced_this_trial = {args.funnel_advanced_this_trial}",
            f"funnel_advanced_vs_prior_trial = {args.funnel_advanced_vs_prior_trial}",
            "---offline iteration---",
            f"offline_iterations_this_turn = {args.offline_iterations}",
            f"offline_target_stage = {args.offline_target_stage}",
            f"offline_target_test = {args.offline_target_test}",
            f"offline_target_test_status = {args.offline_target_test_status}",
            f"real_trial_required_reason = {args.real_trial_required_reason}",
            "---status---",
            (
                f"GOAL_HARNESS_STATUS_V1 "
                f"trial={args.trial} "
                f"phase={args.phase} "
                f"opt_run_id_airline={args.opt_run_id_airline} "
                f"opt_run_id_7now={args.opt_run_id_7now} "
                f"architecture_invariants_held={args.architecture_invariants_held} "
                f"next={args.next_action}"
            ),
            "END EVIDENCE FOR EVALUATOR",
        ]
    )

    sys.stdout.write(block + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
