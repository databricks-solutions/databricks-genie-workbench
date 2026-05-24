#!/usr/bin/env python3
"""Deterministic ``EVIDENCE FOR EVALUATOR`` block emitter for the /goal harness.

This is the only emitter for the block. It is invoked by the
``gso-emit-evidence-for-evaluator`` skill at the end of every assistant
turn under a ``/goal`` session.

Contract (full spec):
``packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/04-evidence-for-evaluator-protocol.md``

Inputs (all CLI args):
  ``--trial INT`` (required)
      Latest ``### Trial N`` row number in the iteration tracker.
      The skill resolves this with ``grep -E '^### Trial [0-9]+'``
      before invoking.
  ``--phase {offline,trigger,postmortem,plan,land,idle}`` (required)
      The dominant work this turn.
  ``--opt-run-id-dc89d1a9 STR`` and ``--opt-run-id-98ec8950 STR``
      Most recent GSO optimization_run_id per anchor, or ``none`` if
      no trial has run yet.
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
  ``--deepest-stage-dc89d1a9 STR`` and ``--deepest-stage-98ec8950 STR``
      One of: hard_qid_seen, diagnosed, clustered, proposed, normalized,
      applyable, applied, evaluated, accepted, unknown.
  ``--funnel-advanced-this-trial {true,false,unknown}``
  ``--funnel-advanced-vs-prior-trial {true,false,unknown}``
  ``--tracker PATH`` (optional override; default resolves to repo path)
  ``--postmortem-root PATH`` (optional override; default ``runid_analysis/``)

The script reads the tracker file to extract the latest ``### Trial N``
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


def _default_tracker() -> Path:
    """Default tracker path relative to this script's location."""
    here = Path(__file__).resolve()
    repo_root = here.parents[3]
    return (
        repo_root
        / "packages"
        / "genie-space-optimizer"
        / "docs"
        / "llmdrivenarchitecture"
        / "v5"
        / "lever-loop-architecture-and-iteration-tracker.md"
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


def _extract_latest_trial_row(tracker_path: Path) -> str:
    """Return the latest `### Trial N` section verbatim.

    The section is everything from the highest-numbered ``### Trial N``
    heading until the next ``### Trial`` heading or the next top-level
    ``## `` heading or EOF.
    """
    if not tracker_path.exists():
        return f"(tracker not found at {tracker_path})"

    text = tracker_path.read_text(encoding="utf-8")
    pattern = re.compile(r"^### Trial (\d+)\b", re.MULTILINE)
    matches = list(pattern.finditer(text))
    if not matches:
        return "(no ### Trial N rows found in tracker)"

    highest = max(matches, key=lambda m: int(m.group(1)))
    section_start = highest.start()

    after = text[highest.end():]
    next_section_pattern = re.compile(r"^(### Trial \d+|## )", re.MULTILINE)
    next_match = next_section_pattern.search(after)
    if next_match:
        section_end = highest.end() + next_match.start()
    else:
        section_end = len(text)

    return text[section_start:section_end].rstrip()


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial", type=int, required=True)
    parser.add_argument("--phase", required=True, choices=sorted(VALID_PHASES))
    parser.add_argument("--opt-run-id-dc89d1a9", required=True)
    parser.add_argument("--opt-run-id-98ec8950", required=True)
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
        "--deepest-stage-dc89d1a9",
        default="unknown",
        choices=sorted(VALID_STAGES),
    )
    parser.add_argument(
        "--deepest-stage-98ec8950",
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
    pm_dc = _resolve_postmortem(postmortem_root, args.opt_run_id_dc89d1a9)
    pm_98 = _resolve_postmortem(postmortem_root, args.opt_run_id_98ec8950)
    verdict_dc = _extract_verdict_lines(pm_dc)
    verdict_98 = _extract_verdict_lines(pm_98)

    block = "\n".join(
        [
            "EVIDENCE FOR EVALUATOR",
            "---tracker excerpt---",
            tracker_excerpt,
            "---postmortem excerpts---",
            "dc89d1a9 ## Verdict:",
            verdict_dc,
            "98ec8950 ## Verdict:",
            verdict_98,
            "---funnel status---",
            f"deepest_stage_reached_dc89d1a9 = {args.deepest_stage_dc89d1a9}",
            f"deepest_stage_reached_98ec8950 = {args.deepest_stage_98ec8950}",
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
                f"opt_run_id_dc89d1a9={args.opt_run_id_dc89d1a9} "
                f"opt_run_id_98ec8950={args.opt_run_id_98ec8950} "
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
